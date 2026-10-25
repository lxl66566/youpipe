//! Run-time assembly for the typed DAG engine: the shared run context
//! (`GraphCore`), worker-budget resolution, parking-lease admission,
//! edge-channel creation, the node spawn walk and completion waiting.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
};

use super::{
    GraphBuilder,
    fanin::{Broadcast, FanIn, RoundSpin},
    plan::{EdgeConnector, Erased, GraphPlan, NodeKind, NodeOpts, SpawnStep},
};
use crate::{
    builder::{
        PanicSlot, ParkingLease, PipelineConfig, cancel_active, floored_channel_capacity,
        record_panic, resume_stored_panic,
    },
    executor::compute::ComputePool,
    handoff::{Receiver as SyncReceiver, SyncSender},
    runtime::AsyncRuntime,
    sync::CancellationToken,
};

/// Fire-and-forget async hooks over the concrete runtime backend. The graph
/// plan is runtime data created before the backend type `R` is chosen
/// (`with_runtime` / `with_async_pool` may switch it after nodes were
/// registered), so the spawn steps receive the backend behind this trait
/// object instead of a monomorphic `R`. Setup-time only: each task is boxed
/// once per run, never per item.
pub(crate) trait AsyncSpawner: Send + Sync {
    /// Spawn a fire-and-forget task.
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>);
    /// Drive `fut` to completion on the calling thread (bridge threads).
    fn block_on(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>);
}

impl<R: AsyncRuntime> AsyncSpawner for R {
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        AsyncRuntime::spawn(self, fut);
    }

    fn block_on(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        AsyncRuntime::block_on(self, fut);
    }
}

/// Worker-completion counter: pre-set to the run's total worker count
/// (source feeders + sync node workers + async node tasks + async sources),
/// decremented by every dispatched job / spawned task on completion —
/// normal or caught-panic — and awaited by the run's main thread. No
/// `JoinHandle` exists to join (pool jobs cannot be joined), so this is the
/// completion signal; the main thread is never a pool worker in pool mode
/// (the nested-run case takes the dedicated-thread fallback), so parking on
/// the condvar is safe.
#[derive(Clone)]
pub(crate) struct DoneCounter(Arc<(Mutex<usize>, Condvar)>);

impl DoneCounter {
    pub(crate) fn new(total: usize) -> Self {
        Self(Arc::new((Mutex::new(total), Condvar::new())))
    }

    fn dec(&self) {
        let (lock, cv) = &*self.0;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = guard.saturating_sub(1);
        if *guard == 0 {
            cv.notify_all();
        }
    }

    fn wait(&self) {
        let (lock, cv) = &*self.0;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *guard > 0 {
            guard = cv
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// Shared per-run context handed by reference to every node's spawn step.
pub(crate) struct GraphCore {
    /// Compute pool serving sync node workers and source feeders.
    pub(crate) pool: ComputePool,
    /// Dedicated-OS-thread fallback for every sync worker / feeder: taken
    /// when the parking lease did not fit or `run()` executes on a worker
    /// of the same pool (nested runs park that worker for the whole run, so
    /// pool admission can never be guaranteed).
    pub(crate) dedicated: bool,
    /// Pool-capacity lease covering the run's whole upper bound of
    /// channel-parking jobs; `None` in dedicated mode.
    pub(crate) lease: Option<ParkingLease>,
    /// User-supplied or internal cancellation token. Worker panics cancel
    /// it too, accelerating the drain before the re-raise.
    pub(crate) cancel: CancellationToken,
    /// First panic recorded anywhere in the run (feeder, worker, task);
    /// re-raised on the caller after the drain. First writer wins.
    pub(crate) panic: PanicSlot,
    pub(crate) done: DoneCounter,
    /// Async backend hooks; built iff the graph contains async producers
    /// (`node_async` / `source_async`), before any spawn, so a construction
    /// failure surfaces as `try_run`'s `Err` instead of a half-spawned
    /// pipeline.
    pub(crate) spawner: Option<Arc<dyn AsyncSpawner>>,
}

impl GraphCore {
    /// Dispatch one sync worker / feeder body: a lease-wrapped pool job in
    /// pool mode, a dedicated OS thread otherwise. The wrapper catches every
    /// panic (a pool job's uncaught panic aborts the process via the pool's
    /// `AbortIfPanic`), records it first-wins in the shared slot, cancels
    /// the token to accelerate the drain, and decrements the completion
    /// counter on all paths.
    pub(crate) fn dispatch_job<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.dispatch_jobs(std::iter::once(job));
    }

    /// Batched variant — one pool submission for a whole node's workers
    /// (the same injector-notification amortization as the streaming
    /// `spawn_stage_jobs`).
    pub(crate) fn dispatch_jobs<F, I>(&self, jobs: I)
    where
        F: FnOnce() + Send + 'static,
        I: IntoIterator<Item = F>,
        I::IntoIter: ExactSizeIterator,
    {
        let wrap = |job: F| {
            let done = self.done.clone();
            let panic = Arc::clone(&self.panic);
            let cancel = self.cancel.clone();
            move || {
                if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)) {
                    record_panic(&panic, payload);
                    cancel.cancel();
                }
                done.dec();
            }
        };
        if self.dedicated {
            for job in jobs {
                std::thread::spawn(wrap(job));
            }
        } else {
            match &self.lease {
                Some(lease) => {
                    let wrapped = jobs.into_iter().map(|job| lease.wrap_job(wrap(job)));
                    self.pool.submit_batch(wrapped);
                },
                None => self.pool.submit_batch(jobs.into_iter().map(wrap)),
            }
        }
    }

    /// Spawn one async task on the runtime backend. The body's panics are
    /// caught at the poll boundary by `async_exec::catch_panic` (record +
    /// cancel), so this site only handles the completion accounting.
    // Called by the async execution engine (next commit).
    #[allow(dead_code)]
    pub(crate) fn spawn_task<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let spawner = self
            .spawner
            .as_ref()
            .expect("async producer requires the async spawner");
        let done = self.done.clone();
        let panic = Arc::clone(&self.panic);
        let cancel = self.cancel.clone();
        spawner.spawn(Box::pin(async move {
            super::async_exec::catch_panic(fut, panic, cancel).await;
            done.dec();
        }));
    }

    /// Per-worker batch-claim share: the global batch cap divided across the
    /// node's workers — one head CAS must not monopolize a ready burst that
    /// the node's other workers should split (same policy as the streaming
    /// `spawn_stage_fanout`'s `claim_cap`).
    fn claim_cap(workers: usize) -> usize {
        let batch_cap = crate::handoff::batch_recv_cap();
        if batch_cap == 0 {
            0
        } else {
            (batch_cap / workers.max(1)).max(1)
        }
    }
}

// ── Worker bodies ──

/// Source feeder: pull the iterator and broadcast each item to every open
/// edge. Single worker per source.
///
/// With the tx-batch knob on, items are staged in `cap`-sized batches and
/// each edge takes one `try_send_batch` per free run (`send_batch`); the
/// per-item path below is the knob-off baseline. Cancel acceptance is per
/// staged batch in the batched arm (the check precedes the send), per item
/// in the baseline; delivery itself never consults the token.
fn source_feeder_worker<T, I>(iter: I, mut out: Broadcast<T>, cancel: Option<&CancellationToken>)
where
    T: Send + Clone + 'static,
    I: Iterator<Item = T> + Send + 'static,
{
    let cap = if super::fanin::graph_tx_batch() {
        crate::handoff::batch_recv_cap()
    } else {
        0
    };
    if cap == 0 {
        for item in iter {
            if cancel_active(cancel) {
                return;
            }
            if !out.send(item) {
                return;
            }
        }
        return;
    }
    let mut iter = iter.into_iter();
    let mut batch: Vec<T> = Vec::with_capacity(cap);
    let mut staged: Vec<std::mem::MaybeUninit<T>> = Vec::with_capacity(cap);
    loop {
        batch.clear();
        for item in iter.by_ref() {
            batch.push(item);
            if batch.len() == cap {
                break;
            }
        }
        if batch.is_empty() {
            return;
        }
        if cancel_active(cancel) {
            return;
        }
        if !out.send_batch(&mut batch, &mut staged) {
            return;
        }
    }
}

/// One sync node worker: fair fan-in claim, map, broadcast. Exits on EOF
/// (every input closed), cancel, or every output edge closed. With the
/// tx-batch knob on the mapped claim batch is staged and flushed per edge
/// with one `try_send_batch` per free run (single-item claims —
/// `claim_cap == 0` — keep the per-item send: staging a 1-item batch only
/// adds moves).
///
/// Cancel acceptance is per claim batch in the batched arm (the token check
/// precedes the batch's mapping, so up to `claim_cap` items can be accepted
/// after the fire) and per item in the per-item arm; either way an accepted
/// item is always delivered — the sends never consult the token — so the
/// emitted set is independent of ring pressure (todo #21, the stream
/// engine's contract).
fn node_worker<I, O, F>(
    f: &F,
    mut fanin: FanIn<I>,
    mut out: Broadcast<O>,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
) where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: Fn(I) -> O + Send + Sync,
{
    let tx_batch = super::fanin::graph_tx_batch() && claim_cap > 0;
    let mut spin = RoundSpin::new(crate::handoff::spin_anchor_us());
    let mut scratch: Vec<I> = Vec::with_capacity(claim_cap.max(1));
    let mut mapped: Vec<O> = Vec::new();
    let mut staged: Vec<std::mem::MaybeUninit<O>> = Vec::new();
    loop {
        let claimed = fanin.claim(&mut scratch, claim_cap, &mut spin);
        if claimed == 0 {
            return; // EOF
        }
        if tx_batch {
            if cancel_active(cancel) {
                return;
            }
            mapped.clear();
            mapped.extend(scratch.drain(..).map(f));
            if !out.send_batch(&mut mapped, &mut staged) {
                return; // every output edge closed
            }
        } else {
            for item in scratch.drain(..) {
                if cancel_active(cancel) {
                    return;
                }
                if !out.send(f(item)) {
                    return; // every output edge closed
                }
            }
        }
    }
}

/// One expansion node worker: like [`node_worker`] but each input expands
/// into zero or more outputs pushed into a reused per-worker scratch `Vec`
/// (zero steady-state allocation), each broadcast downstream. The batched
/// arm flushes the staged outputs whenever they reach the batch cap — an
/// expansion's fan-out must not force the staging buffer to grow with the
/// whole claim batch's expansion.
fn expand_worker<I, O, F>(
    f: &F,
    mut fanin: FanIn<I>,
    mut out: Broadcast<O>,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
) where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: Fn(I, &mut Vec<O>) + Send + Sync,
{
    let tx_batch = super::fanin::graph_tx_batch() && claim_cap > 0;
    let flush_cap = crate::handoff::batch_recv_cap().max(claim_cap).max(1);
    let mut spin = RoundSpin::new(crate::handoff::spin_anchor_us());
    let mut scratch: Vec<I> = Vec::with_capacity(claim_cap.max(1));
    let mut buf: Vec<O> = Vec::new();
    let mut mapped: Vec<O> = Vec::new();
    let mut staged: Vec<std::mem::MaybeUninit<O>> = Vec::new();
    loop {
        let claimed = fanin.claim(&mut scratch, claim_cap, &mut spin);
        if claimed == 0 {
            return;
        }
        if tx_batch {
            if cancel_active(cancel) {
                return;
            }
            mapped.clear();
            for item in scratch.drain(..) {
                buf.clear();
                f(item, &mut buf);
                mapped.append(&mut buf);
                if mapped.len() >= flush_cap {
                    if !out.send_batch(&mut mapped, &mut staged) {
                        return;
                    }
                    mapped.clear();
                }
            }
            if !mapped.is_empty() && !out.send_batch(&mut mapped, &mut staged) {
                return;
            }
        } else {
            for item in scratch.drain(..) {
                if cancel_active(cancel) {
                    return;
                }
                buf.clear();
                f(item, &mut buf);
                for expanded in buf.drain(..) {
                    if !out.send(expanded) {
                        return;
                    }
                }
            }
        }
    }
}

/// The single sink worker: fair fan-in claim, side-effect consume, no output.
fn sink_worker<I, F>(
    f: &F,
    mut fanin: FanIn<I>,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
) where
    I: Send + 'static,
    F: Fn(I) + Send + Sync,
{
    let mut spin = RoundSpin::new(crate::handoff::spin_anchor_us());
    let mut scratch: Vec<I> = Vec::with_capacity(claim_cap.max(1));
    loop {
        let claimed = fanin.claim(&mut scratch, claim_cap, &mut spin);
        if claimed == 0 {
            return;
        }
        for item in scratch.drain(..) {
            if cancel_active(cancel) {
                return;
            }
            f(item);
        }
    }
}

// ── Spawn steps (called from the builder's registration methods) ──

/// Downcast one endpoint bundle to the concrete channel type the node
/// expects. Failing is an internal bug — the connector was created where the
/// edge's item type was pinned to this node's.
pub(crate) fn downcast_endpoints<T: 'static>(
    endpoints: Vec<Erased>,
    side: &str,
    name: &str,
) -> Vec<T> {
    endpoints
        .into_iter()
        .map(|boxed| {
            *boxed.downcast::<T>().unwrap_or_else(|_| {
                panic!(
                    "graph: internal type mismatch on {side} of node `{name}` (edge connector and \
                     node disagree)"
                )
            })
        })
        .collect()
}

/// Type-erased spawn step for one iterator source (see
/// `GraphBuilder::source`): a single feeder job, no inputs.
pub(crate) fn source_spawn<T, I>(iter: I) -> SpawnStep
where
    T: Send + Clone + Unpin + 'static,
    I: IntoIterator<Item = T>,
    // The feeder job runs on a pool worker / dedicated thread; the iterator
    // crosses the thread boundary once.
    I::IntoIter: Send + 'static,
{
    // Convert up front so the Send bound is carried by the iterator itself.
    let iter = iter.into_iter();
    Box::new(
        move |core: &GraphCore, _workers: usize, _inputs: Vec<Erased>, outputs: Vec<Erased>| {
            let txs = downcast_endpoints::<SyncSender<T>>(outputs, "outputs", "source");
            let cancel = core.cancel.clone();
            core.dispatch_job(move || {
                source_feeder_worker(iter, Broadcast::new(txs), Some(&cancel));
            });
        },
    )
}

/// Type-erased spawn step for one sync node (see `GraphBuilder::node`).
pub(crate) fn node_spawn<I, O, F>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: Fn(I) -> O + Send + Sync + 'static,
{
    Box::new(
        move |core: &GraphCore, workers: usize, inputs: Vec<Erased>, outputs: Vec<Erased>| {
            let rxs = downcast_endpoints::<SyncReceiver<I>>(inputs, "inputs", &name);
            let txs = downcast_endpoints::<SyncSender<O>>(outputs, "outputs", &name);
            let claim_cap = GraphCore::claim_cap(workers);
            let f = Arc::new(f);
            core.dispatch_jobs((0..workers).map(|_| {
                let f = Arc::clone(&f);
                let fanin = FanIn::new(rxs.clone());
                let out = Broadcast::new(txs.clone());
                let cancel = core.cancel.clone();
                move || node_worker(&*f, fanin, out, Some(&cancel), claim_cap)
            }));
        },
    )
}

/// Type-erased spawn step for one expansion node (see `GraphBuilder::node_expand`).
pub(crate) fn expand_node_spawn<I, O, F>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: Fn(I, &mut Vec<O>) + Send + Sync + 'static,
{
    Box::new(
        move |core: &GraphCore, workers: usize, inputs: Vec<Erased>, outputs: Vec<Erased>| {
            let rxs = downcast_endpoints::<SyncReceiver<I>>(inputs, "inputs", &name);
            let txs = downcast_endpoints::<SyncSender<O>>(outputs, "outputs", &name);
            let claim_cap = GraphCore::claim_cap(workers);
            let f = Arc::new(f);
            core.dispatch_jobs((0..workers).map(|_| {
                let f = Arc::clone(&f);
                let fanin = FanIn::new(rxs.clone());
                let out = Broadcast::new(txs.clone());
                let cancel = core.cancel.clone();
                move || expand_worker(&*f, fanin, out, Some(&cancel), claim_cap)
            }));
        },
    )
}

/// Type-erased spawn step for one sink (see `GraphBuilder::sink`): exactly
/// one worker, no outputs.
pub(crate) fn sink_spawn<I, F>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + 'static,
    F: Fn(I) + Send + Sync + 'static,
{
    Box::new(
        move |core: &GraphCore, _workers: usize, inputs: Vec<Erased>, _outputs: Vec<Erased>| {
            let rxs = downcast_endpoints::<SyncReceiver<I>>(inputs, "inputs", &name);
            let claim_cap = GraphCore::claim_cap(1);
            let f = Arc::new(f);
            let fanin = FanIn::new(rxs);
            let cancel = core.cancel.clone();
            core.dispatch_job(move || sink_worker(&*f, fanin, Some(&cancel), claim_cap));
        },
    )
}

// ── Budget resolution and the spawn walk ──

/// One out-edge bundle consumed by the walk: target, resolved buffer and the
/// type-erased connector.
struct OutEdge {
    down: usize,
    buffer: usize,
    connector: EdgeConnector,
}

/// Resolve every node's worker count (async nodes: task fan-out) and the
/// dispatch mode (pool jobs under a parking lease vs dedicated threads).
///
/// Liveness invariant (pool mode, same as the stream engine's `resolve_admission`):
/// every blocking job the run parks on a pool thread must be simultaneously
/// schedulable — `source feeders + Σ sync node workers ≤ pool threads` —
/// reserved atomically through [`ParkingLease`]. Async nodes run on the
/// async runtime and consume no pool slot; bridge threads are always
/// dedicated. When the remaining lease capacity cannot host the whole bound,
/// or `run()` itself executes on a worker of the same pool (nested runs),
/// every sync worker / feeder falls back to dedicated OS threads.
fn resolve_budget(plan: &GraphPlan, config: &PipelineConfig, pool: &ComputePool) -> RunPlan {
    let pool_threads = pool.num_workers();
    // A pinned budget wins (clamped by `set_compute_workers`); unpinned
    // follows the pool so a big blocking-IO pool actually gets its workers.
    let workers_budget = if config.compute_workers_pinned {
        config.compute_workers
    } else {
        config.compute_workers.max(pool_threads)
    };

    let feeders = plan
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Source)
        .count();
    let sinks = plan
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Sink)
        .count();
    // Sync nodes whose worker count the budget divides (sinks are fixed at
    // one worker and pinned below as a constant term).
    let divided: Vec<&NodeOpts> = plan
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, NodeKind::Sync | NodeKind::Expand))
        .map(|node| &node.opts)
        .collect();
    let pinned_workers: usize = divided
        .iter()
        .filter_map(|opts| opts.workers)
        .map(std::num::NonZeroUsize::get)
        .sum();
    let unpinned_divided = divided.len() - divided.iter().filter(|o| o.workers.is_some()).count();

    let share_of = |budget: usize| -> usize {
        if unpinned_divided == 0 {
            return 1;
        }
        (budget.saturating_sub(pinned_workers) / unpinned_divided).max(1)
    };

    let (lease, dedicated, share) = if pool.is_on_this_pool() {
        (None, true, share_of(workers_budget))
    } else {
        let used = pool.registry().parking_used();
        // Sinks and feeders take their fixed slots off the pool before the
        // division (each is one channel-parking job for the whole run).
        let live = pool_threads.saturating_sub(used + feeders + sinks);
        let share = share_of(workers_budget.min(live));
        let need = feeders + sinks + pinned_workers + unpinned_divided * share;
        if need <= pool_threads && pool.registry().try_reserve_parking(need) {
            let lease = ParkingLease::new(Arc::clone(pool.registry()), need);
            (Some(lease), false, share)
        } else {
            (None, true, share_of(workers_budget))
        }
    };

    // Per-node worker/task counts. Async nodes resolve their task fan-out
    // here too (io_concurrency pin or the config default; async sources and
    // sinks are single-worker by construction).
    let workers: Vec<usize> = plan
        .nodes
        .iter()
        .map(|node| match node.kind {
            NodeKind::Source | NodeKind::SourceAsync | NodeKind::Sink => 1,
            NodeKind::Sync | NodeKind::Expand => {
                node.opts.workers.map_or(share, std::num::NonZeroUsize::get)
            },
            NodeKind::Async => node
                .opts
                .io_concurrency
                .map_or(config.io_concurrency, std::num::NonZeroUsize::get),
        })
        .collect();

    RunPlan {
        dedicated,
        lease,
        workers,
    }
}

struct RunPlan {
    dedicated: bool,
    lease: Option<ParkingLease>,
    /// Per-node worker count (async nodes: concurrent task count).
    workers: Vec<usize>,
}

/// Execute one graph to whole-graph EOF.
///
/// Topology errors are programming errors and panic with the offending node
/// names (the contract shared with every other youpipe terminal); the only
/// recoverable failure is async-runtime construction, returned as `Err`.
pub(crate) fn run_graph<R: AsyncRuntime>(builder: GraphBuilder<R>) -> std::io::Result<()> {
    let order = match builder.plan.validate_and_order() {
        Ok(order) => order,
        Err(errors) => panic!("graph validation failed: {}", errors.join("; ")),
    };
    let GraphBuilder {
        plan,
        config,
        cancel,
        compute_pool,
        async_pool,
    } = builder;
    if plan.nodes.is_empty() {
        return Ok(());
    }

    let pool = compute_pool.unwrap_or_else(|| ComputePool::global().clone());
    let run = resolve_budget(&plan, &config, &pool);

    // Build the async backend before anything spawns (an async producer
    // needs it at spawn time), so a construction failure surfaces as `Err`
    // instead of a half-spawned pipeline.
    let spawner: Option<Arc<dyn AsyncSpawner>> = if plan.nodes.iter().any(|n| n.kind.is_async()) {
        let backend = match async_pool {
            Some(attached) => attached,
            None => R::build_default(config.async_workers)?,
        };
        Some(Arc::new(backend))
    } else {
        None
    };

    let total_workers: usize = run.workers.iter().sum();
    let core = GraphCore {
        pool,
        dedicated: run.dedicated,
        lease: run.lease,
        cancel: cancel.unwrap_or_default(),
        panic: Arc::new(Mutex::new(None)),
        done: DoneCounter::new(total_workers),
        spawner,
    };

    // Snapshot the per-node buffer pins before the nodes are consumed by the
    // reorder below, then bundle every edge under its upstream node with the
    // edge's resolved capacity: the upstream node's explicit pin verbatim,
    // or the configured buffer floored at the upstream producer count x 4
    // (the same floor the stream engine applies to stage outputs —
    // producing workers always keep a few free slots ahead).
    let n = plan.nodes.len();
    let buffer_pins: Vec<_> = plan.nodes.iter().map(|node| node.opts.buffer).collect();
    let mut out_edges: Vec<Vec<OutEdge>> = (0..n).map(|_| Vec::new()).collect();
    for edge in plan.edges {
        let producers = run.workers[edge.up];
        let buffer = buffer_pins[edge.up].map_or_else(
            || floored_channel_capacity(config.buffer_size, producers),
            std::num::NonZeroUsize::get,
        );
        out_edges[edge.up].push(OutEdge {
            down: edge.down,
            buffer,
            connector: edge.connector,
        });
    }

    // Reorder the nodes into the topological order (spawn steps are FnOnce
    // boxes and must be consumed in walk order).
    let mut pos = vec![0usize; n];
    for (idx, &u) in order.iter().enumerate() {
        pos[u] = idx;
    }
    let mut slots: Vec<Option<_>> = (0..n).map(|_| None).collect();
    for (i, node) in plan.nodes.into_iter().enumerate() {
        slots[pos[i]] = Some((i, node));
    }
    let ordered: Vec<_> = slots
        .into_iter()
        .map(|slot| slot.expect("topo order covers every node"))
        .collect();

    // The walk: for each node in topological order, create its out-edge
    // channels (spawning the async-to-sync bridges), then hand the node its
    // input endpoints (its in-edges' upstreams have all walked already) and
    // output senders. Kahn order spawns every source feeder before any
    // downstream worker job.
    let mut pending: Vec<Vec<Erased>> = (0..n).map(|_| Vec::new()).collect();
    for (u, node) in ordered {
        let mut outputs: Vec<Erased> = Vec::with_capacity(out_edges[u].len());
        for edge in std::mem::take(&mut out_edges[u]) {
            let (tx, rx) = (edge.connector)(&core, edge.buffer);
            outputs.push(tx);
            pending[edge.down].push(rx);
        }
        let inputs = std::mem::take(&mut pending[u]);
        (node.spawn)(&core, run.workers[u], inputs, outputs);
    }

    // `done` is decremented only after each task's `CatchPanic` wrapper
    // resolves (see `spawn_task`), so this wait is strictly ordered after
    // any `record_panic` — the graph drain is completion-gated, never
    // channel-EOF-gated, and is immune to the stream engine's
    // record-vs-unwind race (todo #23, `TaskSettleGate` there).
    core.done.wait();
    resume_stored_panic(&core.panic);
    Ok(())
}
