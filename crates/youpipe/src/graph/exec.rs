//! Run-time assembly for the typed DAG engine: the shared run context
//! (`GraphCore`), worker-budget resolution, parking-lease admission,
//! edge-channel creation, the node spawn walk and completion waiting.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use super::{
    GraphBuilder,
    fanin::FanIn,
    plan::{ClaimScope, EdgeConnector, Erased, GraphPlan, NodeKind, NodeOpts, SpawnStep},
};
use crate::{
    builder::{
        PanicSlot, ParkingLease, PipelineConfig, admit, floored_channel_capacity, record_panic,
        resume_stored_panic,
    },
    executor::compute::ComputePool,
    handoff::{
        CancelCheck, ClaimWindow, Delivery, Receiver as SyncReceiver, SpinBudget, SyncSender,
        cancel_active, claim_expand_deliver, claim_map_deliver, mux::MuxPlan, per_worker_claim_cap,
    },
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
    /// The backend's worker-thread count ([`AsyncRuntime::parallelism`]).
    fn parallelism(&self) -> usize;
}

impl<R: AsyncRuntime> AsyncSpawner for R {
    fn parallelism(&self) -> usize {
        AsyncRuntime::parallelism(self)
    }

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        AsyncRuntime::spawn(self, fut);
    }

    fn block_on(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        AsyncRuntime::block_on(self, fut);
    }
}

/// Worker-completion counter: pre-set to the run's total worker count
/// (source feeders + sync node workers + async node drivers + async sources),
/// decremented by every dispatched job / spawned task on completion —
/// normal or caught-panic — and awaited by the run's main thread. No
/// `JoinHandle` exists to join (pool jobs cannot be joined), so this is the
/// completion signal; the main thread is never a pool worker in pool mode
/// (the nested-run case takes the dedicated-thread fallback), so parking on
/// the condvar is safe.
///
/// The pair comes from `youpipe_sys` rather than `std` so production runs on
/// `parking_lot` (no poisoning to recover from, fairer wake order) and
/// `--cfg loom` swaps in the model-checked pair the `loom_tests` gate drives.
/// The `std::sync::Mutex` imported above stays: it backs the collect-sink
/// buffer and the panic slot, which hold run data across the graph walk and
/// are not modeled here.
#[derive(Clone)]
pub(crate) struct DoneCounter(Arc<(youpipe_sys::Mutex<usize>, youpipe_sys::Condvar)>);

impl DoneCounter {
    pub(crate) fn new(total: usize) -> Self {
        Self(Arc::new((
            youpipe_sys::Mutex::new(total),
            youpipe_sys::Condvar::new(),
        )))
    }

    /// Account one finished job. The count is only ever decremented, so a
    /// second decrement for the same job cannot happen by construction;
    /// `saturating_sub` only keeps such a caller bug from wrapping the count
    /// to `usize::MAX` and parking every waiter forever.
    fn dec(&self) {
        let (lock, cv) = &*self.0;
        let mut guard = lock.lock();
        *guard = guard.saturating_sub(1);
        // One broadcast, at the moment the count reaches zero; there is no
        // other wake source, so a waiter can only observe count 0 after it.
        if *guard == 0 {
            cv.notify_all();
        }
    }

    /// Block until every job finished. The `while` re-checks the count (a
    /// broadcast proves the count reached zero, not that the waiter's own
    /// condition is settled).
    fn wait(&self) {
        let (lock, cv) = &*self.0;
        let mut guard = lock.lock();
        while *guard > 0 {
            cv.wait(&mut guard);
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
    /// The spawner's worker-thread count (1 without a spawner): sizes the
    /// async nodes' driver fan-out.
    pub(crate) async_parallelism: usize,
}

impl GraphCore {
    /// Driver fan-out of an async node admitting `concurrency` in-flight
    /// items. `run_graph` sizes the completion counter with the same plan.
    pub(crate) fn mux_plan(&self, concurrency: usize) -> MuxPlan {
        MuxPlan::new(concurrency, self.async_parallelism)
    }

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
}

// ── Worker bodies ──

/// Source feeder: pull the iterator and deliver each item to every open
/// edge through the node's [`Delivery`] strategy. Single worker per source.
///
/// With the tx-batch knob on, items are staged in `cap`-sized batches and
/// each edge takes one `try_send_batch` per free run (`send_batch`); the
/// per-item path below is the knob-off baseline. Cancel acceptance is per
/// staged batch in the batched arm (the check precedes the send), per item
/// in the baseline; delivery itself never consults the token.
fn source_feeder_worker<T, I, D>(iter: I, mut out: D, cancel: Option<&CancellationToken>)
where
    T: Send + 'static,
    I: Iterator<Item = T> + Send + 'static,
    D: Delivery<T>,
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

/// Resolve a sync/expand node's claim rhythm from its worker count and
/// claim scope (todo #43, the graph port of the stream engine's
/// `ClaimKnobs::resolve`): the per-worker fairness share plus the scratch
/// capacity that bounds it. An async-fed node with the window knob on
/// grows the scratch to the full batch cap so [`ClaimWindow`] can scale
/// claims up to it; every other combination keeps `scratch == share`, which
/// leaves the window inert — the fixed-share rhythm bit-for-bit.
fn claim_knobs(workers: usize, scope: ClaimScope) -> (usize, usize) {
    let share = per_worker_claim_cap(workers);
    let scratch_cap =
        if scope == ClaimScope::AsyncFed && crate::handoff::claim_scale_enabled() && share > 0 {
            crate::handoff::batch_recv_cap()
        } else {
            share
        };
    (share, scratch_cap)
}

/// One sync node worker: fair fan-in claim, map, deliver through the node's
/// [`Delivery`] strategy — the shared `claim_map_deliver` driver with
/// `PerClaim` cancel acceptance (the token check precedes the batch's
/// mapping, so up to `claim_cap` items can be accepted after the fire;
/// either way an accepted item is always delivered, so the emitted set is
/// independent of ring pressure — todo #21). With the tx-batch knob on the
/// mapped claim batch is staged and flushed per edge with one
/// `try_send_batch` per free run (zero-cap claims keep the per-item send:
/// staging a 1-item batch only adds moves). The claim bound is the
/// backlog-scaled [`ClaimWindow`] (share == cap leaves it inert — see
/// [`claim_knobs`]). Exits on EOF (every input closed), cancel, or every
/// output edge closed.
fn node_worker<I, O, F, D>(
    f: &F,
    mut fanin: FanIn<I>,
    mut out: D,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
    scratch_cap: usize,
) where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> O + Send + Sync,
    D: Delivery<O>,
{
    let tx_batch = super::fanin::graph_tx_batch() && scratch_cap > 0;
    let mut spin = SpinBudget::new(crate::handoff::spin_anchor_us());
    let mut win = ClaimWindow::new(claim_cap, scratch_cap);
    claim_map_deliver(
        &mut |scratch| {
            let n = fanin.claim(scratch, win.current(), &mut spin);
            win.note(n);
            n
        },
        f,
        &mut out,
        cancel,
        tx_batch,
        scratch_cap,
        CancelCheck::PerClaim,
    );
}

/// One expansion node worker: like [`node_worker`] but each input expands
/// into zero or more outputs through the shared `claim_expand_deliver`
/// driver (push-style expand into a reused per-worker scratch `Vec`, zero
/// steady-state allocation; the batched arm flushes the staged outputs at
/// the batch cap so an expansion's fan-out cannot grow the staging buffer
/// with the whole claim batch's expansion).
fn expand_worker<I, O, F, D>(
    f: &F,
    mut fanin: FanIn<I>,
    mut out: D,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
    scratch_cap: usize,
) where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I, &mut Vec<O>) + Send + Sync,
    D: Delivery<O>,
{
    let tx_batch = super::fanin::graph_tx_batch() && scratch_cap > 0;
    let flush_cap = crate::handoff::batch_recv_cap().max(scratch_cap).max(1);
    let mut spin = SpinBudget::new(crate::handoff::spin_anchor_us());
    let mut win = ClaimWindow::new(claim_cap, scratch_cap);
    claim_expand_deliver(
        &mut |scratch| {
            let n = fanin.claim(scratch, win.current(), &mut spin);
            win.note(n);
            n
        },
        f,
        &mut out,
        cancel,
        tx_batch,
        scratch_cap,
        flush_cap,
        CancelCheck::PerClaim,
    );
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
    let mut spin = SpinBudget::new(crate::handoff::spin_anchor_us());
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

/// The collect-sink worker (see `GraphBuilder::sink_collect`): fair fan-in
/// claim, push the claimed items into the shared output Vec — one lock per
/// claim batch. The terminal reads the Vec only after the run drained, so
/// the lock is uncontended; taking it under the same mutex also orders the
/// reads after the worker's last push.
fn collect_worker<T>(
    buf: &Arc<Mutex<Vec<T>>>,
    mut fanin: FanIn<T>,
    cancel: Option<&CancellationToken>,
    claim_cap: usize,
) where
    T: Send + 'static,
{
    let mut spin = SpinBudget::new(crate::handoff::spin_anchor_us());
    let mut scratch: Vec<T> = Vec::with_capacity(claim_cap.max(1));
    loop {
        let claimed = fanin.claim(&mut scratch, claim_cap, &mut spin);
        if claimed == 0 {
            return;
        }
        let mut out = buf
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for item in scratch.drain(..) {
            if cancel_active(cancel) {
                return;
            }
            out.push(item);
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
/// `GraphBuilder::source`): a single feeder job, no inputs, delivering
/// through `D`.
pub(crate) fn source_spawn<T, I, D>(iter: I) -> SpawnStep
where
    T: Send + Unpin + 'static,
    I: IntoIterator<Item = T>,
    // The feeder job runs on a pool worker / dedicated thread; the iterator
    // crosses the thread boundary once.
    I::IntoIter: Send + 'static,
    D: Delivery<T> + From<Vec<SyncSender<T>>>,
{
    // Convert up front so the Send bound is carried by the iterator itself.
    let iter = iter.into_iter();
    Box::new(
        move |core: &GraphCore,
              _workers: usize,
              _scope: ClaimScope,
              _inputs: Vec<Erased>,
              outputs: Vec<Erased>| {
            let txs = downcast_endpoints::<SyncSender<T>>(outputs, "outputs", "source");
            let cancel = core.cancel.clone();
            core.dispatch_job(move || {
                source_feeder_worker(iter, D::from(txs), Some(&cancel));
            });
        },
    )
}

/// Type-erased spawn step for one sync node (see `GraphBuilder::node`),
/// delivering through `D`.
pub(crate) fn node_spawn<I, O, F, D>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> O + Send + Sync + 'static,
    D: Delivery<O> + From<Vec<SyncSender<O>>>,
{
    Box::new(
        move |core: &GraphCore,
              workers: usize,
              scope: ClaimScope,
              inputs: Vec<Erased>,
              outputs: Vec<Erased>| {
            let rxs: Arc<[SyncReceiver<I>]> = Arc::from(downcast_endpoints::<SyncReceiver<I>>(
                inputs, "inputs", &name,
            ));
            let txs = downcast_endpoints::<SyncSender<O>>(outputs, "outputs", &name);
            let (claim_cap, scratch_cap) = claim_knobs(workers, scope);
            let f = Arc::new(f);
            core.dispatch_jobs((0..workers).map(|_| {
                let f = Arc::clone(&f);
                let fanin = FanIn::new(Arc::clone(&rxs));
                let out = D::from(txs.clone());
                let cancel = core.cancel.clone();
                move || node_worker(&*f, fanin, out, Some(&cancel), claim_cap, scratch_cap)
            }));
        },
    )
}

/// Type-erased spawn step for one expansion node (see
/// `GraphBuilder::node_expand`), delivering through `D`.
pub(crate) fn expand_node_spawn<I, O, F, D>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I, &mut Vec<O>) + Send + Sync + 'static,
    D: Delivery<O> + From<Vec<SyncSender<O>>>,
{
    Box::new(
        move |core: &GraphCore,
              workers: usize,
              scope: ClaimScope,
              inputs: Vec<Erased>,
              outputs: Vec<Erased>| {
            let rxs: Arc<[SyncReceiver<I>]> = Arc::from(downcast_endpoints::<SyncReceiver<I>>(
                inputs, "inputs", &name,
            ));
            let txs = downcast_endpoints::<SyncSender<O>>(outputs, "outputs", &name);
            let (claim_cap, scratch_cap) = claim_knobs(workers, scope);
            let f = Arc::new(f);
            core.dispatch_jobs((0..workers).map(|_| {
                let f = Arc::clone(&f);
                let fanin = FanIn::new(Arc::clone(&rxs));
                let out = D::from(txs.clone());
                let cancel = core.cancel.clone();
                move || expand_worker(&*f, fanin, out, Some(&cancel), claim_cap, scratch_cap)
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
        move |core: &GraphCore,
              _workers: usize,
              _scope: ClaimScope,
              inputs: Vec<Erased>,
              _outputs: Vec<Erased>| {
            let rxs: Arc<[SyncReceiver<I>]> = Arc::from(downcast_endpoints::<SyncReceiver<I>>(
                inputs, "inputs", &name,
            ));
            let claim_cap = per_worker_claim_cap(1);
            let f = Arc::new(f);
            let fanin = FanIn::new(rxs);
            let cancel = core.cancel.clone();
            core.dispatch_job(move || sink_worker(&*f, fanin, Some(&cancel), claim_cap));
        },
    )
}

/// Type-erased spawn step for one collect sink (see
/// `GraphBuilder::sink_collect`): exactly one worker pushing into the shared
/// buffer the terminal extracts after the drain.
pub(crate) fn collect_spawn<T>(buf: Arc<Mutex<Vec<T>>>) -> SpawnStep
where
    T: Send + 'static,
{
    Box::new(
        move |core: &GraphCore,
              _workers: usize,
              _scope: ClaimScope,
              inputs: Vec<Erased>,
              _outputs: Vec<Erased>| {
            let rxs: Arc<[SyncReceiver<T>]> = Arc::from(downcast_endpoints::<SyncReceiver<T>>(
                inputs,
                "inputs",
                "collect sink",
            ));
            let claim_cap = per_worker_claim_cap(1);
            let cancel = core.cancel.clone();
            core.dispatch_job(move || {
                collect_worker(&buf, FanIn::new(rxs), Some(&cancel), claim_cap);
            });
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
/// The admission skeleton (liveness invariant, pool-scope lease, snapshot /
/// CAS discipline) lives once on the shared [`admit`]; graph specifics: the
/// source feeders and sinks are the fixed lease jobs, sync/expand nodes the
/// divided units. Async nodes run on the async runtime and consume no pool
/// slot; bridge threads are always dedicated.
fn resolve_budget(plan: &GraphPlan, config: &PipelineConfig, pool: &ComputePool) -> RunPlan {
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

    let d = admit(pool, config, feeders + sinks, |worker_budget| {
        let share = share_of(worker_budget);
        (share, pinned_workers + unpinned_divided * share)
    });

    // Per-node worker counts. Async nodes resolve their in-flight slot
    // count here (io_concurrency pin or the config default; the driver
    // fan-out over those slots is `GraphCore::mux_plan`); async sources and
    // sinks are single-worker by construction.
    let workers: Vec<usize> = plan
        .nodes
        .iter()
        .map(|node| match node.kind {
            NodeKind::Source | NodeKind::SourceAsync | NodeKind::Sink => 1,
            NodeKind::Sync | NodeKind::Expand => node
                .opts
                .workers
                .map_or(d.share, std::num::NonZeroUsize::get),
            NodeKind::Async => node
                .opts
                .io_concurrency
                .map_or(config.io_concurrency, std::num::NonZeroUsize::get),
        })
        .collect();

    RunPlan {
        dedicated: d.dedicated,
        lease: d.lease,
        workers,
    }
}

struct RunPlan {
    dedicated: bool,
    lease: Option<ParkingLease>,
    /// Per-node worker count (async nodes: in-flight slot count).
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
    // Collect-sink buffers not extracted by a `run_collect` terminal would
    // drop their items silently; a taken slot is `None` (see `take_output`).
    let uncollected: Vec<&str> = builder
        .outputs
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.is_some())
        .map(|(id, _)| &*builder.plan.nodes[id].name)
        .collect();
    if !uncollected.is_empty() {
        let names = uncollected
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ");
        panic!(
            "graph run: collect sink(s) {names} dropped their items; pass each handle to \
             run_collect (or a try_/_checked variant)"
        );
    }
    let GraphBuilder {
        plan,
        config,
        cancel,
        compute_pool,
        async_pool,
        ..
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

    let async_parallelism = spawner.as_ref().map_or(1, |s| s.parallelism());
    // Async nodes spawn their `MuxPlan` driver count — the plan
    // `GraphCore::mux_plan` hands their spawn steps.
    let total_workers: usize = plan
        .nodes
        .iter()
        .zip(&run.workers)
        .map(|(node, &workers)| match node.kind {
            NodeKind::Async => MuxPlan::new(workers, async_parallelism).drivers(),
            _ => workers,
        })
        .sum();
    let core = GraphCore {
        pool,
        dedicated: run.dedicated,
        lease: run.lease,
        cancel: cancel.unwrap_or_default(),
        panic: Arc::new(Mutex::new(None)),
        done: DoneCounter::new(total_workers),
        spawner,
        async_parallelism,
    };

    // Snapshot the per-node buffer pins before the nodes are consumed by the
    // reorder below, then bundle every edge under its upstream node with the
    // edge's resolved capacity: the upstream node's explicit pin verbatim,
    // or the configured buffer floored at the upstream producer count x 4
    // (the same floor the stream engine applies to stage outputs —
    // producing workers always keep a few free slots ahead).
    let n = plan.nodes.len();
    let buffer_pins: Vec<_> = plan.nodes.iter().map(|node| node.opts.buffer).collect();
    // Kind snapshot for the claim-scope resolution below (the node table is
    // consumed by the reorder before the walk).
    let kinds: Vec<NodeKind> = plan.nodes.iter().map(|node| node.kind).collect();
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
        // Claim scope: does any out-edge feed an async consumer (an async
        // node, or an async source — impossible, sources take no in-edges)?
        // Inert for sources/sinks/async nodes themselves (single-consumer
        // or claim-free sites).
        let scope = if out_edges[u].iter().any(|e| kinds[e.down].is_async()) {
            ClaimScope::AsyncFed
        } else {
            ClaimScope::SyncFed
        };
        let mut outputs: Vec<Erased> = Vec::with_capacity(out_edges[u].len());
        for edge in std::mem::take(&mut out_edges[u]) {
            let (tx, rx) = (edge.connector)(&core, edge.buffer);
            outputs.push(tx);
            pending[edge.down].push(rx);
        }
        let inputs = std::mem::take(&mut pending[u]);
        (node.spawn)(&core, run.workers[u], scope, inputs, outputs);
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

/// The run's completion gate: no waiter may return with a job outstanding, and
/// the single broadcast at zero must release every waiter.
///
/// Not modeled: `GraphCore::dispatch_jobs`'s caught-panic decrement path — loom
/// drives the real unwinding machinery, so a modeled panic aborts the
/// interleaving instead of exploring it. `graph::tests`'
/// `node_panic_propagates_to_run_caller` (plus the feeder and async variants)
/// cover that path on real threads.
///
/// Layout: three threads and `LOOM_MAX_PREEMPTIONS=2` per model — one model is
/// the decrementing thread with two waiters, the other is the waiting thread
/// with two decrementers. Counters are constructed inside the model:
/// `youpipe_sys`'s primitives may only be touched inside a loom execution.
///
/// Falsification runs (temporary edits): dropping the `notify_all` at zero
/// deadlocks both models; `notify_one` deadlocks the two-waiter model and
/// leaves the one-waiter model green; returning from `wait` without parking
/// fails both on the publication assertion; `saturating_sub` ->
/// `wrapping_sub` fails the over-decrement assertion. `wait`'s `while` -> `if`
/// stays green — loom's condvar has no spurious wake and the count is monotone,
/// so the re-check is unreachable in the model and only guards
/// `parking_lot`'s spurious wakes.
#[cfg(all(test, loom))]
mod loom_tests {
    use loom::{cell::UnsafeCell, sync::Arc};

    use super::*;

    /// Value the decrementing thread publishes before it decrements.
    const PUBLISHED: u8 = 0x2a;

    /// `wait` must park while any job is outstanding, and the broadcast at
    /// zero must release *both* waiters: `notify_one` leaves the second parked
    /// and a missing notify leaves both parked, so loom reports the deadlock
    /// either way and the single broadcast cannot be traded for per-waiter
    /// signalling.
    ///
    /// The publication read is what makes an early return falsifiable: both
    /// waiters read the cell the decrementing thread wrote before its first
    /// `dec`, so a `wait` returning with the count still positive reads a stale
    /// value or races the write (loom's causality tracker panics).
    ///
    /// Cost: 873 executions, 0.12 s.
    #[test]
    fn broadcast_after_the_last_decrement_returns_every_waiter() {
        loom::model(|| {
            let done = DoneCounter::new(2);
            let published = Arc::new(UnsafeCell::new(0u8));

            let mut waiters = Vec::with_capacity(2);
            for _ in 0..2 {
                let waiter_done = done.clone();
                let waiter_published = Arc::clone(&published);
                waiters.push(loom::thread::spawn(move || {
                    waiter_done.wait();
                    // SAFETY: `wait` returned, so the count reached zero after
                    // the decrementer's write, and the lock hand-off publishes
                    // it.
                    let seen = waiter_published.with(|ptr| unsafe { *ptr });
                    assert_eq!(seen, PUBLISHED, "wait returned before the last decrement");
                }));
            }

            let observed = done.clone();
            let decrementer_published = Arc::clone(&published);
            let decrementer = loom::thread::spawn(move || {
                // SAFETY: single writer; both reads are licensed by `wait`.
                decrementer_published.with_mut(|ptr| unsafe { *ptr = PUBLISHED });
                done.dec();
                done.dec();
            });

            decrementer.join().unwrap();
            for waiter in waiters {
                waiter.join().unwrap();
            }
            assert_eq!(count(&observed), 0, "every job must be accounted for");
            // A stray second decrement (a caller bug no protocol can reach:
            // `dispatch_jobs` wraps exactly one `dec` per job) must not wrap the
            // count and re-arm the gate.
            observed.dec();
            assert_eq!(
                count(&observed),
                0,
                "over-decrement must not wrap the count"
            );
        });
    }

    /// The other side of no-early-return: the run's main thread is itself a
    /// waiter. Whatever order the two decrements land in, the return is ordered
    /// after the lock hand-off of the decrement that crossed zero, so the
    /// marker written before the *first* decrement is already visible.
    ///
    /// Cost: 115 executions, 0.01 s.
    #[test]
    fn wait_returns_only_after_the_last_decrement() {
        loom::model(|| {
            let done = DoneCounter::new(2);
            let published = Arc::new(UnsafeCell::new(0u8));

            let writer_published = Arc::clone(&published);
            let writer_done = done.clone();
            let writer = loom::thread::spawn(move || {
                // SAFETY: single writer; the read below is licensed by `wait`.
                writer_published.with_mut(|ptr| unsafe { *ptr = PUBLISHED });
                writer_done.dec();
            });
            let other_done = done.clone();
            let other = loom::thread::spawn(move || other_done.dec());

            done.wait();
            // SAFETY: `wait` returned, so both decrements landed; the last one's
            // release of the lock orders this read after the write.
            let seen = published.with(|ptr| unsafe { *ptr });
            assert_eq!(seen, PUBLISHED, "wait returned with a job outstanding");

            writer.join().unwrap();
            other.join().unwrap();
            assert_eq!(count(&done), 0);
        });
    }

    /// The counter's current value, read under its own lock.
    fn count(done: &DoneCounter) -> usize {
        *done.0.0.lock()
    }
}
