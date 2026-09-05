#[cfg(feature = "tokio-runtime")]
use std::{future::Future, sync::OnceLock};
use std::{cell::Cell, marker::PhantomData, num::NonZeroUsize, sync::Arc};

#[cfg(feature = "tokio-runtime")]
use crate::handoff::{
    AsyncReceiver, AsyncRecvItem, MpscAsyncReceiver, async_channel, mpsc_async_channel,
    sync_async_channel,
};
use crate::{
    builder::config::PipelineConfig,
    executor::compute::ComputePool,
    handoff::{
        MpscReceiver, Receiver, RecvItem, SendItem, SyncSender, TryRecvError, channel::channel,
        mpsc_channel,
    },
    runtime::{AsyncRuntime, DefaultRuntime},
    state::{FenceBarrier, FenceMode, run_ordered_collect},
    sync::CancellationToken,
};

// ── Streaming pipeline (chainable, data-first) ──
//
// Each stage wraps the previous stage's chain (`prev`), so the typestate nests
// as the user adds stages: `SyncStage<FenceLink<SyncStage<StreamStart, F1>>>`
// for `.stage(f1).fence(mode).stage(f2)`. The newest stage sits at the
// OUTERMOST level and executes LAST; the recursion in `StageSpawn::spawn`
// recurses into `prev` first (spawning earlier stages), then spawns this
// stage's workers.
//
// Channel topology is assembled at `.run()` time by walking the typestate:
//
//   feeder → [stage 1 workers] → mid₁ → [stage 2 workers] → mid₂ → … →
// collector
//
// Stages may be sync (run on `ComputePool`), async (run on an `AsyncRuntime`
// backend via runtime tasks), or a fence (forward-fence thread between
// adjacent stages).

/// True iff `cancel` is set and the pipeline should stop feeding new work.
#[inline]
fn cancel_active(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(CancellationToken::is_cancelled)
}

/// Bridge an [`AsyncReceiver`] to a sync [`Receiver`] via a dedicated OS
/// thread that runs `block_on` and forwards items. Used when a sync stage
/// (sync / expand / fence) follows an async stage in the chain — the previous
/// stage's output arrives on an async channel but this stage's workers expect a
/// sync one.
#[cfg(feature = "tokio-runtime")]
fn bridge_async_to_sync<T: Send + Unpin + 'static, R: AsyncRuntime>(
    rx: AsyncReceiver<(u64, T)>,
    ctx: &StreamCtx<'_, R>,
) -> Receiver<(u64, T)> {
    let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
    let (s_tx, s_rx) = channel::<(u64, T)>(buffer);
    let cancel = ctx.cancel.clone();
    let pool = ctx.acquire_async().expect("failed to build async runtime");
    std::thread::spawn(move || {
        pool.block_on(async move {
            while let Ok(item) = rx.recv().await {
                if cancel_active(cancel.as_ref()) {
                    return;
                }
                if s_tx.send(item).is_err() {
                    return;
                }
            }
        });
    });
    s_rx
}

/// Caught panic payload from a pool-submitted feeder job, re-raised on the
/// calling thread by [`Feeder::finish`] (preserving the panic-propagation
/// semantics of the old feeder thread's `join`).
type FeederPanicSlot = Arc<std::sync::Mutex<Option<Box<dyn std::any::Any + Send>>>>;

/// Handle returned by [`feed_items`]: either an inline push (already done,
/// nothing to reap) or a detached feeder (pool job or dedicated thread)
/// whose panic payload is re-raised by [`Feeder::finish`].
enum Feeder {
    Pool(FeederPanicSlot),
    Thread(FeederPanicSlot),
    Inline,
}

impl Feeder {
    fn finish(self) {
        let slot = match self {
            Self::Pool(slot) | Self::Thread(slot) => slot,
            Self::Inline => return,
        };
        // Same poison-recovery pattern as the hybrid dispatcher's fail slot.
        if let Some(payload) = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            std::panic::resume_unwind(payload);
        }
    }
}

/// Push `items` into the feeder channel.
///
/// When all items fit in the channel buffer (`items.len() ≤ buffer`), push
/// inline from the calling thread — saving all feeder dispatch overhead.
/// Otherwise the push loop runs as **a job on the compute pool** instead of
/// a dedicated OS thread: pool workers are long-lived, so this saves the
/// ~30-80 µs `thread::spawn` + join per `run()` call while keeping the same
/// effective thread count (one feeder alongside the stage workers). In
/// dedicated-thread mode (`dedicated == true`, see [`StreamCtx`]) the pool
/// cannot be relied on at all, so the feeder gets a dedicated OS thread too.
///
/// # Deadlock safety
///
/// The inline path is safe because `items.len() ≤ buffer` guarantees the
/// sender never blocks on `Full`: even if every downstream worker is blocked
/// on the *output* channel, the calling thread finishes pushing, drops the
/// sender, and proceeds to collect — draining the output and unblocking
/// workers. The pool path is only taken when [`StreamPipe::try_exec`] has
/// reserved a slot for it in the pool's liveness budget (one pool thread on
/// top of every sync stage's workers), so the feeder job is always
/// schedulable. The dedicated-thread path is trivially safe: the OS
/// schedules the thread independently of the pool.
///
/// The pool-path job is **injected** (`submit_injected`), never `submit`ed:
/// it must sit in the injector FIFO *before* the stage-worker jobs that
/// [`StreamPipe::run`] submits after this returns. `submit` from a worker of
/// the same pool pushes onto the calling worker's local LIFO deque instead —
/// and a nested `stream(..).run()` inside a pool closure then deadlocks:
/// every worker parks on an empty channel recv while the feeder sits
/// unreachable behind the blocked caller's own deque (regression-tested in
/// `test_nested_stream_inside_pool_worker_no_deadlock`).
///
/// # Panic semantics
///
/// The detached paths wrap the push loop in `catch_unwind` (an uncaught panic
/// in a pool job would abort the process via the worker's `AbortIfPanic`) and
/// store the payload; [`Feeder::finish`] re-raises it on the caller after
/// the collect returns. The payload store happens before the sender drops
/// (closing the channel), and the close is what releases the collector, so
/// the caller always observes the payload.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn feed_items<I: Send + 'static>(
    pool: &ComputePool,
    items: Vec<I>,
    feeder_tx: SyncSender<(u64, I)>,
    cancel: Option<CancellationToken>,
    buffer: usize,
    dedicated: bool,
) -> Feeder {
    if items.len() <= buffer {
        for (seq, item) in items.into_iter().enumerate() {
            if cancel_active(cancel.as_ref()) {
                break;
            }
            if feeder_tx.send((seq as u64, item)).is_err() {
                break;
            }
        }
        return Feeder::Inline;
    }
    let slot: FeederPanicSlot = Arc::new(std::sync::Mutex::new(None));
    let job_slot = Arc::clone(&slot);
    let push_loop = move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(cancel.as_ref()) {
                    break;
                }
                if feeder_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        }));
        if let Err(payload) = result {
            *job_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(payload);
        }
    };
    if dedicated {
        std::thread::spawn(push_loop);
        Feeder::Thread(slot)
    } else {
        pool.submit_injected(push_loop);
        Feeder::Pool(slot)
    }
}

/// Spawn `parallelism` workers that pull from `rx`, apply `stage`, and
/// forward to `tx` — as pool jobs in pool mode, or as dedicated OS threads
/// when [`StreamCtx::dedicated_threads`] is set. Each worker loops until its
/// receiver disconnects or the supplied cancellation token (if any) is
/// signalled.
///
/// Items carry a `seq` tag so the collector can restore input order; sync
/// stages unwrap, apply `stage`, re-wrap. Tagging is always on (even in
/// unordered mode) because the cost is one `u64` per item — far below the
/// channel handoff itself — and unifying the channels avoids separate
/// ordered/unordered implementations per stage.
///
/// Termination is carried entirely by channel disconnect: the function drops
/// its own endpoint clones, and each worker drops its clones when its recv
/// loop ends — the downstream stage observes "no more items" when the last
/// upstream sender goes away. No join handle is needed (workers are pool
/// jobs or detached threads that always terminate on disconnect); a former
/// WaitGroup here was never awaited and only added per-stage atomics.
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// taking the endpoints by value ensures the caller cannot retain a clone that
// would keep the channel open after the workers have finished.
fn spawn_stage<I, O, Tx, R>(
    ctx: &StreamCtx<'_, R>,
    rx: Receiver<(u64, I)>,
    tx: Tx,
    parallelism: usize,
    stage: impl Fn(I) -> O + Send + Sync + 'static,
) where
    I: Send + Unpin + 'static,
    O: Send + Unpin + 'static,
    Tx: SendItem<(u64, O)>,
    R: AsyncRuntime,
{
    let stage = Arc::new(stage);
    // Collect all worker closures and submit as a single batch. This reduces
    // injector-queue notification overhead from N SeqCst fences + N JEC
    // increments (one per `submit`) down to 1 (one `submit_batch`), which
    // measurably helps the small-workload case where per-run fixed cost
    // dominates.
    let jobs: Vec<_> = (0..parallelism)
        .map(|_| {
            let stage = stage.clone();
            let rx = rx.clone();
            let tx = tx.clone();
            let worker_cancel = ctx.cancel.clone();
            move || {
                'outer: loop {
                    // Anchor: one blocking recv parks the worker when the
                    // channel is empty; Err means all senders are gone.
                    let Ok((seq, item)) = rx.recv() else { break };
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let output = stage(item);
                    if tx.send((seq, output)).is_err() {
                        break;
                    }
                    // Burst-drain: absorb already-queued items while the
                    // channel's cache lines are hot, without re-entering the
                    // blocking-recv preamble per item. With several workers
                    // contending on the same MPMC ring, the workers that lag
                    // behind park on the anchor while the burst winners drain
                    // the backlog — the contending population thins itself
                    // instead of every worker hammering the ring per item.
                    loop {
                        let (seq, item) = match rx.try_recv() {
                            Ok(v) => v,
                            Err(TryRecvError::Empty) => continue 'outer,
                            Err(TryRecvError::Closed) => break 'outer,
                        };
                        if cancel_active(worker_cancel.as_ref()) {
                            break 'outer;
                        }
                        let output = stage(item);
                        if tx.send((seq, output)).is_err() {
                            break 'outer;
                        }
                    }
                }
            }
        })
        .collect();
    ctx.spawn_stage_jobs(jobs);
    drop(rx);
    drop(tx);
}

/// Like [`spawn_stage`] but expands each input into 1..N outputs via `expand`.
/// Each expanded item inherits the parent's `seq` so the collector can group
/// expansions from the same input.
#[allow(clippy::needless_pass_by_value)] // runs inside a `pool.submit(move …)`
fn spawn_expand_stage<I, N, Tx, R>(
    ctx: &StreamCtx<'_, R>,
    rx: Receiver<(u64, I)>,
    tx: Tx,
    parallelism: usize,
    expand: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
) where
    I: Send + Unpin + 'static,
    N: Send + Unpin + 'static,
    Tx: SendItem<(u64, N)>,
    R: AsyncRuntime,
{
    let expand = Arc::new(expand);
    let jobs: Vec<_> = (0..parallelism)
        .map(|_| {
            let expand = expand.clone();
            let rx = rx.clone();
            let tx = tx.clone();
            let worker_cancel = ctx.cancel.clone();
            move || {
                'outer: loop {
                    // Anchor + burst-drain, same shape as `spawn_stage`.
                    let Ok((seq, item)) = rx.recv() else { break };
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    for n in expand(item) {
                        if tx.send((seq, n)).is_err() {
                            break;
                        }
                    }
                    loop {
                        let (seq, item) = match rx.try_recv() {
                            Ok(v) => v,
                            Err(TryRecvError::Empty) => continue 'outer,
                            Err(TryRecvError::Closed) => break 'outer,
                        };
                        if cancel_active(worker_cancel.as_ref()) {
                            break 'outer;
                        }
                        for n in expand(item) {
                            if tx.send((seq, n)).is_err() {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        })
        .collect();
    ctx.spawn_stage_jobs(jobs);
    drop(rx);
    drop(tx);
}

/// Fence forwarder: drains `mid_rx` into a [`FenceBarrier`] and releases
/// batches to `fenced_tx` according to `mode`.
///
/// In [`FenceMode::Barrier`] mode nothing is forwarded until `mid_rx`
/// disconnects (stage 1 fully done) — a hard barrier. In
/// [`FenceMode::Chunked`] mode batches flow as they accumulate, letting
/// stage 2 overlap stage 1.
///
/// `expected` is the input item count (`ctx.n`) — in Barrier mode the whole
/// stream is buffered, so the Vec is preallocated once instead of growing
/// 1→2→4→… (`log2` reallocs). Expand stages upstream may multiply the count;
/// a short preallocation is still a strict improvement, never a correctness
/// issue. Chunked mode ignores it: batch buffers are small and recycled via
/// `reuse`, preallocating `expected` would pin peak-sized memory per batch.
///
/// Draining `mid_rx` eagerly (rather than waiting on a separate barrier
/// first) is what keeps stage 1 from blocking on a full channel: this is the
/// fix for the previous wait-before-drain deadlock.
#[allow(clippy::needless_pass_by_value)] // runs inside a `thread::spawn(move …)`:
// owning `mid_rx` / `fenced_tx` by value lets them drop (and close the channel)
// when the forwarder returns, which is how the downstream stage detects "no
// more items" — taking them by reference would keep the channel open forever.
fn forward_fenced<M, Tx>(
    mid_rx: Receiver<(u64, M)>,
    fenced_tx: Tx,
    mode: FenceMode,
    expected: usize,
    cancel: Option<&CancellationToken>,
) where
    M: Send + Unpin + 'static,
    Tx: SendItem<(u64, M)>,
{
    let mut fence = match mode {
        FenceMode::Barrier => FenceBarrier::with_capacity(mode, expected),
        FenceMode::Chunked(_) => FenceBarrier::new(mode),
    };
    // Push one item through the fence, forwarding any released batch.
    // Returns false when the downstream channel is closed.
    fn fwd<M2, Tx2>(
        fence: &mut FenceBarrier<(u64, M2)>,
        fenced_tx: &Tx2,
        item: (u64, M2),
    ) -> bool
    where
        M2: Send + Unpin + 'static,
        Tx2: SendItem<(u64, M2)>,
    {
        if let Some(mut batch) = fence.push(item) {
            // Drain in place so the allocation survives and can be recycled
            // by the barrier — steady state is zero allocator traffic per
            // batch (see `FenceBarrier::reuse`).
            for it in batch.drain(..) {
                if fenced_tx.send(it).is_err() {
                    return false;
                }
            }
            fence.reuse(batch);
        }
        true
    }
    'outer: loop {
        // Anchor + burst-drain (same shape as the stage workers): the
        // forwarder is the sole consumer, but draining the mid channel while
        // its cache lines are hot also releases upstream backpressure sooner.
        let Ok(item) = mid_rx.recv() else { break };
        if cancel_active(cancel) {
            return;
        }
        if !fwd(&mut fence, &fenced_tx, item) {
            return;
        }
        loop {
            let item = match mid_rx.try_recv() {
                Ok(v) => v,
                Err(TryRecvError::Empty) => continue 'outer,
                Err(TryRecvError::Closed) => break 'outer,
            };
            if cancel_active(cancel) {
                return;
            }
            if !fwd(&mut fence, &fenced_tx, item) {
                return;
            }
        }
    }
    // Normal drain (mid_rx closed): flush remaining buffered items. This path
    // is only reached on completion — the cancel path returns early above
    // without flushing, dropping in-progress items as expected on abort.
    if let Some(remaining) = fence.flush() {
        for it in remaining {
            if fenced_tx.send(it).is_err() {
                return;
            }
        }
    }
}

// ── StreamPipe (data-first chainable streaming pipeline) ──

/// Per-stage tuning overrides, chainable like the pipeline itself.
///
/// Every field is optional: unset fields fall back to the pipeline-level
/// [`PipelineConfig`] value (or the runner's equal division of
/// `compute_workers` across sync stages, for `workers`). Attach to a stage
/// via [`StreamPipe::stage_with`] / [`StreamPipe::stage_async_with`] /
/// [`StreamPipe::expand_with`].
///
/// ```rust
/// use youpipe::prelude::*;
///
/// // Heavy CPU stage gets 8 workers, light one divides the rest; the async
/// // stage runs 512 concurrent IO tasks with a deep buffer.
/// let r: Vec<u64> = (0..1000)
///     .stream()
///     .stage_with(StageOptions::new().workers(8), |x: u64| crunch(x))
///     .stage(|x: u64| x + 1)
///     .stage_async_with(
///         StageOptions::new().io_concurrency(512).buffer(1024),
///         |x: u64| async move { fetch(x).await },
///     )
///     .run();
/// # fn crunch(x: u64) -> u64 { x }
/// # async fn fetch(x: u64) -> u64 { x }
/// ```
///
    /// # Worker budget semantics (`workers`)
    ///
    /// The runner reserves one pool slot for the feeder (a pool job whenever
    /// `n > buffer`), then treats the rest as the liveness budget: explicit
    /// `workers` pins are granted first — upstream stage first — clamped to
    /// what remains with one slot held back per not-yet-spawned sync stage,
    /// and the remainder is divided equally across unpinned stages. Every
    /// sync stage keeps ≥ 1 resident worker and the total blocking pool jobs
    /// never exceed the pool — the "stage 1 fills the pool, stage 2 starves,
    /// deadlock" failure mode. Pins therefore take effect in pipeline order
    /// only until the budget runs out; later stages get 1 worker each.
    ///
    /// When even 1 worker per sync stage does not fit the pool (or `run()`
    /// executes on a worker of the same pool), the runner leaves the pool
    /// alone and spawns dedicated OS threads instead — the request is then
    /// honored as-is, since threads are not pool-bounded.
#[derive(Debug, Clone, Copy, Default)]
pub struct StageOptions {
    pub(crate) workers: Option<NonZeroUsize>,
    pub(crate) io_concurrency: Option<NonZeroUsize>,
    pub(crate) buffer: Option<NonZeroUsize>,
}

impl StageOptions {
    /// A fresh options set — everything unset, everything falls back to the
    /// pipeline-level [`PipelineConfig`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin this (sync or expand) stage's compute-pool worker count,
    /// bypassing the equal division of `compute_workers` across stages.
    ///
    /// Use when stage costs are known to be unequal: a heavy parse stage
    /// deserves more workers than a cheap transform. Clamped per-run to the
    /// item count (`workers = min(workers, n)`) like the default division,
    /// and to the liveness budget remaining after upstream stages' grants —
    /// see the [Worker budget semantics](#worker-budget-semantics-workers)
    /// section above.
    #[must_use]
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = NonZeroUsize::new(n);
        self
    }

    /// Pin this async stage's concurrent-task fan-out, bypassing the global
    /// `io_concurrency`. The right knob when one async stage talks to a
    /// high-latency network (wants 512 in flight) while another hits a local
    /// disk (wants 16).
    #[must_use]
    pub fn io_concurrency(mut self, n: usize) -> Self {
        self.io_concurrency = NonZeroUsize::new(n);
        self
    }

    /// Pin this stage's **output** channel capacity, bypassing
    /// `buffer_size` (and its `downstream_workers * 4` floor). Small buffers
    /// tighten backpressure (less peak memory); large ones absorb bursts.
    /// Correctness is unaffected — only throughput/memory trade off.
    #[must_use]
    pub fn buffer(mut self, n: usize) -> Self {
        self.buffer = NonZeroUsize::new(n);
        self
    }
}

/// Per-run worker-budget summary of a stage chain, reported by
/// [`StageSpawn::stage_budget`] and consumed by `StreamPipe::run` to divide
/// the pool across sync stages. Internal to the streaming engine (not
/// re-exported at the crate root).
#[derive(Debug, Clone, Copy, Default)]
pub struct StageBudget {
    /// Number of stages that consume compute-pool worker slots.
    pub stages: usize,
    /// Number of those stages that pinned their worker count via
    /// `StageOptions::workers`.
    pub explicit_stages: usize,
    /// Sum of the pinned worker counts.
    pub explicit_workers: usize,
}

impl StageBudget {
    /// The per-stage worker count for stages that did not pin one: subtract
    /// the explicit claims from the pool, divide the rest equally (min 1).
    pub(crate) fn default_workers(&self, pool_workers: usize) -> usize {
        let unspecified = self.stages.saturating_sub(self.explicit_stages);
        if unspecified == 0 {
            return 1;
        }
        let remaining = pool_workers.saturating_sub(self.explicit_workers);
        (remaining / unspecified).max(1)
    }
}

/// Streaming pipeline for workloads that cannot be fused at compile time
/// (multi-stage channels, fences, async stages, ordered output, cancellation).
///
/// Build via [`stream`]:
///
/// ```rust
/// # use youpipe::stream;
/// let result = stream(0..100)
///     .stage(|x: i32| x + 1)
///     .stage(|x: i32| x * 2)
///     .ordered()
///     .run();
/// ```
pub struct StreamPipe<S = StreamStart, I = (), O = (), R: AsyncRuntime = DefaultRuntime> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    cancel: Option<CancellationToken>,
    /// Custom compute pool. When `None`, stages use [`ComputePool::global`]
    /// (sized to `num_cpus`). When `Some`, stages run on the user-supplied
    /// pool — useful for oversubscribing threads for blocking-IO sync stages
    /// (e.g. `ComputePool::new(512)` to match tokio's `spawn_blocking` pool).
    compute_pool: Option<ComputePool>,
    #[cfg(feature = "tokio-runtime")]
    async_pool: Option<R>,
    ordered: bool,
    _marker: PhantomData<(O, R)>,
}

/// Typestate marker for the start of a streaming chain (no stages yet).
pub struct StreamStart;

/// Data-first entry point for a streaming pipeline. Stages chained via
/// `.stage()` / `.stage_async()` are connected by channels at `.run()` time.
///
/// Unlike the fused [`crate::Pipe`], a `StreamPipe` always materialises each
/// stage's output through a channel — useful for backpressure-aware flows,
/// async IO stages, fences between stages, or cooperative cancellation.
pub fn stream<I, It>(items: It) -> StreamPipe<StreamStart, I, I, DefaultRuntime>
where
    It: IntoIterator<Item = I>,
    I: Send + Unpin + 'static,
{
    StreamPipe {
        items: items.into_iter().collect(),
        stages: StreamStart,
        config: PipelineConfig::default(),
        cancel: None,
        compute_pool: None,
        #[cfg(feature = "tokio-runtime")]
        async_pool: None,
        ordered: false,
        _marker: PhantomData,
    }
}

// ── Stage markers (typestate chain) ──

/// Synchronous stage: `Fn(O) -> N`, runs on the [`ComputePool`].
#[derive(Clone)]
pub struct SyncStage<Prev, F> {
    pub(super) prev: Prev,
    pub(super) f: F,
    pub(super) opts: StageOptions,
}

/// 1-to-N expansion stage: `Fn(O) -> Vec<N>`. Each input item produces zero or
/// more outputs; expanded items inherit the parent's `seq` for ordered
/// collection.
#[derive(Clone)]
pub struct ExpandStage<Prev, F> {
    pub(super) prev: Prev,
    pub(super) f: F,
    pub(super) opts: StageOptions,
}

/// Async stage: `Fn(O) -> Future<Output = N>`, runs as `io_concurrency` tasks
/// on the [`AsyncRuntime`](crate::AsyncRuntime) backend. Gated behind the
/// `tokio-runtime` feature.
#[cfg(feature = "tokio-runtime")]
#[derive(Clone)]
pub struct AsyncStage<Prev, F> {
    pub(super) prev: Prev,
    pub(super) f: F,
    pub(super) opts: StageOptions,
}

/// Fence link: inserts a [`FenceBarrier`] between two stages. The type is
/// unchanged (it's a passthrough at the item level), but the runtime topology
/// gains a forwarder thread that batches / barriers per `mode`.
#[derive(Clone)]
pub struct FenceLink<Prev> {
    pub(super) prev: Prev,
    pub(super) mode: FenceMode,
}

/// Marker trait for a streaming stage chain that knows how to spawn itself
/// given an input receiver. The recursion walks the typestate inside-out,
/// matching the data-flow direction: the outermost stage (newest closure)
/// recurses into `prev` (older stages) first, then spawns its own workers on
/// the returned mid-channel.
///
/// The final receiver is wrapped in [`FinalRx`] so the collector knows whether
/// to drain synchronously or via the async runtime.
///
/// Returns the number of stages in this chain that consume compute-pool
/// worker slots (sync stages + expand stages). Fence links and async stages
/// don't count — fences run on a dedicated thread, async stages run on the
/// async runtime. The runner uses this (via [`StageBudget`]) to divide the
/// pool budget across stages so the total blocking jobs across all sync
/// stages never exceeds the pool size, preventing the "stage 1 holds all pool
/// threads → stage 2 can't start → deadlock" failure mode that bit the
/// pre-fusion API.
pub trait StageSpawn<In: Send + Unpin + 'static> {
    type Out: Send + Unpin + 'static;
    fn spawn<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Self::Out>;

    /// Like [`spawn`](Self::spawn) but the output channel is MPSC (single
    /// consumer). The collector in [`StreamPipe::run`] is always the sole
    /// consumer of the final channel, so using the MPSC ring buffer eliminates
    /// the per-item `lock cmpxchg` that the MPMC ring buffer pays on every
    /// `recv` — replaced by a plain `store` on the single-consumer dequeue
    /// path.
    ///
    /// The default implementation delegates to [`spawn`](Self::spawn), which
    /// keeps the MPMC channel (no regression for stages that don't override).
    /// Concrete stages that create output channels override this to create an
    /// [`mpsc_channel`] instead.
    fn spawn_single<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Self::Out>
    where
        Self: Sized,
    {
        self.spawn::<R>(rx, ctx)
    }

    /// Worker-budget summary of this chain — number of pool-consuming stages
    /// plus any per-stage worker counts pinned via `StageOptions::workers`.
    /// Used by `StreamPipe::run` to divide the pool across stages.
    fn stage_budget(&self) -> StageBudget;

    /// Returns `true` if this chain contains at least one `ExpandStage`.
    ///
    /// Expand is a 1-to-N fan-out: one input seq produces multiple outputs
    /// that **share** the parent's sequence number. The [`ReorderBuffer`] used
    /// by `.ordered()` is single-item-per-seq, so `expand` + `ordered()` would
    /// silently drop colliding items. [`StreamPipe::run`] checks this flag and
    /// rejects the combination with a clear panic instead of corrupting output.
    fn has_expand(&self) -> bool {
        false
    }

    /// Returns `Some(true)` if the innermost *real* stage in this chain — the
    /// first non-`StreamStart` stage that consumes the feeder channel — is
    /// async, `Some(false)` if it's sync, or `None` if there are no real
    /// stages (the chain is just `StreamStart`).
    ///
    /// Used by [`StreamPipe::run`] to pick the feeder channel type: when the
    /// first real consumer is async, the feeder can push directly into a
    /// mixed-mode (`SyncSender` + `AsyncReceiver`) channel and the sync→async
    /// bridge thread can be skipped entirely.
    ///
    /// The recursion is "innermost wins": each stage defers to its `prev`'s
    /// answer, and only emits its own answer when `prev` had no opinion (i.e.
    /// `prev` was `StreamStart`). Fence links are transparent (don't claim to
    /// be the first consumer).
    fn first_consumer_is_async(&self) -> Option<bool> {
        None
    }

    /// Whether this chain contains at least one async stage — i.e. whether
    /// `run()` will need an async runtime backend at all.
    ///
    /// [`StreamPipe::try_exec`] pre-warms the lazily-built runtime when this
    /// is `true` *before* spawning the chain: the `OnceLock` then caches the
    /// `Ok`, so every `acquire_async().expect(..)` inside the spawn walk
    /// (bridges, async consumers) is guaranteed to succeed — a construction
    /// failure surfaces up front as `try_run`'s `Err` instead of panicking
    /// half way through pipeline setup.
    fn has_async_stage(&self) -> bool {
        false
    }

    /// Spawn with an async feeder receiver. Called by [`StreamPipe::run`]
    /// when [`Self::first_consumer_is_async`] returns `Some(true)`.
    ///
    /// The default implementation bridges `AsyncReceiver → Receiver` (one OS
    /// thread running `block_on`) and delegates to [`Self::spawn`] — a generic
    /// fallback for stage chains the crate does not know. **Every built-in
    /// stage overrides this** to recurse via `prev.spawn_async_feeder(..)`
    /// instead, so the async channel is only converted to sync at the exact
    /// stage that needs a sync receiver — chains like
    /// `stream(..).stage_async(a).stage(s)` pay one async→sync bridge (at
    /// `s`), not a bridge per level plus an async→sync→async round-trip.
    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Self::Out>
    where
        Self: Sized,
    {
        // Bridge async→sync on a dedicated OS thread, then delegate to the
        // sync `spawn` path. The bridge MUST run on an OS thread (via
        // `bridge_async_to_sync`), not a runtime task:
        // `SyncSender::send` is blocking, and running it inside an async task
        // would park the runtime worker thread whenever the downstream sync
        // stage exerts backpressure — the "one thread is both async driver
        // and blocking worker" anti-pattern that stalls every other task on
        // that worker.
        let s_rx = bridge_async_to_sync::<_, R>(rx, ctx);
        self.spawn::<R>(s_rx, ctx)
    }

    /// Like [`spawn_async_feeder`](Self::spawn_async_feeder) but the output
    /// channel is MPSC — the exact counterpart of
    /// [`spawn_single`](Self::spawn_single) for the async-feeder path, kept so
    /// an async-first chain's terminal stage does not lose the per-item
    /// `lock cmpxchg` saving just because its feeder is async.
    ///
    /// The default implementation delegates to
    /// [`spawn_async_feeder`](Self::spawn_async_feeder) (MPMC, no regression
    /// for stages that don't override).
    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder_single<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Self::Out>
    where
        Self: Sized,
    {
        self.spawn_async_feeder::<R>(rx, ctx)
    }

    /// Spawn this stage to feed a downstream **async** consumer, returning the
    /// [`AsyncReceiver`] the consumer should read from.
    ///
    /// This is the sync→async handoff primitive. The default implementation
    /// spawns the stage normally (via [`Self::spawn`]) and then bridges its
    /// output into a mixed-mode channel — i.e. it still pays for a forwarder
    /// thread. **Sync stages override this** to write the mixed-mode
    /// [`SyncSender`] directly from their ComputePool workers, eliminating the
    /// dedicated bridge thread entirely: the workers are already OS threads,
    /// so blocking on `SyncSender::send` under backpressure is the natural
    /// (and correct) behaviour — not the "async driver + blocking worker"
    /// anti-pattern that mandates a bridge when a tokio task would be the
    /// producer.
    ///
    /// `AsyncStage` calls this on its `prev` to obtain its input channel
    /// regardless of whether the preceding stage is sync or async: each stage
    /// picks the channel kind that lets its producers run with the least
    /// friction (mixed-mode for sync producers, fully-async for async ones).
    #[cfg(feature = "tokio-runtime")]
    fn spawn_for_async<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> AsyncReceiver<(u64, Self::Out)>
    where
        Self: Sized,
    {
        // Default: spawn normally, then bridge the output (sync or async) into
        // a mixed-mode channel. Sync stages override this to skip the bridge
        // — see `SyncStage::spawn_for_async`.
        let fr = self.spawn::<R>(rx, ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (tx, a_rx) = sync_async_channel::<(u64, Self::Out)>(buffer);
        let cancel = ctx.cancel.clone();
        match fr {
            FinalRx::Sync(r) => {
                // sync output → mixed-mode: a plain forward thread. Both the
                // source `recv` and the sink `send` are blocking, so this is
                // just a data-copying thread — the same shape as the old
                // sync→async bridge that used to live in `spawn_async_consumers`.
                std::thread::spawn(move || {
                    while let Ok(item) = r.recv() {
                        if cancel_active(cancel.as_ref()) {
                            return;
                        }
                        if tx.send(item).is_err() {
                            return;
                        }
                    }
                });
            },
            FinalRx::Async(r) => {
                // async output → mixed-mode: `block_on` the async receiver on
                // a dedicated OS thread (mirrors `bridge_async_to_sync`, but
                // emits into a mixed-mode sender so the consumer side stays
                // async). Only reached when an async stage feeds another async
                // stage through the default impl — `AsyncStage` overrides this
                // to return its already-async output directly.
                let pool = ctx.acquire_async().expect("failed to build async runtime");
                std::thread::spawn(move || {
                    pool.block_on(async move {
                        while let Ok(item) = r.recv().await {
                            if cancel_active(cancel.as_ref()) {
                                return;
                            }
                            if tx.send(item).is_err() {
                                return;
                            }
                        }
                    });
                });
            },
            FinalRx::SyncSingle(_) | FinalRx::AsyncSingle(_) => {
                unreachable!(
                    "spawn_for_async calls self.spawn() which never returns Single variants"
                )
            },
        }
        a_rx
    }
}

/// Final receiver handed back by [`StageSpawn::spawn`]. Drained by the
/// `StreamPipe::run` collector.
pub enum FinalRx<T: Send + Unpin + 'static> {
    Sync(Receiver<(u64, T)>),
    /// MPSC variant — the receiver uses a lighter ring-buffer algorithm
    /// (store-based dequeue, lock-free waker registry) because the collector
    /// is the sole consumer. Produced by [`StageSpawn::spawn_single`].
    SyncSingle(MpscReceiver<(u64, T)>),
    #[cfg(feature = "tokio-runtime")]
    Async(AsyncReceiver<(u64, T)>),
    #[cfg(feature = "tokio-runtime")]
    AsyncSingle(MpscAsyncReceiver<(u64, T)>),
}

/// Extract the sync `Receiver` from a previous stage's [`FinalRx`].
///
/// [`StageSpawn::spawn`] never produces `SyncSingle` or `AsyncSingle` (only
/// [`StageSpawn::spawn_single`] does), so those arms are unreachable here.
/// Every stage's `spawn`/`spawn_single` calls this to obtain its input channel.
#[cfg_attr(not(feature = "tokio-runtime"), allow(unused_variables))]
fn finalize_prev_rx<T: Send + Unpin + 'static, R: AsyncRuntime>(
    prev_rx: FinalRx<T>,
    ctx: &StreamCtx<'_, R>,
) -> Receiver<(u64, T)> {
    match prev_rx {
        FinalRx::Sync(r) => r,
        FinalRx::SyncSingle(_) => {
            unreachable!("prev.spawn() never returns SyncSingle")
        },
        #[cfg(feature = "tokio-runtime")]
        FinalRx::Async(r) => bridge_async_to_sync::<_, R>(r, ctx),
        #[cfg(feature = "tokio-runtime")]
        FinalRx::AsyncSingle(_) => {
            unreachable!("prev.spawn() never returns AsyncSingle")
        },
    }
}

/// Shared per-run configuration: pool handles, cancellation, buffer sizing.
/// Built fresh in `StreamPipe::run` and passed by reference to every stage's
/// `spawn` call.
///
/// Generic over the async runtime backend `R`. Sync-only chains still
/// instantiate this (with `R = NoRuntime` by default) but never call
/// [`Self::acquire_async`], so the `NoRuntime` methods' panics stay
/// unreachable.
pub struct StreamCtx<'a, R: AsyncRuntime = DefaultRuntime> {
    pub config: &'a PipelineConfig,
    pub cancel: Option<CancellationToken>,
    pub n: usize,
    /// Default per-stage compute-pool parallelism, set by `StreamPipe::run`
    /// from the worker budget: explicit `StageOptions::workers` pins are
    /// deducted from `compute_workers` first, then the remainder is divided
    /// equally across the unpinned sync stages (clamped to ≥ 1). A stage's
    /// actual worker count is resolved in `StreamCtx::stage_workers`, which
    /// consults the stage's own `StageOptions` before falling back here.
    /// Bridges and fences (which have no options of their own) use this value
    /// directly.
    pub per_stage_parallelism: usize,
    /// When `true`, sync/expand stage workers (and a non-inline feeder) run
    /// as dedicated OS threads instead of pool jobs. Chosen by
    /// [`StreamPipe::try_exec`] when the pool's liveness budget cannot give
    /// every sync stage a resident worker (more sync stages than available
    /// pool slots), or when `run()` itself executes on a worker of the same
    /// pool (nested pipelines park that worker in the collector for the
    /// whole run, so pool admission can never be guaranteed). The pool's
    /// fixed thread count makes parked-in-channel jobs unschedulable by
    /// definition; dedicated threads keep such chains deadlock-free at the
    /// cost of one `thread::spawn` per worker.
    pub dedicated_threads: bool,
    /// Pool-mode liveness budget: worker slots still grantable to
    /// not-yet-spawned sync stages. Initialised to `pool_threads - reserved`
    /// (the feeder job reserves one slot when it is not inline) and decremented
    /// by [`Self::stage_workers`] as the spawn walk grants workers upstream
    /// first. Invariant: `worker_slots_left ≥ stages_left` holds at every
    /// grant, so every stage keeps ≥ 1 resident worker and the total number
    /// of blocking pool jobs never exceeds the pool — the "stage 1 fills the
    /// pool, stage 2 starves, deadlock" failure mode.
    pub(crate) worker_slots_left: Cell<usize>,
    /// Sync/expand stages not yet spawned; decremented alongside
    /// [`Self::worker_slots_left`] to preserve the invariant above.
    pub(crate) stages_left: Cell<usize>,
    /// Custom compute pool (cloned from the builder's `with_compute_pool`).
    /// When `None`, sync stages use [`ComputePool::global`].
    pub compute_pool: Option<ComputePool>,
    #[cfg(feature = "tokio-runtime")]
    pub async_pool: Option<R>,
    /// Lazily-constructed runtime for this single `run()` call, used when the
    /// caller did not attach one via [`StreamPipe::with_async_pool`].
    ///
    /// Without this cache every `acquire_async()` call (one per async stage
    /// plus one per sync→async bridge) would build a *fresh* runtime (~ms
    /// each), silently wrecking small workloads. One runtime is built on
    /// first use and reused for the whole `run()`.
    ///
    /// Stored as `io::Result` (not just the pool) so a construction failure
    /// is reported identically to every caller — `OnceLock::get_or_init`
    /// runs the initializer exactly once. (`OnceLock::get_or_try_init` would
    /// be the natural fit but is still unstable as of 1.85.)
    #[cfg(feature = "tokio-runtime")]
    pub(crate) cached_pool: OnceLock<std::io::Result<R>>,
    /// Carries the backend type `R` even when no backend feature is enabled
    /// (then the `async_pool` / `cached_pool` fields don't exist, so `R`
    /// would otherwise be an unused type parameter). Zero-sized at runtime.
    _marker: PhantomData<R>,
}

impl<R: AsyncRuntime> StreamCtx<'_, R> {
    pub fn buffer_size(&self, parallelism: usize) -> usize {
        self.config.buffer_size.max(parallelism * 4)
    }

    /// Resolve a stage's worker count: the stage's explicit
    /// `StageOptions::workers` pin, or the runner-computed default division.
    /// Clamped to `[1, n]` (a stage never gets more workers than items).
    ///
    /// Pool mode additionally clamps the grant to the remaining liveness
    /// budget (`worker_slots_left`), reserving one slot per not-yet-spawned
    /// sync stage so explicit pins cannot push the total blocking pool jobs
    /// past the pool size. Pins are therefore honored in pipeline order
    /// (upstream first) only until the budget runs out; what remains is
    /// spread over later stages at 1 worker each. In dedicated-thread mode
    /// the pool is untouched and the request is honored as-is.
    pub fn stage_workers(&self, opts: &StageOptions) -> usize {
        let requested = opts
            .workers
            .map_or(self.per_stage_parallelism, NonZeroUsize::get);
        let requested = requested.min(self.n.max(1)).max(1);
        if self.dedicated_threads {
            return requested;
        }
        let slots = self.worker_slots_left.get();
        let after = self.stages_left.get() - 1;
        self.stages_left.set(after);
        // `slots ≥ after + 1` holds by construction (try_exec picks pool mode
        // only when the budget covers every sync stage), so `granted ≥ 1`
        // never breaks the invariant — the `max(1)` is just the type-level
        // floor.
        let granted = requested.min(slots.saturating_sub(after)).max(1);
        self.worker_slots_left.set(slots - granted);
        granted
    }

    /// Dispatch a stage's worker closures: one batched pool submission in
    /// pool mode, one dedicated OS thread per worker in dedicated-thread
    /// mode. Threads are detached — workers always terminate on channel
    /// disconnect (see [`spawn_stage`]), and the collector only returns once
    /// every sender is dropped, so no thread outlives the run's usefulness.
    pub(crate) fn spawn_stage_jobs<F>(&self, jobs: Vec<F>)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.dedicated_threads {
            for job in jobs {
                std::thread::spawn(move || {
                    // Match the pool's AbortIfPanic semantics: a panicking
                    // stage worker must abort, not silently detach and
                    // truncate the pipeline's output.
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                        std::process::abort();
                    }
                });
            }
        } else {
            self.compute_pool().submit_batch(jobs);
        }
    }

    /// Resolve a stage's output-channel capacity: the stage's explicit
    /// `StageOptions::buffer` pin (used verbatim — the caller opted into this
    /// exact backpressure), or the config value with the
    /// `downstream_workers * 4` floor.
    pub fn stage_buffer(&self, opts: &StageOptions, parallelism: usize) -> usize {
        opts.buffer
            .map_or_else(|| self.buffer_size(parallelism), NonZeroUsize::get)
    }

    /// Resolve an async stage's task fan-out: the stage's explicit
    /// `StageOptions::io_concurrency` pin, or the global config value.
    /// Clamped to `[1, n]`.
    pub fn stage_io_concurrency(&self, opts: &StageOptions) -> usize {
        let resolved = opts
            .io_concurrency
            .map_or(self.config.io_concurrency, NonZeroUsize::get);
        resolved.max(1).min(self.n.max(1))
    }

    /// Returns the compute pool for this run: the user-supplied pool from
    /// `with_compute_pool`, or the global pool as default.
    pub fn compute_pool(&self) -> &ComputePool {
        match &self.compute_pool {
            Some(p) => p,
            None => ComputePool::global(),
        }
    }

    /// Acquire an async runtime for this run.
    ///
    /// - If the caller attached a pool via `with_async_pool`, hand back a cheap clone (the
    ///   backend's `Clone` is `Arc`/`Handle`-refcounted).
    /// - Otherwise build one lazily on first call via [`AsyncRuntime::build_default`] and cache it
    ///   in [`StreamCtx::cached_pool`] so subsequent calls in the same `run()` reuse the same
    ///   runtime instead of paying the construction cost again.
    #[cfg(feature = "tokio-runtime")]
    pub fn acquire_async(&self) -> std::io::Result<R> {
        if let Some(p) = &self.async_pool {
            return Ok(p.clone());
        }
        // First caller builds; everyone else in this `run()` reuses the same
        // runtime. `get_or_init` is thread-safe — bridges from different
        // stages may race on first call.
        let cached = self
            .cached_pool
            .get_or_init(|| R::build_default(self.config.async_workers));
        match cached {
            Ok(p) => Ok(p.clone()),
            // Rebuild an equivalent error so the same failure is surfaced
            // afresh to every caller rather than moving the singleton out of
            // the lock (`io::Error` is not `Clone`).
            Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
        }
    }
}

// StreamStart: identity spawn — returns rx unchanged.
impl<I: Send + Unpin + 'static> StageSpawn<I> for StreamStart {
    type Out = I;

    fn spawn<R: AsyncRuntime>(self, rx: Receiver<(u64, I)>, _ctx: &StreamCtx<'_, R>) -> FinalRx<I> {
        FinalRx::Sync(rx)
    }

    fn stage_budget(&self) -> StageBudget {
        StageBudget::default()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        None
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, I)>,
        _ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<I> {
        // Identity — pass the async feeder rx through unchanged so the
        // wrapping AsyncStage can consume it directly. This is the key
        // hop-elimination: when the chain is `stream(..).stage_async(..)`,
        // the feeder's mixed-mode channel becomes the AsyncStage's input
        // channel — no bridge thread needed.
        FinalRx::Async(rx)
    }
}

// SyncStage<Prev, F>: recurse into prev, then spawn sync workers for f.
impl<Prev, F, In, M> StageSpawn<In> for SyncStage<Prev, F>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> M + Send + Sync + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
{
    type Out = M;

    fn spawn<R: AsyncRuntime>(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx<'_, R>) -> FinalRx<M> {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = channel::<(u64, M)>(buffer);
        spawn_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::Sync(out_rx)
    }

    fn spawn_single<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M>
    where
        Self: Sized,
    {
        // Terminal sync stage: output goes to the sole collector, so use the
        // lighter MPSC ring buffer (see `spawn_single` trait doc). Previous
        // stages still use MPMC (`prev.spawn`, not `prev.spawn_single`) —
        // their output feeds multiple workers in this stage.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = mpsc_channel::<(u64, M)>(buffer);
        spawn_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::SyncSingle(out_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_for_async<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> AsyncReceiver<(u64, M)> {
        // Direct sync→async handoff: ComputePool workers write the mixed-mode
        // `SyncSender` directly — no bridge thread (see `spawn_for_async`
        // trait doc for why this is correct and load-bearing: chains like
        // `stream(..).stage(cpu).stage_async(io)` previously paid a dedicated
        // forwarding OS thread per item; now the CPU stage's workers ARE the
        // mixed-mode producers).
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = sync_async_channel::<(u64, M)>(buffer);
        spawn_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        out_rx
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M> {
        // Recurse via `prev.spawn_async_feeder` (NOT the default
        // bridge-then-`spawn`): the async feeder channel stays async through
        // every upstream stage and is converted to sync exactly once, here,
        // by `finalize_prev_rx` — only when the prev chain actually ends on
        // an async channel. The default impl instead bridged the feeder
        // async→sync up front AND let each nested `spawn` add its own hops,
        // costing async-first chains 2 extra bridge threads + 2 channel
        // landings per item (measured on `.stage_async(a).stage(s)`).
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = channel::<(u64, M)>(buffer);
        spawn_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::Sync(out_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder_single<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M> {
        // Same as `spawn_async_feeder` but the (terminal) output channel is
        // MPSC — mirrors `spawn_single` vs `spawn`.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = mpsc_channel::<(u64, M)>(buffer);
        spawn_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::SyncSingle(out_rx)
    }

    fn stage_budget(&self) -> StageBudget {
        // This stage consumes a pool slot; recurse to count earlier stages.
        let mut budget = self.prev.stage_budget();
        budget.stages += 1;
        if let Some(w) = self.opts.workers {
            budget.explicit_stages += 1;
            budget.explicit_workers += w.get();
        }
        budget
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Defer to prev's opinion; if prev had none, *we* are the first real
        // consumer — and we're sync.
        self.prev.first_consumer_is_async().or(Some(false))
    }

    fn has_async_stage(&self) -> bool {
        self.prev.has_async_stage()
    }

    fn has_expand(&self) -> bool {
        self.prev.has_expand()
    }
}
impl<Prev, F, In, N> StageSpawn<In> for ExpandStage<Prev, F>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> Vec<N> + Send + Sync + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    N: Send + Unpin + 'static,
{
    type Out = N;

    fn spawn<R: AsyncRuntime>(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx<'_, R>) -> FinalRx<N> {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = channel::<(u64, N)>(buffer);
        spawn_expand_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::Sync(out_rx)
    }

    fn spawn_single<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<N>
    where
        Self: Sized,
    {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = mpsc_channel::<(u64, N)>(buffer);
        spawn_expand_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::SyncSingle(out_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_for_async<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> AsyncReceiver<(u64, N)> {
        // Same direct-handoff optimisation as `SyncStage::spawn_for_async`:
        // expansion workers write the mixed-mode sender directly.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = sync_async_channel::<(u64, N)>(buffer);
        spawn_expand_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        out_rx
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<N> {
        // See `SyncStage::spawn_async_feeder` — async feeder stays async
        // through prev, converted once at our input.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = channel::<(u64, N)>(buffer);
        spawn_expand_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::Sync(out_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder_single<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<N> {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let parallelism = ctx.stage_workers(&self.opts);
        let buffer = ctx.stage_buffer(&self.opts, parallelism);
        let (out_tx, out_rx) = mpsc_channel::<(u64, N)>(buffer);
        spawn_expand_stage(ctx, mid_rx, out_tx, parallelism, self.f);
        FinalRx::SyncSingle(out_rx)
    }

    fn stage_budget(&self) -> StageBudget {
        let mut budget = self.prev.stage_budget();
        budget.stages += 1;
        if let Some(w) = self.opts.workers {
            budget.explicit_stages += 1;
            budget.explicit_workers += w.get();
        }
        budget
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Expand stages are sync — claim "first consumer" only if prev didn't.
        self.prev.first_consumer_is_async().or(Some(false))
    }

    fn has_async_stage(&self) -> bool {
        self.prev.has_async_stage()
    }

    fn has_expand(&self) -> bool {
        true
    }
}
impl<Prev, In> StageSpawn<In> for FenceLink<Prev>
where
    Prev: StageSpawn<In>,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
{
    type Out = Prev::Out;

    fn spawn<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Prev::Out> {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        let expected = ctx.n;
        let cancel = ctx.cancel.clone();
        std::thread::spawn(move || {
            forward_fenced(mid_rx, fenced_tx, mode, expected, cancel.as_ref());
        });
        FinalRx::Sync(fenced_rx)
    }

    fn spawn_single<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Prev::Out>
    where
        Self: Sized,
    {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = mpsc_channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        let expected = ctx.n;
        let cancel = ctx.cancel.clone();
        std::thread::spawn(move || {
            forward_fenced(mid_rx, fenced_tx, mode, expected, cancel.as_ref());
        });
        FinalRx::SyncSingle(fenced_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_for_async<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> AsyncReceiver<(u64, Prev::Out)> {
        // Direct handoff: the fence forwarder writes the mixed-mode sender
        // directly. It already runs on a dedicated OS thread, so blocking on
        // `send` under backpressure is its natural behaviour — no extra bridge
        // needed between the fence and a downstream async stage.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn::<R>(rx, ctx), ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = sync_async_channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        let expected = ctx.n;
        let cancel = ctx.cancel.clone();
        std::thread::spawn(move || {
            forward_fenced(mid_rx, fenced_tx, mode, expected, cancel.as_ref());
        });
        fenced_rx
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Prev::Out> {
        // See `SyncStage::spawn_async_feeder` — the async feeder channel is
        // converted to sync exactly once (at the fence's own input) instead of
        // up front plus per level.
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        let expected = ctx.n;
        let cancel = ctx.cancel.clone();
        std::thread::spawn(move || {
            forward_fenced(mid_rx, fenced_tx, mode, expected, cancel.as_ref());
        });
        FinalRx::Sync(fenced_rx)
    }

    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder_single<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<Prev::Out> {
        let mid_rx = finalize_prev_rx::<_, R>(self.prev.spawn_async_feeder::<R>(rx, ctx), ctx);
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = mpsc_channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        let expected = ctx.n;
        let cancel = ctx.cancel.clone();
        std::thread::spawn(move || {
            forward_fenced(mid_rx, fenced_tx, mode, expected, cancel.as_ref());
        });
        FinalRx::SyncSingle(fenced_rx)
    }

    fn stage_budget(&self) -> StageBudget {
        // Fence runs on a dedicated thread, doesn't consume a pool slot.
        self.prev.stage_budget()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Fence is transparent — defer to prev.
        self.prev.first_consumer_is_async()
    }

    fn has_async_stage(&self) -> bool {
        self.prev.has_async_stage()
    }

    fn has_expand(&self) -> bool {
        self.prev.has_expand()
    }
}

// AsyncStage<Prev, F>: recurse into prev (likely sync), bridge sync→async,
// then spawn `io_concurrency` async tasks on the runtime.
#[cfg(feature = "tokio-runtime")]
impl<Prev, F, In, M, Fut> StageSpawn<In> for AsyncStage<Prev, F>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = M> + Send + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
{
    type Out = M;

    fn spawn<R: AsyncRuntime>(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx<'_, R>) -> FinalRx<M> {
        FinalRx::Async(self.spawn_for_async::<R>(rx, ctx))
    }

    fn spawn_for_async<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> AsyncReceiver<(u64, M)> {
        // async → async: recurse via prev's `spawn_for_async` to obtain our
        // input channel — mixed-mode when prev is sync (ComputePool workers
        // write the sender directly, **no bridge thread**), fully-async when
        // prev is async (runtime-task funnel). Then run the consumer fan-out.
        // The output is already a fully-async channel, handed back directly.
        let a_in_rx = self.prev.spawn_for_async::<R>(rx, ctx);
        spawn_async_consumers_body::<F, Prev::Out, M, Fut, R>(self.f, a_in_rx, &self.opts, ctx)
    }

    fn stage_budget(&self) -> StageBudget {
        // Async stage runs on the async runtime, not the compute pool.
        self.prev.stage_budget()
    }

    fn spawn_single<R: AsyncRuntime>(
        self,
        rx: Receiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M>
    where
        Self: Sized,
    {
        // Terminal async stage: the collector is the sole consumer of the
        // output, so use the lighter MPSC async channel (store-based dequeue,
        // no `lock cmpxchg`). Input channel stays MPMC (`prev.spawn_for_async`)
        // — multiple async consumer tasks share the input via clone.
        let a_in_rx = self.prev.spawn_for_async::<R>(rx, ctx);
        FinalRx::AsyncSingle(
            spawn_async_consumers_body_single::<F, Prev::Out, M, Fut, R>(
                self.f, a_in_rx, &self.opts, ctx,
            ),
        )
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Defer to prev's opinion; if prev had none, *we* are the first real
        // consumer — and we're async.
        self.prev.first_consumer_is_async().or(Some(true))
    }

    fn has_async_stage(&self) -> bool {
        true
    }

    fn has_expand(&self) -> bool {
        self.prev.has_expand()
    }

    fn spawn_async_feeder<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M> {
        // Recurse via `spawn_async_feeder`. When prev is `StreamStart`, this
        // returns the feeder rx unchanged as `FinalRx::Async` — letting us
        // consume it directly and skip the sync→async bridge entirely. Other
        // prev stages keep the async channel as far downstream as their own
        // consumers need it (see their `spawn_async_feeder` overrides).
        let prev_rx = self.prev.spawn_async_feeder::<R>(rx, ctx);
        spawn_async_consumers::<Prev, F, In, M, Fut, R>(self.f, prev_rx, &self.opts, ctx)
    }

    fn spawn_async_feeder_single<R: AsyncRuntime>(
        self,
        rx: AsyncReceiver<(u64, In)>,
        ctx: &StreamCtx<'_, R>,
    ) -> FinalRx<M> {
        // Terminal async stage on the async-feeder path: same bridging as
        // `spawn_async_feeder`, but the consumer fan-out writes an MPSC
        // channel (`spawn_async_consumers_body_single`) — the collector is
        // the sole consumer, mirroring `spawn_single` vs `spawn`.
        let prev_rx = self.prev.spawn_async_feeder::<R>(rx, ctx);
        let buffer = ctx.stage_buffer(&self.opts, ctx.stage_io_concurrency(&self.opts));
        let a_in_rx = bridge_final_rx_to_async::<Prev::Out, R>(prev_rx, buffer, ctx);
        FinalRx::AsyncSingle(
            spawn_async_consumers_body_single::<F, Prev::Out, M, Fut, R>(
                self.f, a_in_rx, &self.opts, ctx,
            ),
        )
    }
}

/// Spawn `io_concurrency` async consumer tasks that read `a_in_rx`, apply `f`,
/// and forward to a fresh async output channel; returns that output channel.
///
/// This is the consumer fan-out half of an async stage, shared by both entry
/// points: `AsyncStage::spawn_for_async` (the `spawn` path — `a_in_rx` arrives
/// directly from `prev.spawn_for_async`) and `spawn_async_consumers` (the
/// `spawn_async_feeder` path — `a_in_rx` is bridged from a `FinalRx` first).
#[cfg(feature = "tokio-runtime")]
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// `f` is moved into the `Arc` shared across consumer tasks; taking it by value
// expresses "this is the last stop for the closure".
fn spawn_async_consumers_body<F, In, M, Fut, R>(
    f: F,
    a_in_rx: AsyncReceiver<(u64, In)>,
    opts: &StageOptions,
    ctx: &StreamCtx<'_, R>,
) -> AsyncReceiver<(u64, M)>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = M> + Send + 'static,
    In: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
    R: AsyncRuntime,
{
    let concurrency = ctx.stage_io_concurrency(opts);
    let buffer = ctx.stage_buffer(opts, concurrency);
    let (a_out_tx, a_out_rx) = async_channel::<(u64, M)>(buffer);
    let pool = ctx.acquire_async().expect("failed to build async runtime");
    let f = Arc::new(f);
    let cancel = ctx.cancel.clone();
    for _ in 0..concurrency {
        let f = f.clone();
        let rx = a_in_rx.clone();
        let tx = a_out_tx.clone();
        let c = cancel.clone();
        // Spawn via the runtime-agnostic backend. `pool.spawn` is the explicit
        // spawn — it does not depend on a TLS current-runtime context, so no
        // `enter()` guard is needed (a future non-tokio backend's scoped-tls
        // `enter` would have no RAII guard anyway).
        pool.spawn(async move {
            loop {
                let Ok((seq, item)) = rx.recv().await else {
                    break;
                };
                if cancel_active(c.as_ref()) {
                    break;
                }
                let out = f(item).await;
                if tx.send((seq, out)).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(a_out_tx);
    drop(a_in_rx);
    a_out_rx
}

/// Like [`spawn_async_consumers_body`] but produces an MPSC output channel
/// ([`MpscAsyncReceiver`]) instead of MPMC — the right shape when this is the
/// terminal async stage and the collector is the sole consumer of the output
/// (same MPSC-vs-MPMC rationale as [`StageSpawn::spawn_single`]). The sender
/// side stays async (`MpscAsyncSender::send().await`): the producers are
/// runtime tasks, not OS threads — a blocking send would stall the worker.
///
/// Invoked by [`AsyncStage::spawn_single`] via [`StageSpawn::spawn_single`].
#[cfg(feature = "tokio-runtime")]
#[allow(clippy::needless_pass_by_value)] // `f` is moved into the `Arc` shared
// across consumer tasks; taking it by value expresses "this is the last stop
// for the closure" (same rationale as `spawn_async_consumers_body`).
fn spawn_async_consumers_body_single<F, In, M, Fut, R>(
    f: F,
    a_in_rx: AsyncReceiver<(u64, In)>,
    opts: &StageOptions,
    ctx: &StreamCtx<'_, R>,
) -> MpscAsyncReceiver<(u64, M)>
where
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = M> + Send + 'static,
    In: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
    R: AsyncRuntime,
{
    let concurrency = ctx.stage_io_concurrency(opts);
    let buffer = ctx.stage_buffer(opts, concurrency);
    let (a_out_tx, a_out_rx) = mpsc_async_channel::<(u64, M)>(buffer);
    let pool = ctx.acquire_async().expect("failed to build async runtime");
    let f = Arc::new(f);
    let cancel = ctx.cancel.clone();
    for _ in 0..concurrency {
        let f = f.clone();
        let rx = a_in_rx.clone();
        let tx = a_out_tx.clone();
        let c = cancel.clone();
        pool.spawn(async move {
            loop {
                let Ok((seq, item)) = rx.recv().await else {
                    break;
                };
                if cancel_active(c.as_ref()) {
                    break;
                }
                let out = f(item).await;
                if tx.send((seq, out)).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(a_out_tx);
    drop(a_in_rx);
    a_out_rx
}

/// Bridge prev's output (sync or async) into an async input channel.
///
/// Serves the `spawn_async_feeder` path (chains whose first stage is async):
/// an [`AsyncStage`] obtains its input as a `FinalRx` from
/// `prev.spawn_async_feeder(..)` and needs an [`AsyncReceiver`] to fan out
/// over its consumer tasks.
///
///   sync → async: dedicated OS thread + blocking `send` over a mixed-mode
///                 channel. Blocking on a runtime worker thread would stall
///                 the executor, so the producer side must be a thread.
///
///   async → async: runtime task + async `send().await` over a fully async
///                 channel (see the NOTE(perf) below — the task is
///                 load-bearing).
#[cfg(feature = "tokio-runtime")]
fn bridge_final_rx_to_async<T, R>(
    prev_rx: FinalRx<T>,
    buffer: usize,
    ctx: &StreamCtx<'_, R>,
) -> AsyncReceiver<(u64, T)>
where
    T: Send + Unpin + 'static,
    R: AsyncRuntime,
{
    let bridge_cancel = ctx.cancel.clone();
    match prev_rx {
        FinalRx::Sync(mid_rx) => {
            // sync → async bridge. The `spawn` path no longer reaches here —
            // sync stages override `spawn_for_async` to write the mixed-mode
            // sender directly. This arm serves only `spawn_async_feeder`
            // (first-stage-async chains where a later sync stage feeds us).
            let (a_in_tx, a_in_rx) = sync_async_channel::<(u64, T)>(buffer);
            std::thread::spawn(move || {
                while let Ok(item) = mid_rx.recv() {
                    if cancel_active(bridge_cancel.as_ref()) {
                        return;
                    }
                    if a_in_tx.send(item).is_err() {
                        return;
                    }
                }
            });
            a_in_rx
        },
        FinalRx::SyncSingle(_) => {
            unreachable!("spawn_async_feeder path never produces SyncSingle")
        },
        FinalRx::Async(prev_async_rx) => {
            // NOTE(perf): this bridge task is NOT redundant — do not try to
            // remove it by having consumers clone `prev_async_rx` directly.
            //
            // Attempted: consumers clone the upstream receiver and pull
            // directly, eliminating one task + one bounded channel per item.
            // Measured on `io_async_pure` (sample-size 30, vs the
            // readme_20260627_v3 baseline): youpipe_async/200 +0.50 %,
            // /500 +0.82 % (both p ≈ 0.03–0.04) — consistently a regression.
            // Hypothesis: with the bridge, it is the sole registered waker on
            // `prev_async_rx`, so each produced item wakes exactly one task.
            // Without it, all `concurrency` consumer clones register wakers on
            // the same `MAsyncRx` (`crossfire::mpmc` uses `RegistryMulti`), so
            // one item can spuriously wake several consumers — all but one
            // poll an empty queue, re-register, return `Pending`. That
            // scheduler churn outweighs the saved hop at `io_concurrency ≥ 64`.
            //
            // The bridge is a load-bearing 1-task funnel converting the MPMC
            // upstream into a single-waker source. Keep it.
            let (a_in_tx, a_in_rx) = async_channel::<(u64, T)>(buffer);
            let pool = ctx.acquire_async().expect("failed to build async runtime");
            pool.spawn(async move {
                while let Ok(item) = prev_async_rx.recv().await {
                    if cancel_active(bridge_cancel.as_ref()) {
                        return;
                    }
                    if a_in_tx.send(item).await.is_err() {
                        return;
                    }
                }
            });
            a_in_rx
        },
        FinalRx::AsyncSingle(_) => {
            unreachable!("spawn_async_feeder path never produces AsyncSingle")
        },
    }
}

/// Bridge prev's [`FinalRx`] into an async input channel via
/// [`bridge_final_rx_to_async`], then run the consumer fan-out via
/// [`spawn_async_consumers_body`] (MPMC output).
///
/// This serves **only the `spawn_async_feeder` path** (chains whose first
/// stage is async). The regular `spawn` path never reaches here: sync
/// stages override `spawn_for_async` to write the mixed-mode sender directly,
/// so `AsyncStage::spawn_for_async` obtains its input channel with no bridge.
#[cfg(feature = "tokio-runtime")]
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// `f` is moved into the `Arc` shared across consumer tasks; taking it by value
// expresses "this is the last stop for the closure".
fn spawn_async_consumers<Prev, F, In, M, Fut, R>(
    f: F,
    prev_rx: FinalRx<Prev::Out>,
    opts: &StageOptions,
    ctx: &StreamCtx<'_, R>,
) -> FinalRx<M>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = M> + Send + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
    R: AsyncRuntime,
{
    let buffer = ctx.stage_buffer(opts, ctx.stage_io_concurrency(opts));
    let a_in_rx = bridge_final_rx_to_async::<Prev::Out, R>(prev_rx, buffer, ctx);
    FinalRx::Async(spawn_async_consumers_body::<F, Prev::Out, M, Fut, R>(
        f, a_in_rx, opts, ctx,
    ))
}

// ── StreamPipe builder methods ──

impl<S, I, O, R: AsyncRuntime> StreamPipe<S, I, O, R> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Attach a [`CancellationToken`] for cooperative cancellation. Feeder,
    /// stage workers, and bridges all check the token per iteration.
    #[must_use]
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Mark the output as order-preserving. The feeder tags each item with a
    /// sequence number; the collector uses a [`ReorderBuffer`] to emit results
    /// in input order. The default is unordered (faster — no reorder pass).
    #[must_use]
    pub fn ordered(mut self) -> Self {
        self.ordered = true;
        self
    }

    /// Attach a managed async runtime so async stages (added via
    /// [`Self::stage_async`]) reuse it across runs instead of building a
    /// transient runtime per call.
    ///
    /// This changes the pipeline's runtime type parameter to `R2`, so a chain
    /// built without it (defaulting to [`DefaultRuntime`]) is monomorphised to
    /// the concrete backend you pass here.
    ///
    /// Recommended inside tight loops (e.g. criterion benches): runtime
    /// construction costs ~ms, which would otherwise dominate small workloads.
    #[cfg(feature = "tokio-runtime")]
    #[must_use]
    pub fn with_async_pool<R2: AsyncRuntime>(self, pool: R2) -> StreamPipe<S, I, O, R2> {
        // Rebuild with the new runtime type, transplanting every other field.
        // The `async_pool` field is the only one whose type depends on `R`.
        StreamPipe {
            items: self.items,
            stages: self.stages,
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            async_pool: Some(pool),
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Attach a custom [`ComputePool`] for sync stages. When omitted, sync
    /// stages run on the global pool (sized to `num_cpus`).
    ///
    /// The primary use case is **oversubscribing threads for blocking-IO sync
    /// stages**: the global pool has one thread per core, which caps blocking
    /// concurrency at `num_cpus`. For workloads that mix blocking IO into a
    /// sync `.stage()` (rather than using `.stage_async()`), a larger pool
    /// (e.g. `ComputePool::new(512)`) matches tokio's `spawn_blocking`
    /// behaviour.
    ///
    /// `ComputePool` is cheap to clone (`Arc` + one atomic), so the pool can
    /// be created once and reused across many `run()` calls — important for
    /// tight loops where per-call pool construction (~ms) would dominate.
    ///
    /// The worker budget divided across sync stages follows the pool's thread
    /// count — unless [`with_compute_workers`](Self::with_compute_workers)
    /// pinned one explicitly, in which case the pin wins regardless of the
    /// order the two setters were called in (clamped to the pool size, which
    /// is a hard ceiling on schedulable blocking jobs).
    ///
    /// (Pool sizes like 128 fit blocking-IO oversubscription; kept small
    /// here so the example also runs under miri — the streaming design
    /// schedules `workers + 1 feeder` blocking jobs, which must stay within
    /// the pool size.)
    ///
    /// ```rust
    /// use youpipe::{ComputePool, stream};
    ///
    /// let pool = ComputePool::new(4);
    /// let result = stream(0..100)
    ///     .with_compute_pool(pool)
    ///     .stage(|x: u64| x + 1)
    ///     .run();
    /// ```
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        // No budget overwrite here: run() resolves the worker budget from the
        // pool's thread count unless the user pinned one via
        // `with_compute_workers` (`PipelineConfig::compute_workers_pinned`).
        // Overwriting here instead made the two setters silently clobber each
        // other based on call order (a pool set after a workers pin reverted
        // the pin to the pool size).
        self.compute_pool = Some(pool);
        self
    }

    /// Set the compute-pool worker budget that gets divided across sync
    /// stages (streaming counterpart of the fused path's thread count).
    /// Unset fields in a [`StageOptions`] attached to a stage fall back to a
    /// share of this budget.
    ///
    /// The budget is **pinned** by this call: it applies regardless of
    /// [`with_compute_pool`](Self::with_compute_pool) and regardless of the
    /// order the two setters were called in, clamped to the pool's thread
    /// count. With no pin set, the budget follows the pool (global pool →
    /// one worker per core).
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Set the async runtime's OS-thread count (streaming-only; async stages
    /// multiplex [`PipelineConfig::io_concurrency`] tasks over these threads).
    #[must_use]
    pub fn with_async_workers(mut self, n: usize) -> Self {
        self.config.async_workers = n.max(1);
        self
    }

    /// Set the per-channel buffer capacity between stages (streaming-only).
    /// See [`PipelineConfig::with_buffer_size`] for the
    /// `max(downstream_workers * 4)` floor.
    #[must_use]
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.config.buffer_size = n.max(1);
        self
    }

    /// Set the default concurrent-task fan-out for async stages
    /// (streaming-only). Overridable per stage via
    /// [`StageOptions::io_concurrency`] on [`Self::stage_async_with`].
    #[must_use]
    pub fn with_io_concurrency(mut self, n: usize) -> Self {
        self.config.io_concurrency = n.max(1);
        self
    }

    /// Append a synchronous CPU stage: `Fn(O) -> N`. Runs on the work-stealing
    /// [`ComputePool`]; the output type changes to `N`.
    pub fn stage<N>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> StreamPipe<SyncStage<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
    {
        self.stage_with(StageOptions::new(), f)
    }

    /// [`Self::stage`] with per-stage tuning — see [`StageOptions`].
    ///
    /// ```rust
    /// use youpipe::{StageOptions, stream};
    ///
    /// // Heavy parse stage pinned to 4 workers; later stages divide the rest.
    /// let result: Vec<i32> = stream(0..100)
    ///     .stage_with(StageOptions::new().workers(4), |x: i32| x + 1)
    ///     .stage(|x: i32| x * 2)
    ///     .run();
    /// # assert_eq!(result.len(), 100);
    /// ```
    pub fn stage_with<N>(
        self,
        opts: StageOptions,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> StreamPipe<SyncStage<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: SyncStage {
                prev: self.stages,
                f,
                opts,
            },
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            #[cfg(feature = "tokio-runtime")]
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Append a 1-to-N expansion stage: `Fn(O) -> Vec<N>`. Each input item
    /// produces zero or more outputs (like `flat_map`); expanded items inherit
    /// the parent's sequence tag for ordered collection.
    #[allow(clippy::type_complexity)] // typestate builder return type; the
    // `impl Fn` + nested `ExpandStage` + `R` param are inherent to the design
    // and not helpfully decomposable.
    pub fn expand<N>(
        self,
        f: impl Fn(O) -> Vec<N> + Send + Sync + 'static,
    ) -> StreamPipe<ExpandStage<S, impl Fn(O) -> Vec<N> + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
    {
        self.expand_with(StageOptions::new(), f)
    }

    /// [`Self::expand`] with per-stage tuning — see [`StageOptions`].
    #[allow(clippy::type_complexity)] // typestate builder return type; the
    // `impl Fn` + nested `ExpandStage` + `R` param are inherent to the design
    // and not helpfully decomposable.
    pub fn expand_with<N>(
        self,
        opts: StageOptions,
        f: impl Fn(O) -> Vec<N> + Send + Sync + 'static,
    ) -> StreamPipe<ExpandStage<S, impl Fn(O) -> Vec<N> + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: ExpandStage {
                prev: self.stages,
                f,
                opts,
            },
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            #[cfg(feature = "tokio-runtime")]
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Insert a fence (materialisation barrier) between the stages chained
    /// **before** this call and the stages chained **after** it.
    ///
    /// # Scope — one boundary, not the whole stream
    ///
    /// A fence controls exactly **one** adjacent stage transition — between
    /// whatever precedes it and whatever follows it; it never affects other
    /// boundaries. A chain may insert as many as the topology needs:
    ///
    /// ```text
    /// stream(..)
    ///     .stage(s1)
    ///     .fence(m1)        // ← boundary between s1 and (s2, s3)
    ///     .stage(s2)
    ///     .stage(s3)
    ///     .fence(m2)        // ← boundary between (s2, s3) and s4
    ///     .stage(s4)
    ///     .run();
    /// ```
    ///
    /// # Modes
    ///
    /// - [`FenceMode::Barrier`] fully drains the upstream before downstream starts (hard isolation;
    ///   max peak memory, no staging overlap).
    /// - [`FenceMode::Chunked`] releases batches as soon as they form so the two sides overlap —
    ///   the right default for mixed CPU/IO loads.
    pub fn fence(self, mode: FenceMode) -> StreamPipe<FenceLink<S>, I, O, R> {
        StreamPipe {
            items: self.items,
            stages: FenceLink {
                prev: self.stages,
                mode,
            },
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            #[cfg(feature = "tokio-runtime")]
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Append an async IO stage: `Fn(O) -> Future<Output = N>`. Runs as
    /// `io_concurrency` tasks on the [`AsyncRuntime`] backend — the runtime's
    /// scheduler multiplexes those tasks over `async_workers` OS threads, so
    /// concurrency is bounded by `io_concurrency` (not by the thread count).
    ///
    /// For work that *blocks* the OS thread (e.g. `std::thread::sleep`), prefer
    /// [`Self::stage`]: a blocking call inside an async task stalls a runtime
    /// worker and forfeits the M:N advantage.
    #[cfg(feature = "tokio-runtime")]
    pub fn stage_async<N, Fut>(
        self,
        f: impl Fn(O) -> Fut + Send + Sync + 'static,
    ) -> StreamPipe<AsyncStage<S, impl Fn(O) -> Fut + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
        Fut: Future<Output = N> + Send + 'static,
    {
        self.stage_async_with(StageOptions::new(), f)
    }

    /// [`Self::stage_async`] with per-stage tuning — see [`StageOptions`].
    /// The interesting knob here is [`StageOptions::io_concurrency`]: pin a
    /// high fan-out for network-bound stages, a low one for disk-bound
    /// stages, instead of one global `io_concurrency` for the whole chain.
    ///
    /// ```rust
    /// # use youpipe::{stream, StageOptions};
    /// # async fn fetch(u: u64) -> u64 { u }
    /// let result: Vec<u64> = stream(0..64)
    ///     .stage_async_with(
    ///         StageOptions::new().io_concurrency(32),
    ///         |u: u64| async move { fetch(u).await },
    ///     )
    ///     .run();
    /// # assert_eq!(result.len(), 64);
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub fn stage_async_with<N, Fut>(
        self,
        opts: StageOptions,
        f: impl Fn(O) -> Fut + Send + Sync + 'static,
    ) -> StreamPipe<AsyncStage<S, impl Fn(O) -> Fut + Send + Sync + 'static>, I, N, R>
    where
        N: Send + Unpin + 'static,
        Fut: Future<Output = N> + Send + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: AsyncStage {
                prev: self.stages,
                f,
                opts,
            },
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }
}

// ── Run (execute the chain) ──

/// How a streaming run drains the final receiver: into a `Vec` (`.run()`) or
/// item-by-item through a closure (`.for_each()` — no output `Vec`
/// materialised).
trait Terminal<T>: Sized {
    type Out: Send + 'static;
    /// Result for an empty input (nothing is spawned at all).
    fn drain_empty(self) -> Self::Out;
    fn drain_sync<R: RecvItem<(u64, T)>>(self, rx: R, ordered: bool, n: usize) -> Self::Out;
    #[cfg(feature = "tokio-runtime")]
    fn drain_async<R: AsyncRecvItem<(u64, T)>>(
        self,
        rx: R,
        ordered: bool,
        n: usize,
    ) -> impl Future<Output = Self::Out>;
}

/// `Vec`-materialising terminal — the behaviour of `.run()`.
struct VecCollector;

impl<T: Send + Unpin + 'static> Terminal<T> for VecCollector {
    type Out = Vec<T>;

    fn drain_empty(self) -> Vec<T> {
        Vec::new()
    }

    fn drain_sync<R: RecvItem<(u64, T)>>(self, rx: R, ordered: bool, n: usize) -> Vec<T> {
        collect_sync(rx, ordered, n)
    }

    #[cfg(feature = "tokio-runtime")]
    async fn drain_async<R: AsyncRecvItem<(u64, T)>>(
        self,
        rx: R,
        ordered: bool,
        n: usize,
    ) -> Vec<T> {
        collect_async(rx, ordered, n).await
    }
}

/// Side-effect terminal — the behaviour of `.for_each()`. `f` runs on the
/// calling thread (both the sync drain and the `block_on` async drain), so it
/// needs no `Send`/`'static` bounds.
struct ForEachCollector<F> {
    f: F,
}

impl<T: Send + Unpin + 'static, F: FnMut(T)> Terminal<T> for ForEachCollector<F> {
    type Out = ();

    fn drain_empty(self) {}

    fn drain_sync<R: RecvItem<(u64, T)>>(self, rx: R, ordered: bool, n: usize) {
        for_each_sync(rx, ordered, n, self.f);
    }

    #[cfg(feature = "tokio-runtime")]
    async fn drain_async<R: AsyncRecvItem<(u64, T)>>(self, rx: R, ordered: bool, n: usize) {
        for_each_async(rx, ordered, n, self.f).await;
    }
}

/// Item-by-item drain of a sync final receiver — the shared
/// [`drain_unordered`]/[`drain_ordered`] loops with the user closure as the
/// sink.
#[allow(clippy::needless_pass_by_value)] // terminal drain: sole receiver by value
fn for_each_sync<R, T, F>(rx: R, ordered: bool, n: usize, f: F)
where
    R: RecvItem<(u64, T)>,
    T: Send + Unpin + 'static,
    F: FnMut(T),
{
    if ordered {
        crate::state::drain_ordered(&rx, n, f);
    } else {
        crate::state::drain_unordered(&rx, f);
    }
}

/// Async counterpart of [`for_each_sync`] — the shared
/// [`drain_unordered_async`]/[`drain_ordered_async`] loops with the user
/// closure as the sink.
#[cfg(feature = "tokio-runtime")]
#[allow(clippy::needless_pass_by_value)] // terminal drain: sole receiver by value
async fn for_each_async<R, T, F>(rx: R, ordered: bool, n: usize, f: F)
where
    R: AsyncRecvItem<(u64, T)>,
    T: Send + Unpin + 'static,
    F: FnMut(T),
{
    if ordered {
        crate::state::drain_ordered_async(&rx, n, f).await;
    } else {
        crate::state::drain_unordered_async(&rx, f).await;
    }
}

impl<S, I, O, R: AsyncRuntime> StreamPipe<S, I, O, R>
where
    S: StageSpawn<I, Out = O>,
    I: Send + Unpin + 'static,
    O: Send + Unpin + 'static,
{
    /// Execute the streaming pipeline and collect results into a `Vec<O>`.
    ///
    /// Feeds `items` through the stage chain (channels between each stage),
    /// optionally reorders by sequence tag if `.ordered()` was called, and
    /// drains the final receiver into a `Vec`.
    ///
    /// # Panics
    ///
    /// Panics if `.ordered()` is combined with `.expand()` (see
    /// [`FenceMode`] docs), if the async runtime cannot be constructed
    /// (e.g. OS thread/resource limits), or if `run()` is invoked inside an
    /// async context on a chain with an async stage (e.g. from a tokio/axum
    /// handler) — the terminal drives its collector via
    /// [`AsyncRuntime::block_on`](crate::AsyncRuntime::block_on), which cannot
    /// block a thread that is running async tasks; wrap the call in
    /// `tokio::task::spawn_blocking` instead. To handle runtime construction
    /// failure gracefully, use [`try_run`](Self::try_run) or pass a pre-built
    /// backend via [`with_async_pool`](Self::with_async_pool).
    pub fn run(self) -> Vec<O> {
        self.try_run().expect(
            "StreamPipe::run: failed to build async runtime (OS resource limit? pass a custom \
             backend via with_async_pool, or use try_run to handle the failure)",
        )
    }

    /// Execute the streaming pipeline, applying `f` to each output item for
    /// its side effect — **no output `Vec` is materialised**.
    ///
    /// The streaming counterpart of the fused path's
    /// [`for_each`](crate::Pipe::for_each): for pipelines whose last step is
    /// a side effect (file writes, shared-state mutation, logging), this
    /// avoids the structural cost of an `n`-slot output buffer plus the full
    /// `Vec` that [`run`](Self::run) would allocate.
    ///
    /// Without `.ordered()`, `f` sees items in completion order. With
    /// `.ordered()`, items are re-sequenced through a [`ReorderBuffer`] in
    /// input order first (note: `.ordered()` + `.expand()` still panics, same
    /// as [`run`](Self::run)).
    ///
    /// `f` runs on the calling thread, so it needs no `Send`/`'static`
    /// bounds — accumulate into local state directly, no atomics required.
    ///
    /// ```rust
    /// # use youpipe::stream;
    /// let mut total = 0u64;
    /// stream(0..100)
    ///     .stage(|x: u64| x * 2)
    ///     .for_each(|x| total += x); // plain &mut capture, no Arc<Atomic>
    /// assert_eq!(total, (0..100u64).map(|x| x * 2).sum::<u64>());
    /// ```
    ///
    /// # Panics
    ///
    /// Same contract as [`run`](Self::run).
    pub fn for_each<F>(self, f: F)
    where
        F: FnMut(O),
    {
        self.try_exec(ForEachCollector { f })
            .expect("StreamPipe::for_each: failed to build async runtime (see run/try_run)");
    }

    /// Fallible counterpart to [`run`](Self::run): returns the runtime
    /// construction error instead of panicking.
    ///
    /// The only recoverable failure today is async runtime construction
    /// (e.g. OS thread/resource limits). Programming errors —
    /// `.ordered()` + `.expand()`, panics inside stage closures, feeder-thread
    /// join failures — still panic, matching the contract of every other
    /// youpipe terminal (`.collect()`, `.for_each()`).
    ///
    /// ```rust
    /// # use youpipe::prelude::*;
    /// // Equivalent to `.run()` for sync chains — the Result matters when
    /// // the chain contains `.stage_async(..)` and runtime construction
    /// // might fail.
    /// let r: Vec<i32> = (0..100)
    ///     .stream()
    ///     .stage(|x: i32| x + 1)
    ///     .try_run()
    ///     .expect("try_run on sync chain never fails");
    /// assert_eq!(r.len(), 100);
    /// ```
    ///
    /// # Panics
    ///
    /// Same programming-error panics as [`run`](Self::run).
    pub fn try_run(self) -> std::io::Result<Vec<O>> {
        self.try_exec(VecCollector)
    }

    /// Shared execution core: spawn the chain, feed the items, and hand the
    /// final receiver to a [`Terminal`] sink.
    // The only fallible path is the async-pool acquisition (`ctx
    // .acquire_async()`), which exists solely under `tokio-runtime`; without
    // that feature every branch is infallible and the `Result` looks like a
    // needless wrap. The signature stays fallible for the shared public API
    // (`try_run` returns `io::Result`).
    #[cfg_attr(not(feature = "tokio-runtime"), allow(clippy::unnecessary_wraps))]
    fn try_exec<T: Terminal<O>>(self, terminal: T) -> std::io::Result<T::Out> {
        let n = self.items.len();
        if n == 0 {
            return Ok(terminal.drain_empty());
        }
        // `expand` produces multiple outputs sharing one parent seq, but the
        // `ReorderBuffer` (`.ordered()`) is single-item-per-seq — the collision
        // silently drops data. Reject the combination loudly instead.
        assert!(
            !(self.ordered && self.stages.has_expand()),
            "`.ordered()` is incompatible with `.expand()`: expand fan-out shares the parent \
             sequence number, which the ReorderBuffer cannot re-sequence. Drop `.ordered()` \
             (completion order is still correct) or replace `expand` with a 1:1 `stage`."
        );
        let Self {
            items,
            stages,
            config,
            cancel,
            compute_pool,
            #[cfg(feature = "tokio-runtime")]
            async_pool,
            ordered,
            _marker,
        } = self;

        // Compute the default per-stage compute-pool parallelism and the
        // dispatch mode (pool jobs vs dedicated OS threads).
        //
        // Liveness invariant (pool mode): every blocking job the run parks
        // on a pool thread must be simultaneously schedulable — that is
        // `feeder(≤1) + Σ stage workers ≤ pool_threads`. A worker parked
        // inside a crossfire send/recv never returns to the pool's find_work
        // loop, so a job left queued while every thread is parked deadlocks
        // the run (reproduced pre-fix: 4 stages on a 4-thread pool with
        // n > k × buffer hung forever — the feeder job was uncounted and the
        // per-stage floor of 1 pushed the total past the pool).
        //
        // The feeder therefore reserves a slot *before* dividing the budget:
        // it is a pool job whenever `n > buffer`, inline on the calling
        // thread otherwise. Explicit `StageOptions::workers` pins are granted
        // first (clamped to what remains — see `StreamCtx::stage_workers`),
        // then the rest is divided equally across unpinned stages.
        //
        // When even 1 worker per sync stage does not fit (k + feeder >
        // pool_threads), or when `run()` itself executes on a worker of the
        // same pool (nested pipelines: the collector parks that worker for
        // the whole run, so pool admission of *any* further blocking job —
        // feeder included — depends on other tenants' courtesy), stage
        // workers and the feeder fall back to dedicated OS threads, which the
        // OS schedules independently of pool occupancy.
        let budget = stages.stage_budget();
        let pool = compute_pool
            .as_ref()
            .map_or_else(|| ComputePool::global().clone(), Clone::clone);
        let pool_threads = pool.num_workers();
        // Buffer/parallelism are mutually dependent (the buffer floor is
        // `parallelism * 4`), so the feeder-path prediction uses a
        // provisional division over the full pool. Pool mode only ever
        // divides *fewer* slots, shrinking the buffer floor — never flipping
        // a predicted-inline feeder into a blocking one. Dedicated mode may
        // divide more, but there the feeder is a thread anyway.
        //
        // Budget resolution: an explicitly pinned `with_compute_workers` wins
        // (clamped to the pool below); unpinned follows the pool's thread
        // count so a big blocking-IO pool actually gets its workers.
        let workers_budget = if config.compute_workers_pinned {
            config.compute_workers
        } else {
            config.compute_workers.max(pool_threads)
        };
        let provisional = budget.default_workers(workers_budget.min(pool_threads));
        let feeder_buffer = config.buffer_size.max(provisional * 4);
        let live_slots = pool_threads.saturating_sub(usize::from(n > feeder_buffer));
        let dedicated_threads = pool.is_on_this_pool() || live_slots < budget.stages;
        let per_stage_parallelism = if dedicated_threads {
            // Threads are not pool-bounded; divide the configured budget.
            budget.default_workers(workers_budget)
        } else {
            budget.default_workers(workers_budget.min(live_slots))
        };
        let (worker_slots_left, stages_left) = if dedicated_threads {
            (0, 0)
        } else {
            (live_slots, budget.stages)
        };

        let ctx: StreamCtx<'_, R> = StreamCtx {
            config: &config,
            cancel,
            n,
            per_stage_parallelism,
            dedicated_threads,
            worker_slots_left: Cell::new(worker_slots_left),
            stages_left: Cell::new(stages_left),
            compute_pool: Some(pool),
            #[cfg(feature = "tokio-runtime")]
            async_pool,
            #[cfg(feature = "tokio-runtime")]
            cached_pool: OnceLock::new(),
            _marker: PhantomData,
        };

        // Warm the lazily-built async runtime (when the chain needs one)
        // BEFORE spawning it: the OnceLock caches this result, so every
        // `acquire_async().expect(..)` inside the spawn walk (bridges, async
        // consumers) is guaranteed to read an `Ok` and can never panic — a
        // construction failure (e.g. OS thread limits) surfaces here as
        // `try_run`'s `Err` instead of aborting a half-spawned pipeline.
        #[cfg(feature = "tokio-runtime")]
        if stages.has_async_stage() {
            ctx.acquire_async()?;
        }

        let buffer = ctx.buffer_size(per_stage_parallelism);

        // Pick the feeder channel type from the chain's innermost real stage.
        //
        // When the first real consumer is async (i.e. the chain is shaped like
        // `stream(..).stage_async(..)[.fence(..)...]`), use a mixed-mode
        // (`SyncSender` + `AsyncReceiver`) feeder channel. The feeder still
        // pushes via the blocking `SyncSender::send`, but the AsyncStage's
        // `spawn_async_feeder` consumes the `AsyncReceiver` *directly* —
        // skipping the dedicated OS-thread bridge that the sync-feeder path
        // has to spawn. For every other chain shape (sync first stage, or no
        // stages at all) the regular sync feeder channel is used.
        //
        // Both feeder branches share an identical push loop — the only
        // difference is whether the *receiver* end is sync (-> `spawn_single`)
        // or async (-> `spawn_async_feeder_single`). The sender side
        // (`SyncSender`) is the same type either way, so [`feed_items`]
        // handles both, and both `_single` terminals produce an MPSC final
        // channel for the collector.
        //
        // Without any backend feature, `AsyncStage` doesn't exist, so
        // `first_consumer_is_async` can never return `Some(true)` and the
        // async branch is unreachable; the `cfg_not` block keeps the function
        // compilable in that configuration.
        let async_feeder = stages.first_consumer_is_async() == Some(true);
        let feeder_cancel = ctx.cancel.clone();
        debug_assert!(
            cfg!(feature = "tokio-runtime") || !async_feeder,
            "first_consumer_is_async == Some(true) requires an async runtime backend feature"
        );

        #[cfg(feature = "tokio-runtime")]
        let (final_rx, feeder) = if async_feeder {
            let (feeder_tx, feeder_rx) = sync_async_channel::<(u64, I)>(buffer);
            let feeder = feed_items(
                ctx.compute_pool(),
                items,
                feeder_tx,
                feeder_cancel,
                buffer,
                dedicated_threads,
            );
            // `spawn_async_feeder_single` keeps the terminal MPSC property
            // (same rationale as the sync branch's `spawn_single`): without
            // it, async-first chains fell back to an MPMC final channel and
            // the collector paid the per-item `lock cmpxchg` again.
            (
                stages.spawn_async_feeder_single::<R>(feeder_rx, &ctx),
                feeder,
            )
        } else {
            let (feeder_tx, feeder_rx) = channel::<(u64, I)>(buffer);
            let feeder = feed_items(
                ctx.compute_pool(),
                items,
                feeder_tx,
                feeder_cancel,
                buffer,
                dedicated_threads,
            );
            // Use `spawn_single` so the terminal stage's output channel is MPSC
            // (store-based dequeue, lock-free waker registry) — the collector is
            // always the sole consumer of the final channel.
            (stages.spawn_single::<R>(feeder_rx, &ctx), feeder)
        };
        #[cfg(not(feature = "tokio-runtime"))]
        let (final_rx, feeder) = {
            let (feeder_tx, feeder_rx) = channel::<(u64, I)>(buffer);
            let feeder = feed_items(
                ctx.compute_pool(),
                items,
                feeder_tx,
                feeder_cancel,
                buffer,
                dedicated_threads,
            );
            (stages.spawn_single::<R>(feeder_rx, &ctx), feeder)
        };

        // Terminal-channel regression guard: both feeder paths must hand the
        // collector a Single (MPSC) final channel — the collector is always
        // the sole consumer. Async-first chains once silently degraded to the
        // MPMC terminal (per-item `lock cmpxchg` in the collector); every
        // debug-mode test run re-checks this here. Built-in stages all
        // override `spawn_single` / `spawn_async_feeder_single`; `StageSpawn`
        // is crate-private (not exported), so no external impl can return a
        // non-Single variant.
        #[cfg(feature = "tokio-runtime")]
        debug_assert!(
            matches!(final_rx, FinalRx::SyncSingle(_) | FinalRx::AsyncSingle(_)),
            "terminal channel must be MPSC (Single variant)"
        );
        #[cfg(not(feature = "tokio-runtime"))]
        debug_assert!(
            matches!(final_rx, FinalRx::SyncSingle(_)),
            "terminal channel must be MPSC (Single variant)"
        );

        let results = match final_rx {
            FinalRx::Sync(rx) => terminal.drain_sync(rx, ordered, n),
            FinalRx::SyncSingle(rx) => terminal.drain_sync(rx, ordered, n),
            #[cfg(feature = "tokio-runtime")]
            FinalRx::Async(rx) => {
                let pool = ctx.acquire_async()?;
                pool.block_on(terminal.drain_async(rx, ordered, n))
            },
            #[cfg(feature = "tokio-runtime")]
            FinalRx::AsyncSingle(rx) => {
                let pool = ctx.acquire_async()?;
                pool.block_on(terminal.drain_async(rx, ordered, n))
            },
        };

        feeder.finish();
        Ok(results)
    }
}

/// Sync collector: drains `rx` into a `Vec` via the shared drain loops —
/// ordered through [`run_ordered_collect`], unordered through
/// [`drain_unordered`](crate::state) with a `Vec` push sink.
#[allow(clippy::needless_pass_by_value)]
// `rx` is the terminal drain of the
// pipeline: `run` passes the sole receiver by value to express "consume fully".
fn collect_sync<R, T>(rx: R, ordered: bool, n: usize) -> Vec<T>
where
    R: RecvItem<(u64, T)>,
    T: Send + Unpin + 'static,
{
    if ordered {
        run_ordered_collect(&rx, n)
    } else {
        let mut results = Vec::with_capacity(n);
        crate::state::drain_unordered(&rx, |item| results.push(item));
        results
    }
}

/// Async collector: drains `rx` into a `Vec` via the shared async drain
/// loops. If `ordered`, re-sequences through a [`ReorderBuffer`].
///
/// Generic over the async receiver type so it works with both MPMC
/// ([`AsyncReceiver`]) and MPSC ([`MpscAsyncReceiver`]) channels — the
/// collector is always the sole consumer of the final channel, and the MPSC
/// variant eliminates the per-item `lock cmpxchg` that the MPMC ring buffer
/// pays on every `recv`.
#[cfg(feature = "tokio-runtime")]
async fn collect_async<R, T>(rx: R, ordered: bool, n: usize) -> Vec<T>
where
    R: AsyncRecvItem<(u64, T)>,
    T: Send + Unpin + 'static,
{
    let mut results = Vec::with_capacity(n);
    if ordered {
        crate::state::drain_ordered_async(&rx, n, |item| results.push(item)).await;
    } else {
        crate::state::drain_unordered_async(&rx, |item| results.push(item)).await;
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Feeder::finish` re-raises the caught feeder-job payload (the
    /// panic-propagation contract inherited from the old feeder thread's
    /// `join`) and is a no-op for the inline variant.
    #[test]
    fn test_feeder_finish_resumes_payload() {
        let slot: FeederPanicSlot = Arc::new(std::sync::Mutex::new(Some(
            Box::new("feeder boom") as Box<dyn std::any::Any + Send>
        )));
        let result = std::panic::catch_unwind(move || Feeder::Pool(slot).finish());
        let payload = result.expect_err("stored payload must be resumed");
        assert_eq!(payload.downcast_ref::<&str>().copied(), Some("feeder boom"));

        Feeder::Inline.finish();
    }
}
