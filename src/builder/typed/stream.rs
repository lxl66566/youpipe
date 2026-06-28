#[cfg(feature = "tokio-runtime")]
use std::future::Future;
#[cfg(feature = "tokio-runtime")]
use std::sync::OnceLock;
use std::{marker::PhantomData, sync::Arc};

#[cfg(feature = "tokio-runtime")]
use crate::handoff::{AsyncReceiver, TryRecvError, async_channel, sync_async_channel};
use crate::{
    builder::config::PipelineConfig,
    executor::compute::ComputePool,
    handoff::{Receiver, Sender, SyncSender, SharedWaitGroup, channel::channel},
    state::{FenceBarrier, FenceMode, ReorderBuffer, run_ordered_collect},
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
// Stages may be sync (run on `ComputePool`), async (run on `AsyncPool` via
// tokio tasks), or a fence (forward-fence thread between adjacent stages).

/// True iff `cancel` is set and the pipeline should stop feeding new work.
#[inline]
fn cancel_active(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(CancellationToken::is_cancelled)
}

/// Handle returned by [`feed_items`]: either an inline push (already done,
/// nothing to join) or a spawned feeder thread.
enum Feeder {
    Thread(std::thread::JoinHandle<()>),
    Inline,
}

impl Feeder {
    fn join(self) {
        if let Self::Thread(h) = self {
            h.join().expect("feeder thread panicked");
        }
    }
}

/// Push `items` into the feeder channel.
///
/// When all items fit in the channel buffer (`items.len() ≤ buffer`), push
/// inline from the calling thread — saving ~20-50 µs of thread-spawn/join
/// overhead per `run()` call, which is a measurable fraction of small
/// workloads (e.g. mixed_cpu_io_unbalanced/200 ≈ 680 µs total).
///
/// # Deadlock safety
///
/// The inline path is safe because `items.len() ≤ buffer` guarantees the
/// sender never blocks on `Full`: even if every downstream worker is blocked
/// on the *output* channel, the calling thread finishes pushing, drops the
/// sender, and proceeds to collect — draining the output and unblocking
/// workers. With the thread path, the feeder and collector run concurrently
/// so neither can starve the other.
fn feed_items<I: Send + 'static>(
    items: Vec<I>,
    feeder_tx: SyncSender<(u64, I)>,
    cancel: Option<CancellationToken>,
    buffer: usize,
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
        Feeder::Inline
    } else {
        Feeder::Thread(std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(cancel.as_ref()) {
                    break;
                }
                if feeder_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        }))
    }
}

/// Spawn `parallelism` workers on `pool` that pull from `rx`, apply `stage`,
/// and forward to `tx`. Each worker loops until its receiver disconnects or the
/// supplied cancellation token (if any) is signalled.
///
/// Items carry a `seq` tag so the collector can restore input order; sync
/// stages unwrap, apply `stage`, re-wrap. Tagging is always on (even in
/// unordered mode) because the cost is one `u64` per item — far below the
/// channel handoff itself — and unifying the channels avoids separate
/// ordered/unordered implementations per stage.
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// taking the endpoints by value ensures the caller cannot retain a clone that
// would keep the channel open after the workers have finished.
fn spawn_stage<I, O>(
    pool: &ComputePool,
    rx: Receiver<(u64, I)>,
    tx: Sender<(u64, O)>,
    parallelism: usize,
    cancel: Option<CancellationToken>,
    stage: impl Fn(I) -> O + Send + Sync + 'static,
) -> SharedWaitGroup
where
    I: Send + Unpin + 'static,
    O: Send + Unpin + 'static,
{
    let stage = Arc::new(stage);
    let wg = SharedWaitGroup::new();
    wg.add(parallelism);
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
            let wg = wg.clone();
            let worker_cancel = cancel.clone();
            move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let output = stage(item);
                    if tx.send((seq, output)).is_err() {
                        break;
                    }
                }
                wg.done();
            }
        })
        .collect();
    pool.submit_batch(jobs);
    drop(rx);
    drop(tx);
    wg
}

/// Like [`spawn_stage`] but expands each input into 1..N outputs via `expand`.
/// Each expanded item inherits the parent's `seq` so the collector can group
/// expansions from the same input.
#[allow(clippy::needless_pass_by_value)] // runs inside a `pool.submit(move …)`
fn spawn_expand_stage<I, N>(
    pool: &ComputePool,
    rx: Receiver<(u64, I)>,
    tx: Sender<(u64, N)>,
    parallelism: usize,
    cancel: Option<CancellationToken>,
    expand: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
) -> SharedWaitGroup
where
    I: Send + Unpin + 'static,
    N: Send + Unpin + 'static,
{
    let expand = Arc::new(expand);
    let wg = SharedWaitGroup::new();
    wg.add(parallelism);
    let jobs: Vec<_> = (0..parallelism)
        .map(|_| {
            let expand = expand.clone();
            let rx = rx.clone();
            let tx = tx.clone();
            let wg = wg.clone();
            let worker_cancel = cancel.clone();
            move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    for n in expand(item) {
                        if tx.send((seq, n)).is_err() {
                            break;
                        }
                    }
                }
                wg.done();
            }
        })
        .collect();
    pool.submit_batch(jobs);
    drop(rx);
    drop(tx);
    wg
}

/// Fence forwarder: drains `mid_rx` into a [`FenceBarrier`] and releases
/// batches to `fenced_tx` according to `mode`.
///
/// In [`FenceMode::Barrier`] mode nothing is forwarded until `mid_rx`
/// disconnects (stage 1 fully done) — a hard barrier. In
/// [`FenceMode::Chunked`] mode batches flow as they accumulate, letting
/// stage 2 overlap stage 1.
///
/// Draining `mid_rx` eagerly (rather than waiting on a separate barrier
/// first) is what keeps stage 1 from blocking on a full channel: this is the
/// fix for the previous wait-before-drain deadlock.
#[allow(clippy::needless_pass_by_value)] // runs inside a `thread::spawn(move …)`:
// owning `mid_rx` / `fenced_tx` by value lets them drop (and close the channel)
// when the forwarder returns, which is how the downstream stage detects "no
// more items" — taking them by reference would keep the channel open forever.
fn forward_fenced<M>(mid_rx: Receiver<(u64, M)>, fenced_tx: Sender<(u64, M)>, mode: FenceMode)
where
    M: Send + Unpin + 'static,
{
    let mut fence = FenceBarrier::<(u64, M)>::new(mode);
    while let Ok(item) = mid_rx.recv() {
        if let Some(batch) = fence.push(item) {
            for it in batch {
                if fenced_tx.send(it).is_err() {
                    return;
                }
            }
        }
    }
    if let Some(remaining) = fence.flush() {
        for it in remaining {
            if fenced_tx.send(it).is_err() {
                return;
            }
        }
    }
}

// ── StreamPipe (data-first chainable streaming pipeline) ──

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
pub struct StreamPipe<S = StreamStart, I = (), O = ()> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    cancel: Option<CancellationToken>,
    #[cfg(feature = "tokio-runtime")]
    async_pool: Option<crate::executor::AsyncPool>,
    ordered: bool,
    _marker: PhantomData<O>,
}

/// Typestate marker for the start of a streaming chain (no stages yet).
pub struct StreamStart;

/// Data-first entry point for a streaming pipeline. Stages chained via
/// `.stage()` / `.stage_async()` are connected by channels at `.run()` time.
///
/// Unlike the fused [`crate::Pipe`], a `StreamPipe` always materialises each
/// stage's output through a channel — useful for backpressure-aware flows,
/// async IO stages, fences between stages, or cooperative cancellation.
pub fn stream<I, It>(items: It) -> StreamPipe<StreamStart, I, I>
where
    It: IntoIterator<Item = I>,
    I: Send + Unpin + 'static,
{
    StreamPipe {
        items: items.into_iter().collect(),
        stages: StreamStart,
        config: PipelineConfig::default(),
        cancel: None,
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
}

/// 1-to-N expansion stage: `Fn(O) -> Vec<N>`. Each input item produces zero or
/// more outputs; expanded items inherit the parent's `seq` for ordered
/// collection.
#[derive(Clone)]
pub struct ExpandStage<Prev, F> {
    pub(super) prev: Prev,
    pub(super) f: F,
}

/// Async stage: `Fn(O) -> Future<Output = N>`, runs as `io_concurrency` tokio
/// tasks on the [`AsyncPool`]. Gated behind the `tokio-runtime` feature.
#[cfg(feature = "tokio-runtime")]
#[derive(Clone)]
pub struct AsyncStage<Prev, F> {
    pub(super) prev: Prev,
    pub(super) f: F,
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
/// `worker_stages` returns the number of stages in the chain that consume
/// **compute-pool worker slots** (sync stages + expand stages). Fence links
/// and async stages don't count — fences run on a dedicated thread, async
/// stages run on the async runtime. The runner divides
/// `config.compute_workers` by this count so the total blocking jobs across
/// all sync stages never exceeds the pool size, preventing the
/// "stage 1 holds all pool threads → stage 2 can't start → deadlock" failure
/// mode that bit the pre-fusion API.
pub trait StageSpawn<In: Send + Unpin + 'static> {
    type Out: Send + Unpin + 'static;
    fn spawn(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<Self::Out>;

    /// Number of stages in this chain that consume compute-pool worker slots.
    /// Used by `StreamPipe::run` to divide the pool across stages.
    fn worker_stages(&self) -> usize;

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

    /// Spawn with an async feeder receiver. Called by [`StreamPipe::run`]
    /// when [`Self::first_consumer_is_async`] returns `Some(true)`.
    ///
    /// The default implementation bridges `AsyncReceiver → Receiver` (one
    /// tokio task) and delegates to [`Self::spawn`]. Stages whose immediate
    /// consumer is async should override to skip the bridge.
    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder(self, rx: AsyncReceiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<Self::Out>
    where
        Self: Sized,
    {
        // Default: bridge async → sync (one tokio task on the runtime), then
        // delegate to the sync `spawn` path. Async-first stages override this
        // to consume the async rx directly and skip the extra hop.
        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (s_tx, s_rx) = channel::<(u64, In)>(buffer);
        let cancel = ctx.cancel.clone();
        let pool = ctx.acquire_async().expect("failed to build async runtime");
        let _enter = pool.handle().enter();
        tokio::spawn(async move {
            while let Ok(item) = rx.recv().await {
                if cancel_active(cancel.as_ref()) {
                    return;
                }
                if s_tx.send(item).is_err() {
                    return;
                }
            }
        });
        self.spawn(s_rx, ctx)
    }
}

/// Final receiver handed back by [`StageSpawn::spawn`]. Drained by the
/// `StreamPipe::run` collector.
pub enum FinalRx<T: Send + Unpin + 'static> {
    Sync(Receiver<(u64, T)>),
    #[cfg(feature = "tokio-runtime")]
    Async(AsyncReceiver<(u64, T)>),
}

/// Shared per-run configuration: pool handles, cancellation, buffer sizing.
/// Built fresh in `StreamPipe::run` and passed by reference to every stage's
/// `spawn` call.
pub struct StreamCtx<'a> {
    pub config: &'a PipelineConfig,
    pub cancel: Option<CancellationToken>,
    pub n: usize,
    /// Per-stage compute-pool parallelism, set by `StreamPipe::run` as
    /// `compute_workers / worker_stages` (clamped to ≥ 1). Each sync stage
    /// uses this many pool workers so the total across all sync stages fits
    /// inside the pool — preventing the "stage 1 fills the pool, stage 2
    /// starves, deadlock" failure mode.
    pub per_stage_parallelism: usize,
    #[cfg(feature = "tokio-runtime")]
    pub async_pool: Option<crate::executor::AsyncPool>,
    /// Lazily-constructed runtime for this single `run()` call, used when the
    /// caller did not attach one via [`StreamPipe::with_async_pool`].
    ///
    /// Without this cache every `acquire_async()` call (one per async stage
    /// plus one per sync→async bridge) would build a *fresh* tokio runtime —
    /// each costing ~ms — silently wrecking small workloads. The cache keeps
    /// the "no config needed" default path fast: a single runtime is built on
    /// first use and dropped at the end of `run()`.
    ///
    /// Stored as `io::Result` (not just `AsyncPool`) so a construction failure
    /// is reported identically to every caller — `OnceLock::get_or_init`
    /// runs the initializer exactly once and hands back the same outcome to
    /// every subsequent `acquire_async()` call. (`OnceLock::get_or_try_init`
    /// would be the natural fit but is still unstable as of 1.85.)
    #[cfg(feature = "tokio-runtime")]
    pub(crate) cached_pool: OnceLock<std::io::Result<crate::executor::AsyncPool>>,
}

impl StreamCtx<'_> {
    pub fn buffer_size(&self, parallelism: usize) -> usize {
        self.config.buffer_size.max(parallelism * 4)
    }

    /// Acquire an async runtime for this run.
    ///
    /// - If the caller attached a pool via `with_async_pool`, wrap its handle
    ///   (cheap — `Handle` is internally `Arc`-refcounted).
    /// - Otherwise build one lazily on first call and cache it in
    ///   [`StreamCtx::cached_pool`] so subsequent calls in the same `run()`
    ///   reuse the same runtime instead of paying the ~ms construction cost
    ///   again.
    #[cfg(feature = "tokio-runtime")]
    pub fn acquire_async(&self) -> std::io::Result<crate::executor::AsyncPool> {
        if let Some(p) = &self.async_pool {
            return Ok(crate::executor::AsyncPool::new(
                p.handle().clone(),
                self.config.async_workers,
            ));
        }
        // First caller builds; everyone else in this `run()` reuses the same
        // runtime. `get_or_init` is thread-safe — bridges from different
        // stages may race on first call.
        let cached = self
            .cached_pool
            .get_or_init(|| crate::executor::AsyncPool::from_global(self.config.async_workers));
        match cached {
            Ok(p) => Ok(crate::executor::AsyncPool::new(
                p.handle().clone(),
                self.config.async_workers,
            )),
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
    fn spawn(self, rx: Receiver<(u64, I)>, _ctx: &StreamCtx) -> FinalRx<I> {
        FinalRx::Sync(rx)
    }
    fn worker_stages(&self) -> usize {
        0
    }
    fn first_consumer_is_async(&self) -> Option<bool> {
        None
    }
    #[cfg(feature = "tokio-runtime")]
    fn spawn_async_feeder(self, rx: AsyncReceiver<(u64, I)>, _ctx: &StreamCtx) -> FinalRx<I> {
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

    fn spawn(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<M> {
        let prev_rx = self.prev.spawn(rx, ctx);
        let mid_rx = match prev_rx {
            FinalRx::Sync(r) => r,
            #[cfg(feature = "tokio-runtime")]
            FinalRx::Async(r) => {
                // async → sync bridge: dedicated thread runs `block_on` on the
                // async receiver and forwards to a sync sender.
                let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
                let (s_tx, s_rx) = channel::<(u64, Prev::Out)>(buffer);
                let cancel = ctx.cancel.clone();
                let pool = ctx.acquire_async().expect("failed to build async runtime");
                std::thread::spawn(move || {
                    pool.block_on(async move {
                        while let Ok(item) = r.recv().await {
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
        };

        let parallelism = ctx.per_stage_parallelism.min(ctx.n.max(1)).max(1);
        let buffer = ctx.buffer_size(parallelism);
        let (out_tx, out_rx) = channel::<(u64, M)>(buffer);
        let _wg = spawn_stage(
            ComputePool::global(),
            mid_rx,
            out_tx,
            parallelism,
            ctx.cancel.clone(),
            self.f,
        );
        FinalRx::Sync(out_rx)
    }

    fn worker_stages(&self) -> usize {
        // This stage consumes a pool slot; recurse to count earlier stages.
        1 + self.prev.worker_stages()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Defer to prev's opinion; if prev had none, *we* are the first real
        // consumer — and we're sync.
        self.prev.first_consumer_is_async().or(Some(false))
    }
}

// ExpandStage<Prev, F>: recurse into prev, then spawn expand workers.
impl<Prev, F, In, N> StageSpawn<In> for ExpandStage<Prev, F>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> Vec<N> + Send + Sync + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    N: Send + Unpin + 'static,
{
    type Out = N;

    fn spawn(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<N> {
        let prev_rx = self.prev.spawn(rx, ctx);
        let mid_rx = match prev_rx {
            FinalRx::Sync(r) => r,
            #[cfg(feature = "tokio-runtime")]
            FinalRx::Async(r) => {
                let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
                let (s_tx, s_rx) = channel::<(u64, Prev::Out)>(buffer);
                let cancel = ctx.cancel.clone();
                let pool = ctx.acquire_async().expect("failed to build async runtime");
                std::thread::spawn(move || {
                    pool.block_on(async move {
                        while let Ok(item) = r.recv().await {
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
        };

        let parallelism = ctx.per_stage_parallelism.min(ctx.n.max(1)).max(1);
        let buffer = ctx.buffer_size(parallelism);
        let (out_tx, out_rx) = channel::<(u64, N)>(buffer);
        let _wg = spawn_expand_stage(
            ComputePool::global(),
            mid_rx,
            out_tx,
            parallelism,
            ctx.cancel.clone(),
            self.f,
        );
        FinalRx::Sync(out_rx)
    }

    fn worker_stages(&self) -> usize {
        1 + self.prev.worker_stages()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Expand stages are sync — claim "first consumer" only if prev didn't.
        self.prev.first_consumer_is_async().or(Some(false))
    }
}

// FenceLink<Prev>: recurse into prev, then insert a fence forwarder thread.
impl<Prev, In> StageSpawn<In> for FenceLink<Prev>
where
    Prev: StageSpawn<In>,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
{
    type Out = Prev::Out;

    fn spawn(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<Prev::Out> {
        let prev_rx = self.prev.spawn(rx, ctx);
        let mid_rx = match prev_rx {
            FinalRx::Sync(r) => r,
            #[cfg(feature = "tokio-runtime")]
            FinalRx::Async(r) => {
                let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
                let (s_tx, s_rx) = channel::<(u64, Prev::Out)>(buffer);
                let cancel = ctx.cancel.clone();
                let pool = ctx.acquire_async().expect("failed to build async runtime");
                std::thread::spawn(move || {
                    pool.block_on(async move {
                        while let Ok(item) = r.recv().await {
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
        };

        let buffer = ctx.buffer_size(ctx.per_stage_parallelism);
        let (fenced_tx, fenced_rx) = channel::<(u64, Prev::Out)>(buffer);
        let mode = self.mode;
        std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));
        FinalRx::Sync(fenced_rx)
    }

    fn worker_stages(&self) -> usize {
        // Fence runs on a dedicated thread, doesn't consume a pool slot.
        self.prev.worker_stages()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Fence is transparent — defer to prev.
        self.prev.first_consumer_is_async()
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

    fn spawn(self, rx: Receiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<M> {
        let prev_rx = self.prev.spawn(rx, ctx);
        spawn_async_consumers::<Prev, F, In, M, Fut>(self.f, prev_rx, ctx)
    }

    fn worker_stages(&self) -> usize {
        // Async stage runs on the async runtime, not the compute pool.
        self.prev.worker_stages()
    }

    fn first_consumer_is_async(&self) -> Option<bool> {
        // Defer to prev's opinion; if prev had none, *we* are the first real
        // consumer — and we're async.
        self.prev.first_consumer_is_async().or(Some(true))
    }

    fn spawn_async_feeder(self, rx: AsyncReceiver<(u64, In)>, ctx: &StreamCtx) -> FinalRx<M> {
        // Recurse via `spawn_async_feeder`. When prev is `StreamStart`, this
        // returns the feeder rx unchanged as `FinalRx::Async` — letting us
        // consume it directly and skip the sync→async bridge entirely. Other
        // prev stages fall back to the default impl (bridge async→sync) and
        // end up going through the normal sync `spawn` path.
        let prev_rx = self.prev.spawn_async_feeder(rx, ctx);
        spawn_async_consumers::<Prev, F, In, M, Fut>(self.f, prev_rx, ctx)
    }
}

/// Shared body of [`StageSpawn::spawn`] and [`StageSpawn::spawn_async_feeder`]
/// for `AsyncStage`: bridge prev's output (sync or async) into our async input
/// channel, then spawn `io_concurrency` async consumer tasks on the runtime.
///
/// Factored out as a free function (rather than a method on `AsyncStage<Prev,
/// F>`) because Rust's `impl` blocks can only constrain type parameters that
/// appear in the `Self` type — `In`, `M`, `Fut` only show up in the bounds, so
/// they have to live on the function itself. The two entry points (`spawn`,
/// `spawn_async_feeder`) differ only in how `prev_rx` is obtained — sync
/// `Receiver` via `spawn`, or async `AsyncReceiver` via `spawn_async_feeder`.
/// Once the upstream `FinalRx` is in hand, the consumer setup is identical.
///
/// # Bridge topology
///
///   sync → async: dedicated OS thread + blocking `send` over a mixed-mode
///                 channel (`SyncSender` + `AsyncReceiver` sharing one
///                 `mpmc::Array`). Backpressure parks the bridge thread via
///                 crossfire's internal waker — no `try_send` + `yield_now`
///                 busy-spin.
///
///   async → async: tokio task + async `send().await` over a fully async
///                 channel. Blocking on the runtime worker thread would stall
///                 the executor, so this side stays async.
#[cfg(feature = "tokio-runtime")]
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// `f` is moved into the `Arc` shared across consumer tasks; taking it by value
// expresses "this is the last stop for the closure".
fn spawn_async_consumers<Prev, F, In, M, Fut>(
    f: F,
    prev_rx: FinalRx<Prev::Out>,
    ctx: &StreamCtx,
) -> FinalRx<M>
where
    Prev: StageSpawn<In>,
    F: Fn(Prev::Out) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = M> + Send + 'static,
    In: Send + Unpin + 'static,
    Prev::Out: Send + Unpin + 'static,
    M: Send + Unpin + 'static,
{
    let concurrency = ctx.config.io_concurrency.max(1).min(ctx.n.max(1));
    let buffer = ctx.buffer_size(concurrency);
    let bridge_cancel = ctx.cancel.clone();
    let a_in_rx: AsyncReceiver<(u64, Prev::Out)> = match prev_rx {
        FinalRx::Sync(mid_rx) => {
            let (a_in_tx, a_in_rx) = sync_async_channel::<(u64, Prev::Out)>(buffer);
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
        }
        FinalRx::Async(prev_async_rx) => {
            // NOTE(perf): this bridge task is NOT redundant — do not try to
            // remove it by having consumers clone `prev_async_rx` directly.
            //
            // Attempted in the `try(perfopt)` recorded below: replace this
            // arm with `prev_async_rx` (consumers clone it N ways and pull
            // directly, eliminating one tokio task + one bounded channel per
            // item). Measured result on `io_async_pure` (sample-size 30,
            // measurement-time 5, vs the readme_20260627_v3 baseline):
            //
            //   youpipe_async/200  +0.50%  (p = 0.04, flagged noise threshold)
            //   youpipe_async/500  +0.82%  (p = 0.03, flagged noise threshold)
            //
            // Both point estimates were *positive* (regression) — the
            // simplification is consistently slower, not faster. The
            // hypothesis: with the bridge in place the bridge task is the
            // sole registered waker on `prev_async_rx`, so each item the
            // upstream produces wakes exactly one task. Without the bridge,
            // all `concurrency` consumer clones register wakers on the same
            // `MAsyncRx` (`crossfire::mpmc` uses `RegistryMulti`), so a
            // single produced item can spuriously wake several consumers —
            // all but one then poll an empty queue, re-register, and return
            // `Pending`. That extra scheduler churn outweighs the saved
            // channel hop at `io_concurrency ≥ 64`.
            //
            // The bridge is therefore load-bearing: it's a 1-task funnel
            // that converts the MPMC upstream into a single-waker source for
            // the consumer fan-out. Keep it.
            let (a_in_tx, a_in_rx) = async_channel::<(u64, Prev::Out)>(buffer);
            let pool = ctx.acquire_async().expect("failed to build async runtime");
            let _enter = pool.handle().enter();
            tokio::spawn(async move {
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
        }
    };

    let (a_out_tx, a_out_rx) = async_channel::<(u64, M)>(buffer);
    let pool = ctx.acquire_async().expect("failed to build async runtime");
    let _enter = pool.handle().enter();
    let f = Arc::new(f);
    let cancel = ctx.cancel.clone();
    let mut consumers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let f = f.clone();
        let rx = a_in_rx.clone();
        let tx = a_out_tx.clone();
        let c = cancel.clone();
        consumers.push(tokio::spawn(async move {
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
        }));
    }
    drop(a_out_tx);
    drop(a_in_rx);
    // Detach: tasks complete as channels close; we don't need the JoinHandles
    // (output is observed via the channel).
    drop(consumers);

    FinalRx::Async(a_out_rx)
}

// ── StreamPipe builder methods ──

impl<S, I, O> StreamPipe<S, I, O> {
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
    /// Recommended inside tight loops (e.g. criterion benches): tokio runtime
    /// construction costs ~ms, which would otherwise dominate small workloads.
    #[cfg(feature = "tokio-runtime")]
    #[must_use]
    pub fn with_async_pool(mut self, pool: crate::executor::AsyncPool) -> Self {
        self.async_pool = Some(pool);
        self
    }

    /// Append a synchronous CPU stage: `Fn(O) -> N`. Runs on the work-stealing
    /// [`ComputePool`]; the output type changes to `N`.
    pub fn stage<N>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> StreamPipe<SyncStage<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N>
    where
        N: Send + Unpin + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: SyncStage {
                prev: self.stages,
                f,
            },
            config: self.config,
            cancel: self.cancel,
            #[cfg(feature = "tokio-runtime")]
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Append a 1-to-N expansion stage: `Fn(O) -> Vec<N>`. Each input item
    /// produces zero or more outputs (like `flat_map`); expanded items inherit
    /// the parent's sequence tag for ordered collection.
    pub fn expand<N>(
        self,
        f: impl Fn(O) -> Vec<N> + Send + Sync + 'static,
    ) -> StreamPipe<ExpandStage<S, impl Fn(O) -> Vec<N> + Send + Sync + 'static>, I, N>
    where
        N: Send + Unpin + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: ExpandStage {
                prev: self.stages,
                f,
            },
            config: self.config,
            cancel: self.cancel,
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
    /// A fence controls exactly **one** adjacent stage transition — the
    /// boundary between whatever precedes it and whatever follows it. It does
    /// *not* impose a barrier across the entire pipeline. Each `.fence()`
    /// call is an independent boundary, so a chain may insert as many as the
    /// topology needs:
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
    /// This keeps the chain composable: each `.fence()` is local to its
    /// position, never affecting upstream or downstream boundaries.
    ///
    /// # Modes
    ///
    /// - [`FenceMode::Barrier`] fully drains the upstream before downstream
    ///   starts (hard isolation; max peak memory, no staging overlap).
    /// - [`FenceMode::Chunked`] releases batches as soon as they form so the
    ///   two sides overlap — the right default for mixed CPU/IO loads.
    pub fn fence(self, mode: FenceMode) -> StreamPipe<FenceLink<S>, I, O> {
        StreamPipe {
            items: self.items,
            stages: FenceLink {
                prev: self.stages,
                mode,
            },
            config: self.config,
            cancel: self.cancel,
            #[cfg(feature = "tokio-runtime")]
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }

    /// Append an async IO stage: `Fn(O) -> Future<Output = N>`. Runs as
    /// `io_concurrency` tokio tasks on the [`AsyncPool`] — the runtime's M:N
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
    ) -> StreamPipe<AsyncStage<S, impl Fn(O) -> Fut + Send + Sync + 'static>, I, N>
    where
        N: Send + Unpin + 'static,
        Fut: Future<Output = N> + Send + 'static,
    {
        StreamPipe {
            items: self.items,
            stages: AsyncStage {
                prev: self.stages,
                f,
            },
            config: self.config,
            cancel: self.cancel,
            async_pool: self.async_pool,
            ordered: self.ordered,
            _marker: PhantomData,
        }
    }
}

// ── Run (execute the chain) ──

impl<S, I, O> StreamPipe<S, I, O>
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
    pub fn run(self) -> Vec<O> {
        let n = self.items.len();
        if n == 0 {
            return Vec::new();
        }
        let Self {
            items,
            stages,
            config,
            cancel,
            #[cfg(feature = "tokio-runtime")]
            async_pool,
            ordered,
            _marker,
        } = self;

        // Compute the per-stage compute-pool parallelism. Each sync stage
        // claims an equal slice of `compute_workers` so the total across all
        // sync stages fits inside the pool — preventing the
        // "stage 1 fills the pool, stage 2 starves, deadlock" failure mode.
        // Fences and async stages don't consume pool slots.
        let worker_stages = stages.worker_stages().max(1);
        let per_stage_parallelism = (config.compute_workers / worker_stages).max(1);

        let ctx = StreamCtx {
            config: &config,
            cancel,
            n,
            per_stage_parallelism,
            #[cfg(feature = "tokio-runtime")]
            async_pool,
            #[cfg(feature = "tokio-runtime")]
            cached_pool: OnceLock::new(),
        };

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
        // difference is whether the *receiver* end is sync (-> `spawn`) or
        // async (-> `spawn_async_feeder`). The sender side (`SyncSender`) is
        // the same type either way, so [`feed_items`] handles both.
        //
        // Without `tokio-runtime`, `AsyncStage` doesn't exist, so
        // `first_consumer_is_async` can never return `Some(true)` and the
        // async branch is unreachable; the `cfg_not` block keeps the function
        // compilable in that configuration.
        let async_feeder = stages.first_consumer_is_async() == Some(true);
        let feeder_cancel = ctx.cancel.clone();
        debug_assert!(
            cfg!(feature = "tokio-runtime") || !async_feeder,
            "first_consumer_is_async == Some(true) requires the tokio-runtime feature"
        );

        #[cfg(feature = "tokio-runtime")]
        let (final_rx, feeder) = if async_feeder {
            let (feeder_tx, feeder_rx) = sync_async_channel::<(u64, I)>(buffer);
            // Feed items BEFORE spawning stages: preserves the pre-inline
            // ordering where the feeder starts while stages are being
            // submitted. For the inline path, items are already queued when
            // workers start (no wakeup round-trip). For the thread path, the
            // feeder pushes concurrently with stage startup.
            let feeder = feed_items(items, feeder_tx, feeder_cancel, buffer);
            (stages.spawn_async_feeder(feeder_rx, &ctx), feeder)
        } else {
            let (feeder_tx, feeder_rx) = channel::<(u64, I)>(buffer);
            let feeder = feed_items(items, feeder_tx, feeder_cancel, buffer);
            (stages.spawn(feeder_rx, &ctx), feeder)
        };
        #[cfg(not(feature = "tokio-runtime"))]
        let (final_rx, feeder) = {
            let (feeder_tx, feeder_rx) = channel::<(u64, I)>(buffer);
            let feeder = feed_items(items, feeder_tx, feeder_cancel, buffer);
            (stages.spawn(feeder_rx, &ctx), feeder)
        };

        let results = match final_rx {
            FinalRx::Sync(rx) => collect_sync(rx, ordered, n),
            #[cfg(feature = "tokio-runtime")]
            FinalRx::Async(rx) => {
                let pool = ctx.acquire_async().expect("failed to build async runtime");
                pool.block_on(collect_async(rx, ordered, n))
            }
        };

        feeder.join();
        results
    }
}

/// Sync collector: drains `rx` into a `Vec`. If `ordered`, uses a
/// [`ReorderBuffer`] to restore input order.
#[allow(clippy::needless_pass_by_value)] // `rx` is the terminal drain of the
// pipeline: `run` passes the sole receiver by value to express "consume fully".
// The drain loop uses `recv()` by ref, but owning the receiver keeps its
// lifetime bounded to this call so the caller can't accidentally reuse it after
// the run.
fn collect_sync<T: Send + Unpin + 'static>(
    rx: Receiver<(u64, T)>,
    ordered: bool,
    n: usize,
) -> Vec<T> {
    if ordered {
        run_ordered_collect(&rx, n)
    } else {
        let mut results = Vec::with_capacity(n);
        while let Ok((_, item)) = rx.recv() {
            results.push(item);
        }
        results
    }
}

/// Async collector: drains `rx` into a `Vec` via the async runtime. If
/// `ordered`, uses a [`ReorderBuffer`].
///
/// # Burst-drain strategy (unordered path)
///
/// When multiple items land in the channel before the collector loops back
/// (the common case once the first wave of async consumers wakes from their
/// sleeps — tokio's coarse timer wheel batches same-duration timeouts into
/// the same tick), a pure `while let Ok(..) = rx.recv().await` pays one
/// waker-register / waker-wake round-trip per item even though every item
/// after the first is already queued. The unordered path therefore drains
/// in two phases per burst:
///
///   1. Spin `try_recv` until `Empty` — no `await`, no waker registration, just
///      non-blocking pops at ~atomic-op cost.
///   2. When the queue is drained but the channel is still open, `recv().await`
///      exactly once to register a waker and yield until the next item lands.
///      Then loop back to step 1.
///
/// This converts the per-item `await` cost into a per-burst `await` cost.
/// For `io_async_pure` at size 500 (~450 items completing in the same ~1 ms
/// timer tick) the savings is measurable.
#[cfg(feature = "tokio-runtime")]
async fn collect_async<T: Send + Unpin + 'static>(
    rx: AsyncReceiver<(u64, T)>,
    ordered: bool,
    n: usize,
) -> Vec<T> {
    if ordered {
        let capacity = n.next_power_of_two().clamp(1 << 10, 1 << 20);
        let mut buffer = ReorderBuffer::new(capacity);
        let mut results = Vec::with_capacity(n);
        while let Ok((seq, o)) = rx.recv().await {
            results.extend(buffer.insert(seq, o));
        }
        results.extend(buffer.flush_remaining());
        results
    } else {
        let mut results = Vec::with_capacity(n);
        loop {
            // Burst-drain: pop everything already queued without awaiting.
            // `try_recv` is a non-blocking pop; on `Empty` we fall through
            // to the awaited `recv` below.
            loop {
                match rx.try_recv() {
                    Ok((_, item)) => results.push(item),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => return results,
                }
            }
            // Queue is drained but channel may still be open. Await exactly
            // one item to register a waker; the next iteration's burst-drain
            // picks up anything that arrived in the meantime.
            match rx.recv().await {
                Ok((_, item)) => results.push(item),
                Err(_) => return results,
            }
        }
    }
}
