use std::{marker::PhantomData, sync::Arc};

use super::config::{PipelineConfig, Workload};
use crate::{
    executor::compute::ComputePool,
    handoff::{Receiver, Sender, SharedWaitGroup, channel::channel},
    state::{FenceBarrier, FenceMode, run_ordered_collect},
    sync::CancellationToken,
};

// ── Join-based parallel helpers ──

/// Compute the number of recursive split levels. Aiming for ~4 tasks per
/// thread gives good work-stealing without excessive task overhead.
fn split_depth(n: usize, num_threads: usize) -> usize {
    let desired_tasks = (num_threads * 4).max(1);
    let by_threads = desired_tasks.next_power_of_two().trailing_zeros() as usize;
    let by_len = n.max(1).next_power_of_two().trailing_zeros() as usize;
    by_threads.min(by_len).max(1)
}

/// Recursive join-based map. Splits `items` in half at each level until
/// `splits_left` hits 0, then processes each leaf sequentially. Results are
/// returned in input order.
///
/// The recursive split is the key to handling unbalanced workloads: if one
/// half is expensive, the thread processing the cheap half finishes and steals
/// sub-tasks from the expensive half's deque.
fn join_map<T, R, F>(mut items: Vec<T>, f: &F, splits_left: usize) -> Vec<R>
where
    T: Send,
    R: Send,
    F: Fn(T) -> R + Sync,
{
    if splits_left == 0 || items.len() <= 1 {
        return items.into_iter().map(|x| f(x)).collect();
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_map(items, f, splits_left - 1),
        || join_map(right, f, splits_left - 1),
    );
    let mut result = left_r;
    result.extend(right_r);
    result
}

/// Fallible recursive join-based map. Returns the first error encountered.
fn join_try_map<T, R, E, F>(mut items: Vec<T>, f: &F, splits_left: usize) -> Result<Vec<R>, E>
where
    T: Send,
    R: Send,
    E: Send,
    F: Fn(T) -> Result<R, E> + Sync,
{
    if splits_left == 0 || items.len() <= 1 {
        return items.into_iter().map(|x| f(x)).collect();
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_try_map(items, f, splits_left - 1),
        || join_try_map(right, f, splits_left - 1),
    );
    match (left_r, right_r) {
        (Ok(mut l), Ok(r)) => {
            l.extend(r);
            Ok(l)
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// Parallel map over an iterator. Uses [`Workload::Balanced`] by default.
///
/// Internally uses recursive `join`-based splitting (like rayon) which enables
/// work-stealing of sub-tasks. This handles both balanced and skewed workloads
/// well without per-item atomics.
pub fn par_map<I, F, R>(iter: I, f: F) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> R + Send + Sync + Clone + 'static,
    R: Send + 'static,
{
    par_map_with_workload(iter, f, Workload::Balanced)
}

/// Parallel map with explicit [`Workload`] hint.
///
/// Uses recursive `join`-based splitting for both variants. `Unbalanced`
/// creates more split points (finer granularity) to improve work-stealing
/// when per-item cost varies widely.
pub fn par_map_with_workload<I, F, R>(iter: I, f: F, workload: Workload) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> R + Send + Sync + Clone + 'static,
    R: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 {
        return Vec::new();
    }
    let num_threads = ComputePool::global().num_workers();
    if n <= 1 || num_threads <= 1 {
        return items.into_iter().map(f).collect();
    }

    let splits = match workload {
        Workload::Balanced => split_depth(n, num_threads),
        Workload::Unbalanced => split_depth(n, num_threads * 2),
    };

    join_map(items, &f, splits)
}

/// Parallel chunked map — splits items into `chunk_size` slices and calls `f`
/// on each slice, collecting all results.
pub fn par_chunks_map<I, F, R>(iter: I, chunk_size: usize, f: F) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(&[I::Item]) -> Vec<R> + Send + Sync + Clone + 'static,
    R: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 || chunk_size == 0 {
        return Vec::new();
    }

    let num_threads = ComputePool::global().num_workers();
    let num_chunks = n.div_ceil(chunk_size);
    if num_chunks <= 1 || num_threads <= 1 {
        return items.chunks(chunk_size).flat_map(&f).collect();
    }

    let splits = split_depth(num_chunks, num_threads);
    join_chunks_map(items, chunk_size, &f, splits)
}

/// Recursive join-based chunked map.
fn join_chunks_map<T, R, F>(mut items: Vec<T>, chunk_size: usize, f: &F, splits_left: usize) -> Vec<R>
where
    T: Send,
    R: Send,
    F: Fn(&[T]) -> Vec<R> + Sync,
{
    if splits_left == 0 || items.len() <= chunk_size {
        return items.chunks(chunk_size).flat_map(|c| f(c)).collect();
    }
    // Split at a chunk boundary.
    let mid = ((items.len() / 2) / chunk_size) * chunk_size;
    let mid = mid.max(chunk_size);
    let right = items.split_off(mid);

    let (left_r, right_r) = ComputePool::global().join(
        || join_chunks_map(items, chunk_size, f, splits_left - 1),
        || join_chunks_map(right, chunk_size, f, splits_left - 1),
    );
    let mut result = left_r;
    result.extend(right_r);
    result
}

/// Fallible parallel map. Both branches always execute (join guarantee); the
/// first error is returned.
pub fn try_par_map<I, F, R, E>(iter: I, f: F) -> Result<Vec<R>, E>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> Result<R, E> + Send + Sync + Clone + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let num_threads = ComputePool::global().num_workers();
    if n <= 1 || num_threads <= 1 {
        return items.into_iter().map(f).collect();
    }

    let splits = split_depth(n, num_threads);
    join_try_map(items, &f, splits)
}

// ── Marker traits ──

/// Type-level marker for a pipeline stage. Maps `Input` to `Self::Output`.
pub trait StageMarker<Input> {
    type Output;
}

/// Identity stage — passes items through unchanged.
#[derive(Clone)]
pub struct Identity;

impl<T> StageMarker<T> for Identity {
    type Output = T;
}

/// Synchronous map stage: `Fn(T) -> O`.
#[derive(Clone)]
pub struct SyncMap<Prev, F> {
    prev: Prev,
    f: F,
}

impl<Prev, F, I, O> StageMarker<I> for SyncMap<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
}

/// Filter stage: keeps items where `Fn(&T) -> bool` returns `true`.
#[derive(Clone)]
pub struct Filter<Prev, F> {
    prev: Prev,
    f: F,
}

impl<Prev, F, I> StageMarker<I> for Filter<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
}

/// Barrier / fence stage. Forces a materialization boundary in the streaming
/// pipeline.
#[derive(Clone)]
pub struct Fence<Prev> {
    prev: Prev,
    #[allow(dead_code)]
    chunk_size: Option<usize>,
}

impl<Prev, I> StageMarker<I> for Fence<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

/// Ordered output stage. Preserves input ordering in the final collection.
#[derive(Clone)]
pub struct Ordered<Prev> {
    prev: Prev,
}

impl<Prev, I> StageMarker<I> for Ordered<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

// ── FusedStage trait ──

/// Compile-time fused stage: applies multiple pipeline stages in a single pass
/// without intermediate allocations.
pub trait FusedStage<T> {
    type Output;
    fn apply(&self, item: T) -> Option<Self::Output>;
}

impl<T> FusedStage<T> for Identity {
    type Output = T;
    fn apply(&self, item: T) -> Option<T> {
        Some(item)
    }
}

impl<Prev, F, I, O> FusedStage<I> for SyncMap<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
    fn apply(&self, item: I) -> Option<O> {
        self.prev.apply(item).map(|v| (self.f)(v))
    }
}

impl<Prev, F, I> FusedStage<I> for Filter<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item).filter(|v| (self.f)(v))
    }
}

impl<Prev, I> FusedStage<I> for Fence<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
}

impl<Prev, I> FusedStage<I> for Ordered<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
}

// ── Pipeline (main user-facing type) ──

/// A type-state pipeline builder. Stages are fused at compile time into a
/// single pass over the data when possible (no `fence` / `ordered` boundaries).
///
/// Use [`Pipeline::from_vec`] to start, chain `.map()` / `.filter()` calls,
/// then call `.collect(items)` to execute.
pub struct Pipeline<S = Identity, T = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<T>,
}

impl<T: Send + 'static> Pipeline<Identity, T> {
    /// Create a new pipeline (type-state entry point).
    #[must_use]
    pub fn from_vec(_items: Vec<T>) -> Self {
        Self {
            stages: Identity,
            config: PipelineConfig::default(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> Pipeline<S, T> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Append a synchronous map stage.
    pub fn map<O: Send + 'static>(
        self,
        f: impl Fn(T) -> O + Send + Sync + Clone + 'static,
    ) -> Pipeline<SyncMap<S, impl Fn(T) -> O + Send + Sync + Clone + 'static>, O>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage. Keeps items where `f` returns `true`.
    pub fn filter(
        self,
        f: impl Fn(&T) -> bool + Send + Sync + Clone + 'static,
    ) -> Pipeline<Filter<S, impl Fn(&T) -> bool + Send + Sync + Clone + 'static>, T>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a fence (materialization barrier).
    pub fn fence(self) -> Pipeline<Fence<S>, T>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: Fence {
                prev: self.stages,
                chunk_size: None,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a chunked fence with the given chunk size.
    pub fn fence_chunked(self, chunk_size: usize) -> Pipeline<Fence<S>, T>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: Fence {
                prev: self.stages,
                chunk_size: Some(chunk_size),
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Mark the output as order-preserving.
    pub fn ordered(self) -> Pipeline<Ordered<S>, T>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: Ordered { prev: self.stages },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

// ── Collect for fully-fused sync pipelines ──

impl<S, T> Pipeline<S, T>
where
    S: FusedStage<T> + Send + Sync + Clone + 'static,
    T: Send + 'static,
    S::Output: Send + 'static,
{
    /// Execute the fused pipeline over `items` and collect results.
    ///
    /// Uses recursive `join`-based splitting for both balanced and unbalanced
    /// workloads. The recursive split enables work-stealing of sub-tasks.
    pub fn collect<I: IntoIterator<Item = T>>(self, items: I) -> Vec<S::Output> {
        let items: Vec<T> = items.into_iter().collect();
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let num_threads = ComputePool::global().num_workers();
        if n <= 1 || num_threads <= 1 {
            return items
                .into_iter()
                .filter_map(|item| self.stages.apply(item))
                .collect();
        }

        let splits = match self.config.workload {
            Workload::Balanced => split_depth(n, num_threads),
            Workload::Unbalanced => split_depth(n, num_threads * 2),
        };

        join_fused_collect(items, &self.stages, splits)
    }
}

/// Recursive join-based collect for fused pipeline stages.
fn join_fused_collect<S, T>(mut items: Vec<T>, stages: &S, splits_left: usize) -> Vec<S::Output>
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
{
    if splits_left == 0 || items.len() <= 1 {
        return items
            .into_iter()
            .filter_map(|item| stages.apply(item))
            .collect();
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_fused_collect(items, stages, splits_left - 1),
        || join_fused_collect(right, stages, splits_left - 1),
    );
    let mut result = left_r;
    result.extend(right_r);
    result
}

// ── Streaming pipeline (for stages that break fusion: async, fence, ordered)
// ──

/// Streaming pipeline for workloads that cannot be fused at compile time
/// (async stages, fences, ordered output, multi-stage).
///
/// Data flows through channels between feeder → workers → collector.
pub struct StreamPipeline {
    config: PipelineConfig,
    cancel: Option<CancellationToken>,
}

// ── Streaming stage helpers (used by the fence pipeline) ──

/// Spawn `parallelism` workers on `pool` that pull from `rx`, apply `stage`,
/// and forward to `tx`. Each worker loops until its receiver disconnects.
///
/// The supplied `rx`/`tx` are consumed: clones are handed to workers and the
/// originals dropped here, so channel close propagates correctly once all
/// workers exit (no external `WaitGroup` needed for completion signalling).
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// taking the endpoints by value ensures the caller cannot retain a clone that
// would keep the channel open after the workers have finished.
fn spawn_stage<I, O>(
    pool: &ComputePool,
    rx: Receiver<I>,
    tx: Sender<O>,
    parallelism: usize,
    stage: impl Fn(I) -> O + Send + Sync + 'static,
) where
    I: Send + 'static,
    O: Send + 'static,
{
    let stage = Arc::new(stage);
    for _ in 0..parallelism {
        let stage = stage.clone();
        let rx = rx.clone();
        let tx = tx.clone();
        pool.submit(move || {
            while let Ok(item) = rx.recv() {
                if tx.send(stage(item)).is_err() {
                    break;
                }
            }
        });
    }
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
// the endpoints must be owned so they are dropped (closing the channels) when
// forwarding completes.
fn forward_fenced<M>(mid_rx: Receiver<M>, fenced_tx: Sender<M>, mode: FenceMode)
where
    M: Send + 'static,
{
    let mut fence = FenceBarrier::<M>::new(mode);
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

impl StreamPipeline {
    /// Create a new streaming pipeline with the given config.
    #[must_use]
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            cancel: None,
        }
    }

    /// Attach a [`CancellationToken`] for cooperative cancellation.
    #[must_use]
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Run a single stage over `items`. If `ordered` is true, output preserves
    /// input order.
    pub fn run<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + Clone + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_ordered(items, stage)
        } else {
            self.run_unordered(items, stage)
        }
    }

    fn run_ordered<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);
        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder_handle = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if feeder_cancel
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let stage = Arc::new(stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let stage = stage.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if worker_cancel
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        break;
                    }
                    let output = stage(item);
                    if tx.send((seq, output)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let collector_handle = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder_handle.join().unwrap();
        wg.wait();
        collector_handle.join().unwrap()
    }

    fn run_unordered<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);
        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder_handle = std::thread::spawn(move || {
            for item in items {
                if feeder_cancel
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let stage = Arc::new(stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let stage = stage.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if worker_cancel
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        break;
                    }
                    let output = stage(item);
                    if tx.send(output).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder_handle.join().unwrap();
        wg.wait();
        results
    }

    /// Run two stages in sequence (stage1 → stage2).
    pub fn run_multi_stage<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_multi_stage_ordered(items, stage1, stage2)
        } else {
            self.run_multi_stage_unordered(items, stage1, stage2)
        }
    }

    fn run_multi_stage_ordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers;
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        let par1 = parallelism / 2;
        let par2 = parallelism - par1;
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    let out = s(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        for _ in 0..par2 {
            let s = s2.clone();
            let rx = mid_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    let out = s(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        drop(in_rx);
        drop(mid_tx);
        drop(mid_rx);
        drop(out_tx);

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        wg.wait();
        collector.join().unwrap()
    }

    fn run_multi_stage_unordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers;
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for item in items {
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        let par1 = parallelism / 2;
        let par2 = parallelism - par1;
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    let out = s(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        for _ in 0..par2 {
            let s = s2.clone();
            let rx = mid_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    let out = s(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        drop(in_rx);
        drop(mid_tx);
        drop(mid_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder.join().unwrap();
        wg.wait();
        results
    }

    /// Run two stages separated by a fence whose isolation strength is
    /// controlled by `mode`.
    ///
    /// [`FenceMode::Barrier`] fully drains stage 1 before stage 2 sees any
    /// item (hard isolation). [`FenceMode::Chunked`] releases batches as they
    /// form so the stages overlap — the right default for mixed CPU/IO loads.
    pub fn run_with_fence<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
        mode: FenceMode,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_with_fence_ordered(items, stage1, stage2, mode)
        } else {
            self.run_with_fence_unordered(items, stage1, stage2, mode)
        }
    }

    fn run_with_fence_ordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
        mode: FenceMode,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        // Split workers across the two stages so the total count of blocking
        // pool jobs never exceeds the pool size. Submitting `parallelism`
        // workers per stage (2× oversubscription) starves the pool: stage 2
        // jobs can't get scheduled while stage 1 jobs block, deadlocking the
        // pipeline. This mirrors `run_multi_stage`'s invariant.
        let parallelism = self.config.compute_workers.min(n);
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        // Thread seq through the tuple so input order survives the fence.
        spawn_stage(
            pool,
            in_rx,
            mid_tx,
            par1,
            move |(seq, item): (u64, I)| (seq, stage1(item)),
        );

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(
            pool,
            fenced_rx,
            out_tx,
            par2,
            move |(seq, mid): (u64, M)| (seq, stage2(mid)),
        );

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        fence_thread.join().unwrap();
        collector.join().unwrap()
    }

    fn run_with_fence_unordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + Clone + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + Clone + 'static,
        mode: FenceMode,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        // See `run_with_fence_ordered`: keep total blocking jobs ≤ pool size.
        let parallelism = self.config.compute_workers.min(n);
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for item in items {
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        spawn_stage(pool, in_rx, mid_tx, par1, stage1);

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(pool, fenced_rx, out_tx, par2, stage2);

        let collector = std::thread::spawn(move || {
            let mut results = Vec::with_capacity(n);
            while let Ok(item) = out_rx.recv() {
                results.push(item);
            }
            results
        });

        feeder.join().unwrap();
        fence_thread.join().unwrap();
        collector.join().unwrap()
    }
}

impl StreamPipeline {
    /// Run a nested (1-to-N) pipeline: `outer_stage` expands each item,
    /// `inner_stage` maps each expanded item.
    pub fn run_nested<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + Clone + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + Clone + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_nested_ordered(items, outer_stage, inner_stage)
        } else {
            self.run_nested_unordered(items, outer_stage, inner_stage)
        }
    }

    fn run_nested_ordered<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + Clone + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        let expanded: Vec<(u64, N)> = items
            .into_iter()
            .enumerate()
            .flat_map(|(seq, item)| {
                let nested = outer_stage(item);
                nested.into_iter().map(move |n| (seq as u64, n))
            })
            .collect();

        let parallelism = self.config.compute_workers;
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;
        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }

        let (in_tx, in_rx) = channel::<(u64, N)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        let effective = parallelism.min(n);
        wg.add(effective);
        for _ in 0..effective {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    let out = inner(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        wg.wait();
        collector.join().unwrap()
    }

    fn run_nested_unordered<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + Clone + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + Clone + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        let expanded: Vec<N> = items.into_iter().flat_map(outer_stage).collect();

        let parallelism = self.config.compute_workers;
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size;
        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }

        let (in_tx, in_rx) = channel::<N>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        let effective = parallelism.min(n);
        wg.add(effective);
        for _ in 0..effective {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    let out = inner(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder.join().unwrap();
        wg.wait();
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fused_sync_collect() {
        let items: Vec<i32> = (0..100).collect();
        let result = Pipeline::from_vec(items.clone())
            .map(|x: i32| x * 2)
            .map(|x: i32| x + 1)
            .collect(items);
        let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, expected);
    }

    #[test]
    fn test_fused_filter() {
        let items: Vec<i32> = (0..20).collect();
        let result = Pipeline::from_vec(items.clone())
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .collect(items);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, vec![0, 20, 40, 60, 80, 100, 120, 140, 160, 180]);
    }

    #[test]
    fn test_empty_input() {
        let items: Vec<i32> = vec![];
        let result = Pipeline::from_vec(items.clone())
            .map(|x: i32| x * 2)
            .collect(items);
        assert!(result.is_empty());
    }

    #[test]
    fn test_par_map() {
        let items: Vec<i32> = (0..100).collect();
        let result = par_map(items, |x: i32| x * 3);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_single_stage_unordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run(items, |x: i32| x * 2, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_single_stage_ordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let result = sp.run(items, |x: i32| x * 2, true);
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_multi_stage() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run_multi_stage(items, |x: i32| x + 1, |x: i32| x * 3, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| (x + 1) * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_ok() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x * 3) });
        let mut r = result.unwrap();
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_err() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, String> {
            if x == 50 {
                Err(format!("bad: {x}"))
            } else {
                Ok(x * 2)
            }
        });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "bad: 50");
    }

    #[test]
    fn test_try_par_map_empty() {
        let items: Vec<i32> = vec![];
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x) });
        assert_eq!(result.unwrap(), Vec::<i32>::new());
    }

    #[test]
    fn test_stream_cancel_unordered() {
        let token = crate::sync::CancellationToken::new();
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config).with_cancel(token.clone());
        let items: Vec<i32> = (0..1000).collect();
        token.cancel();
        let result = sp.run(
            items,
            |x: i32| -> i32 {
                std::thread::sleep(std::time::Duration::from_micros(100));
                x * 2
            },
            false,
        );
        assert!(result.len() < 1000);
    }

    #[test]
    fn test_stream_no_cancel() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..50).collect();
        let result = sp.run(items, |x: i32| x * 2, false);
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn test_stream_nested() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..5).collect();
        let mut result = sp.run_nested(items, |x: i32| vec![x, x + 100], |x: i32| x * 2, false);
        result.sort_unstable();
        let mut expected: Vec<i32> = (0..5).flat_map(|x| vec![x * 2, (x + 100) * 2]).collect();
        expected.sort_unstable();
        assert_eq!(result, expected);
    }
}
