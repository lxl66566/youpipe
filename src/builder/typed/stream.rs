#[cfg(feature = "tokio-runtime")]
use std::future::Future;
use std::sync::Arc;

use crate::{
    builder::config::PipelineConfig,
    executor::compute::ComputePool,
    handoff::{Receiver, Sender, SharedWaitGroup, channel::channel},
    state::{FenceBarrier, FenceMode, run_ordered_collect},
    sync::CancellationToken,
};
#[cfg(feature = "tokio-runtime")]
use crate::{
    executor::AsyncPool,
    handoff::{async_channel, channel::TrySendError},
    state::ReorderBuffer,
};

// ── Streaming pipeline (for stages that break fusion: async, fence, ordered)
// ──

/// Streaming pipeline for workloads that cannot be fused at compile time
/// (async stages, fences, ordered output, multi-stage).
///
/// Data flows through channels between feeder → workers → collector.
pub struct StreamPipeline {
    config: PipelineConfig,
    cancel: Option<CancellationToken>,
    /// Optional externally-managed async runtime. When `None`, async methods
    /// build a transient runtime per call (simpler, but pays runtime creation
    /// on every invocation); when supplied via [`Self::with_async_pool`], the
    /// runtime is reused across runs (recommended for tight loops / benches).
    #[cfg(feature = "tokio-runtime")]
    async_pool: Option<crate::executor::AsyncPool>,
}

// ── Streaming stage helpers (used by the fence pipeline) ──

/// True iff `cancel` is set and the pipeline should stop feeding new work.
#[inline]
fn cancel_active(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(CancellationToken::is_cancelled)
}

/// Spawn `parallelism` workers on `pool` that pull from `rx`, apply `stage`,
/// and forward to `tx`. Each worker loops until its receiver disconnects or the
/// supplied cancellation token (if any) is signalled.
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
    cancel: Option<CancellationToken>,
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
        let cancel = cancel.clone();
        pool.submit(move || {
            while let Ok(item) = rx.recv() {
                if cancel_active(cancel.as_ref()) {
                    break;
                }
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
            #[cfg(feature = "tokio-runtime")]
            async_pool: None,
        }
    }

    /// Attach a [`CancellationToken`] for cooperative cancellation.
    #[must_use]
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Attach a managed async runtime ([`AsyncPool`]) so async methods
    /// ([`Self::run_async`], [`Self::run_mixed_async`]) reuse it across runs
    /// instead of building a transient runtime per call.
    ///
    /// Recommended inside tight loops (e.g. criterion benches): tokio runtime
    /// construction costs ~ms, which would otherwise dominate small workloads.
    #[cfg(feature = "tokio-runtime")]
    #[must_use]
    pub fn with_async_pool(mut self, pool: crate::executor::AsyncPool) -> Self {
        self.async_pool = Some(pool);
        self
    }

    /// Obtain an [`AsyncPool`] for this run: a handle-only wrapper around the
    /// attached runtime (if any), otherwise a fresh owning runtime built from
    /// `config.async_workers`.
    #[cfg(feature = "tokio-runtime")]
    fn acquire_async(&self) -> std::io::Result<crate::executor::AsyncPool> {
        match &self.async_pool {
            Some(p) => Ok(crate::executor::AsyncPool::new(
                p.handle().clone(),
                self.config.async_workers,
            )),
            None => crate::executor::AsyncPool::from_global(self.config.async_workers),
        }
    }

    /// Run a single stage over `items`. If `ordered` is true, output preserves
    /// input order.
    pub fn run<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + 'static,
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
        stage: impl Fn(I) -> O + Send + Sync + 'static,
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
                if cancel_active(feeder_cancel.as_ref()) {
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
                    if cancel_active(worker_cancel.as_ref()) {
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
        stage: impl Fn(I) -> O + Send + Sync + 'static,
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
                if cancel_active(feeder_cancel.as_ref()) {
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
                    if cancel_active(worker_cancel.as_ref()) {
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        // Floor each stage at one worker so a small `compute_workers` (e.g. a
        // single-threaded pool) still runs both stages instead of silently
        // starving one of them.
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        // Floor each stage at one worker so a small `compute_workers` (e.g. a
        // single-threaded pool) still runs both stages instead of silently
        // starving one of them.
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
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
            self.cancel.clone(),
            move |(seq, item): (u64, I)| (seq, stage1(item)),
        );

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(
            pool,
            fenced_rx,
            out_tx,
            par2,
            self.cancel.clone(),
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
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
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
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        spawn_stage(pool, in_rx, mid_tx, par1, self.cancel.clone(), stage1);

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(pool, fenced_rx, out_tx, par2, self.cancel.clone(), stage2);

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
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
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
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
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

        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);

        let (in_tx, in_rx) = channel::<(u64, N)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        let expanded: Vec<N> = items.into_iter().flat_map(outer_stage).collect();

        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);

        let (in_tx, in_rx) = channel::<N>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
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

// ── Async streaming stages (mixed sync+async) ──
//
// Unlike the pure-sync streaming methods above, these run an IO stage as
// `async` tasks on a dedicated async runtime (`AsyncPool`). The runtime's M:N
// scheduler multiplexes `io_concurrency` concurrent IO tasks over
// `async_workers` OS threads: each task yields its thread back to the runtime
// while it awaits (e.g. `tokio::time::sleep`, real network/disk IO), so
// concurrency is bounded by `io_concurrency`, NOT by the thread count. For
// truly async IO this beats the blocking-thread-per-core model, which can only
// run as many concurrent waits as it has OS threads.
//
// Availability is gated behind `tokio-runtime` because the async stage needs a
// reactor; the sync streaming API remains runtime-agnostic.

#[cfg(feature = "tokio-runtime")]
impl StreamPipeline {
    /// Run a single **async** stage over `items` with high concurrency.
    ///
    /// `stage` returns a [`Future`]; each item becomes a task on the async
    /// runtime. With [`PipelineConfig::io_concurrency`] ≫ cores this achieves
    /// M:N concurrency for yielded (async) waits — the right choice for
    /// IO-bound work whose waits actually yield (network/disk IO,
    /// `tokio::time::sleep`).
    ///
    /// For work that *blocks* the OS thread (e.g. `std::thread::sleep`), prefer
    /// the sync [`Self::run`]: a blocking call inside a task stalls a runtime
    /// worker and forfeits the M:N advantage.
    pub fn run_async<I, O, F, Fut>(&self, items: Vec<I>, stage: F, ordered: bool) -> Vec<O>
    where
        I: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let pool = self.acquire_async().expect("failed to build async runtime");
        let concurrency = self.config.io_concurrency.max(1).min(n);
        let buffer_size = self.config.buffer_size.max(concurrency * 2);
        if ordered {
            self.run_async_ordered(&pool, items, stage, concurrency, buffer_size)
        } else {
            self.run_async_unordered(&pool, items, stage, concurrency, buffer_size)
        }
    }

    fn run_async_unordered<I, O, F, Fut>(
        &self,
        pool: &AsyncPool,
        items: Vec<I>,
        stage: F,
        concurrency: usize,
        buffer_size: usize,
    ) -> Vec<O>
    where
        I: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let n = items.len();
        let (in_tx, in_rx) = async_channel::<I>(buffer_size);
        let (out_tx, out_rx) = async_channel::<O>(buffer_size);
        let cancel = self.cancel.clone();
        let stage = Arc::new(stage);

        pool.block_on(async move {
            // Feeder: stream items in, keeping the input channel open via a
            // cloned sender so consumers see Close only after the feeder ends.
            let feeder_cancel = cancel.clone();
            let feeder_tx = in_tx.clone();
            let feeder = tokio::spawn(async move {
                for item in items {
                    if cancel_active(feeder_cancel.as_ref()) {
                        break;
                    }
                    if feeder_tx.send(item).await.is_err() {
                        break;
                    }
                }
            });
            drop(in_tx);

            let mut consumers = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let stage = stage.clone();
                let rx = in_rx.clone();
                let tx = out_tx.clone();
                let c = cancel.clone();
                consumers.push(tokio::spawn(async move {
                    loop {
                        let Ok(item) = rx.recv().await else { break };
                        if cancel_active(c.as_ref()) {
                            break;
                        }
                        let out = stage(item).await;
                        if tx.send(out).await.is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(out_tx);
            drop(in_rx);

            // Collect CONCURRENTLY with feeder + consumers: the main future is
            // polled on the calling thread while spawned tasks run on runtime
            // workers. Awaiting consumers before draining would deadlock once
            // `out` fills (consumers block on `send`, no drainer yet).
            let mut results = Vec::with_capacity(n);
            while let Ok(o) = out_rx.recv().await {
                results.push(o);
            }
            // `out` closed ⇒ every consumer has exited; join for panic surfacing.
            let _ = feeder.await;
            for h in consumers {
                let _ = h.await;
            }
            results
        })
    }

    fn run_async_ordered<I, O, F, Fut>(
        &self,
        pool: &AsyncPool,
        items: Vec<I>,
        stage: F,
        concurrency: usize,
        buffer_size: usize,
    ) -> Vec<O>
    where
        I: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let n = items.len();
        let (in_tx, in_rx) = async_channel::<(u64, I)>(buffer_size);
        let (out_tx, out_rx) = async_channel::<(u64, O)>(buffer_size);
        let cancel = self.cancel.clone();
        let stage = Arc::new(stage);

        pool.block_on(async move {
            let feeder_cancel = cancel.clone();
            let feeder_tx = in_tx.clone();
            let feeder = tokio::spawn(async move {
                for (seq, item) in items.into_iter().enumerate() {
                    if cancel_active(feeder_cancel.as_ref()) {
                        break;
                    }
                    if feeder_tx.send((seq as u64, item)).await.is_err() {
                        break;
                    }
                }
            });
            drop(in_tx);

            let mut consumers = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let stage = stage.clone();
                let rx = in_rx.clone();
                let tx = out_tx.clone();
                let c = cancel.clone();
                consumers.push(tokio::spawn(async move {
                    loop {
                        let Ok((seq, item)) = rx.recv().await else {
                            break;
                        };
                        if cancel_active(c.as_ref()) {
                            break;
                        }
                        let out = stage(item).await;
                        if tx.send((seq, out)).await.is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(out_tx);
            drop(in_rx);

            // Collect + reorder CONCURRENTLY with feeder + consumers (see
            // `run_async_unordered` for the deadlock rationale).
            let capacity = n.next_power_of_two().clamp(1 << 10, 1 << 20);
            let mut buffer = ReorderBuffer::new(capacity);
            let mut results = Vec::with_capacity(n);
            while let Ok((seq, o)) = out_rx.recv().await {
                results.extend(buffer.insert(seq, o));
            }
            results.extend(buffer.flush_remaining());
            let _ = feeder.await;
            for h in consumers {
                let _ = h.await;
            }
            results
        })
    }

    /// Run a **mixed sync+async** pipeline: a synchronous CPU stage on the
    /// compute pool feeds an async IO stage on the async runtime.
    ///
    /// `cpu_stage` (`Fn(I) -> M`) runs on [`ComputePool`] workers; `io_stage`
    /// (`Fn(M) -> Future<Output = O>`) runs as `io_concurrency` async tasks.
    /// The two stages overlap — the async IO side starts consuming as soon as
    /// the first CPU result arrives — so a CPU-bound stage and an IO-bound
    /// stage progress in parallel rather than back-to-back.
    pub fn run_mixed_async<I, M, O, CF, AF, Fut>(
        &self,
        items: Vec<I>,
        cpu_stage: CF,
        io_stage: AF,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        CF: Fn(I) -> M + Send + Sync + 'static,
        AF: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let pool = self.acquire_async().expect("failed to build async runtime");
        let parallelism = self.config.compute_workers.min(n);
        let io_concurrency = self.config.io_concurrency.max(1).min(n);
        let buffer_size = self
            .config
            .buffer_size
            .max(parallelism.max(io_concurrency) * 2);
        if ordered {
            self.run_mixed_async_ordered(
                &pool,
                items,
                cpu_stage,
                io_stage,
                parallelism,
                io_concurrency,
                buffer_size,
            )
        } else {
            self.run_mixed_async_unordered(
                &pool,
                items,
                cpu_stage,
                io_stage,
                parallelism,
                io_concurrency,
                buffer_size,
            )
        }
    }

    #[allow(clippy::too_many_arguments)] // internal helper: per-stage tuning knobs
    fn run_mixed_async_unordered<I, M, O, CF, AF, Fut>(
        &self,
        pool: &AsyncPool,
        items: Vec<I>,
        cpu_stage: CF,
        io_stage: AF,
        parallelism: usize,
        io_concurrency: usize,
        buffer_size: usize,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        CF: Fn(I) -> M + Send + Sync + 'static,
        AF: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let total = items.len();
        // CPU stage (sync) channels + bridge into the async IO channels.
        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (a_in_tx, a_in_rx) = async_channel::<M>(buffer_size);
        let (a_out_tx, a_out_rx) = async_channel::<O>(buffer_size);
        let compute = ComputePool::global();

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let cpu = Arc::new(cpu_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let cpu = cpu.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let c = self.cancel.clone();
            compute.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(c.as_ref()) {
                        break;
                    }
                    let m = cpu(item);
                    if tx.send(m).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(mid_tx);

        // Bridge: sync `mid_rx` → async `a_in_tx`. A dedicated thread isolates
        // the backpressure spin (try_send on a full async channel) from the
        // compute pool, mirroring the existing fence-forwarder thread pattern.
        let bridge_cancel = self.cancel.clone();
        let bridge = std::thread::spawn(move || {
            while let Ok(mut m) = mid_rx.recv() {
                loop {
                    match a_in_tx.try_send(m) {
                        Ok(()) => break,
                        Err(TrySendError::Full(ret)) => {
                            if cancel_active(bridge_cancel.as_ref()) {
                                return;
                            }
                            m = ret;
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => return,
                    }
                }
            }
        });

        let io_cancel = self.cancel.clone();
        let results = pool.block_on(async move {
            let io_stage = Arc::new(io_stage);
            let mut consumers = Vec::with_capacity(io_concurrency);
            for _ in 0..io_concurrency {
                let stage = io_stage.clone();
                let rx = a_in_rx.clone();
                let tx = a_out_tx.clone();
                let cancel = io_cancel.clone();
                consumers.push(tokio::spawn(async move {
                    loop {
                        let Ok(item) = rx.recv().await else { break };
                        if cancel_active(cancel.as_ref()) {
                            break;
                        }
                        let out = stage(item).await;
                        if tx.send(out).await.is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(a_out_tx);
            drop(a_in_rx);
            // Drain concurrently with the IO consumers (see run_async_unordered).
            let mut results = Vec::with_capacity(total);
            while let Ok(o) = a_out_rx.recv().await {
                results.push(o);
            }
            for h in consumers {
                let _ = h.await;
            }
            results
        });

        feeder.join().unwrap();
        bridge.join().unwrap();
        results
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // internal helper
    fn run_mixed_async_ordered<I, M, O, CF, AF, Fut>(
        &self,
        pool: &AsyncPool,
        items: Vec<I>,
        cpu_stage: CF,
        io_stage: AF,
        parallelism: usize,
        io_concurrency: usize,
        buffer_size: usize,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + Unpin + 'static,
        O: Send + Unpin + 'static,
        CF: Fn(I) -> M + Send + Sync + 'static,
        AF: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        let total = items.len();
        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (a_in_tx, a_in_rx) = async_channel::<(u64, M)>(buffer_size);
        let (a_out_tx, a_out_rx) = async_channel::<(u64, O)>(buffer_size);
        let compute = ComputePool::global();

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let cpu = Arc::new(cpu_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let cpu = cpu.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let c = self.cancel.clone();
            compute.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(c.as_ref()) {
                        break;
                    }
                    let m = cpu(item);
                    if tx.send((seq, m)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(mid_tx);

        let bridge_cancel = self.cancel.clone();
        let bridge = std::thread::spawn(move || {
            while let Ok(mut m) = mid_rx.recv() {
                loop {
                    match a_in_tx.try_send(m) {
                        Ok(()) => break,
                        Err(TrySendError::Full(ret)) => {
                            if cancel_active(bridge_cancel.as_ref()) {
                                return;
                            }
                            m = ret;
                            std::thread::yield_now();
                        }
                        Err(TrySendError::Closed(_)) => return,
                    }
                }
            }
        });

        let io_cancel = self.cancel.clone();
        let results = pool.block_on(async move {
            let io_stage = Arc::new(io_stage);
            let mut consumers = Vec::with_capacity(io_concurrency);
            for _ in 0..io_concurrency {
                let stage = io_stage.clone();
                let rx = a_in_rx.clone();
                let tx = a_out_tx.clone();
                let cancel = io_cancel.clone();
                consumers.push(tokio::spawn(async move {
                    loop {
                        let Ok((seq, item)) = rx.recv().await else {
                            break;
                        };
                        if cancel_active(cancel.as_ref()) {
                            break;
                        }
                        let out = stage(item).await;
                        if tx.send((seq, out)).await.is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(a_out_tx);
            drop(a_in_rx);
            // Drain + reorder concurrently with the IO consumers.
            let capacity = total.next_power_of_two().clamp(1 << 10, 1 << 20);
            let mut buffer = ReorderBuffer::new(capacity);
            let mut results = Vec::with_capacity(total);
            while let Ok((seq, o)) = a_out_rx.recv().await {
                results.extend(buffer.insert(seq, o));
            }
            results.extend(buffer.flush_remaining());
            for h in consumers {
                let _ = h.await;
            }
            results
        });

        feeder.join().unwrap();
        bridge.join().unwrap();
        results
    }
}
