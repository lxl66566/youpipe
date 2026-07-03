/// Hint about the per-item **cost distribution** of a fused pipeline, used to
/// pick the fork/join oversplit factor (see `workload_oversplit`).
///
/// This is *not* about how many items each stage receives (streaming handles
/// that via per-stage parallelism + MPMC channels). It is about how much
/// **wall-clock time** each item takes relative to its siblings within a single
/// `pipe(..).collect()` / `for_each()` run:
///
/// - `Balanced` — items cost roughly the same. The fork/join tree needs little
///   stealing slack, so oversplit is adaptive (`1` for small batches, `4` for
///   large). This is the right default for the vast majority of workloads.
/// - `Unbalanced` — a few items are far slower than the rest (skewed tail). The
///   tree always uses `8×` oversplit so an idle worker can steal a slow
///   sibling's remaining leaves, shrinking tail latency.
///
/// # Scope
///
/// Only the **fused** path (`pipe` / `scope` / `try_map`) consults this. The
/// streaming path (`stream(..)`) ignores it: streaming already load-balances
/// per-item skew through its MPMC channel + `per_stage_parallelism` workers (a
/// stalled worker simply stops draining while peers keep consuming), and there
/// is no fork/join oversplit decision to tune. To control streaming tail
/// latency, raise `compute_workers` / `per_stage_parallelism`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Workload {
    /// Per-item cost is roughly uniform. Adaptive oversplit (`1` for small
    /// batches, `4` for large). The right choice for most workloads.
    #[default]
    Balanced,
    /// Per-item cost is skewed (expensive tail). Always `8×` oversplit for
    /// finer-grained work stealing. Costs more dispatch overhead per batch, so
    /// only opt in when the tail is genuinely uneven.
    Unbalanced,
}

/// Top-level configuration for a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of threads dedicated to CPU-bound (sync) work.
    pub compute_workers: usize,
    /// Number of OS threads backing the async I/O runtime (tokio worker
    /// threads). Async stages multiplex many more tasks than this via the
    /// runtime's M:N scheduler — see [`Self::io_concurrency`].
    pub async_workers: usize,
    /// Per-channel buffer capacity (items) between stages.
    pub buffer_size: usize,
    /// Number of concurrently in-flight async I/O tasks per async stage.
    ///
    /// This is the M:N concurrency multiplier: async I/O tasks (e.g.
    /// `tokio::time::sleep`, real network/disk IO) yield the OS thread back to
    /// the runtime while waiting, so `io_concurrency` can be far larger than
    /// `async_workers` (the thread count). Defaults to 128 — high enough to
    /// saturate the runtime with yielded waits, bounded to cap memory.
    pub io_concurrency: usize,
    /// Expected workload distribution pattern.
    pub workload: Workload,
}

impl Default for PipelineConfig {
    /// Returns a config that defaults to the number of available CPU cores
    /// for both worker pools, a 256-slot buffer, and 128-way async IO
    /// concurrency.
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        Self {
            compute_workers: cpus,
            async_workers: cpus,
            buffer_size: 256,
            io_concurrency: 128,
            workload: Workload::Balanced,
        }
    }
}

impl PipelineConfig {
    /// Sets the number of CPU-bound worker threads.
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.compute_workers = n;
        self
    }

    /// Sets the number of async I/O worker threads.
    #[must_use]
    pub fn with_async_workers(mut self, n: usize) -> Self {
        self.async_workers = n;
        self
    }

    /// Sets the per-channel buffer capacity.
    #[must_use]
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.buffer_size = n;
        self
    }

    /// Sets the number of concurrently in-flight async I/O tasks per async
    /// stage. Higher values trade memory for IO concurrency (see
    /// [`PipelineConfig::io_concurrency`]).
    #[must_use]
    pub fn with_io_concurrency(mut self, n: usize) -> Self {
        self.io_concurrency = n;
        self
    }

    /// Sets the expected workload distribution pattern.
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.workload = workload;
        self
    }
}
