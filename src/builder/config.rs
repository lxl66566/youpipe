/// Describes how work is distributed across stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Workload {
    /// Items are roughly evenly distributed across stages.
    #[default]
    Balanced,
    /// Items may be heavily concentrated in a subset of stages.
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
