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
    /// Number of threads dedicated to async I/O work.
    pub async_workers: usize,
    /// Per-channel buffer capacity (items) between stages.
    pub buffer_size: usize,
    /// Expected workload distribution pattern.
    pub workload: Workload,
}

impl Default for PipelineConfig {
    /// Returns a config that defaults to the number of available CPU cores
    /// for both worker pools and a 256-slot buffer.
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        Self {
            compute_workers: cpus,
            async_workers: cpus,
            buffer_size: 256,
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

    /// Sets the expected workload distribution pattern.
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.workload = workload;
        self
    }
}
