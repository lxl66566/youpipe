use crate::handoff::BatchConfig;

pub struct SchedulerConfig {
    pub compute_workers: usize,
    pub async_workers: usize,
    pub batch_config: BatchConfig,
    pub ring_capacity: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        Self {
            compute_workers: cpus,
            async_workers: cpus,
            batch_config: BatchConfig::default(),
            ring_capacity: 1024,
        }
    }
}
