mod worker;

pub use worker::ComputePool;

/// Hard cap on worker threads per [`ComputePool`] (511 on 64-bit targets,
/// 255 on 32-bit).
///
/// The scheduler's sleeping-worker bitmask packs one bit per worker into a
/// fixed `[AtomicU64; N]` array sized to fit a single cache line, and the
/// packed sleep counters reserve 9 bits for the thread index — so a pool
/// physically cannot address more than 511 workers. Values above the cap are
/// **silently truncated** by `ComputePool::new` /
/// `PipelineConfig::with_compute_workers` (they do not panic), so exploratory
/// oversubscription configs keep running. 511 covers any realistic machine
/// (the largest cloud bare-metal instances top out at 448 vCPUs).
pub const MAX_COMPUTE_WORKERS: usize = crate::pool::sleep::THREADS_MAX;
