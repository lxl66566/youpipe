/// Hint about the per-item **cost distribution** of a fused pipeline, used to
/// pick the fork/join oversplit factor (see `workload_oversplit`).
///
/// This is *not* about how many items each stage receives (streaming handles
/// that via per-stage parallelism + MPMC channels). It is about how much
/// **wall-clock time** each item takes relative to its siblings within a single
/// `pipe(..).collect()` / `for_each()` run:
///
/// - `Balanced` — items cost roughly the same. Little stealing slack is needed, so oversplit is
///   adaptive (`1` for small batches, `4` for large). Right default for the vast majority of
///   workloads.
/// - `Unbalanced` — a few items are far slower than the rest (skewed tail). Always `8×` oversplit
///   so an idle worker can steal a slow sibling's remaining leaves, shrinking tail latency.
/// - `Custom(factor)` — pick the oversplit factor yourself. `Custom(1)` is the coarsest tree (one
///   leaf per worker, minimal dispatch overhead); `Custom(16)` is very fine-grained stealing for
///   extreme skew. The useful envelope on large machines is roughly `4..=16`.
///
/// # Oversplit vs oversubscribe
///
/// `Workload` (this enum) tunes the **task-split granularity** of the fork/join
/// tree — how many leaves each worker gets to steal from. It does **not**
/// change the thread count. To add threads for blocking-IO sync workloads, see
/// [`crate::Pipe::with_oversubscribe`] / [`crate::ComputePool::new`].
///
/// # Scope
///
/// Only the **fused** path (`pipe` / `scope` / `try_map`) consults this. The
/// streaming path (`stream(..)`) ignores it: streaming already load-balances
/// per-item skew through its MPMC channel + per-stage workers (a stalled
/// worker simply stops draining while peers keep consuming), and there is no
/// fork/join oversplit decision to tune. To control streaming tail latency,
/// raise `compute_workers` or the stage's `StageOptions::workers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Workload {
    /// Adaptive oversplit (see above). Right choice for most workloads.
    #[default]
    Balanced,
    /// Always `8×` oversplit. Costs more dispatch overhead per batch, so only
    /// opt in when the tail is genuinely uneven.
    Unbalanced,
    /// Manual oversplit factor, independent of batch size.
    Custom(std::num::NonZeroUsize),
}

/// Top-level configuration for a pipeline run.
///
/// All fields are `pub(crate)` — construction and mutation go through
/// [`Default`] + the `with_*` builder methods. This lets the crate add
/// invariants (e.g. clamping `compute_workers` to ≥ 1 and ≤
/// [`MAX_COMPUTE_WORKERS`](crate::MAX_COMPUTE_WORKERS)) without worrying that
/// a caller has mutated a field directly.
///
/// # Field scope
///
/// Not every field applies to every engine — a fused `pipe()` run reads only
/// `compute_workers` and `workload` (there are no channels and no async
/// runtime in the fused path); a `stream()` run reads all of them. Setting a
/// field your engine ignores is harmless (it is silently unused), but the
/// per-engine builders (`Pipe::with_compute_workers`, `StreamPipe::with_*`)
/// expose only the knobs that actually take effect.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of threads dedicated to CPU-bound (sync) work.
    ///
    /// Fused path: the pool size — the terminal runs on a transient pool of
    /// this many threads whenever it differs from the machine default (the
    /// global pool, one thread per core), and
    /// [`with_oversubscribe`](crate::Pipe::with_oversubscribe) multiplies it.
    /// An explicit `with_compute_pool` always takes precedence. Streaming
    /// path: the worker budget divided across sync stages (see
    /// `StageOptions::workers` for per-stage overrides).
    ///
    /// Clamped to `[1, MAX_COMPUTE_WORKERS]` — the scheduler's sleep bitmask
    /// packs thread indices into 9 bits (511 threads), so larger values are
    /// truncated, not rejected.
    pub(crate) compute_workers: usize,
    /// Whether `compute_workers` was explicitly pinned via any
    /// `with_compute_workers` builder (or a hand-built config). Streaming
    /// consults this to resolve the worker budget: a pinned value is honoured
    /// as-is regardless of the compute pool's thread count, while an unpinned
    /// one follows the pool. The fused path ignores it (it compares the value
    /// against the machine default instead).
    pub(crate) compute_workers_pinned: bool,
    /// Number of OS threads backing the async I/O runtime (worker threads of
    /// the active [`AsyncRuntime`](crate::AsyncRuntime) backend). Async stages
    /// multiplex many more tasks than this via the runtime's scheduler — see
    /// [`Self::io_concurrency`]. Streaming-only.
    pub(crate) async_workers: usize,
    /// Per-channel buffer capacity (items) between stages. Streaming-only.
    ///
    /// Note: the effective buffer for a given channel is
    /// `max(buffer_size, downstream_workers * 4)` — a floor that keeps every
    /// downstream worker able to hold a few items in flight even when the
    /// configured capacity is smaller than the fan-out. An explicit
    /// [`StageOptions::buffer`](crate::StageOptions::buffer) override replaces
    /// this logic for that stage's output channel.
    pub(crate) buffer_size: usize,
    /// Number of concurrently in-flight async I/O tasks per async stage.
    ///
    /// This is the concurrency multiplier: async I/O tasks (e.g. a timer,
    /// real network/disk IO) yield the OS thread back to the runtime while
    /// waiting, so `io_concurrency` can be far larger than `async_workers`
    /// (the thread count). Defaults to 128 — high enough to saturate the
    /// runtime with yielded waits, bounded to cap memory.
    ///
    /// Streaming-only, and applies to *every* async stage in the chain; use
    /// [`StageOptions::io_concurrency`](crate::StageOptions::io_concurrency)
    /// to size stages individually.
    pub(crate) io_concurrency: usize,
    /// Expected workload distribution pattern. Fused-only (see [`Workload`]).
    pub(crate) workload: Workload,
}

impl Default for PipelineConfig {
    /// Returns a config that defaults to the number of available CPU cores
    /// for both worker pools, a 256-slot buffer, and 128-way async IO
    /// concurrency.
    fn default() -> Self {
        let cpus = crate::num_cpus();
        Self {
            compute_workers: cpus,
            compute_workers_pinned: false,
            async_workers: cpus,
            buffer_size: 256,
            io_concurrency: 128,
            workload: Workload::Balanced,
        }
    }
}

impl PipelineConfig {
    /// Clamp-and-record a compute-worker budget set through any builder.
    /// Shared by every `with_compute_workers` so the clamp and the pin flag
    /// (see [`Self::compute_workers_pinned`]) cannot drift apart.
    pub(crate) fn set_compute_workers(&mut self, n: usize) {
        self.compute_workers = n.clamp(1, crate::MAX_COMPUTE_WORKERS);
        self.compute_workers_pinned = true;
    }

    /// Sets the number of CPU-bound worker threads.
    ///
    /// Silently clamped to `[1, MAX_COMPUTE_WORKERS]` (511 on 64-bit): the
    /// scheduler's sleep bitmask packs thread indices into 9 bits, so a pool
    /// beyond that cannot exist. Values above the cap truncate rather than
    /// panic so exploratory configs (`num_cpus * 16`, …) keep running.
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.set_compute_workers(n);
        self
    }

    /// Sets the number of async I/O worker threads. Streaming-only.
    #[must_use]
    pub fn with_async_workers(mut self, n: usize) -> Self {
        self.async_workers = n.max(1);
        self
    }

    /// Sets the per-channel buffer capacity. Streaming-only; see the
    /// [`PipelineConfig`] field docs for the `max(downstream_workers * 4)`
    /// floor.
    #[must_use]
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.buffer_size = n.max(1);
        self
    }

    /// Sets the number of concurrently in-flight async I/O tasks per async
    /// stage. Higher values trade memory for IO concurrency (see
    /// [`PipelineConfig::io_concurrency`]). Streaming-only.
    #[must_use]
    pub fn with_io_concurrency(mut self, n: usize) -> Self {
        self.io_concurrency = n.max(1);
        self
    }

    /// Sets the expected workload distribution pattern.
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.workload = workload;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_workers_clamp_and_pin() {
        let mut cfg = PipelineConfig::default();
        assert!(!cfg.compute_workers_pinned, "default is unpinned");
        cfg.set_compute_workers(0);
        assert_eq!(cfg.compute_workers, 1);
        cfg.set_compute_workers(10_000);
        assert_eq!(cfg.compute_workers, crate::MAX_COMPUTE_WORKERS);
        assert!(cfg.compute_workers_pinned, "set_compute_workers pins");

        let public = PipelineConfig::default().with_compute_workers(8);
        assert_eq!(public.compute_workers, 8);
        assert!(public.compute_workers_pinned, "public builder pins too");
    }
}
