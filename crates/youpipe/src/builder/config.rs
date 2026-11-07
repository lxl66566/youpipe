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
/// - `Unbalanced` — a few items are far slower than the rest (skewed tail). Always `64×` oversplit
///   (runtime-tunable via the `YOUPIPE_OVERSPLIT` env var) so an idle worker can steal a slow
///   sibling's remaining leaves, shrinking tail latency.
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
/// raise `compute_workers` or the stage's
/// `SyncStageOptions::workers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Workload {
    /// Adaptive oversplit (see above). Right choice for most workloads.
    #[default]
    Balanced,
    /// Always `64×` oversplit (`YOUPIPE_OVERSPLIT` env override). The deep
    /// tree is tail-latency insurance for genuinely uneven items (its leaf
    /// budget reaches single items around n ≈ threads × 64); batches of
    /// ns-scale items pay ~+20 % dispatch overhead for it — leave them on
    /// [`Balanced`](Workload::Balanced).
    Unbalanced,
    /// Manual oversplit factor, independent of batch size.
    Custom(std::num::NonZeroUsize),
}

/// Which implementation a **filter collect** uses: a chain that contains a
/// [`Filter`](crate::Filter) stage, terminated by `.collect()` /
/// `.try_collect()` (by-ref or owned). Output is identical across all
/// variants — same values, same order — only performance differs (and the
/// returned `Vec`'s spare capacity, see [`FilterCollectMode::Wtc`]).
///
/// Resolution order: an explicit variant set here wins; otherwise the
/// `YOUPIPE_FILTER_COLLECT` environment knob (`"merge"` / `"ctp"` / `"wtc"`)
/// applies; otherwise [`FilterCollectMode::Auto`]'s length gate decides.
///
/// Fused only. By-ref chains follow [`FilterCollectMode::Auto`]'s length
/// gate; owned and fallible filter collects keep the merge tree under
/// `Auto` — their stages consume items by value, so `Ctp`'s counting pass
/// cannot re-run them (`Ctp` degrades to the merge tree) — but honor `Wtc`
/// (the single-pass write-then-compact core works unchanged on consumed
/// items) via the explicit mode or the knob. Non-filtering chains have no
/// variant to choose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterCollectMode {
    /// By-ref: length-gated — merge tree below `100_000` items,
    /// write-then-compact at or above. See [`FilterCollectMode::Wtc`] for
    /// the measured basis of the gate, and why the (at >= 100K high
    /// selectivity even faster) count-then-place variant is never chosen
    /// automatically: it runs the chain TWICE, which is observable to
    /// side-effecting closures. Owned filter collects: merge tree (no gate).
    #[default]
    Auto,
    /// Per-leaf `Vec`s concatenated up a join tree. Best for small/mid
    /// batches at mid/high selectivity; at large sizes each survivor moves
    /// O(depth) times through `extend`.
    Merge,
    /// Count-then-place: pass 1 counts survivors per leaf, pass 2 re-runs the
    /// chain and writes straight into one exactly-sized buffer. Fastest for
    /// by-ref batches of 100K+ items keeping ~30 % or more, but the chain
    /// runs TWICE (observable side effects, double stage cost) and it loses
    /// on every smaller shape. By-ref only — for owned collects this
    /// degrades to [`FilterCollectMode::Merge`] (consumed items cannot be
    /// re-counted).
    Ctp,
    /// Write-then-compact: one pass writes survivors leaf-contiguously into
    /// an n-slot buffer, a compaction pass moves each surviving segment down
    /// at most once. Beat the merge tree at every measured >= 100K
    /// selectivity (10-90 % keep); regressed at 10K high selectivity — hence
    /// Auto's gate. The returned `Vec` may keep the n-slot capacity instead
    /// of shrinking to the survivor count (length and contents unchanged).
    Wtc,
}

/// Chain capacity floor tier — the per-pipeline middle layer of the
/// `YOUPIPE_CHAIN_FLOOR` env knob, which lifts the unpinned channel
/// capacities of async-tailed chains and chains with a `Chunked`-mode
/// fence (the slack removes their per-item wake-chain / convoy slow
/// modes; measurements in the tuning guide's `YOUPIPE_CHAIN_FLOOR`
/// entry).
///
/// Resolution order: an explicitly-set `YOUPIPE_CHAIN_FLOOR` env value
/// shadows a value set here, which shadows [`ChainFloor::Default`]. The
/// distinction matters per pipeline, not per process: one binary may host
/// a latency-critical zero-CPU chain on a lower tier while the rest keep
/// the default — the env knob cannot express that.
///
/// Streaming-only. Pure-sync chains without a `Chunked` fence never lift
/// (no slow mode to remove), `Barrier`-mode fences never lift (nothing
/// overlaps), and explicit per-stage `buffer` pins win verbatim over any
/// tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChainFloor {
    /// The built-in tier: lift to `min(n, 65536)` slots, byte-clamped at
    /// 8 MiB per ring. Identical to leaving the config field unset.
    #[default]
    Default,
    /// No floor — unfloored channel sizing (the `YOUPIPE_CHAIN_FLOOR=0`
    /// escape). Reach for it only to pin backpressure tightly chain-wide;
    /// per-stage `buffer` pins are the better tool for one stage.
    Off,
    /// An explicit tier: lift to `min(n, slots)` slots, byte-clamped per
    /// ring. `Slots(16384)` is the documented lower tier for zero-CPU
    /// latency-bound chains (the 64K tier's 1 MiB rings recycle through
    /// L3 and cost ~2 % median there).
    Slots(std::num::NonZeroUsize),
}

impl ChainFloor {
    /// The knob value this tier denotes (`None` = off) — the
    /// representation the `YOUPIPE_CHAIN_FLOOR` env tier pins and the
    /// stream engine's capacity floor consumes.
    pub(crate) fn resolved_slots(self) -> Option<usize> {
        match self {
            ChainFloor::Default => Some(crate::handoff::CHAIN_FLOOR_SLOTS),
            ChainFloor::Off => None,
            ChainFloor::Slots(n) => Some(n.get()),
        }
    }
}

/// Which hops of an async-tailed chain may batch their output sends — the
/// per-pipeline middle layer of the `YOUPIPE_ASYNC_TX_BATCH` env knob
/// (which refines the env-only global `YOUPIPE_STREAM_TX_BATCH` switch).
///
/// Resolution order: an explicitly-set `YOUPIPE_ASYNC_TX_BATCH` env value
/// shadows a value set here, which shadows [`AsyncTxGate::MixedOnly`].
///
/// Streaming-only, and consulted only on chains containing an async
/// stage; pure-sync chains batch every hop under the global switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AsyncTxGate {
    /// Batched delivery only on hops whose consumers are async (the mixed
    /// ring into the async stage); sync→sync hops of async chains keep
    /// per-item sends. The measured default: the mixed hop wins batched
    /// (−33 % merged @1K stacked with `YOUPIPE_CLAIM_SCALE`) while the mid
    /// hops lose at either claim shape (+17.4 % @100K fully lifted).
    #[default]
    MixedOnly,
    /// Every hop of an async-tailed chain keeps per-item sends (the
    /// 2026-10 gate). Reach for it when chasing a regression on one
    /// specific async chain.
    Off,
    /// Batched delivery on every hop — a measured loss on split shapes
    /// (+17.4 % `async_pair_cpu_split/100K`, 0/25); kept for A/B
    /// reproduction.
    Full,
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
/// `compute_workers`, `workload` and (for filter collects) `filter_collect`
/// (there are no channels and no async runtime in the fused path); a
/// `stream()` run reads all of them. Setting a
/// field your engine ignores is harmless (it is silently unused), but the
/// per-engine builders (`Pipe::with_compute_workers`, `StreamPipe::with_*`)
/// expose only the knobs that actually take effect.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of threads dedicated to CPU-bound (sync) work.
    ///
    /// Fused path: the pool size — the terminal runs on a pool of this many
    /// threads whenever it differs from the machine default (the global
    /// pool, one thread per core), and
    /// [`with_oversubscribe`](crate::Pipe::with_oversubscribe) multiplies it.
    /// Those pools are recycled through the process-wide cache (see
    /// [`ComputePool::new`](crate::ComputePool::new)) — in a loop the second
    /// run onward is spawn-free. An explicit `with_compute_pool` always
    /// takes precedence. Streaming
    /// path: the worker budget divided across sync stages (see
    /// `SyncStageOptions::workers` for per-stage overrides).
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
    /// multiplex many more in-flight items than this — see
    /// [`Self::io_concurrency`]. Streaming-only.
    pub(crate) async_workers: usize,
    /// Per-channel buffer capacity (items) between stages. Streaming-only.
    ///
    /// Note: the effective buffer for a given channel is
    /// `max(buffer_size, workers * 4)`, where `workers` is the fan-out of
    /// the stage producing into that channel — a floor that keeps each of
    /// those workers a few free slots ahead even when the configured
    /// capacity is smaller than the fan-out. An explicit
    /// [`SyncStageOptions::buffer`](crate::SyncStageOptions::buffer) override
    /// replaces
    /// this logic for that stage's output channel.
    pub(crate) buffer_size: usize,
    /// Number of concurrently in-flight async I/O items per async stage.
    ///
    /// This is the concurrency multiplier: async I/O futures (e.g. a timer,
    /// real network/disk IO) yield the OS thread back to the runtime while
    /// waiting, so `io_concurrency` can be far larger than `async_workers`
    /// (the thread count). The items are multiplexed over at most one driver
    /// task per runtime worker thread. Defaults to 128 — high enough to saturate the
    /// runtime with yielded waits, bounded to cap memory.
    ///
    /// Streaming-only, and applies to *every* async stage in the chain; use
    /// [`AsyncStageOptions::io_concurrency`](crate::AsyncStageOptions::io_concurrency)
    /// to size stages individually.
    pub(crate) io_concurrency: usize,
    /// Expected workload distribution pattern. Fused-only (see [`Workload`]).
    pub(crate) workload: Workload,
    /// By-ref filter collect implementation. Fused by-ref-only (see
    /// [`FilterCollectMode`]).
    pub(crate) filter_collect: FilterCollectMode,
    // ── Per-pipeline data-plane policy (r4 audit) ──
    //
    // Middle tier of the three-tier precedence `handoff::resolve_knob`
    // implements: an explicitly-set `YOUPIPE_*` env pin shadows these
    // fields, these fields shadow the built-in defaults. `None` = unset =
    // the built-in default, so a config-less pipeline is bit-for-bit the
    // env-knob-only behavior.
    /// Terminal fan-in channel shape (streaming-only): `Some(true)` = one
    /// SPSC output ring per terminal worker, `Some(false)` = one shared
    /// MPSC ring. Shadowed by an explicit `YOUPIPE_SHARDED_TERM`.
    pub(crate) sharded_terminal: Option<bool>,
    /// Chain capacity floor tier (streaming-only; see [`ChainFloor`]).
    /// Shadowed by an explicit `YOUPIPE_CHAIN_FLOOR`.
    pub(crate) chain_floor: Option<ChainFloor>,
    /// Async-tail tx-batch gate (streaming-only; see [`AsyncTxGate`]).
    /// Shadowed by an explicit `YOUPIPE_ASYNC_TX_BATCH`.
    pub(crate) async_tx_gate: Option<AsyncTxGate>,
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
            filter_collect: FilterCollectMode::Auto,
            sharded_terminal: None,
            chain_floor: None,
            async_tx_gate: None,
        }
    }
}

impl PipelineConfig {
    /// Shared zero-value rejection for every public count/capacity setter:
    /// `0` is never a meaningful pin or size, so it fails loudly at build
    /// time instead of silently truncating to `1` or un-pinning (the two
    /// legacy behaviours this unified; record in docs core-types.md
    /// "Options types split by stage kind").
    pub(crate) fn require_nonzero(n: usize, knob: &'static str) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(n).unwrap_or_else(|| panic!("{knob} expects a non-zero value"))
    }

    /// Clamp-and-record a compute-worker budget set through any builder.
    /// Shared by every `with_compute_workers` so the clamp and the pin flag
    /// (see [`Self::compute_workers_pinned`]) cannot drift apart.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see [`Self::require_nonzero`]).
    pub(crate) fn set_compute_workers(&mut self, n: usize) {
        // `require_nonzero` already floors at 1; the min is the 9-bit cap.
        let pinned = Self::require_nonzero(n, "with_compute_workers");
        self.compute_workers = pinned.get().min(crate::MAX_COMPUTE_WORKERS);
        self.compute_workers_pinned = true;
    }

    /// Sets the number of CPU-bound worker threads.
    ///
    /// On the fused path a non-default value resolves to a pool from the
    /// recycling cache ([`ComputePool::new`](crate::ComputePool::new)) —
    /// dropped pools park instead of joining; call
    /// [`ComputePool::clear_cached_pools`](crate::ComputePool::clear_cached_pools)
    /// to reclaim their threads.
    ///
    /// Clamped to `[1, MAX_COMPUTE_WORKERS]` (511 on 64-bit): the
    /// scheduler's sleep bitmask packs thread indices into 9 bits, so a pool
    /// beyond that cannot exist. Values above the cap truncate rather than
    /// panic so exploratory configs (`num_cpus * 16`, …) keep running;
    /// `0` is never exploratory-useful and panics instead.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see `Self::set_compute_workers`).
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.set_compute_workers(n);
        self
    }

    /// Sets the number of async I/O worker threads. Streaming-only.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see `PipelineConfig::require_nonzero`).
    #[must_use]
    pub fn with_async_workers(mut self, n: usize) -> Self {
        self.async_workers = Self::require_nonzero(n, "with_async_workers").get();
        self
    }

    /// Sets the per-channel buffer capacity. Streaming-only; see the
    /// [`PipelineConfig`] field docs for the `max(downstream_workers * 4)`
    /// floor.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see `PipelineConfig::require_nonzero`).
    #[must_use]
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.buffer_size = Self::require_nonzero(n, "with_buffer_size").get();
        self
    }

    /// Sets the number of concurrently in-flight async I/O tasks per async
    /// stage. Higher values trade memory for IO concurrency (see
    /// `PipelineConfig::io_concurrency`). Streaming-only.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see `PipelineConfig::require_nonzero`).
    #[must_use]
    pub fn with_io_concurrency(mut self, n: usize) -> Self {
        self.io_concurrency = Self::require_nonzero(n, "with_io_concurrency").get();
        self
    }

    /// Sets the expected workload distribution pattern.
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.workload = workload;
        self
    }

    /// Sets the by-ref filter collect implementation (default
    /// [`FilterCollectMode::Auto`]; see that type for variant semantics and
    /// the resolution order).
    #[must_use]
    pub fn with_filter_collect(mut self, mode: FilterCollectMode) -> Self {
        self.filter_collect = mode;
        self
    }

    /// Sets the terminal fan-in channel shape (streaming-only, default
    /// `true`; see the tuning guide's `YOUPIPE_SHARDED_TERM` entry).
    /// Precedence: an explicitly-set env value shadows this field (see
    /// `handoff::resolve_knob`).
    #[must_use]
    pub fn with_sharded_terminal(mut self, on: bool) -> Self {
        self.sharded_terminal = Some(on);
        self
    }

    /// Sets the chain capacity floor tier (streaming-only, default
    /// [`ChainFloor::Default`]; see [`ChainFloor`] for variant semantics
    /// and the resolution order).
    #[must_use]
    pub fn with_chain_floor(mut self, floor: ChainFloor) -> Self {
        self.chain_floor = Some(floor);
        self
    }

    /// Sets which hops of an async-tailed chain may batch their sends
    /// (streaming-only, default [`AsyncTxGate::MixedOnly`]; see
    /// [`AsyncTxGate`] for variant semantics and the resolution order).
    #[must_use]
    pub fn with_async_tx_gate(mut self, gate: AsyncTxGate) -> Self {
        self.async_tx_gate = Some(gate);
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
        cfg.set_compute_workers(1);
        assert_eq!(cfg.compute_workers, 1);
        cfg.set_compute_workers(10_000);
        assert_eq!(cfg.compute_workers, crate::MAX_COMPUTE_WORKERS);
        assert!(cfg.compute_workers_pinned, "set_compute_workers pins");

        let public = PipelineConfig::default().with_compute_workers(8);
        assert_eq!(public.compute_workers, 8);
        assert!(public.compute_workers_pinned, "public builder pins too");
    }

    #[test]
    #[should_panic(expected = "with_compute_workers expects a non-zero value")]
    fn test_compute_workers_zero_panics() {
        let _ = PipelineConfig::default().with_compute_workers(0);
    }

    #[test]
    #[should_panic(expected = "with_buffer_size expects a non-zero value")]
    fn test_buffer_size_zero_panics() {
        let _ = PipelineConfig::default().with_buffer_size(0);
    }

    #[test]
    #[should_panic(expected = "with_io_concurrency expects a non-zero value")]
    fn test_io_concurrency_zero_panics() {
        let _ = PipelineConfig::default().with_io_concurrency(0);
    }

    #[test]
    #[should_panic(expected = "with_async_workers expects a non-zero value")]
    fn test_async_workers_zero_panics() {
        let _ = PipelineConfig::default().with_async_workers(0);
    }
}
