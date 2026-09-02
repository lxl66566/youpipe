# Performance Benchmarks & Methodology

> [← Documentation index](README.md)

> All numbers below are from a 32-core AMD (Zen) Linux machine, `criterion`
> `--sample-size 30 --measurement-time 5`. Methodology note: `pipe()` takes
> ownership of the input, so a benchmark iteration must rebuild the input
> (`warm_clone`). glibc's large `memcpy` uses non-temporal stores that bypass
> the cache, so a naïve `data.clone()` arrives **cold-from-RAM** — measuring
> allocator/memory latency rather than the framework. The `sync_vs_rayon` bench
> therefore warms the input in the (untimed) setup so the timed region is a
> fair, like-for-like comparison with rayon's warm `par_iter` borrow. A
> `_cold` variant is kept for the lightweight group to document the one-shot
> cold-memory cost.

### CPU-Heavy `pipe()` vs rayon (`sync_cpu_heavy`, 100 iters/item, warm input)

| Size | youpipe | rayon   |
| ---- | ------- | ------- |
| 1K   | ~59 µs  | ~38 µs  |
| 10K  | ~61 µs  | ~69 µs  |
| 100K | ~102 µs | ~137 µs |

The 1K case still trails rayon but the gap narrowed from ~33 µs to ~21 µs after
two changes that together removed ~11 µs of fixed overhead:

1. **`Workload::Balanced` is now the default** (was `Unbalanced` → oversplit 8).
   For 1K/10K batches `n / num_threads ≤ 1024`, so the adaptive path picks
   `oversplit = 1` (32 leaves) instead of 8 (256 leaves) — far fewer internal
   nodes to dispatch.
2. **Spin-then-park for the off-pool wait** (`CountLatch::wait_spin`): the
   hybrid driver tight-spins on the `counter` atomic for a bounded budget
   (4096 PAUSE iters ≈ 100–150 µs) before acquiring the latch's mutex. In the
   short-wait regime the last chunk's `fetch_sub` lands inside the spin window,
   so the condvar park/notify syscall (~10–20 µs of fixed overhead) is skipped
   entirely; long waits still fall through to the condvar. The mutex acquire is
   load-bearing for soundness — it serializes against the last chunk's
   in-flight `LockLatch::set`, preventing a use-after-free (spinning on the
   counter and returning directly would race the latch free against that
   access; observed as SIGSEGV).

The residual ~21 µs is no longer the condvar handshake — it is the inject+
wake cascade (pushing `num_threads` JobRefs through the injector + waking the
workers). An attempt to close it by injecting a single root job (mirroring
rayon's `join` unfold) **regressed** — the log2(num_threads) ramp-up via
work-stealing cost more than the per-chunk overhead it saved (hotpath
confirmed `steal` at ~98 ns is not the bottleneck; the inject + wake cascade
is). The real wins were:

1. **`Box<[ChunkJob]>` consolidation**: all `num_threads` chunks share one
   heap allocation (1 instead of N), saving the per-chunk malloc/free.
2. **Driver-inline participation** (mirrors rayon's calling thread): chunk 0
   runs on the off-pool driver while the pool handles chunks 1..N. This saves
   1 injector push and reduces the condvar wake cascade by 1. Guarded by
   `chunk_splits == 0` (small/medium batches where the chunk is a single leaf)
   to avoid memory-bandwidth contention on large memory-bound workloads.

Together these shaved ~5–8 % off 1K–10K `collect` batches.

An earlier version silently routed small batches to a serial loop to win this
benchmark, but that was deceptive (the API promises parallelism) and
catastrophic for expensive per-item work (file IO, crypto) whose small batches
would be wrongly serialized. The heuristic was removed — see `prefers_serial`
in `src/builder/typed/fused.rs`.

### Pipeline Fusion (3 stages) vs rayon chain (`pipeline_fusion`, warm input)

| Size | youpipe fused | rayon chain |
| ---- | ------------- | ----------- |
| 10K  | ~59 µs        | ~66 µs      |
| 100K | ~79 µs        | ~100 µs     |

The fused stage chain now beats rayon at every size after five changes: the
sleeping-bitmask rewrite of `wake_any_threads`, moving the `condvar.notify_one`
outside the `is_blocked` mutex, the `.cargo/config.toml` perf-friendly
`opt-level=3`/`panic=unwind` override, adaptive oversplit (`workload_oversplit`,
which drops to `oversplit = 1` for small batches ≤ 1024 items/worker), and
**hybrid flat/tree dispatch** (`par_index_collect_hybrid`) — injecting
`num_threads` broad top-level chunks so every worker starts busy at t≈0 with no
fork/join ramp-up, while each chunk recurses via the tree for distributed
stealing. The hybrid alone measured −6.5 % @ 10 k and −6.7 % @ 100 k.

### Lightweight `pipe()` vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (warm) | youpipe (cold) | rayon   |
| ---- | -------------- | -------------- | ------- |
| 10K  | ~59 µs         | ~67 µs         | ~64 µs  |
| 100K | ~77 µs         | ~120 µs        | ~105 µs |
| 1M   | ~540 µs        | ~4.23 ms       | ~273 µs |

Warm-input lightweight improved ~1.9 ms (pre-`Slots`) → ~730 µs (after `Slots`)
→ ~390 µs (slice view) → ~570 µs (after perf-config + sleeping-bitmask wake +
notify-outside-lock) → **~516 µs after hybrid flat/tree dispatch** (which alone
shaved −9.6 % / −55 µs by eliminating fork/join ramp-up). The 1 M case still
trails rayon because the leaf work itself is so cheap (~0.12 ns/item) that the
off-pool spin/mutex wait + per-chunk tree fixed cost dominate; at 10 k and
100 k youpipe beats rayon because the leaf amortises the overhead better.

### Fallible `try_map().try_collect()` vs rayon (`try_collect`, warm input)

When the chain has `MAY_FILTER == false`, `try_collect` uses the same
zero-allocation index-based fast path as `collect` — pre-allocating the output
buffer and writing at known indices instead of the `Vec`-merge fallback.

| Size | youpipe try_map | rayon   |
| ---- | --------------- | ------- |
| 10K  | ~64 µs          | ~66 µs  |
| 100K | ~85 µs          | ~98 µs  |

### `for_each()` vs rayon (`sync_for_each`, cpu_heavy per item, warm input)

| Size | youpipe `for_each` | rayon `for_each` |
| ---- | ------------------ | ---------------- |
| 1K   | ~62 µs             | ~47 µs           |
| 10K  | ~183 µs            | ~203 µs          |
| 100K | ~1.54 ms           | ~1.49 ms         |

`for_each` was the last fused terminal still on the single-tree path — it
never went through `hybrid_dispatch`'s `inject_batch` +
`CountLatch::wait_spin` pattern, so it paid the full fork/join ramp-up cost.
Porting it to the shared `hybrid_dispatch` (via the `SinkStrategy` impl of
`HybridStrategy`) measured **−8.7 % @ 1K, −7.2 % @ 10K, −5.0 % @ 100K** vs the
prior tree-only `par_for_each`. At 10K youpipe now beats rayon; the 1K case
still trails because the off-pool driver blocks instead of participating the
way rayon's `par_iter` runs inline on the caller (a known remaining gap —
see the "off-pool driver blocks" note under "CPU-Heavy `pipe()` vs rayon"
above). A subsequent change consolidated all `num_threads` chunk
jobs into a single `Box<[ChunkJob]>` (1 heap allocation instead of N+1),
which shaved a further **~3 % @ 1K–10K** by eliminating the per-chunk
malloc/free overhead. An attempt to instead inject a single root job (rayon's
`join`-unfold pattern) **regressed** — the work-stealing ramp-up cost exceeded
the per-chunk savings on youpipe's scheduler, so the hybrid chunk strategy was
kept.

### Mixed Load — `stream()` vs `tokio::spawn_blocking` (`mixed_load`)

| Size | youpipe stream | spawn_blocking | rayon (CPU-only) |
| ---- | -------------- | -------------- | ---------------- |
| 1K   | ~832 µs        | ~2.92 ms       | ~38 µs           |
| 10K  | ~9.5 ms        | ~27.4 ms       | ~68 µs           |
| 100K | ~95.9 ms       | ~239 ms        | ~111 µs          |

`StreamPipe` beats `tokio::spawn_blocking` (the design target for mixed CPU/IO)
at every size, with the margin widest at smaller sizes where per-task spawn
overhead dominates tokio's cost, and narrowing at larger sizes where channel
bandwidth becomes the bottleneck. `rayon::par_iter` is fastest here because
this benchmark is pure-CPU and rayon's direct fork-join skips channel handoff
entirely. All youpipe variants use `warm_clone` (cache-warmed input) for fair
comparison against rayon's warm borrow.

### Async IO — `.stage_async()` (`io_async`, yielding IO)

Simulated IO uses `tokio::time::sleep` (90% × 1 ms, 10% × 8 ms tail) — a wait
that _yields_ the OS thread, the regime where M:N async concurrency beats the
blocking-thread-per-core model. `io_concurrency = 512`, 32-core machine.

#### Pure IO (`io_async_pure`)

| Size | youpipe_async | youpipe_blocking | youpipe_blocking_oversub | tokio_async_native | tokio_spawn_blocking |
| ---- | ------------- | ---------------- | ------------------------ | ------------------ | -------------------- |
| 200  | ~9.32 ms      | ~16.56 ms        | ~11.31 ms                | ~9.16 ms           | ~8.38 ms             |
| 500  | ~9.65 ms      | ~33.08 ms        | ~19.46 ms                | ~9.30 ms           | ~8.83 ms             |

`youpipe_async` matches `tokio_async_native` (the async ceiling) within ~3% and
stays well ahead of `youpipe_blocking`. `tokio_spawn_blocking` edges it via
tokio's 512-thread blocking pool — aggressive OS-thread oversubscription that
only pays off for pure-sleep (no CPU) work. The gap to the async ceiling shrank
after three changes: eliminating the sync→async bridge thread for
`stream(..).stage_async(..)` (the feeder pushes into a mixed-mode `SyncSender`
+ `AsyncReceiver` channel that the AsyncStage consumes directly — see the
`StreamPipe` section of core-types.md),
and replacing the collector's per-item `recv().await` with a `try_recv`
burst-drain that absorbs tokio's timer-tick completion bursts without per-item
waker overhead.

`youpipe_blocking_oversub` uses `.with_compute_pool(ComputePool::new(512))`
to match tokio's 512-thread blocking pool, narrowing the gap substantially.
The remaining gap is streaming infrastructure overhead (channel handoff,
injector scheduling) — the tradeoff for backpressure, ordering, and
multi-stage composition that raw `spawn_blocking` doesn't provide. For
blocking IO, `.stage_async()` remains the recommended tool.

#### Mixed CPU (sync) + IO (`io_async_mixed`)

| Size | youpipe_mixed_async | youpipe_mixed_blocking | tokio_mixed_blocking |
| ---- | ------------------- | ---------------------- | -------------------- |
| 200  | ~9.48 ms            | ~27.3 ms               | ~8.93 ms             |
| 500  | ~9.97 ms            | ~60.0 ms               | ~10.1 ms             |

`youpipe_mixed_async` stays well ahead of the all-blocking two-stage baseline,
and at size 500 edges out `tokio_mixed_blocking` by ~150 µs: the async path
overlaps the CPU and IO stages on separate pools, whereas the all-blocking path
splits one compute pool between two blocking stages. At size 200 the fixed
per-run setup cost (feeder thread, channel allocation, runtime entry) is a
larger fraction of the ~9 ms total, so tokio's simpler spawn-per-item model
still leads there.

### Channel Throughput

| Size | crossfire    | crossbeam-channel | std_mpsc     |
| ---- | ------------ | ----------------- | ------------ |
| 10K  | 27.1 Melem/s | 20.2 Melem/s      | 37.0 Melem/s |
| 100K | 34.7 Melem/s | 17.3 Melem/s      | 44.6 Melem/s |

### hotpath instrumentation round (2026-09)

With `perf/hotpath-profile` (p50 percentiles over `HOTPATH_OUTPUT_FORMAT=json`
reports — p50 is the noise-robust statistic; raw call counts from hotpath are
approximate under load because its per-thread batch queue drops events):

- `sync_lightweight` 1 M warm: `collect` p50 ≈ 262 µs ≈ 61 GB/s of buffer
  traffic — the fused index path is **memory-bandwidth-bound** at large N;
  the documented criterion gap at 1 M is dominated by the benchmark's own
  input-rebuild (`warm_clone`) in the timed region, which rayon's borrow-based
  `par_iter` does not pay.
- 1 K `cpu_heavy`: `inject_batch` p50 ≈ 5 µs of a ~32 µs collect (the wake
  cascade of parked workers); the rest of the wall is worker wake latency —
  see the rejected cost-adaptive chunk experiment in
[scheduler.md](scheduler.md#dispatch-granularity--cost-adaptive-chunk-counts-tried-rejected).

### Final criterion verdict (2026-09 round, clean 4-pass interleaved A/B)

Baseline = `9b31fb0` (crates.io st3 0.4 / concurrent-queue 2.5) vs this
branch (vendored forks + pool-job feeder), median of two passes per side,
`taskset 1-31` both sides:

- **stream_pipeline** 1 K: ordered −13.5 %, unordered −13.4 %,
  multi_stage_2 −13.9 % (pool-job feeder + injector backoff)
- **stream_pipeline** 100 K: ordered −8.8 %, unordered −6.3 %;
  with_fence/100 K −5.7 %
- sync_lightweight cold/100 K −5.3 %; sync_for_each 100 K −6.8 %;
  sync_cpu_heavy sequential/10-100 K −1.6…−5.4 %
- everything else (CPU fused vs rayon, unbalanced, io_async, oversubscribe)
  within ±2 % noise; **no regression beyond noise**

Methodology notes learned the hard way, both worth repeating: (1) a
full-group criterion pass has ±10 % inter-run variance on the stream family
(each iteration runs a pool-wide wake cascade); verdicts for that family
need isolated alternating runs (`--sample-size 20`, base/new/base/new) —
those confirm every stream win above; (2) `target/criterion` accumulates
`base/`/`change/`/saved-baseline subdirectories from earlier rounds — a diff
script must read **only** `new/estimates.json` or it silently compares
against stale runs (this produced phantom 2-4× "regressions" on the 100 K
sync benches that vanished on re-measurement).

### Round 2 verdict (2026-09-02: lazy reorder, fence recycling, `push_n`)

Three further optimizations (see [streaming.md](streaming.md) and the
vendored-queue notes in [scheduler.md](scheduler.md#vendored-scheduler-dependencies-vendor)),
A/B'd with isolated alternating runs (3 rounds, median) against the round-1
tip:

- ordered stream 100 K **−21.7 %** (lazy slot array: zero allocation, zero
  init, zero cache traffic for in-order streams), with_fence 100 K **−23 %**
  (batch-allocation recycling); 10 K sizes −2…−3 %
- `push_n` segment reservation: cpu_heavy fused dispatch −0.5…−2.6 %,
  sync_lightweight/try_collect −1…−2 %
- **no regression anywhere** — including the four 100 K fused benches that
  a full-group pass flagged at +80…+290 %

That last point is a third measurement trap, bigger than both above: the
full-group `sync_vs_rayon` pass can collapse the 100 K (800 KB-input)
benches by 2-3× *regardless of code version* — the unmodified baseline
binary reproduces the same collapse minutes later, and the pure-rayon
control bench (`mixed_load/rayon_par_iter`, which shares none of youpipe's
code) swings +5.2 % in-group vs +1.2 % isolated. The 1 M (8 MB-input) sizes
stay clean at ±1 %, so the mechanism is the 800 KB-mmap-regime allocation
path interacting with whole-group sequence state, not code. **Verdicts for
the 100 K fused family must come from isolated alternating runs**; in-group
numbers for that family are meaningless on this machine.

The vendored queue's loom suite also has a runtime trap: without upstream's
CI setting `LOOM_MAX_PREEMPTIONS=2`, the `spsc`/`spsc_force` models run for
an hour+ without completing; with it the whole suite finishes in seconds.
