# Performance Benchmarks & Methodology

> All numbers below are from a 32-core AMD (Zen) Linux machine, `criterion`
> `--sample-size 30 --measurement-time 5` (the historical full-treatment
> config; see below), tables refreshed with the current quick config.
> Methodology note: comparisons against rayon borrow the input on both sides
> (`pipe_ref(&data)` vs `par_iter`) — the same warm slice, nothing cloned or
> freed inside the timed region, no lifecycle-alignment tricks needed. The
> engines that *must* take ownership (`pipe(v)`, `stream(v)`) rebuild the
> input in the untimed setup and pull it into cache (`warm_clone`): glibc's
> large `memcpy` uses non-temporal stores that bypass the cache, so a naïve
> `data.clone()` arrives **cold-from-RAM** and measures allocator/memory
> latency instead of the framework. The `_owned_cold` variant in the
> lightweight group documents that one-shot cold cost.

## Suite budget (quick config)

A full-suite pass with criterion's defaults costs >1 h. All bench targets
now share `crates/youpipe/benches/common/mod.rs`, which defaults to the verdict-proven
interleaved-A/B regime — **20 samples, 1 s warm-up, 2 s measurement**
(~3.5 s per bench id, full suite ≈ 10 min including compile) — and shrinks
each group's size axis to its two anchors (the dropped midpoints
interpolate monotonically; see the group comments for specifics). Every
knob is env-overridable without code edits, and the override applies to
both sides of any A/B equally, so fairness is preserved:

```sh
# full-treatment config for a single deep-dive bench
BENCH_SAMPLE_SIZE=100 BENCH_WARMUP_MS=3000 BENCH_MEASUREMENT_MS=5000 \
    cargo bench --bench sync_vs_rayon -- youpipe_par_map/1000
```

For A/B verdicts use `perf/bench-suite` (interleaved rounds, CPU pinning,
median-of-rounds comparison) rather than back-to-back full-group passes —
the stream family has ±10 % inter-run drift and the 100 K fused family is
subject to the whole-group measurement trap documented at the bottom of
this file.

## Interleaved A/B tooling (`perf/bench-suite`)

`bench_ab.sh` materializes each side (git worktree, or `wt` for a snapshot
of the working tree), then runs interleaved rounds — every round runs every
side, and odd rounds reverse the side order to cancel position bias. Every
bench process is pinned via `taskset` (default: cores `1..N-1`, keeping
core 0 for OS/IRQ housekeeping), the criterion sampling budget is forced
through CLI flags so both sides measure identically even across historical
revisions with different in-file defaults, and each (round, side) gets its
own `CRITERION_HOME` so aggregation only ever reads fresh
`new/estimates.json` files. `compare.py` reduces each id to the median of
its per-round medians and calls a delta "stable" only when it exceeds the
observed round-to-round spread with every round leaning the same way.

```sh
# full two-sided A/B, 3 interleaved rounds (both sides build once)
perf/bench-suite/bench_ab.sh -a base=9b31fb0 -b new=HEAD

# drift-sensitive families: isolated per-id interleaving, extra rounds
perf/bench-suite/bench_ab.sh -a base -b wt -r 5 --per-id \
    'stream_pipeline/single_stage_ordered' 'with_fence'

# three-way: rounds interleave A,B,C (label=rev syntax)
perf/bench-suite/bench_ab.sh -a old=HEAD~2 -b mid=HEAD~1 -c wt

python3 perf/bench-suite/compare.py target/bench-ab/run-<ts>   # verdict
```

Appending `-r N` to the same outdir later adds rounds (the worktrees and
their builds are reused); cleanup is
`rm -rf <outdir> && git worktree prune`.

### CPU-Heavy `pipe_ref()` vs rayon (`sync_cpu_heavy`, 100 iters/item, borrowed input)

| Size | youpipe | rayon   |
| ---- | ------- | ------- |
| 1K   | ~10 µs  | ~40 µs  |
| 100K | ~53 µs  | ~146 µs |

youpipe leads at every size (2026-09-05 rerun, quick config). The historical
"1K trails rayon" story turned out to be neither the condvar handshake nor
the inject + wake cascade — it was **`std::thread::available_parallelism()`
re-reading the cgroup CPU-quota files on every call** (~26 µs of
openat/statx/read syscalls per call on this machine; strace: 5 openat +
8 read + 5 statx per call). Two calls sat on every fused terminal
(`PipelineConfig::default()` at `pipe()`/`pipe_ref()` construction and
`resolve_exec_pool` at the terminal), i.e. ~50 µs of pure syscall overhead
on a 57 µs measurement. Both now go through `crate::num_cpus()`, a
process-wide `OnceLock` cache (affinity changes after first use are
intentionally invisible; rayon sizes its global pool once for the same
reason). Found by bisecting a horizontal-suite regression to the commit that
added the second call — the fixed cost scaled the whole fused family by a
constant, invisible to instruction counters.

The two changes below removed ~11 µs of genuine fixed overhead earlier and
remain load-bearing:

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

What remained after those two was attributed to the inject + wake cascade
(pushing `num_threads` JobRefs through the injector + waking the workers) —
wrongly, as the `available_parallelism` find above showed. An attempt to
close it by injecting a single root job (mirroring rayon's `join` unfold)
**regressed** — the log2(num_threads) ramp-up via work-stealing cost more
than the per-chunk overhead it saved (hotpath confirmed `steal` at ~98 ns is
not the bottleneck). The real wins were:

1. **`Box<[ChunkJob]>` consolidation**: all `num_threads` chunks share one
   heap allocation (1 instead of N), saving the per-chunk malloc/free.
2. **Driver-inline participation** (mirrors rayon's calling thread): chunk 0
   runs on the off-pool driver while the pool handles chunks 1..N. This saves
   1 injector push and reduces the condvar wake cascade by 1. Guarded by
   `chunk_splits == 0` (small/medium batches where the chunk is a single leaf)
   to avoid memory-bandwidth contention on large memory-bound workloads.

Together these shaved ~5–8 % off 1K–10K `collect` batches.

#### Driver work-assist via reserve chunks (2026-09; kept, no measurable win)

The 1K gap measured at the time (later shown to be dominated by the uncached
`available_parallelism` calls — see above) was hypothesized to be the wake
cascade — parked workers take µs-scale to wake, and the slowest-to-wake
worker gates the batch tail. The hybrid dispatcher now withholds one tail
chunk from the
injector (`ASSIST_RESERVE_CHUNKS`) and the off-pool driver executes it from
its wait loop (`CountLatch::wait_spin_assist`) — the natural extension of
driver-inline chunk 0. A/B (3+2 interleaved rounds): **no measurable
wall-time change at reserve 1 or 4** — under back-to-back benchmark loops
most workers stay ready, so the rescue only pays off when workers are
absent (pinned by `test_hybrid_driver_assists_when_workers_busy`). Kept at
the conservative 1 for that liveness property. Three earlier variants were
**rejected with lessons** (all recorded at the `hybrid_dispatch` reserve
block):

1. *claim-flag* — driver claims an injected chunk while its `JobRef` stays
   queued; the worker's no-op `execute` then races the driver's frame
   teardown → use-after-free (caught by the panic-propagation tests as a
   misaligned deref). `counter == 0` must imply every injected `JobRef` was
   fully consumed — the teardown's safety invariant.
2. *pop-and-execute anything* — foreign injector jobs are not guaranteed to
   run on an arbitrary thread (`in_worker_cold`'s `StackJob` asserts on the
   worker TLS); executing one unwinds straight through the driver frame,
   skipping the latch wait.
3. *pop + identity check + re-queue foreign* — re-queueing reorders the
   injector's FIFO, which stream pipelines depend on (feeder submitted
   before its resident stage-worker jobs); a feeder pushed behind them
   starves while every worker parks on an empty channel recv → whole-pool
   deadlock, reproduced under the parallel test suite and by
   `crates/youpipe/tests/hybrid_assist.rs`.

An earlier version silently routed small batches to a serial loop to win this
benchmark, but that was deceptive (the API promises parallelism) and
catastrophic for expensive per-item work (file IO, crypto) whose small batches
would be wrongly serialized. The heuristic was removed — see `prefers_serial`
in `crates/youpipe/src/builder/typed/fused.rs`.

### Pipeline Fusion (3 stages) vs rayon chain (`pipeline_fusion`, borrowed input)

| Size | youpipe fused | rayon chain |
| ---- | ------------- | ----------- |
| 10K  | ~10.5 µs      | ~59 µs      |
| 100K | ~24.7 µs      | ~88 µs      |

Under the borrowed caliber (both sides read a warm slice) the fused chain
leads at both sizes (−82 % @ 10K, −72 % @ 100K; 2026-09-05 rerun with the
`num_cpus` cache). The margin comes from five changes: the
sleeping-bitmask rewrite of `wake_any_threads`, moving the `condvar.notify_one`
outside the `is_blocked` mutex, the `.cargo/config.toml` perf-friendly
`opt-level=3`/`panic=unwind` override, adaptive oversplit (`workload_oversplit`,
which drops to `oversplit = 1` for small batches ≤ 1024 items/worker), and
**hybrid flat/tree dispatch** (`par_index_collect_hybrid`) — injecting
`num_threads` broad top-level chunks so every worker starts busy at t≈0 with no
fork/join ramp-up, while each chunk recurses via the tree for distributed
stealing. The hybrid alone measured −6.5 % @ 10 k and −6.7 % @ 100 k.

### Lightweight `pipe()` vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (borrowed) | youpipe (owned, cold clone) | rayon   |
| ---- | ------------------ | --------------------------- | ------- |
| 10K  | ~11 µs             | ~18 µs                      | ~60 µs  |
| 1M   | ~176 µs            | ~4.18 ms                    | ~251 µs |

The borrowed `pipe_ref` row leads rayon at both sizes (−82 % @ 10K, −30 % @
1M; 2026-09-05 rerun); the owned row's ~4.2 ms is the cold-clone trap (the
fresh 8 MB buffer arrives cold-from-RAM), not engine cost — the very reason
`pipe_ref` exists. Warm-input lightweight went ~1.9 ms (pre-`Slots`) →
~390 µs (slice view) → ~187 µs (perf-config + sleeping-bitmask wake +
hybrid flat/tree dispatch) → **~176 µs** (`num_cpus` cache, which also
collapsed the 10K row from ~58 µs to ~11 µs — at 10K the leaf work is so
cheap (~0.12 ns/item) that the measurement had been pure fixed dispatch
cost, ~50 µs of which was the two cgroup-reading `available_parallelism`
syscalls).

### Fallible `try_map().try_collect()` vs rayon (`try_collect`, borrowed input)

When the chain has `MAY_FILTER == false`, `try_collect` uses the same
zero-allocation index-based fast path as `collect` — pre-allocating the output
buffer and writing at known indices instead of the `Vec`-merge fallback.

| Size | youpipe try_map | rayon    |
| ---- | --------------- | -------- |
| 10K  | ~11 µs          | ~409 µs  |
| 100K | ~24 µs          | ~698 µs  |

(rayon row: `par_iter().map(Result).collect::<Result<Vec<_>>>()` has no
indexed fast path — it builds and reduces per-split partial `Vec`s, so the
gap is structural, not a tuning artifact. rayon 1.12.)

### `for_each()` vs rayon (`sync_for_each`, cpu_heavy per item, borrowed input)

| Size | youpipe `for_each` | rayon `for_each` |
| ---- | ------------------ | ---------------- |
| 1K   | ~9.9 µs            | ~38 µs           |
| 100K | ~49.6 µs           | ~121 µs          |

`for_each` was the last fused terminal still on the single-tree path. Porting
it to the shared `hybrid_dispatch` (via the `SinkStrategy` impl of
`HybridStrategy`) measured **−8.7 % @ 1K, −7.2 % @ 10K, −5.0 % @ 100K** vs the
prior tree-only `par_for_each` (sizes at the time of that A/B). With the
`num_cpus` cache (2026-09-05) it leads rayon at every size, including 1K —
the earlier "1K trails because the off-pool driver blocks instead of
participating" gap was, in hindsight, mostly the uncached
`available_parallelism` syscalls, not the driver model. Consolidating all
`num_threads` chunk jobs into a single `Box<[ChunkJob]>` shaved a further
**~3 % @ 1K–10K**. An attempt to instead inject a single root job (rayon's
`join`-unfold pattern) **regressed** — the work-stealing ramp-up cost exceeded
the per-chunk savings, so the hybrid chunk strategy was kept.

### Mixed Load — `stream()` vs `tokio::spawn_blocking` (`mixed_load`)

| Size | youpipe stream | spawn_blocking | rayon (CPU-only) |
| ---- | -------------- | -------------- | ---------------- |
| 1K   | ~317 µs        | ~2.49 ms       | ~37 µs           |
| 100K | ~33.1 ms       | ~239 ms        | ~92 µs           |

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
| 200  | ~9.21 ms      | ~16.52 ms        | ~9.11 ms                 | ~9.16 ms           | ~8.39 ms             |
| 500  | ~9.38 ms      | ~34.72 ms        | ~8.52 ms                 | ~9.29 ms           | ~8.77 ms             |

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
(512 is silently clamped to `MAX_COMPUTE_WORKERS = 511` — one thread short of
tokio's pool, immaterial for the comparison.)
The remaining gap is streaming infrastructure overhead (channel handoff,
injector scheduling) — the tradeoff for backpressure, ordering, and
multi-stage composition that raw `spawn_blocking` doesn't provide. For
blocking IO, `.stage_async()` remains the recommended tool.

#### Mixed CPU (sync) + IO (`io_async_mixed`)

| Size | youpipe_mixed_async | youpipe_mixed_blocking | tokio_mixed_blocking |
| ---- | ------------------- | ---------------------- | -------------------- |
| 200  | ~9.31 ms            | ~28.6 ms               | ~8.92 ms             |
| 500  | ~9.55 ms            | ~63.2 ms               | ~10.08 ms            |

`youpipe_mixed_async` stays well ahead of the all-blocking two-stage baseline,
and at size 500 edges out `tokio_mixed_blocking` by ~150 µs: the async path
overlaps the CPU and IO stages on separate pools, whereas the all-blocking path
splits one compute pool between two blocking stages. At size 200 the fixed
per-run setup cost (feeder, channel allocation, runtime entry) is a
larger fraction of the ~9 ms total, so tokio's simpler spawn-per-item model
still leads there.

### Channel Throughput

| Size | crossfire    | crossbeam-channel | std_mpsc     |
| ---- | ------------ | ----------------- | ------------ |
| 10K  | 55.7 Melem/s | 25.1 Melem/s      | 66.9 Melem/s |
| 100K | 77.2 Melem/s | 24.8 Melem/s      | 85.1 Melem/s |

(2026-09-05 rerun, crossfire 3.1.20.)

### hotpath instrumentation round (2026-09)

With `crates/youpipe-bench-hotpath-profile` (p50 percentiles over `HOTPATH_OUTPUT_FORMAT=json`
reports — p50 is the noise-robust statistic; raw call counts from hotpath are
approximate under load because its per-thread batch queue drops events):

- `sync_lightweight` 1 M warm: `collect` p50 ≈ 262 µs ≈ 61 GB/s of buffer
  traffic — the fused index path is **memory-bandwidth-bound** at large N;
  the documented criterion gap at 1 M is dominated by the benchmark's own
  input-rebuild (`warm_clone`) in the timed region, which rayon's borrow-based
  `par_iter` does not pay.
- 1 K `cpu_heavy`: `inject_batch` p50 ≈ 5 µs of a ~32 µs collect. Attributed
  at the time to worker wake latency (see the rejected cost-adaptive chunk
  experiment in
  [scheduler.md](scheduler.md#dispatch-granularity--cost-adaptive-chunk-counts-tried-rejected));
  the later `available_parallelism` find shows most of that wall was syscall
  overhead hotpath does not attribute to a function, not wake latency.

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

## LLVM folds constant-iteration CPU work (2026-09-07)

A fourth trap, found while chasing the expensive-item regime: the
`r = r*7+13` CPU-work kernel is a first-order linear recurrence, which LLVM
strength-reduces by folding **every 8 iterations into one `imul $0x57f6c1`
+ `add $0xbe96a0`** (7⁸ and 13·(7⁸−1)/6). Runtime iteration counts measure
~0.095 ns/iter, not the ~1 ns a naive cycle estimate suggests; and when the
iteration count is a *compile-time constant* (the horizontal
`cpu_balanced`'s 100, criterion `sync_cpu_heavy`'s same shape) the whole
loop collapses to its closed form — **~6 ns/item, not "100 iters ≈ 100 ns"
as the bench comments claimed**.

Fairness survives (both frameworks run the same folded kernel), but every
absolute load level in those tables is ~16× lighter than documented, and
the expensive-item regime (µs–ms per item, where scheduler handoffs matter
most) was simply never exercised by them. Consequences:

- The 1 M `cpu_balanced` parity with rayon does not extend upward:
  at 2 M/4 M (5-round medians, table below) rayon pulls ahead +14 %/+12 %
  — above 1 M the batches leave the cache-resident regime that hides
  per-item path differences (see "Reading the results").
- Load-level claims in bench comments must derive from *measured*
  ns/iter at the actual iteration count, not from iteration counts.
- Expensive-item benchmarks need **runtime iteration counts** so LLVM
  cannot fold them: the criterion `unbalanced` family's `(item, iters)`
  tuples and the `zstd_shape` group (see dev/scheduler.md "flat top-level
  dispatch") are built that way.

## Horizontal cross-library comparison (2026-09)

`crates/youpipe/benches/horizontal.rs` answers the "what should I pick for my workload?"
question across the ecosystem — youpipe, rayon, tokio, `futures::stream`,
and hand-written `std::thread` baselines — over seven representative
scenarios, and exports JSON that `perf/plot-horizontal.py` renders into the
published SVG charts — the README plus the charts below (matplotlib via
`uv run`; throughput, higher is better, min–max whiskers on the bar panels).
The source data for the published charts is committed at
`perf/horizontal/results.json`.

```sh
cargo bench -p youpipe --bench horizontal -- --rounds 5  # ~5 min incl. build
uv run perf/plot-horizontal.py                     # results.json → docs/src/assets/*.svg
```

### Charts

Throughput, higher is better; bar-chart whiskers span the five interleaved
rounds. The balanced-CPU panel is drawn as × rayon on a linear axis: its
absolute values span three decades across the batch sweep (1.8 → 2070
M items/s), and on the previous log-scale line chart the 4–5× youpipe
advantage read as near-parity. Rayon's absolute throughput at each batch
size rides as a second line under the batch-size tick labels, so any
bar's absolute value is ratio × that number.

![CPU pipelines: youpipe vs rayon vs hand-written std threads](../assets/bench-cpu.svg)

![IO pipelines: youpipe vs tokio vs futures](../assets/bench-io.svg)

![Mixed sync + async pipelines: youpipe vs tokio vs futures vs rayon](../assets/bench-real.svg)

### Methodology

- **Interleaved rounds, ABCABC not AABBCC.** Every round runs every
  (scenario, batch, library) once; odd rounds reverse the library order to
  cancel position bias. Verdicts are the median across rounds — the same
  drift-cancelling logic as `perf/bench-suite`, applied inside a single
  process (one tokio runtime + one `TokioPool` handle shared by everything;
  per-iteration runtime construction would dominate small batches).
- **Per-iteration timing, setup excluded.** Each job times only the workload
  (`Instant` around the run). CPU scenarios are **borrowed-input on both
  sides** — youpipe `pipe_ref(&data)` vs rayon `data.par_iter()` vs chunked
  borrows: each library's idiomatic call over the same warm slice, nothing
  cloned or freed inside the timed region. (History: before `pipe_ref`
  existed, the owned `pipe(v)` rows consumed a fresh cache-warmed clone and
  freed it in-region, and rayon was given an `into_par_iter` twin to match —
  a four-caliber tangle whose lesson, that fresh-clone/free cycles are worth
  ~35 % at 1 M, lives on in the criterion benches above.) Results are
  black-boxed so LLVM cannot DCE the map chains.
- **Simulated IO never touches the disk.** Async waits use
  `tokio::time::sleep`, blocking waits `thread::sleep` (1 ms / 8 ms tail).
  The realistic web scenario speaks HTTP/1.1 over a **loopback Unix-socket
  mock server** with server-side latency control — UDS rather than TCP
  because short-lived TCP connections pile up tens of thousands of
  client-side TIME_WAIT sockets across rounds and eventually exhaust the
  ephemeral port range; UDS has no TIME_WAIT.
- **In-flight caps must be aligned within a scenario.** This one silently
  flipped two verdicts by ~2× in the first draft: youpipe's
  `io_concurrency` (256/512) capped concurrent IO while the tokio baseline
  spawned one task per item (effectively unbounded). With the cap left low,
  the doc pipeline scored youpipe 35 ms vs tokio 17 ms; raising youpipe to
  4096 (above the largest batch, i.e. "unbounded" like spawn-per-item)
  reversed it to 14 ms vs 17 ms — the original gap was queueing behind the
  concurrency cap, not framework overhead. Rule: `io_async` compares the
  *finite-cap* regime (all three at 512), `mixed/real_doc/real_web` compare
  the *unbounded* regime (all three effectively unlimited). Fairness is a
  property of the scenario, not of each library's default.
- **Input lifecycle must be uniform within a CPU scenario** (2026-09 fix,
  superseded 2026-09 by `pipe_ref`): comparing an owned-input call against a
  borrowing one measures the input lifecycle, not the engine — the freeing
  side pays the clone's cache thrash right before the clock starts, an
  asymmetry worth ~35 % at 1 M. The fix is structural now: both sides borrow
  the same warm slice (`pipe_ref` vs `par_iter`), which also collapsed the
  1 M round-to-round drift from ±25 % (fresh 8 MB clone + in-region free) to
  a few percent.
- Machine: 32-core AMD (Zen) Linux, bench pinned to cores 1–31
  (`taskset -c 1-31`, core 0 left to OS/IRQ housekeeping), 5 rounds ×
  700 ms measurement, ~2-8 % cross-round spread on most cells.

### Results (median ms per iteration, 5 interleaved rounds; 2026-09-07 rerun, cpu_balanced extended to 2 M/4 M)

| Scenario | n | Best | Runner-up | Rest |
| --- | --- | --- | --- | --- |
| cpu_balanced | 1K | youpipe 0.009 | rayon 0.037 | std threads 0.556 |
| cpu_balanced | 10K | youpipe 0.012 | rayon 0.063 | std threads 0.583 |
| cpu_balanced | 100K | youpipe 0.053 | rayon 0.123 | std threads 0.764 |
| cpu_balanced | 1M | rayon 0.469 | youpipe 0.475 | std threads 2.719 |
| cpu_balanced | 2M | rayon 0.832 | youpipe 0.949 | std threads 6.210 |
| cpu_balanced | 4M | rayon 1.707 | youpipe 1.907 | std threads 11.739 |
| cpu_unbalanced | 10K | youpipe (Unbalanced) 0.029 | youpipe (default) 0.037 | rayon 0.079, std threads 0.587 |
| cpu_unbalanced | 100K | youpipe (Unbalanced) 0.204 | youpipe (default) 0.238 | rayon 0.263, std threads 0.815 |
| io_async | 500 | futures 9.123 | youpipe 9.458 | tokio 9.471 |
| io_async | 2K | futures 17.516 | youpipe 18.213 | tokio 18.462 |
| io_async | 5K | futures 34.082 | youpipe 34.766 | tokio 35.739 |
| io_blocking | 500 | youpipe (512 thr) 8.569 | tokio 8.871 | std threads 16.988, youpipe (31 thr) 34.725 |
| io_blocking | 2K | youpipe (512 thr) 12.555 | tokio 12.716 | std threads 47.889, youpipe (31 thr) 122.419 |
| mixed_cpu_io | 500 | futures 9.155 | youpipe 9.502 | tokio 10.728 |
| mixed_cpu_io | 2K | futures 9.340 | youpipe 10.636 | tokio 13.393 |
| real_doc | 1K | youpipe 10.753 | tokio 10.884 | rayon 37.918 |
| real_doc | 4K | youpipe 14.199 | tokio 17.385 | rayon 137.073 |
| real_web | 500 | youpipe 11.866 | tokio 12.888 | futures 13.155 |
| real_web | 2K | youpipe 22.681 | tokio 27.610 | futures 28.840 |

### Reading the results

- **Balanced CPU** (`pipe_ref` vs `par_iter`, both borrowing warm data):
  youpipe leads 1K–100K (−76 % @ 1K, −81 % @ 10K, −57 % @ 100K), ties at
  1 M (+1 %), then rayon pulls ahead at 2 M (+14 %) and 4 M (+12 %) —
  above ~1 M the batches (≥ 32 MB of R+W buffer traffic) leave the
  cache-resident regime and rayon's collect path sustains ~38 GB/s where
  youpipe's holds ~34 GB/s. Whether the youpipe gap is output-slot
  indexing, dispatch traffic, or allocator behavior is unattributed.
  Before the `num_cpus` cache (2026-09-05), rayon won 1K and 1M — the 1K
  loss was ~50 µs of cgroup-reading `available_parallelism` syscalls per
  run, not scheduling overhead. Equal-chunk hand-threading is 10–60×
  behind everywhere: 31 spawns per call, no stealing.
- **Skewed CPU**: `Workload::Unbalanced` + work stealing now beats rayon at
  both sizes (0.03 vs 0.081 @ 10K; 0.213 vs 0.267 @ 100K) — at 10K the fixed
  syscall cost previously masked the win — and static chunking is ~3-19×
  behind, stranding the 10 % heavy items in whichever chunks they landed in.
- **Async IO is a near-tie** — the spreads overlap. youpipe
  multiplexes over the same tokio runtime: ±1 % vs tokio (ahead at ≥2K items
  as channel throughput stops mattering), 2–5 % behind `futures::stream`,
  the lightest async *combinator* stack. futures' mixed_cpu_io lead has the
  same cause: it runs the CPU stage inline on runtime workers. That is fine
  at 100 ns/item CPU, and the reason youpipe exists is everything it can't
  do there: fences, cancellation, ordered output, dedicated CPU-pool
  isolation, backpressure across *stages* rather than futures.
- **Blocking IO is a configuration story**: correctly oversubscribed, youpipe
  ≈ tokio `spawn_blocking` (same 512 threads); at the default 31 threads the
  waits serialize (122 ms @ 2K). The chart keeps that failure visible on
  purpose — blocking stages must size the pool, not the framework.
- **Realistic pipelines** are where the streaming engine pays off: 3-stage
  sync+async chains beat hand-written tokio channel plumbing by up to 23 %
  at the larger batches (fewer tasks, pooled scheduling, mixed-mode
  channels) and beat rayon by ~10× once IO blocks its workers.

## Perf-event counter measurement (`crates/youpipe-bench-counter`)

`crates/youpipe-bench-counter` runs the same bench code under Linux perf hardware
counters (instructions / cycles / ref-cycles / cache-misses / …) instead of
wall time, via the workspace's `youpipe-criterion-perf-counters` crate — a maintained
fork of criterion-perf-events re-targeted at criterion 0.8 and extended with
process-wide per-thread counters (upstream counts the main thread only,
which for a pool library measures the coordinator and misses the workers).
Threads that spawn and exit inside one measurement window are invisible, so
channel benches can't use it; plain `b.iter` only (`BatchSize::PerIteration`
windows multiply the per-window `4 × n_threads` counter syscalls by the
iteration count and inflate fast benches).

```sh
cargo bench -p youpipe-bench-counter --bench perf_events
PERF_EVENT=ref-cycles cargo bench -p youpipe-bench-counter
crates/youpipe-bench-counter/run-drift-exp.sh   # N runs per event + drift summary table
```

Drift experiment (2026-09, 3 runs × 20 samples per kind, taskset 1-31) —
cross-run CV of run means / criterion's within-run CV:

| kind         | sequential 100K | youpipe cpu_heavy 100K | rayon cpu_heavy 100K | youpipe light 10K | rayon light 10K |
| ------------ | --------------- | ---------------------- | -------------------- | ----------------- | --------------- |
| walltime     | 0.13 %          | 0.47 %                 | 1.20 %               | 0.96 %            | 1.39 %          |
| instructions | **0.00 %**      | 0.79 %                 | 3.32 %               | 1.02 %            | 1.46 %          |
| cycles       | 0.03 %          | 1.24 %                 | 0.78 %               | 1.72 %            | 1.32 %          |
| ref-cycles   | 0.06 %          | 0.61 %                 | 1.58 %               | 3.87 %            | 0.87 %          |
| cache-misses | 20 % (≈200)     | 0.76 %                 | 0.61 %               | 0.73 %            | 0.48 %          |

Verdict:

- Deterministic instruction streams reproduce exactly (CV 0.00 % vs walltime
  0.13 %) — a counter run is the cheapest "this change should touch nothing"
  check.
- Pool-bench drift is scheduling, not measurement: counters do not replace
  the interleaved-A/B methodology for throughput verdicts (rayon cpu_heavy
  instructions CV 3.3 % > walltime 1.2 %).
- Their payoff is work metrics wall time cannot express: instr/elem
  (cpu_heavy/100K: youpipe 120.5 vs rayon 136.6) and cycles/elem (youpipe
  82, IPC 1.47, vs rayon 179, IPC 0.76); cache-misses reproduces at
  0.5-0.8 % CV for real-traffic benches — a stable memory-behavior signal.
- Counter traps: cycles track wall time under frequency boost (stable here
  only because the governor pins frequency); cache-misses is meaningless
  when the absolute count is tiny; no counter is uniformly most stable
  (ref-cycles was worst for youpipe lightweight).
