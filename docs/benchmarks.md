# Performance Benchmarks & Methodology

> [← Documentation index](README.md)

> All numbers below are from a 32-core AMD (Zen) Linux machine, `criterion`
> `--sample-size 30 --measurement-time 5` (the historical full-treatment
> config; see below). Methodology note: `pipe()` takes ownership of the
> input, so a benchmark iteration must rebuild the input (`warm_clone`).
> glibc's large `memcpy` uses non-temporal stores that bypass the cache, so
> a naïve `data.clone()` arrives **cold-from-RAM** — measuring
> allocator/memory latency rather than the framework. The `sync_vs_rayon`
> bench therefore warms the input in the (untimed) setup so the timed region
> is a fair, like-for-like comparison with rayon's warm `par_iter` borrow. A
> `_cold` variant is kept for the lightweight group to document the one-shot
> cold-memory cost.

## Suite budget (quick config)

A full-suite pass with criterion's defaults costs >1 h. All bench targets
now share `benches/common/mod.rs`, which defaults to the verdict-proven
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

#### Driver work-assist via reserve chunks (2026-09; kept, no measurable win)

The residual 1K gap after all of the above is the wake cascade — parked
workers take µs-scale to wake, and the slowest-to-wake worker gates the
batch tail. The hybrid dispatcher now withholds one tail chunk from the
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
   `tests/hybrid_assist.rs`.

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

Warm-input lightweight went ~1.9 ms (pre-`Slots`) → ~390 µs (slice view) →
**~516 µs** (perf-config + sleeping-bitmask wake + hybrid flat/tree dispatch,
which alone shaved −9.6 % by eliminating fork/join ramp-up). The 1 M case still
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

`for_each` was the last fused terminal still on the single-tree path. Porting
it to the shared `hybrid_dispatch` (via the `SinkStrategy` impl of
`HybridStrategy`) measured **−8.7 % @ 1K, −7.2 % @ 10K, −5.0 % @ 100K** vs the
prior tree-only `par_for_each`. At 10K youpipe now beats rayon; the 1K case
still trails because the off-pool driver blocks instead of participating the
way rayon's `par_iter` runs inline on the caller (a known remaining gap —
see the note under "CPU-Heavy `pipe()` vs rayon" above). Consolidating all
`num_threads` chunk jobs into a single `Box<[ChunkJob]>` shaved a further
**~3 % @ 1K–10K**. An attempt to instead inject a single root job (rayon's
`join`-unfold pattern) **regressed** — the work-stealing ramp-up cost exceeded
the per-chunk savings, so the hybrid chunk strategy was kept.

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
(512 is silently clamped to `MAX_COMPUTE_WORKERS = 511` — one thread short of
tokio's pool, immaterial for the comparison.)
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
per-run setup cost (feeder, channel allocation, runtime entry) is a
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
## Horizontal cross-library comparison (2026-09)

`benches/horizontal.rs` answers the "what should I pick for my workload?"
question across the ecosystem — youpipe, rayon, tokio, `futures::stream`,
and hand-written `std::thread` baselines — over seven representative
scenarios, and exports JSON that `perf/plot-horizontal.py` renders into the
README's SVG charts (hand-written SVG, zero plotting dependencies). The
source data for the published charts is committed at
`perf/horizontal/results.json`.

```sh
cargo bench --bench horizontal -- --rounds 5      # ~6 min incl. build
python3 perf/plot-horizontal.py                   # perf/horizontal/results.json → docs/assets/*.svg
```

### Methodology

- **Interleaved rounds, ABCABC not AABBCC.** Every round runs every
  (scenario, batch, library) once; odd rounds reverse the library order to
  cancel position bias. Verdicts are the median across rounds — the same
  drift-cancelling logic as `perf/bench-suite`, applied inside a single
  process (one tokio runtime + one `TokioPool` handle shared by everything;
  per-iteration runtime construction would dominate small batches).
- **Per-iteration timing, setup excluded.** Each job times only the workload
  (`Instant` around the run); input rebuild / cache-warming (`warm_clone`,
  same rationale as `sync_vs_rayon`) happens outside the timed region.
  Results are black-boxed so LLVM cannot DCE the map chains.
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
- **Input lifecycle must be aligned within a CPU scenario** (2026-09 fix).
  youpipe's `pipe(v)` takes ownership: each timed iteration consumes a fresh
  `warm_clone` and frees the input buffer *inside* the timed region, paying
  the clone's cache thrash right before the clock starts. rayon's idiomatic
  `data.par_iter()` borrows the same warm buffer every iteration — nothing
  freed in-region, no clone thrash — an asymmetric advantage that grew with
  batch size (at 1 M, the 8 MB clone+free cycle is worth ~35 % of the
  batch). The chart therefore shows **four readings**: the primary `rayon`
  row consumes a fresh `warm_clone` via `into_par_iter()` (like-for-like
  memory behavior vs the owned `pipe(v)`); `rayon (borrowed)` keeps the
  idiomatic call; `youpipe` pays the owned lifecycle; `youpipe (borrowed)`
  (`pipe_ref(&data)`, added 2026-09) is youpipe's idiomatic borrow — the
  natural like-for-like pair of `rayon (borrowed)`. The distinction matters:
  under the aligned lifecycle rayon's 100 K–1 M numbers rise 50–60 % (it,
  too, pays the fresh-input costs), while youpipe's stay put — those costs
  were always in its baseline.
- Machine: 32-core AMD (Zen) Linux, bench pinned to cores 1–31
  (`taskset -c 1-31`, core 0 left to OS/IRQ housekeeping), 5 rounds ×
  700 ms measurement, ~2-8 % cross-round spread on most cells.

### Results (median ms per iteration, 5 interleaved rounds; cpu_balanced 9)

| Scenario | n | Best | Runner-up | Rest |
| --- | --- | --- | --- | --- |
| cpu_balanced | 1K | rayon (borrowed) 0.038 | rayon 0.040 | youpipe 0.057 = youpipe (borrowed) 0.056*, std threads 0.545 |
| cpu_balanced | 10K | youpipe 0.060 | youpipe (borrowed) 0.059* | rayon (borrowed) 0.062, rayon 0.063, std threads 0.562 |
| cpu_balanced | 100K | youpipe (borrowed) 0.097* | youpipe 0.114 | rayon (borrowed) 0.122, rayon 0.187, std threads 0.736 |
| cpu_balanced | 1M | rayon (borrowed) 0.473 | rayon 0.557 | youpipe (borrowed) 0.532*, youpipe 0.672, std threads 2.80 |
| cpu_unbalanced | 10K | youpipe (Unbalanced) 0.073 | youpipe (default) 0.079 | rayon (borrowed) 0.079, rayon 0.081, std 0.568 |
| cpu_unbalanced | 100K | youpipe (Unbalanced) 0.254 | rayon (borrowed) 0.272 | youpipe (default) 0.258, rayon 0.315, std 0.781 |
| io_async | 500 | futures 9.12 | tokio 9.45 | youpipe 9.57 |
| io_async | 2000 | futures 17.6 | youpipe 18.2 | tokio 18.5 |
| io_async | 5000 | futures 34.0 | youpipe 34.8 | tokio 35.5 |
| io_blocking | 500 | youpipe (512 thr) 8.65 | tokio 8.85 | std 16.9, youpipe (32 thr) 34.1 |
| io_blocking | 2000 | youpipe (512 thr) 12.6 | tokio 12.7 | std 47.8, youpipe (32 thr) 122 |
| mixed_cpu_io | 500 | futures 9.14 | youpipe 9.68 | tokio 10.6 |
| mixed_cpu_io | 2000 | futures 9.32 | youpipe 10.9 | tokio 13.4 |
| real_doc | 1000 | tokio 10.8 | youpipe 11.2 | rayon 38.0 |
| real_doc | 4000 | youpipe 14.1 | tokio 17.2 | rayon 137 |
| real_web | 500 | youpipe 12.0 | tokio 12.8 | futures 13.1 |
| real_web | 2000 | youpipe 22.9 | tokio 27.7 | futures 29.7 |

### Reading the results

- **Balanced CPU, four readings** (`rayon` = fresh-input lifecycle aligned
  with youpipe's ownership API; `rayon (borrowed)` = idiomatic borrow;
  `youpipe` = owned; `youpipe (borrowed)` = `pipe_ref`, idiomatic borrow):
  rayon wins 1K on both (fixed setup, its caller-inline fork-join); youpipe
  wins the 10K–100K middle on every pairing, widest at 100K (−21 % for
  borrowed-vs-borrowed, −39 % vs aligned rayon). At 1M rayon's borrowed row
  wins (bandwidth-bound; ~470 µs, ±3 %), but `pipe_ref` closes most of the
  owned row's gap (0.532 vs 0.672 ms) **and collapses the variance**: the
  owned 1 M rows drift ±25 % round-to-round (fresh 8 MB clone + in-region
  free; the documented large-batch measurement trap), while `pipe_ref`'s
  spread is ±2.5 % ([518–543] µs over 5 rounds) — with nothing cloned or
  freed in-region there is no allocator/system state to drift on. The
  borrowed and owned youpipe rows otherwise sit on the same execution core
  (all four 2026-09 re-run medians within noise), so the lifecycle caliber,
  not the engine, is what separates them. Equal-chunk hand-threading is
  5–10× behind everywhere: 32 spawns per call, and no stealing.
  (* `youpipe (borrowed)` cells: separate 5-round run, 2026-09 `pipe_ref`
  verification; other cells from the 9-round dataset.)
- **Skewed CPU**: with `Workload::Unbalanced` youpipe beats both rayon
  readings at 100K (0.254 vs 0.272/0.315 ms) and both are ~3× ahead of
  static chunking, which strands the 10 % heavy items in whichever chunks
  they landed in. At 10K youpipe's adaptive oversplit already handles the
  skew (the `Unbalanced` knob adds nothing at that size).
- **Async IO is a near-tie** — youpipe multiplexes over the same tokio
  runtime: ±2 % vs tokio (crossing ahead at ≥2K items as channel throughput
  stops mattering), 2–5 % behind `futures::stream`, the lightest async
  *combinator* stack. futures' mixed_cpu_io lead has the same cause: it runs
  the CPU stage inline on runtime workers. That is fine at 100 ns/item CPU,
  and the reason youpipe exists is everything it can't do there: fences,
  cancellation, ordered output, dedicated CPU-pool isolation, backpressure
  across *stages* rather than futures.
- **Blocking IO is a configuration story**: correctly oversubscribed, youpipe
  ≈ tokio `spawn_blocking` (same 512 threads); at the default 32 threads the
  waits serialize (122 ms @ 2K). The chart keeps that failure visible on
  purpose — blocking stages must size the pool, not the framework.
- **Realistic pipelines** are where the streaming engine pays off: 3-stage
  sync+async chains beat hand-written tokio channel plumbing by 17-19 % at
  the larger batches (fewer tasks, pooled scheduling, mixed-mode channels)
  and beat rayon by ~10× once IO blocks its workers.

## Perf-event counter measurement (`perf/counter-bench`)

`perf/counter-bench` runs the same bench code under Linux perf hardware
counters (instructions / cycles / ref-cycles / cache-misses / …) instead of
wall time, via the standalone `criterion-perf-counters` crate — a maintained
fork of criterion-perf-events re-targeted at criterion 0.8 and extended with
process-wide per-thread counters (upstream counts the main thread only,
which for a pool library measures the coordinator and misses the workers).
Threads that spawn and exit inside one measurement window are invisible, so
channel benches can't use it; plain `b.iter` only (`BatchSize::PerIteration`
windows multiply the per-window `4 × n_threads` counter syscalls by the
iteration count and inflate fast benches).

```sh
cargo bench --manifest-path perf/counter-bench/Cargo.toml --bench perf_events
PERF_EVENT=ref-cycles cargo bench --manifest-path perf/counter-bench/Cargo.toml
perf/counter-bench/run-drift-exp.sh   # N runs per event + drift summary table
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
