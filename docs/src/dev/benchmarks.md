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
its per-round medians and grades every delta with two signals: *stable*
(the delta clearly exceeds the observed round-to-round spread with every
round leaning the same way) and *dominant* (the `dom b` column — how many
of the na×nb cross-side round pairs side b won; full separation survives a
single outlier round, which the spread scale cannot: one slow round inflates
a side's spread to ~30 % and buried a real −19.9 % improvement as `noise`,
with the improved side winning all 25/25 pairwise rounds).
`--fail-on-regression` trips on stable-or-dominant regressions.

```sh
# full two-sided A/B, 3 interleaved rounds (both sides build once)
perf/bench-suite/bench_ab.sh -a base=9b31fb0 -b new=HEAD

# drift-sensitive families: isolated per-id interleaving, extra rounds
perf/bench-suite/bench_ab.sh -a base -b wt -r 5 --per-id \
    'stream_pipeline/single_stage_ordered' 'with_fence'


# same-binary runtime-knob A/B (scheduler-class changes: recompiles swing
# tight benchmarks ±30 % through pure code layout)
perf/bench-suite/bench_ab.sh -a off=wt -b on=wt \
    -E off=YOUPIPE_ONPOOL_HYBRID=0 -E on=YOUPIPE_ONPOOL_HYBRID=1 \
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

### Input materialization: `pipe(0..n)` vs `pipe_range` (`sync_lightweight_input_materialize`)

`pipe(items)` materializes any non-`Vec` input on the calling thread before
the parallel phase — `(0..n).collect::<Vec<_>>()` is a serial O(n) iota fill
(a `Vec` input rides std's `vec::IntoIter` collect specialization for free,
so this caliber only bites range/iterator inputs). Quantification group
(2026-09-27, quick config, criterion medians; `borrowed_floor` = `pipe_ref`,
engine only):

| Shape | 1M | 4M |
| --- | --- | --- |
| `range_input` (`pipe(0..n)`) | ~584–624 µs | ~16.8–17.2 ms |
| `vec_input` (`pipe(v.clone())`) | ~536–549 µs | ~19.4–21.7 ms |
| `borrowed_floor` (`pipe_ref`) | ~176–177 µs | ~1.20–1.24 ms |
| `range_gen` (`pipe_range(0..n)`) | ~175 µs | ~0.97 ms |

The owned rows are one-shot-cost calibers (fresh buffer + free inside the
timed region): criterion's back-to-back sampling pays the mmap/fault churn
of a fresh 8–32 MB buffer per iteration, so the medians sit far above a
steady-state best-of. Attribution probe (standalone binary, best-of-100,
`taskset 1-31`): iota fill alone 167 µs @ 1M / 1.1 ms @ 4M; whole range call
332 µs / 4.2 ms vs engine floor 110 µs / 1.9 ms — **the input's fill +
lifecycle is 56–70 % of an owned range-input call** at those sizes.
`pipe_range(0..n).map(+1).collect()` lands ON the borrowed floor at 1 M
(−72 % vs `pipe(0..n)`) and **beats it by ~21 % at 4 M** (−94 % vs
`pipe(0..n)`): beyond L3 the materialized paths pay the input's DRAM read,
which the generation core never performs.

Regression check (3 interleaved per-id rounds vs the branch point,
`bench_ab.sh -B sync_vs_rayon`): every youpipe family within ±3 % noise;
the only flagged row was the `rayon_nested_saturated/1000` *anchor* (+5.4 %,
0/9 dominant — layout/drift wobble on untouched rayon code). Horizontal
`cpu_balanced`/`cpu_balanced_readback` two-binary alternating A/B (5 pairs ×
2 rounds, `taskset 1-31`): pooled +0.1…+1.0 % at 1 M/4 M with mixed
per-pair signs — no regression. Miri (Tree Borrows) covers the generation
cores' guards via the `pipe_range` integration tests.

`pipe_range(range)` therefore removes the input buffer entirely: the item at
index `i` IS the index, generated inside the leaves (`RangeGenStrategy` /
`par_range_gen_*` over the same `hybrid_dispatch`, input handle `IN = ()`) —
no input allocation, no serial fill, no input cache traffic. Allocation
proof: `tests/pipe_range_alloc.rs` counts N-sized allocations — exactly 1
(the output buffer) vs 2 on the materialized path. Filter chains and the
fallible terminal materialize once at the terminal (documented fallback —
same fill `pipe(range)` always paid). If you already own a `Vec`, `pipe(v)`
stays zero-copy; a `pipe_vec` constructor would add nothing.

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

The owned + filter caliber (`youpipe_try_filter_owned`, `MAY_FILTER == true`
→ the range-tree path) was added 2026-09 together with its port off the last
`Vec::split_off` tree: **−18.3 % @ 10K, −19.9 % @ 100K** (5 interleaved
rounds; the 100K base side had one outlier round that inflated its spread —
every post-change round beat every pre-change round).

### Filter-chain collect: leaf pre-allocation, honest merge cost, count-then-place, write-then-compact (2026-09-27)

Filter chains (`MAY_FILTER == true`) cannot use the index-based core
(output cardinality is unknown up front), so leaves build per-leaf `Vec`s
and the tree concatenates them. Three rounds of work on that path:

**Leaf pre-allocation** (rayon's shape): `filter_leaf` / `filter_try_leaf`
grew from `Vec::new()` and `join_fused_collect_by_ref` used
`filter_map(..).collect()` — log-many reallocs + partial memcpys per leaf —
while sibling `join_fused_try_collect_by_ref` already pre-allocated.
Unified to `Vec::with_capacity(leaf_input_len)` (all-survive is the exact
worst case; over-allocation bounded by the leaf input when items are
filtered out). Isolated per-id A/B vs the branch point (6 interleaved
rounds, 33 % selectivity): **100K −6.2 % borrowed / −7.1 % owned / −4.5 %
try+owned** (dominant 25–32/36 pairwise), **1K owned −4.6 % stable**; 10K
is a wash (−0.9 %…+2.9 % across sessions, 0–21/36 — which 10K row leans
+2…3 % flips between sessions; fused recompile layout noise covers it,
while every 100K row improves in every session: the post-landing
confirmation run read −13.9 % borrowed / −6.4 % owned @100K, −6.4 %
@1K owned).

**Merge cost, honestly stated.** The range-tree comments claimed "each
surviving item moves exactly once"; in reality every internal node's
`l.extend(r)` reserves and memcpys one child's `Vec` — O(depth) moves per
survivor, so the merge tree's output-side cost scales with the survivor
count. (The measured win over the old `Vec::split_off` tree came from never
copying the *input* side, not single-move outputs.) Comments corrected.

**`filter_selectivity` group** (keep 10/50/90 %, borrowed caliber, 10K/100K)
anchors the survival-rate axis; the rayon rows double as drift controls for
same-binary knob A/Bs.

**Count-then-place (`YOUPIPE_FILTER_COLLECT=ctp`, opt-in)** —
two-pass by-ref filter collect: pass 1 counts survivors per leaf, a
sequential scan turns counts into output offsets, pass 2 re-runs the chain
and writes survivors straight into one exactly-sized buffer (each survivor
moves exactly once; no per-leaf `Vec`s, no tree merges). Cost is flat in
selectivity: stage closures run TWICE over the input (observable for
side-effecting/interior-mutable closures) plus a second fork/join wave.
Same-binary knob A/B (5 interleaved rounds, 32-core, borrowed):

| shape @100K | merge tree | count-then-place | Δ |
| ----------- | ---------- | ---------------- | --- |
| keep 10 % | 31.9 µs | 45.0 µs | **+41.3 %** |
| keep 33 % (`sync_filter`) | 54.1 µs | 37.3 µs | **−31.1 %** |
| keep 50 % | 77.4 µs | 48.7 µs | **−37.1 %** |
| keep 90 % | 106.6 µs | 52.8 µs | **−50.5 %** |

All eight rows stable (25/25 or 0/25 pairwise); at 10K ctp loses across
the board (+28…+75 %, the fixed second wave dominates) and the owned-path
control reads −0.1 % (the knob must not — and does not — touch it). The
crossover sits near ~25 % selectivity at 100K: the merge tree scales with
survivors, count-then-place with the input. The original hypothesis
("count-then-place for LOW-selectivity batches") is inverted — the knob
pays off for filters that KEEP most items on ≥ 100K batches. Owned /
scoped / try filter paths keep the merge tree: their stages consume items
by value, so a counting pass cannot re-run them. A single-stage-pass variant landed as write-then-compact
(below); it did NOT dominate everywhere, but it wins the large-batch side
of the crossover.

**Write-then-compact (`YOUPIPE_FILTER_COLLECT=wtc`, opt-in)** — the
single-stage-pass variant: each leaf runs the chain exactly ONCE and
writes its survivors contiguously into the low end of its slice of ONE
n-slot output buffer (leaf-contiguous, not input-indexed — exact-index
writes would scatter at low selectivity and need per-run metadata for the
compaction); a compaction pass prefix-sums per-leaf counts into final
offsets and block-copies each leaf's contiguous segment (a segment
already in place skips the copy; payloads ≥ 256 KB copy through a
parallel join-tree wave into a SEPARATE exactly-sized buffer — an
in-place parallel sweep is not interleaving-safe, see the section comment
in `fused.rs`). One allocation, one pass, one payload move per survivor;
the price is an n-sized (not survivor-sized) output buffer. Final
three-sided same-binary knob A/B (5 interleaved rounds, 32 cores, all
rayon drift controls within ±4 % noise):

| shape | merge tree | count-then-place | write-then-compact | wtc vs merge |
| ----- | ---------- | ---------------- | ------------------ | ------------ |
| 1K keep33 (`sync_filter`) | 13.1 µs | 21.1 µs | 11.5 µs | −12.8 % (25/25) |
| 10K keep 10 % | 12.7 µs | 22.5 µs | 12.4 µs | −2.2 % (noise) |
| 10K keep 50 % | 16.6 µs | 23.4 µs | 16.7 µs | +0.7 % (noise) |
| 10K keep33 (`sync_filter`) | 14.0 µs | 21.7 µs | 15.1 µs | +7.9 % (noise, spread 14 %) |
| 10K keep 90 % | 17.9 µs | 23.6 µs | 22.4 µs | **+25.1 % (0/25)** |
| 100K keep 10 % | 30.8 µs | 43.7 µs | 30.8 µs | −0.1 % (noise) |
| 100K keep33 (`sync_filter`) | 53.3 µs | 36.6 µs | 40.3 µs | **−24.4 % (25/25)** |
| 100K keep 50 % | 80.9 µs | 49.1 µs | 55.9 µs | **−30.9 % (25/25)** |
| 100K keep 90 % | 107.1 µs | 54.3 µs | 66.9 µs | **−37.5 % (25/25)** |

Verdict: **the merge tree stays the default.** wtc wins or ties
everywhere except mid/high-selectivity 10K — keep90 +25 % stable — where
the survivor payload (72 KB @ keep90) sits under the 256 KB
parallel-compaction threshold and the driver-sequential sweep pays
cross-core transfers for cache lines the workers just wrote; at 1K the
payload is too small for that to matter, at ≥ 100K the parallel wave
amortizes it. A size gate would need data points between 10K and 100K to
place and only buys that one window — not worth the complexity (same call
as the ctp crossover). ctp keeps the ≥ 100K mid/high-selectivity crown
(−10…−23 % vs wtc: exact-size buffer, no compaction pass) but loses
everywhere else. Per-shape guidance: `merge` for small/mid batches at
mid/high selectivity; `wtc` for ≥ 100K batches at any selectivity, and
for small batches; `ctp` for ≥ 100K batches that keep ≳ 30 %. The ctp
boolean knob was promoted to the three-way `YOUPIPE_FILTER_COLLECT`
(unset/`merge`/`ctp`/`wtc`; invalid values panic) accordingly.

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


### Reduce terminals vs collect-then-sum (`sync_reduce`, `x+1` map, 2026-09-28)

The aggregation terminals (`reduce`/`fold`/`sum`/`count`/`min`/`max` and
the fallible twins) never allocate the output `Vec`: each leaf folds its
range into a partial and the tree combines partials through `join`
returns; the chunk driver publishes one partial per chunk into a one-shot
cell (design history in dev/scheduler.md). Caliber: borrowed `x+1` map over
a warm slice (owned side pays a fresh clone, identical on every A/B side).
5 interleaved per-id rounds — the family is new, so there is no base side;
the old path (`youpipe_collect_sum_borrowed`) and `rayon_sum` run in the
same rounds as drift anchors.

| Size | youpipe `sum` (borrowed) | collect+sum (old path) | rayon `.sum()` | sequential |
| ---- | ------------------------ | ---------------------- | -------------- | ---------- |
| 1K   | ~10.3 µs | ~9.4 µs  | ~39.2 µs | ~0.3 µs  |
| 10K  | ~10.6 µs | ~12.7 µs | ~53.7 µs | ~3.0 µs  |
| 100K | ~12.2 µs | ~46.6 µs | ~69.5 µs | ~29.4 µs |
| 1M   | ~29.9 µs | ~235.7 µs| ~121.2 µs| ~293.0 µs|

vs the materialize-then-fold path: **−16 % @ 10K, −74 % @ 100K, −87 % @
1M**; at 1K the reduce core medians +9.5 % but with 18 % bimodal round
spread (8.8/10.3 µs alternating; 2/5 rounds beat the old path) — borderline
noise, below the size-gating threshold, no gating done. vs rayon:
**−74…−82 % at every size** (the cpu-heavy collect caliber narrows that —
this is the lightweight end where the removed output buffer dominates).
Owned input pays the fresh clone (~17.3 µs @ 10K, ~74.5 µs @ 100K) —
identical lifecycle on every side of any owned A/B.

### On-pool nested terminals (`sync_nested_on_pool`, cpu_heavy per item)

Fused terminals reached from *inside* a worker of the driving pool —
`pool.submit` tasks or stream stage closures calling `.collect()` —
benchmarked as one submitted job (`nested_single`: isolates batch ramp-up
with P−1 workers free) and as P concurrent submitted jobs
(`nested_saturated`: every worker is a driver waiting on its own latch
while stealing), against same-shaped `ThreadPool::spawn` + nested
`par_iter` on a same-sized rayon pool.

The on-pool hybrid dispatch (`Stealing` latch) is gated by batch regime —
same-binary knob A/B (`YOUPIPE_ONPOOL_HYBRID`, 5 interleaved rounds, 32
cores):

- `nested_single/100K` **−3.5 %** (25/25 dominant) — the ramp-up win the
  change targets;
- `nested_saturated/100K` +2.0 % lean (within spread, 0/25) — every worker
  nesting large batches is exotic;
- 1K sizes are pure noise *with the gate*; ungated hybrid there measured
  **+430 %** (`nested_saturated/1K`, P×P tiny chunks collapsing the single
  injector) and +3 % (`nested_single/1K`) — the recompile-based A/B that
  motivated the gate.

Methodology note: the first recompile A/B of this change produced a
contradictory second session (+18 % on `nested_single/100K` **and +31 % on
the untouched rayon anchor**) — pure code-layout noise on a recompiled
binary, exactly the trap that motivates the knob methodology. Scheduler-
class changes get verified with `-E` same-binary env A/Bs (`bench_ab.sh -a
off=wt -b on=wt -E off=YOUPIPE_X=0 -E on=YOUPIPE_X=1`), not recompiles.
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

### Adjacent-sync fusion A/B (`sync_fuse`, 2026-09-29)

Evidence bench for the falsified "fuse adjacent sync stages in streaming
topology" idea (verdict + probe data in dead-ends.md). Same-binary knob A/B
(`YOUPIPE_SYNC_FUSE_VARIANT=split|merged`, 5 per-id isolated interleaved
rounds, median): `split` runs the sync stages as separate worker populations,
`merged` hand-composes them into one closure — identical async/fence/cancel
tails on both sides.

| shape              | split @1K | merged @1K | split @100K | merged @100K |
| ------------------ | --------- | ---------- | ----------- | ----------- |
| cancel_pair_cpu    | 279 µs    | 317 µs     | 24.6 ms     | 29.8 ms     |
| cancel_quad_cpu    | 260 µs    | 315 µs     | 22.2 ms     | 29.9 ms     |
| async_pair_cpu     | 482 µs    | 521 µs     | 236.9 ms    | 25.1 ms     |
| fence_pair_cpu     | 1.17 ms   | 1.64 ms    | 36.2 ms     | 209.1 ms    |

Stable-regime rows (pure-sync chains forced onto streaming by `with_cancel`):
merging populations regresses monotonically with per-channel worker count
(8 W → 222 ns/item, 16 W → 246 ns, 31 W → 298 ns) — channel hops pipeline,
so fusion only concentrates MPMC/collector contention. The async/fence rows
are dominated by a bistable convoy pathology unrelated to fusion (a zero-CPU
`bump.fence.bump` chain costs 226 ms @100K; the two-sync-prefix async chain
samples bimodally 23↔244 ms) — tracked as todo P1 #4, with `fence_infra`
kept in the family as the canary.

### Expand-Heavy — owned `Vec` vs push-style expansion (`expand_heavy`)

Matrix: fan-out ∈ {4, 64} × cost ∈ {cheap, cpu} at 10 K inputs; throughput
counts output elements. `owned_vec` = `expand(Fn -> Vec)` (one malloc +
free per input item), `push_emit` = `expand_emit(Fn(I, &mut Vec))` (per-worker
reused scratch buffer); rayon `flat_map` / `flat_map_iter` rows are the
external anchors for the two shapes.

Verdict (2026-10, single-session interleaved runs of the `fanout=4/cost=cheap`
pair — 12 alternating rounds, `taskset 1-31`):

- **Allocation evidence is deterministic** (counting global allocator,
  `tests/expand_alloc.rs`): `expand` performs ≥ 1 `malloc` per input item
  (1024 items → ≥ 1024 allocations); `expand_emit` performs a constant
  independent of item count (8× the inputs adds ≤ a couple of output-`Vec`
  growth reallocs).
- **Wall clock: the win is bounded by the malloc/channel cost ratio, and on
  glibc it is small.** With glibc's per-thread tcache absorbing same-size
  small frees (~50–100 ns per input), the eliminated malloc is minor against
  the ~250 ns per *output* channel handoff — paired-round medians showed
  push ahead by ~2.5 % at `fanout=4/cost=cheap`, inside a ±20 %
  environmental noise floor (per-round deltas flipped sign; the session's
  load average included the benches themselves). The original premise that
  allocator traffic *dominates* expand-heavy loads is **falsified for
  glibc** — it only holds under allocators without thread caches, or when
  expansions are large enough to bypass tcache bins.
- High fan-out amortizes the per-input malloc over more outputs; the
  structural gap to rayon in this group is the streaming engine's channel
  infrastructure (see `mixed_load` above — since solved for pure sync chains
  by the fused pass-through, see `guide/stream.md`), not the expansion
  API shape.


### Channel Throughput
Two-thread ping-pong (1 producer, 1 consumer, `u64`), all bounded rows at
capacity 256. Caliber note (2026-10 fix): the old table compared bounded
crossfire against **unbounded** `std::sync::mpsc::channel` — the unbounded
channel does no capacity accounting and never blocks the producer, so that
column was not a like-for-like row. Rows are now named by caliber
(`*_bounded`/`*_unbounded`, `*_mpmc`/`*_mpsc`); numbers are medians of 5
interleaved rounds, `taskset 1-31` — the 1P1C shape is placement-sensitive
(unpinned runs collapse up to −58 %), so pinning is mandatory for this group.

| Size | youpipe_mpmc | youpipe_mpsc | crossbeam_bounded | std_mpsc_bounded | std_mpsc_unbounded |
| ---- | ------------ | ------------ | ----------------- | ---------------- | ------------------ |
| 10K  | 58 Melem/s   | 35 Melem/s   | 26 Melem/s        | 41 Melem/s        | 65 Melem/s         |
| 100K | 85 Melem/s   | 44 Melem/s   | 26 Melem/s        | 58 Melem/s        | 92 Melem/s         |

Readings:

- Same-caliber bounded-MPSC: `std sync_channel` beats crossfire's mpsc flavor
  by ~17–31 % in this uncontended 1P1C shape. The MPSC-flavored collector
  channel was adopted from **in-pipeline** profiling under N-producer
  contention (recv-side CAS dominates there, see `handoff/channel.rs`); this
  microbench has no recv-side contention, so it measures the pure
  cache-line-transfer/wake path instead. The in-pipeline follow-up was run
  and falsified (e1684fc → revert aa842a6): burst-boundary park round-trips
  regress every real-collector shape — see [dead-ends.md](dead-ends.md).
- The inter-stage MPMC channel (crossfire, 85 Melem/s at 100K) beats every
  bounded alternative here — the "nothing left to squeeze" conclusion for
  the middle channels stands (see scheduler.md).
- `std_mpsc_unbounded` keeps its historical role as the no-backpressure
  ceiling reference only.

### hotpath instrumentation round (2026-09)

With the `hotpath-profile` binary of `crates/youpipe-bench` (p50 percentiles over `HOTPATH_OUTPUT_FORMAT=json`
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

## Fence forwarders as leased pool jobs (2026-09-28)

The five `FenceLink` spawn sites replaced their per-run `std::thread::spawn`
with a leased pool job routed through `StreamCtx::spawn_stage_jobs` (pool
mode only; the dedicated-thread fallback keeps the OS thread), and
`try_exec`'s lease reservation gained a `fences` term (see
[streaming.md](streaming.md)). Verified with 5-round isolated interleaved
A/B (`bench_ab.sh -1`, `stream_pipeline` families, base b2e25e8):

- `with_fence/1K` **1.603 ms → 1.542 ms, −3.8 %** (25/25 dominant, spreads
  1.7/2.6 %) — the removed thread spawn+join is the ~61 µs/fence fixed
  cost, matching the 30–80 µs estimate from the feeder-side removal.
- `with_fence/100K` −0.4 % noise: the fixed saving is ~0.03 % of a 209 ms
  run, invisible by design.
- No-fence regression check (`single_stage_unordered`, `multi_stage_2` at
  1K/100K): all noise (±0.4 %, no dominance).

Correctness: without the `fences` term the shape
`stage_with(6) → fence → stage_with(1)` on an 8-thread pool admits
feeder + 6 + forwarder + 1 = 9 parking jobs; the injector FIFO pops
[feeder, stage-1 ×6, forwarder] onto the 8 workers and the queued stage-2
job wedges the run with every worker parked on a full channel.
`test_fence_forwarder_counts_in_parking_lease_no_deadlock` hits the 30 s
watchdog without the term and completes (dedicated-thread fallback) with
it.

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
  youpipe's holds ~34 GB/s. Resolved 2026-09-25 by two same-day
  attributions (see "NT-store attribution" and "Attributing the 2M/4M
  fused-collect gap" below): the dominant term is the plain output
  stores' read-for-ownership + L3 pollution — non-temporal leaf stores
  (`YOUPIPE_NT_STORE`) close and reverse the gap (2 M 0.82 vs rayon
  0.83 ms, 4 M 1.57 vs 1.68 ms); a secondary worker park/wake
  occupancy loss (~1-4 pt, causal via the spin-rounds knob) remains.
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

### NT-store attribution (2026-09-25): the ≥2 M gap was output RFO

Hypothesis (resolved P0): the collect output buffer is written once and
never read (the bench only black-boxes and drops it), so every 64 B line
pays a read-for-ownership plus L3 pollution before its DRAM writeback.
The fused leaves therefore grew a runtime knob: `YOUPIPE_NT_STORE=1`
writes eligible 8-byte outputs with `movnti` streaming stores (x86_64
SSE2 baseline; eligibility is compile-time, the store is a scalar
drop-in for the leaf's scalar store loop; `sfence` orders the weak NT
stores before every leaf exit — success and unwind — ahead of the
completion latch's release). Default off: a consumer that reads the
output right after collect trades a cache hit for a DRAM round-trip.

A/B: horizontal `cpu_balanced`, same binary, two processes alternating
(the knob is a process-level `OnceLock`), 6 pairs × `--rounds 2`
(forward+reversed internal rounds), `taskset -c 1-31`, pooled medians
with per-pair pairing and the rayon column as drift control:

| n | youpipe off→on (ms) | delta | rayon control |
| --- | --- | --- | --- |
| 1 K | 0.010 → 0.010 | −1 % | ±0 % |
| 10 K | 0.012 → 0.012 | +2 % | ±0 % |
| 100 K | 0.053 → 0.048 | +10 % | ±0 % |
| 1 M | 0.48 → 0.41 | +15 % | ±0 % |
| 2 M | 0.95 → 0.82 | +15 % | ±0 % |
| 4 M | 1.91 → 1.57 | +18 % | ±0 % |

Raw JSON under `target/horizontal/nt_ab2/` (not committed). Two
methodology notes: (1) the knob treats any value other than "0" as ON —
a first A/B attempt passing `YOUPIPE_NT_STORE=off` enabled NT on both
sides and measured +0 % everywhere; (2) two pairs of a prior run were
polluted by background build load (rayon control drifted +4–14 %),
which only per-pair pairing + the control column exposed — pooled
medians alone would have read it as a win/loss.

**Read-back follow-up (same day, same caliber): the feared consumer
penalty did not materialize.** New horizontal shape
`cpu_balanced_readback` — same `cpu_work(x, 100)` load, but after
`collect()` the consumer folds the whole output `Vec` inside the timed
region (write-then-immediately-read, the exact shape the default-off
was guarding against). Same-binary two-process A/B, 5 alternating
pairs × `--rounds 2`, `taskset 1-31`, per-pair pairing, rayon column as
drift control; write-once `cpu_balanced` re-run in the same session
(gain holds):

| n | write-once off→on (ms) | delta | read-back off→on (ms) | delta | rayon control (w / r) |
| --- | --- | --- | --- | --- | --- |
| 100 K | 0.052 → 0.047 | +9.3 % | 0.081 → 0.072 | +11.5 % | +0.7 % / +0.5 % |
| 1 M | 0.486 → 0.419 | +12.3 % | 0.656 → 0.580 | +11.6 % | ±0 % / −0.3 % |
| 2 M | 0.962 → 0.830 | +13.6 % | 1.281 → 1.109 | +13.3 % | −0.3 % / −0.5 % |
| 4 M | 1.947 → 1.612 | +17.8 % | 2.569 → 2.192 | +14.8 % | −0.1 % / +0.2 % |

Raw JSON under `target/horizontal/nt_readback_ab/` (not committed).
Mechanism: the consumer's re-read is a *sequential, prefetch-friendly*
sweep, while the RFO elimination pays off during the 31-thread parallel
phase — the asymmetry holds at every measured size ≥ 100 K.

**Default-tier decision (todo, closed 2026-09)**: NT wins both consumer
shapes, so the knob graduated from opt-in to an **auto tier**:
`nt_store_enabled` resolves per *whole-batch* output size (a leaf only
sees its chunk), ≥ 8 MiB → NT, threaded down as a leaf `bool`. Env
became a tri-state — unset = auto, `"0"` = force off, `"1"` = force on,
any other value panics (the `=off` trap above is now a loud failure).
8 MiB rather than 800 KB: the sub-threshold gains (+9–11 % at 100 K)
shrink toward the ±2 % noise floor where the balance gets
machine-dependent; known write-once shapes below the threshold can
force it on. The remaining occupancy deficit (~1–4 pt) stays tracked as
todo P1 #3.
## Attributing the 2M/4M fused-collect gap (2026-09-25)

Single-shape single-library runs of the horizontal binary itself
(`--libs X --batches N`, taskset 1-31, A-B-A-B interleaved; iteration counts
from the JSON `iters` field normalize whole-process `perf stat`). Machine:
16 physical / 32 SMT threads, 32 MB shared L3 + 32 × 1 MB L2.

**Wall caliber the attribution targets**: 2M youpipe 1.011 / rayon 0.928 ms
(+9.0 %), 4M 2.003 / 1.784 ms (+12.3 %) in the morning session; a parallel
same-binary session (alignment A/B) measured +13…+14 % @ 2M and +12 % @ 4M.
Later the same day both sides drifted +25 % absolute with the gap compressed
to +1…+3.5 % — the youpipe deficit is a fixed µs-scale per-iteration cost,
so it looms large exactly when compute is fast (see the frequency note in
"Reading the results" history). Counter ratios below are from that drifted
session and are frequency-robust.

| per-iteration counter (2M / 4M) | youpipe / rayon | verdict |
| --- | --- | --- |
| cycles | 0.86 / 0.90 | youpipe burns *fewer* core-cycles |
| instructions | 0.96 / 0.96 | same work, youpipe slightly leaner |
| L1D-miss → L2 accesses (`l2_cache_req_stat.dc_access_in_l2`) | 1.00 / 0.97 | parity |
| store RFOs into L2 (`l2_request_g1.change_to_x`) | 0.94 / 0.78 | parity or better |
| demand DRAM fills (`ls_dmnd_fills_from_sys.dram_io_near+far`) | 0.53 / 0.73 | both trivial (see below) |
| L1D TLB misses hitting L2 (`ls_l1_d_tlb_miss.all_l2_miss`) | 0.44 / 0.55 | parity or better |
| task-clock (busy CPUs of 31, 2M) | **26.7 vs 30.7** | **the gap** |
| context-switches / iteration (2M) | **99–102 vs 13–19** | park/wake churn |
| cpu-migrations / iteration (2M) | **7.6–8.0 vs 0.5** | parked workers land on cold cores |
| page-faults / iteration | 42.5 vs 43.2 | allocator parity |

**Falsified premise — "≥ 32 MB leaves the cache-resident regime"**: demand
DRAM fills measure 2–9 K lines per iteration against 256 K/512 K lines
touched (≤ 1.7 %). The 64 MB combined L2+L3 hierarchy absorbs the whole
32–64 MB R+W working set in the bench's steady state on this machine, for
both libraries. Chunk-boundary alignment indeed cannot move the gap
(`YOUPIPE_ALIGN_CHUNKS` A/B ±0.3 %, falsified). The NT-store prediction was
**wrong** — corrected the same day by the causal same-binary A/B ("NT-store
attribution" above): +15–18 % wall at 1–4 M, gap reversed. The NT "no
improvement" observed during this attribution session was the invalid
both-sides-on round (the knob treats any value other than "0" as ON; the
off side had passed the literal string "off"). Lesson: core-side counter
parity (demand fills, L2 RFO counts) does not capture the store path's
ownership/writeback latency that NT bypasses — a same-binary causal knob
outranks counter-based exclusion.

**Occupancy deficit — a real but secondary component** — `perf record`
(999 Hz, dwarf) shows 94.3 %
of youpipe's on-core samples in `par_index_rec_by_ref` (the leaf loop; the
scheduler, injector, and latch never reach 0.2 %), versus rayon's 90.2 % in
its `bridge` leaf plus ~4.4 % in `join_context`/`with_handle` spinning.
youpipe executes its (slightly fewer) instructions with *fewer* total
core-cycles yet finishes later: the missing time is spent halted. Idle
workers exhaust the 32-spin + 32-yield round window and condvar-park;
waking them costs futex latencies and lands them on migrated cores, ~4 of
31 CPUs' worth of average occupancy per iteration — matching the +12–15 %
wall-gap *shape* and initially read as the whole gap. The NT-store A/B
showed the store path dominates (~13 pt); occupancy is worth a measured
1–4 pt on top (causal spin-knob check below) and the two partially overlap
(NT shortens leaf duration, which also shortens the straggler tail that
wake latency gates). Rayon avoids the loss by never parking mid-iteration —
burning +11–16 % more cycles (spin) per iteration than youpipe.

**Causal check (same-binary knob A/B)**: `YOUPIPE_SPIN_ROUNDS=2048
YOUPIPE_YIELD_ROUNDS=2048` (workers stay hot across the inter-iteration
gap) improves youpipe at every shape — 1M −7.5 % → −11.9 %, 2M −2.3 % →
−5.8 %, 4M +3.5 % → +2.3 % vs rayon — confirming park/wake as a real cost,
but also that it is only worth 1.2–4.4 pt of the ~13 pt gap in that
session. Widening the window indiscriminately burns idle CPU (the
`ROUNDS_SPIN` history), so the open lever is structural: keep workers
across a batch's back-to-back iterations (or shave the tail chunk) instead
of stretching the idle window. Status (2026-09-26): the keep-warm side is
resolved by opt-in core pinning (`ComputePool::new_pinned` — −5.1…−7.2 %
on this exact caliber, no burn; see scheduler.md "Worker affinity"), and
re-measuring the gap under the NT-store auto default found it collapsed:
1M +0.6 %, 2M +1.5 %, 4M −6.8 % (7 interleaved rounds, taskset 1-31).

Harness note: attribution initially used a standalone runner
(`youpipe-bench`'s `horizontal-counters`, same workload); its wall times
proved layout-lottery-bound (youpipe 0.94–1.31 ms across binaries, rayon
0.87–1.13 ms, consistent with the documented ±30 % code-layout noise), and
a per-iteration console write in its timing loop widened the
inter-iteration gap enough to triple youpipe's context switches (+30 %
wall) — both pitfalls are now documented in the runner's source, and all
conclusive numbers above come from the layout-canonical horizontal binary.

## Perf-event counter measurement (`crates/youpipe-bench`)

`crates/youpipe-bench` (the opt-in lab-bench crate) runs the same bench code under
Linux perf hardware counters (instructions / cycles / ref-cycles / cache-misses /
…) instead of wall time, via the workspace's `youpipe-criterion-perf-counters` crate
— a maintained fork of criterion-perf-events re-targeted at criterion 0.8 and
extended with process-wide per-thread counters (upstream counts the main thread
only, which for a pool library measures the coordinator and misses the workers).
Threads that spawn and exit inside one measurement window are invisible, so
channel benches can't use it; plain `b.iter` only (`BatchSize::PerIteration`
windows multiply the per-window `4 × n_threads` counter syscalls by the
iteration count and inflate fast benches).

```sh
cargo bench -p youpipe-bench --bench perf_events
PERF_EVENT=ref-cycles cargo bench -p youpipe-bench --bench perf_events
crates/youpipe-bench/run-drift-exp.sh          # N runs per event + drift summary table
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

## Deterministic instruction counts (`crates/youpipe-gungraun`)

`crates/youpipe-gungraun` counts executed instructions under the Valgrind
simulator (gungraun, the iai-callgrind successor) instead of sampling hardware
counters — one run per benchmark, no clock, no statistics. Where the perf-event
counter rows above still show 0.5–3 % cross-run CV on pool benches (scheduling
leaks into even instruction counters on a real machine), the valgrind
simulator's serialized scheduler removes that source entirely:

| shape | worst run-to-run Ir drift |
| --- | --- |
| youpipe channel rows (MPMC / MPSC ping-pong) | **0** — exact to the instruction |
| crossbeam channel row | **0** |
| std `sync_channel` row | ±0.04 % |
| fused `pipe` rows | 0 .. 0.003 % |
| rayon rows | ±0.06 % (timer-based idle sleeps) |
| `stream` rows | ±0.02 % |

Methodology essentials (full details in the crate's Readme):

- **Count-all-threads caliber.** gungraun's default per-function Callgrind
  toggle is per-thread state — pool workers never enter the bench function and
  are invisible (measured: a 100 K `pipe().collect()` counted only ~26 kIr of
  driver dispatch). The benches therefore run `EntryPoint::None` +
  `--collect-atstart=yes` and report **process totals**; every row of a group
  executes identical setup (`both_pools()` spawns a 4-worker youpipe pool and
  a 4-worker rayon pool even where a row uses neither) so the fixed offset
  cancels in `compare_by_id` deltas. Read deltas, not absolutes.
- **Pinned 4-worker pools** on both sides: default pools size to
  `available_parallelism()`, which would make counts host-dependent. The
  pinned pools still exercise the full dispatch/steal/wake surface.
- **No async stages**: tokio timers make counts time-dependent.
- Regression gating: `--save-baseline=main` / `--baseline=main
  --callgrind-limits='ir=2%'` exits 3 on regression — with the drift above, 2 %
  is comfortably tight and one run suffices (vs 3–5 interleaved criterion
  rounds for the same confidence).

First readings (process totals, deltas are the signal): cpu_heavy 1 K —
youpipe +8.7 % Ir vs rayon (the known fixed dispatch cost); cpu_heavy 100 K —
youpipe −2.3 %; light 100 K — youpipe −8 % vs rayon, +21 % vs sequential;
`stream` single-stage pass-through +3 % vs sequential at 10 K.

```sh
cargo bench -p youpipe-gungraun            # ~1 min, all three bench files
cargo bench -p youpipe-gungraun -- --save-baseline=main
```

## Lab bench crate layout (`crates/youpipe-bench`)

The four former standalone bench crates were consolidated (2026-10) into one
opt-in lab crate, `crates/youpipe-bench`: `--bench perf_events` (the
perf-counter criterion bench above), `--bin file-encrypt` (real-disk mixed
CPU/IO, recorded results in `results-file-encrypt.txt`), `--bin
hotpath-profile --features hotpath` (the hotpath driver). The fourth
(`pipeline-bench`, simulated-IO 5-strategy document pipeline) was deleted
rather than merged: its scenario already lived on as the `real_doc` row of
`benches/horizontal` (interleaved rounds, stricter methodology), and its
"youpipe all-sync 3 stages" row is moot since pure-sync `stream` chains now
fuse onto the fused core.
