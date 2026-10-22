# Fused and Sync Benchmarks vs rayon

## CPU-heavy `pipe_ref()` vs rayon (`sync_cpu_heavy`, 100 iters/item, borrowed input)

| Size | youpipe | rayon   |
| ---- | ------- | ------- |
| 1K   | ~10 µs  | ~40 µs  |
| 100K | ~53 µs  | ~146 µs |

youpipe leads at every size (2026-09-05 rerun, quick config). The historical "1K trails rayon" reading came from `std::thread::available_parallelism()` re-reading the cgroup CPU-quota files on every call, ~26 µs of openat/statx/read syscalls per call on this machine (strace: 5 openat + 8 read + 5 statx per call). Two calls sat on every fused terminal, `PipelineConfig::default()` at `pipe()`/`pipe_ref()` construction and `resolve_exec_pool` at the terminal, together ~50 µs of pure syscall overhead on a 57 µs measurement. Both now go through `crate::num_cpus()`, a process-wide `OnceLock` cache; affinity changes after first use are intentionally invisible, and rayon sizes its global pool once for the same reason. The find came from bisecting a horizontal-suite regression to the commit that added the second call: the fixed cost scaled the whole fused family by a constant, invisible to instruction counters.

Two earlier changes removed ~11 µs of genuine fixed overhead and remain load-bearing:

1. `Workload::Balanced` is now the default (was `Unbalanced` → oversplit 8). For 1K/10K batches `n / num_threads ≤ 1024`, so the adaptive path picks `oversplit = 1` (32 leaves) instead of 8 (256 leaves), far fewer internal nodes to dispatch.
2. Spin-then-park for the off-pool wait (`CountLatch::wait_spin`): the hybrid driver tight-spins on the `counter` atomic for a bounded budget (4096 PAUSE iters ≈ 100–150 µs) before acquiring the latch's mutex. In the short-wait regime the last chunk's `fetch_sub` lands inside the spin window, so the condvar park/notify syscall (~10–20 µs of fixed overhead) is skipped entirely; long waits still fall through to the condvar. The mutex acquire is load-bearing for soundness: it serializes against the last chunk's in-flight `LockLatch::set`, preventing a use-after-free (spinning on the counter and returning directly would race the latch free against that access; observed as SIGSEGV).

What remained after those two was first attributed to the inject + wake cascade, wrongly, as the `available_parallelism` find above showed. Closing it by injecting a single root job (mirroring rayon's `join` unfold) regressed: the log2(num_threads) ramp-up through work stealing cost more than the per-chunk overhead it saved, and hotpath confirmed `steal` at ~98 ns is off the critical path. The real wins were:

1. `Box<[ChunkJob]>` consolidation: all `num_threads` chunks share one heap allocation (1 instead of N), saving the per-chunk malloc/free.
2. Driver-inline participation (mirrors rayon's calling thread): chunk 0 runs on the off-pool driver while the pool handles chunks 1..N. This saves 1 injector push and reduces the condvar wake cascade by 1. Guarded by `chunk_splits == 0` (small/medium batches where the chunk is a single leaf) to avoid memory-bandwidth contention on large memory-bound workloads.

Together these shaved ~5–8 % off 1K–10K `collect` batches.

### Driver work-assist via reserve chunks (2026-09; kept, no measurable win)

The hybrid dispatcher withholds one tail chunk from the injector (`ASSIST_RESERVE_CHUNKS`) and the off-pool driver executes it from its wait loop (`CountLatch::wait_spin_assist`), the natural extension of driver-inline chunk 0. A/B over 3+2 interleaved rounds found no measurable wall-time change at reserve 1 or 4: under back-to-back benchmark loops most workers stay ready, so the rescue only pays off when workers are absent (pinned by `test_hybrid_driver_assists_when_workers_busy`). Kept at the conservative 1 for that liveness property. Three earlier variants were rejected with lessons (all recorded at the `hybrid_dispatch` reserve block):

1. claim-flag: the driver claims an injected chunk while its `JobRef` stays queued; the worker's no-op `execute` then races the driver's frame teardown, a use-after-free caught by the panic-propagation tests as a misaligned deref. `counter == 0` must imply every injected `JobRef` was fully consumed, that is the teardown's safety invariant.
2. pop-and-execute anything: foreign injector jobs are not guaranteed to run on an arbitrary thread (`in_worker_cold`'s `StackJob` asserts on the worker TLS); executing one unwinds straight through the driver frame, skipping the latch wait.
3. pop + identity check + re-queue foreign: re-queueing reorders the injector's FIFO, which stream pipelines depend on (feeder submitted before its resident stage-worker jobs); a feeder pushed behind them starves while every worker parks on an empty channel recv, a whole-pool deadlock reproduced under the parallel test suite and by `crates/youpipe/tests/hybrid_assist.rs`.

An earlier version silently routed small batches to a serial loop to win this benchmark. That was deceptive, since the API promises parallelism, and catastrophic for expensive per-item work (file IO, crypto) whose small batches would be wrongly serialized. The heuristic was removed (see `prefers_serial` in `crates/youpipe/src/builder/typed/fused.rs`).

## Pipeline fusion (3 stages) vs rayon chain (`pipeline_fusion`, borrowed input)

| Size | youpipe fused | rayon chain |
| ---- | ------------- | ----------- |
| 10K  | ~10.5 µs      | ~59 µs      |
| 100K | ~24.7 µs      | ~88 µs      |

Under the borrowed caliber (both sides read a warm slice) the fused chain leads at both sizes, −82 % @ 10K and −72 % @ 100K (2026-09-05 rerun with the `num_cpus` cache). The margin comes from five changes: the sleeping-bitmask rewrite of `wake_any_threads`, moving the `condvar.notify_one` outside the `is_blocked` mutex, the `.cargo/config.toml` perf-friendly `opt-level=3`/`panic=unwind` override, adaptive oversplit (`workload_oversplit`, which drops to `oversplit = 1` for small batches at ≤ 1024 items/worker), and hybrid flat/tree dispatch (`par_index_collect_hybrid`), which injects `num_threads` broad top-level chunks so every worker starts busy at t≈0 with no fork/join ramp-up while each chunk recurses via the tree for distributed stealing. The hybrid alone measured −6.5 % @ 10K and −6.7 % @ 100K.

## Lightweight `pipe()` vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (borrowed) | youpipe (owned, cold clone) | rayon   |
| ---- | ------------------ | --------------------------- | ------- |
| 10K  | ~11 µs             | ~18 µs                      | ~60 µs  |
| 1M   | ~176 µs            | ~4.18 ms                    | ~251 µs |

The borrowed `pipe_ref` row leads rayon at both sizes (−82 % @ 10K, −30 % @ 1M; 2026-09-05 rerun). The owned row's ~4.2 ms is the cold-clone trap, a fresh 8 MB buffer arriving cold-from-RAM, and measures the engine's one-shot input cost rather than engine throughput; that trap is the reason `pipe_ref` exists. Warm-input lightweight went ~1.9 ms (pre-`Slots`) → ~390 µs (slice view) → ~187 µs (perf config + sleeping-bitmask wake + hybrid flat/tree dispatch) → ~176 µs (`num_cpus` cache, which also collapsed the 10K row from ~58 µs to ~11 µs: at 10K the leaf work is so cheap (~0.12 ns/item) that the measurement had been pure fixed dispatch cost, ~50 µs of which was the two cgroup-reading `available_parallelism` syscalls).

## Input materialization: `pipe(0..n)` vs `pipe_range` (`sync_lightweight_input_materialize`)

`pipe(items)` materializes any non-`Vec` input on the calling thread before the parallel phase, and `(0..n).collect::<Vec<_>>()` is a serial O(n) iota fill. A `Vec` input rides std's `vec::IntoIter` collect specialization for free, so this caliber only bites range/iterator inputs. Quantification group (2026-09-27, quick config, criterion medians; `borrowed_floor` = `pipe_ref`, engine only):

| Shape | 1M | 4M |
| --- | --- | --- |
| `range_input` (`pipe(0..n)`) | ~584–624 µs | ~16.8–17.2 ms |
| `vec_input` (`pipe(v.clone())`) | ~536–549 µs | ~19.4–21.7 ms |
| `borrowed_floor` (`pipe_ref`) | ~176–177 µs | ~1.20–1.24 ms |
| `range_gen` (`pipe_range(0..n)`) | ~175 µs | ~0.97 ms |

The owned rows are one-shot-cost calibers (fresh buffer + free inside the timed region): criterion's back-to-back sampling pays the mmap/fault churn of a fresh 8–32 MB buffer per iteration, so the medians sit far above a steady-state best-of. Attribution probe (standalone binary, best-of-100, `taskset 1-31`): iota fill alone 167 µs @ 1M / 1.1 ms @ 4M; whole range call 332 µs / 4.2 ms vs engine floor 110 µs / 1.9 ms, so the input's fill + lifecycle is 56–70 % of an owned range-input call at those sizes. `pipe_range(0..n).map(+1).collect()` lands on the borrowed floor at 1M (−72 % vs `pipe(0..n)`) and beats it by ~21 % at 4M (−94 % vs `pipe(0..n)`): beyond L3 the materialized paths pay the input's DRAM read, which the generation core never performs.

Regression check (3 interleaved per-id rounds vs the branch point, `bench_ab.sh -B sync_vs_rayon`): every youpipe family within ±3 % noise; the only flagged row was the `rayon_nested_saturated/1000` anchor (+5.4 %, 0/9 dominant, layout/drift wobble on untouched rayon code). Horizontal `cpu_balanced`/`cpu_balanced_readback` two-binary alternating A/B (5 pairs × 2 rounds, `taskset 1-31`): pooled +0.1…+1.0 % at 1M/4M with mixed per-pair signs, no regression. Miri (Tree Borrows) covers the generation cores' guards via the `pipe_range` integration tests.

`pipe_range(range)` removes the input buffer entirely: the item at index `i` IS the index, generated inside the leaves (`RangeGenStrategy` / `par_range_gen_*` over the same `hybrid_dispatch`, input handle `IN = ()`), so there is no input allocation, no serial fill, and no input cache traffic. Allocation proof: `tests/pipe_range_alloc.rs` counts N-sized allocations and finds exactly 1 (the output buffer) vs 2 on the materialized path. Filter chains and the fallible terminal materialize once at the terminal (documented fallback, the same fill `pipe(range)` always paid). If you already own a `Vec`, `pipe(v)` stays zero-copy, and a `pipe_vec` constructor would add nothing.

## Fallible `try_map().try_collect()` vs rayon (`try_collect`, borrowed input)

When the chain has `MAY_FILTER == false`, `try_collect` uses the same zero-allocation index-based fast path as `collect`, pre-allocating the output buffer and writing at known indices instead of the `Vec`-merge fallback.

| Size | youpipe try_map | rayon    |
| ---- | --------------- | -------- |
| 10K  | ~11 µs          | ~409 µs  |
| 100K | ~24 µs          | ~698 µs  |

The rayon row is structural: `par_iter().map(Result).collect::<Result<Vec<_>>>()` has no indexed fast path, it builds and reduces per-split partial `Vec`s, so the gap is not a tuning artifact (rayon 1.12).

The owned + filter caliber (`youpipe_try_filter_owned`, `MAY_FILTER == true`, the range-tree path) was added 2026-09 together with its port off the last `Vec::split_off` tree: **−18.3 % @ 10K, −19.9 % @ 100K** (5 interleaved rounds; the 100K base side had one outlier round that inflated its spread, and every post-change round beat every pre-change round).

## Filter-chain collect: merge tree, count-then-place, write-then-compact (2026-09-27)

Filter chains (`MAY_FILTER == true`) cannot use the index-based core, because the output cardinality is unknown up front, so leaves build per-leaf `Vec`s and the tree concatenates them. The path went through four rounds of work: leaf pre-allocation, an honest statement of merge cost, the opt-in count-then-place variant, and the opt-in write-then-compact variant, ending in an `Auto` default.

### Leaf pre-allocation

`filter_leaf` / `filter_try_leaf` grew from `Vec::new()` and `join_fused_collect_by_ref` used `filter_map(..).collect()`, log-many reallocs plus partial memcpys per leaf, while sibling `join_fused_try_collect_by_ref` already pre-allocated. Unified to `Vec::with_capacity(leaf_input_len)`: all-survive is the exact worst case, and over-allocation is bounded by the leaf input when items are filtered out. Isolated per-id A/B vs the branch point (6 interleaved rounds, 33 % selectivity): **100K −6.2 % borrowed / −7.1 % owned / −4.5 % try+owned** (dominant 25–32/36 pairwise), **1K owned −4.6 % stable**; 10K is a wash (−0.9 %…+2.9 % across sessions, 0–21/36, and which 10K row leans +2…3 % flips between sessions, so fused recompile layout noise covers it), while every 100K row improves in every session: the post-landing confirmation run read −13.9 % borrowed / −6.4 % owned @100K and −6.4 % @1K owned.

### Merge cost, honestly stated

The range-tree comments claimed "each surviving item moves exactly once"; in reality every internal node's `l.extend(r)` reserves and memcpys one child's `Vec`, O(depth) moves per survivor, so the merge tree's output-side cost scales with the survivor count. The measured win over the old `Vec::split_off` tree came from never copying the input side, and the comments were corrected accordingly.

### `filter_selectivity` group

The `filter_selectivity` group (keep 10/50/90 %, borrowed caliber, 10K/100K) anchors the survival-rate axis, and the rayon rows double as drift controls for same-binary knob A/Bs.

### Count-then-place (`YOUPIPE_FILTER_COLLECT=ctp`, opt-in)

Two-pass by-ref filter collect: pass 1 counts survivors per leaf, a sequential scan turns counts into output offsets, and pass 2 re-runs the chain and writes survivors straight into one exactly-sized buffer (each survivor moves exactly once, no per-leaf `Vec`s, no tree merges). Cost is flat in selectivity: stage closures run twice over the input, which is observable for side-effecting or interior-mutable closures, plus a second fork/join wave. Same-binary knob A/B (5 interleaved rounds, 32-core, borrowed):

| shape @100K | merge tree | count-then-place | Δ |
| ----------- | ---------- | ---------------- | --- |
| keep 10 % | 31.9 µs | 45.0 µs | **+41.3 %** |
| keep 33 % (`sync_filter`) | 54.1 µs | 37.3 µs | **−31.1 %** |
| keep 50 % | 77.4 µs | 48.7 µs | **−37.1 %** |
| keep 90 % | 106.6 µs | 52.8 µs | **−50.5 %** |

All eight rows stable (25/25 or 0/25 pairwise); at 10K ctp loses across the board (+28…+75 %, the fixed second wave dominates) and the owned-path control reads −0.1 % (the knob must not, and does not, touch it). The crossover sits near ~25 % selectivity at 100K: the merge tree scales with survivors, count-then-place with the input. The original hypothesis ("count-then-place for low-selectivity batches") is inverted, the knob pays off for filters that keep most items on ≥ 100K batches. Owned / scoped / try filter paths keep the merge tree: their stages consume items by value, so a counting pass cannot re-run them.

### Write-then-compact (`YOUPIPE_FILTER_COLLECT=wtc`, opt-in)

The single-stage-pass variant: each leaf runs the chain exactly once and writes its survivors contiguously into the low end of its slice of one n-slot output buffer (leaf-contiguous, not input-indexed, since exact-index writes would scatter at low selectivity and need per-run metadata for the compaction). A compaction pass prefix-sums per-leaf counts into final offsets and block-copies each leaf's contiguous segment; a segment already in place skips the copy, and payloads ≥ 256 KB copy through a parallel join-tree wave into a separate exactly-sized buffer, because an in-place parallel sweep is not interleaving-safe (see the section comment in `fused.rs`). One allocation, one pass, one payload move per survivor; the price is an n-sized rather than survivor-sized output buffer. Final three-sided same-binary knob A/B (5 interleaved rounds, 32 cores, all rayon drift controls within ±4 % noise):

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

wtc wins or ties everywhere except mid/high-selectivity 10K, keep90 +25 % stable: there the survivor payload (72 KB @ keep90) sits under the 256 KB parallel-compaction threshold and the driver-sequential sweep pays cross-core transfers for cache lines the workers just wrote. At 1K the payload is too small for that to matter, and at ≥ 100K the parallel wave amortizes it. ctp keeps the ≥ 100K mid/high-selectivity crown (−10…−23 % vs wtc: exact-size buffer, no compaction pass) and loses everywhere else.

### Default policy (`FilterCollectMode::Auto`)

The merge-tree default was only right for small/mid batches; ≥ 100K users ate the wtc loss at every selectivity. `Auto` gates on item count: merge tree below 100K, wtc at or above (`FILTER_COLLECT_AUTO_WTC_MIN_N`, fused.rs). The gate sits at the smallest measured length with stable multi-selectivity wtc wins (100K) rather than interpolating the unmeasured 10K..100K range, since keep90 @ 10K is a stable +25 % regression and a lower gate is not free; the 1K single-shape win (−12.8 %) is too narrow to open a second small-batch window. ctp is never chosen automatically, even where it is fastest: it runs the user chain twice, observable to side-effecting closures, and a default must keep the merge tree's single-pass semantics. `with_filter_collect(FilterCollectMode)` (`PipeRef` / `PipelineConfig`) pins a variant; the `YOUPIPE_FILTER_COLLECT` knob (unset/`merge`/`ctp`/`wtc`; invalid values panic) overrides only `Auto`, explicit API beats env. The returned `Vec`'s spare capacity may differ across variants (wtc keeps the n-slot buffer); values and order are identical.

Post-landing confirmation (same-binary A/B, env knob `merge` baseline vs unset Auto, one interleaved session, 3 s/50 samples; rayon rows as drift controls): 100K keep33 **−31.8 %**, keep50 **−29.3 %**, keep90 **−34.0 %** (all p < 0.05); 100K keep10 +3.0 % against a +2.0 % rayon drift control (net ≈ +1 %, matching the knob-era noise verdict for that shape). Below the gate the path is bit-identical to the old default: 1K −0.03 %, 10K keep33 −0.27 %, 10K keep90 +0.25 % (all n.s., and keep90 @ 10K is exactly the shape the gate exists to protect from wtc's +25 %).

## `for_each()` vs rayon (`sync_for_each`, cpu_heavy per item, borrowed input)

| Size | youpipe `for_each` | rayon `for_each` |
| ---- | ------------------ | ---------------- |
| 1K   | ~9.9 µs            | ~38 µs           |
| 100K | ~49.6 µs           | ~121 µs          |

`for_each` was the last fused terminal still on the single-tree path. Porting it to the shared `hybrid_dispatch` (via the `SinkStrategy` impl of `HybridStrategy`) measured **−8.7 % @ 1K, −7.2 % @ 10K, −5.0 % @ 100K** vs the prior tree-only `par_for_each` (sizes at the time of that A/B). With the `num_cpus` cache (2026-09-05) it leads rayon at every size including 1K; the earlier "1K trails because the off-pool driver blocks instead of participating" gap was in hindsight mostly the uncached `available_parallelism` syscalls, not the driver model. Consolidating all `num_threads` chunk jobs into a single `Box<[ChunkJob]>` shaved a further ~3 % @ 1K–10K. Injecting a single root job instead (rayon's `join`-unfold pattern) regressed, the work-stealing ramp-up cost exceeded the per-chunk savings, so the hybrid chunk strategy was kept.

## Reduce terminals vs collect-then-sum (`sync_reduce`, `x+1` map, 2026-09-28)

The aggregation terminals (`reduce`/`fold`/`sum`/`count`/`min`/`max` and the fallible twins) never allocate the output `Vec`: each leaf folds its range into a partial, the tree combines partials through `join` returns, and the chunk driver publishes one partial per chunk into a one-shot cell (design history in [scheduler.md](../scheduler.md)). Caliber: borrowed `x+1` map over a warm slice (the owned side pays a fresh clone, identical on every A/B side). 5 interleaved per-id rounds; the family was new so there is no base side, and the old path (`youpipe_collect_sum_borrowed`) and `rayon_sum` run in the same rounds as drift anchors.

| Size | youpipe `sum` (borrowed) | collect+sum (old path) | rayon `.sum()` | sequential |
| ---- | ------------------------ | ---------------------- | -------------- | ---------- |
| 1K   | ~10.3 µs | ~9.4 µs  | ~39.2 µs | ~0.3 µs  |
| 10K  | ~10.6 µs | ~12.7 µs | ~53.7 µs | ~3.0 µs  |
| 100K | ~12.2 µs | ~46.6 µs | ~69.5 µs | ~29.4 µs |
| 1M   | ~29.9 µs | ~235.7 µs| ~121.2 µs| ~293.0 µs|

Vs the materialize-then-fold path: **−16 % @ 10K, −74 % @ 100K, −87 % @ 1M**; at 1K the reduce core medians +9.5 % with 18 % bimodal round spread (8.8/10.3 µs alternating, 2/5 rounds beat the old path), borderline noise below the size-gating threshold, no gating done. Vs rayon: **−74…−82 % at every size** (the cpu-heavy collect caliber narrows that, this is the lightweight end where the removed output buffer dominates). Owned input pays the fresh clone (~17.3 µs @ 10K, ~74.5 µs @ 100K), an identical lifecycle on every side of any owned A/B.

## On-pool nested terminals (`sync_nested_on_pool`, cpu_heavy per item)

Fused terminals reached from inside a worker of the driving pool, `pool.submit` tasks or stream stage closures calling `.collect()`, benchmarked as one submitted job (`nested_single`, isolating batch ramp-up with P−1 workers free) and as P concurrent submitted jobs (`nested_saturated`, every worker is a driver waiting on its own latch while stealing), against same-shaped `ThreadPool::spawn` + nested `par_iter` on a same-sized rayon pool.

The on-pool hybrid dispatch (`Stealing` latch) is gated by batch regime, verified by same-binary knob A/B (`YOUPIPE_ONPOOL_HYBRID`, 5 interleaved rounds, 32 cores):

- `nested_single/100K` **−3.5 %** (25/25 dominant), the ramp-up win the change targets.
- `nested_saturated/100K` +2.0 % lean (within spread, 0/25); every worker nesting large batches is an exotic shape.
- 1K sizes are pure noise with the gate; ungated hybrid there measured **+430 %** (`nested_saturated/1K`, P×P tiny chunks collapsing the single injector) and +3 % (`nested_single/1K`), the recompile-based A/B that motivated the gate.

Methodology note: the first recompile A/B of this change produced a contradictory second session (+18 % on `nested_single/100K` and +31 % on the untouched rayon anchor), pure code-layout noise on a recompiled binary, exactly the trap that motivates the knob methodology. Scheduler-class changes get verified with `-E` same-binary env A/Bs (`bench_ab.sh -a off=wt -b on=wt -E off=YOUPIPE_X=0 -E on=YOUPIPE_X=1`), not recompiles.

## Expand-heavy: owned `Vec` vs push-style expansion (`expand_heavy`)

Matrix: fan-out ∈ {4, 64} × cost ∈ {cheap, cpu} at 10K inputs; throughput counts output elements. `owned_vec` = `expand(Fn -> Vec)` (one malloc + free per input item), `push_emit` = `expand_emit(Fn(I, &mut Vec))` (per-worker reused scratch buffer); rayon `flat_map` / `flat_map_iter` rows are the external anchors for the two shapes.

Verdict (2026-10, single-session interleaved runs of the `fanout=4/cost=cheap` pair, 12 alternating rounds, `taskset 1-31`):

- Allocation evidence is deterministic (counting global allocator, `tests/expand_alloc.rs`): `expand` performs ≥ 1 `malloc` per input item (1024 items → ≥ 1024 allocations); `expand_emit` performs a count independent of item count (8× the inputs adds at most a couple of output-`Vec` growth reallocs).
- Wall clock: the win is bounded by the malloc/channel cost ratio, and on glibc it is small. With glibc's per-thread tcache absorbing same-size small frees (~50–100 ns per input), the eliminated malloc is minor against the ~250 ns per output channel handoff: paired-round medians showed push ahead by ~2.5 % at `fanout=4/cost=cheap`, inside a ±20 % environmental noise floor (per-round deltas flipped sign, and the session's load average included the benches themselves). The premise that allocator traffic dominates expand-heavy loads is falsified for glibc; it only holds under allocators without thread caches, or when expansions are large enough to bypass tcache bins.
- High fan-out amortizes the per-input malloc over more outputs. The structural gap to rayon in this group is the streaming engine's channel infrastructure (see `mixed_load` in [streaming.md](streaming.md); pure sync chains now take the fused pass-through, see `guide/stream.md`), not the expansion API shape.

## Adjacent-sync fusion (`sync_fuse`, 2026-09-29)

Evidence bench for the falsified "fuse adjacent sync stages in streaming topology" idea (verdict + probe data in [dead-ends.md](../dead-ends.md)). Same-binary knob A/B (`YOUPIPE_SYNC_FUSE_VARIANT=split|merged`, 5 per-id isolated interleaved rounds, median): `split` runs the sync stages as separate worker populations, `merged` hand-composes them into one closure, with identical async/fence/cancel tails on both sides.

| shape              | split @1K | merged @1K | split @100K | merged @100K |
| ------------------ | --------- | ---------- | ----------- | ----------- |
| cancel_pair_cpu    | 279 µs    | 317 µs     | 24.6 ms     | 29.8 ms     |
| cancel_quad_cpu    | 260 µs    | 315 µs     | 22.2 ms     | 29.9 ms     |
| async_pair_cpu     | 482 µs    | 521 µs     | 236.9 ms    | 25.1 ms     |
| fence_pair_cpu     | 1.17 ms   | 1.64 ms    | 36.2 ms     | 209.1 ms    |

Stable-regime rows (pure-sync chains forced onto streaming by `with_cancel`): merging populations regresses monotonically with per-channel worker count (8 W → 222 ns/item, 16 W → 246 ns, 31 W → 298 ns). Channel hops pipeline, so fusion only concentrates MPMC and collector contention. The async/fence rows are dominated by a bistable convoy pathology unrelated to fusion (a zero-CPU `bump.fence.bump` chain costs 226 ms @100K, and the two-sync-prefix async chain samples bimodally 23↔244 ms), with `fence_infra` kept in the family as the canary. Attributed 2026-09-29: a per-item park+wake trickle at serial-supplier→crowd channel interfaces (boundary matrix, perf/trace signatures and fix directions in [streaming.md](../streaming.md) "Convoy collapse forensics", harness = the `youpipe-bench` binary `convoy-probe`).
