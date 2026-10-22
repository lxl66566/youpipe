# TODO

Scope: **unlanded** work items. Landing records and measured readings for closed requirements live in the topical docs under docs/src/dev/ (scheduler.md / the benchmarks chapters / streaming.md / core-types.md) and in the code comments; do not redo them from memory. Falsified directions are in [dev/dead-ends.md](src/dev/dead-ends.md). Entry numbers are never reused (the dev docs reference entries as "todo #N"); closed entries leave gaps in the numbering.

Verification conventions (see `dev/benchmarks/`):

- Every verdict uses `perf/bench-suite` interleaved A/B (same machine, same session, median-of-rounds).
- The 100K fused families must run as isolated per-id interleaving (running the whole group has a 2–3× collapse trap); the stream families drift ±10% and also need isolated interleaving.
- Scheduler-class changes use same-binary runtime-knob A/B (`bench_ab.sh -E`); recompilation swings tight benches ±30% through pure code layout (measured +31% on an untouched rayon anchor in 2026-07).
- Heavy-tail conclusions need multiple seeds (chunk-boundary luck dominates single-seed variance).

Priorities: P0 = high value / strong evidence; P1 = medium; P2 = experimental / micro-optimization / caliber fixes. Code locations are given by symbol name (line numbers drift on main).

---

## Performance

### 2. [P1] zstd_shape residual gap (capped/uniform behind)

- Status: the wide-tier boundary 64→48 has landed (scheduler.md "Wide-tier boundary scan"; the heavy-tail n=3000 vs-rayon gap tightened to +2.5%); the opt-in `ComputePool::new_pinned` has reclaimed most of the residual from the wake-landing side (capped −6.1…−6.9%, uniform −4.0…−4.6%, scheduler.md "Worker affinity"). Residual: capped +1…+2%, uniform +4…+6%, and heavy-tail n=2000 (narrow) ~+1% still behind rayon; the default-path (no pinning) residual needs re-estimation under the pinning-gate caliber.
- Direction: the capped-shape residual is heavy-tail spread (4.2 pt), not a mean shift — consider reordering entries within chunks, or a third tier; cost-EMA-style adaptation is twice falsified (dead-ends.md), do not take the runtime-cost-estimation route again.
- Verification: `zstd_shape` all shapes × multiple seeds (`ZSTD_SEEDS`/`ZSTD_SHAPE_NS` grid passed through); the cheap-side tiers, checked arithmetically, do not move with the boundary (cpu_unbalanced n=200/5000, neither crosses 48).

### 3. [P1] fused inter-batch park/wake occupancy loss (default-path residual)

- Status: attribution and closure records in [attribution.md](src/dev/benchmarks/attribution.md) ("Attributing the 2M/4M fused-collect gap", "NT-store attribution"): idle workers parking + futex wakes + landing on cold cores, vs rayon spinning warm throughout. NT-store auto (~13 pt) and opt-in pinning (cpu_balanced 1M/2M/4M −5.1…−7.2%) have landed; under the new caliber (2026-09-26, 7 interleaved rounds) cpu_balanced 1M +0.6%, 2M +1.5%, 4M −6.8% (ahead of rayon), and the readback caliber leads by 24–29%. **The default-path residual is ≈1.5 pt @2M**.
- Direction: tail-straggler refinement — finer-grained oversplit of the final segment (dynamic; not the cost-EMA route, which is falsified; limited residual value, only +1.5% @2M under the new caliber). Do not lengthen the global spin window and do not build time-window or quiescence-gated keep-warm (both falsified, see dead-ends.md).
- Verification: `cpu_balanced` 1M/2M/4M isolated A/B + `horizontal-counters` (youpipe-bench) to re-check task-clock / ctx-switch / migration.

### 4. [P2] throughput collapse of streaming async shapes (residual ~1%)

- Status: `YOUPIPE_ASYNC_CHAIN_FLOOR` (on by default, 2026-10-22) resolved the main body — the async chain's unpinned channels (stage outputs + the feeder ring) are floored to `min(n, 16384)` slots (an 8 MiB/ring byte budget), taking the async2 deep-soak slow runs from 18–30/120 to 0/120 (release-gate binary 21/96 → 2/96); terminal-only decoupling and the necessity of `buffer = n` are both falsified (dead-ends.md; the landing record is in [streaming.md](src/dev/streaming.md) "Async-chain capacity floor — resolution" and the same-named section of [attribution.md](src/dev/benchmarks/attribution.md)).
- Residual: under the final gate the on side still shows ~1% slow runs (2/216, a single-process sporadic). If reproducible, candidates: raise the floor to the 64K tier (64K also read 0/48 in the sweep), or the combination surface with `YOUPIPE_SPIN_ANCHOR`.

### 5. [P1] `ordered()` + `expand()`: the batched-payload design

The 2026-10 design analysis confirmed three hard constraints that must be solved before implementation (details in the corresponding record in [core-types.md](src/dev/core-types.md)):

- **Empty expand groups cannot carry a signal (blocker)**: in the `(seq, sub, end_of_group)` design the `end` flag can only attach to the last entry of a group; when `expand` returns an empty `Vec`, no message of that group can carry the completion signal — from the first empty group on, the collector can no longer advance the prefix flush and degrades to sorting the whole remaining stream at close: the rest of the stream is buffered in full (memory cost) and the fixed-capacity reorder ring silently drops items (data loss). The only sound design is **batched payloads downstream of the expand** (the channel carries `(seq, Vec<N>)`; an empty group is an empty-Vec message; nested expands keep order naturally; the ReorderBuffer is reused as-is), but `ordered` is a runtime flag while batched-vs-not is a compile-time type — `ordered` must be promoted to a typestate (`.ordered()` must precede `.expand()`), or a dual API is needed.
- **Multi-level expand needs the full path**: the global order of a two-level expand is the `(seq, sub1, sub2)` lexicographic order; a 3-field tag loses `sub1`; the batched-payload design is naturally immune (nesting flattens).
- **Generic-constraint trap**: `Fn(I, &mut Vec<N>)` does not constrain `N` in argument position (E0207); it must be carried as a struct generic, `ExpandStage<Prev, F, N>` (expand_emit already does this).

### 7. [P1] cancellation partial-output semantics (residual: RunOutcome reporting)

- The documentation contract is in place (2026-10-20, r7): `run()` gained a Cancellation section, `for_each`/`with_cancel` gained inline notes, and guide/stream.md was synced — cancellation produces silently truncated output, indistinguishable from a normal short run. The old guide wording was also corrected: a worker stops once it has finished the current item (the token is checked per item), it does not "drain in-flight items to completion".
- Residual direction: `RunOutcome`, or a `try_run_checked`, reporting `Cancelled { emitted }` (the collector checks the token after draining).

### 16. ~~[P1] batching the per-item sends of streaming stage workers~~ (landed)

- `YOUPIPE_STREAM_TX_BATCH` (2026-10-22, on by default) + the fork MPSC ring's native `try_push_batch` have landed: multi2 −49%, cancel_quad −65%, fence_pair −20…−25%, workers2 −10…−17%, all stable at 25/25 or 9/9; async-tail chains stay per-item under the `!has_async_stage` gate (both batched hops +2.2…+4.9%, falsification record in dead-ends.md). Readings: [benchmarks/streaming.md](src/dev/benchmarks/streaming.md) "Stream worker tx batching"; mechanism and semantics: [streaming.md](src/dev/streaming.md) "Worker output batching".

## Code health (refactoring / smells)

### 13. [P2] correctness-adjacent items and miscellany

- `try_exec`, ~257 lines, mixes four duties (admission/lease resolution, StreamCtx construction, feeder channel routing, terminal drain) → split out `resolve_admission` so the invariant comments sit next to the code that maintains them.

### 14. [P1] `pipeline_integration` intermittent hang, residual (stream side)

- Status: **the graph-engine occurrences are root-caused and fixed (2026-10-23)** — not a fork lost-wake. The 2026-10-22 `graph_dag/diamond_broadcast_conservation` (n=10K, buffer=4) hang was the fan-in anchor parking on a single input ring: the liveness comment copied from the stream anchors ("the producer blocks on its own upstream, progress elsewhere eventually feeds it") only holds for a linear chain's sole input. In a DAG, broadcast edges flush sequentially and block per edge, so the anchored ring's producer chain can be parked on a full ring whose only drainer is the anchored worker itself; with every fan-in worker on the starved ring the run deadlocks with exactly the observed signature (producers Full / consumers Empty / main in the join condvar). Fix: the sync anchor parks registered on every open input at once (crossfire `Select`, reservation tokens; `FanIn::anchor`), the async twin awaits all inputs (private `FirstReady` combinator; `AsyncFanIn::anchor`); a single open input keeps the plain recv park — the fast path is load-bearing, without it the Select registration cost read +20…+40 % on the single-input chain nodes ([benchmarks/graph.md](src/dev/benchmarks/graph.md)). Regression tests at both anchors (`anchored_*_claims_from_other_input`: a silent ring A, items in ring B only — the pre-fix anchor never claims B); post-fix the `graph_dag` binary passed 25/25 rounds on the final code and miri (tree-borrows) is clean on both modules.
- Residual: the 2026-09-28 stream-side occurrence (pipeline_integration, 107 threads in futex) predates the graph engine and is NOT this bug — the stream engine's anchors are single-input by construction (stage rings; the sharded terminal's shards all hang off one producer population in a linear chain, re-derived safe: an all-frozen configuration always leaves a progressing worker). `crossfire-trace` forensics + gdb under a stream-side reproduction remain the direction; do not touch the code without one.

### 15. [P2] leftovers from the 2026-10-20 audit

Items from the 2026-10-20 three-way audit that were not scheduled into the adjacent round:

- st3 `WorkerThread::push`'s `is_empty()`+`push()` performs 5 atomic reads on the line shared with stealers, reducible to 3 — first verify with cargo-show-asm whether LLVM already merges the adjacent Relaxed loads (generic monomorphization within one CGU may already eliminate them); only if unmerged, evaluate a fork API (costly under the fork-stays-diffable constraint, expected gain <1%).
- `try_apply_pure` has no bench id: the path is sensitive to codegen layout (ErasedStrategy recorded ±16–30% layout regressions), so a layout shift could silently regress the `try_collect` fast path.
- `perfcnt` → `nom v4` future-incompat: upstream is unmaintained, a future rustc will refuse to compile it; when that happens, vendor/fork it or port the counters onto gungraun's valgrind path.
- mdbook has no linkcheck preprocessor, so relative intra-book links are unvalidated (a one-off script scan on 2026-10-20 found 0 broken links; moving it into CI is pending).

### 17. [P2] seq-less unordered channel flavor

A seq-less unordered channel flavor (dropping the `(u64, T)` tag; `spawn_stage`'s unification rationale is at `crates/youpipe/src/builder/typed/stream.rs:611`) would halve per-item slot bytes for `u64` payloads, but it splits every spawn path per ordering. The expected win is single-digit % at best on cache-bound shapes. Worth only a throwaway prototype.
