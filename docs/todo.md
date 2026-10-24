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

### 7. ~~[P1] cancellation partial-output semantics (residual: RunOutcome reporting)~~ (landed)

- The documentation contract landed 2026-10-20 (r7): `run()` gained a Cancellation section, `for_each`/`with_cancel` gained inline notes, and guide/stream.md was synced (also correcting that a worker stops once it has finished the current item).
- The residual landed 2026-10-25 (r8): `StreamPipe::run_checked`/`try_run_checked` and `GraphBuilder::run_checked`/`try_run_checked` return `RunOutcome` — `Cancelled { partial, emitted }` with `emitted == partial.len()` exactly for the collecting terminal (one post-drain token read, no hot-path cost; the graph engine has no collector, `emitted` stays 0). Mechanism and tests: [streaming.md](src/dev/streaming.md) "Cancellation outcome reporting".

### 16. ~~[P1] batching the per-item sends of streaming stage workers~~ (landed)

- `YOUPIPE_STREAM_TX_BATCH` (2026-10-22, on by default) + the fork MPSC ring's native `try_push_batch` have landed: multi2 −49%, cancel_quad −65%, fence_pair −20…−25%, workers2 −10…−17%, all stable at 25/25 or 9/9; async-tail chains stay per-item under the `!has_async_stage` gate (both batched hops +2.2…+4.9%, falsification record in dead-ends.md). Readings: [benchmarks/streaming.md](src/dev/benchmarks/streaming.md) "Stream worker tx batching"; mechanism and semantics: [streaming.md](src/dev/streaming.md) "Worker output batching".

## Code health (refactoring / smells)

### 13. ~~[P2] correctness-adjacent items and miscellany~~ (landed)

- The `try_exec` split landed 2026-10-25 (r10): the run core now delegates to `resolve_admission` (admission/lease resolution + worker division, returned as an `Admission` bundle that carries the `StreamCtx` budget inits), `route_feeder` (feeder ring sizing + in-flight note, sync/mixed-mode channel pick, terminal Single-variant guard) and `drain_terminal` (reorder window, terminal drain dispatch, feeder/async-panic re-raise); `try_exec` keeps the entry gates and the `StreamCtx` wiring. Pure refactor — same call graph, zero public API change; every invariant comment moved with its code. Interleaved per-id A/B (base=main, 3 rounds, `stream_pipeline` + `sync_fuse/cancel_pair` ids): all reroute paths (with_fence −2.4…+0.2%, every cancel id −2.4…+3.6%) read mixed-sign layout noise; the fused-pass-through ids that never execute `try_exec` swung +25%/−9% — the documented single-CGU layout class (`.cargo/config.toml`), not a behavioral delta. Full native suite green; miri (tree-borrows) clean on `pipeline_integration`.

### 14. [P1] `pipeline_integration` intermittent hang, residual (stream side)

- Status: **the graph-engine occurrences are root-caused and fixed (2026-10-23)** — not a fork lost-wake. The 2026-10-22 `graph_dag/diamond_broadcast_conservation` (n=10K, buffer=4) hang was the fan-in anchor parking on a single input ring: the liveness comment copied from the stream anchors ("the producer blocks on its own upstream, progress elsewhere eventually feeds it") only holds for a linear chain's sole input. In a DAG, broadcast edges flush sequentially and block per edge, so the anchored ring's producer chain can be parked on a full ring whose only drainer is the anchored worker itself; with every fan-in worker on the starved ring the run deadlocks with exactly the observed signature (producers Full / consumers Empty / main in the join condvar). Fix: the sync anchor parks registered on every open input at once (crossfire `Select`, reservation tokens; `FanIn::anchor`), the async twin awaits all inputs (private `FirstReady` combinator; `AsyncFanIn::anchor`); a single open input keeps the plain recv park — the fast path is load-bearing, without it the Select registration cost read +20…+40 % on the single-input chain nodes ([benchmarks/graph.md](src/dev/benchmarks/graph.md)). Regression tests at both anchors (`anchored_*_claims_from_other_input`: a silent ring A, items in ring B only — the pre-fix anchor never claims B); post-fix the `graph_dag` binary passed 25/25 rounds on the final code and miri (tree-borrows) is clean on both modules.
- Residual: the 2026-09-28 stream-side occurrence (pipeline_integration, 107 threads in futex) predates the graph engine and is NOT this bug — the stream engine's anchors are single-input by construction (stage rings; the sharded terminal's shards all hang off one producer population in a linear chain, re-derived safe: an all-frozen configuration always leaves a progressing worker). `crossfire-trace` forensics + gdb under a stream-side reproduction remain the direction; do not touch the code without one.

### 15. [P2] leftovers from the 2026-10-20 audit

Items from the 2026-10-20 three-way audit that were not scheduled into the adjacent round:

- `perfcnt` → `nom v4` future-incompat: upstream is unmaintained, a future rustc will refuse to compile it; when that happens, vendor/fork it or port the counters onto gungraun's valgrind path.
- mdbook has no linkcheck preprocessor, so relative intra-book links are unvalidated: `perf/verify/linkcheck.sh` (2026-10-25) now covers them (0 broken on landing); moving it into CI is pending.

### 17. [P2] seq-less unordered channel flavor

A seq-less unordered channel flavor (dropping the `(u64, T)` tag; `spawn_stage`'s unification rationale is at `crates/youpipe/src/builder/typed/stream.rs:611`) would halve per-item slot bytes for `u64` payloads, but it splits every spawn path per ordering. The expected win is single-digit % at best on cache-bound shapes. Worth only a throwaway prototype.

### 20. ~~[P2] `Broadcast::send_batch` re-introduces the extend-over-drain staging 7d3cff3 removed~~ (landed)

- Landed 2026-10-25 (r10): the last edge routes through the now-pub(crate) `stage_batch`; the recompile A/B gate (graph_shapes, 5 per-id interleaved rounds) read all-noise with the stream controls within ±1.2 %. Readings: [benchmarks/graph.md](src/dev/benchmarks/graph.md) "Broadcast staging drop-glue audit".

### 21. ~~[P2] graph-engine cancel semantics diverge from the stream engine's~~ (landed)

- Landed 2026-10-25 (r10): acceptance stays batch-granular (per-item token checks would tax the batched claims) and is now documented as such (`with_cancel` rustdoc; guide/graph.md's "per item" overclaim fixed); delivery is cancellation-blind — `Broadcast::send_batch`/`flush_edge` drop the token parameter (`SendItem::send_batch` parity) and park per item on a full ring instead of dropping the mapped remainder, so the emitted set no longer depends on ring pressure or `YOUPIPE_GRAPH_TX_BATCH`. Every park resolves under cancel (downstream drains or exits; the receivers' drop closes the ring and wakes parked senders). Mechanism and tests: [streaming.md](src/dev/streaming.md) "Graph cancel semantics".

### 22. ~~[P3] review miscellany (2026-10-25 audit, non-blocking)~~ (landed)

- Landed 2026-10-25 (r10): guide tuning-table wording fixed ("pin the compute-worker budget divided across sync nodes"); async_exec module doc deduplicated.
- The `FanIn::anchor` scratch hoist is falsified: hoisting the per-episode `order` Vec into a FanIn field measured a stable regression on the @1K graph_shapes chain/diamond ids (+3.7…+6.3 %, dominance 0/25, side spreads <3 %, stream controls flat; the `Select` itself is not hoistable — its handlers borrow the receivers for the episode's lifetime). Left as a per-episode Vec with the measurement recorded at the allocation site; readings in [benchmarks/graph.md](src/dev/benchmarks/graph.md) "Any-of-N fan-in anchors" addendum.

## Verification suite (coverage / duration)

### 18. [P0→closed] miri.sh default target list is stale — graph_dag and batch_recv uncovered

- Status: **closed (2026-10-25)**. The default list now runs 13 more integration binaries, each measured in full under miri (durations in [testing.md](src/dev/testing.md) "Suite duration census"): graph_dag 85 s, batch_recv 30 s, cancel_outcome 63 s, sharded_terminal 48 s, sharded_terminal_async 281 s, expand_alloc 20 s (miri /8 added), pipe_range_alloc 12 s, filter_collect_panic 5 s, submit_batch_alloc 5 s, async_waker_alloc 169 s (miri /4 added), stream_reduce 10 s (miri /20 added), on_pool_nested 26 s, pool_cache 21 s — ≈13 min added. Four binaries stay out with reasons (inlined in miri.sh): hybrid_assist (only test is `#[cfg_attr(miri, ignore)]`), profile_guard (native config guard), ordered_window (size-pinned >1 Mi repro shapes; scaling vacates the assertion), stream_reduce_alloc (needs N ≥ 1<<17 for its ≥1 MiB counter signal → hours interpreted).
- Residual: the projected default pass is ≈57 min until todo #19's pipeline_integration divisor work lands (that binary is still ~31 min of the total).

### 19. ~~[P2] suite-duration trims (census 2026-10-25)~~ (landed)

- Landed 2026-10-25 (r9): all five trims, measured before/after in
  [testing.md](src/dev/testing.md) "After r9 trims (2026-10-25)".
  hybrid_assist watchdog 2 s; stream_reduce_alloc `N` 1<<19; the
  pipeline_integration miri heavy tier (10 fence/async/stress tests, the
  census's "pin" family turned out to be `cfg_attr(miri, ignore)`d) and
  the compute_pool hybrid-driver test with deepened `cfg!(miri)` divisors;
  the bench sweep mode implemented as `bench_ab.sh --sweep`
  (benchmarks/README.md "Exploratory sweeps"). No release-gate-only test
  split was needed — divisors alone cut the heavy tier 565 s → 145 s.

Baseline and methodology: [testing.md](src/dev/testing.md) "Suite duration census (2026-10-25)". Each line: trim → protection lost.

- hybrid_assist `stress_streams_plus_assist_drivers`: fixed `sleep(Duration::from_secs(6))` watchdog → 2 s. Lost: 3× shorter soak window for the assist-driver interleaving (periodic panic/drop accounting still runs several rounds).
- stream_reduce_alloc: `N = 1<<20` → `1<<19` (output Vec still 4 MiB, above the 1 MiB counter threshold — same detection classes). Lost: fewer items through the same aggregation shapes.
- miri pipeline_integration (≈31 min of the ≈48 min full pass): heavy-tier fence/stress/pin tests measure 55–76 s each; deepen their `cfg!(miri)` divisors (graph_dag's /20 convention) or split the heaviest 3–5 into a release-gate-only target. Lost: fewer interpreted repetitions of interleaving classes whose park/wake cores are already exhaustively model-checked by loom.
- miri compute_pool (72 s, dominated by `test_hybrid_driver_assists_when_workers_busy`): scale its miri iteration counts. Lost: fewer assist interleavings interpreted (Sleep/CountLatch/SleepMask are loom-covered; the interpretation adds UB detection, which the channel-side targets keep).
- Bench full sweeps: 273 criterion ids ≈ 16 min per pass; sync_vs_rayon (88) + unbalanced (46) are 49 % of the grid. In exploratory sweeps skip the rayon/tokio drift-control ids and the middle size of 3-size families; keep both in interleaved A/B, where the verdict methodology needs them. Lost: cross-library drift visibility and size-axis coverage in sweep mode only (A/B unaffected).
