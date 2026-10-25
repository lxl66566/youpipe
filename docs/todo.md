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

### 2. ~~[P1] zstd_shape residual gap (capped/uniform behind)~~ (landed)

- Closed 2026-10-26 (r15): the residual decomposed cleanly in a 4-side same-binary screen (`YOUPIPE_OVERSPLIT`/`YOUPIPE_CHUNK_SLACK`/`YOUPIPE_PIN_WORKERS`, n=2000, 6 seeds, 3 interleaved rounds, drift-cancelled vs same-pass rayon). The default-path residual that session was uniform +10.8%, capped +3.9% (4.1 pt seed spread), heavy-tail +1.3% (22 pt spread). `UNBALANCED_OVERSPLIT` 32→64 landed (uniform −5.5%, capped −1.7%, heavy-tail −1.4%; readings and the n=4000 confirmation in scheduler.md "`UNBALANCED_OVERSPLIT` 32 → 64"); the remainder is wake/placement — the pin column reproduced the `new_pinned` readings (−8.1% uniform), which stay the opt-in answer for default-path users.
- Both suggested directions falsified: within-chunk reorder is provably void for iid cost draws (index-only information; every fixed item→chunk permutation is exchangeable — see the scheduler.md note), and a third slack tier has no supporting scan point (the [42, 63) items/chunk band's two measured endpoints are monotone; forced wide at 42 re-measured neutral for the third time).
- Verification: `ZSTD_SEEDS`/`ZSTD_SHAPE_NS` grids; cheap-side guard re-measured under the flip (cpu_unbalanced n=5000, one extra tree level; n=200 arithmetically unchanged).

### 3. [P1] fused inter-batch park/wake occupancy loss (default-path residual)

- Status: attribution and closure records in [attribution.md](src/dev/benchmarks/attribution.md) ("Attributing the 2M/4M fused-collect gap", "NT-store attribution"): idle workers parking + futex wakes + landing on cold cores, vs rayon spinning warm throughout. NT-store auto (~13 pt) and opt-in pinning (cpu_balanced 1M/2M/4M −5.1…−7.2%) have landed; under the new caliber (2026-09-26, 7 interleaved rounds) cpu_balanced 1M +0.6%, 2M +1.5%, 4M −6.8% (ahead of rayon), and the readback caliber leads by 24–29%. **The default-path residual is ≈1.5 pt @2M**.
- Direction: tail-straggler refinement — finer-grained oversplit of the final segment (dynamic; not the cost-EMA route, which is falsified; limited residual value, only +1.5% @2M under the new caliber). Do not lengthen the global spin window and do not build time-window or quiescence-gated keep-warm (both falsified, see dead-ends.md).
- Verification: `cpu_balanced` 1M/2M/4M isolated A/B + `horizontal-counters` (youpipe-bench) to re-check task-clock / ctx-switch / migration.

### 4. ~~[P2] throughput collapse of streaming async shapes~~ (landed)

- The main body landed 2026-10-22 as `YOUPIPE_ASYNC_CHAIN_FLOOR` (terminal-only decoupling and the necessity of `buffer = n` both falsified, dead-ends.md; landing record in [streaming.md](src/dev/streaming.md) "Async-chain capacity floor — resolution" and [attribution.md](src/dev/benchmarks/attribution.md)).
- The residual landed (closed) 2026-10-25 (r12): the ~1% sporadic slow runs reproduced (10/2160 at the 16K default, 0.46%, 95% CI 0.22–0.85) and the tier A/B promoted the default to `ASYNC_CHAIN_FLOOR_SLOTS = 65536` — 0/2160 slow runs, mid-tail 50–100 ms 38 → 1, pooled mean −6%, and the criterion A/B found `async0_cheap/100K` −30.7% stable (binary×env crossover ruled out layout), every other async family noise. One corner cost accepted: +2.4% fast-mode median on the zero-CPU forensic probe (L3-vs-L2 ring recycling), with the knob growing an explicit-tier escape (`=16384` reproduces the old default). Record: [attribution.md](src/dev/benchmarks/attribution.md) "Floor-tier promotion".

### 5. ~~[P1] `ordered()` + `expand()`: the batched-payload design~~ (landed)

Landed 2026-10-25 (r11) as the three-mode ordering typestate (`Unordered` / `Ordered` / `Grouped`) with batched group payloads downstream of an ordered expand: `.ordered()` now requires an expand-free chain (a compile error where it used to panic at run time) and `.ordered()….expand()` carries each expansion group as one `(seq, Vec<N>)` channel message — empty groups included, nested expands flattening naturally, the `ReorderBuffer` reused as-is at group granularity. The typestate-vs-dual-API rationale, the runtime mechanics and the cost characteristics: [core-types.md](src/dev/core-types.md) "Ordered expansion — group payloads"; the data-plane mechanics: [streaming.md](src/dev/streaming.md) "Ordered expansion (group payloads)". Canary: the `expand_heavy/ordered_push` bench ids (`crates/youpipe/benches/expand.rs`).

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
- Stakeout (2026-10-25, r12): **no stream-side repro in 1365 full-binary fresh-process passes** (750 release + 615 dev, default knobs and harness, 46-min window on an otherwise idle 32-core machine; 20 s watchdog with a 0-CPU probe — zero trips, every wall 2.1–2.9 s). The harness landed as `perf/verify/hang-stakeout.sh` (forensics-before-kill: gdb `thread apply all bt` + `/proc` task states precede the SIGKILL — both earlier occurrences were killed bare, which cost this investigation its primary evidence); run it on the next sighting before touching the process. The same soak caught a different intermittent — the async panic-swallow race, todo #23 — which is NOT this hang (the swallow returns truncated output; the 09-28 occurrence sat at 0 CPU for 13 min).

### 23. [P1] async stage panic re-raise races the channel close (swallowed panics) — CLOSED 2026-10-25 (r13)

- Caught by the 2026-10-25 hang stakeout on `pipeline_integration` (readings in [dev/streaming-forensics.md](src/dev/streaming-forensics.md)): 15/1365 full-binary passes (~1.1 %) failed an async-panic-propagation test with `run()` returning normally — the stage panic silently swallowed, truncated output, no error signal (the exact pre-5f579db behaviour, intermittently resurrected). Both terminal flavors (ordered 8x, `to_caller` 7x) and both profiles (8/750 release, 7/615 dev). Not todo #14's hang — this returns, it does not stall.
- Mechanism (pinned by release run 152, which printed both halves of the race — the boom panic on a `tokio-rt-worker` and `expect_err` on the test thread): `CatchTaskPanic::poll` receives the payload only after the unwind has dropped the inner future's state, including the task's `tx`, so the output-channel close that releases the collector is not ordered after `record_panic`; `drain_terminal`'s `ctx.async_panic.get()` can observe an empty slot inside the unwind tail's window. The wrapper's doc comment claims the opposite ordering ("payload is recorded before the task's channel ends drop") — that only holds for the no-panic exit. Release additionally hides the missing seq (`verify_ordered_accounting` is debug-only); a dev-profile loss is predicted to surface as the misleading "items vanished" accounting panic instead, but the observed dev failures were output-silent too — under the 32-way harness the parallel `catch_panic_asserting` tests race the global panic hook, so every face of this failure can print nothing. Present since 5f579db (2026-10-06) — a residual of the original swallow fix, not an r10 regression.
- Fix direction: establish the happens-before at the task boundary — a per-run in-flight counter bumped at `CatchTaskPanic::poll` entry (RAII, decremented only after the record) and the terminal drain at EOF waiting it out before the final `get()`; at EOF every `tx` is already dropped, so every task is in or past its final poll and the wait is bounded by one unwind tail. Owning the sender beyond the catch inside the wrapper is the alternative and is blocked by async-block ownership semantics. Fix in the same pass: give `catch_panic_asserting` a hook-race-free assertion form (e.g. assert on the payload only, without silencing the global hook, or serialize the hook window) so the next sighting prints its face.
- **Landed (2026-10-25, r13)**: `TaskSettleGate` — refined from the proposal: the counter is bumped at the wrapper's *first poll* (per task, not per poll-entry; poll granularity under load is per-item-ish, so a per-poll increment would tax the data plane) and settled at the final `Poll::Ready` (record strictly before settle). The terminal drain waits `outstanding == 0` before the final slot read (both the Ok path and the drain-panicked path), bounded by `SETTLE_BOUND` (30 s degenerate cap; one atomic load on the no-panic path). Mechanism confirmed mechanically pre-fix (unwind-order probe: generator captures drop during the unwind, before the catch; in-process stress 7–15/3600 swallows, every swallow's record strictly after the drain's read). Graph engine untouched — its drain waits on `GraphCore::done`, decremented after `CatchPanic` resolves, so the record is always ordered before it (documented in `async_exec.rs`). Regression: `settle_gate_orders_record_after_sender_drop` (deterministic); `catch_panic_asserting` now hook-race-free (one filtering hook + suppression counter, payload-only assertion). Post-fix: 0/21 600 stress runs, 0 fresh-process loop failures (release + dev). Full narrative in [dev/streaming-forensics.md](src/dev/streaming-forensics.md) "Fix (2026-10-25, r13)".

### 24. ~~[P3] `drain_terminal`'s abort arm drops a stored feeder panic~~ — CLOSED 2026-10-26 (r14)

- Found by the 2026-10-25 panic/error/cancellation audit (coverage record in [dev/streaming-forensics.md](src/dev/streaming-forensics.md) "r13 panic/error/cancellation audit"). `drain_terminal` runs the terminal drain under `catch_unwind` so a recorded async-stage panic beats the ordered drain's accounting assertion; the `Err(drain_panic)` arm checks only `ctx.async_panic` — the feeder's `PanicSlot` (`Feeder::finish`) is never consulted there, nor on the `Ok(Err(e))` async-runtime-acquisition early return, so a payload stored by a panicking detached feeder is silently dropped and the caller sees the secondary panic (or the `io::Err`) instead of the root cause. Symmetric fix: `feeder.finish()` first in both early exits (root cause outranks the drain panic), matching the async-slot precedence.
- Trigger today is theoretical: the detached feeder loop (`push_items`) runs no user code — the input iterator is collected before any topology exists — so a feeder payload implies an internal channel failure. Fix opportunistically when the drain's error paths are next touched (S11's #23 fix rewrites this neighborhood — fold it in there rather than a dedicated pass).
- **Landed (2026-10-26, r14)**: both early exits now run `feeder.finish()` first — the `Err(drain_panic)` arm and the `Ok(Err(e))` acquisition return, in the EOF path's feeder-then-async order — so a stored feeder payload is re-raised as the root cause; the drain panic / `io::Err` surfaces only when no payload exists. Regressions `drain_terminal_drain_panic_resumes_feeder_payload_first` / `drain_terminal_acquire_err_resumes_feeder_payload_first` (both verified failing pre-fix); record in [dev/streaming-forensics.md](src/dev/streaming-forensics.md) "r13 panic/error/cancellation audit".

### 15. ~~[P2] leftovers from the 2026-10-20 audit~~ (landed)

Items from the 2026-10-20 three-way audit that were not scheduled into the adjacent round. Both closed 2026-10-26 (r14):

- `perfcnt` → `nom v4` future-incompat: resolved by swapping the backend of `youpipe-criterion-perf-counters` to the maintained `perf-event` 0.4 crate. Vendor/fork was rejected on usage-surface grounds: only ~7 perfcnt API points were live (per-tid open, disable-at-open, enable/disable/read/reset, six `PERF_TYPE_HARDWARE` events), all covered 1:1 by perf-event (`observe_pid`, counters start disabled by default, `exclude_kernel`/`exclude_hv` are the backend defaults), whereas a fork would drag in perfcnt's nom-v4 `/proc/cpuinfo` parsing wholesale — the very code the diagnostic lives in. The builder-factory API (which existed only because perfcnt's builder cannot be cloned) became a `HardwareEvent` enum; backend types no longer leak and `youpipe-bench` lost its direct perfcnt dev-dependency. `cargo build -p youpipe-criterion-perf-counters` is future-incompat clean; perfcnt and nom v4 are gone from Cargo.lock (the remaining nom 7 comes from criterion's terminfo, no diagnostic). The gungraun/valgrind path stays what it always was — a complementary deterministic counter, not a replacement.
- mdbook linkcheck: `perf/verify/linkcheck.sh` wired into CI — first step of the Test workflow's `test` job (PR gate, fails fast before any toolchain setup) and a pre-build step of the Docs workflow (validates before every Pages deploy). testing.md now records which verify runners CI covers verbatim vs in reduced form.

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
