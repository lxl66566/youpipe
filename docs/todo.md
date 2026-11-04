# TODO

Scope: **open work only**. An entry lives in this file from creation until it lands; the moment it is done it is deleted here, in the landing commit — never kept as a struck-through or "(landed)" tombstone. The landing record and its measured readings live in the topical docs under docs/src/dev/ (scheduler.md / the benchmarks chapters / streaming.md / core-types.md / crossfire-waker-designs.md) and in code comments; do not redo them from memory. Falsified directions are in [dev/dead-ends.md](src/dev/dead-ends.md). Entry numbers are never reused (the dev docs reference entries as "todo #N"); deleted entries leave permanent gaps in the numbering.

Verification conventions (see `dev/benchmarks/`):

- Every verdict uses `perf/bench-suite` interleaved A/B (same machine, same session, median-of-rounds).
- The 100K fused families must run as isolated per-id interleaving (running the whole group has a 2–3× collapse trap); the stream families drift ±10% and also need isolated interleaving.
- Scheduler-class changes use same-binary runtime-knob A/B (`bench_ab.sh -E`); recompilation swings tight benches ±30% through pure code layout (measured +31% on an untouched rayon anchor in 2026-07).
- Heavy-tail conclusions need multiple seeds (chunk-boundary luck dominates single-seed variance).

Priorities: P0 = high value / strong evidence; P1 = medium; P2 = experimental / micro-optimization / caliber fixes. Code locations are given by symbol name (line numbers drift on main).

---

### 14. [P1] pipeline_integration intermittent hang, residual (stream side)

- Context: the graph-engine occurrences are root-caused and fixed (2026-10-23, [benchmarks/graph.md](src/dev/benchmarks/graph.md)); the crossfire waker fire-steal — both faces (§12.5 cross-registry stale entry, §12.6 pop/wake waiter migration) — is fixed in the fork, records in [dev/crossfire-waker-designs.md](src/dev/crossfire-waker-designs.md). Attribution for the 09-28 stream-side occurrence remains circumstantial (no runtime evidence).
- Pending: the async face audit (async_rx/async_tx waker state machine) — the fix covers the shared `reg_waker`/`set_seq` steal surface async re-arms use.
- Standing: run `perf/verify/hang-stakeout.sh` (forensics-before-kill) on the next sighting before touching the process.

### 29. [P1] Engine duplication: the worker loop and the parking-lease admission live twice

- A 2026-11-05 structural review found the stream and graph engines carrying hand-synced copies of correctness-critical scaffolding:
  - The claim → map → staged-batch-deliver worker loop (`spawn_stage_fanout` / `spawn_expand_stage_fanout` in `builder/typed/stream.rs` vs `node_worker` / `expand_worker` / `source_feeder_worker` in `graph/exec.rs`): identical per-worker claim-cap share formula, tx-batch gate, scratch buffers, cancel-observed-before-map and deliver-even-on-cancel semantics — parity kept by cross-referencing comments, and the tx-batch gates have already drifted (stream gates on `!ctx.has_async_stage`, graph has no equivalent).
  - The parking-lease admission (`resolve_admission` vs `resolve_budget`): same pinned-vs-pool budget, nested-run fallback, snapshot → need → CAS → fallback skeleton; the stream copy carries the 2026-10-20 stale-snapshot underflow fix, the graph copy structurally cannot reuse it as written.
- Fix shape: one generic worker-loop driver (claim/deliver closures, monomorphizes identically) in `handoff`; one `admit(pool, fixed_jobs, pinned_workers, unpinned_count, budget)` in `builder/` with the invariant comments in one place. Any future cancel-semantics or liveness fix (todo #21-class) otherwise lands in both engines or silently misses one.
- Verify with the full test suite + a spot horizontal run (the loops are the streaming hot path; the refactor must be code-motion only).

### 30. [P2] Engine duplication, second tier: spin state machines, bridge loops, staged-flush unsafe

- Same review, lower-stakes duplicates:
  - `AnchorSpin` (stream.rs) vs `RoundSpin` (graph/fanin.rs): the same ~120-line adaptive-spin state machine with the constants (`SPIN_ROUNDS_PER_US`, `MAX_POLL_PAUSES`) declared twice — fanin.rs says "copied from the stream engine's". Unify behind one `SpinBudget` generic over a `poll_once()` closure; no behavior change (this is not spin-tuning).
  - Six copies of the cancel-parity forward loop across the bridge threads/tasks (three sync `recv→send` bodies, three async `recv_cached→send_cached` twins) — one `forward_sync`/`forward_async` pair in `handoff` covers them.
  - The unsafe staged-flush contract (`graph/fanin.rs::flush_staged` vs `SendItem::send_batch`'s tail): two hand-written MaybeUninit move-out bookkeeping paths; express the graph side through a `SendItem` method so the unsafe surface exists once.
- The `drain_async_sharded` trait requirement (no default impl, six collector impls) for the default-off falsified `YOUPIPE_SHARDED_TERM_ASYNC` flavour is pure tax on every new collector type; default it (unreachable body when the knob is off) or feature-gate the flavour.

### 31. [P2] Terminal run-family triplication on `StreamMode`

- The 11-12-method terminal family (run/collect/for_each/reduce/fold/sum/count/min/max/try_run/…) is hand-copied across the Unordered/Ordered/Grouped impl blocks; the copies already produced one real API hole (`Grouped::max` was missing until 2026-11-05). Move the family onto one `impl<.., M: StreamMode>` block with the mode trait supplying the pass-core selector (Grouped needs its `Out = Vec<O>` payload split kept typestate-clean); ~300 lines of copy-paste go away and the next terminal lands once.

### 32. [P2] `AsyncFanIn::anchor` allocates three `Vec`s per park episode

- `graph/async_exec.rs`: fresh `pairs`/`order`/`futs` `Vec::with_capacity` on every any-of-N park cycle (every empty-input gap of every async-node task). `futs` borrows `self.slots` and stays, but `pairs`/`order` are plain index buffers that could be `AsyncFanIn` fields (or a cursor rotation without collecting). Per park, not per item — worth an A/B before landing; the sync-side hoist in `fanin.rs` once measured a layout regression, so gate on measurement, not intuition.
