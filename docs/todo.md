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
- Pending: the fire-path perf A/B of the waker fix (+1 SeqCst load +1 mfence per popped entry) under bench.lock.
- Pending: the async face audit (async_rx/async_tx waker state machine) — the fix covers the shared `reg_waker`/`set_seq` steal surface async re-arms use.
- Standing: run `perf/verify/hang-stakeout.sh` (forensics-before-kill) on the next sighting before touching the process.
