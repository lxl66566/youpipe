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
- Closed (2026-11-05): the async face audit found no race — every youpipe async re-arm site is clean by argument plus model check (3 loom arbiters pinning the async re-arm shapes: retire-keep, `_reg_waker_async` Reuse's stamp-preserving reset, future-Drop abandon; 4 exactly-once cancel-storm tests). Records in crossfire-waker-designs.md §16 (local doc, gitignored) and the test commit.
- Standing: run `perf/verify/hang-stakeout.sh` (forensics-before-kill) on the next sighting before touching the process.


### 33. [P1] graph engine residual per-run assembly cost (@1K)

- Context: the ~23 µs @1K graph−stream chain delta was attributed phase-by-phase and cut to ~3–9 µs by sharing each node's receiver set across its workers (2026-11-05, [benchmarks/graph.md](src/dev/benchmarks/graph.md) "per-run assembly attribution"). The residue sits outside `graph/`:
  - Per-worker sender clones: falsified and closed (2026-11-05) — the shared-sender `Delivery` reshape (one `Arc<[SyncSender]>` per node, closed edges as a per-worker `ClosedSet` mask instead of `swap_remove`) measured noise on every gated id (graph @1K −1.4…+0.8 %, below the 2 % floor; the stream gate rows carry no per-worker sender builds at all — the fused pass-through ids have no senders, and `with_fence`'s whole clone slice is ≈ 0.7 µs of a 164 µs run): the builds are tcache-cheap, unlike their receiver-side twins. Records in [benchmarks/graph.md](src/dev/benchmarks/graph.md) and the stream-side attribution in [benchmarks/streaming.md](src/dev/benchmarks/streaming.md).
  - Per-job pool submission ≈ 0.3 µs/job (HeapJob boxing, injector segment push, wake scan) — pool-side, shared with every engine; staging jobs into one batch was falsified (execution no longer overlaps late assembly).
  - Per-edge channel creation ≈ 1.4 µs (crossfire ring allocation) — symmetric with the stream engine, cancels in the engine delta but bounds both.
  - The async twin still clones per task (`AsyncFanIn::new(rxs.clone())` in `graph/async_exec.rs`); a default 128-task fan-out pays it 128× — fold into the same shared-set shape when the async face is touched next.
