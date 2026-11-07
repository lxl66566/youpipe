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

---

### 43. [P2] backlog-scaled claim window for graph FanIn claims (r4 audit)

- Context: graph node workers claim with a fixed fair share (`per_worker_claim_cap` → `FanIn::claim`); the stream twin of exactly this face (31 workers on a trivial map feeding an async consumer) measured 31 % of span in the `SyncSender::send` tail CAS and won the mixed hop (merged/1K −33 %, 25/25) via `ClaimWindow`. handoff/mod.rs explicitly notes the port was never measured for the graph engine.
- Dead-ends check: the falsified entry is the *universal* window (pure-sync fence +60 %); scope here follows the landed async-fed rule (graph `MixedOnly` analogue).
- Protocol: add a `graph_shapes` id mirroring `sync_fuse/async_pair_cpu_split` (cheap sync node → async node), then same-binary env-scoped per-id A/B on it plus the existing `chain_*_graph`/`fanin_*_graph` ids as no-regression gates.

---

### 45. [P2] per-run ring recycling (channel construction cache)

- Context: per-run assembly is the dominant residual @1K fixed cost — graph edges allocate ~1.4 µs/ring (3 rings/run, graph.md "Per-run assembly attribution") and the stream sharded terminal holds 17.1 µs of `with_fence/1K`'s 164 µs (streaming.md). Analogue of the pool recycling cache: cache drained-and-closed crossfire rings keyed on (slot size, capacity), via a fork `try_reset` extension (acceptable only when both endpoint refcounts are zero and the ring is empty — the same class as `pop_batch`).
- Risk: the fork must stay diffable; the waker-registry reset semantics are the hard part.
- Protocol: `stream_pipeline/with_fence/1000`, `graph_shapes/chain_cheap_graph/1000`, `sharded_term` @1K cells; default-off env gate, then per-id `-E` A/B.

