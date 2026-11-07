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

---

### 45. [P2] per-run ring recycling (channel construction cache)

- Context: per-run assembly is the dominant residual @1K fixed cost — graph edges allocate ~1.4 µs/ring (3 rings/run, graph.md "Per-run assembly attribution") and the stream sharded terminal holds 17.1 µs of `with_fence/1K`'s 164 µs (streaming.md). Analogue of the pool recycling cache: cache drained-and-closed crossfire rings keyed on (slot size, capacity), via a fork `try_reset` extension (acceptable only when both endpoint refcounts are zero and the ring is empty — the same class as `pop_batch`).
- Risk: the fork must stay diffable; the waker-registry reset semantics are the hard part.
- Protocol: `stream_pipeline/with_fence/1000`, `graph_shapes/chain_cheap_graph/1000`, `sharded_term` @1K cells; default-off env gate, then per-id `-E` A/B.


---

### 48. [P2] sharded-terminal per-shard depth caps the collector's batch claims (r5 audit)

- Context: the sync sharded terminal sizes each shard at `max(MIN_SHARD_BUFFER, buffer / workers)` (`sharded_mpsc_channel`), and `buffer` resolves to `max(256, parallelism * 4)` (`floored_channel_capacity`) — a 32-worker single-stage terminal runs 8-deep shards while the collector's `drain_pass_batched` claims with a 64-slot scratch (`batch_recv_cap`). r5 attribution (`hotpath-profile stream-engine-ordered` / `stream-engine`, 1M x 20, one process per scenario, bench.lock): the collector paces both shapes (`drain_ordered_sharded` 150.7 / `drain_unordered_sharded` 164.5 ns/item, inside the family's bistability band), `MpscReceiver::try_recv_batch` holds 77-84 % of the drain at 5.0 M calls / 20 M items = 0.25 claims/item — real claims cluster at the shard cap (~8 items), paying the stamp-walk preamble plus one SeqCst cursor store per ~4 items (0-claim probes P50 50-81 ns, full claims µs-scale). Deep terminal shards are already the landed verdict where `YOUPIPE_CHAIN_FLOOR` reaches the terminal buffer through `stage_buffer` (`Chunked`-fence chains run 2048/shard at 100K); the 8-deep cell is pure-sync chains — cancel-token / worker-pin forced streaming, the fused-eligible shapes.
- Dead-ends check: distinct from the falsified async-terminal in-flight bound (wake-chain mechanism, async flavour), from the universal claim window (input-ring claim size; `with_fence` +60 %), and from the seq-less slot flavor. Not a `chain_floor` predicate extension: pure-sync chains stay unlifted, only the terminal factory's per-shard floor moves (aggregate rises above the single-channel backpressure point — the documented trade to beat).
- Protocol: same-binary env knob (e.g. `YOUPIPE_SHARD_DEPTH`) flooring per-shard capacity at `max(current, batch_recv_cap())` when `YOUPIPE_BATCH_RECV` is on; `bench_ab.sh -E` per-id isolated interleaving on `sharded_term/single_unordered_{cheap,cpu}` + `single_ordered_cpu` @1K/@100K (the collector-paced cells), `workers2_cpu` / `multi2_cpu` as shallow-shape controls, `stream_pipeline/single_stage_{ordered,unordered}` + `mixed_load/youpipe_stream_cpu` as no-regression gates; accept only above the 2 % floor on the target cells with the controls flat.

---

### 49. [P2] hotpath probe for `ReorderBuffer::insert_into` (r5 audit, caliber)

- Context: every ordered drain (`drain_ordered_with` / `drain_ordered_sharded` / async twins) carries `#[hotpath::measure]`, but the reorder slice inside them has no probe — r5 had to size it by subtraction (`drain_ordered_sharded` 150.7 vs `drain_unordered_sharded` 164.5 ns/item: the reorder + scratch-drain + accounting slice is sub-noise on the in-order-dominated shape). The skew-heavy shapes (a straggler worker forcing the slot-write slow path per item; the 2026-10 `sharded_term/single_ordered_cpu` bistability investigation, ~29 vs ~80 ms from unchanged binaries) have no direct reading of the fast/slow path split inside `insert_into`. One `#[cfg_attr(feature = "hotpath", hotpath::measure)]` on `insert_into` (not also on `flush_ready_into` — the nested span would double-count) closes the gap; zero-cost with the feature off.
- Dead-ends check: none — pure instrumentation.
- Protocol: after landing the probe, rerun `stream-engine-ordered` (fast path ≈ 1 call/item) and a straggler-shaped variant before proposing any further ordered-path change; r5's negative result — ordered ≈ unordered within the family's bistability band, per-item reorder cost below the action floor — is the baseline any proposal must beat.
