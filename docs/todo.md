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


### 34. [P1] sync stage ring contention under trivial per-item work (async-fed chains)

- Context (2026-11-06, found while landing the async stage drivers, [dev/streaming.md](src/dev/streaming.md) "Async stage drivers"): in `stream(v).stage(cheap).stage_async(io)` the sync stage cannot take the fused pass-through, so 30 pool workers move 2000 trivial items through two rings in ~30→500 µs — ~7 µs per item per worker; the async side's last start trails the sync stage's last hop by only ~0.05–0.3 ms. perf: 31 % of samples in the mixed ring's tail-CAS backoff spin (`SyncSender::send`, per-item sends — async chains keep `YOUPIPE_STREAM_TX_BATCH` off); lifting that gate (throwaway prototype) shrank the span only to ~400 µs and moved the spin to the input head (`claim_poll` 29 %): each worker's claim share is `64 / 31 = 2` items per CAS.
- Candidate levers: a backlog-scaled claim share (claim `clamp(len / workers, 1, batch cap)` — large backlogs amortize the head CAS, small bursts keep the per-worker fairness `per_worker_claim_cap` exists for); re-measuring the async-chain tx-batch gate against the driver consumers (it was falsified against per-slot tasks — the precondition changed); fewer workers for stages whose measured per-item cost is below the ring handoff (the cost-EMA family is falsified twice on the fused side — an explicit hint, not inference).
- Gate: the horizontal `mixed_cpu_io` cells and `sync_fuse` async rows; `mixed_load` stream rows as the pure-sync control.
