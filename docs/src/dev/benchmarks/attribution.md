# Attribution and Forensics

## hotpath instrumentation round (2026-09)

With the `hotpath-profile` binary of `crates/youpipe-bench` (p50 percentiles over `HOTPATH_OUTPUT_FORMAT=json` reports; p50 is the noise-robust statistic, and raw call counts from hotpath are approximate under load because its per-thread batch queue drops events):

- `sync_lightweight` 1M warm: `collect` p50 ≈ 262 µs ≈ 61 GB/s of buffer traffic, so the fused index path is memory-bandwidth-bound at large N; the documented criterion gap at 1M is dominated by the benchmark's own input-rebuild (`warm_clone`) in the timed region, which rayon's borrow-based `par_iter` does not pay.
- 1K `cpu_heavy`: `inject_batch` p50 ≈ 5 µs of a ~32 µs collect. Attributed at the time to worker wake latency (see the rejected cost-adaptive chunk experiment in [scheduler.md](../scheduler.md#dispatch-granularity--cost-adaptive-chunk-counts-tried-rejected)); the later `available_parallelism` find shows most of that wall was syscall overhead that hotpath does not attribute to a function, not wake latency.

## Data-plane probes & true-streaming baseline (2026-09-29)

Follow-up to the stream-engine finding (1M×30 unordered single stage at ~365 ns/item, worker-side pool activity 0.4 %): none of the 72 probes covered the crossfire wrappers in `crates/youpipe/src/handoff/channel.rs`, so producers blocked inside crossfire were invisible. Every wrapper now carries `#[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = ...))]` (feature-gated, zero-cost when off):

- MPMC sync: `SyncSender::{send,try_send}`, `SyncReceiver::{recv,try_recv}`
- MPMC async: `AsyncSender::{send,try_send}`, `AsyncReceiver::{recv,try_recv}`
- MPSC sync: `MpscSender::send`, `MpscReceiver::{recv,try_recv}`
- MPSC async: `MpscAsyncSender::{send,try_send}`, `MpscAsyncReceiver::{recv,try_recv}`

The `SendItem`/`RecvItem`/`AsyncRecvItem` trait impls delegate to these inherent methods, so every call funnels through exactly one probe (no double counting; all `T` monomorphizations aggregate under one label). `hotpath-profile` gained four true-streaming scenarios (dormant cancel token where the chain is pure sync, the `run_stream_engine` trick): `stream-engine-ordered`, `stream-engine-foreach`, `stream-engine-fence` (Chunked 500), `stream-engine-async`. Run one scenario per process invocation: the `HotpathGuard` aggregates function stats over the whole process, so a mixed run merges the per-scenario numbers.

Caliber: attribution only. The per-call guard inflates absolute per-item cost ~2.6× (unordered 1M: 957 ns/item probed vs 365 ns unprobed); compare distributions and ratios, never unprobed wall time. The guard itself is cheap, the same probe reads p50 40 ns on `SyncReceiver::try_recv`, so the 551 ns p50 on `MpscReceiver::try_recv` is channel cost rather than instrumentation. Call counts on the hottest probes undercount ~24 % (hotpath's per-thread batch queue drops events); durations average over sampled calls.

Baseline (32-core, unpinned, release, 1M items; "% wall" sums aggregate thread time, so >100 % means multi-threaded):

| scenario (iters) | ns/item probed | collector drain | inside the collector |
| --- | --- | --- | --- |
| unordered ×30 | 957 | `drain_unordered` 99.9 % | `try_recv` 60 % (p50 551 ns), `recv` anchors 33 % (9.4 M × ~1 µs, burst ≈ 3.2) |
| ordered ×20 | 1 103 | `drain_ordered` 99.9 % | `try_recv` 91 % (p50 972 ns), anchors 4 % (burst ≈ 24) |
| for_each ×20 | 990 | `drain_unordered` 99.9 % | `try_recv` 70 % (p50 712 ns), anchors 25 % — same shape as `.run()` |
| fence ×5 | 1 868 | `drain_unordered` 99.8 % | anchors 94 % (4.2 M × 2.09 µs, burst ≈ 1.2), `try_recv` 1 % |
| async ×10 | 392 | `drain_unordered_async` 99.3 % | anchors 63 % (0.96 M × 2.5 µs), `try_recv` 11 % (p50 20 ns) |

Producer side (worker thread budget ≈ 31 workers × wall):

- Terminal fan-in send: unordered `MpscSender::send` p50 401 ns (fast path) but p90 65 µs / p99 193 µs; workers park inside crossfire for 552 s aggregate = 62 % of the worker budget (ordered: 64 %, avg 32 µs). This is the previously invisible "producer blocked in crossfire".
- Input-side anchor (`SyncReceiver::recv`, waiting for the single feeder): 36 % of the worker budget (avg 34.6 µs). Cross-check: pool activity (`sleep` + `no_work_found` + `find_work` + `steal`) totals ~1.7 s aggregate ≈ 0.2 % of the worker budget, so the "0.4 % worker activity" reading reproduces and the missing ~98 % is now accounted for: ~62 % parked in terminal `send` plus ~36 % parked in input `recv`.
- Feeder: `SyncSender::send` 901 ns avg × 30 M = 94 % of the wall; the single feeder thread is saturated pushing into the 31-thief MPMC ring, a co-bottleneck within ~6 % of the collector's 957 ns/item.
- The fence shape inverts the picture: terminal fan-in uncontended (`MpscSender::send` p50 30 ns), waiting dominates instead. Stage-2 workers block on `SyncReceiver::recv` avg 30.7 µs at chunk boundaries and the collector starves (burst 1.2, anchor 2.09 µs ≈ 94 % of wall). Stop-and-go convoy, a distinct pathology from fan-in.
- The async mixed chain is the healthiest shape (392 ns/item): the async consumers absorb per-item awaits overlapped (10 M `AsyncReceiver::recv` avg 43 µs each, the per-item waker round-trip the burst-drain collector exists to avoid), and the collector's `MpscAsyncReceiver` side is nearly free (try_recv p50 20 ns).

Attribution verdict for the single-stage shape: fan-in contention, cache-line form. The collector's `try_recv` pays ~550–1000 ns/item (~20–40× the 23 ns/item raw MPSC capacity) because 31 senders' ring updates invalidate the line the single collector polls; backpressure pacing (worker parks) is the propagated effect rather than the cause, and a wakeup storm is ruled out (anchors are 1–2 µs waits, pool wakes ≈ 0.2 % of worker budget). This supports relieving the fan-in through per-worker SPSC shards or a burst-recv interface. Caveat: the feeder-side MPMC send (901 ns, 94 % of feeder) sits within ~6 % of the collector's pace, so terminal-channel sharding alone may just move the critical path to the feeder, while a batched recv interface relieves both sides. Ordered mode's +15 % is a higher per-call `try_recv` under bigger bursts rather than reorder cost (invisible, ≤ 6 % residual).

Reproduce:

```sh
for s in stream-engine stream-engine-ordered stream-engine-foreach \
         stream-engine-fence stream-engine-async; do
  HOTPATH_OUTPUT_FORMAT=json-pretty HOTPATH_OUTPUT_PATH=/tmp/$s.json \
    cargo run --release -p youpipe-bench --bin hotpath-profile \
      --features hotpath -- $s 1000000 30
done
```

## Final criterion verdict (2026-09 round, clean 4-pass interleaved A/B)

Baseline = `9b31fb0` (crates.io st3 0.4 / concurrent-queue 2.5) vs the branch (vendored forks + pool-job feeder), median of two passes per side, `taskset 1-31` both sides:

- `stream_pipeline` 1K: ordered −13.5 %, unordered −13.4 %, multi_stage_2 −13.9 % (pool-job feeder + injector backoff)
- `stream_pipeline` 100K: ordered −8.8 %, unordered −6.3 %; with_fence/100K −5.7 %
- sync_lightweight cold/100K −5.3 %; sync_for_each 100K −6.8 %; sync_cpu_heavy sequential/10-100K −1.6…−5.4 %
- everything else (CPU fused vs rayon, unbalanced, io_async, oversubscribe) within ±2 % noise; no regression beyond noise

Methodology notes learned the hard way, worth repeating: (1) a full-group criterion pass has ±10 % inter-run variance on the stream family (each iteration runs a pool-wide wake cascade), so verdicts for that family need isolated alternating runs (`--sample-size 20`, base/new/base/new), which confirm every stream win above; (2) `target/criterion` accumulates `base/`/`change/`/saved-baseline subdirectories from earlier rounds, so a diff script must read only `new/estimates.json` or it silently compares against stale runs (this produced phantom 2–4× "regressions" on the 100K sync benches that vanished on re-measurement).

## Round 2 verdict (2026-09-02: lazy reorder, fence recycling, `push_n`)

Three further optimizations (see [streaming.md](../streaming.md) and the vendored-queue notes in [scheduler.md](../scheduler.md#vendored-scheduler-dependencies-vendor)), A/B'd with isolated alternating runs (3 rounds, median) against the round-1 tip:

- ordered stream 100K **−21.7 %** (lazy slot array: zero allocation, zero init, zero cache traffic for in-order streams), with_fence 100K **−23 %** (batch-allocation recycling); 10K sizes −2…−3 %
- `push_n` segment reservation: cpu_heavy fused dispatch −0.5…−2.6 %, sync_lightweight/try_collect −1…−2 %
- no regression anywhere, including the four 100K fused benches that a full-group pass flagged at +80…+290 %

That last point is a third measurement trap, bigger than both above: the full-group `sync_vs_rayon` pass can collapse the 100K (800 KB-input) benches by 2–3× regardless of code version. The unmodified baseline binary reproduces the same collapse minutes later, and the pure-rayon control bench (`mixed_load/rayon_par_iter`, which shares none of youpipe's code) swings +5.2 % in-group vs +1.2 % isolated. The 1M (8 MB-input) sizes stay clean at ±1 %, so the mechanism is the 800 KB-mmap-regime allocation path interacting with whole-group sequence state, not code. Verdicts for the 100K fused family must come from isolated alternating runs; in-group numbers for that family are meaningless on this machine.

The vendored queue's loom suite also has a runtime trap: without upstream's CI setting `LOOM_MAX_PREEMPTIONS=2`, the `spsc`/`spsc_force` models run for an hour+ without completing; with it the whole suite finishes in seconds.

## Fence forwarders as leased pool jobs (2026-09-28)

The five `FenceLink` spawn sites replaced their per-run `std::thread::spawn` with a leased pool job routed through `StreamCtx::spawn_stage_jobs` (pool mode only; the dedicated-thread fallback keeps the OS thread), and `try_exec`'s lease reservation gained a `fences` term (see [streaming.md](../streaming.md)). Verified with 5-round isolated interleaved A/B (`bench_ab.sh -1`, `stream_pipeline` families, base b2e25e8):

- `with_fence/1K` **1.603 ms → 1.542 ms, −3.8 %** (25/25 dominant, spreads 1.7/2.6 %): the removed thread spawn+join is the ~61 µs/fence fixed cost, matching the 30–80 µs estimate from the feeder-side removal.
- `with_fence/100K` −0.4 % noise: the fixed saving is ~0.03 % of a 209 ms run, invisible by design.
- No-fence regression check (`single_stage_unordered`, `multi_stage_2` at 1K/100K): all noise (±0.4 %, no dominance).

Correctness: without the `fences` term, the shape `stage_with(6) → fence → stage_with(1)` on an 8-thread pool admits feeder + 6 + forwarder + 1 = 9 parking jobs; the injector FIFO pops [feeder, stage-1 ×6, forwarder] onto the 8 workers and the queued stage-2 job wedges the run with every worker parked on a full channel. `test_fence_forwarder_counts_in_parking_lease_no_deadlock` hits the 30 s watchdog without the term and completes (dedicated-thread fallback) with it.

## LLVM folds constant-iteration CPU work (2026-09-07)

A fourth trap, found while chasing the expensive-item regime: the `r = r*7+13` CPU-work kernel is a first-order linear recurrence, which LLVM strength-reduces by folding every 8 iterations into one `imul $0x57f6c1` + `add $0xbe96a0` (7⁸ and 13·(7⁸−1)/6). Runtime iteration counts measure ~0.095 ns/iter rather than the ~1 ns a naive cycle estimate suggests, and when the iteration count is a compile-time constant (the horizontal `cpu_balanced`'s 100, criterion `sync_cpu_heavy`'s same shape) the whole loop collapses to its closed form: ~6 ns/item, far off the "100 iters ≈ 100 ns" the bench comments claimed.

Fairness survives (both frameworks run the same folded kernel), but every absolute load level in those tables is ~16× lighter than documented, and the expensive-item regime (µs–ms per item, where scheduler handoffs matter most) was never exercised by them. Consequences:

- The 1M `cpu_balanced` parity with rayon does not extend upward: at 2M/4M (5-round medians, see [counters.md](counters.md)) rayon pulls ahead +14 %/+12 %, because above 1M the batches leave the cache-resident regime that hides per-item path differences.
- Load-level claims in bench comments must derive from measured ns/iter at the actual iteration count, not from iteration counts.
- Expensive-item benchmarks need runtime iteration counts so LLVM cannot fold them: the criterion `unbalanced` family's `(item, iters)` tuples and the `zstd_shape` group (see [scheduler.md](../scheduler.md) "flat top-level dispatch") are built that way.

## Codegen layout lottery and 32 B function alignment (2026-10-06)

An acceptance bench surfaced the sharpest instance of the layout trap yet: horizontal `cpu_balanced` 100K–4M regressed +29…+40 % against v0.6.0. A same-session interleaved bisect (the rayon and std-threads controls stayed flat, so the environment was clean) pinned the first bad commit to `5e6bd83`, a change touching only async parking code that the sync fused-collect path never executes. The regression was a pure instruction-layout shift under `codegen-units = 1` (plus thin LTO letting a crossfire-only edit reshuffle the bench binary): the hot leaf loops landed on worse addresses.

Mitigation, now permanent in `.cargo/config.toml`:

```
rustflags = ["-C", "panic=unwind", "-C", "llvm-args=-align-all-functions=5"]
```

With 32-byte function alignment both endpoints measure 1.00x (v0.6.0 vs the campaign tip, 4 interleaved pairs), the bad commit itself measures 1.00x against its parent, and v0.6.0 with the flag is 1.00x vs without, so the grid costs nothing on a lucky layout. Alignment does not fix everything: `cpu_balanced_readback/100K` keeps a ~+7 % wobble that bisects to `98c8d4d` (crossfire-only changes, likewise unreachable from that path) and survives both `align-all-functions=6` and LLVM's JCC branch-target alignment (`-x86-align-branch-boundary=32` plus `-x86-align-branch=fused,jcc,jmp,call,ret,indirect`, tested 2026-10-06, no effect, the flags were not adopted). That residual is most likely heap/page placement of the output buffer the fold reads rather than code layout, and no available compiler flag stabilizes it.

Practical consequences:

- Any cross-commit benchmark comparison must be same-session interleaved and should treat ±10 % on a single sub-case as layout noise until bisected; ±30 % is not the ceiling, +40 % was measured.
- The `~/.cargo/config.toml` host profile (`lto = "thin"` on `bench`) makes the lottery span crates: an edit in `youpipe-crossfire` can shift the fused collect loop that generics monomorphize into the bench binary.
- `codegen-units = 1` alone never killed the lottery; it only made the dice rolls rarer and bigger. Function alignment is what makes the grid stable.
- The flag is repo-local: rustflags do not propagate to downstream crates, and neither does the lottery. A downstream binary's layout is an independent draw driven by its own dependency graph, so a "regression" of this class measured in our bench binary says nothing about theirs, and perf-sensitive downstream A/B comparisons need the same flag in their own `.cargo/config.toml`, where it covers the whole dependency graph.

A fifth face of the lottery (2026-10-18): the same commit compiled in two independent `bench_ab.sh` materializations swings too, because cargo's metadata hash is path-dependent and each materialized worktree path yields a differently-laid-out bench binary. The cancel/fence/async 1K pair family in `sync_fuse` swung ±2–7 % across independent same-commit builds: one session's +4.0 % stable 0/9 "regression" of `cancel_pair_cpu_split/100000` measured −1.2 % in the next session's build of the identical commit, and the tell was non-monotonic (with-shortcut and without-shortcut bins both slow while the merged bin is not, impossible for a semantic effect). Verdicts for that family need same-commit re-materialization cross-checks, not just interleaved rounds.

Settled the same day by double-controlled A/B (solo cherry-pick and main-revert sides agreeing): the injector `is_empty()` advisory gate in `pop_injected_job` costs ~+2.3 % on `mixed_load/youpipe_stream_cpu/1000` and buys nothing measurable elsewhere, and the −8 % first attributed to it was itself a branch-level layout artifact. Kept for now (within the pair-family noise band, and the theory of skipping a locked op on idle passes still holds); revisit when next tuning the injector path.

## `sharded_term_async` async1 @100K draw band (2026-10-18)

A v0.6.0-vs-0.6.1 full interleaved A/B flagged `async1_cpu/100000` +18.1 % and `async1_ordered_cpu/100000` +11.1 % (0/9 dominant), with `async0_cheap/100000` +6.6 % weaker and all 1K rows inside ±2 %. The flag is a draw artifact of this family (knobs off, the sharded paths are inactive), established by elimination plus two measurements:

- Semantic survey d18f24f..fed26b2: on the default-knob async1 path only e1fa541 (the injector `is_empty` gate) executes new code. 58ec78b adds terminals the bench never calls; cb4c162/7878716/6445f59 are FWD_BATCH/SHARDED_TERM-gated; 97a7457 moves OnceLock readers; c96870a shares an async consumer loop that is byte-identical after monomorphization; 07902f7+8248d1b net to a comment.
- Gate re-check, same binary (temporary local `YOUPIPE_INJ_EMPTY_GATE` patch, 5 per-id rounds): gate ON is the faster side, async1_cpu −6.7 % (21/25), ordered −5.9 % (24/25), both noise-grade. The gate is exonerated and mildly favourable here, so the flagging regression cannot be scheduler-semantic.
- Three-draw band, same session (`base=d18f24f`, `h1=h2=fed26b2`, 5 per-id rounds; medians of round medians, ms):

  | pair @100K        | async1_cpu | async1_ordered_cpu | dominant |
  | ----------------- | ---------- | ------------------ | -------- |
  | base → h1         | +5.9 %     | +8.4 %             | base 21/25, 23/25 (noise) |
  | base → h2         | −3.4 %     | +3.9 %             | 17/25, 9/25 (noise) |
  | h1 → h2 (same commit) | −8.8 % | −4.2 %            | h2 22/25, 22/25 (noise) |

  The same-commit pair swings as much as any cross-commit pair, and no pairing approaches +18 %. (h1 sat mid-round-order throughout the 3-sided interleave; the cpu id showed a fast-first-position pattern there but ordered showed the reverse, and any middle-position penalty only inflates base→h1, the one regression-lean cell.) Round medians per binary span 31.9–38.3 ms (cpu) and 38.5–49.5 ms (ordered); cross-session, the same v0.6.0 commit drew 30.3–31.6 ms in the flagging session and 33.6–36.5 ms in this one.

Verdict: no semantic regression in v0.6.0..0.6.1 on this family; a small (+4–8 %) ordered lean seen across both HEAD draws stays inside the same-commit swing and has no candidate mechanism (the ordered drain is untouched, the window-sizing changes are sharded-gated). Verdicts for `sharded_term_async` @100K need ≥5 rounds and a same-commit re-materialization cross-check; single-pair deltas up to ~+18 % sit inside the family's draw band.

## v0.6.1 release regression check (2026-10-18)

Full-suite interleaved A/B, `v0.6.0` (d18f24f) vs the 0.6.1 tip: 10 criterion targets × 3 rounds × 2 sides (filter-chunked for the 10-min tool cap; every round interleaves both sides, taskset 1-31). Every flag resolved as layout/draw artifact or accepted tradeoff; no semantic regression remains:

- `channel_throughput/std_mpsc_bounded/10000` +71.8 %, and the `expand_heavy` rayon rows +6.6…+14.4 % stable, both control rows on std/rayon code; same-commit re-materialization showed the std row bimodal ±22…143 % and HEAD draws of the rayon rows spanning 1.06→1.13 ms. The youpipe `owned_vec` +7.4 % sits inside the same draw band (130.6→144.4 ms across HEAD draws).
- `sync_fuse` merged/1K pair family +2…+14 %, the documented swing family; same-commit probe read −5.8…+1.9 % with 7–30 % spreads.
- `stream_pipeline/multi_stage_2/1000` +3.2 % dominant; same-commit +1.2 % with a 14 % spread on one side.
- `sharded_term_async/async1_*@100K` +11…18 %, a draw artifact (section above).
- `sync_reduce/youpipe_sum_owned/1000` +3.2 % (+330 ns at the 1K fixed floor, tight spreads both sides, same-commit ±0.5 %): the profile and magnitude match the injector `is_empty` gate's documented fixed cost (kept; see the registry.rs re-check note). Everything ≥10K is noise.
- Real improvements: `sync_filter/youpipe_filter_map` −2…−8 % (dominant), `pool_reuse/transient_*` −3 %, `expand_heavy/push_emit` −3 %, `sync_nested_on_pool` 1K −2 %.

## Collector 0-claim probe + fence staging reuse (2026-10-20)

Two stream-hygiene changes: the collector's 0-claim probe resolves via one `is_disconnected` tx-count load instead of a second `try_recv` ring walk, and `fence_batch_send` reuses a forwarder-owned staging scratch instead of `collect()`-ing a fresh `Vec` per released chunk. Both changes sit behind default-off knobs, so the A/B ran with `YOUPIPE_BATCH_RECV=1 YOUPIPE_FWD_BATCH=1` on both sides (`bench_ab.sh -a base=main -b wt -r 5 --per-id`, targets `async_vs_tokio` + `sharded_term`, env via `-E`); `tokio_spawn_blocking` is the off-data-plane layout control:

| id | base (main) | wt | Δ | verdict |
| --- | --- | --- | --- | --- |
| `stream_pipeline/single_stage_unordered/1000` | 9.78 µs | 9.54 µs | −2.5 % | improvement, 25/25 dominant |
| `stream_pipeline/single_stage_unordered/100000` | 112.3 µs | 118.2 µs | +5.3 % | noise — base bimodal 94–118 µs, wt tight 117.5–119.4 µs |
| `stream_pipeline/single_stage_ordered/1000` | 9.72 µs | 10.76 µs | +10.7 % | noise (10/25, spreads 18 %/12 %) |
| `stream_pipeline/single_stage_ordered/100000` | 117.7 µs | 117.7 µs | −0.0 % | noise |
| `stream_pipeline/with_fence/1000` | 717.7 µs | 711.3 µs | −0.9 % | noise |
| `stream_pipeline/with_fence/100000` | 61.12 ms | 60.41 ms | −1.2 % | noise |
| `sharded_term/single_unordered_cpu/100000` | 21.37 ms | 21.71 ms | +1.6 % | noise |
| `sharded_term/multi2_cpu/100000` | 16.57 ms | 16.56 ms | −0.0 % | noise |
| `sharded_term/workers2_cpu/100000` | 2.63 ms | 2.24 ms | −14.9 % | noise (spreads 17 %/56 %) |
| `tokio_spawn_blocking/spawn_blocking_cpu/1000` (control) | 2.301 ms | 2.299 ms | −0.1 % | noise |

One dominant improvement, the 1K unordered single stage, the shape with the most burst gaps per item (the probe runs once per gap), and zero regressions beyond noise. The predicted magnitude was sub-1 % wall, and the staging reuse (with_fence rows) is indeed invisible at criterion resolution (one k-sized allocation per k items); the value of the pair is consistency across the three batch-claim sites rather than throughput. The `single_stage_unordered/100000` base side is bimodal across rounds (94–118 µs) while the wt side is tight, consistent with dropping the extra empty-ring walk, though 5 rounds do not separate that from draw.

## fire() wake-one accounting (2026-10-21)

`RegistryMulti::fire()` wakes exactly one committed waiter per event and is expressed as `_fire_n(1)`. Code reading says the legacy body was already wake-one on committed queues, so the expected verdict was neutral and the A/B served as the release gate. Same-binary env A/B (temporary `YOUPIPE_FIRE_ALL` knob, deleted with the legacy body after the decision): `bench_ab.sh -a legacy=wt -b wakeone=wt -E … -B channel_bench -B mixed_load -B io_async -B async_vs_tokio -B expand -r 3` (`target/bench-ab/fire-wakeone`).

Zero regressions across all 40+ ids; every crossfire-mediated family (`youpipe_mpsc`/`youpipe_mpmc`, `stream_pipeline`, `io_async`, `expand_heavy`, `mixed_load` youpipe rows) verdicts noise, |Δ| ≤ 6 % with bistable spreads. The only two improvement rows are controls that never touch crossfire (`channel_throughput/crossbeam_bounded/100000` −8.0 %, `mixed_load/tokio_spawn_blocking_cpu/1000` −2.7 %): the session leaned slightly toward the wakeone side, which the drift-detection controls exist to expose. Convoy shapes separately: async1/async2 bistable with medians flipping within noise on both sides (see [streaming.md](../streaming.md) "Async face re-attribution (2026-10-21)").

## Async-chain capacity floor (`YOUPIPE_ASYNC_CHAIN_FLOOR`, 2026-10-22)

The remaining convoy direction, capacity-side decoupling of the async terminal, was resolved by measurement in three steps (all convoy-probe async2 @100K unless noted; "slow" = run > 100 ms, the forensics-calibrated collapsed threshold; fresh process per cell, sides alternating).

Step 1, terminal-only decoupling is falsified. Sizing only the terminal ring at the run's in-flight bound `n` (prototype knob `YOUPIPE_ASYNC_TERM_DECOUPLE`, byte-budget-gated): a 10-process soak read off 22/120 slow runs, on 17/120, so the deep slow mode persists at the same rate and only the medians tighten (on 19.1–20.7 ms vs off 20.5–41.9). The mid/mixed rings re-seed the per-item wake chain without the terminal's help.

Step 2, capacity sweep: ~16K everywhere beats `n`. `--buffer` sweep (4 processes × 12 runs per cell, mean of per-process medians):

| buffer | 256 (default) | 1024 | 4096 | 16384 | 65536 | 100000 (= n) |
| --- | --- | --- | --- | --- | --- | --- |
| mean med (ms) | 36.5 | 20.9 | 19.8 | **19.0** | 19.1 | 23.3 |
| slow runs | 9/48 | 1/48 | 3/48 | **0/48** | 0/48 | 0/48 |

Every ring at ~16K slots removes the slow mode entirely; `buffer = n` is slightly worse than 16K/64K (the bigger eager allocation's cache footprint). Isolation via `--pinbuf 16384` (stage outputs only, feeder and terminal small): 1/48 slow, so the sync-side rings carry most of the effect, and adding the terminal decouple completes it (0/48, 20.9 ms).

Step 3, the floor knob, default-on. The landed policy lifts every unpinned channel an async-tailed chain builds (stage outputs + the feeder ring) to `min(n, 16384)` slots, byte-clamped at 8 MiB per ring; explicit `buffer` pins win verbatim; pure-sync chains are not lifted. Soaks:

| cell | off ("0") | on (unset) |
| --- | --- | --- |
| async2, stage-outputs-only floor (pre-feeder-lift tree) | 30/120 slow | 3/120 slow |
| async2, final policy | 18/120 slow, med 20.5–41.9 ms | **0/120 slow, med 17.7–20.4 ms** |
| async2, final binary release gate (8 proc) | 21/96 slow | 2/96 slow, med 17.7–19.8 ms |
| async1 probe (6 proc) | med 21.8–23.2 ms | med 19.2–21.0 ms |
| async0 probe (6 proc) | med 52.0–67.1 ms | med 35.2–48.9 ms |
| sync2 / single / fence probes | flat both sides (knob inert, no async stage) | same |

Criterion A/B (`bench_ab.sh -E`, 3 interleaved rounds, same binary, off=`"0"` vs on=`"1"`): every async shape improves 9/9 dominant, `sharded_term_async` async0_cheap/100K **−41.8 %**, async1_ordered_cpu/100K **−15.3 %**, async0_cheap/1K −13.6 %, async1_cpu −6.3 %, async1_cheap −5.6 %, the 1K async1 rows −2.2…−2.8 %; `sync_fuse` async_pair_cpu_split/100K **−61.0 %** (the off side collapsed in 2/3 rounds: 49.7/77.5 vs 19.2–19.5 ms tight), merged/1K −22.0 %, split/1K −11.1 %, merged/100K −8.2 %. All pure-sync families noise; the one flag (`fence_pair_cheap_split/100000` +18.2 %) re-tested at 5 rounds reads −7.3 % noise (the documented ±28 % swing family, and a chain the knob cannot touch since `has_async_stage` is false). Final-binary confirmation on the promotion tree (`-E old="0"` vs new-default unset) favors the new default on every async row (old side collapsed 38–69 ms where the new side is tight at 19–23 ms).

The eager-allocation cost of the lift is bounded: 16K slots × 16 B = 256 KB per u64 ring, and the 8 MiB byte budget declines the lift entirely for payloads above ~500 B/slot, where per-item work dwarfs the ~1–2 µs wake RTT that drives the pathology anyway.

## Floor-tier promotion: 16K → 64K (2026-10-25, todo #4 residual)

The final-gate residual (~1 % slow runs, 2/216) was re-measured and the 64K tier decided on the same binary — the knob temporarily accepted an explicit decimal slot count (unset = the 16K tier at the time), so both sides were one build reading `YOUPIPE_ASYNC_CHAIN_FLOOR` per process.

**Slow-run rate (convoy-probe async2 @100K, deep-soak caliber — fresh process per cell, sides alternating with the leading side flipped on even rounds, `taskset 1-31`, 180 processes × 12 back-to-back runs per side = 2160 runs/side):**

| side | slow > 100 ms | mid-tail 50–100 ms | pooled med | pooled mean | proc-med mean |
| --- | --- | --- | --- | --- | --- |
| 16K (unset) | 10/2160 = 0.46 % (Poisson 95 % 0.22–0.85) | 38/2160 | 19.89 ms | 21.42 ms | 19.88 |
| 64K (`=65536`) | **0/2160** (95 % ≤ 0.14) | 1/2160 | 20.31 ms | **20.14 ms** | 20.32 |

The residual reproduces (10 vs 0, Fisher p ≈ 0.001) and 64K eliminates it — including the whole 50–100 ms mid-tail: the slow mode is capacity-graded, not binary. Pooled with the 2026-10-22 gate the 16K rate is 12/2376 = 0.51 %. The 16K fast-mode median is slightly better (19.89 vs 20.31, paired per-proc medians +0.42 ms, sign test p = 4×10⁻⁶) — but the 16K side's own tail costs more than that saves: the pooled mean is −6.0 % at 64K, and per-run-index means favor 64K at 10 of 12 indices.

**Throughput A/B (criterion, `bench_ab.sh -E` same-binary env sides 16k=`1` vs 64k=`65536`, 3 interleaved per-id rounds, `sharded_term_async` + `sync_fuse` async rows):** `async0_cheap/100K` **−30.7 % stable 9/9** (42.68 → 29.57 ms) — the async-only chain's single mixed ring stays backpressure-bound at the 16K floor; every other row noise (async1 family −3.6…+4.3 %, async_pair_cpu ±1 %, and the 1K rows are a pure control: both tiers clamp to `min(n, ·)` = 1000 slots). A 2×2 crossover falsification (both worktree binaries × both env values, interleaved) confirmed the delta follows the env, not the binary (16K-binary: 42.7/32.4 ms, 64K-binary: 43.5/29.8 ms, per floor=16K/64K) — not a code-layout artifact, the trap this caliber exists for.

**Verdict: promoted (`ASYNC_CHAIN_FLOOR_SLOTS = 65536`).** The promotion bar was "median cost within noise": by the criterion verdict caliber (median-of-rounds) every family is noise-or-better and one canonical family wins 30 %; the only median regression is the zero-CPU forensic probe's fast mode (+2.4 %), whose tail-adjusted aggregate still favors 64K and whose medians read flat (19.0 vs 19.1) in the 2026-10-22 sweep session — a corner effect, consistent with the 1 MiB ring recycling through L3 where the 16K tier's 256 KiB stayed inside one core's L2 (unmeasured directly; the cpu-bearing shapes hide it). Memory cost: 4× the eager ring allocation on async chains only (1 MiB vs 256 KiB per lifted ring for u64 slots), still byte-clamped at 8 MiB per ring — the clamp's payload threshold moves from ~512 B to ~128 B/slot, above which the lift declines exactly as before. The knob kept the explicit-tier escape (`=16384` reproduces the old default for the zero-CPU latency corner).

## v0.7.0 release regression check (2026-10-23)

Full-suite interleaved A/B, `v0.6.1` (0ddc9c2) vs the 0.7.0 tip: 11 criterion targets × 3 rounds × 2 sides (graph_shapes has no v0.6.1 counterpart, the engine is new in 0.7.0), taskset 1-31. The 0.7.0 defaults — sharded+batch terminals, the async-chain capacity floor, stream tx batching — show up as real, family-wide improvements: `sync_fuse` cancel/pair families −30…−91 % (async_pair_cpu_split/100K −91.4 %), `sharded_term` −34…−88 % (workers2_cpu/100K −78.5 %), `sharded_term_async` async0/1@100K −23…−42 %, `expand_heavy` fanout rows −44…−51 %, `stream_pipeline/with_fence` −21…−40 %, `channel_throughput/youpipe_mpmc`/`std_mpsc_unbounded` dominant wins. Every flag resolved as artifact; no semantic regression remains:

- `channel_throughput/youpipe_mpsc/10000` +32.2 % stable 0/9 collapsed under per-id isolation (5 rounds: +9.5 %, dominant 5/25; base side bimodal 71 % spread) — the documented placement sensitivity of the channel-throughput group, whose absolute medians also move ~3× between group-pass and per-id modes. Same-commit re-materializations (n1/n2, 5 rounds) sit +6 % apart, covering the residual lean.
- `sync_filter/youpipe_filter_map{,_owned}/1000` +2…+3 %: the control row `sync_reduce/sequential_sum` (pure sequential sum, no youpipe code in the loop) reads −18…−23 % *stable, 25/25* in the same binaries — the sync_vs_rayon bench binary's codegen got a full reshuffle, so the control row invalidates every tight ±3 % verdict from that binary. Per-id recheck of the filter rows: noise.
- `sync_reduce/rayon_sum/10000` +2.5 % (1/9): rayon control row, same layout drift.
- `channel_throughput/std_mpsc_bounded/10000` +77 % noise: the documented bimodal std row (same flag as the v0.6.1 check).

Horizontal rerun (5 interleaved rounds, cores 1–31) initially showed `cpu_balanced` youpipe 100K–4M +20…+40 % — **the one regression the criterion suite is blind to** (its `sync_lightweight/youpipe_par_map_borrowed/1M` twin stayed +0.1 %: different bench binary, different map closure, different layout draw). Interleaved same-session two-binary runs reproduced it; three independent materializations of HEAD agreed within ±1 %, so deterministic, not a draw. Bisect pinned it to 5d199e7 (stream tx batch): the `Vec::extend`-over-`drain` staging in `SendItem::send_batch` emits a panic landing pad that monomorphizes `drop_in_place::<Map<Drain<T>, _>>` per channel item type — three ~112 B glue functions that landed before the fused collect leaf in the single codegen unit and shifted its hot loop by 336 B (≡ 48 mod 64), across a 32 B uop-cache window boundary (disassembly: old and new loops byte-identical modulo the +0x770 shift). A new face of the [layout lottery](#codegen-layout-lottery-and-32-b-function-alignment-2026-10-06) that 32 B function alignment does not cure: glue emission, not function entry alignment, rolled the dice. Fixed by `stage_batch` (handoff/channel.rs): a panic-free pointer-move staging loop (reserve precedes every move) emits no landing pads and no per-type drop glue; `#[inline(never)]` makes the footprint caller-independent. Four independent builds verified flat (±2 %) against the pre-regression level. Structural mitigation left open: `-align-all-functions=6` (64 B) would pin every function start mod 64 and shrink this whole failure class, at the cost of re-rolling every bench baseline once.

Final 0.7.0 horizontal: fused CPU, io_async, io_blocking and cpu_unbalanced all reproduce the 0.6.1 readings within ±3 %; the streaming-engine scenarios capture the 0.7.0 gains — mixed_cpu_io/2K −5.5 %, real_doc/4K −3.1 %, real_web/2K −2.2 % vs the v0.6.1 session (data and charts refreshed in [counters.md](counters.md)).

## NT-store attribution (2026-09-25): the ≥2M gap was output RFO

Hypothesis (resolved P0): the collect output buffer is written once and never read (the bench only black-boxes and drops it), so every 64 B line pays a read-for-ownership plus L3 pollution before its DRAM writeback. The fused leaves therefore grew a runtime knob: `YOUPIPE_NT_STORE=1` writes eligible 8-byte outputs with `movnti` streaming stores (x86_64 SSE2 baseline; eligibility is compile-time, the store is a scalar drop-in for the leaf's scalar store loop; `sfence` orders the weak NT stores before every leaf exit, success and unwind, ahead of the completion latch's release). Default off, because a consumer that reads the output right after collect trades a cache hit for a DRAM round-trip.

A/B: horizontal `cpu_balanced`, same binary, two processes alternating (the knob is a process-level `OnceLock`), 6 pairs × `--rounds 2` (forward+reversed internal rounds), `taskset -c 1-31`, pooled medians with per-pair pairing and the rayon column as drift control:

| n | youpipe off→on (ms) | delta | rayon control |
| --- | --- | --- | --- |
| 1K | 0.010 → 0.010 | −1 % | ±0 % |
| 10K | 0.012 → 0.012 | +2 % | ±0 % |
| 100K | 0.053 → 0.048 | +10 % | ±0 % |
| 1M | 0.48 → 0.41 | +15 % | ±0 % |
| 2M | 0.95 → 0.82 | +15 % | ±0 % |
| 4M | 1.91 → 1.57 | +18 % | ±0 % |

Raw JSON under `target/horizontal/nt_ab2/` (not committed). Two methodology notes: (1) the knob treats any value other than "0" as ON, and a first A/B attempt passing `YOUPIPE_NT_STORE=off` enabled NT on both sides and measured +0 % everywhere; (2) two pairs of a prior run were polluted by background build load (rayon control drifted +4–14 %), which only per-pair pairing plus the control column exposed, since pooled medians alone would have read it as a win/loss.

Read-back follow-up (same day, same caliber): the feared consumer penalty did not materialize. New horizontal shape `cpu_balanced_readback`, the same `cpu_work(x, 100)` load, but after `collect()` the consumer folds the whole output `Vec` inside the timed region (write-then-immediately-read, the exact shape the default-off was guarding against). Same-binary two-process A/B, 5 alternating pairs × `--rounds 2`, `taskset 1-31`, per-pair pairing, rayon column as drift control, and the write-once `cpu_balanced` re-run in the same session (gain holds):

| n | write-once off→on (ms) | delta | read-back off→on (ms) | delta | rayon control (w / r) |
| --- | --- | --- | --- | --- | --- |
| 100K | 0.052 → 0.047 | +9.3 % | 0.081 → 0.072 | +11.5 % | +0.7 % / +0.5 % |
| 1M | 0.486 → 0.419 | +12.3 % | 0.656 → 0.580 | +11.6 % | ±0 % / −0.3 % |
| 2M | 0.962 → 0.830 | +13.6 % | 1.281 → 1.109 | +13.3 % | −0.3 % / −0.5 % |
| 4M | 1.947 → 1.612 | +17.8 % | 2.569 → 2.192 | +14.8 % | −0.1 % / +0.2 % |

Raw JSON under `target/horizontal/nt_readback_ab/` (not committed). Mechanism: the consumer's re-read is a sequential, prefetch-friendly sweep, while the RFO elimination pays off during the 31-thread parallel phase, and the asymmetry holds at every measured size ≥ 100K.

Default-tier decision (closed 2026-09): NT wins both consumer shapes, so the knob graduated from opt-in to an auto tier. `nt_store_enabled` resolves per whole-batch output size (a leaf only sees its chunk), ≥ 8 MiB → NT, threaded down as a leaf `bool`. Env became a tri-state: unset = auto, `"0"` = force off, `"1"` = force on, any other value panics (the `=off` trap above is now a loud failure). 8 MiB rather than 800 KB: the sub-threshold gains (+9–11 % at 100K) shrink toward the ±2 % noise floor where the balance gets machine-dependent, and known write-once shapes below the threshold can force it on. The remaining occupancy deficit (~1–4 pt) remains open.

## Attributing the 2M/4M fused-collect gap (2026-09-25)

Single-shape single-library runs of the horizontal binary itself (`--libs X --batches N`, taskset 1-31, A-B-A-B interleaved; iteration counts from the JSON `iters` field normalize whole-process `perf stat`). Machine: 16 physical / 32 SMT threads, 32 MB shared L3 + 32 × 1 MB L2.

Wall caliber the attribution targets: 2M youpipe 1.011 / rayon 0.928 ms (+9.0 %), 4M 2.003 / 1.784 ms (+12.3 %) in the morning session; a parallel same-binary session (alignment A/B) measured +13…+14 % @ 2M and +12 % @ 4M. Later the same day both sides drifted +25 % absolute with the gap compressed to +1…+3.5 %: the youpipe deficit is a fixed µs-scale per-iteration cost, so it looms large exactly when compute is fast (see the session-drift note in [counters.md](counters.md)). Counter ratios below are from that drifted session and are frequency-robust.

| per-iteration counter (2M / 4M) | youpipe / rayon | verdict |
| --- | --- | --- |
| cycles | 0.86 / 0.90 | youpipe burns fewer core-cycles |
| instructions | 0.96 / 0.96 | same work, youpipe slightly leaner |
| L1D-miss → L2 accesses (`l2_cache_req_stat.dc_access_in_l2`) | 1.00 / 0.97 | parity |
| store RFOs into L2 (`l2_request_g1.change_to_x`) | 0.94 / 0.78 | parity or better |
| demand DRAM fills (`ls_dmnd_fills_from_sys.dram_io_near+far`) | 0.53 / 0.73 | both trivial, see below |
| L1D TLB misses hitting L2 (`ls_l1_d_tlb_miss.all_l2_miss`) | 0.44 / 0.55 | parity or better |
| task-clock (busy CPUs of 31, 2M) | **26.7 vs 30.7** | **the gap** |
| context-switches / iteration (2M) | **99–102 vs 13–19** | park/wake churn |
| cpu-migrations / iteration (2M) | **7.6–8.0 vs 0.5** | parked workers land on cold cores |
| page-faults / iteration | 42.5 vs 43.2 | allocator parity |

Falsified premise, "≥ 32 MB leaves the cache-resident regime": demand DRAM fills measure 2–9 K lines per iteration against 256 K/512 K lines touched (≤ 1.7 %). The 64 MB combined L2+L3 hierarchy absorbs the whole 32–64 MB R+W working set in the bench's steady state on this machine, for both libraries. Chunk-boundary alignment indeed cannot move the gap (`YOUPIPE_ALIGN_CHUNKS` A/B ±0.3 %, falsified). The NT-store prediction was wrong and corrected the same day by the causal same-binary A/B (NT-store attribution above): +15–18 % wall at 1–4M, gap reversed. The NT "no improvement" observed during this attribution session was the invalid both-sides-on round (the knob treats any value other than "0" as ON, and the off side had passed the literal string "off"). Lesson: core-side counter parity (demand fills, L2 RFO counts) does not capture the store path's ownership/writeback latency that NT bypasses, and a same-binary causal knob outranks counter-based exclusion.

Occupancy deficit, a real but secondary component: `perf record` (999 Hz, dwarf) shows 94.3 % of youpipe's on-core samples in `par_index_rec_by_ref` (the leaf loop; the scheduler, injector, and latch never reach 0.2 %), versus rayon's 90.2 % in its `bridge` leaf plus ~4.4 % in `join_context`/`with_handle` spinning. youpipe executes its slightly fewer instructions with fewer total core-cycles yet finishes later: the missing time is spent halted. Idle workers exhaust the 32-spin + 32-yield round window and condvar-park; waking them costs futex latencies and lands them on migrated cores, ~4 of 31 CPUs' worth of average occupancy per iteration. That matches the +12–15 % wall-gap shape and initially read as the whole gap. The NT-store A/B showed the store path dominates (~13 pt), occupancy is worth a measured 1–4 pt on top (causal spin-knob check below), and the two partially overlap (NT shortens leaf duration, which also shortens the straggler tail that wake latency gates). Rayon avoids the loss by never parking mid-iteration, burning +11–16 % more cycles (spin) per iteration than youpipe.

Causal check (same-binary knob A/B): `YOUPIPE_SPIN_ROUNDS=2048 YOUPIPE_YIELD_ROUNDS=2048` (workers stay hot across the inter-iteration gap) improves youpipe at every shape, 1M −7.5 % → −11.9 %, 2M −2.3 % → −5.8 %, 4M +3.5 % → +2.3 % vs rayon. This confirms park/wake as a real cost and also that it is only worth 1.2–4.4 pt of the ~13 pt gap in that session. Widening the window indiscriminately burns idle CPU (the `ROUNDS_SPIN` history), so the open lever is structural: keep workers across a batch's back-to-back iterations, or shave the tail chunk, instead of stretching the idle window. Status (2026-09-26): the keep-warm side is resolved by opt-in core pinning (`ComputePool::new_pinned`, −5.1…−7.2 % on this exact caliber, no burn; see [scheduler.md](../scheduler.md) "Worker affinity"), and re-measuring the gap under the NT-store auto default found it collapsed: 1M +0.6 %, 2M +1.5 %, 4M −6.8 % (7 interleaved rounds, taskset 1-31).

Harness note: attribution initially used a standalone runner (`youpipe-bench`'s `horizontal-counters`, same workload); its wall times proved layout-lottery-bound (youpipe 0.94–1.31 ms across binaries, rayon 0.87–1.13 ms, consistent with the documented ±30 % code-layout noise), and a per-iteration console write in its timing loop widened the inter-iteration gap enough to triple youpipe's context switches (+30 % wall). Both pitfalls are documented in the runner's source, and all conclusive numbers above come from the layout-canonical horizontal binary.
