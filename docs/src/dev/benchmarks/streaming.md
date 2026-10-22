# Streaming Benchmarks

## Mixed load: `stream()` vs `tokio::spawn_blocking` (`mixed_load`)

| Size | youpipe stream | spawn_blocking | rayon (CPU-only) |
| ---- | -------------- | -------------- | ---------------- |
| 1K   | ~317 µs        | ~2.49 ms       | ~37 µs           |
| 100K | ~33.1 ms       | ~239 ms        | ~92 µs           |

`StreamPipe` beats `tokio::spawn_blocking` (the design target for mixed CPU/IO) at every size. The margin is widest at smaller sizes, where per-task spawn overhead dominates tokio's cost, and narrows at larger sizes, where channel bandwidth becomes the bottleneck. `rayon::par_iter` is fastest here because this benchmark is pure CPU and rayon's direct fork-join skips channel handoff entirely. All youpipe variants use `warm_clone` (cache-warmed input) for a fair comparison against rayon's warm borrow.

## Async IO: `.stage_async()` (`io_async`, yielding IO)

Simulated IO uses `tokio::time::sleep` (90% × 1 ms, 10% × 8 ms tail), a wait that yields the OS thread, the regime where M:N async concurrency beats the blocking-thread-per-core model. `io_concurrency = 512`, 32-core machine.

### Pure IO (`io_async_pure`)

| Size | youpipe_async | youpipe_blocking | youpipe_blocking_oversub | tokio_async_native | tokio_spawn_blocking |
| ---- | ------------- | ---------------- | ------------------------ | ------------------ | -------------------- |
| 200  | ~9.21 ms      | ~16.52 ms        | ~9.11 ms                 | ~9.16 ms           | ~8.39 ms             |
| 500  | ~9.38 ms      | ~34.72 ms        | ~8.52 ms                 | ~9.29 ms           | ~8.77 ms             |

`youpipe_async` matches `tokio_async_native` (the async ceiling) within ~3 % and stays well ahead of `youpipe_blocking`. `tokio_spawn_blocking` edges it via tokio's 512-thread blocking pool, aggressive OS-thread oversubscription that only pays off for pure-sleep (no CPU) work. The gap to the async ceiling shrank after two changes: eliminating the sync→async bridge thread for `stream(..).stage_async(..)` (the feeder pushes into a mixed-mode `SyncSender` + `AsyncReceiver` channel that the AsyncStage consumes directly, see the `StreamPipe` section of [core-types.md](../core-types.md)), and replacing the collector's per-item `recv().await` with a `try_recv` burst-drain that absorbs tokio's timer-tick completion bursts without per-item waker overhead.

`youpipe_blocking_oversub` uses `.with_compute_pool(ComputePool::new(512))` to match tokio's 512-thread blocking pool, narrowing the gap substantially. 512 is silently clamped to `MAX_COMPUTE_WORKERS = 511`, one thread short of tokio's pool, immaterial for the comparison. The remaining gap is streaming infrastructure overhead (channel handoff, injector scheduling), the price of the backpressure, ordering, and multi-stage composition that raw `spawn_blocking` does not provide. For blocking IO, `.stage_async()` remains the recommended tool.

### Mixed CPU (sync) + IO (`io_async_mixed`)

| Size | youpipe_mixed_async | youpipe_mixed_blocking | tokio_mixed_blocking |
| ---- | ------------------- | ---------------------- | -------------------- |
| 200  | ~9.31 ms            | ~28.6 ms               | ~8.92 ms             |
| 500  | ~9.55 ms            | ~63.2 ms               | ~10.08 ms            |

`youpipe_mixed_async` stays well ahead of the all-blocking two-stage baseline, and at size 500 edges out `tokio_mixed_blocking` by ~150 µs: the async path overlaps the CPU and IO stages on separate pools, while the all-blocking path splits one compute pool between two blocking stages. At size 200 the fixed per-run setup cost (feeder, channel allocation, runtime entry) is a larger fraction of the ~9 ms total, so tokio's simpler spawn-per-item model still leads there.

## Sharded terminal fan-in A/B (`sharded_term`, 2026-09-29)

Same-binary knob A/B for collector-side fan-in pacing: `YOUPIPE_SHARDED_TERM=0/1` selects one shared MPSC terminal ring vs one SPSC ring per terminal worker (`bench_ab.sh -E`, 5 per-id isolated interleaved rounds, 32 cores, median-of-round-medians). Every shape is forced onto the streaming topology by an inert `with_cancel` token; `cheap` = x+1 (channel-dominated), `cpu` = 50 mul-adds (~25 ns).

| shape @1K              | off    | on     | Δ%     | verdict |
| ---------------------- | ------ | ------ | ------ | ------- |
| single_unordered_cheap | 307 µs | 272 µs | −11.4  | stable  |
| single_unordered_cpu   | 305 µs | 274 µs | −10.3  | stable  |
| single_ordered_cpu     | 308 µs | 276 µs | −10.5  | stable  |
| multi2_cpu             | 273 µs | 260 µs | −4.9   | (dominant 25/25) |
| workers2_cpu           | 51.0 µs | 41.7 µs | −18.2 | stable  |
| expand_cheap           | 527 µs | 303 µs | −42.5  | stable  |

| shape @100K            | off    | on     | Δ%     | verdict |
| ---------------------- | ------ | ------ | ------ | ------- |
| single_unordered_cheap | 29.9 ms | 20.4 ms | −31.7 | dominant |
| single_unordered_cpu   | 29.9 ms | 21.2 ms | −29.1 | stable  |
| single_ordered_cpu     | 29.7 ms | 20.9 ms | −29.6 | stable  |
| multi2_cpu             | 24.3 ms | 22.4 ms | −8.0  | (dominant 25/25) |
| workers2_cpu           | 4.44 ms | 3.41 ms | −23.2 | dominant |
| expand_cheap           | 48.6 ms | 20.2 ms | −58.5 | stable  |

No id regressed; every cross-side round pair favoured the ON side (25/25 or 20/20). `workers2` improving rules out a per-shard fixed-cost threshold, so the knob ships without an auto gate. Mechanism: the send-side `compare_exchange` contention and the shared-ring cache lines vanish, and the collector pays a round-robin pass over k rings instead (k−1 failed `try_recv` per pass, amortised over the burst). The default stayed off pending a soak of fence/convoy interactions: during smoke the OFF side entered the known long-run convoy burn (26 cores spinning, collector parked inside crossfire `_read`'s stamp wait) while the ON side ran clean, suggestive but not a verdict (single observation, no multi-seed). The soak later promoted the knob, paired with batching, to the default; see "Default-policy soak + pair promotion" below.

Control runs: `mixed_load/youpipe_stream_cpu` (fused pass-through, no terminal channel) off vs on is noise (+0.6 %/−0.1 %, 3 rounds), confirming the knob does not leak outside the streaming terminal path. The `sync_fuse` shapes are unaffected to better: `fence_infra/1K` −0.8 % noise (the convoy canary), `cancel_pair_cpu_split/100K` −7.9 % at 9/9 dominant.

## Sharded-terminal default-policy soak (pre-convoy-fix baseline, 2026-09-29)

Multi-seed soak for the `YOUPIPE_SHARDED_TERM` default decision, run on the pre-convoy-fix main baseline (library at 022e018; this round is the "before" half and was repeated after the convoy fix landed, see the r9 section below). Harness: `convoy-probe` (a new `single` shape plus the existing sync2/fence/async2/async1/async0 cells), 100K `u64`, one shape × knob per fresh process, 12 processes × 12 runs per cell (`taskset -c 1-31`, `--threads` ctx-switch deltas, peak RSS via GNU time). Seeds are process order plus an alternating in-process rhythm (`--delay-ms` 0/300); the bump workloads are data-independent, so run-order randomness stands in for data seeds. "Collapsed" uses the forensics-calibrated >100 ms threshold (the fence family's fast/collapsed gap is 25–36 ms vs 190–275 ms).

| shape (100K) | off med | on med | collapsed off | collapsed on |
| ------------ | ------- | ------ | ------------- | ------------ |
| single        | 26.9 ms | 11.3 ms | 7/144 = 4.9 %  | 1/144 = 0.7 % |
| sync2         | 8.9 ms  | 27.5 ms | 0/144          | 0/144         |
| fence         | 62.2 ms | 15.3 ms | 65/144 = 45.1 % | 26/144 = 18.1 % |
| async2        | 251.5 ms | 237.9 ms | 119/144 = 82.6 % | 95/144 = 66.0 % |
| async1        | 28.2 ms | 29.3 ms | 1/144 | 2/144 |
| async0        | 19.6 ms | 25.5 ms | 0/144 | 0/144 |

Readings:

- Calibration first: in this binary the knob is a no-op on the async cells (async sharding did not exist yet, the async terminal keeps the single MPSC ring), so their off/on deltas (median ±15 %, early-vs-late drift differing up to ~2×) measure the bistable noise band. Do not read them as knob effects; single/sync2/fence deltas outside that band are the signal.
- The OFF-side pathology reproduces at scale: the known contention mode (collector fan-in: ~85 K involuntary switches per 100 K-item run, workers churning on the shared ring) dominates `single` OFF (76 % of runs at 27–40 ms vs a rare 4–5 ms lucky mode); the fence convoy collapses 45 % of runs; the two-sync-prefix async chain 83 %.
- ON is not a convoy fix: fence still collapses 18 % of ON runs and async2 is unchanged, consistent with the serial-supplier→crowd root cause living on the mid channels rather than the terminal. ON halves the fence collapse rate and improves the contention-mode medians (single −58 %, fence −75 % median).
- The ON-side regression face: `sync2` OFF reaches an 8.9 ms pipelined mode in back-to-back cadence (119/144 runs) that ON never reaches, with ON's bulk staying at the isolated-round level (25–40 ms, ~1.8 K voluntary parks/run vs OFF's ~35). The isolated-round A/B (`multi2` −8 % ON) never sees OFF's fast mode, because criterion's per-round cadence (measurement + reporting gaps) reseeds it. Mechanism unattributed; recorded as the standing counter-evidence against flipping the default.
- ON-side stability: no monotonic wall drift attributable to the knob (the async0 drift asymmetry is inside the noise band); peak RSS ON ≤ OFF by 1–7 % (32 shards do not register against the ~5.5 MB baseline).

Verdict at the time: default stays off. The soak supported opt-in for contention-shaped terminals (`single`/fence families) and showed a real regression face on back-to-back sync2. The re-made decision on the post-convoy-fix tree promoted the pair to the default and dissolved this table's counter-evidence; see "Default-policy soak + pair promotion" below.

## Async sharded-terminal A/B (`sharded_term_async`, 2026-09-29)

The async terminal's fan-in: `io_concurrency` (128 by default) runtime tasks all `send().await` into one shared async MPSC ring. `YOUPIPE_SHARDED_TERM=0/1` swaps it for `min(io_concurrency, async_workers)` = 32 shard rings (4 tasks share each ring), same binary (`bench_ab.sh -E`), 32 cores, 10 interleaved rounds (5 combined + 5 per-id isolated, both halves agree round-for-round). Shapes are the convoy-immune family: the `async1_*` cells (one sync prefix into `stage_async`) are the direct shape, and `async0_*` (async-only chain) was fielded as a knob-insensitive leakage control but in fact measures a second, send-dense terminal shape, since its only stage IS the terminal.

| shape @100K     | off     | on      | Δ%    | verdict |
| --------------- | ------- | ------- | ----- | ------- |
| async1_cheap    | 26.8 ms | 38.4 ms | +43.3 | REGRESSION stable (0/100) |
| async1_cpu      | 27.0 ms | 40.7 ms | +50.9 | REGRESSION stable (0/100) |
| async1_ordered_cpu | 27.2 ms | 38.6 ms | +41.6 | REGRESSION stable (0/100) |
| async0_cheap    | 72.7 ms | 30.9 ms | −57.5 | improvement stable (100/100) |

| shape @1K       | off     | on      | Δ%    | verdict |
| --------------- | ------- | ------- | ----- | ------- |
| async1_cheap    | 1.31 ms | 1.35 ms | +3.2  | REGRESSION dominant (0/100) |
| async1_cpu      | 1.31 ms | 1.36 ms | +3.8  | REGRESSION (1/100) |
| async1_ordered_cpu | 1.31 ms | 1.34 ms | +2.5  | REGRESSION dominant (0/100) |
| async0_cheap    | 1.26 ms | 1.16 ms | −8.4  | improvement stable (100/100) |

Verdict: the direct shapes regress hard, every async1 round on either side of the table is slower ON (0/100 cross-round wins @100K). Attribution (analysis, not a measured decomposition): sharding pays only where sender-cursor contention dominates the terminal. The async-only chain's feeder lets 128 tasks send densely into one ring (72.7 ms OFF, the slowest async reading of either side, contention-bound), so sharding removes contention and wins −57.5 %. The sync prefix in the async1 shapes throttles send density enough that the shared ring is cheap, and the one-shard anchor becomes the dominant cost: the async collector registers its recv waker with ONE shard per park (crossfire's per-channel waker slot; the any-of-N alternative would allocate per registration, see [streaming.md](../streaming.md) "Async terminal flavour"), so while the other 31 rings are full their producer tasks park on space that only the collector's next burst pass can release, and the collector is itself parked waiting for the anchored shard's next item. Global pacing collapses onto the anchor ring's rhythm. With one shared ring the collector's waker is always on the only ring, every send wakes it directly, no pacing loss. The sync terminal avoids this asymmetry because its producers are OS threads (a full ring parks the sender thread itself, not the whole stage's progress) and the sync anchor's blocking `recv` has no waker registration cost.

Consequences: the profitable async0 shape is a feeder-saturated corner, not the target workload, and a mixed pipeline (sync terminal + async terminal) turning the knob on for the sync win pays +42…+51 % on the async side. The async flavour therefore moved to its own `YOUPIPE_SHARDED_TERM_ASYNC` knob (2026-10-20), removing that coupling: `YOUPIPE_SHARDED_TERM` now governs sync/expand terminals only, and a sync-default flip no longer needs an async-side decoupling step.

## Batched ring ops four-combo A/B (`sharded_term` × `batch_recv`, 2026-09-30)

Four sides over the same binary (`bench_ab.sh -a/-b/-c/-d -E`, 5 per-id isolated interleaved rounds, 32 cores): base = both knobs off, batch = `YOUPIPE_BATCH_RECV=1` alone, shard = `YOUPIPE_SHARDED_TERM=1` alone, both = both on. Cap is the default 64.

| shape @100K      | base    | batch alone | shard alone | both   | batch on shard | both/base |
| ---------------- | ------- | ----------- | ----------- | ------ | -------------- | --------- |
| single_unordered_cpu | 29.4 ms | 43.4 ms (bimodal 19.5–51) | 19.3 ms | 12.7 ms | −34.4 % (25/25 dom) | 2.32× |
| single_unordered_cheap | 29.7 ms | 38.0 ms (bimodal) | 11.8 ms (bimodal 9.1–21) | 12.5 ms | +6.0 % (10/25) | 2.38× |
| single_ordered_cpu | 29.6 ms | 28.4 ms (noise) | 9.5 ms (bimodal 8.5–20) | 10.0 ms | +4.8 % (mixed) | 2.95× |
| expand_cheap     | 49.2 ms | 55.4 ms (noise) | 23.6 ms | 23.2 ms | −1.7 % (noise) | 2.11× |
| multi2_cpu       | 25.6 ms | 16.8 ms (stable) | 21.9 ms | 18.7 ms | −14.7 % (25/25 dom) | 1.37× |
| workers2_cpu     | 4.70 ms | 2.56 ms (stable) | 3.67 ms | 1.14 ms | −68.9 % (25/25 dom) | 4.12× |

Readings:

- Cumulative criterion (both vs base ≥1.6× at 100K): met on 5/6 shapes (2.1–4.1×); `multi2` lands at 1.37×.
- Batch's marginal on sharded splits: dominant wins on the fan-in shapes (`workers2` −69 %, `single_unordered_cpu` −34 %, `multi2` −15 %) and noise-to-slightly-negative on `expand`/`single_ordered`. The +5 % medians on the cheap/ordered singles trade peak speed for consistency: shard-alone is bimodal there (2/5 rounds at ~20 ms, the convoy mode) while both is tight (12.0–12.7 ms), so batch removes the slow mode rather than the median.
- Batch alone was not shippable as a default: on the shared terminal ring the single-stage 100K shapes go bimodal (19.5–51 ms rounds, spread 35–73 %, the same convoy mode amplified by the bigger claim steps), and `workers2_cpu/1K` regresses +38 % (0/25). At 1K batch alone wins −33 % on the single shapes (stable); `expand_cheap/1K` with both is the one clear 1K negative (+17.5 % vs shard-alone).
- Hotpath probe (same binary, env switch, `stream-engine` 1M × 30; this session's paired off-side reads 882 ns, do not compare against other sessions): collector `MpscReceiver::try_recv` 30 M calls p50 882 ns → `try_recv_batch` 8.79 M calls p50 100 ns (≈352 ns/item amortised over ~3.4-item runs, one probe guard per batch). The dominant win is on the other side of the same ring: worker→terminal `MpscSender::send` p50 1.74 µs → 90 ns, total thread-time 772 s → 324 s, because batched draining keeps the ring off the full/stamp-wait cliff. Feeder side: `try_send_batch` 469 K calls p50 380 ns (≈64/run) plus a per-item tail at 20 ns. Engine pace 975.7 → 751.3 ms/iter (−23 %).
- Controls: `mixed_load/youpipe_stream_cpu/100K` (fused, no channels) −0.2 % noise, no leak off the streaming path. The `sync_fuse` cancel_pair shapes are streaming-shaped (the cancel token declines fusion) and improve −6.8 %/−10.7 % (9/9 dominant), real gains at the integration points rather than leaks. `fence_infra/100K` is the documented bistable convoy canary: off caught one fast-mode round (10.8 ms) in 3, on settled slow-mode throughout (122–131 ms), inconclusive at n=3 given the pathology.

Default policy at landing: `YOUPIPE_BATCH_RECV` stayed opt-in, with pairing advice in the knob docs: enable together with `YOUPIPE_SHARDED_TERM` for fan-in shapes (dominant 25/25 wins) and where the sharded terminal shows round-to-round bimodality; keep it off for 1K expand-shaped short runs and never enable it alone on a shared terminal ring. Superseded 2026-10-20: the pair is now the default, see "Default-policy soak + pair promotion" below.

## Default-policy soak + pair promotion (2026-10-20, r9)

The post-convoy-fix re-run of the sharded-terminal default soak (the pre-fix baseline is the section above, the convoy fix branches are merged now), on the promotion tree with the async flavour already decoupled into `YOUPIPE_SHARDED_TERM_ASYNC`. Harness: `convoy-probe`, one shape × knob set per fresh process, 12 processes × 12 runs per cell (`taskset -c 1-31`, `--threads`, `--delay-ms` 0/300 alternating per process pair, side order flipped per pair). Sides: off = `YOUPIPE_SHARDED_TERM=0 YOUPIPE_BATCH_RECV=0`, sharded = terminal knob alone, pair = both on. The async cells' "sharded" side additionally verified the decoupling leak check, that the sync knob does not touch the async terminal.

First finding, sharded alone is not promotable (pre-share-cap tree, 144 runs/cell): on `single` the median wins (20.9 vs 31.1 ms) but the convoy slow mode returns in ~30 % of runs regardless of cadence (delay-0: 25/72, delay-300: 19/72 runs >40 ms; p90 80–100 ms, mean 37.3 vs off 35.4), while OFF holds a stable 30 ms contention floor with a tight p90 (36.5). The pre-fix soak's counter-evidence, sync2's 8.9 ms back-to-back fast mode that sharded never reached, dissolved on the merged tree: OFF now floors at 21.5 ms and both sides sit at ~28 ms.

Second finding, the pair dominates every cell (share-cap tree, see below; means in ms, 144 runs/cell, async2 48):

| shape @100K | off | batch alone | pair | pair vs off |
| ----------- | --- | ----------- | ---- | ----------- |
| single      | 35.7 (p90 36.5, max 138) | 27.9 | **13.4 (p90 15.5, max 18.2)** | −62 % |
| sync2       | 29.5 | 22.3 | 24.0 | −19 % |
| fence       | 157.6 | 99.1 | 98.4 | −37 % |
| async2      | 230.1 | 139.5 | 109.0 (p50 28.9 vs off 242.4) | −53 % |

At 1K (144 runs/cell): single 571→403 µs mean (p50 421→292), sync2 547→461 µs mean (p50 426→334; p90 653→786, the one lean-anywhere tail, mean and p50 clearly better). The pair's single-cell tail is tighter than the old default's, max 18.2 ms vs off's 138 ms: batched claiming removes the sharded-alone bimodality under back-to-back cadence too, not just under criterion rounds. Batch alone measured tight and better-than-off everywhere in this session (the four-combo's pre-merge bimodality did not reproduce post-merge); sync2 even prefers it over the pair, but `single`/`expand` want the sharded ring for the send side.

Parallelism fix riding along, the share cap: promoting the pair exposed what the cheap-item benches never could, that `claim_poll`'s whole-run batch claim lets ONE worker monopolize a small burst its idle peers should split (`test_stage_options_workers_pins_parallelism`: a 40-item burst, 2-worker pin, 5 ms items → max concurrency 1). The stage/expand worker loops now cap each claim at `max(1, batch_cap / workers)`, a per-worker share of the ring; sole-consumer sites (collector, feeder, fence forwarder) keep the full cap, where a big claim starves nobody. The soak above was re-run on the share-cap tree (the numbers in the table); the pair's wins survive it, since trickle-fed rings claim 1–3 items either way and the share only binds on bursts.

Criterion confirmation on the final binary (`bench_ab.sh -E`, old defaults `=0/=0` vs new default unset, 3 per-id isolated rounds; the flip changes only the readers' defaults, so this re-measures the four-combo on the promotion tree): every `sharded_term` id won 9/9, @100K single_unordered 29.9→11.9 ms, single_ordered 53.1→12.3 ms (old side bimodal, 75 % spread), workers2 4.46→1.14 ms, expand 47.3→21.7 ms, multi2 23.2→18.3 ms; @1K −21…−47 %; `sync_fuse/fence_infra/100000` 152.6→111.1 ms (old side in the documented collapsed mode); the fused control `mixed_load/youpipe_stream_cpu/1000` −0.5 % noise (the 100K control read +14.6 % with 19 % spread and 2/9 dominance; the same control measured ±0.5 % in the four-combo, and the knobs cannot touch the fused path, which has no channels, so treat it as draw wobble).

Decision: the pair is the default. `YOUPIPE_SHARDED_TERM` and `YOUPIPE_BATCH_RECV` unset = on (cap 64); `"0"` on either recovers the shared-ring / per-item configuration. `YOUPIPE_SHARDED_TERM_ASYNC` stays off (falsified on sync-fed async terminals), and `YOUPIPE_SPIN_ANCHOR` / `YOUPIPE_FWD_BATCH` stay opt-in (their cost faces are load-shape-local: the spin's 2–4× lucky-fast-seed cost and idle burn). Note for deterministic-Ir baselines: the flip changes the streaming paths' instruction counts by design, so re-save gungraun baselines after this change.

## Convoy fix A/B (`YOUPIPE_SPIN_ANCHOR` / `YOUPIPE_FWD_BATCH`, 2026-09-30)

Fix verdict for the convoy pathology (mechanism in [streaming.md](../streaming.md) "Fix (a)+(b) landed"; all cells `convoy-probe` @100K, same binary, fresh process per cell, 3 interleaved off/on rounds; the off side collapses in ~1/3 of processes, so ranges rather than medians carry the verdict):

| cell (fence family) | knob off | `SPIN_ANCHOR=30` |
| --- | --- | --- |
| fence auto 14/14 | 120–254 ms bimodal | 27–57 ms, no ≥180 run |
| fence 9/9 | 12–107 ms drifting | 23–28 ms |
| fence 1/15 (crowd at output) | 90–127 ms (lucky 5.5) | 24–33 ms |
| fence 20/1 (pipelined ref) | 24–27 ms | 24–28 ms (untouched) |
| soak 100K×10, 3 procs | mode wanders, 646 ms outlier | 26–107 ms, no ≥180 run |
| yp-pool parks/run | 90–197 K (1–2/item) | 0.3–2.6 K |

`async2/15-1` stays bimodal both ways (off 25–188, spin 29–309 ms): its slow mode parks on the collector (2.3/item) and the send side into the mixed async channel (1.2/item), not on the recv anchor, so it is outside the knob's reach (attribution in [streaming.md](../streaming.md)). Poll-ceiling sweep at spin=30: caps 1/8/16 regress or only partially heal the fence family (219/193/181 ms med) vs 64/128 (33–102 ms); tight polling bounces the ring stamps faster than items arrive. `FWD_BATCH` standalone gives no rescue (fence-auto med 205/165 ms) and combined with the spin is within noise. Both knobs stay default-off; per-shape guidance in [tuning](../../advanced/tuning.md).

Evidence-bench confirmation (`bench_ab.sh -E`, 5 per-id isolated interleaved rounds, same binary, median):

| id | off | `SPIN_ANCHOR=30` | Δ |
| --- | --- | --- | --- |
| `sync_fuse/fence_infra/100000` (canary) | 200.5 ms | 41.9 ms | −79 % dominant |
| `sync_fuse/fence_pair_cpu_split/100000` | 33.6 ms | 24.4 ms | −27 % stable |
| `sync_fuse/async_pair_cpu_split/100000` | 230.1 ms | 208.8 ms | −9 % (pathology remains) |
| `sync_fuse/cancel_pair_cpu_split/100000` | 24.5 ms | 24.8 ms | +1 % noise |
| `mixed_load/youpipe_stream_cpu/100000` (fused) | 39.7 µs | 39.3 µs | −1 % noise |
| `pipeline_fusion/fused_3_stages/100000` (fused) | 23.3 µs | 23.2 µs | −0 % noise |
| `horizontal/cpu_balanced/1M` (fused, direct) | 0.83 ms | 0.82 ms | noise |
| `sharded_term/single_unordered_cpu/100000` (sharded on) | 21.7 ms | 21.3 ms | −2 % noise |
| `sharded_term/multi2_cpu/100000` (sharded on) | 23.0 ms | 22.9 ms | −0 % noise |

The criterion harness pins the canary's off side in the collapsed mode (200 ms stable, unlike the probe's process bistability), which is why the evidence-bench delta is even cleaner than the probe's. The last two rows are the `YOUPIPE_SHARDED_TERM` orthogonality check: with sharded on, the spin knob neither helps nor hurts, the two knobs act on disjoint interfaces.

## Post-merge re-tests (convoy fix × batch recv, 2026-09-30)

After both branches landed, the worker recv loops were unified on one anchor head (`anchor_claim`: non-blocking claim → activity-gated spin window → blocking recv), so each branch's headline verdicts were re-confirmed on the merged binary (same-binary knob A/B, `bench_ab.sh -E`, per-id isolated, 3 interleaved rounds, harness = `convoy-probe`). Readings vs the branch tables above:

| cell | knob off | knob on | Δ | verdict |
| --- | --- | --- | --- | --- |
| `sync_fuse/fence_infra/100000` | 188.3 ms (169–222, collapsed) | 45.5 ms (27.9–60.3) | −75.9 % | 9/9 dominant — branch: 200.5→41.9 ms |
| `sync_fuse/fence_infra/1000` | 1.561 ms | 870 µs | −44.2 % | stable |
| `sharded_term/workers2_cpu/100000` | 4.594 ms (base) | 1.101 ms (SHARDED+BATCH) | 4.17× | 9/9 — branch: 4.12× |
| `sharded_term/single_unordered_cpu/100000` | 29.07 ms (base) | 11.02 ms (SHARDED+BATCH) | 2.64× | stable — branch: 2.32× |
| `sharded_term/workers2_cpu/1000` | 54.2 µs | 27.4 µs | 1.98× | stable |
| `sharded_term/single_unordered_cpu/1000` | 288 µs | 244 µs | 1.18× | stable |

Both conclusions carry through the composition: the convoy fix survives the merged anchor (the canary's off side still pins the collapsed ~190 ms mode, the on side never exceeds 61 ms), and the batch+sharded pair keeps its ≥1.6× cumulative criterion on the fan-in cells while the base sides match the branch baselines, so there is no default-path regression. The re-run also surfaced a pre-existing `sync_fuse` harness bug: `fence_infra` was registered inside the side loop, so an unset `YOUPIPE_SYNC_FUSE_VARIANT` tripped criterion 0.8's duplicate-ID panic before the 100 K cells ever registered (fixed alongside the merge).

## Channel throughput

Two-thread ping-pong (1 producer, 1 consumer, `u64`), all bounded rows at capacity 256. Caliber note (2026-10 fix): the old table compared bounded crossfire against unbounded `std::sync::mpsc::channel`; the unbounded channel does no capacity accounting and never blocks the producer, so that column was not a like-for-like row. Rows are now named by caliber (`*_bounded`/`*_unbounded`, `*_mpmc`/`*_mpsc`). Numbers are medians of 5 interleaved rounds, `taskset 1-31`; the 1P1C shape is placement-sensitive (unpinned runs collapse up to −58 %), so pinning is mandatory for this group.

| Size | youpipe_mpmc | youpipe_mpsc | crossbeam_bounded | std_mpsc_bounded | std_mpsc_unbounded |
| ---- | ------------ | ------------ | ----------------- | ---------------- | ------------------ |
| 10K  | 58 Melem/s   | 35 Melem/s   | 26 Melem/s        | 41 Melem/s        | 65 Melem/s         |
| 100K | 85 Melem/s   | 44 Melem/s   | 26 Melem/s        | 58 Melem/s        | 92 Melem/s         |

Readings:

- Same-caliber bounded-MPSC: `std sync_channel` beats crossfire's mpsc flavour by ~17–31 % in this uncontended 1P1C shape. The MPSC-flavoured collector channel was adopted from in-pipeline profiling under N-producer contention, where recv-side CAS dominates (see `handoff/channel.rs`); this microbench has no recv-side contention, so it measures the pure cache-line-transfer/wake path instead. The in-pipeline follow-up was run and falsified: burst-boundary park round-trips regress every real-collector shape, see [dead-ends.md](../dead-ends.md).
- The inter-stage MPMC channel (crossfire, 85 Melem/s at 100K) beats every bounded alternative here, so the "nothing left to squeeze" conclusion for the middle channels stands (see [scheduler.md](../scheduler.md)).
- `std_mpsc_unbounded` keeps its historical role as the no-backpressure ceiling reference only.

## Stream worker tx batching (`YOUPIPE_STREAM_TX_BATCH`, 2026-10-22)

The same lever as graph data-plane batching, ported to the streaming engine's stage/expand workers: the mapped claim batch is staged and flushed with one `try_send_batch` per free run plus a per-item blocking fallback (`SendItem::send_batch`), instead of one blocking `send` per item. The MPSC ring gained a native fork extension for it (one `sender` CAS per free run; without it the terminal/shard rings rode the per-item fallback and only the mid-MPMC hops benefited, and the workers2 rows moved from noise to −16.5 % the moment it landed). Chains containing an async stage keep the per-item sends on every hop (the gate measured below).

Same-binary knob A/B (`bench_ab.sh -E`, isolated per-id, 5 interleaved rounds, 32 cores; fused-path `merged` rows and `mixed_load` stream ids are controls):

| id | Δ | verdict |
| --- | --- | --- |
| sync_fuse/cancel_quad_cpu_split @100K / @1K | **−64.7 % / −49.9 %** | 9/9, 9/9 stable |
| sharded_term/multi2_cpu @100K / @1K | **−49.1 % / −37.4 %** | 25/25 stable |
| sync_fuse/cancel_pair_cpu_split @100K / @1K | **−50.2 % / −38.5 %** | 9/9 stable |
| sync_fuse/cancel_pair_cheap_split @100K / @1K | −50.2 % / −38.6 % | 25/25 stable |
| sync_fuse/fence_pair_cpu_split @1K | −24.9 % | 25/25 stable |
| sync_fuse/fence_pair_cheap_split @1K / @100K | −24.6 % / −20.4 % | 25/25 dominant |
| sharded_term/workers2_cpu @1K / @100K | −16.5 % / −9.7 % | 25/25 stable |
| sync_fuse/async_pair_cpu_split @1K (pre-gate) | −14.4 % | 25/25 — see below |
| sync_fuse/fence_infra @100K / @1K | −6.7 % / −5.8 % | 25/25 |
| sharded_term/single_* (6 rows) | −1.3…+0.3 % | noise, leaning win |
| sharded_term/expand_cheap @1K/@100K | +0.3/+1.9 % | noise (19–24 % spreads) |
| expand_heavy youpipe rows | −0.4…−3.9 % | noise-to-win; rayon control ±4 % |
| async rows (post-gate), fused controls, mixed_load | ±1.5 % | noise |

The async-tail gate: on `stage().stage().stage_async()` the batched delivery regressed **+4.9 % @100K (0/25)** with the mixed hop batched and still **+3.3 % @1K (0/25)** with only the mid-MPMC hop batched. Both hops of an async-tailed chain lose under send-side runs: the async terminal's task pacing does not benefit the way sync consumer loops do (convoy-probe async2: median +3–7 %, no slow mode on either side). The gate is `!ctx.has_async_stage`, the same predicate as the capacity floor, so async chains keep the per-item rhythm byte-for-byte and read ±1.5 % noise. The pre-gate 1K win (−14.4 %) was the mixed hop's, outweighed by its @100K cost; see [dead-ends.md](../dead-ends.md). Single-stage rows sit at noise: their only channel is the terminal shard ring, whose send-side CAS was already uncontended (sole producer); the win concentrates where sends contend (mid MPMC rings shared by k workers) or the claim share is small (workers2).
