# Graph Engine Benchmarks

## Graph-engine DAG shapes (`graph_shapes`, 2026-10-22)

First A/B of the `graph()` engine against its closest sibling, the streaming engine, plus DAG-only shape anchors. Same-binary knob (`YOUPIPE_GRAPH_VARIANT=graph|stream`), 5 per-id isolated interleaved rounds on the 32-core Linux box (spread < 8 % across rounds; the same shapes on the Windows dev box drifted ±30 % and even showed a phantom 2.5× chain gap that vanished on Linux, so the drift, not the engine, was responsible).

| shape              | graph @1K | stream @1K | graph @100K | stream @100K |
| ------------------ | --------- | ---------- | ----------- | ----------- |
| chain_cpu (2 hops) | 233 µs    | 210 µs     | 18.45 ms    | 18.39 ms    |
| chain_cheap        | 235 µs    | 213 µs     | 18.39 ms    | 18.37 ms    |
| diamond_cpu        | 327 µs    | 192 µs     | 30.37 ms    | 15.32 ms    |

`chain_*` is the strict same-shape control (source → cpu → cpu → sink; the stream side's never-fired `with_cancel` only forces the streaming topology): steady-state throughput is a wash at 100K (1.00×), so the graph assembly layer (runtime plan, Kahn validation, type-erased spawn, budget split) costs nothing per item; @1K it shows as a fixed ~23 µs per-run assembly delta.

`diamond_cpu` compares the graph diamond (hash → broadcast → fast/slow → fair fan-in merge → sink) against the workload-equivalent serial stream chain (3× cpu + 1 bump per item, a shape comparison rather than an engine A/B): the diamond costs 2× the serial chain. The cheap anchors attribute it: `diamond_cheap_graph` @100K = 29.8 ms ≈ `diamond_cpu_graph` = 30.4 ms, so the closure work is fully hidden and the gap is pure data plane. Per source item the diamond pays 2 clones + 6 channel ops (vs 3 hops serial) and every node's claim cap is split across more worker populations. `fanin_cheap_graph` (two sources → one node, no broadcast) @100K = 56.0 ms for 200K items ≈ 280 ns/item vs the chain's 184 ns/item, so the fair fan-in round-robin poll alone accounts for most of the per-item premium.

Verdict: no regression for what `graph()` exists for (topologies `stream` cannot express); the DAG data-plane premium (~1.5–2× per item at the 50-mul CPU anchor) is dominated by per-item channel ops on the output edges. With real CPU/IO per-item work (µs-scale, the engine's target regime) it amortizes to noise. The two improvement levers this named were tried the same day, see the next section: batched broadcast sends landed (`YOUPIPE_GRAPH_TX_BATCH`, default on), fan-in claim sweeps falsified.

## Graph data-plane batching (`YOUPIPE_GRAPH_TX_BATCH`, 2026-10-22)

The batched-send lever, landed: source feeders stage `cap`-sized batches from the iterator and node/expand workers stage the mapped claim batch, then every open edge takes one `try_send_batch` per free run (clones for all edges but the last, which takes the owned items) with the per-item blocking fallback, exactly `push_items` / `fence_batch_send`'s rhythm generalized to k edges. Four-combo same-binary A/B (`bench_ab.sh -E`, sides = {tx, sweep} × on/off, 5 per-id isolated rounds, 32 cores; `chain/diamond_*_stream` rows run as drift controls):

| shape | off | tx on | Δ |
| ----- | --- | ----- | --- |
| diamond_cpu @100K | 29.03 ms | 10.06 ms | **−65.3 %** (25/25) |
| diamond_cheap @100K | 29.56 ms | 10.07 ms | **−65.9 %** (25/25) |
| diamond @1K | 328 µs | 148 µs | **−54.7 %** (25/25) |
| chain_cpu @100K | 17.94 ms | 8.47 ms | **−52.8 %** (25/25) |
| chain_cheap @1K | 227 µs | 131 µs | **−42.2 %** (25/25) |
| fanin_cheap @100K | 52.75 ms | 37.35 ms | **−29.2 %** (25/25) |
| fanin_cheap @1K | 560 µs | 425 µs | **−24.1 %** (25/25) |
| stream control rows (6) | — | — | ±2 % noise |

Every graph id stable 25/25; final-binary confirmation (default on vs `"0"`, 3 rounds) reproduced all of them 9/9 (−27.7…−66.5 %). Consequences:

- The graph engine beats the streaming engine on the identical chain shape (8.5 vs 18.4 ms @100K, 135 vs 214 µs @1K despite the ~23 µs assembly overhead), because the stream stage workers still `send` per item. The port of this lever to the streaming workers landed the same day as `YOUPIPE_STREAM_TX_BATCH` (see [streaming.md](streaming.md)).
- The diamond's residual premium over the serial chain collapsed to ~10.0 vs 15.3 ms, so the DAG parallelism now wins the shape comparison that first recorded the diamond at 2× the serial chain's cost.
- The sibling lever, fan-in claim sweeps (one `FanIn::round` claiming from every open input, the verdict's "claim cap" idea), measured as pure noise alone (|Δ| ≤ 5 %, mixed signs) and +6–8 % on top of tx on the diamond @100K: with sends batched, the per-claim round walking over k rings is off the critical path. Removed, see [dead-ends.md](../dead-ends.md).

## Any-of-N fan-in anchors (2026-10-23)

Correctness fix (todo #14's graph-side hang) landed against this bench as the no-regression gate (5 per-id isolated interleaved rounds, same caliber as the sections above). Root cause: the fan-in anchor parked on ONE input ring; in a DAG the anchored ring's producer chain can block behind a ring only the parked worker drains (broadcast edges flush sequentially and block per edge), so a diamond whose fan-in workers all anchor on the starved ring deadlocks. The sync anchor now parks registered on every open input at once (crossfire `Select`, reservation tokens), the async twin awaits all inputs (private `FirstReady` combinator); a single open input keeps the plain `recv` park.

The single-input fast path is load-bearing. First A/B (any-of-N on every anchor): `chain_cpu_graph` @100K **+20.4 %** (0/25), `chain_cheap_graph` @100K **+39.8 %** (0/25, fix-side spread 27.6 % — the Select registration is bimodal on park-prone single-ring nodes), chain/diamond @1K +13…+19 % — chains' nodes have one input each, so every park paid the multi-ring registration for nothing. With the fast path (`open == 1` → plain `recv`): all 17 ids noise, |Δ| ≤ 4.9 %, mixed signs, worst dominance 19/25; the multi-input shapes sit inside the band too (diamond_cpu @100K +2.6 %, 13/25; fanin_cheap @100K +2.5 %, 8/25). Regression tests: `anchored_worker_claims_from_other_input` (sync) / `anchored_async_task_claims_from_other_input` (async) — pre-fix the anchored worker never claims the other input's items.

Addendum (2026-10-25, todo #22): hoisting the anchor's per-episode `order` Vec into per-worker scratch (a `FanIn` field) measured as a stable regression on the same @1K family — chain_cpu +6.3 %, chain_cheap +6.1 %, diamond_cpu +4.5 %, diamond_cheap +3.7 %, all dominance 0/25 with side spreads <3 % and the `*_stream` controls flat (recompile A/B, 5 per-id interleaved rounds) — so the park-path allocation stays (falsification note at the allocation site in `graph/fanin.rs`); the crossfire `Select` itself cannot be hoisted regardless (its handlers borrow the receivers for the episode's lifetime).

## Broadcast staging drop-glue audit (todo #20, 2026-10-25)

`Broadcast::send_batch`'s last-edge staging (`staged.extend(batch.drain(..).map(MaybeUninit::new))`) re-introduced the exact `Map<Drain<T>>` drop-glue/landing-pad class commit 7d3cff3 eradicated from `SendItem::send_batch` — the single-CGU layout hazard documented in `.cargo/config.toml` that once regressed horizontal cpu_balanced +33-40 % through code the staging never executes. The last edge now routes through the shared panic-free `stage_batch` (made `pub(crate)`); the per-edge clone staging (`extend(iter().cloned())`) holds trivially-droppable iterators and stays. Recompile A/B gate (graph_shapes, 5 per-id isolated interleaved rounds, the `*_stream` rows as layout controls): all 16 ids noise — controls within ±1.2 %, every graph id within ±2.1 % except `fanin_cheap_graph` @100K +6.4 % sitting on a 34 % base-side spread (that id's known bimodal slow mode; dominance 5/25, fix side tight at 5.2 %). The change removes the risk class, not a measured cost.
