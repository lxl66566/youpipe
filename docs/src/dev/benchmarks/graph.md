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

## Async fan-in anchor allocation levers (todo #32, 2026-11-05)

`graph_shapes` gained `fanin_async_{cheap,cpu}_graph` (@1K/100K): two `source_async` feeders into one `node_async` with the default 128-task fan-out — the async twin of `fanin_cheap_graph`, and until now no bench family reached the async graph engine at all. The contending task population parks in `AsyncFanIn::anchor`'s any-of-N combinator on every empty-input gap. Baseline @100K ≈ 142–145 ms for 200K items ≈ 715 ns/item, ~3.6× the sync twin's ~190 ns/item — the async wake/registration machinery, not the fan-in poll (cheap vs cpu differ by ≈1 ns/item: fully channel/park-bound). Each `g.run()` lazily builds the async runtime backend, so the @1K rows (~2.1 ms) carry that fixed cost on top of the per-item path.


The allocation churn itself (todo #32's follow-up, same day) was real and landed: the any-of-N select drops its k−1 losing futures every park episode, and each dropped pending future *freed* its waker node — the next anchor allocated a fresh `Arc<WakerInner>` per loser. Instrumented counts on the `fanin_async` shape (2×4k items, 128 tasks; fresh-`new_async` counters, distinct from the 48-byte bucket the anchor's per-episode `futs` Vec also lands in): park episodes ranged 70–2200 per run (pacing is load-bimodal — under sibling bench load the m→out ring runs chronically full and the *send* side parks instead), and every pre-lever regime paid one fresh node per park episode on whichever side parked (~0.2/item worst case). Two fork levers closed it (crossfire commit "retire the cancelled episode's waker node into the slot"): the cancel-Drop of a cached future retires its abandoned node to `Reuse` and keeps it in the slot, and `poll_send`'s direct-copy (`Done`) branch keeps the node `cancel_reuse_waker` just retired. Post-lever, fresh registrations sit at the endpoint floor in both regimes (rx = 258 = 128 tasks × 2 input slots + bridge; tx = 128+2 first-park only) with zero `will_wake` misses.

A/B verdict (`bench_ab.sh -a base=15699b5 -b wt -B graph_shapes --per-id -r 5`, the falsified-lever caliber; wt = the two fork commits): the target family moves decisively and the layout controls stay put —

| id | med base | med wt | Δ% | dom | verdict |
|---|---|---|---|---|---|
| `fanin_async_cpu_graph/100000` | 146.0 ms | 126.2 ms | **−13.6** | 25/25 | improvement·stable |
| `fanin_async_cheap_graph/100000` | 142.9 ms | 126.2 ms | **−11.7** | 25/25 | improvement·dominant |
| `fanin_async_cpu_graph/1000` | 1.80 ms | 1.84 ms | +2.2 | 10/25 | noise (45.9 % base spread — the fixed runtime-build row) |
| `fanin_async_cheap_graph/1000` | 1.83 ms | 1.82 ms | −0.5 | 16/25 | noise |
| `fanin_cheap_graph/100000` (sync control) | 38.6 ms | 38.9 ms | +0.8 | 9/25 | noise |
| `fanin_cheap_graph/1000` (sync control) | 463.6 µs | 466.4 µs | +0.6 | 14/25 | noise |
| `chain_cpu_graph/100000` (sync control) | 8.98 ms | 8.72 ms | −2.9 | 14/25 | noise |

@100K the per-item cost drops ≈715 → ≈630 ns/item (200K items); the @1K rows carry the fixed lazy-runtime-build cost and do not resolve. The async-vs-sync gap narrows from ~3.6× to ~3.2× — the rest is the wake/registration protocol itself, not allocation. No-regression controls on the streaming engine: `io_async_pure`/`io_async_mixed` @200/@2000 all noise (|Δ| ≤ 0.6 %, spreads ≤ 1.2 %) and `sharded_term_async` `async1_{cheap,cpu}` @1K/@100K all noise (|Δ| ≤ 1.4 %, dominance ≤ 13/25) — expected, those paths never cancel parked cached futures (P-4 already floored them; the sharded anchor parks one shard at a time through the Single registry, whose node stays registered on cancel).


## Per-run assembly attribution, shared fan-in receivers (2026-11-05)

The fixed @1K assembly delta above was attributed phase-by-phase before any change: a temporary `Instant` probe of `run_graph` (env-gated, removed after the study) over the chain shape @1K — 4 nodes / 3 edges / 32 pool jobs — plus an interleaved graph-vs-stream driver for the end-to-end delta.

| run-graph phase | per run | note |
| --------------- | ------- | ---- |
| `validate_and_order` + collector walk | 0.45 µs | the pre-study suspects — name→index re-walks, build-then-validate passes — are noise at real node counts; Kahn and its HashSets are not the cost |
| `resolve_budget` + pool admission | 0.5 µs | |
| out-edge bundling + topo reorder | 0.3 µs | |
| edge-channel creation | 4.2 µs | 3 crossfire 256-slot rings ≈ 1.4 µs each; symmetric with the stream engine's stage channels |
| spawn walk | 19–22 µs | the body of the delta: type-erased endpoint downcasts 0.2 µs (erasure is free), per-worker closure builds 8.8–9.6 µs, `submit_batch` rounds 10–11 µs |

Two experiments came out of that table:

- **Falsified: staging the walk's jobs into one `inject_batch`.** The spawn side dropped to 9.2 µs but end-to-end @1K regressed (+6 µs cheap / +1 µs cpu): with every job held back until the walk ends, execution no longer overlaps late assembly — the source feeder's early Kahn-order submission is worth more than the saved submission rounds. The same run proved `submit_batch`'s cost is per-job, not per-round: one 32-job flush costs the same ~11 µs as the four batches (1+15+15+1) — HeapJob boxing, injector segment push and the wake scan scale with job count.
- **Landed: one receiver set per node, shared by its workers.** A crossfire receiver is a pure view of the channel's shared context — a clone carries no per-consumer state, only the drop refcount — so `FanIn` now holds `Arc<[Receiver]>` and a worker's `FanIn::new` is one `Arc` clone instead of a `Vec` clone (an allocation plus one `add_rx` bump per input edge per worker). The per-worker `closed` `Vec<bool>` became a `u64` bitmask (≤ 64 inputs; a `Wide` Vec fallback keeps larger fan-ins). Semantics unchanged: the last worker's drop closes the input exactly when the last per-worker clone would, and contention patterns are identical because the consumer state was always in the shared context. Probe caliber: build phase 9.6 → 2.8–3.2 µs, chain @1K driver totals −11 µs, graph−stream delta ~16 → ~3–9 µs.

Official verdict (`bench_ab.sh -a base=main -b wt`, `graph_shapes`, 5 per-id interleaved rounds; the `*_stream` rows are drift controls):

| id | base @med | wt @med | Δ | dominance |
| --- | --------- | ------- | --- | --------- |
| fanin_cheap_graph @1K | 464.9 µs | 416.8 µs | **−10.3 %** | 24/25 |
| chain_cpu_graph @1K | 145.1 µs | 133.3 µs | **−8.1 %** | 23/25 |
| diamond_cheap_graph @1K | 151.3 µs | 141.0 µs | **−6.8 %** | 25/25 |
| chain_cheap_graph @1K | 145.4 µs | 136.2 µs | **−6.3 %** | 25/25 |
| diamond_cpu_graph @1K | 150.1 µs | 141.3 µs | **−5.9 %** | 25/25 |
| @100K graph rows (5) | — | — | −0.5…+1.2 %, mixed signs | ≤15/25 (noise) |
| stream controls @1K (2) | — | — | −0.8 / +0.2 % | noise |
| stream control chain_cpu @100K | — | — | −2.0 % | layout-band drift (no stream path was touched) |

Residual (todo #33): the remaining @1K delta (~3–9 µs) is per-worker sender `Vec` clones in the spawn steps (delivery construction goes through `From<Vec<SyncSender>>`; sharing needs the `Delivery` strategies to serve from `&`-access — `Broadcast` mutates its edge Vec), per-job pool submission (~0.3 µs/job), and the per-edge ring allocation — all outside `graph/`.

## Shared sender sets: falsified (2026-11-05, todo #33)

The symmetric shape of the shared-receiver landing was implemented and gated: `Unicast`/`Broadcast` served from one `Arc<[SyncSender]>` per node (per worker an `Arc` clone instead of a `Vec` alloc plus a sender clone per edge; `Broadcast` closed edges via a per-worker `ClosedSet` bitmask — `swap_remove` compaction replaced by masking, semantics documented identical) and the graph spawn steps built the set once (`From<Arc<[SyncSender]>>`). A/B (`bench_ab.sh -a base=main -b wt`, 5 per-id interleaved rounds, same caliber as the receiver-side table):

| id | base @med | wt @med | Δ | dominance |
| --- | --------- | ------- | --- | --------- |
| chain_cheap_graph @1K | 128.6 µs | 126.8 µs | −1.4 % | 22/25 |
| chain_cpu_graph @1K | 127.7 µs | 126.0 µs | −1.3 % | 17/25 |
| diamond_cpu_graph @1K | 137.7 µs | 136.2 µs | −1.1 % | 15/25 |
| diamond_cheap_graph @1K | 137.2 µs | 136.9 µs | −0.2 % | 15/25 |
| fanin_cheap_graph @1K | 424.6 µs | 427.9 µs | +0.8 % | 16/25 |
| graph @100K rows (5) | — | — | −1.2…+4.6 %, mixed signs | ≤16/25 (noise; fanin +4.6 % on its known bimodal mode, 7/25) |
| `*_stream` controls (4) | — | — | −0.1…+4.8 % | noise (drift band) |
| mixed_load/youpipe_stream_cpu @1K | 9.19 µs | 9.17 µs | −0.2 % | 15/25 |
| mixed_load @100K (youpipe + 2 controls) | — | — | −8.9 % (19.6 % spread) / −1.2 % / −0.2 % | 15–21/25 (noise) |

Every id verdicts noise: the medians lean toward the change on 4 of 5 @1K graph rows by −1.1…−1.4 %, but that is under the 2 % floor with dominance at most 22/25 and side spreads of 4.6–5 %. The stream-side gate rows are structurally out of reach — `stream_pipeline/single_stage_*` and `mixed_load/youpipe_stream_cpu` are the type-level fused pass-through (no channels, no spawn walk, no senders; measured −0.6/+0.3 % @1K, pure noise), and `with_fence` @1K carries a sender-clone slice of ≈ 0.7 µs of a 164 µs run (see [streaming.md](streaming.md) "Stream-side per-run assembly attribution") — its A/B read −18 % median on 20–27 % spreads, the fence family's bimodality, verdict noise, and @100K +1.2 % noise. `mixed_load` completed the gate (youpipe @1K −0.2 % on 1.7 % spreads, dead flat, as the attribution predicts for the fused path; the rayon/tokio controls ±1.2 % rule out session drift), and the async fan-in ids — untouched `AsyncDelivery` twins — read +1.3/+2.4 % @1K, noise.

Falsified premise: the per-worker sender `Vec` builds are tcache-cheap — one node's 32 single-edge builds (alloc + `add_tx` + `Arc` bump ≈ 2 atomic RMWs each) cost ~1–2 µs, an order below the family's per-round spread, unlike the receiver-side builds they mirror (those carried a second per-worker allocation — the closed-input `Vec<bool>` — and measured −5.9…−10.3 %). The change was reverted; the `From<Vec<SyncSender>>` per-worker build stays, and the @1K assembly residue is now fully attributed: per-job pool submission (~0.3 µs/job) and the per-edge ring allocation (~1.4 µs) are the only members left with real mass.

## Shared async driver receiver sets: falsified (2026-11-06, todo #33)

The entry's last lever — the async twin's per-driver receiver builds (`AsyncFanIn::new(rxs.clone())`, one receiver `Vec` clone plus a `closed` `Vec<bool>` per driver, `min(io_concurrency, workers)` = 31 per node per run under the bench taskset since the M:N drivers) — was implemented as the exact mirror of the sync landing: one `Arc<[AsyncReceiver]>` per node, one `Arc` clone per driver, the per-driver `closed` `Vec` folded into the shared `ClosedSet` mask (waker slots and cursor/open episode state stay per-driver). A/B (`bench_ab.sh -a base=e7d25e0 -b side=10c7f3b -r 5 --per-id`, anchored per-id filters, the same caliber as the tables above):

| id | base @med | side @med | Δ | dominance |
| --- | --------- | --------- | --- | --------- |
| fanin_async_cheap_graph @1K | 1.367 ms | 1.252 ms | −8.4 % | 19/25 |
| fanin_async_cpu_graph @1K | 1.369 ms | 1.268 ms | −7.3 % | 18/25 |
| fanin_async_cheap_graph @100K | 57.60 ms | 56.98 ms | −1.1 % | 20/25 |
| fanin_async_cpu_graph @100K | 57.61 ms | 57.32 ms | −0.5 % | 19/25 |
| chain_cpu_graph @1K (sync control) | 131.8 µs | 133.5 µs | +1.2 % | 8/25 |
| sharded_term_async/async1_* @1K/@100K (4) | — | — | +0.4…+2.4 % | ≤11/25 |
| io_async youpipe rows (4) | — | — | −0.8…−0.1 % | ≤20/25 |
| mixed_load/youpipe_stream_cpu @1K/@100K | — | — | +0.5 / −5.6 % | ≤14/25 |

Every id verdicts noise. Falsified structurally, not only by the floor: the lever's whole mass is ~62 tcache `Vec` builds + ~62 receiver refcount clones ≈ 1–2 µs per node per run, but the async family has no assembly-sensitive row to resolve it on — each `g.run()` lazily builds the async runtime backend, so the @1K ids sit on that ~1 ms fixed cost (side spreads 15–33 %; the −7…−8 % median lean is the family's load-bimodal pacing — the side's fast rounds read 0.986/0.993 ms against a base minimum of 1.183 ms, and dominance caps at 19/25), and @100K amortizes per-run assembly to ~0.004 %. The sync twin's −5.9…−10.3 % lived on 130–460 µs rows carrying the same per-worker mass; no async row exists in that regime. The change was reverted; the per-driver `Vec` clone stays. This closes todo #33: the @1K assembly cost is fully attributed on both sides — per-job pool submission (~0.3 µs/job) and the per-edge ring allocation (~1.4 µs) on the sync side, plus the async-only lazy runtime build, are the only members with real mass.

## Async broadcast fan-out ids (2026-11-06)

`graph_shapes` gained `fanout_async_{cheap,cpu}_graph` (@1K/100K): one `source_async` feeder into a `node_async_broadcast` feeding two sinks — the fan-out mirror of the `fanin_async_*` family; until now every async graph id was a unicast 2-in-1-out shape. Purpose: isolate `AsyncBroadcast::deliver`'s unbatched per-item outlet cost — k−1 clones + k awaited `send_cached`s per item (k=2: one clone, two per-edge sends) — the target shape of a future batched-delivery lever. Items stay `u64` per family convention so the ids compare against the diamond/fanin anchors. Instrumentation-only addition, sanity-checked standalone (5 verdict-caliber passes, spreads 0.9–2.5 %, monotone @1K→@100K); no A/B recorded.

## Poll-based async intake: landed (2026-11-06, C1 stage 1)

`mux::drive` used to restart its select loop per completion: every `Event::Done` re-ran `try_claim` (an O(n) round walk), re-constructed the claim future — whose first poll ran the round AGAIN — and, on empty rings, entered `AsyncFanIn::anchor`: three fresh Vecs (`pairs`/`order`/`futs`) plus per-input waker re-registration per park episode, torn down again on the next `Done` (each teardown abandoning every node: one registry-mutex pass, then another on the re-arm). The landing replaces `AsyncIntake::claim() -> impl Future` with `poll_claim(&mut Context) -> Poll<Option<T>>`: the anchor folds into a cursor-ordered by-index poll of every open input (close marks handled in the same scan), `anchor`/`anchor_single`/`FirstReady` and their Vecs are deleted, and `drive`'s select polls the intake inline. The load-bearing constraint the conversion surfaced: a transient `recv_cached` future per poll LOSES the wake (its Drop abandons the node), so the fork gained `AsyncRx::poll_recv_cached` — `poll_item` over the caller's slot with no episode future; on `Pending` the slot-owned waker node stays registered across polls (any-input liveness, todo #14, now pinned by `fanin_poll_claim_marks_closes_and_claims_any_input`), and a steady-state re-poll re-validates in place with one state check per input, no registry mutex. Cancel-safety became structural (no episode exists); the per-impl cancel contract was deleted with the trait method.

A/B (`bench_ab.sh -a base=af704fe -b side=8e539c1 -r 5 --per-id`, the same caliber as the tables above):

| id | base @med | side @med | Δ | dominance |
| --- | --------- | --------- | --- | --------- |
| fanin_async_cheap_graph @100K | 57.16 ms | 55.25 ms | **−3.3 %** | 25/25 |
| fanin_async_cpu_graph @100K | 57.06 ms | 55.29 ms | **−3.1 %** | 25/25 |
| fanin_async_cpu_graph @1K | 1.403 ms | 1.372 ms | **−2.2 %** | 25/25 |
| fanin_async_cheap_graph @1K | 1.389 ms | 1.365 ms | −1.7 % | 25/25 (inside the side's 11.6 % spread — one load-bimodal fast round) |
| sharded_term_async/async1_* @1K/@100K (6) | — | — | −0.7…+0.3 % | ≤20/25 (noise) |
| sync_fuse/async_pair_cpu_* @1K/@100K (4) | — | — | −0.3…+0.9 % | ≤14/25 (noise) |
| io_async youpipe rows (4) | — | — | −0.1…+0.2 % | noise (dead flat) |
| mixed_load rows (youpipe_stream_cpu + rayon/tokio controls, 6) | — | — | −0.8…+3.8 % | ≤17/25 (noise; the @100K fused row sits in its documented 26–33 % bimodal band with its rayon control at +1.5 %) |

Both @100K primaries clear the 2 % floor at full 25/25 dominance — consistent with deleting, per completion, the future build+drop cycle (n abandon + n re-arm registry-mutex passes) and, per park episode, three mallocs; what remains per `Done` is the refill round + the poll's round walk plus one state check per input. The instant-future residue family (`sharded_term_async/async1_*`, the documented +2.8…+3.7 % knob rows) did NOT move: the single-ring `RxIntake` was already registration-cheap per completion, so the churn this removed was specific to the multi-input fan-in. Stage 2 (per-input tagged wakers over a ready bitmask, killing the O(n) walk per completion wake) remains open if a `hotpath-profile` attribution shows the walk is still a visible share.


## Batched async outlet: landed default-on (2026-11-06, C2)

`AsyncOutlet::deliver` was the last per-item delivery on either engine's hot path: every completion paid its own awaited `send_cached` per edge (k−1 clones + k sends for a broadcast), while completions arrive in clusters — one `FuturesUnordered` wake resolves several futures, and a cheap future resolves on its first inline poll, so the driver re-entered the whole select per item. `YOUPIPE_ASYNC_OUTLET_BATCH` (default on) arms the batched outlet: the driver stages inline-slot completions during refill, drains the rest of the set after the select's first `Done`, and hands the batch to `AsyncOutlet::deliver_batch` — per edge one `try_send_batch` free-run prefix (the fork gained `AsyncTx::try_send_batch`, consumer wakes merged via `fire_n`) plus the per-item awaited fallback (`AsyncSendItem::flush_staged`, the async twin of `SendItem::flush_staged`). The flush always precedes the select's park, so a pending in-flight set cannot delay ready items behind a batch boundary; no EOF/cancel/close exit returns with accepted outputs staged; and the emitted sequence is provably the per-item arm's (`drive_batched_delivery_matches_per_item_sequence` runs one gated schedule through both arms).

Same-binary knob A/B (`bench_ab.sh -a off=wt -b on=wt -E off=YOUPIPE_ASYNC_OUTLET_BATCH=0 -E on=YOUPIPE_ASYNC_OUTLET_BATCH=1 -r 5`, per-id for the graph rows), measured on top of the poll-based-intake baseline:

| id | off @med | on @med | Δ | dominance | verdict |
| --- | --------- | --------- | --- | --------- | ------- |
| fanout_async_cheap_graph @100K | 31.6 ms | 26.53 ms | **−16.0 %** | 25/25 | improvement·stable |
| fanout_async_cpu_graph @100K | 31.7 ms | 26.8 ms | **−15.5 %** | 25/25 | improvement·stable |
| fanin_async_cheap_graph @1K | 1.384 ms | 1.277 ms | **−7.7 %** | 25/25 | improvement·stable |
| fanin_async_cpu_graph @1K | 1.383 ms | 1.278 ms | **−7.6 %** | 25/25 | improvement·stable |
| fanout_async_cheap_graph @1K | 1.21 ms | 1.157 ms | **−4.4 %** | 25/25 | improvement·stable |
| fanin_async_cheap_graph @100K | 55.63 ms | 53.35 ms | **−4.1 %** | 25/25 | improvement |
| fanout_async_cpu_graph @1K | 1.206 ms | 1.158 ms | **−4.0 %** | 25/25 | improvement·stable |
| fanin_async_cpu_graph @100K | 55.47 ms | 53.35 ms | **−3.8 %** | 25/25 | improvement·stable |
| chain_{cheap,cpu}_graph @1K/@100K (4) | — | — | −0.7…+0.2 % | ≤19/25 | noise (sync-only control) |
| sharded_term_async/async1_{cheap,cpu,ordered_cpu} @100K | — | — | **−4.9…−5.7 %** | 25/25 | improvement (on top of the todo #34 baseline) |
| sharded_term_async/async1_* @1K (3) | — | — | −0.6…+0.1 % | ≤17/25 | noise |
| io_async youpipe rows (4) | — | — | −0.4…+0.1 % | ≤19/25 | noise (dead flat — staging does not delay first delivery) |
| mixed_load (youpipe_stream_cpu + rayon/tokio controls, 6) | — | — | −0.6…+1.0 % | ≤13/25 | noise |

Every primary row clears the 2 % floor at full dominance; the @1K broadcast rows carry the fixed runtime-build cost, so their −4 % is the per-item path. The unicast fan-in twin improving as much as it did says the cluster-drain + sender-run amortization (not the clone elision — unicast never clones) carries the win; broadcast keeps its k−1 clones per item by contract. The parky `io_async` shapes staying dead flat is the flush-before-park invariant paying off. Promoted default-on with the A/B above; `"0"` recovers the per-item deliveries.
