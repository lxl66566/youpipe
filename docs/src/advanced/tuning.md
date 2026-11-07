# Tuning knobs by workload

Every knob has a sensible default; tune only when a measured problem points at one.

## Knob overview

| Knob | Effect | Applies to | Reach for when |
| --- | --- | --- | --- |
| `with_workload(Workload)` | fork/join split granularity | fused + `stream()`'s fused cores | item costs are skewed |
| `with_filter_collect(FilterCollectMode)` | implementation of filter collects (by-ref default `Auto`: merge tree < 100K items, write-then-compact ≥ 100K; owned default: merge tree, `Wtc` opt-in — wins ~1K batches −14 %, regresses ≥ 100K +6…+9 %) | fused `.collect()` / `.try_collect()` with a filter stage | the Auto gate mismatches your shape, or a small owned batch wants wtc (all identical output; see [dev/benchmarks](../dev/benchmarks/sync-fused.md) "Filter-chain collect") |
| `with_buffer_size(n)` | channel capacity between stages | streaming | bursty producers, memory bounds |
| `with_io_concurrency(n)` | in-flight async items per async stage | streaming + graph | IO waits are cheap and plentiful |
| `.ordered()` | reorder pass restoring input order | streaming | output must match input order |
| `.fence(mode)` / `.fence_with(FenceOptions, mode)` | isolation at one stage boundary (`buffer` pin) | streaming | downstream must not see partial upstream |
| `SyncStageOptions` / `AsyncStageOptions` | per-stage workers (sync) / io_concurrency (async) / buffer (both) | streaming | stages have unequal costs |
| `with_sharded_terminal(bool)` | terminal fan-in channel shape — per-pipeline tier of `YOUPIPE_SHARDED_TERM` (default `true`; env shadows) | streaming | memory-shape only: many small pipelines in one process sharing the terminal rings |
| `with_shard_depth(ShardDepth)` | sync sharded terminal's per-shard capacity floor — per-pipeline tier of `YOUPIPE_SHARD_DEPTH` (default `Off`; env shadows) | streaming | shape-specific wins only (the scan falsified every auto tier): an expand terminal wants `Slots(16)` (−31…−43 %), a two-stage chain's terminal @100K wants `Slots(64)` (−5.6…−6.5 %); the wrong tier regresses up to +58 % |
| `with_chain_floor(ChainFloor)` | capacity floor tier of async-tailed / `Chunked`-fence chains — per-pipeline tier of `YOUPIPE_CHAIN_FLOOR` (default `ChainFloor::Default` = `min(n, 65536)` slots; env shadows) | streaming | one zero-CPU latency-critical chain in a process of default-tier chains → `Slots(16384)`; tight chain-wide backpressure → `Off` |
| `with_async_tx_gate(AsyncTxGate)` | which hops of an async-tailed chain batch their sends — per-pipeline tier of `YOUPIPE_ASYNC_TX_BATCH` (default `AsyncTxGate::MixedOnly`; env shadows) | streaming (chains containing an async stage) | chasing a regression on one specific async chain → `Off` |
| `ComputePool::new_pinned(n)` | workers pinned 1:1 to allowed CPUs | pools | tight loops of large saturated fused batches (**not** streaming — see [pools](pools.md)) |
| `YOUPIPE_POOL_SPIN_BUDGET` | adaptive pre-park spin window (µs) over the pool idle loop's busy-spin arm — the budget grows on parks shorter than 4× the window and quarters on quiet parks (default off; `"1"` = 100 µs; a decimal ≥ 2 = an explicit window; an explicit `YOUPIPE_SPIN_ROUNDS` disarms it) | pools (fused batch terminals) | a tight driver loop submitting µs-scale batches back-to-back (−18…−22 % on 1K-item criterion batches); a measured loss on ≥100K-item batches and ms-scale cadences (see [dev/scheduler](../dev/scheduler.md)) |
| `YOUPIPE_SHARDED_TERM` | terminal channel = per-worker SPSC shards (default on; `"0"` = shared ring; an explicit value shadows `with_sharded_terminal`) | streaming (sync/expand terminals) | leave on for multi-worker terminals; the shared ring only wins single-worker terminals, which never shard anyway |
| `YOUPIPE_BATCH_RECV` | batched ring claims on the streaming data plane (default on, cap 64; `"0"` = per-item) | streaming | leave on; pairs with `YOUPIPE_SHARDED_TERM` — the two share the default |
| `YOUPIPE_SHARDED_TERM_ASYNC` | async terminal channel = per-driver-group async shards | streaming (async terminals) | async-only chain whose feeder saturates the single output ring (niche; a measured loss on sync-fed async terminals) |
| `YOUPIPE_SHARD_DEPTH` | per-shard capacity floor of the sync sharded terminal (default off; `"1"` = floor at the batch claim cap; `"0"` = off; `"N"` (≥ 2) = explicit per-shard floor; an explicit value shadows `with_shard_depth`) | streaming (sync/expand terminals) | not a default, and no auto tier survives the shape scan — winners disagree on the tier (expand `Slots(16)` −31…−43 %, two-stage-chain terminal `Slots(64)` @100K −5.6…−6.5 %, everything else flat-to-+58 %; see [dev/dead-ends](../dev/dead-ends.md)). Pin per shape |
| `YOUPIPE_ASYNC_MUX` | async stage/node driver tasks: `min(io_concurrency, runtime workers)`, each multiplexing its share of the in-flight items (default on; `"0"` = one task per in-flight slot; `"N"` = at most N drivers) | streaming + graph (async stages/nodes) | leave on — it removes `io_concurrency` task spawns and per-item cross-thread wakes from the run; `"0"` only to isolate the knob in a bench |
| `YOUPIPE_SPIN_ANCHOR` | adaptive pre-anchor `try_recv` spin window (µs) in sync worker recv loops | streaming | convoy collapse on shapes the defaults exclude: `Barrier` fences, pinned buffers, or a residual slow-mode sighting (see [dev/streaming](../dev/streaming.md)) |
| `YOUPIPE_FWD_BATCH` | fence forwarder flushes released chunks via `try_send_batch` (default on; `"0"` = per-item `send`) | streaming (fence forwarder) | leave on — it caps the fence convoy's tail at every size; `"0"` only to isolate the knob in a bench |
| `YOUPIPE_CHAIN_FLOOR` | unpinned channel capacities of chains with an async stage or a `Chunked` fence floor at `min(n, 65536)` slots (default on; `"0"` = unfloored; a decimal = an explicit tier; an explicit value shadows `with_chain_floor`) | streaming | leave on — it removes the async and fence convoy slow modes; escape only to pin backpressure tightly (per-stage `buffer` pins are the better tool) or to lower the tier on zero-CPU latency-critical chains |
| `YOUPIPE_GRAPH_TX_BATCH` | graph node/source outputs flushed per claim batch (`try_send_batch` per edge; default on; `"0"` = per-item `send`) | graph | leave on — the per-item loops cost 2–3× on light items; `"0"` only to isolate the knob in a bench |
| `YOUPIPE_STREAM_TX_BATCH` | stream stage/expand workers flush the mapped claim batch (`try_send_batch` per free run; default on; `"0"` = per-item `send`) | streaming (pure-sync chains fully; async-tailed chains per `YOUPIPE_ASYNC_TX_BATCH`) | leave on — 1.6–3× on multi-stage light-item chains; `"0"` only to isolate the knob in a bench |
| `YOUPIPE_ASYNC_TX_BATCH` | which hops of an async-tailed chain may batch their sends: default = only the hop into the async stage (the mixed ring); `"0"` = the historical per-item gate on every async-chain hop; `"1"` = every hop (measured loss); an explicit value shadows `with_async_tx_gate` | streaming (chains containing an async stage) | leave at the default — it stacks with `YOUPIPE_CLAIM_SCALE` (−33 % on `stage().stage_async()` @1K); `"0"` if chasing a regression on an async chain |
| `YOUPIPE_CLAIM_SCALE` | stage/expand/node workers' claim bound grows with the observed ring backlog (doubles on full claims, collapses to the per-worker share on partial/park; default on) | streaming + graph (async-fed faces only — async-tailed stream chains and graph sync/expand nodes whose output feeds an async node; pure-sync chains/nodes keep the fixed share) | leave on — it amortizes the ring-head CAS under an async consumer (−33 % on `stage().stage_async()` @1K stream, −6…−7 % on the graph `chain_async` shapes @100K); `"0"` only to isolate the knob in a bench |
| `YOUPIPE_ASYNC_OUTLET_BATCH` | async-producer outlet (mux drivers, graph async broadcast/unicast, graph `source_async` feeds) delivers drained clusters per edge via `try_send_batch` prefix + per-item awaited fallback (default on, cap 64; `"0"` = per-item; `"N"` = explicit cap) | streaming + graph (async stages/nodes/sources) | leave on — −4…−16 % on the async broadcast/fan-in graph shapes, −3 % on source-fed fan-out @1K and −5 % on async terminals @100K, parky IO shapes flat (see [dev/benchmarks/graph](../dev/benchmarks/graph.md)); `"0"` only to isolate the knob in a bench; a distinct hop from `YOUPIPE_ASYNC_TX_BATCH` |
| `YOUPIPE_RING_CACHE` | per-run channel rings recycled across runs: the handoff factories park each fresh ring's shared handle and the next construction of the same (channel type, capacity) rebuilds endpoints over it via the crossfire fork's `try_reset` (default off — measured a REGRESSION on its target shapes: the allocator's tcache already recycles the chunks; `"1"` = on; ≤48 rings / ≤64 KiB per ring / ≤2 MiB total) | streaming + graph (per-run construction cost) | nothing measured justifies enabling it (todo #45 A/B, [dev/benchmarks/graph](../dev/benchmarks/graph.md) "Per-run ring recycling: falsified"); kept as the A/B vehicle; a cancelled run's leftover items defer their drops to the next same-key construction |

`Workload` and `buffer_size`/`async_workers`/`io_concurrency` are disjoint: a
fused `pipe()` ignores the streaming knobs (it has no channels and no async
runtime); `stream()`'s channel topology ignores `Workload` (its MPMC channels
load-balance skew per item already) — `StreamPipe::with_workload` reaches only
the fused cores a pure sync chain can take (the `run()` pass-through and the
fused reduce/fold cores). `with_filter_collect` is likewise fused-only — it
applies to chains (by-ref or owned) containing a filter stage and ending in
`.collect()`/`.try_collect()`; `Ctp` degrades to the merge tree on owned
chains. An explicit mode beats the `YOUPIPE_FILTER_COLLECT` env knob, which
beats the `Auto` length gate (by-ref; owned `Auto` is always the merge tree).

## Per-pipeline data-plane policy (streaming)

Four of the `YOUPIPE_*` data-plane knobs also have a per-pipeline
middle layer — `with_sharded_terminal`, `with_shard_depth`,
`with_chain_floor`, `with_async_tx_gate` — so one process can
differentiate two concurrent pipelines where the env knob (process-global)
cannot. Precedence, for each of the four:

> explicitly-set env knob > per-pipeline config field > built-in default

The env tier outranks the config field — the **opposite** of
`FilterCollectMode`'s order — on purpose: bench A/Bs (`bench_ab.sh -E`)
compare the *same binary* through env vars, so an operator's explicit
pin must beat any config a library user set. With neither env nor config
set the behavior is bit-for-bit the env-knob default. The remaining
data-plane knobs stay env-only: their off positions are bench-isolation
vehicles (`YOUPIPE_BATCH_RECV`, `YOUPIPE_STREAM_TX_BATCH`,
`YOUPIPE_FWD_BATCH`, `YOUPIPE_CLAIM_SCALE`), scheduler A/B vehicles
(`YOUPIPE_POOL_SPIN_BUDGET`, `ONPOOL_HYBRID`), or niche single-shape
switches (`YOUPIPE_SHARDED_TERM_ASYNC`) — promote one only with a
measured per-workload story.

- `with_sharded_terminal(bool)`: terminal fan-in channel shape (see
  [`YOUPIPE_SHARDED_TERM`](#youpipe_sharded_term-terminal-fan-in-sharding-streaming)).
  Tune it for memory shape — a process hosting many small pipelines can
  keep their terminals on one shared ring each; throughput-wise the
  sharded default wins every measured shape.
- `with_shard_depth(ShardDepth)`: the sync sharded terminal's per-shard
  capacity floor (see
  [`YOUPIPE_SHARD_DEPTH`](#youpipe_shard_depth-sharded-terminal-shard-depth-streaming)).
  Default `Off` — the depth response is shape×tier×size specific (the
  todo #48 scan falsified the claim-cap default AND every scoped
  auto-tier predicate: the winners disagree on the tier), so the field
  exists to pin it per shape: `Slots(16)` on expand terminals,
  `ClaimCap`/`Slots(64)` on a two-stage chain's terminal at large n.
- `with_chain_floor(ChainFloor)`: the tier lifting unpinned channel
  capacities of async-tailed / `Chunked`-fence chains (see
  [`YOUPIPE_CHAIN_FLOOR`](#youpipe_chain_floor-chain-capacity-floor-streaming)).
  `ChainFloor::Default` = `min(n, 65536)` slots, `Slots(n)` = an explicit
  tier (`Slots(16384)` is the documented zero-CPU latency tier), `Off` =
  unfloored. The per-pipeline tier is the escape the env knob cannot
  express: one latency-critical chain on `Slots(16384)` while the rest of
  the process keeps the default — with the env knob that pin would lower
  every chain's tier.
- `with_async_tx_gate(AsyncTxGate)`: which hops of an async-tailed chain
  may batch their sends (see
  [`YOUPIPE_ASYNC_TX_BATCH`](#youpipe_async_tx_batch-async-tail-tx-batch-gate-streaming)).
  `MixedOnly` (default) batches exactly the hop into the async stage,
  `Off` restores per-item sends on every async-chain hop, `Full` batches
  every hop (a measured loss; A/B reproduction). Reach for `Off` when
  chasing a regression on one specific async chain without touching the
  process's other pipelines.

## Workload: split granularity (fused)

Motivating shape: ~10 % of items cost 1000× the rest. With coarse splits the
slow items strand a few workers while the rest go idle.

| Variant | Oversplit | Behaviour |
| --- | --- | --- |
| `Workload::Balanced` (default) | adaptive: 1 below ~1024 items/worker, else 4 | right for near-equal costs |
| `Workload::Unbalanced` | fixed 64, plus 8 extra top-level chunks (16 when `n / (workers + 16)` ≥ 48) | idle workers can steal the remaining leaves around a slow item; late-arriving workers find a leftover chunk in the injector; the wider tier scatters heavy-tail chunk-boundary luck |
| `Workload::Custom(n)` | pinned `n` | manual; useful envelope 8..=32 on large machines |

```rust
use youpipe::prelude::*;

// ~10 % slow items, 1000× cost spread → finer leaves, shorter tail.
let r: Vec<u64> = (0..5_000).pipe()
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();
```

`Workload` never changes the thread count — that is a pool decision
([pools](pools.md)). Fused terminals with a non-default worker budget
(`with_compute_workers(n)`, `with_oversubscribe(f)`) in loops recycle their
pools automatically (run 2 onward is spawn-free), but a pre-created
`ComputePool` + `with_compute_pool` remains the cheapest and most explicit
form — see [pools](pools.md).

For same-binary A/B benchmarking the two `Unbalanced` knobs are
runtime-overridable: `YOUPIPE_OVERSPLIT` (leaf-count factor, default 64)
and `YOUPIPE_CHUNK_SLACK` (extra top-level chunks; overrides the adaptive
8/16 tiering; set 0 to recover the cheap-item side, which pays ~2–7 %
for slack it cannot use — see the scheduler notes in the developer
guide).

The worker idle backoff windows are runtime-overridable too:
`YOUPIPE_SPIN_ROUNDS` (busy-spin rounds before yielding, default 32) and
`YOUPIPE_YIELD_ROUNDS` (yield rounds before the condvar park, default 32).
The defaults are A/B-tuned on the reference 32-core machine — widening or
narrowing them measured as global regressions there (history in `sleep.rs`)
— so treat them as experiment knobs for heterogeneous machines, not
tuning levers with known upside.

The fused `.collect()` output-store policy defaults to **auto**:
eligible 8-byte outputs of at least 8 MiB per whole batch are
written with non-temporal (streaming) stores that bypass the cache
hierarchy — at those sizes removing read-for-ownership traffic and L3
pollution dominates everything else. Runtime-overridable tri-state via
`YOUPIPE_NT_STORE`: unset = auto, `"0"` = force off, `"1"` = force on
(any other value panics — an early A/B passed `=off` and silently
enabled NT on both sides).

`YOUPIPE_SPIN_ANCHOR=<µs>` (unset/`"0"` = off, suggested `20`–`30`):
before each blocking recv, a sync stage/expand/fence worker spins
`try_recv` for an adaptive budget gated on recent channel liveness — a
park that returned within 4× the window doubles the budget, a quiet park
quarters it to zero, so idle stages park exactly as before. This removes
the convoy pathology (per-item futex park+wake at every
serial-supplier→crowd channel: collapsed fence runs 180–254 ms → 27–57 ms
@100K, parks 90–197K/run → 0.3–2.6K) at the cost of ~2–4× versus a lucky
fast seed in shapes that were already fast, plus on-CPU spinning while a
channel is live-but-sparse. Both convoy families now have default answers
(the [`YOUPIPE_CHAIN_FLOOR`](#youpipe_chain_floor-chain-capacity-floor-streaming)
lift and the fence forwarder's batched flush, `YOUPIPE_FWD_BATCH`): reach
for the spin only on the shapes those exclude — `Barrier`-mode fences with
explicitly pinned buffers — or on a residual slow-mode sighting.

`YOUPIPE_FWD_BATCH` (default on; `"0"` = per-item `send`): the fence
forwarder flushes each released chunk through one `try_send_batch` per
free run, parking at most once per full ring, and the chunk's consumer
wakes go out under one registry acquisition instead of one contended lock
per item. This is what caps the fence convoy's tail at every size (with
the floor, @1K 890 → 145 µs; the slow-mode tail disappears outright up
through 200K-scale runs); leave it on.

`YOUPIPE_BATCH_RECV`: default on with cap 64 (unset or `"1"`); `"0"` =
the per-item loops; `"N"` (2..=4096) = on with cap N. Batches the
streaming data plane's cursor updates: the collector's terminal drain,
the stage / expand / fence-forwarder burst phase and the feeder push loop
claim runs of consecutive ring slots with one atomic cursor update per
run instead of one per item (fork extension — mechanism in
[dev/streaming](../dev/streaming.md), verdicts in
[dev/benchmarks](../dev/benchmarks/streaming.md)). Backpressure granularity, item
order, capacity and cancellation semantics are unchanged; async terminals
keep per-item recv. Stage/expand workers claim at most `cap / workers`
items per claim (a whole-run claim would let one worker monopolize a
small burst its idle peers should split). Default on since 2026-10-20,
promoted together with `YOUPIPE_SHARDED_TERM`: the pair measured −19…−62 %
mean on every multi-process soak cell with the sharded-alone bimodality
removed, and `"0"` on both knobs is the documented escape hatch to the
shared-ring per-item configuration (readings:
[dev/benchmarks](../dev/benchmarks/streaming.md) "Default-policy soak + pair
promotion").

Why auto is safe for consumers that read the output right after collect
(the feared DRAM round-trip): measured on the reference machine
(same-binary two-process A/B, rayon drift control) the knob wins *both*
shapes at 2–4 M items — write-once outputs +14–18 % wall, and a fold
over the output immediately after collect +12–15 %
(`cpu_balanced_readback`) — the parallel phase's RFO elimination
outweighs the consumer's prefetched sequential re-read. At 1 M items
(8 MB, still inside the 32 MB L3) the sign is machine-state gated:
−12–13 % in a slow memory state, +39…+107 % loss in a fast one, so auto
keeps that cell on plain stores. Below the threshold gains shrink
toward ±2 % (output fits cache), which is why
auto starts at 8 MiB; known write-once workloads below it can force the
knob on (100 K items measured +9–11 %). Data in the developer guide's
benchmarks notes ("NT-store attribution").
## `YOUPIPE_SHARDED_TERM`: terminal fan-in sharding (streaming)

Terminal-stage channel shape: default (unset or `"1"`) = one SPSC ring
per terminal worker (sync/expand terminals), round-robin burst-drained
by the collector; `"0"` = one shared MPSC terminal ring. Sharding
removes the send-side CAS contention and the shared-ring cache lines of
the terminal fan-in (−5…−58 % on every measured shape, zero
regressions). Channel shape only — worker count, backpressure budget,
ordering and cancellation semantics are unchanged. Per-pipeline tier:
`with_sharded_terminal(bool)` (an explicit env value shadows the field —
see the [precedence rule](#per-pipeline-data-plane-policy-streaming)).
Default on since
2026-10-20, promoted together with `YOUPIPE_BATCH_RECV`: sharded alone
is bimodal on single-stage fan-in shapes (median wins, convoy slow mode
in ~30 % of back-to-back runs), while the pair with batched claiming is
tight and wins every soak cell — turning either knob off should turn
both off (readings: [dev/benchmarks](../dev/benchmarks/streaming.md)
"Default-policy soak + pair promotion").

## `YOUPIPE_SHARDED_TERM_ASYNC`: async terminal sharding (streaming)

The async-terminal flavour of the same channel swap (one async shard ring
per task group, shards capped at `async_workers`): unset/`"0"` = one
shared MPSC async ring (default), `"1"` = sharded. A separate knob on
purpose — the async flavour is a measured **loss** on sync-fed async
terminals (+42…+51 % @100K: the one-shard anchor's waker asymmetry paces
the whole fan-in), so it must not ride along with `YOUPIPE_SHARDED_TERM`.
Its sole measured winner is the async-only feeder-saturated chain at
−57.5 % (readings: [dev/benchmarks](../dev/benchmarks/streaming.md)); reach for it
only on that shape.

## `YOUPIPE_SHARD_DEPTH`: sharded-terminal shard depth (streaming)

Per-shard capacity floor of the sync sharded terminal (the async flavour
never lifts — its per-item drain has no batch claims to amortize, deep
rings would only add eager allocation). Default off: each shard keeps
`max(4, buffer / workers)` slots. `"1"` floors every shard at the
collector's batch claim cap (`YOUPIPE_BATCH_RECV`, 64) so
`try_recv_batch` can claim a full-cap run from one shard; a decimal
`>= 2` pins an explicit floor (the sweep caliber). Per-pipeline tier:
`with_shard_depth(ShardDepth)` (`ClaimCap` / `Off` / `Slots(n)`),
shadowed by an explicit env value (see the
[precedence rule](#per-pipeline-data-plane-policy-streaming)).

The claim-cap tier is a measured **loss** on the shape it was proposed
for — the single-stage multi-worker terminal (r5 read claims clustering
at the 8-slot shard cap and expected claim amortization; the A/B read
+14…+16 % @1K and +43…+53 % @100K, 0/25 — the shallow cap is what bounds
producer runaway and keeps the collector's round-robin pass balanced).
Its one measured winner is the terminal fed by an upstream stage
(`multi2` @100K, −5.7 %, 25/25 — paced producers only gain drain slack),
and the floor is inert wherever `buffer / workers >= cap` (e.g.
`workers(2)` at the default buffer). Readings:
[dev/benchmarks](../dev/benchmarks/streaming.md) "Sharded-terminal shard
depth"; falsification index in [dev/dead-ends](../dev/dead-ends.md).

## `YOUPIPE_ASYNC_TX_BATCH`: async-tail tx-batch gate (streaming)

Which hops of a chain containing an async stage may batch their output
sends (`try_send_batch` staging instead of one blocking `send` per item):
unset = only the hop **into** the async stage (the mixed ring, whose
consumers are the burst-refilling M:N drivers), `"0"` = the historical
per-item gate on every async-chain hop, `"1"` = every hop. The default is
the todo #34 four-side A/B: the mixed hop wins batched (−33 % merged @1K
stacked with `YOUPIPE_CLAIM_SCALE`) while the sync→sync mid hops lose at
either claim shape (+17.4 % `async_pair_cpu_split/100K` fully lifted), so
only the mixed hop batches. Independent of the global
`YOUPIPE_STREAM_TX_BATCH` switch (`"0"` there keeps per-item sends
everywhere; its off position is a bench-isolation vehicle, which is why
it stays env-only).

Per-pipeline tier: `with_async_tx_gate(AsyncTxGate)` — `MixedOnly` /
`Off` / `Full` mirror unset / `"0"` / `"1"`; an explicit env value shadows
the field (see the
[precedence rule](#per-pipeline-data-plane-policy-streaming)). Reach for
`Off` on one specific async chain that regresses under the mixed-hop
batching, without touching the process's other pipelines.

## `YOUPIPE_CHAIN_FLOOR`: chain capacity floor (streaming)

Default (unset or `"1"`): when the chain contains an async stage or a
`Chunked`-mode fence, every unpinned channel it builds — each stage's
output ring and the feeder ring — floors at `min(n, 65536)` slots,
byte-clamped at 8 MiB per ring; `"0"` recovers the unfloored sizing, and an
explicit decimal slot count overrides the tier (`"16384"` is the 2026-10-22
async tier). The slack removes both convoy slow modes. Async: with the
small default buffers a sync-prefix→`stage_async` chain collapses into a
per-item park+wake chain paced by wake latency (~20 % of back-to-back runs
at 100K items; 130–230 ms vs 18–21 ms fast), and the floor takes the
collapsed runs to zero while tightening the fast mode; the 64K tier
additionally keeps the async-only chain out of its residual
backpressure-bound mode (async0_cheap/100K −30 %, tier A/B in
[dev/benchmarks](../dev/benchmarks/attribution.md) "Floor-tier promotion").
Fence: a `Chunked` fence chain collapses the same way at both
serial-supplier→crowd interfaces (83 % of back-to-back runs @100K, median
118 ms vs 9.6 ms fast); the floor drops that to ~1 % (mean −78 %), with
the large-`n` tail the forwarder's batched flush (`YOUPIPE_FWD_BATCH`,
default on) then caps outright — on-side max 31.7 ms @200K vs the off
side's 215 ms, no run above 60 ms. `Barrier`-mode fences never lift the
chain (nothing overlaps a barrier; deep rings only add eager allocation),
and pure-sync chains without a fence never lift either.

Backpressure semantics are unchanged (bounded rings, blocking on `Full`
past the floor), and explicit per-stage `buffer` pins win verbatim — to
tighten one stage's memory, pin that stage's buffer instead of disabling
the floor chain-wide. The lift's eager allocation is bounded per ring
(65536 slots ≤ 8 MiB; small runs lift only to `n`). The one measurable
cost of the 64K tier is ~2 % median on zero-CPU latency-bound chains
(the 1 MiB ring recycles through L3 where the 16K tier's 256 KiB stayed
inside L2); set `YOUPIPE_CHAIN_FLOOR=16384` if that corner is your
shape — or `with_chain_floor(ChainFloor::Slots(16384))` when only some
chains of the process are that shape, leaving the others on the default
tier (an explicit env value shadows the field — see the
[precedence rule](#per-pipeline-data-plane-policy-streaming)).

## Worker budget across stages (streaming)

`compute_workers` is a **budget**, not a thread count. The runner first
reserves one pool slot for the feeder (a pool job whenever `n > buffer`),
then grants `SyncStageOptions::workers` pins in pipeline order — each clamped
  to
what remains, with one slot held back per not-yet-spawned sync stage — and
divides the rest equally across unpinned stages. Every sync stage keeps at
least 1 resident worker, so total blocking pool jobs never exceed the pool:
the "stage 1 filled the pool, stage 2 starved" deadlock cannot occur. Pins
exceeding the budget are silently clamped (later stages get 1 worker each) —
if you need the pins honored exactly, give the pipeline a larger pool.

```rust
use youpipe::prelude::*;

// Heavy CPU stage pinned to 8 workers; the light stage divides the rest.
// The async stage gets its own io_concurrency and buffer, overriding the
// pipeline-level values.
let r: Vec<u64> = items.stream()
    .stage_with(SyncStageOptions::new().workers(8), |x: u64| crunch(x))
    .stage(|x: u64| light(x))
    .stage_async_with(
        AsyncStageOptions::new().io_concurrency(512).buffer(1024),
        |x: u64| async move { io(x).await },
    )
    .run();
```

`with_compute_workers(n)` **pins** the budget: it applies regardless of
`with_compute_pool` and regardless of the order the two were called in,
clamped to the pool's thread count. With no pin set, the budget follows the
pool (the global pool grants one worker per core) — see
[pools](pools.md).

The budget is enforced **pool-wide**: a run atomically leases its whole
upper bound of channel-parking jobs (feeder + stage workers + fence
forwarders) from the pool, so concurrent `run()`s on a shared pool —
including the global pool from multiple threads — cannot jointly
oversubscribe it with parked workers. When the remaining lease capacity
cannot host the run (busy pool, or more sync stages than pool slots), or
`run()` is itself called on a worker of the same pool (nested pipelines
park that worker in the collector for the whole run), the runner does not
touch the pool at all: stage workers, fence forwarders and the feeder run
as dedicated OS threads — deadlock-free by construction, at the cost of one
`thread::spawn` per job.

## `io_concurrency`: M:N async fan-out (streaming)

Async IO futures yield the OS thread while waiting, so the in-flight cap
(`io_concurrency`, default 128) can far exceed `async_workers` (the thread
count). Raise it to keep the runtime saturated with cheap waits; it doubles
as the memory bound — each in-flight item holds its input and buffers.

The items run on at most one driver task per runtime worker thread, each
multiplexing its share of the cap, so a large `io_concurrency` costs no
extra tasks: raising it from 128 to 4096 adds no per-run spawn cost.

Override per stage when stages differ: a network stage wants ~512, a local
disk stage ~16 — via
`stage_async_with(AsyncStageOptions::new().io_concurrency(n), ..)`.

## `buffer_size`: backpressure depth (streaming)

Per-channel capacity between stages, default 256. The effective capacity is
`max(buffer_size, workers * 4)`, where `workers` is the fan-out of the stage
producing into that channel — a floor so each of those workers has a few free
slots ahead; an explicit `SyncStageOptions::buffer(n)` replaces that logic
for that stage's output channel. Chains containing an async stage or a
`Chunked`-mode fence additionally apply the
[`YOUPIPE_CHAIN_FLOOR`](#youpipe_chain_floor-chain-capacity-floor-streaming)
lift (`min(n, 65536)`, byte-clamped) to every unpinned channel. Small
buffers tighten backpressure (less peak memory); large ones absorb bursts.

## Ordering and fences

`.run()` collects in completion order — no reorder cost. `.ordered()` tags
each item with a sequence number and buffers through a `ReorderBuffer`; pay
it only when the consumer needs input order. Ordered expansion requires
`.ordered()` *before* `.expand()` (after it: compile error) and then carries
each group as one message — one `Vec` allocation per input item, fewer and
larger channel slots than the per-item fan-out.

Fences trade throughput for semantics. The default (overlapping stages) is
fastest; add `FenceMode::Barrier` when a stage needs the upstream fully
drained, `FenceMode::Chunked(k)` for batch-wise isolation that still
overlaps — the right default for mixed CPU/IO.

## Scenario recipes

| Scenario | Knobs |
| --- | --- |
| Skewed CPU (10 % of items cost 1000×) | `with_workload(Workload::Unbalanced)`; extreme skew: `Custom(16..=32)`; cheap ns-scale items: `YOUPIPE_CHUNK_SLACK=0` |
| Blocking IO inside a sync `.stage()` | oversized compute pool via `with_compute_pool` — see [pools](pools.md); `Workload` will not help |
| Many small async IO ops | raise `io_concurrency` (512+) globally or per stage; widen `buffer_size` for bursty sources |
| Heavy CPU stage + light IO stage | pin the heavy stage's `SyncStageOptions::workers`; give the async stage its own `AsyncStageOptions::io_concurrency`/`buffer` |
| One zero-CPU latency-critical async chain in a process of default-tier chains | `with_chain_floor(ChainFloor::Slots(16384))` on that pipeline only (the env pin would lower every chain's tier) |
| Consumer needs input order | `.ordered()`; drop it if completion order is acceptable |
