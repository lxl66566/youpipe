# Tuning knobs by workload

Every knob has a sensible default; tune only when a measured problem points at one.

## Knob overview

| Knob | Effect | Applies to | Reach for when |
| --- | --- | --- | --- |
| `with_workload(Workload)` | fork/join split granularity | fused only | item costs are skewed |
| `with_buffer_size(n)` | channel capacity between stages | streaming | bursty producers, memory bounds |
| `with_io_concurrency(n)` | in-flight async tasks per async stage | streaming | IO waits are cheap and plentiful |
| `.ordered()` | reorder pass restoring input order | streaming | output must match input order |
| `.fence(mode)` / `.fence_with(StageOptions, mode)` | isolation at one stage boundary (`buffer` pin) | streaming | downstream must not see partial upstream |
| `StageOptions` | per-stage workers / io_concurrency / buffer | streaming | stages have unequal costs |
| `ComputePool::new_pinned(n)` | workers pinned 1:1 to allowed CPUs | pools | tight loops of large saturated fused batches (**not** streaming — see [pools](pools.md)) |
| `YOUPIPE_SHARDED_TERM` | terminal channel = per-worker / per-task-group SPSC shards | streaming | multi-worker (sync) or high-fanout (async) terminal stage is the bottleneck (see [dev/streaming](../dev/streaming.md)) |
| `YOUPIPE_SPIN_ANCHOR` | adaptive pre-anchor `try_recv` spin window (µs) in sync worker recv loops | streaming | convoy collapse: serial supplier (feeder/fence) feeding ≥ ~10 workers, run times ~items × 2 µs (see [dev/streaming](../dev/streaming.md)) |
| `YOUPIPE_FWD_BATCH` | fence forwarder pushes released batches via `try_send` | streaming | rarely useful alone; pairs with `YOUPIPE_SPIN_ANCHOR` on fence-heavy chains |

`Workload` and `buffer_size`/`async_workers`/`io_concurrency` are disjoint: a
fused `pipe()` ignores the streaming knobs (it has no channels and no async
runtime); `stream()` ignores `Workload` (its MPMC channels load-balance skew
per item already).

## Workload: split granularity (fused)

Motivating shape: ~10 % of items cost 1000× the rest. With coarse splits the
slow items strand a few workers while the rest go idle.

| Variant | Oversplit | Behaviour |
| --- | --- | --- |
| `Workload::Balanced` (default) | adaptive: 1 below ~1024 items/worker, else 4 | right for near-equal costs |
| `Workload::Unbalanced` | fixed 32, plus 8 extra top-level chunks (16 when `n / (workers + 16)` ≥ 64) | idle workers can steal the remaining leaves around a slow item; late-arriving workers find a leftover chunk in the injector; the wider tier scatters heavy-tail chunk-boundary luck |
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
runtime-overridable: `YOUPIPE_OVERSPLIT` (leaf-count factor, default 32)
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
eligible 8-byte outputs of at least 8 MiB (1 M items) per whole batch are
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
channel is live-but-sparse. Leave off unless the shape matches the
pathology; it cannot help async-terminal chains (their slow mode parks on
the send side and the collector, not the recv anchor).

`YOUPIPE_FWD_BATCH=1`: the fence forwarder drains a released chunk with
back-to-back `try_send`, parking at most once per full ring. Measured no
independent win while downstream workers still park per item on recv; keep
for chains that already run `YOUPIPE_SPIN_ANCHOR`.

Why auto is safe for consumers that read the output right after collect
(the feared DRAM round-trip): measured on the reference machine
(same-binary two-process A/B, rayon drift control) the knob wins *both*
shapes — write-once outputs +12–18 % wall at 1–4 M items, and a fold
over the output immediately after collect +12–15 % at 1–4 M
(`cpu_balanced_readback`) — the parallel phase's RFO elimination
outweighs the consumer's prefetched sequential re-read. Below the
threshold gains shrink toward ±2 % (output fits cache), which is why
auto starts at 8 MiB; known write-once workloads below it can force the
knob on (100 K items measured +9–11 %). Data in the developer guide's
benchmarks notes ("NT-store attribution").
## `YOUPIPE_SHARDED_TERM`: terminal fan-in sharding (streaming)

Terminal-stage channel shape: unset/`"0"` = one shared MPSC terminal ring
(default), `"1"` = one SPSC ring per terminal worker (sync/expand
terminals) or per task group (async terminals, shards capped at
`async_workers`), round-robin burst-drained by the collector. Removes the
send-side CAS contention and the shared-ring cache lines of the terminal
fan-in; sync shapes measured −5…−58 % with zero regressions (2026-09-29).
The async flavour is a measured **loss** on sync-fed async terminals
(+42…+51 % @100K: the one-shard anchor's waker asymmetry paces the whole
fan-in; async-only feeder-saturated chains are the sole winner at
−57.5 % — see [dev/benchmarks](../dev/benchmarks.md)), so mixed CPU/IO
pipelines should leave the knob off unless the sync terminal dominates.
Channel shape only — worker count, backpressure budget, ordering and
cancellation semantics are unchanged. Default stays off pending the
post-convoy-fix soak.

## Worker budget across stages (streaming)

`compute_workers` is a **budget**, not a thread count. The runner first
reserves one pool slot for the feeder (a pool job whenever `n > buffer`),
then grants `StageOptions::workers` pins in pipeline order — each clamped to
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
    .stage_with(StageOptions::new().workers(8), |x: u64| crunch(x))
    .stage(|x: u64| light(x))
    .stage_async_with(
        StageOptions::new().io_concurrency(512).buffer(1024),
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

Async IO tasks yield the OS thread while waiting, so the in-flight task cap
(`io_concurrency`, default 128) can far exceed `async_workers` (the thread
count, default = available_parallelism). Raise it to keep the runtime
saturated with cheap waits; it doubles as the memory bound — each in-flight
task holds its item and buffers.

Override per stage when stages differ: a network stage wants ~512, a local
disk stage ~16 — via `stage_async_with(StageOptions::new().io_concurrency(n), ..)`.

## `buffer_size`: backpressure depth (streaming)

Per-channel capacity between stages, default 256. The effective capacity is
`max(buffer_size, downstream_workers * 4)` — a floor so every downstream
worker can hold items in flight; an explicit `StageOptions::buffer(n)`
replaces that logic for that stage's output channel. Small buffers tighten
backpressure (less peak memory); large ones absorb bursts.

## Ordering and fences

`.run()` collects in completion order — no reorder cost. `.ordered()` tags
each item with a sequence number and buffers through a `ReorderBuffer`; pay
it only when the consumer needs input order. `.ordered()` plus `.expand()`
panics (1-to-N breaks the single-sequence assumption).

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
| Heavy CPU stage + light IO stage | pin the heavy stage's `StageOptions::workers`; give the async stage its own `io_concurrency`/`buffer` |
| Consumer needs input order | `.ordered()`; drop it if completion order is acceptable |
