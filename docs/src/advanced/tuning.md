# Tuning knobs by workload

Every knob has a sensible default; tune only when a measured problem points at one.

## Knob overview

| Knob | Effect | Applies to | Reach for when |
| --- | --- | --- | --- |
| `with_workload(Workload)` | fork/join split granularity | fused only | item costs are skewed |
| `with_buffer_size(n)` | channel capacity between stages | streaming | bursty producers, memory bounds |
| `with_io_concurrency(n)` | in-flight async tasks per async stage | streaming | IO waits are cheap and plentiful |
| `.ordered()` | reorder pass restoring input order | streaming | output must match input order |
| `.fence(mode)` | isolation at one stage boundary | streaming | downstream must not see partial upstream |
| `StageOptions` | per-stage workers / io_concurrency / buffer | streaming | stages have unequal costs |

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
| `Workload::Unbalanced` | fixed 8 | idle workers can steal the remaining leaves around a slow item |
| `Workload::Custom(n)` | pinned `n` | manual; useful envelope 4..=16 on large machines |

```rust
use youpipe::prelude::*;

// ~10 % slow items, 1000× cost spread → finer leaves, shorter tail.
let r: Vec<u64> = (0..5_000).pipe()
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();
```

`Workload` never changes the thread count — that is a pool decision
([pools](pools.md)).

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

`with_compute_pool(pool)` sets the budget to the pool's actual worker count
(see [pools](pools.md)).

When the budget cannot give every sync stage a resident worker (more sync
stages than pool slots), or `run()` is itself called on a worker of the same
pool (nested pipelines park that worker in the collector for the whole run),
the runner does not touch the pool at all: stage workers and the feeder run
as dedicated OS threads — deadlock-free by construction, at the cost of one
`thread::spawn` per worker.

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
| Skewed CPU (10 % of items cost 1000×) | `with_workload(Workload::Unbalanced)`; extreme skew: `Custom(8..=16)` |
| Blocking IO inside a sync `.stage()` | oversized compute pool via `with_compute_pool` — see [pools](pools.md); `Workload` will not help |
| Many small async IO ops | raise `io_concurrency` (512+) globally or per stage; widen `buffer_size` for bursty sources |
| Heavy CPU stage + light IO stage | pin the heavy stage's `StageOptions::workers`; give the async stage its own `io_concurrency`/`buffer` |
| Consumer needs input order | `.ordered()`; drop it if completion order is acceptable |
