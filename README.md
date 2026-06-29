# youpipe

English | [简体中文](./README.zh-CN.md)

youpipe is a high-performance, data-first parallel pipeline supporting mixed
CPU workloads and streaming async IO. Items enter at the front, stages chain
naturally, and a single terminal call (`.collect()` / `.run()`) executes the
whole chain. Two pipeline engines cover different regimes:

- `Pipe` — compile-time fused CPU chains. `.map().filter().map()` becomes a
  single monomorphized closure per worker with no intermediate allocations.
- `StreamPipe` — channel-backed streaming for cases fusion cannot cover:
  async IO, cancellation, fences, 1-to-N expansion, and more.

A rayon-style work-stealing scheduler (`st3` LIFO deque + packed atomic
sleep counters) handles balanced and unbalanced loads. `scope()` supports non-`'static`
closures that borrow stack-local data.

Usage: `cargo add youpipe`.

## API

`pipe(items)` / `items.pipe()` produce the same types — either works.

```rust
use youpipe::pipe;
let r: Vec<i32> = pipe(0..1000).map(|x| x + 1).collect();
// same as
use youpipe::prelude::*;
let r: Vec<i32> = (0..1000).pipe().map(|x| x + 1).collect();
```

Pick the entry point by workload:

| Workload                     | Entry                                                |
| ---------------------------- | ---------------------------------------------------- |
| Pure CPU map/filter          | `pipe(items)`                                        |
| Async IO, mixed sync+async   | `stream(items).stage_async(...)`                     |
| Unbalanced CPU workloads     | `pipe(items).with_workload(Unbalanced)`              |
| Cancellation, fences, expand | `stream(items).with_cancel(..).fence(..).expand(..)` |
| Borrow stack-local data      | `scope(\|s\| s.pipe(..)....)`                        |

Below ~10 µs of total work or ~100 ns per item, youpipe is not recommended —
the parallel setup overhead won't pay off. Sequential `iter().map().collect()`
is faster in that range.

## Examples

youpipe does **not** wait for one stage to finish completely before starting
the next. Use a fence between stages if you need strict stage isolation.

```rust
use std::num::NonZeroUsize;
use youpipe::prelude::*;

// fused CPU bound
let r: Vec<i32> = (0..1000).pipe()
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x| x * 10)
    .collect();

// fallable
let r: Result<Vec<String>, _> = (0..100).pipe()
    .try_map(|x: i32| if x == 50 { Err("bad") } else { Ok(x * 2) })
    .map(|x| format!("{x}"))
    .try_collect();

// sync CPU stage + async IO stage (overlap on separate pools)
let r: Vec<u64> = (0..1000).stream()
    .stage(|x: u64| x + 1)
    .stage_async(|x: u64| async move { fetch(x).await })
    .run();

// fence: batch every 64 items between two adjacent stages
let r: Vec<i32> = (0..1000).stream()
    .stage(|x: i32| x + 1)
    .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
    .stage(|x: i32| x * 2)
    .run();

// scope borrows local `factor` and `table`, no clone
let factor = 7;
let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
let r: Vec<usize> = scope(|s| {
    s.pipe(0..table.len()).map(|i: usize| table[i].len() * factor).collect()
});
```

## Performance

7945HX 32-core Linux. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#9-performance-benchmarks).

fused `pipe()` — CPU-heavy (100 iters/item, warm input):

| Size | youpipe | rayon  |
| ---- | ------- | ------ |
| 1K   | 34 µs   | 40 µs  |
| 10K  | 66 µs   | 73 µs  |
| 100K | 106 µs  | 147 µs |

fused `pipe()` — lightweight `x+1` (warm input):

| Size | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 64 µs   | 69 µs  |
| 100K | 85 µs   | 110 µs |
| 1M   | 606 µs  | 272 µs |

fused `pipe()` — 3-stage chain (`x+1`, `x*3`, `x-2`):

| Size | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 65 µs   | 70 µs  |
| 100K | 90 µs   | 104 µs |

fused `try_map().try_collect()` — fallible `Result` chain (warm input):

| Size | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 64 µs   | 68 µs  |
| 100K | 88 µs   | 101 µs |

streaming `stream()` — single sync stage (`cpu_work`, 100 iters/item):

| Size | youpipe | tokio spawn_blocking |
| ---- | ------- | -------------------- |
| 1K   | 0.72 ms | 2.46 ms              |
| 10K  | 8.8 ms  | 23.5 ms              |
| 100K | 88.6 ms | 236 ms               |

Pure async IO (`tokio::time::sleep`, ~1 ms latency, 90/10 tail, 500 items):

| Topology                               | Time    |
| -------------------------------------- | ------- |
| youpipe: async IO (`.stage_async`)     | 9.65 ms |
| tokio: native async                    | 9.30 ms |
| youpipe: blocking IO (`.stage`)        | 33.1 ms |
| youpipe: blocking IO (oversub 512 thr) | 19.5 ms |
| tokio: spawn_blocking                  | 8.83 ms |

Mixed CPU + IO (two stages, 500 items):

| Topology                              | Time    |
| ------------------------------------- | ------- |
| youpipe: sync CPU + async IO          | 9.97 ms |
| tokio: mixed spawn_blocking           | 10.1 ms |
| youpipe: sync CPU + blocking IO       | 60.0 ms |

## Advanced usage

Defaults: `compute_workers = async_workers = available_parallelism`,
`io_concurrency = 128`, `buffer_size = 256`, `Workload::Balanced`. The tokio
runtime is built lazily on first `.run()` and reused for that run; pass an
`AsyncPool` to share one across runs.

```rust
use youpipe::prelude::*;

// Unbalanced: ~10% slow items, 1000× cost spread → raises oversplit factor
let r: Vec<_> = (0..5_000).pipe()
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();

// Tuned config + reused runtime
let cfg = PipelineConfig::default()
    .with_compute_workers(16)
    .with_async_workers(8)
    .with_io_concurrency(512)
    .with_buffer_size(1024);
let pool = AsyncPool::from_default()?;
let r = items.stream()
    .with_config(cfg)
    .with_async_pool(pool)
    .stage_async(|x| async move { io(x).await })
    .run();

// Cancellation
let token = CancellationToken::new();
let r = (0..10_000).stream()
    .with_cancel(token.clone())
    .stage(|x| expensive(x))
    .run();

// Oversubscribed compute pool for blocking-IO sync stages
let pool = ComputePool::new(512);
let r = (0..1000).stream()
    .with_compute_pool(pool)
    .stage(|x| blocking_io(x))
    .run();
```

`io_concurrency` is the M:N multiplier — async tasks yield the OS thread
while waiting, so it can be far larger than `async_workers` (the thread
count). Bound it to cap memory.

`.fence(mode)` acts on one adjacent stage boundary. `FenceMode::Barrier`
drains upstream fully before downstream starts; `FenceMode::Chunked(k)`
releases every `k` items as they form (the default for mixed CPU/IO).
`.run()` returns results in completion order; append `.ordered()` to restore
input order via a `ReorderBuffer`.

## How it works

`Pipe` composes a compile-time typestate chain that monomorphises into a
single closure per worker — no `dyn`, no per-stage `Vec`. The fused hot path
allocates input/output buffers once and recurses on the index range `[0, n)`,
handing each leaf a `&[T]` / `&mut [R]` slice view so the leaf loop stays
branch-free and vectorisable.

`StreamPipe` walks the chain at `.run()` time, spawning workers per stage
over channels. Sync stages run on the `ComputePool`; async stages multiplex
`io_concurrency` tokio tasks on `async_workers` OS threads. Full design
rationale, module walkthrough and panic-safety discussion in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
