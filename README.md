# youpipe

High-performance Rust concurrent pipeline batch processing framework with compile-time fusion.

## Features

- **Data-First API** — `pipe(items).map().filter().collect()`; data enters at the front, not at the end
- **Compile-Time Fusion** — `.map().filter().map()` compiles to a single closure per worker, zero intermediate allocations
- **Workload Hints** — `.with_workload(Workload::Balanced)` (zero-atomics) or `Workload::Unbalanced` (adaptive fetch-add)
- **Work-Stealing Pool** — Lock-free `st3` LIFO deque scheduler with EventCount wake-up
- **Streaming Pipelines** — `stream(items).stage().stage_async()` with channels between stages, ordered/unordered output
- **Async IO Stages** — `.stage_async()` for M:N IO concurrency on a tokio runtime
- **Fallible Chains** — `.try_map()` / `.try_collect()` with early termination on first error
- **Cancellation** — `.with_cancel(token)` for cooperative StreamPipe shutdown
- **Scoped Execution** — `scope()` for non-`'static` closures that borrow stack-local data
- **1-to-N Expansion** — `.expand()` for flatMap-style stages

## Pipe vs StreamPipe

| 维度 | `Pipe` (fused) | `StreamPipe` (stream) |
|---|---|---|
| **组合方式** | 编译时类型状态链，零成本抽象 | 运行时闭包 + channel 连接 |
| **执行引擎** | `par_index_collect` work-stealing join，单次预分配 | producer-consumer channel + compute pool workers |
| **内存** | 无中间分配，最终 `Vec<O>` 一次性填充 | 每阶段一个 channel pair，额外 buffer |
| **异步/IO** | ❌ 不支持 | ✅ `.stage_async()` |
| **取消** | ❌ 不支持 | ✅ `.with_cancel(token)` |
| **阶段数** | 编译期固定（链式） | 可变（`.stage()` / `.expand()` / `.fence()`） |
| **闭包生命周期** | `'static` | `'static` |
| **开销** | 接近手写循环 | channel 同步 + 内存拷贝 |
| **适用场景** | 纯 CPU map-filter 流水线 | 异步 IO、取消、运行时阶段拼接 |

`Pipe` 适合纯 CPU 密集任务，追求极致性能；`StreamPipe` 覆盖异步 IO、取消、运行时阶段组合等前者无法处理的场景，以少量同步开销换取灵活性。两者执行引擎完全不同（work-stealing join vs channel），不可互相替代。

## Quick Start

```toml
[dependencies]
youpipe = "0.2"
```

### Fused Pipe (CPU-bound)

```rust
use youpipe::pipe;

// Data-first: items enter at the front, stages chain, `.collect()` executes.
let result: Vec<i32> = pipe(0..1000)
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x| x * 10)
    .collect();

// Unbalanced workload → finer-grained task stealing.
use youpipe::Workload;
let r: Vec<i32> = pipe(0..1000)
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();
```

### Fallible chain

```rust
use youpipe::pipe;

// Interleave `.try_map()` and `.map()`; `.try_collect()` short-circuits on the
// first `Err`.
let result: Result<Vec<String>, &str> = pipe(0..100)
    .try_map(|x: i32| if x == 50 { Err("bad") } else { Ok(x * 2) })
    .map(|x| format!("{x}"))
    .try_collect();
```

### Streaming Pipe (channels between stages)

```rust
use youpipe::stream;

// Stages connected by lock-free channels; output arrives in completion order.
// Add `.ordered()` to restore input order via a ReorderBuffer.
let result = stream(0..1000)
    .stage(|x: i32| x + 1)
    .stage(|x: i32| x * 2)
    .ordered()
    .run();
```

### Async IO stage (mixed sync CPU + async IO)

`.stage_async()` runs an async stage as `io_concurrency` tasks on a tokio
runtime (M:N concurrency for yielding IO — network/disk, `tokio::time::sleep`).
Reuse the runtime across runs by attaching it via `.with_async_pool(...)`.

```rust
use youpipe::{stream, AsyncPool, PipelineConfig};

let pool = AsyncPool::from_global(8).unwrap();
let r = stream(vec![1u64, 2, 3])
    .with_config(PipelineConfig::default().with_io_concurrency(256))
    .with_async_pool(pool)
    .stage(|x: u64| x + 1)                       // sync CPU on compute pool
    .stage_async(|m: u64| async move { m * 2 })  // async IO on runtime (M:N)
    .run();
```

### Scoped Pipe (non-`'static` closures)

```rust
use youpipe::scope;

let factor: usize = 7;
let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
// Borrow `factor` and `&table` from every worker — no clone, no Arc.
let result: Vec<usize> = scope(|s| {
    s.pipe(0..table.len())
        .map(|i: usize| table[i].len() * factor)
        .collect()
});
```

## API

| Function / Type | Description |
|---|---|
| `pipe(iter)` → `.map()` → `.filter()` → `.collect()` | Data-first fused CPU pipeline |
| `pipe(iter).try_map().map().try_collect()` | Fallible fused chain (short-circuits) |
| `.with_workload(Workload)` / `.with_config(config)` | Tune oversplit / config |
| `stream(iter)` → `.stage()` → `.expand()` → `.fence()` → `.run()` | Streaming pipeline (channels between stages) |
| `.stage_async(fut)` | Async IO stage on the tokio runtime (M:N) |
| `.ordered()` | Restore input order via `ReorderBuffer` |
| `.with_cancel(token)` / `.with_async_pool(pool)` | Cancellation / runtime reuse |
| `scope(\|s\| s.pipe(iter)…)` | Non-`'static` scoped fused pipeline |
| `CancellationToken` | Cooperative cancellation |
| `ComputePool` | Work-stealing thread pool |
| `channel(cap)` / `async_channel(cap)` | MPMC channels |

## Benchmarks

```bash
cargo bench --bench channel_bench    # Channel throughput
cargo bench --bench sync_vs_rayon    # CPU-heavy, fusion, lightweight
cargo bench --bench unbalanced       # Unbalanced workloads
cargo bench --bench mixed_load       # Mixed CPU/IO (blocking)
cargo bench --bench io_async         # Async IO (pure + mixed sync+async)
cargo bench --bench async_vs_tokio   # Stream vs tokio spawn_blocking
```

Results for individual benches are documented in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Testing

```bash
cargo test
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" cargo miri test
```

## License

MIT
