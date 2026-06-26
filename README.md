# youpipe

High-performance Rust concurrent pipeline batch processing framework with compile-time fusion.

## Features

- **Compile-Time Fusion** — `.map().filter().map()` compiles to a single closure per worker, zero intermediate allocations
- **Workload Hints** — `Workload::Balanced` (zero-atomics) or `Workload::Unbalanced` (adaptive fetch-add)
- **Work-Stealing Pool** — Lock-free `crossbeam-deque` scheduler with EventCount wake-up
- **Streaming Pipelines** — Multi-stage channel pipelines with ordered/unordered output
- **Fallible Parallelism** — `try_par_map` with early termination on first error
- **Cancellation** — `CancellationToken` for cooperative StreamPipeline shutdown
- **Scoped Execution** — `scope()` for non-`'static` closures
- **Chunked Map** — `par_chunks_map` for batch/SIMD-friendly processing

## Quick Start

```toml
[dependencies]
youpipe = "0.2"
```

### par_map

```rust
use youpipe::{par_map, par_map_with_workload, Workload};

let squares: Vec<i64> = par_map(0..1000, |x| (x as i64).pow(2));

// For skewed workloads
let results = par_map_with_workload(0..1000, |x| expensive(x), Workload::Unbalanced);
```

### try_par_map

```rust
use youpipe::try_par_map;

let results: Result<Vec<i32>, String> = try_par_map(0..100, |x| {
    if x == 50 { Err("bad") } else { Ok(x * 2) }
});
```

### Fused Pipeline

```rust
use youpipe::Pipeline;

let result = Pipeline::from_vec(vec![])
    .map(|x: i32| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x: i32| x * 10)
    .collect(0..1000);
```

### Streaming Pipeline

```rust
use youpipe::{StreamPipeline, PipelineConfig, CancellationToken};

let config = PipelineConfig::default().with_compute_workers(8);
let token = CancellationToken::new();
let sp = StreamPipeline::new(config).with_cancel(token.clone());

let result = sp.run(vec![1, 2, 3, 4, 5], |x: i32| x * 2, true);
```

### Scoped Pipeline

```rust
use youpipe::scope;

let factor = 7;
let result = scope(|s| {
    s.pipeline((0..100).collect())
        .map(|x: i32| x * factor)
        .par_map(|x: i32| x + 1, 4)
        .collect()
});
```

## API

| Function / Type | Description |
|---|---|
| `par_map(iter, f)` | Parallel map (balanced) |
| `par_map_with_workload(iter, f, Workload)` | Parallel map with workload hint |
| `par_chunks_map(iter, chunk_size, f)` | Chunked parallel map |
| `try_par_map(iter, f)` | Fallible parallel map |
| `Pipeline::from_vec()` → `.map()` → `.filter()` → `.collect()` | Fused pipeline |
| `StreamPipeline::new(config)` → `.run()` | Streaming pipeline |
| `CancellationToken` | Cooperative cancellation |
| `scope(\|s\| ...)` | Non-`'static` scoped execution |
| `ComputePool` | Work-stealing thread pool |
| `channel(cap)` / `async_channel(cap)` | MPMC channels |

## Benchmarks

```bash
cargo bench --bench channel_bench    # Channel throughput
cargo bench --bench sync_vs_rayon    # CPU-heavy, fusion, lightweight
cargo bench --bench unbalanced       # Unbalanced workloads
cargo bench --bench mixed_load       # Mixed CPU/IO
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
