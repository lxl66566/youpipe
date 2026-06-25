# youpipe

高性能 Rust 并发流水线批处理框架，支持编译时融合。

## 特性

- **编译时融合** — `.map().filter().map()` 编译为每个 worker 的单次闭包调用，零中间分配
- **负载提示** — `Workload::Balanced`（零原子操作）或 `Workload::Unbalanced`（自适应 fetch-add）
- **工作窃取线程池** — 基于 `st3` 的无锁 LIFO deque 调度，EventCount 唤醒
- **流式管道** — 多阶段通道管道，支持有序/无序输出
- **可失败并行** — `try_par_map` 首次错误即终止
- **取消支持** — `CancellationToken` 协作式 StreamPipeline 关闭
- **作用域执行** — `scope()` 支持非 `'static` 闭包
- **分块映射** — `par_chunks_map` 适用于批处理/SIMD 场景

## 快速入门

```toml
[dependencies]
youpipe = "0.2"
```

### par_map

```rust
use youpipe::{par_map, par_map_with_workload, Workload};

let squares: Vec<i64> = par_map(0..1000, |x| (x as i64).pow(2));

// 不均衡负载
let results = par_map_with_workload(0..1000, |x| expensive(x), Workload::Unbalanced);
```

### try_par_map

```rust
use youpipe::try_par_map;

let results: Result<Vec<i32>, String> = try_par_map(0..100, |x| {
    if x == 50 { Err("bad") } else { Ok(x * 2) }
});
```

### 融合管道

```rust
use youpipe::Pipeline;

let result = Pipeline::new()
    .map(|x: i32| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x: i32| x * 10)
    .collect(0..1000);
```

### 流式管道

```rust
use youpipe::{StreamPipeline, PipelineConfig, CancellationToken};

let config = PipelineConfig::default().with_compute_workers(8);
let token = CancellationToken::new();
let sp = StreamPipeline::new(config).with_cancel(token.clone());

let result = sp.run(vec![1, 2, 3, 4, 5], |x: i32| x * 2, true);
```

### 作用域管道

```rust
use youpipe::scope;

let factor = 7;
let result = scope(|s| {
    s.pipeline()
        .map(|x: i32| x * factor)
        .map(|x: i32| x + 1)
        .collect((0..100).collect())
});
```

## API 一览

| 函数 / 类型 | 说明 |
|---|---|
| `par_map(iter, f)` | 并行映射（均衡） |
| `par_map_with_workload(iter, f, Workload)` | 带负载提示的并行映射 |
| `par_chunks_map(iter, chunk_size, f)` | 分块并行映射 |
| `try_par_map(iter, f)` | 可失败并行映射 |
| `Pipeline::new()` → `.map()` → `.filter()` → `.collect()` | 融合管道 |
| `StreamPipeline::new(config)` → `.run()` | 流式管道 |
| `CancellationToken` | 协作式取消 |
| `scope(\|s\| ...)` | 非 `'static` 作用域执行 |
| `ComputePool` | 工作窃取线程池 |
| `channel(cap)` / `async_channel(cap)` | MPMC 通道 |

## 性能基准测试

```bash
cargo bench
```

## 测试

```bash
cargo test
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" cargo miri test
```

## 许可证

MIT
