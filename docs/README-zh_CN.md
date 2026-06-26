# youpipe

高性能 Rust 并发流水线批处理框架，支持编译时融合。

## 特性

- **数据优先 API** — `pipe(items).map().filter().collect()`；数据在最前面进入，而不是最后
- **编译时融合** — `.map().filter().map()` 编译为每个 worker 的单次闭包调用，零中间分配
- **负载提示** — `.with_workload(Workload::Balanced)`（零原子操作）或 `Workload::Unbalanced`（自适应 fetch-add）
- **工作窃取线程池** — 基于 `st3` 的无锁 LIFO deque 调度，EventCount 唤醒
- **流式管道** — `stream(items).stage().stage_async()`，阶段间以通道连接，支持有序/无序输出
- **异步 IO 阶段** — `.stage_async()` 在 tokio 运行时上实现 M:N IO 并发
- **可失败链式** — `.try_map()` / `.try_collect()`，首次错误即终止
- **取消支持** — `.with_cancel(token)` 协作式 StreamPipe 关闭
- **作用域执行** — `scope()` 支持非 `'static` 闭包，可借用栈上数据
- **一对多展开** — `.expand()` 实现 flatMap 式阶段

## 快速入门

```toml
[dependencies]
youpipe = "0.2"
```

### 融合管道（CPU 密集）

```rust
use youpipe::pipe;

// 数据优先：数据从前端进入，阶段链式组合，`.collect()` 执行。
let result: Vec<i32> = pipe(0..1000)
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x| x * 10)
    .collect();

// 不均衡负载 → 更细粒度的任务窃取。
use youpipe::Workload;
let r: Vec<i32> = pipe(0..1000)
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();
```

### 可失败链式

```rust
use youpipe::pipe;

// 自由穿插 `.try_map()` 与 `.map()`；`.try_collect()` 首个 `Err` 即短路。
let result: Result<Vec<String>, &str> = pipe(0..100)
    .try_map(|x: i32| if x == 50 { Err("bad") } else { Ok(x * 2) })
    .map(|x| format!("{x}"))
    .try_collect();
```

### 流式管道（阶段间通道）

```rust
use youpipe::stream;

// 阶段由无锁通道连接；输出按完成顺序到达。
// 加 `.ordered()` 通过 ReorderBuffer 还原输入顺序。
let result = stream(0..1000)
    .stage(|x: i32| x + 1)
    .stage(|x: i32| x * 2)
    .ordered()
    .run();
```

### 异步 IO 阶段（同步 CPU + 异步 IO 混合）

`.stage_async()` 在 tokio 运行时上以 `io_concurrency` 个任务运行异步阶段
（让出式 IO 的 M:N 并发 —— 网络/磁盘、`tokio::time::sleep`）。
通过 `.with_async_pool(...)` 附加运行时以跨多次运行复用。

```rust
use youpipe::{stream, AsyncPool, PipelineConfig};

let pool = AsyncPool::from_global(8).unwrap();
let r = stream(vec![1u64, 2, 3])
    .with_config(PipelineConfig::default().with_io_concurrency(256))
    .with_async_pool(pool)
    .stage(|x: u64| x + 1)                       // 计算池上的同步 CPU
    .stage_async(|m: u64| async move { m * 2 })  // 运行时上的异步 IO（M:N）
    .run();
```

### 作用域管道（非 `'static` 闭包）

```rust
use youpipe::scope;

let factor: usize = 7;
let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
// 从每个 worker 借用 `factor` 和 `&table` —— 无需 clone，无需 Arc。
let result: Vec<usize> = scope(|s| {
    s.pipe(0..table.len())
        .map(|i: usize| table[i].len() * factor)
        .collect()
});
```

## API 一览

| 函数 / 类型 | 说明 |
|---|---|
| `pipe(iter)` → `.map()` → `.filter()` → `.collect()` | 数据优先融合 CPU 管道 |
| `pipe(iter).try_map().map().try_collect()` | 可失败融合链（短路） |
| `.with_workload(Workload)` / `.with_config(config)` | 调节过分割 / 配置 |
| `stream(iter)` → `.stage()` → `.expand()` → `.fence()` → `.run()` | 流式管道（阶段间通道） |
| `.stage_async(fut)` | tokio 运行时上的异步 IO 阶段（M:N） |
| `.ordered()` | 通过 `ReorderBuffer` 还原输入顺序 |
| `.with_cancel(token)` / `.with_async_pool(pool)` | 取消 / 运行时复用 |
| `scope(\|s\| s.pipe(iter)…)` | 非 `'static` 作用域融合管道 |
| `CancellationToken` | 协作式取消 |
| `ComputePool` | 工作窃取线程池 |
| `channel(cap)` / `async_channel(cap)` | MPMC 通道 |

## 性能基准测试

```bash
cargo bench --bench channel_bench    # 通道吞吐
cargo bench --bench sync_vs_rayon    # CPU 密集、融合、轻量
cargo bench --bench unbalanced       # 不均衡负载
cargo bench --bench mixed_load       # 混合 CPU/IO（阻塞）
cargo bench --bench io_async         # 异步 IO（纯 + 混合同步+异步）
cargo bench --bench async_vs_tokio   # Stream vs tokio spawn_blocking
```

## 测试

```bash
cargo test
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" cargo miri test
```

## 许可证

MIT
