# youpipe

[English](./README.md) | 简体中文

youpipe 是一个高性能、数据优先、支持混合 CPU 负载与流式异步 IO 的并行 pipeline。数据从入口传入，各阶段自然串联，最终通过一次终端调用
（`.collect()` / `.run()`）执行完整链。两种 pipeline 引擎覆盖不同场景：

- `Pipe` — 编译期融合的 CPU 链。`.map().filter().map()` 编译为每个工作线程上单一
  的单态化闭包，不产生任何中间分配。
- `StreamPipe` — 基于通道的流式处理，覆盖融合无法处理的场景：异步 IO、Cancellation、fence、
  一对多展开等。

工作窃取调度器采用 rayon 风格的 `st3` LIFO 双端队列 + `EventCount`，兼顾均衡与不
均衡负载。`scope()` 支持借用栈上局部数据的非 `'static` 闭包。

使用：`cargo add youpipe`。

## API

`pipe(items)` / `items.pipe()` 产生完全相同的类型，任意选择均可。

```rust
use youpipe::pipe;
let r: Vec<i32> = pipe(0..1000).map(|x| x + 1).collect();
// same as
use youpipe::prelude::*;
let r: Vec<i32> = (0..1000).pipe().map(|x| x + 1).collect();
```

按负载选择入口：

| 负载                      | 入口                                                 |
| ------------------------- | ---------------------------------------------------- |
| 纯 CPU map/filter         | `pipe(items)`                                        |
| 异步 IO、同步+异步混合    | `stream(items).stage_async(...)`                     |
| 非均衡的 CPU 负载         | `pipe(items).with_workload(Unbalanced)`              |
| Cancellation、fence、展开 | `stream(items).with_cancel(..).fence(..).expand(..)` |
| 借用栈上局部数据          | `scope(\|s\| s.pipe(..)....)`                        |

总工作量低于 ~10 µs 或单操作低于 ~100 ns 时，不建议使用 youpipe，并行设置开销无法收回成本。此时使用顺序 `iter().map().collect()` 更快。

## 示例

youpipe **不会**在某一阶段全部完成后，再进入下一阶段。如果需要严格的阶段隔离，需要在 stage 之间使用 fence。

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

// 同步 CPU 阶段 + 异步 IO 阶段（在各自线程池上重叠运行）
let r: Vec<u64> = (0..1000).stream()
    .stage(|x: u64| x + 1)
    .stage_async(|x: u64| async move { fetch(x).await })
    .run();

// fence：在两个相邻阶段间每 64 个元素批处理一次
let r: Vec<i32> = (0..1000).stream()
    .stage(|x: i32| x + 1)
    .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
    .stage(|x: i32| x * 2)
    .run();

// scope 借用局部 `factor` 和 `table`，无需 clone
let factor = 7;
let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
let r: Vec<usize> = scope(|s| {
    s.pipe(0..table.len()).map(|i: usize| table[i].len() * factor).collect()
});
```

## 性能

7945HX 32-Core Linux，详见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#9-performance-benchmarks)。

fused `pipe()` —— CPU 密集型操作（每元素 100 次迭代）：

| 规模 | youpipe | rayon  |
| ---- | ------- | ------ |
| 1K   | 72 µs   | 38 µs  |
| 10K  | 133 µs  | 90 µs  |
| 100K | 366 µs  | 313 µs |

fused `pipe()` —— 轻量操作 `x+1`：

| 规模 | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 120 µs  | 66 µs  |
| 100K | 142 µs  | 114 µs |
| 1M   | 739 µs  | 291 µs |

流式 `stream()` —— 单个同步阶段（`cpu_work`，每元素 100 次迭代）：

| 规模 | youpipe | tokio spawn_blocking |
| ---- | ------- | -------------------- |
| 1K   | 801 µs  | 2.45 ms              |
| 10K  | 7.73 ms | 23.2 ms              |

异步 IO（`tokio::time::sleep`，~1 ms 延迟，`io_concurrency = 512`），500 项：

| 拓扑                                      | 耗时    |
| ----------------------------------------- | ------- |
| youpipe：纯异步 IO                        | 9.82 ms |
| tokio：原生异步                           | 9.33 ms |
| youpipe：同步 CPU + 异步 IO 混合          | 9.93 ms |
| tokio：混合 spawn_blocking                | 10.1 ms |
| youpipe：同步 CPU + 阻塞 IO（计算线程池） | 60.0 ms |

## 深入用法

默认值：`compute_workers = async_workers = available_parallelism`、
`io_concurrency = 128`、`buffer_size = 256`、`Workload::Balanced`。tokio 运行时在
首次 `.run()` 时延迟构建，并在该次运行内复用；传入 `AsyncPool` 可跨运行共享。

```rust
use youpipe::prelude::*;

// 不均衡：约 10% 慢项，成本差距 1000 倍 → 提高过度拆分因子
let r: Vec<_> = (0..5_000).pipe()
    .with_workload(Workload::Unbalanced)
    .map(|x| expensive(x))
    .collect();

// 调优配置 + 复用运行时
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

// 取消
let token = CancellationToken::new();
let r = (0..10_000).stream()
    .with_cancel(token.clone())
    .stage(|x| expensive(x))
    .run();
```

`io_concurrency` 是 M:N 乘数——异步任务在等待时会放弃 OS 线程，因此该值可以远大于
`async_workers`（线程数量）。限制此值以控制内存上限。

`.fence(mode)` 作用于一个相邻阶段边界。`FenceMode::Barrier` 让上游完全排空后下游
才开始；`FenceMode::Chunked(k)` 每凑齐 `k` 个元素就立即释放（混合 CPU/IO 的推荐
默认）。`.run()` 默认按完成顺序返回结果；追加 `.ordered()` 通过 `ReorderBuffer`
恢复输入顺序。

## 工作原理

`Pipe` 组合一条编译期类型状态链，编译器将其单态化为每个工作线程上的单一闭包——无
`dyn`、无阶段级 `Vec`。融合热路径一次性分配输入/输出缓冲区，并在索引范围 `[0, n)`
上递归，每个叶子接收 `&[T]` / `&mut [R]` 切片视图，使叶子循环保持无分支、可向量化。

`StreamPipe` 在 `.run()` 时遍历链，通过通道为每个阶段生成工作线程。同步阶段运行在
`ComputePool` 上；异步阶段在 `async_workers` 个 OS 线程上复用 `io_concurrency` 个
tokio 任务。完整设计原理、模块详解与 panic 安全性讨论见
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。
