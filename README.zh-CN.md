# youpipe

[English](./README.md) | 简体中文

youpipe 是一个高性能、数据优先、支持混合 CPU 负载与流式异步 IO 的并行 pipeline。数据从入口传入，各阶段自然串联，最终通过一次终端调用
（`.collect()` / `.run()`）执行完整链。两种 pipeline 引擎覆盖不同场景：

- `Pipe` — 编译期融合的 CPU 链。`.map().filter().map()` 编译为每个工作线程上单一
  的单态化闭包，不产生任何中间分配。
- `StreamPipe` — 基于通道的流式处理，覆盖融合无法处理的场景：异步 IO、Cancellation、fence、
  一对多展开等。

工作窃取调度器采用 rayon 风格的 `st3` LIFO 双端队列 + 紧凑原子计数器，兼顾均衡与不
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

fused `pipe()` —— CPU 密集型操作（每元素 100 次迭代，热输入）：

| 规模 | youpipe | rayon  |
| ---- | ------- | ------ |
| 1K   | 62 µs   | 37 µs  |
| 10K  | 64 µs   | 70 µs  |
| 100K | 105 µs  | 145 µs |

fused `pipe()` —— 轻量操作 `x+1`（热输入）：

| 规模 | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 63 µs   | 67 µs  |
| 100K | 79 µs   | 104 µs |
| 1M   | 516 µs  | 265 µs |

fused `pipe()` —— 3 轮轻量操作 (`x+1`, `x*3`, `x-2`)：

| 规模 | youpipe | rayon  |
| ---- | ------- | ------ |
| 10K  | 61 µs   | 67 µs  |
| 100K | 82 µs   | 101 µs |

fused `try_map().try_collect()` —— fallible `Result` 链（热输入）：

| 规模 | youpipe | rayon |
| ---- | ------- | ----- |
| 10K  | 64 µs   | 66 µs |
| 100K | 85 µs   | 98 µs |

流式 `stream()` —— 单个同步阶段（`cpu_work`，每元素 100 次迭代）：

| 规模 | youpipe | tokio spawn_blocking |
| ---- | ------- | -------------------- |
| 1K   | 0.72 ms | 2.46 ms              |
| 10K  | 8.8 ms  | 23.5 ms              |
| 100K | 88.6 ms | 236 ms               |

纯异步 IO（`tokio::time::sleep`，~1 ms 延迟，90/10 尾部，500 项）：

| 拓扑                                | 耗时    |
| ----------------------------------- | ------- |
| youpipe：异步 IO（`.stage_async`）  | 9.65 ms |
| tokio：原生异步                     | 9.30 ms |
| youpipe：阻塞 IO（`.stage`）        | 33.1 ms |
| youpipe：阻塞 IO（过订阅 512 线程） | 19.5 ms |
| tokio：spawn_blocking               | 8.83 ms |

CPU + IO 混合（两阶段，500 项）：

| 拓扑                        | 耗时    |
| --------------------------- | ------- |
| youpipe：同步 CPU + 异步 IO | 9.97 ms |
| tokio：混合 spawn_blocking  | 10.1 ms |
| youpipe：同步 CPU + 阻塞 IO | 60.0 ms |

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

// 为阻塞 IO 同步阶段过订阅计算线程池
let pool = ComputePool::new(512);
let r = (0..1000).stream()
    .with_compute_pool(pool)
    .stage(|x| blocking_io(x))
    .run();
```

`io_concurrency` 是 M:N 乘数——异步任务在等待时会放弃 OS 线程，因此该值可以远大于
`async_workers`（线程数量）。限制此值以控制内存上限。

`.fence(mode)` 作用于一个相邻阶段边界。`FenceMode::Barrier` 让上游完全排空后下游
才开始；`FenceMode::Chunked(k)` 每凑齐 `k` 个元素就立即释放（混合 CPU/IO 的推荐
默认）。`.run()` 默认按完成顺序返回结果；追加 `.ordered()` 通过 `ReorderBuffer`
恢复输入顺序。

## 工作原理

见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。

## 第三方声明

`src/pool/` 中的工作窃取调度器改编自
[rayon-core](https://github.com/rayon-rs/rayon)。
