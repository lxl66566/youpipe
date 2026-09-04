# youpipe

[English](https://github.com/lxl66566/youpipe/blob/main/README.md) | 简体中文

youpipe 是一个高性能、数据优先、支持混合 CPU 负载与流式异步 IO 的并行 pipeline。
数据从入口传入，各阶段自然串联，最终通过一次终端调用（`.collect()` / `.run()`）
执行完整链。两种 pipeline 引擎覆盖不同场景：

- `Pipe` — 编译期融合的 CPU 链。`.map().filter().map()` 编译为每个工作线程上
  单一的单态化闭包，不产生任何中间分配。
- `StreamPipe` — 基于通道的流式处理，覆盖融合无法处理的场景：异步 IO、
  Cancellation、fence、一对多展开等。

工作窃取调度器采用 rayon 风格的 `st3` LIFO 双端队列 + 紧凑原子休眠计数器，
兼顾均衡与不均衡负载。`scope()` 支持借用栈上局部数据的非 `'static` 闭包。

使用：`cargo add youpipe`。

## 快速上手

`pipe(items)` / `items.pipe()` 产生完全相同的类型，任意选择均可。

```rust
use youpipe::prelude::*;

// 融合 CPU 链：每个 worker 上一个单态化闭包
let r: Vec<i32> = (0..1000).pipe()
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x| x * 10)
    .collect();

// 同步 CPU 阶段 + 异步 IO 阶段（在各自线程池上重叠运行）
let r: Vec<u64> = (0..1000).stream()
    .stage(|x: u64| x + 1)
    .stage_async(|x: u64| async move { fetch(x).await })
    .run();
```

按负载选择引擎与调优旋钮——见[选择合适的引擎](https://lxl66566.github.io/youpipe/advanced/choosing-engine.html)。

## 文档

完整手册见 **[lxl66566.github.io/youpipe](https://lxl66566.github.io/youpipe/)**：

- [用户指南](https://lxl66566.github.io/youpipe/guide/getting-started.html) ——
  融合、流式、借用与 fallible pipeline
- [性能调优](https://lxl66566.github.io/youpipe/advanced/choosing-engine.html) ——
  引擎选择、workload 提示、线程池、逐阶段旋钮
- [Benchmark 与方法论](https://lxl66566.github.io/youpipe/dev/benchmarks.html) ——
  横向库对比及其测量方法
- [开发者指南](https://lxl66566.github.io/youpipe/dev/design.html) ——
  调度器、通道、验证（miri/loom）

源码位于 `docs/`（`mdbook build docs` 本地构建）。

## 性能

横向库对比（youpipe vs rayon vs tokio vs `futures::stream` vs 手写
`std::thread` pipeline），覆盖七类负载（均衡/倾斜 CPU、异步/阻塞 IO、混合
sync+async，以及两个真实的三阶段 pipeline，含本地 mock server 上的 HTTP）：
youpipe 在中等规模 CPU 区间领先（100K 时比 rayon 快 23%），倾斜负载靠
`Workload::Unbalanced` 胜出，真实 sync+async pipeline 上比手写 tokio channel
链最多快 23%。32 核 AMD (Zen) Linux，固定 31 核，5 个交叠轮次取中位数。
模拟 IO 全部为 sleep，不碰磁盘。方法论与完整数据见
[Benchmark 与方法论](https://lxl66566.github.io/youpipe/dev/benchmarks.html)。

<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-cpu.svg" alt="CPU pipelines: youpipe vs rayon vs hand-written std threads">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-io.svg" alt="IO pipelines: youpipe vs tokio vs futures">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-real.svg" alt="Mixed sync + async pipelines: youpipe vs tokio vs futures vs rayon">
</p>

总工作量低于 ~10 µs 或单操作低于 ~100 ns 时，不建议使用 youpipe，并行设置
开销无法收回成本。此时使用顺序 `iter().map().collect()` 更快。

## 第三方声明

`crates/youpipe/src/pool/` 中的工作窃取调度器改编自
[rayon-core](https://github.com/rayon-rs/rayon)。
