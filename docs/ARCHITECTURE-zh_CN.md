# youpipe v0.2.0 — 内部架构设计

本文档面向希望了解 youpipe 内部实现、设计决策及扩展方式的开发者和贡献者。

---

## 1. 设计哲学

### 编译时流水线融合

youpipe 采用**泛型嵌套类型**实现编译时流水线融合——类似迭代器的 `Map<Filter<Iter, F1>, F2>` 模式。当用户链式调用 `.map().filter().map()` 时，不会产生中间 `Vec` 或虚函数分派开销：

```rust
Pipeline::from_vec(items)
    .map(|x: i32| x + 1)      // SyncMap<Identity, F1>
    .filter(|x: &i32| *x > 0) // Filter<SyncMap<Identity, F1>, F2>
    .map(|x: i32| x * 2)      // SyncMap<Filter<...>, F3>
    .collect(items)
```

编译器将所有阶段单态化为一个具体的 `FusedStage::apply()` 调用，零间接开销。

### 数据优先

所有数据通过类型化通道（`crossfire` MPMC）在阶段间流动。每个阶段拥有自己的输入接收器和输出发送器，无共享内存共识对象。

### 非 `'static` 生命周期支持

`scope()` API 基于 `std::thread::scope`，允许闭包借用栈上的局部变量，无需 `'static` 约束。

### 运行时无关

核心模块（`builder/`、`handoff/`、`executor/`、`state/`、`scope/`）不依赖任何异步运行时。`runtime/` 模块定义了 `Runtime` trait，目前实现了 `TokioRuntime`（通过 `tokio-runtime` feature 启用）。

---

## 2. 模块架构

```
src/
├── builder/          # 强类型 Pipeline API + 编译时融合 + StreamPipeline
│   ├── mod.rs        # 公共 re-exports
│   ├── config.rs     # PipelineConfig, Workload 枚举
│   └── typed.rs      # Pipeline<S,T>, par_map(), StreamPipeline, FusedStage, ConsumedBuffer
├── executor/
│   ├── compute/      # 基于 crossbeam-deque 的工作窃取 CPU 线程池
│   │   ├── mod.rs    # ComputePool 单元测试
│   │   └── worker.rs # ComputePool 实现：Injector/Stealer/EventCount 唤醒/优雅关闭/join
│   ├── async_pool/   # Tokio 异步任务池（feature-gated）
│   │   ├── mod.rs
│   │   └── driver.rs # AsyncPool（tokio::runtime::Handle 包装）
│   ├── scheduler.rs  # SchedulerConfig
│   └── mod.rs
├── handoff/          # 数据传递层
│   ├── channel.rs    # MPMC 通道（crossfire 封装：sync + async）
│   ├── ring_buffer.rs # Lock-free SPSC 环形缓冲区（2 的幂，缓存行填充）
│   ├── batcher.rs    # 自动批处理层（SharedRingBuffer + BatchConfig）
│   ├── notify.rs     # EventCount（条件变量通知）、WaitGroup（计数屏障）
│   └── mod.rs
├── state/            # 有序输出 & 流式执行
│   ├── reorder.rs    # ReorderBuffer<T>（最小堆，恢复有序输出）
│   ├── fence.rs      # FenceBarrier<T>（可配置 chunk_size 的分块屏障）
│   ├── stream.rs     # StreamExecutor、run_sync_stage、run_ordered/unordered_collect、feed_items
│   └── mod.rs
├── scope/            # 非 'static 生命周期支持
│   ├── pipeline_scope.rs # scope()、PipelineScope、ScopedPipeline（std::thread::scope）
│   └── mod.rs
├── sync/             # 同步原语
│   ├── cancel.rs     # CancellationToken（Arc<AtomicBool>）
│   ├── sys.rs        # cfg(miri) 抽象：生产用 parking_lot，Miri 用 std::sync
│   └── mod.rs
├── runtime/          # 异步运行时抽象
│   ├── traits.rs     # Runtime trait（spawn / spawn_blocking / block_on）
│   ├── tokio_impl.rs # TokioRuntime 实现
│   └── mod.rs
└── graph/            # 逻辑流水线 DAG 表示（未来扩展）
    ├── node.rs
    ├── edge.rs
    └── mod.rs
```

---

## 3. 核心类型详解

### 3.1 `Workload` — 调度策略提示

```rust
pub enum Workload {
    Balanced,    // 静态范围分配，零原子操作
    Unbalanced,  // 自适应 fetch-add，4× 过度分割
}
```

- `Balanced`：数据被分割为 N 个连续范围（`start = id * base + remainder.min(id)`）。每个 worker 通过 `ptr::read` 迭代其范围。每项零原子操作。
- `Unbalanced`：使用 `ConsumedBuffer` + `AtomicUsize` 步长计数器。Worker 通过 `fetch_add(stride)` 动态领取工作。4× 过度分割确保不均衡负载的良好平衡。

### 3.2 `ConsumedBuffer<T>` — 零拷贝输入缓冲区

```rust
struct ConsumedBuffer<T> {
    ptr: NonNull<T>,
    cap: usize,
}
```

通过 `ManuallyDrop` 包装 `Vec<T>`。Worker 通过 `ptr::read` 消费元素（无克隆、无分配）。Drop 仅释放内存——元素必须被完全消费或显式 drop。

用于 `par_map_fine`、`par_map_adaptive`、`try_par_map` 和 `collect_adaptive`。

### 3.3 `Pipeline<S, T>` — 编译时融合管道

`Pipeline` 是一个带有两个泛型参数的结构体：

- `S`: 阶段链（嵌套的 `SyncMap` / `Filter` / `Fence` / `Ordered` / `Identity`）
- `T`: 当前输出类型

```rust
pub struct Pipeline<S = Identity, T = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<T>,
}
```

**类型转换链**：

| 方法调用 | 类型变化 |
|---------|---------|
| `Pipeline::from_vec(items)` | `Pipeline<Identity, T>` |
| `.map(\|x\| f(x))` | `Pipeline<SyncMap<Identity, F>, O>` |
| `.filter(\|x\| p(x))` | `Pipeline<Filter<SyncMap<...>, F>, T>` |
| `.fence()` | `Pipeline<Fence<...>, T>` |
| `.ordered()` | `Pipeline<Ordered<...>, T>` |

`collect()` 方法要求 `S: FusedStage<T> + Send + Clone + 'static`，此时阶段被克隆到每个 worker 上执行。

### 3.4 `FusedStage` trait — 零分派执行

```rust
pub trait FusedStage<T> {
    type Output;
    fn apply(&self, item: T) -> Option<Self::Output>;
}
```

- `SyncMap::apply()` → `self.prev.apply(item).map(|v| (self.f)(v))`
- `Filter::apply()` → `self.prev.apply(item).filter(|v| (self.f)(v))`
- `Fence::apply()` → 透传（`fence` 语义由 `StreamPipeline` 执行时处理）
- `Ordered::apply()` → 透传（有序由 `ReorderBuffer` 处理）

返回 `Option` 允许 `filter` 语义自然融入融合链。

### 3.5 `par_map()` — 便捷并行映射

```rust
pub fn par_map<I, F, R>(iter: I, f: F) -> Vec<R>
pub fn par_map_with_workload<I, F, R>(iter: I, f: F, workload: Workload) -> Vec<R>
pub fn par_chunks_map<I, F, R>(iter: I, chunk_size: usize, f: F) -> Vec<R>
pub fn try_par_map<I, F, R, E>(iter: I, f: F) -> Result<Vec<R>, E>
```

- `par_map`：委托给 `par_map_with_workload(..., Balanced)`
- `par_map_with_workload`：根据 Workload 分派到 `par_map_fine`（ConsumedBuffer + 静态范围）或 `par_map_adaptive`（ConsumedBuffer + fetch-add）
- `par_chunks_map`：分割为 worker 范围，每个再细分为 chunk_size 子块。保留 LLVM SIMD 自动向量化。
- `try_par_map`：`AtomicBool` 错误标志 + `Mutex<Option<E>>` 错误槽。Worker 逐项检查标志；首个错误被存储，剩余元素被 drop。使用 ConsumedBuffer + 静态范围。

### 3.6 `StreamPipeline` — 流式多阶段管道

当管道包含 `fence`、`ordered` 等语义时，需要运行时通道连接，使用 `StreamPipeline`：

| 方法 | 说明 |
|------|------|
| `run()` | 单阶段流式执行 |
| `run_multi_stage()` | 双阶段流水线（stage1 → channel → stage2） |
| `run_with_fence()` | 屏障模式：stage1 全部完成 → fence 线程分块转发 → stage2 处理 |
| `run_nested()` | 展开模式：outer_stage 1:N 展开 → inner_stage 并行处理 |

所有流式方法支持 `ordered: bool` 参数，通过 `ReorderBuffer` 恢复原始顺序。可选的 `CancellationToken` 支持协作式取消——feeder 和 worker 每次迭代检查 `is_cancelled()`。

### 3.7 `ScopedPipeline` — 非 `'static` 管道

```rust
youpipe::scope(|s| {
    let factor = 10;
    s.pipeline(items)
        .map(|x: i32| x * factor)   // 借用了栈上 factor
        .collect()
})
```

内部使用 `std::thread::scope` 执行，闭包类型为 `Box<dyn FnOnce() + Send + 'env>`。

---

## 4. ComputePool — 工作窃取线程池

### 架构

```
Injector（全局队列）
    ↓ steal
Worker₀ ←→ Stealer₀
Worker₁ ←→ Stealer₁
Worker₂ ←→ Stealer₂
Worker₃ ←→ Stealer₃
```

- 基于 `crossbeam-deque`：每个 worker 有本地 FIFO deque，其他 worker 通过 `Stealer` 窃取
- 全局 `Injector` 接收外部提交的任务
- `EventCount`（条件变量）唤醒空闲 worker

### 任务提交流程

1. `pool.submit(job)` → `injector.push(Box::new(job))`
2. 若 `idle_count > 0` → `event.notify_one()` 唤醒一个 worker
3. Worker 被唤醒 → `find_work()` 按优先级搜索

### 工作搜索策略

```
find_work():
    1. local.pop()           → 命中则返回
    2. injector.steal()      → 命中则返回
    3. peer stealers         → 命中则返回
    4. cpu_since_yield++
       若 > MAX_SPINS(64):
           yield_now()
           重置计数
```

### 优雅关闭

`Drop` 实现：设置 `shutdown` 标志 → `event.notify()` 唤醒所有 worker → `drain_remaining()` 处理剩余任务 → join 所有线程。

---

## 5. 数据传递层 (`handoff/`)

### 5.1 MPMC 通道 (`channel.rs`)

基于 `crossfire` 封装，提供统一 API：

| 类型 | 实现 |
|------|------|
| `SyncSender<T>` / `SyncReceiver<T>` | `crossfire::mpmc::bounded_blocking` |
| `AsyncSender<T>` / `AsyncReceiver<T>` | `crossfire::mpmc::bounded_async` |

额外添加 `closed: Arc<AtomicBool>` 标志，支持早期终止而不与 crossfire 内部断开检测产生竞态。

### 5.2 SPSC 环形缓冲区 (`ring_buffer.rs`)

Lock-free SPSC 环形缓冲区，特性：

- 容量必须是 2 的幂（掩码运算代替取模）
- `CachePadded<AtomicUsize>` 分离 head/tail 到不同缓存行，避免伪共享
- `push_batch()` / `pop_batch()` 支持批量操作
- `Drop` 实现正确析构未消费的元素

### 5.3 EventCount (`notify.rs`)

条件变量封装，用于 ComputePool 的 worker 唤醒：

- `notify()` / `notify_one()` → 递增 state + Condvar 唤醒
- `wait()` → 记住当前 state key，Condvar 等待直到 key 变化
- `cfg(miri)` 分支使用 `std::sync` 而非 `parking_lot`

### 5.4 WaitGroup (`notify.rs`)

计数屏障：`add(n)` 增加、`done()` 递减、`wait()` 阻塞直到归零。当计数从 1→0 时 Condvar 广播唤醒。

---

## 6. 有序输出 (`state/reorder.rs`)

`ReorderBuffer<T>` 使用最小堆恢复并行处理后元素的原始顺序：

1. 每个元素发送时附带序列号 `(seq, item)`
2. `insert(seq, item)` 压入最小堆
3. `flush_ready()` 弹出堆顶连续的元素（`next_expected` 起始）
4. `flush_remaining()` 处理末尾剩余（排序后返回）

---

## 7. Fence 屏障 (`state/fence.rs` + `StreamPipeline::run_with_fence`)

Fence 确保前阶段全部完成后才开始后阶段：

1. Stage1 workers 从 `in_rx` 拉取 → 处理 → 发送到 `mid_tx`
2. `WaitGroup` 跟踪 stage1 workers 完成
3. Fence 线程等待 `wg1.wait()` → 从 `mid_rx` 读取 → 按 `chunk_size` 分块 → 转发到 `fenced_tx`
4. Stage2 workers 从 `fenced_rx` 拉取 → 处理 → 发送到 `out_tx`

`FenceBarrier<T>` 提供分块聚合：当 buffer 达到 `chunk_size` 时自动 flush。

---

## 8. Miri 兼容性

`sync::sys` 模块通过 `cfg(miri)` 提供统一 API：

| 环境 | Mutex | Condvar |
|------|-------|---------|
| 生产 | `parking_lot::Mutex` | `parking_lot::Condvar` |
| Miri | `std::sync::Mutex`（`Option<MutexGuard>` 包装） | `std::sync::Condvar` |

`parking_lot` 使用 Windows FFI（`GetModuleHandleA`），Miri 无法处理。统一 API 使两种实现对调用方透明。

---

## 9. 性能基准

### CPU 密集型 par_map vs rayon

| 数据量 | youpipe | rayon | 结果 |
|--------|---------|-------|------|
| 1K | ~130µs | ~130µs | 持平 |
| 10K | ~1.1ms | ~1.1ms | 持平 |
| 100K | ~10.6ms | ~10.6ms | 持平 |

### 流水线融合（3 阶段）vs rayon 链

| 数据量 | youpipe 融合 | rayon 链 | 结果 |
|--------|-------------|---------|------|
| 10K | 3.2µs | 10.6µs | **youpipe 快 3.3 倍** |
| 100K | 32µs | 105µs | **youpipe 快 3.3 倍** |

### 混合负载 vs tokio::spawn_blocking

| 数据量 | youpipe stream | spawn_blocking | 结果 |
|--------|---------------|----------------|------|
| 100 | 1.29ms | 1.42ms | 快 1.1 倍 |
| 500 | 2.32ms | 4.58ms | **快 2.0 倍** |
| 1000 | 3.07ms | 8.93ms | **快 2.9 倍** |

### 通道吞吐量

| 数据量 | crossfire | crossbeam-channel | 结果 |
|--------|-----------|-------------------|------|
| 100K | 83.1 Melem/s | 63.6 Melem/s | **快 1.3 倍** |

---

## 10. 扩展系统

### 添加新的融合阶段

1. 定义阶段结构体，实现 `StageMarker<T>` 和 `FusedStage<T>`
2. 在 `Pipeline<S, T>` 上添加 builder 方法，返回 `Pipeline<NewStage<S, ...>, O>`
3. `FusedStage::apply()` 中组合 `self.prev.apply(item)` 与新逻辑

### 添加新的运行时

实现 `Runtime` trait：

```rust
pub trait Runtime: Send + Sync + 'static {
    fn spawn<F>(&self, future: F) where F: Future<Output = ()> + Send + 'static;
    fn spawn_blocking<F, R>(&self, f: F) -> Pin<Box<dyn Future<Output = R> + Send + 'static>>
    where F: FnOnce() -> R + Send + 'static, R: Send + 'static;
    fn block_on<F>(&self, future: F) -> F::Output where F: Future;
}
```

在 `Cargo.toml` 中添加 feature flag，在 `runtime/` 下添加实现模块。
