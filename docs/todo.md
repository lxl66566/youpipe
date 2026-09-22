# 性能改进 TODO

来源：代码注释与 docs 中记录的未解决差距 + 已知测量缺口。每项附证据出处与验证方式。
验证统一规范（见 `dev/benchmarks.md`）：

- 结论一律用 `perf/bench-suite` 交错 A/B（同机同 session，median-of-rounds）；
- 100K fused 家族必须隔离交替跑（整组跑有 2–3× 塌缩陷阱）；stream 家族 ±10% 漂移需隔离交替；
- 调度器类改动用**同 binary 运行时旋钮** A/B（重编译对紧基准有 ±30% 纯代码布局噪声）；
- 重尾（heavy-tail）结论必须多 seed（chunk 边界运气主导单 seed 方差）。

优先级：P0 = 高价值/证据充分；P1 = 中等；P2 = 实验/微优化/口径修正。

---

## P0

### 1. 纯同步链的 fused 直通（streaming 引擎最大结构性开销）

- **现状**：`stream(v).stage(f).run()` 语义上 ≡ `pipe(v).map(f).collect()`（1:1
  映射、无 filter），却付出完整流式基础设施：k 条 channel、worker job 提交、
  feeder、seq 打包、collector drain。证据：`mixed_load/youpipe_stream_cpu`
  1K ≈ 317 µs vs rayon 37 µs（`benches/mixed_load.rs`，单 sync stage 纯 CPU）——
  差距几乎全是基础设施，而非引擎。
- **方案**：`StreamPipe::try_exec` 前在类型层面检测「链 = 连续 `SyncStage`，
  无 `expand`/`fence`/`AsyncStage`/`with_cancel`」→ 组合 `g∘f` 直接走
  `par_index_collect`（hybrid dispatch + Slots 零拷贝）。多级连续 sync stage
  一并合成。
- **语义变化（需 docs 同步）**：
  - 失去 stage 间 backpressure（峰值内存 = 输入+输出，不再被 buffer 截断）；
  - unordered 模式输出从完成序变为输入序（"任意序"合同内，但对用户可见）；
  - stage panic 从 abort（worker `AbortIfPanic`）变为向调用者传播（改善，但
    是行为变化）；
  - `StageOptions::workers`/`buffer` pin 在直通下失效——建议显式 pin 时不直通，
    保持用户预期。
- **验证**：`mixed_load`、`stream_pipeline` 全家族（含 with_fence/ordered/cancel
  负例确认不直通）。

### 2. ≥2M 大批量 fused collect 带宽差距归因并收窄

- **现状**：horizontal `cpu_balanced` 1M 打平后，2M rayon 领先 +14%、4M +12%
  （38 vs ~34 GB/s 输出吞吐）。`dev/benchmarks.md` "Reading the results" 明确
  标注 *unattributed*（输出槽位索引 / 派发流量 / 分配器行为均未排除）。
- **方向**：
  1. 先归因再动手：perf counter（cache-misses/cycles/instr-per-elem）+
     `perf record` 对比 rayon 同口径；
  2. 候选实验：叶子输出写非临时 store（≥32 MB R+W 流量、写后不回读，
     NT store 可绕过缓存污染）；chunk 边界缓存行对齐（现为 `n/num_chunks`
     任意切，破坏向量化叶子的对齐前提）；`Slots::uninit` 的 first-touch /
     分配器路径。
- **验证**：`cpu_balanced` 1M/2M/4M 隔离 A/B（horizontal 扩展轴已有）。

---

## P1

### 3. on-pool 调用者的 hybrid dispatch（消除嵌套 fused 的 ramp-up）

- **现状**：`par_index_collect` 等在 `is_on_this_pool()` 为真时退回单树
  （`fused.rs` 各 on_pool 分支），理由是 hybrid 的 `CountLatch` park 会死锁
  同池 worker——但 `CountLatch::with_count` 本就有 **Stealing** 变体
  （等待期间偷任务，`Registry::wait_until_worker`），`hybrid_dispatch` 目前
  只构造 Blocking（传 `None`）。
- **场景**：池 worker 闭包内嵌套 fused 终端（`pool.submit` 任务、stream
  stage 闭包内调 `pipe().collect()`）——单树要付 log2(P) 级 fork/join ramp-up。
- **注意**：Stealing 分支的 `wait_spin_assist` 会忽略 assist hook（reserve
  chunk 兜底失效）——可接受（on-pool 本就不缺算力）或另行改造；需补 loom
  覆盖与 same-pool 回归测试。
- **验证**：新增嵌套 bench（worker 内 collect）+ 全量 fused 家族确认无回归。

### 4. zstd_shape 残余差距与 slack 档位边界

- **现状**：latecomer slack + 两档 tier 后，heavy-tail n=2000 已领先 rayon
  −6.5…−10%，但 capped +1…+4%、uniform +1…+8% 仍落后；wide tier 边界
  `UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK = 64` 是在实测 42（n=2000 差）与
  83（n=4000 好）之间拍的，边界本身未扫描。
- **方向**：
  1. 边界扫描（32/48/64/96，多 seed，同 binary `YOUPIPE_CHUNK_SLACK`）；
  2. capped 形状的残余是重尾 spread（4.2 pt）而非均值——考虑 chunk 内条目
     乱序化或第三档，但注意 cost-EMA 类自适应已两次证伪（见 scheduler.md），
     不要再走运行时成本估计路线。
- **验证**：`zstd_shape` 全形状 × 多 seed + `cpu_unbalanced`（cheap 侧回退
  监控）。

### 5. 终端 collector 通道 in-pipeline A/B：`std sync_channel` vs crossfire mpsc

  2026-10）显示无竞争 1P1C 形状下 `std::sync::mpsc::sync_channel(256)` 比当前
  collector 用的 crossfire mpsc flavor 快 ~17–31 %（41/58 vs 35/44 Melem/s）。
  当初切 MPSC 的依据是 in-pipeline profiling（N 生产者竞争下 recv 侧 CAS 主导，
  见 handoff/channel.rs），微基准无法复现该竞争，两者口径不同、并不矛盾。
- **方向**：在真实 pipeline 里 A/B 把 collector 通道换成 `sync_channel`
  （`SyncSender: Clone` 满足多生产者，`RecvItem` 抽象已就位）。注意 std 阻塞
  send 无自旋窗口、park 策略不同，低深度背压场景可能反而回退。
- **验证**：`stream_pipeline` 全家族 + `mixed_load` 隔离交替 A/B。

### 6. crossfire 阻塞路径的每次 park 40 B `ArcWaker` 分配（2026-10 新发现）

- **现状**：`tests/expand_alloc.rs` 调试期间用 size 直方图定位：饱和/背压下
  streaming 数据面出现大量 size=40 分配。归因：crossfire `blocking_tx/rx` 的
  `o_waker: Option<ArcWaker>` 每次调用从 `None` 起步，spin 失败进入 park 前调
  `ArcWaker::new_blocking()`（`Arc` 头 16 B + `WakerInner` 24 B = 40 B）；
  upstream 把 `cache_waker` 注释掉了（crossfire 3.1.20 `blocking_tx.rs` L146、
  `blocking_rx.rs` L99，`WakerCache` 设施本身还在 `waker.rs` L365）。
- **量化（2026-10 size 直方图，临时 example 复测）**：无竞争快阶段下每次 run
  仅 2-4 次（fast path `try_send` 完全绕过 waker）；背压下爆发且高度调度依赖
  ——同 pipeline 三连 run 分别 17/387/2 次（双峰抖动，这就是无过滤计数测试
  不可复现的原因）；expand fanout-9（8192 入 → 73728 出，2 个 hop）稳态
  1654-2653 次/run，是全 pipeline 第一大分配类（第二名仅 45×32B scratch 增长）。
  极限逼近每 contended send/recv 尝试一次（含 double-check 假唤醒的重试）。
- **影响**：这是背压下的第一分配流量来源，覆盖**所有** stage 间 hop（对比：已关闭的
  expand push-API 只消除 stage 函数内部的 Vec 分配），且调度依赖的双峰计数
  本身就是潜在尾延迟抖动源。
- **方向**：
  1. 先量化：hotpath/计数分配器下测 `stream_pipeline` 全家族的 40 B 分配率；
  2. 修补选项：给 upstream 提 waker 复用 patch（per-Sender/Receiver 缓存——
     `WakerState::Init` 重置语义已有 `reset_init()` 支持）；或 wrapper 层
     （handoff/channel.rs）预暖不现实，得动 crossfire fork（与
     youpipe-concurrent-queue 同策略：fork 到 crates/ 下可 diff 维护）；
  3. 与 #5 联动：若换 `std sync_channel` 做 collector，此问题只影响 stage 间
     MPMC，缩小暴露面。
- **验证**：`expand_heavy` + `stream_pipeline` 隔离交替 A/B + 计数分配器
  size 直方图（40 B 档消失）。
---

## P2

### 7. 池 worker 核绑定（affinity）实验

latecomer 分析（`dev/scheduler.md`）表明 SMT 过下载下 CFS 唤醒延迟
100 µs–1.7 ms 是残余 straggler 的根源，属内核调度行为。可选：per-worker
`pthread_setaffinity` 配置项（默认关闭），在 zstd_shape/unbalanced 上 A/B。
风险：与用户 `taskset` 冲突、跨 NUMA 迁移损失、库越权管理拓扑。

### 8. transient pool 复用缓存

`with_compute_workers(n≠ncpus)` / `with_oversubscribe` 每次终端调用建池
拆池（~ms 级，`ExecPool::Owned`）。可做进程内按尺寸的小 LRU 缓存。
风险：线程数失控（用户以为池已销毁）；至少在 rustdoc 与 tuning.md 把
「紧循环请预建池」的警示提级。

### 9. ReorderBuffer 微优化（仅在有场景时做）

- `Slot` 为 seq + occupied + `MaybeUninit`（u64 项时 24 B/槽）：可把 occupied
  编码进 seq 高位，密度 +33%，大窗口时缓存友好；
- 容量预置条件（同刻 outstanding > capacity 时**静默丢弃**）：当前 clamp
  [1Ki, 1Mi] 对 `buffer_size` 配得极大的场景（buffer > 1Mi）没有防护，
  至少应 debug_assert 或文档标注上界推导。

### 10. （非性能，顺带记录）`ordered()` + `expand()` panic（2026-10 设计分析）

可用 `(seq, sub_seq)` 子序号支持展开保序，解除当前组合禁用。属 API 能力项。
2026-10 深入设计后确认三条硬约束，后续实现前必须先解决：

- **空展开组不可信令（阻断项）**：`(seq, sub, end_of_group)` 方案里 `end` 标志只能
  附着在组内最后一个条目上；`expand` 返回空 `Vec` 时该组**没有任何消息**可携带
  完成信号。后果：collector 从第一个空组起无法推进前缀 flush，退化为 close 时
  `flush_remaining` 全量排序——正确但 (a) 剩余流全量缓冲（内存），(b) 固定容量
  reorder ring 会被静默丢弃（数据丢失）。channel 元组必须携带 T，控制消息需要
  enum 化 payload（下游 stage 全链路加分支）或 side channel（无法穿越 typestate）。
  唯一完好方案是 **expand 后链路改批量 payload**（channel 携带 `(seq, Vec<N>)`，
  空组=空 Vec 消息，天然支持嵌套 expand 保序，ReorderBuffer 直接复用），但
  `ordered` 是运行时 flag、批量与否是编译期类型——需把 `ordered` 提为 typestate
  （`.ordered()` 必须先于 `.expand()` 调用）或双 API。
- **多层 expand 需要完整路径**：两级 expand 的全局序是 `(seq, sub1, sub2)` 字典序，
  3 字段 tag 丢 `sub1`；批量 payload 方案无此问题（嵌套即展平）。
- **泛型约束坑**：`Fn(I, &mut Vec<N>)` 参数位不约束 `N`（E0207），
  需 `ExpandStage<Prev, F, N>` 结构体泛型承载（expand_emit 已这么做了）。

若做批量 payload 方案，注意与 #6（crossfire park 40 B ArcWaker）叠加后每组的
channel hop 数减少，是顺带收益。
---

## 已证伪方向（勿重复尝试）

以下均已实现并测过为负/危险，详细记录见 `dev/scheduler.md`、
`dev/benchmarks.md` 与对应代码注释：

- **driver 偷已注入 chunk** 的三个变体（claim-flag / pop-any / pop-requeue）：
  破坏 injector FIFO（stream 正确性的隐式全局依赖）或 `counter==0 ⇒ JobRef
  全部消费` 不变量（UAF / 全池死锁）。安全形态只有 reserve chunk（现状）。
- **flat 顶层派发**：小中 N 赢、大 N 单注入器争用崩盘；hybrid 是定论。
- **cost-EMA 自适应 chunk 数 / execute-time split-back**：门条件在稳态几乎
  不同时成立（实测 1 次/进程），机制零命中即删。
- **加宽自旋或 yield 窗口**（32/64 之外）、**限制 steal 扫描范围**（有界探测）：
  均为全局回退。
- **去掉 async→async bridge funnel**（consumer 直接 clone 上游 receiver）：
  MPMC 多 waker 唤醒风暴，+0.5…0.8% 稳定回退。
- **async stage 消费端 burst-drain recv**（2026-09）：把逐项 `rx.recv().await`
  改成 anchor + `try_recv` 突发，`io_async_pure/mixed`（200/500/2000，5 轮
  交错 A/B）全部 ±0.5% 纯噪声（sync 侧同形状 −4.9…−13.8%）。机制：crossfire
  `MAsyncRx::recv` 先 `try_recv` 后才注册 waker，非空队列上本就无 waker 往返
  可省。详见 `spawn_async_consumers_body` 的 NOTE(perf)。
- **池缩到物理核数**：zstd SMT 收益 ~1.9×，直接 +78…89% 墙钟。
- **小批量自动串行**：API 诚实性问题（`prefers_serial` 注释），仅保留
  n≤1 / 单线程池的平凡短路。
