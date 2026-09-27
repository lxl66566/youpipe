# TODO

范围：**未落地**的工作项。已关闭需求的落地记录与实测读数保存在 docs/src/dev/
对应主题文档（scheduler.md / benchmarks.md / streaming.md / core-types.md）
与代码注释里，勿凭记忆重做；已证伪方向见 [dev/dead-ends.md](src/dev/dead-ends.md)。

验证统一规范（见 `dev/benchmarks.md`）：

- 结论一律用 `perf/bench-suite` 交错 A/B（同机同 session，median-of-rounds）；
- 100K fused 家族必须隔离交替跑（整组跑有 2–3× 塌缩陷阱）；stream 家族 ±10% 漂移需隔离交替；
- 调度器类改动用**同 binary 运行时旋钮** A/B（`bench_ab.sh -E`，重编译对紧基准有 ±30% 纯代码布局噪声，2026-07 在未改动的 rayon 锚点上实测 +31%）；
- 重尾（heavy-tail）结论必须多 seed（chunk 边界运气主导单 seed 方差）。

优先级：P0 = 高价值/证据充分；P1 = 中等；P2 = 实验/微优化/口径修正。
代码位置以符号名为主（行号随主分支漂移）。

---

## 性能

### 1. [P1] zstd_shape 残余差距（capped/uniform 落后）

- 现状：wide-tier 边界 64→48 已落地（scheduler.md "Wide-tier boundary
  scan"，heavy-tail n=3000 vs-rayon 收至 +2.5%）；opt-in
  `ComputePool::new_pinned` 已从唤醒落核侧收回大部分残差（capped
  −6.1…−6.9%、uniform −4.0…−4.6%，scheduler.md "Worker affinity"）。残余：
  capped +1…+2%、uniform +4…+6%、heavy-tail n=2000（narrow）~+1% 仍落后
  rayon；默认路径（无 pinning）残余需按 pinning 闸下口径重估。
- 方向：capped 形状的残余是重尾 spread（4.2 pt）而非均值——考虑 chunk 内
  条目乱序化或第三档；cost-EMA 类自适应已两次证伪（dead-ends.md），勿再走
  运行时成本估计路线。
- 验证：`zstd_shape` 全形状 × 多 seed（`ZSTD_SEEDS`/`ZSTD_SHAPE_NS` 网格
  透传）；cheap 侧档位经算术核对不随边界变化（cpu_unbalanced n=200/5000
  均未跨 48）。

### 2. [P1] fused 批间泊车/唤醒占用亏损（默认路径残余）

- 现状：归因与收束记录见 benchmarks.md（"Attributing the 2M/4M
  fused-collect gap"、"NT-store attribution"）：空闲工人泊车 + futex 唤醒
  + 落冷核 vs rayon 全程自旋保温。NT-store auto（~13 pt）与 opt-in
  pinning（cpu_balanced 1M/2M/4M −5.1…−7.2%）已落地；新口径（2026-09-26，
  7 轮交错）cpu_balanced 1M +0.6%、2M +1.5%、4M −6.8%（反超），readback
  口径领先 24–29%。**默认路径残余 ≈1.5 pt @2M**。
- 方向：尾部 straggler 细化——末段更细粒度 oversplit（动态，非 cost-EMA
  路线，残余价值有限，新口径下 2M 仅 +1.5%）；勿拉长全局自旋窗口、勿做
  时间窗/静默门控保温（均证伪，见 dead-ends.md）。
- 验证：`cpu_balanced` 1M/2M/4M 隔离 A/B + `horizontal-counters`（youpipe-bench）
  复查 task-clock / ctx-switch / migration。

### 3. [P1] reduction 终端（reduce/fold/sum/count）

- 现状：六个 builder（Pipe/TryPipe/PipeRef/TryPipeRef/ScopedPipe/
  ScopedTryPipe）只有 collect / try_collect / for_each 三类终端；聚合需求
  必须物化整棵 `Vec<O>`（n 槽 Slots 分配 + n 次写 + 串行 fold）——对
  `.map(f).sum()` 形状是纯浪费，正是 `for_each` 规避的结构问题（见
  fused.rs for_each 的结构性优势注释）。
- 方向：`ReduceStrategy: HybridStrategy`（`cleanup_success_chunk` = no-op）
  + `par_reduce_rec`（无输出 buffer，叶内部分聚合 + 树形 combine）；
  HybridStrategy 抽象使派发半边免费，只需叶/combine 逻辑。API：
  `reduce`/`fold` + 便捷 `sum`/`count`/`min`/`max`；stream 纯 sync 链的
  fuse 路径同享。与 #13 的 par_tree_rec 抽象配套做可省一遍树形样板。

### 4. [P2] streaming 相邻 sync stage 融合

整链 fuse 仅在纯 sync、无 pin、无 cancel 时触发（`fuse_exec` 谱系）；
`stream(..).stage(a).stage(b)` 是两个 worker 种群 + 每 item 一次 channel
hop，而等价 fused 链单遍完成（`FuseCompose` 已具备闭包复合能力）。可在
spawn 链游走时对相邻 unpinned `SyncStage` 合并成单 worker 池，任一侧 pin
`workers`/`buffer` 即放弃。投机项：先 bench 后落地。

### 5. [P2] fence 的每 run 专线程

每个 `FenceLink` 变体 `std::thread::spawn`（stream.rs 五处，~30–80 µs/
fence/run，feeder 侧同款成本曾被专门移除），且 fence 是唯一没有
StageOptions 的链类型（buffer 硬编码为 `ctx.buffer_size(parallelism)`）。
pool 模式下可作为 leased pool job（parks 在 mid channel 上，与 stage
worker 同形，计入 parking lease），dedicated 模式保留线程；顺带提供
`fence_with(StageOptions, mode)`。

### 6. [P2] filter 高选择率：write-then-compact 单遍 stage 变体

- count-then-place（已落地 opt-in `YOUPIPE_FILTER_CTP`）高选择率 −31…−50 %
  @100K，但 stage 跑两遍、10K 与低选择率回退（见 benchmarks.md filter 小节）。
- write-then-compact：叶子把存活项按输入下标直写 n 槽输出 buffer 并记录
  per-leaf 存活数；compaction 遍按 scan 偏移整块拷贝各叶连续存活段——
  stage 只跑一遍、每存活项恰好一次拷贝（偏移相等时可跳过），代价是 n 尺寸
  （而非 total 尺寸）输出 buffer。有望全形状占优或接近，需 A/B 定夺。

## 用户 API

### 8. [P1] `ordered()` + `expand()`：批量 payload 方案

2026-10 设计分析确认三条硬约束，实现前必须先解决（详见 dev/core-types.md
对应记录）：

- **空展开组不可信令（阻断项）**：`(seq, sub, end_of_group)` 方案里 `end`
  标志只能附着在组内最后一个条目上；`expand` 返回空 `Vec` 时该组没有任何
  消息可携带完成信号——collector 从第一个空组起无法推进前缀 flush，退化为
  close 时全量排序：剩余流全量缓冲（内存）+ 固定容量 reorder ring 静默丢弃
  （数据丢失）。唯一完好方案是 **expand 后链路改批量 payload**（channel 携带
  `(seq, Vec<N>)`，空组=空 Vec 消息，天然支持嵌套 expand 保序，ReorderBuffer
  直接复用），但 `ordered` 是运行时 flag、批量与否是编译期类型——需把
  `ordered` 提为 typestate（`.ordered()` 必须先于 `.expand()`）或双 API。
- **多层 expand 需要完整路径**：两级 expand 的全局序是 `(seq, sub1, sub2)`
  字典序，3 字段 tag 丢 `sub1`；批量 payload 方案天然免疫（嵌套即展平）。
- **泛型约束坑**：`Fn(I, &mut Vec<N>)` 参数位不约束 `N`（E0207），需
  `ExpandStage<Prev, F, N>` 结构体泛型承载（expand_emit 已这么做了）。

顺带收益：与 crossfire per-thread waker（设计 C，dev/crossfire-waker-designs.md
§11）叠加后每组的 channel hop 数减少。

### 9. [P1] StageOptions 类型分裂与零值语义统一

- **无效旋钮静默忽略**：`workers` 对 async stage 无意义（async 扇出只读
  `io_concurrency`/`buffer`）、`io_concurrency` 对 sync stage 无意义——
  `.stage_async_with(StageOptions::new().workers(64), f)` 编译运行皆通过但
  什么都不做。拆 `SyncStageOptions { workers, buffer }` /
  `AsyncStageOptions { io_concurrency, buffer }`，类型系统已在区分 stage
  种族，编译期拦下比 debug_assert 强。
- **零值语义两套并存**：`StageOptions::workers(0)`/`buffer(0)`/
  `io_concurrency(0)` 经 `NonZeroUsize::new` 静默变 `None`=回退默认，而
  pipeline 级 setter 全是 `max(1)` 截断；`with_oversubscribe(0)` 同病
  （六处 `.max(1)`），与 `Workload::Custom(NonZeroUsize)` 的纪律不一致。
  统一为 assert! 或 NonZero 入参，一次 breaking 收敛。

### 10. [P1] 取消的部分输出语义

取消时 feeder/worker break，collector 排干通道即返回——中途取消产出静默
截断的 `Vec`，与正常短 run 不可区分；`run()` / `for_each` / `with_cancel`
均未文档化。至少补文档；更优：`RunOutcome` 或 `try_run_checked` 报告
`Cancelled { emitted }`（collector 排干后查 token 即可）。

### 11. [P2] API 小项包

- **文档失实（快修）**：`Workload::Unbalanced` 文档说 "Always 8× oversplit"
  （builder/config.rs 两处），实际 `unbalanced_oversplit()` 默认 32
  （fused.rs，`YOUPIPE_OVERSPLIT` 可调）；
- `StreamPipe` 缺 `with_workload`：纯 sync 链 pass-through 到 fused 路径
  时会读 `config.workload`，目前唯一入口 `with_config` 是整体替换；
- `Pipe::filter` 缺 `map` 有的 `O: Send + 'static` bound——非 Send 输出
  能穿过任意级 stage，到终端才爆一墙 trait-solver 错误；
- `MpscSender` 无 `try_send`（其余三种 sender 均有，不对称）；
- 收集终端命名：`StreamPipe::run()` vs 其余 builder 的 `collect()`——加
  alias 或统一；
- `with_config` 整体替换 config，先前 `with_*` 静默丢失（顺序敏感）——
  文档写明或改为字段合并；
- drain 家族 unjustified `'static`（state/stream.rs `drain_*` /
  `run_ordered_collect`，循环内不 spawn 不存储，`Send` 即可）——放宽容许
  借用数据出流；
- `run()` 在 async runtime 构建失败处 `.expect("failed to build async
  runtime")` panic（线程配额耗尽的用户即触发），`try_run` 却正常传播——
  把失败前移到 `.stage_async()` 构建期，或 rustdoc 指向 `try_run`。

## 代码健康（重构/坏味道）

### 12. [P1] 终端 prologue 十处复制

`n==0` 短路 → `resolve_exec_pool` → `prefers_serial` 串行回退（含
MAY_FILTER 分派）→ `SplitPlan::new` → MAY_FILTER 两路核心选择——同一序列
在 fused.rs 十个终端复制（`Pipe::collect`/`for_each`、`TryPipe::try_collect`、
scoped 三件、by_ref 三件、`fused_pass_collect`）。split 策略或串行语义一动
就要改十处。抽 `terminal_plan(n, config, pool) -> Plan { Serial, Parallel(
SplitPlan) }` + 共享串行回退 helper；prologue 每 run 一次，零热路径风险。

### 13. [P1] par_*_rec 内部节点失败清理四份 + 六个 guard 变体

`(Ok,Ok)/(Err,Ok)/(Ok,Err)/(Err,Err)` 兄弟区间丢弃 match 在 `par_index_rec`
/ `par_index_try_rec` / `par_index_rec_by_ref` / `par_index_try_rec_by_ref`
四处逐字复制；`LeafGuard`/`TryLeafGuard`/`RefLeafGuard`/`TryRefLeafGuard`/
`ForEachGuard`/`FilterGuard` 六个同型 raw-pointer/tail-drop guard。抽泛型
`par_tree_rec` + `drop_success_range` 钩子（即既有 `HybridStrategy::
cleanup_success_chunk` 语义）；**叶函数保持独立单态化**（向量化论证只对叶
成立）。落地后新终端（#3 reduce）只需叶 + 钩子。

### 14. [P1] StageSpawn 五路 spawn 体 × 四 stage 类型

每 stage 类型手写 `spawn`/`spawn_single`/`spawn_for_async`/
`spawn_async_feeder`/`spawn_async_feeder_single`，~15 个近同体（stream.rs
SyncStage/ExpandStage/FenceLink + 两个 async consumer body），差异仅三点：
调哪个 prev 方法、channel 构造器（`channel`/`mpsc_channel`/
`sync_async_channel`）、`FinalRx` 包装；`spawn_async_consumers_body` 与
`_single` 45 行只差一个构造调用。引 channel-factory trait（`OutChannel {
type Tx; type Rx; fn make(buffer); fn wrap(rx) -> FinalRx }` 三实现 Mpmc/
Mpsc/MixedAsync），每 stage 一个泛型 `spawn_into`，五方法变单行委托；async
侧补 `AsyncSendItem` trait（对齐既有 `AsyncRecvItem`，handoff/channel.rs）
收编 `_single` 孪生。回归防护（try_exec 的终端 Single-variant debug_assert）
重构期作安全网。

### 15. [P1] 六 builder × 五 setter 复制

Pipe/TryPipe/PipeRef/TryPipeRef/ScopedPipe/ScopedTryPipe 各手抄
`with_config`/`with_workload`/`with_compute_workers`/`with_compute_pool`/
`with_oversubscribe` + 同构 `map`/`filter` 体重建（~30 份方法拷贝，每方法
五行字段搬运）；曾因此出过 budget 被静默忽略的漂移 bug（fused.rs 测试注释
有记录）。抽内部 `ExecOptions { config, compute_pool, oversubscribe }` 被
move 进各 builder + setter 宏，加字段只改一处。

### 16. [P2] 死代码清理

pool/mod.rs 模块级 `#![allow(dead_code)]`（"Scope/spawn infrastructure not
yet wired"）已掩盖真死代码（repo 内零调用者，2026-09 核实）：`spawn_static`
（pool/registry.rs）、`CountLatch::increment`/`wait`（pool/latch.rs，fused
只用 `wait_spin` 系）、`word_and_bit`（pool/sleep_mask.rs，`split_index`
的完全重复）；另有 `BoxFuture` 类型别名（runtime/mod.rs，零引用）、
`CancellationToken::reset`（sync/cancel.rs，仅自测调用；复活 token 对在飞
worker 是脚枪）。删项后摘掉模块级 allow，让编译器重新把关。

### 17. [P2] 正确性相邻与杂项

- **ReorderBuffer 窗口溢出静默丢数据**：`insert` 慢路径里 slot 被占且
  tag 不同（两 seq 别名同槽 = 容量前置被超）时，debug/release 均无信号、
  直接丢旧项（state/reorder.rs；重复 seq 分支有 debug_assert，别名分支没
  有）。窗口 clamp 上限 1 Mi，深乱序可达。扩 debug_assert 覆盖别名分支 +
  release 侧 `dropped` 计数器，collector 校验 `collected + dropped ==
  expected`；
- `FenceBarrier::reuse` 对非空批仅 debug_assert，release 静默丢弃——改
  total API（非空时 append 回 buffer）消灭契约；
- feeder-slot 预测 `n > buffer_size.max(4)` 手抄了 buffer floor
  `max(parallelism * 4)` 的最小值（stream.rs `try_exec` vs `stage_buffer`）
  ——floor 规则一改 parking lease 欠预留、死锁回归且无测试兜底；抽共享
  常量/函数并注释互锁；
- drain 四体复制（`drain_ordered`/`drain_ordered_async` × unordered，
  state/stream.rs，~70 行只差 recv vs recv().await 与 ReorderBuffer 插入）
  → `macro_rules!` 模板（tokio 的 sync/async 孪生惯用法），防止 burst-drain
  策略改动四处漂移；
- TLS worker 指针 + registry_id 校验在 pool/registry.rs 三处 + worker.rs
  一处手写（核心安全前置，各自独立 unsafe 块）→ 单一
  `Registry::current_worker_of` helper 集中 SAFETY 论证；
- crossfire 错误映射七份手抄（handoff/channel.rs 各 try_send/try_recv
  包装内的同构 match）→ `map_send_err`/`map_recv_err`；
- `try_exec` ~257 行混四职责（admission/lease 解析、StreamCtx 构建、
  feeder 通道选路、终端 drain）→ 拆 `resolve_admission`，让不变量注释贴着
  维护它的代码；
- StageSpawn 默认实现删除改必选（默认体掩盖漏覆盖，且空链
  `stream(0..n).with_cancel(t).run()` 在 fused pass-through 拒绝后走到
  终端 MPSC debug_assert，debug 构建必 panic——顺带修该 panic 或特判空链）；
- reorder window 魔法数 `next_power_of_two().clamp(1<<10, 1<<20)` 两处
  重复（state/stream.rs）抽 helper；
- 文档 bug：`run()` Panics 节把 ordered+expand 的说明链到 `FenceMode`
  （应为 `StageSpawn::has_expand`）；`stage_buffer` floor 措辞
  "downstream_workers * 4" 与调用点传本 stage 自身 worker 数不符
  （stream.rs 两处）。
