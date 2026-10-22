# TODO

范围：**未落地**的工作项。已关闭需求的落地记录与实测读数保存在 docs/src/dev/
对应主题文档（scheduler.md / benchmarks.md / streaming.md / core-types.md）
与代码注释里，勿凭记忆重做；已证伪方向见 [dev/dead-ends.md](src/dev/dead-ends.md)。
编号不复用（dev 文档以「todo #N」引用条目），已关闭条目留下空缺编号。

验证统一规范（见 `dev/benchmarks.md`）：

- 结论一律用 `perf/bench-suite` 交错 A/B（同机同 session，median-of-rounds）；
- 100K fused 家族必须隔离交替跑（整组跑有 2–3× 塌缩陷阱）；stream 家族 ±10% 漂移需隔离交替；
- 调度器类改动用**同 binary 运行时旋钮** A/B（`bench_ab.sh -E`，重编译对紧基准有 ±30% 纯代码布局噪声，2026-07 在未改动的 rayon 锚点上实测 +31%）；
- 重尾（heavy-tail）结论必须多 seed（chunk 边界运气主导单 seed 方差）。

优先级：P0 = 高价值/证据充分；P1 = 中等；P2 = 实验/微优化/口径修正。
代码位置以符号名为主（行号随主分支漂移）。

---

## 性能

### 2. [P1] zstd_shape 残余差距（capped/uniform 落后）

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

### 3. [P1] fused 批间泊车/唤醒占用亏损（默认路径残余）

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

### 4. [P2] streaming async 形状的吞吐塌缩（残余 ~1%）

- 现状：`YOUPIPE_ASYNC_CHAIN_FLOOR`（默认开，2026-10-22）已解决主体——
  async 链的未 pin 通道（stage 输出 + feeder 环）floor 到
  `min(n, 16384)` slot（8 MiB/环字节预算），async2 深 soak 慢档
  18–30/120 → 0/120（release gate 二进制 21/96 → 2/96）；terminal-only
  解耦与 `buffer=n` 必要性均已证伪（dead-ends.md；落地记录见
  dev/streaming.md「Async-chain capacity floor — resolution」与
  benchmarks.md 同名节）。
- 残余：最终 gate 下 on 侧仍有 ~1% 慢 run（2/216，单进程偶发）。若可
  复现，候选：floor 提到 64K 档（扫描中 64K 同样 0/48）、或与
  `YOUPIPE_SPIN_ANCHOR` 的组合面。

### 5. [P1] `ordered()` + `expand()`：批量 payload 方案

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

### 7. [P1] 取消的部分输出语义（残余：RunOutcome 报告）

- 文档契约已补（2026-10-20，r7）：`run()` 新增 Cancellation 节，
  `for_each`/`with_cancel` 内联说明，guide/stream.md 同步——取消产出静默
  截断的输出，与正常短 run 不可区分。顺带修正 guide 旧措辞：worker 只
  完成当前条目即停（逐条目查 token），并非 "in-flight items drain to
  completion"。
- 残余方向：`RunOutcome` 或 `try_run_checked` 报告
  `Cancelled { emitted }`（collector 排干后查 token 即可）。

### 16. ~~[P1] streaming stage worker 的逐项 send 批量化~~（已落地）

- `YOUPIPE_STREAM_TX_BATCH`（2026-10-22，默认开）+ fork MPSC ring 原生
  `try_push_batch` 已落地：multi2 −49 %、cancel_quad −65 %、fence_pair
  −20…−25 %、workers2 −10…−17 %，全 25/25 或 9/9 稳定；async 尾链按
  `!has_async_stage` 门控保持逐项（两跳批量均 +2.2…+4.9 %，证伪记录在
  dead-ends.md）。读数：benchmarks.md「Stream worker tx batching」；
  机制与语义：streaming.md「Worker output batching」。

## 代码健康（重构/坏味道）

### 13. [P2] 正确性相邻与杂项

- `try_exec` ~257 行混四职责（admission/lease 解析、StreamCtx 构建、
  feeder 通道选路、终端 drain）→ 拆 `resolve_admission`，让不变量注释贴着
  维护它的代码。

### 14. [P1] `pipeline_integration` 间歇性 hang 残余

- 现状：pool-wide parking lease（74211b1）修复了确定的并发预算成分后，hang
  仍偶发（2026-09-28 又一次：并行 cargo test 下某测试二进制 107 线程全部
  futex wait、13 分钟 0 CPU，kill 后其余测试正常；2026-10-22 又一次：
  **graph 引擎侧**，`graph_dag` 二进制 `diamond_broadcast_conservation`
  （n=10K、buffer=4），68 线程全部 futex——21 个阻塞在 `recv`、11 个阻塞
  在 `send`（ring 满）、主线程在 `graph/exec.rs` 完成 condvar wait，
  standalone 重跑 20/20 秒通过，与 stream 侧改动无关）。Cargo.toml 的
  `crossfire-trace` feature 即为其取证插桩；graph 侧复现的 stack 签名
  （producers Full / consumers Empty / 主线程 join condvar）与 #14 历史
  形态一致，说明该间歇性死锁跨引擎存在。
- 方向：复现时用 `crossfire-trace` + gdb 取证（lost-wakeup 假设优先）；关注
  fence pool-job 化（41e0a6a）后的新交互面。
- 验证：并行 harness 反复跑 `pipeline_integration`；无确定性复现前不动代码。

### 15. [P2] 2026-10-20 审计遗留项

Round-6 三路审计的完整发现（机制、证据、验证计划）记录在各主题文档的
`Audit 2026-10-20: open items` 节（scheduler.md / streaming.md / testing.md）；
此处仅列未安排进紧邻轮次的条目：

- st3 `WorkerThread::push` 的 `is_empty()`+`push()` 对偷取者共享行的 5 次原子
  读可减为 3 次——先 cargo-show-asm 验证 LLVM 是否已合并相邻 Relaxed 载荷
  （泛型单态化在同 CGU 可能已消除），未合并再评估 fork API（fork 可 diff
  约束下代价高，预期收益 <1%）；
- `try_apply_pure` 无 bench id：该路径对 codegen 布局敏感（ErasedStrategy
  记录过 ±16–30% 布局回归），布局一变 `try_collect` 快路径可能无声回归；
- `perfcnt`→`nom v4` future-incompat：upstream 停更，rustc 未来版本会拒绝
  编译，届时需 vendor/fork 或把计数器移植到 gungraun 的 valgrind 路径；
- mdbook 无 linkcheck preprocessor，页内相对链接无校验（2026-10-20 一次性
  脚本扫描为 0 断链，CI 化待做）。
