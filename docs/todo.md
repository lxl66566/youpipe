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

### 1. 纯同步链的 fused 直通（✅ 已落地，2026-09）

- **结果**：`try_run` 在类型层面检测「链 = 连续未 pin 的 `SyncStage`，无
  expand/fence/AsyncStage，无 `with_cancel`，无 `with_compute_workers` pin」→
  `StageSpawn::fuse_exec` 沿递归把链组成 `g∘f`（`FuseCompose`，闭包按引用）
  → `fused_pass_collect`（= `Pipe::collect` 的 serial 短路 + hybrid dispatch
  + Slots 零拷贝）。实现无需 specialization：组合算子的类型经 trait 方法的
  `OP` 泛型穿递归；非 sync 节点保持默认 `Err(items)`，链以 `&self` 借用故
  可无损回退流式路径。
- **运行时排除**（类型看不见的）：`with_cancel`（fused 核心无 cancel 检查）；
  `with_compute_workers` pin（pin 语义是「在大池上限制 stage 并发」，fused
  核心无此概念——`test_compute_workers_pin_survives_compute_pool` 会挂；顺带
  避免 ~ms transient 池）；各级 `StageOptions::workers/buffer` pin。`.ordered()`
  **不排除**：直通输出即输入序，恰是 ordered 合同。
- **语义变化**（已写入 rustdoc 与 docs/src/guide/stream.md）：失去 backpressure
  （峰值内存 = 输入+输出）；unordered 输出从完成序变输入序；stage panic 从
  abort 变为向调用者传播；`for_each` 不直通（调用者线程 FnMut drain）。
- **实测**（perf/bench-suite 3 轮隔离交错 A/B，32 核）：
  `mixed_load/youpipe_stream_cpu` 1K **310.5 µs → 9.98 µs（−96.8%）**、
  100K **30.45 ms → 139.3 µs（−99.5%）**（9/9 轮占优）；stream_pipeline 的
  single/multi/ordered 全家族同幅度；负例 `with_fence` 与 rayon 锚点纯噪声；
  fused 家族（sync_vs_rayon）全噪声无回归。直通后 1K 比 rayon 快 −75%（9.98
  vs 39.9 µs）；100K 慢 +51%（139 vs 92 µs）系口径差：mixed_load 的 youpipe
  案例每轮 fresh owned 输入（warm_clone），rayon 借用常驻共享切片——借入口径
  的 fused（`sync_cpu_heavy/youpipe_par_map`）在同一 work 函数下为 52 µs。
- **验证**：新增 6 个集成测试（等价性含 ordered/serial 短路、零 stage 恒等、
  panic 传播=直通激活正例、cancel/pin 排除负例）；全量测试 + clippy 绿。

### 2. ≥2M 大批量 fused collect 带宽差距归因并收窄

- **现状**：horizontal `cpu_balanced` 1M 打平后，2M rayon 领先 +14%、4M +12%
  （38 vs ~34 GB/s 输出吞吐）。`dev/benchmarks.md` "Reading the results" 明确
  标注 _unattributed_（输出槽位索引 / 派发流量 / 分配器行为均未排除）。
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

### 3. on-pool 调用者的 hybrid dispatch（✅ 已落地，2026-07，带 regime 门控）

- **结果**：`hybrid_dispatch` 用 `ComputePool::on_this_pool_owner()` 取当前
  worker 的 `(registry, index)` 传入 `CountLatch::with_count` 构造 **Stealing**
  变体——driver 等待走 `wait_until` 工作窃取循环（park 经 sleep 模块的
  latch 协议：`CoreLatch::set` → `notify_worker_latch_is_set`，与
  `join`/`SpinLatch` 同协议），取代原先「on-pool 一律退单树」的路径；6 个
  终端入口（owned/by_ref × collect/for_each/try_collect）统一进 dispatcher，
  单树回退收敛为 dispatcher 内部一处。
- **regime 门控（实测推翻了无条件切换）**：仅大批量（`chunk_splits > 0`）
  走 hybrid；小批量保留单树。同 binary 旋钮 A/B（`YOUPIPE_ONPOOL_HYBRID`，
  5 轮交错，32 核）：`nested_single/100K` **−3.5 %（25/25 dominant）**、
  `nested_saturated/100K` +2.0 %（spread 内）。无门控时小批量崩盘：
  `nested_saturated/1K` **+430 %**（P 个并发嵌套批次把 ~P² 个小 chunk 灌进
  全局 injector，每个 driver 的窃取等待都在同一个 MPMC 上弹跳出队——正是
  大 N flat 派发的单注入器崩盘复刻）、`nested_single/1K` +3 %。
- **方法论教训（已记 benchmarks.md）**：本改动的首轮 recompile A/B 第二个
  session 在**未改动的 rayon 锚点上 +31 %**、目标 id 反转 +18 %——纯代码
  布局噪声。调度器类改动的结论必须走 `-E` 同 binary 环境变量 A/B
  （bench_ab.sh 新增该选项）。另修了 bench_ab per-id 默认 filter 的引号
  bug（`'.*'` 字面量匹配不到任何 id，整场 A/B 静默空跑）。
- **验证**：新增 `tests/on_pool_nested.rs`（6 测试：嵌套 collect/for_each/
  try_collect 错误路径/panic 传播/全池并发嵌套/stream stage 闭包内 pipe，
  双旋钮取值均过）+ loom 模型 `sleeper_is_woken_by_latch_set`（Stealing set
  臂 vs park 的丢失唤醒协议）+ miri（on_pool_nested/compute_pool）+ 全量
  fused 家族 per-id A/B 无回归（sequential 锚点 ±6–13 % 对向漂移=布局彩票）。
  bench：`sync_nested_on_pool` 家族（nested_single/nested_saturated ×
  youpipe/rayon × 1K/100K）。

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

### 6. crossfire 阻塞路径的每次 park 40 B `ArcWaker` 分配（✅ 已落地；miri/loom 验证完成）

- **结果**：已按 fork 路径落地——`crates/youpipe-crossfire`（源：`/root/programs/fork/crossfire-rs` 分支 `waker-tl`），per-thread 不死 waker + 全局 seq 戳标记队列项（设计 C，取代先落地的设计 A/`waker-cache` 分支）。验收与实测细节见 `dev/crossfire-waker-designs.md` §11；考古与设计 A 的落地记录见 `dev/crossfire-waker-cache.md` §7；设计空间完整分析（A/C/B/E 对比、事实清单 F1–F15）见 `dev/crossfire-waker-designs.md`。
- **验收读数**：expand fanout-9 的 40B 分配从 upstream 稳态 ~2000/run 降至 A 的 62–433/run，再降至 C 的**稳态 1/run**（结构性归零：总量 104 次/run 对 73,728 item）；背压、无竞争场景同样归 1。C 无 fast-cancel 残余（无出口概念），`expand_alloc.rs` 的按尺寸过滤已可考虑收紧。
- **对蓝图的三处修正**（已记入 designs §11.2）：① `close()` 也必须做 seq 检查，否则会把别处现役 waiter 盖成 Closed（虚假 Disconnect）；② seq 戳源改全局计数器——per-registry 计数器数值可碰撞，会让陈旧项冒充现役偷 fire；③ `_clear_wakers` 维持节点 seq 语义（entry-seq 反而少摘陈旧项）。
- **验证状态**：fork check/test 全绿（新增 2 个 C 专属单测），upstream test-suite 串行 334/334；youpipe 全套测试 + 50 轮 pipeline_integration 压测全绿。**miri（tree-borrows）+ loom 已完成**（designs §11.6）：vendored lib 21 测试 + youpipe 集成测试 `handoff_channel.rs`（8 个：park/唤醒、断连、close-vs-重臂、超时、竞争）进 `perf/verify/miri.sh`；5 个 loom 模型（seq 突变验证可抓失效）进 `perf/verify/loom.sh`；seq 传播窄窗口经 loom 枚举确认可达且良性（绝不 Closed）。**一次未复现挂起**无对应反例，维持高负载饥饿归因。

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
