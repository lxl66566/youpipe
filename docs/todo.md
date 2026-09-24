# 性能改进 TODO

来源：代码注释与 docs 中记录的未解决差距 + 已知测量缺口。每项附证据出处与验证方式。
已关闭的需求从本文件移出——落地记录与实测读数保存在 docs/ 对应主题文档
（scheduler.md / benchmarks.md / streaming.md / core-types.md）与代码注释里，
勿凭记忆重做；已证伪方向沉淀在文末清单。

验证统一规范（见 `dev/benchmarks.md`）：

- 结论一律用 `perf/bench-suite` 交错 A/B（同机同 session，median-of-rounds）；
- 100K fused 家族必须隔离交替跑（整组跑有 2–3× 塌缩陷阱）；stream 家族 ±10% 漂移需隔离交替；
- 调度器类改动用**同 binary 运行时旋钮** A/B（`bench_ab.sh -E`，重编译对紧基准有 ±30% 纯代码布局噪声，2026-07 在未改动的 rayon 锚点上实测 +31%）；
- 重尾（heavy-tail）结论必须多 seed（chunk 边界运气主导单 seed 方差）。

优先级：P0 = 高价值/证据充分；P1 = 中等；P2 = 实验/微优化/口径修正。

---

## P1

### 1. zstd_shape 残余差距（capped/uniform 落后）

- **现状**：wide-tier 边界已扫描并落地 64→48（2026-09，scheduler.md
  "Wide-tier boundary scan"：42 items/chunk 中性、63/chunk wide 全形状
  占优，heavy-tail n=3000 vs-rayon 差距 +8.6%→+2.5%）。残余：capped
  +1…+2%、uniform +4…+6%、heavy-tail n=2000（narrow）~+1% 仍落后 rayon。
- **方向**：capped 形状的残余是重尾 spread（4.2 pt）而非均值——考虑 chunk 内条目
  乱序化或第三档，但注意 cost-EMA 类自适应已两次证伪（见 scheduler.md），
  不要再走运行时成本估计路线。
- **验证**：`zstd_shape` 全形状 × 多 seed（criterion bench 已支持
  `ZSTD_SEEDS`/`ZSTD_SHAPE_NS` id 网格透传）；cheap 侧档位经算术核对
  不随边界变化（cpu_unbalanced n=200/5000 均未跨 48）。

### 2. 终端 collector 通道 in-pipeline A/B：`std sync_channel` vs crossfire mpsc

2026-10）显示无竞争 1P1C 形状下 `std::sync::mpsc::sync_channel(256)` 比当前
collector 用的 crossfire mpsc flavor 快 ~17–31 %（41/58 vs 35/44 Melem/s）。
当初切 MPSC 的依据是 in-pipeline profiling（N 生产者竞争下 recv 侧 CAS 主导，
见 handoff/channel.rs），微基准无法复现该竞争，两者口径不同、并不矛盾。

- **方向**：在真实 pipeline 里 A/B 把 collector 通道换成 `sync_channel`
  （`SyncSender: Clone` 满足多生产者，`RecvItem` 抽象已就位）。注意 std 阻塞
  send 无自旋窗口、park 策略不同，低深度背压场景可能反而回退。
- **验证**：`stream_pipeline` 全家族 + `mixed_load` 隔离交替 A/B。

### 3. （正确性存疑）`pipeline_integration` 间歇性挂死：未定位的丢唤醒窗口

- **现状**（2026-09 复现记录，详见 `dev/crossfire-waker-designs.md` §12.4）：
  `cargo test --release --test pipeline_integration` 单测试二进制循环
  （无外部负载、90 s 超时判定）下间歇挂死：waker 设计 C（vendored HEAD）
  20 轮第 16 轮挂、设计 B（`waker-intrusive` vendor 快照）20 轮第 7 轮挂，
  另有 10 轮全套循环内 1 次（多测试并发）——**与 crossfire waker 实现无关**
  （两种实现均复现，共享的是通道语义层），属预存在问题。每轮 ~5–15% 概率。
- **形态**（gdb 全线程回溯，两次挂起一致）：池 worker 停在 crossfire mpmc
  `Tx::send` 满 park、消费端停 `MpscReceiver::recv` park、
  `test_hybrid_dispatch_spin_wait_stress` 停 `LockLatch::wait`；挂起时全部
  线程 futex wait（无 CPU 消耗），通道对端不消费也不被唤醒的死锁形态。
- **排除项**：crossfire waker 的 C 与 B 两个实现；两实现的 miri
  （tree-borrows）+ loom 协议验证均无反例（designs §11.6 / §12.1）。
- **方向**：① `timeout`+gdb 循环复现，重点查 `hybrid_dispatch`/LockLatch
  唤醒与 stage 通道 close/drop 的时序、以及多通道 episode 的唤醒配对；
  ② youpipe 侧补"多通道 select + 池 sleep"交错的 loom 模型；③ 不排除测试
  自身（spin_wait_stress 与其余测试并发抢核导致的长饥饿尾巴）。
- **验证**：归因后复现轮转绿；`perf/verify` 脚本长循环稳定性。

---

### 4. fused 批次间 worker 泊车/唤醒占用亏损（NT store 收窄后的残余项）

- **现状**（2026-09-25 归因，详见 `dev/benchmarks.md` "Attributing the
  2M/4M fused-collect gap"）：cpu_balanced 大批量上 youpipe 每迭代 cycles/
  指令均少于 rayon 却壁钟更慢——task-clock 26.7 vs 30.7（/31 CPU）、上下文
  切换 99 vs 13–19/迭代、迁移 7.6 vs 0.5/迭代：空闲工人泊车 + futex 唤醒 +
  落冷核；rayon 靠全程自旋保温（多烧 +11–16% cycles）。同 binary 旋钮因果
  验证：`YOUPIPE_SPIN_ROUNDS=YOUPIPE_YIELD_ROUNDS=2048` 各 shape 一致改善，
  但只救回 1–4 pt（NT store 另行救回 ~13 pt 并反超，见 P2 #8）。
- **方向**（结构性手段；勿拉长全局自旋窗口——`ROUNDS_SPIN` 历史 +20–36% 回退）：
  1. 背靠背批次（bench 循环、流式多批次）下「下一批将至」提示 / 短窗口热身，
     让工人跨迭代保温（时间戳触发的 hot-epoch 窗口已证伪，见文末清单——
     时间窗无法区分批内 ramp-down idle 与批间 gap idle）；
  2. 尾部 straggler 细化：末段更细粒度 oversplit（动态，非 cost-EMA 路线，
     该路线已两次证伪）；
  3. 1M 打平而 2M 落后的边界为何不随占用亏损移动，未解释；NT 默认档落定后
     复查。
- **验证**：`cpu_balanced` 1M/2M/4M 隔离 A/B + `horizontal-counters`
  （youpipe-bench）复查 task-clock / ctx-switch / migration。

---

## P2

### 5. 池 worker 核绑定（affinity）实验

latecomer 分析（`dev/scheduler.md`）表明 SMT 过下载下 CFS 唤醒延迟
100 µs–1.7 ms 是残余 straggler 的根源，属内核调度行为。可选：per-worker
`pthread_setaffinity` 配置项（默认关闭），在 zstd_shape/unbalanced 上 A/B。
风险：与用户 `taskset` 冲突、跨 NUMA 迁移损失、库越权管理拓扑。

### 6. transient pool 复用缓存

`with_compute_workers(n≠ncpus)` / `with_oversubscribe` 每次终端调用建池
拆池（~ms 级，`ExecPool::Owned`）。可做进程内按尺寸的小 LRU 缓存。
风险：线程数失控（用户以为池已销毁）；至少在 rustdoc 与 tuning.md 把
「紧循环请预建池」的警示提级。

### 7. （非性能，顺带记录）`ordered()` + `expand()` panic（2026-10 设计分析）

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

若做批量 payload 方案，注意与 crossfire per-thread waker（设计 C，
`dev/crossfire-waker-designs.md` §11）叠加后每组的 channel hop 数减少，是顺带收益。

### 8. NT store 默认档位（原「≥2M 带宽差距」项的收尾决策）

- **现状**：`YOUPIPE_NT_STORE=1`（fused collect 叶子输出非临时 store）实测
  cpu_balanced 100K/1M/2M/4M +10/+15/+15/+18%，默认 off——输出写后立读的
  形状会把 cache hit 换成 DRAM round-trip。归因与数据：`dev/benchmarks.md`
  "NT-store attribution"。
- **方向**：决定默认档位——保持纯 opt-in、按输出字节数/缓存几何自动启用，
  或作为 collect Options 暴露；需要「写后立读」形状的回归数据支撑。
- **验证**：horizontal `cpu_balanced` 全档 + criterion fused 家族隔离交替
  （100K 家族塌缩陷阱）。

---

## 已证伪方向（勿重复尝试）

以下均已实现并测过为负/危险，详细记录见 `dev/scheduler.md`、
`dev/benchmarks.md` 与对应代码注释：

- **driver 偷已注入 chunk** 的三个变体（claim-flag / pop-any / pop-requeue）：
  破坏 injector FIFO（stream 正确性的隐式全局依赖）或 `counter==0 ⇒ JobRef
  全部消费` 不变量（UAF / 全池死锁）。安全形态只有 reserve chunk（现状）。
- **flat 顶层派发**：小中 N 赢、大 N 单注入器争用崩盘；hybrid 是定论。
- **on-pool 无门控 hybrid**（2026-07）：小批量 P 并发嵌套把 ~P² 个小 chunk
  灌进全局 injector，每个 driver 的窃取等待都在同一 MPMC 上弹跳出队——
  `nested_saturated/1K` **+430 %**、`nested_single/1K` +3 %。单树的本地
  deque + 偷取才是小批量的正确形态；on-pool hybrid 仅大批量（`chunk_splits
  > 0`）启用。详见 `hybrid_dispatch` 的 regime 注释。
- **local-deque hybrid 解除小批量门控**（2026-09-25，`YOUPIPE_ONPOOL_HYBRID
  =2` + `YOUPIPE_ONPOOL_HYBRID_SMALL`，5 轮交错）：分布到 driver 本地 deque
  只救回饱和侧——`nested_saturated/1K` −8 %（25/25），但 `nested_single/1K`
  仍 **+80 %**（0/25）：P−1 worker 停机、单 driver 的形状里，hybrid 的固定
  开销（chunk 划分 + 一次 wake cascade + latch 等待）压不过单树逐层 push
  的增量 ramp。按 regime 混合结论 ⇒ 门控保留；自适应门控即 cost-EMA 类，
  已两次证伪。大批量侧的 level 2 本体成立（vs level 1 稳定 −1.7…−2.9 %，
  见 scheduler.md），默认仍 0。
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
- **chunk 边界 cache line 对齐**（`YOUPIPE_ALIGN_CHUNKS`，2026-09）：顶层
  chunk 边界与 `par_*_rec` 每层 mid 全部 snap 到 input/output 双 buffer
  的 64B 边界格点（同余联合，output 优先；纯 index 重排，off 路径
  bit-for-bit 不变）。同 binary 5 对 off/on 进程交替（horizontal
  cpu_balanced，rounds=5，taskset 1-31）：youpipe 2M +0.35%、4M +0.28%、
  1M −0.02%、100K −0.31%，全部在 rayon 对照 ±0.8% 噪声底内；1K +1.9%、
  10K +0.7% 反付每 batch 的格点计算 + bounds Vec 分配固定开销。机制归因：
  每 chunk（4M/32 ≈ 131K 元素）仅首尾 2/16K 条 line 跨界（~0.01% 量级），
  且 SIMD store 对齐前提已由 malloc 16B 对齐保证——64B 全 line 对齐在 Zen
  上无边际收益，差距不在边界对齐。
- **hot-epoch 条件性保温窗口**（`YOUPIPE_HOT_EPOCH_MS`，2026-09-25）：机制——
  进程级 activity 时间戳（锚点仅 2 个：fused dispatch 完成 + fused 批注入，
  每批各一次，driver 侧执行）+ idle sleepy 决策点判定：热窗内（now −
  last_activity < epoch）把该 idle episode 退避临时扩为 2048/2048（同因果
  验证值），deadline 封顶、过期即回默认泊车；off 路径行为不变。A/B
  （horizontal cpu_balanced 100K/1M/2M/4M，同 binary 两进程交替 ×5 对 +
  rayon 对照进程，taskset 1-31，1 对 off 进程被外部负载污染剔除）：per-pair
  中位 100K ~0%、1M −2.0%、2M −0.3%、4M −1.4%，全部低于 +3% 门槛（2M 对
  rayon gap 仅 12.5→11.5 pt）。空闲 burn（3×2M 批后 10s 空转，getrusage
  全进程 utime+stime）：off 0.1–0.2 ms、epoch=1ms 2–6 ms、epoch=2ms
  0.4–15 ms（末批尾部分布的运气主导，鲁棒性差）、epoch=5ms 47–70 ms ≈
  全局 2048/2048（~60 ms）——封顶语义生效，但窗口≈批时长（5ms vs ~1ms/批）
  时机制退化为全局加宽。失败根因：时间窗无法区分两种 idle——批内
  ramp-down（工人做完 chunk，widening 烧核干扰在职工人，正是 ROUNDS_SPIN
  回退机制）与批间 gap（widening 的收益来源）；2M 批执行 ~1ms >> gap
  30–100µs，净收益抵消至 ~1 pt，与全局 2048 的 1–4 pt 因果上界一致。
  旁证（不作结论）：外部 bench 挤占 20+ 核的争载下同 A/B 一致 −8~−13%，
  提示泊车频率被放大时窗口才有净收益。「下一批将至」的显式提示（caller
  侧声明、非时间推断）与尾部 straggler 细化仍开放。
- **`Slots::uninit` 输出分配 `MADV_HUGEPAGE`**（2026-09，机制性证伪未跑
  A/B）：sysfs THP `enabled=[madvise]`（非 never），但此机内存状态使
  madvise 无效——buddyinfo Normal zone order-9 空闲块 0、MemFree ~2 GB
  碎片化，fault-time 2 MiB 分配恒失败（纯 mmap 2M 对齐 + madvise + 写
  touch 实测 6/6 `AnonHugePages: 0 kB`）；khugepaged 异步 collapse 30 s
  后仍为 0（`pages_to_scan=4096`/轮 × 10 s 周期，bench 秒级迭代等不到）。
  bench 场景依赖 fault-time 路径，除非机器重启后早期或空闲大块充足，
  否则该方向不可测。first-touch 单 NUMA 无意义（已排除）。
