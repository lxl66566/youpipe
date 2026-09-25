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
- **归因收束（2026-09-26）**：capped/uniform 残差主体是批间 park/wake
  占用（keep-hot 旋钮因果验证：capped −3.8…−5.3%、uniform −1.5…−3.9%，
  但 heavy-tail +1.1…+4.1%——全局加宽的 Pareto 代价）。**已落地 opt-in
  解法**：`ComputePool::new_pinned` 核绑定（capped −6.1…−6.9%、uniform
  −4.0…−4.6%、heavy-tail 也 −1.6…−3.8%，无烧核副作用；小批量/流式
  regime 代价见 scheduler.md "Worker affinity"）。默认路径的残余改按
  pinning 闸下口径重估。
- **方向**：capped 形状的残余是重尾 spread（4.2 pt）而非均值——考虑 chunk 内条目
  乱序化或第三档，但注意 cost-EMA 类自适应已两次证伪（见 scheduler.md），
  不要再走运行时成本估计路线。
- **验证**：`zstd_shape` 全形状 × 多 seed（criterion bench 已支持
  `ZSTD_SEEDS`/`ZSTD_SHAPE_NS` id 网格透传）；cheap 侧档位经算术核对
  不随边界变化（cpu_unbalanced n=200/5000 均未跨 48）。

### 2. `pipeline_integration` 间歇性挂死：已修复（双重根因，2026-09-25 收束）

- **成分一**（详见 `dev/streaming.md` "Pool-wide parking
  lease"）：stream run 的 liveness 预算按单 run 独占池计算，多 run 并发共享
  池时联合超订——全部 worker park 在通道 send/recv 上、排队的 stage-worker
  job 与其后 injector FIFO 里的 fused chunk 永远无法弹出。放大复现（4 个
  barrier 对齐 2-stage run）5/5 挂死；修复为池级 parking lease（run 整体
  CAS 预留 parking jobs 上界，放不下则转 dedicated threads），复现 8/8
  通过、40 轮单二进制循环零挂；`mixed_load` A/B 无回归；回归测试
  `test_concurrent_full_budget_runs_share_pool_no_deadlock` /
  `test_concurrent_pinned_runs_mixed_admission_no_deadlock`（pre-fix 双挂）。
- **成分二**（详见 `dev/crossfire-waker-designs.md` §12.4 末节）：crossfire
  `RegistrySingle` 状态真值源分裂——`get_waker_state` 读 WeakCell 槽位占用而
  wake 写节点状态；`fire()` 的 pop 与 wake 两步之间被调度延迟任意放大后，
  等待者 cancel+re-arm 重新填充槽位，迟到的 wake 置节点 Woken 但线程读槽位
  得 Init 误判虚假唤醒再 park，此后所有 fire 对 Woken 节点 Skip 不 unpark，
  事件流枯竭即永久死锁（解释了 n=100≪容量却 send park、closed 唤醒未达、
  同通道 send 满/recv 空并存全部残余指纹）。修复：`get_waker_state` 改读节点
  状态 + `_fire` Skip 重试补发；wepipe 新增 `crossfire-trace` forensics
  feature（trace_log 转发 + 通道地址标识插桩）支撑本次定位。
- **验证**：插桩复现器（3 进程 × 30 轮/组）修复前 ~1/6 组挂、修复后 75 组
  零挂；crossfire 25 测试（含两个 pre-fix 失败的回归测试）+ youpipe 全量
  release 测试 + 双 crate miri + clippy 全绿。已知理论残余：
  `RegistryMulti` 跨 registry stale entry 偷单次 fire（Relaxed seq 无 hb，
  loom 契约"一个事件内恢复"），未观察到闭环实例，留观察。

---

### 3. fused 批次间 worker 泊车/唤醒占用亏损（NT store 收窄后的残余项）

- **现状**（2026-09-25 归因，详见 `dev/benchmarks.md` "Attributing the
  2M/4M fused-collect gap"）：cpu_balanced 大批量上 youpipe 每迭代 cycles/
  指令均少于 rayon 却壁钟更慢——task-clock 26.7 vs 30.7（/31 CPU）、上下文
  切换 99 vs 13–19/迭代、迁移 7.6 vs 0.5/迭代：空闲工人泊车 + futex 唤醒 +
  落冷核；rayon 靠全程自旋保温（多烧 +11–16% cycles）。同 binary 旋钮因果
  验证：`YOUPIPE_SPIN_ROUNDS=YOUPIPE_YIELD_ROUNDS=2048` 各 shape 一致改善，
  但只救回 1–4 pt（NT store 另行救回 ~13 pt 并反超，已落 auto 默认档，见
  benchmarks.md "NT-store attribution"）。
- **方向**（结构性手段；勿拉长全局自旋窗口——`ROUNDS_SPIN` 历史 +20–36% 回退）：
  1. ~~背靠背批次下「下一批将至」提示 / 短窗口热身~~——已由 opt-in
     `ComputePool::new_pinned` 核绑定覆盖（2026-09-26 落地，cpu_balanced
     1M/2M/4M −5.1…−7.2%，见 scheduler.md "Worker affinity"；时间戳
     hot-epoch 窗口与 quiescence 门控均证伪，见文末清单）；
  2. 尾部 straggler 细化：末段更细粒度 oversplit（动态，非 cost-EMA 路线，
     该路线已两次证伪）——残余价值有限（新口径下 2M 仅 +1.5%）；
  3. ~~新口径复查~~（2026-09-26，7 轮交错，taskset 1-31）：fused 差距已收束
     ——cpu_balanced 1M +0.6%、2M +1.5%、4M **−6.8%**（youpipe 反超）；
     readback 口径 youpipe 领先 24–29%。占用亏损主体已由 NT-store auto +
     pinning 闸下可选收回，默认路径残余 ≈1.5 pt @2M。
- **验证**：`cpu_balanced` 1M/2M/4M 隔离 A/B + `horizontal-counters`
  （youpipe-bench）复查 task-clock / ctx-switch / migration。

---

## P2

### 4. 池 worker 核绑定（affinity）：已落地（2026-09-26，opt-in `ComputePool::new_pinned`）

latecomer 分析（`dev/scheduler.md`）表明 SMT 过下载下 CFS 唤醒延迟
100 µs–1.7 ms 是残余 straggler 的根源，属内核调度行为。已实现 per-worker
`sched_setaffinity`（worker i → 允许集第 i 个 CPU，round-robin），默认关闭。
zstd 全形状 −1.6…−6.9%、fused 1M/2M/4M −5.1…−7.2%，且无 keep-hot 自旋的
heavy-tail 副作用；但小批量/流式 regime 回退 +5…+48%（woken 线程无法迁往
空闲 CPU），故仅作显式构造器 + `YOUPIPE_PIN_WORKERS` A/B 旋钮，regime 表与
警示见 scheduler.md "Worker affinity" 与 advanced/pools.md。

### 5. transient pool 复用缓存

`with_compute_workers(n≠ncpus)` / `with_oversubscribe` 每次终端调用建池
拆池（~ms 级，`ExecPool::Owned`）。可做进程内按尺寸的小 LRU 缓存。
风险：线程数失控（用户以为池已销毁）；至少在 rustdoc 与 tuning.md 把
「紧循环请预建池」的警示提级。

### 6. （非性能，顺带记录）`ordered()` + `expand()` panic（2026-10 设计分析）

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
- **quiescence 门控保温自旋**（2026-09-26，实现后 A/B 前回退）：机制——
  worker 在 spin→yield 相位边界检查 `counters.inactive == num_threads`
  （全池静默 = 没有在执行的工人 = 批间 gap 而非 ramp-down），静默期间以
  有界预算（env 旋钮）延长 busy-spin 跨过批间 gap。判据本身正确地区分了
  两种 idle，但**生效太晚**：capped/uniform 形状里早停工人在批尾（尚有
  straggler 在跑、非静默）就耗尽 32+32 窗口泊车，静默成立时救援窗口已过；
  冒烟（uniform，q=2048 vs 0）+1.5…+2.8% 无收益，而对照组全局
  spin=2048 同形状 −4%（收益全部来自「非静默期间也保温」，即被证伪的
  全局加宽）。结论：批间 park/wake 残差不存在只作用于 gap idle 的
  池内信号；opt-in 核绑定（P2#4）从唤醒落核侧收回了同一残差。
- **`Slots::uninit` 输出分配 `MADV_HUGEPAGE`**（2026-09，机制性证伪未跑
  A/B）：sysfs THP `enabled=[madvise]`（非 never），但此机内存状态使
  madvise 无效——buddyinfo Normal zone order-9 空闲块 0、MemFree ~2 GB
  碎片化，fault-time 2 MiB 分配恒失败（纯 mmap 2M 对齐 + madvise + 写
  touch 实测 6/6 `AnonHugePages: 0 kB`）；khugepaged 异步 collapse 30 s
  后仍为 0（`pages_to_scan=4096`/轮 × 10 s 周期，bench 秒级迭代等不到）。
  bench 场景依赖 fault-time 路径，除非机器重启后早期或空闲大块充足，
  否则该方向不可测。first-touch 单 NUMA 无意义（已排除）。
- **终端 collector 通道换 `std::sync::mpsc::sync_channel`**（2026-09，实验
  e1684fc → revert aa842a6）：微基准（无竞争 1P1C）曾显示 sync_channel(256)
  比当前 crossfire mpsc flavor 快 17–31 %（41/58 vs 35/44 Melem/s），但
  in-pipeline A/B（`bench_ab.sh -a base=main -b wt`，5 轮 per-id 交错；控制列
  `mixed_load/rayon_par_iter` ±0.5 %、`pipeline_fusion/fused_3_stages` ±0.1 %
  排除环境漂移与布局噪声）全面回退：`stream_pipeline` 1K 档三 id
  +11.1…+12.0 %（两侧轮次中位区间几乎不重叠）、`with_fence/100K` +13.6 %、
  100K 多生产者档 +1.2…+1.9 %；`mixed_load/youpipe_stream_cpu` 1K/100K
  +0.1/−3.6 %（±22–25 % 噪声底内）。机制：微基准赢在无竞争稳态吞吐
  （recv 侧无 CAS、无 waker 注册表），但真实 collector 是 burst-drain 节奏
  ——burst 之间通道清空，std 的空 recv / 满 send 立即 park（无自旋窗口），
  每个 burst 边界都付一次 futex 往返；1K 小批量另付每 run 的通道构造差异
  （std block 链表初始化 vs crossfire ring 一次分配）；`with_fence` 的
  单生产者→单消费者 ping-pong（Chunked(500) 突发释放）是 std 最差形状，
  每次唤醒延迟直接串行化进 chunk 间隔。语义对齐本身无问题（disconnect
  排空后 Err、try_recv 区分 Empty/Closed、多生产者 Clone，std 亦无
  crossfire #70 假 Disconnected 窗口）。结论：crossfire mpsc 保留；微基准
  （无竞争稳态）与 in-pipeline（burst 节奏 + park 往返）两种口径的矛盾
  以此收束——当初切 crossfire 的 in-pipeline profiling 依据仍成立。
