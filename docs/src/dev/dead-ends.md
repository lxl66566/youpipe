# 已证伪方向（勿重复尝试）

跨主题的失败实验索引：每项均已实现并实测为负/危险后回退。详细数据与机制
记录在 [scheduler.md](scheduler.md)、[benchmarks.md](benchmarks.md)、
[crossfire-waker-designs.md](crossfire-waker-designs.md) 与对应代码注释。
新想法先对照本清单；开放中的工作项见 [todo.md](../../todo.md)。

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
- **count-then-place 用于低选择率 filter**（2026-09-27，`YOUPIPE_FILTER_CTP`
  实验，5 轮同 binary knob A/B）：前提反了——merge 树开销随存活数缩放，
  count-then-place 随输入数持平（stage 跑两遍），交叉点 ~25 % @100K：
  keep10 **+41 %**、全部 10K 形状 +28…+75 %（均 0/25）。高选择率侧成立
  （keep90 −50.5 %、keep50 −37.1 %、33 % −31.1 %，均 25/25），故 knob 以
  opt-in 保留、默认关。数据见 benchmarks.md filter 小节。
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
  侧声明、非时间推断）与尾部 straggler 细化仍开放（todo.md）。
- **quiescence 门控保温自旋**（2026-09-26，实现后 A/B 前回退）：机制——
  worker 在 spin→yield 相位边界检查 `counters.inactive == num_threads`
  （全池静默 = 没有在执行的工人 = 批间 gap 而非 ramp-down），静默期间以
  有界预算（env 旋钮）延长 busy-spin 跨过批间 gap。判据本身正确地区分了
  两种 idle，但**生效太晚**：capped/uniform 形状里早停工人在批尾（尚有
  straggler 在跑、非静默）就耗尽 32+32 窗口泊车，静默成立时救援窗口已过；
  冒烟（uniform，q=2048 vs 0）+1.5…+2.8% 无收益，而对照组全局
  spin=2048 同形状 −4%（收益全部来自「非静默期间也保温」，即被证伪的
  全局加宽）。结论：批间 park/wake 残差不存在只作用于 gap idle 的
  池内信号；opt-in 核绑定（scheduler.md "Worker affinity"）从唤醒落核侧
  收回了同一残差。
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
  排干后 Err、try_recv 区分 Empty/Closed、多生产者 Clone，std 亦无
  crossfire #70 假 Disconnected 窗口）。结论：crossfire mpsc 保留；微基准
  （无竞争稳态）与 in-pipeline（burst 节奏 + park 往返）两种口径的矛盾
  以此收束——当初切 crossfire 的 in-pipeline profiling 依据仍成立。
