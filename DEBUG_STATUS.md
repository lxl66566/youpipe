# 工作进展记录

> 本文档持续更新，记录当前正在进行的调试/修复工作。最新状态见各节「状态」。

## 任务清单

1. **修复 P0 流式引擎死锁**（CODE_REVIEW.md Finding 1）——已修复、文档已同步、
   已提交（e4985d0）。
2. ~~mdbook 部署到 GitHub Pages 的 CI~~——用户已取消。
3. **流式管道高压丢 item——已定位根因并修复**：crossfire blocking 通道
   `try_recv` 误报 `Disconnected` 导致 collector 提前退出。修复已提交
   （ed2098d），完整排查记录见 `docs/src/dev/crossfire-try-recv-bug.md`
   （5e43cba）。根因表述已修正为「`try_recv` 检查顺序竞态」（见该文档
   Correction 段）；上游报告草稿在 `crossfire-bug-report.md`。
   **后续（2026-09-05）：上游 3.1.20 已修复（issue #70），全量验证通过，
   workaround 已全部回滚**，见第 2 节末尾。
4. **B1「池丢唤醒挂死」——已结案：是 repro 自身的 bug，不是库 bug**。
   池侧嫌疑全部排除，详见末节「B1 结案报告」。

---

## 1. P0 死锁修复（已完成，已提交 e4985d0）

### 问题回顾

`stream(..)` 默认配置下，当同步阶段数 k ≥ 池线程数 P 且 n 超过通道填充阈值时永久挂死：
worker 预算漏算了 feeder 任务（n > buffer 时 feeder 是池任务），且每阶段下限 1 个 worker，
导致「阻塞在 crossfire 通道里的池任务」总数 > 池线程数，最后一个阶段永远排不上。

### 修复方案（`crates/youpipe/src/builder/typed/stream.rs`）

- **预算记账**：`try_exec` 里先解析池（自定义或全局），给 feeder 预留 1 个槽位（n > buffer 时），
  活性预算 `live_slots = pool_threads - reserved`。`StreamCtx` 新增
  `worker_slots_left` / `stages_left` 两个 `Cell`，spawn 过程中按上游优先顺序发放 worker，
  保证每阶段 ≥1 且总数 ≤ live_slots（显式 `StageOptions::workers` 超预算时按序钳制）。
- **回退路径**：当 `live_slots < 阶段数`（连每阶段 1 个都放不下），或检测到在同池 worker 上
  嵌套调用（`is_on_this_pool()`，collector 会占住调用线程），整条链的 stage worker 和 feeder
  改用**专用 OS 线程**（与 fence forwarder 既有做法一致），panic 语义对齐池的 AbortIfPanic。
- 关键经验：review 建议的「`compute_workers - 1` 再除以 k」单独**不足以**修复 k = P 的情形
  （floor 1 会把总数顶回 P+1），必须有线程回退路径。

### 已加回归测试（`tests/pipeline_integration.rs`，均带 30s watchdog）

- `test_stream_stages_at_pool_size_no_deadlock`：k=P 和 k=2P 两种形状，n=2000。
- `test_stream_explicit_pins_exceeding_pool_no_deadlock`：workers(3)+workers(3) 在 4 线程池上。
- `test_nested_stream_many_stages_no_deadlock`：同池嵌套 + 内层 4 阶段 n=2000。

### 状态

- 三个新测试 + 原有 `test_nested_stream_inside_pool_worker_no_deadlock`、
  `test_stage_budget_explicit_deduction` 全部通过。
- 全量 `cargo test`（默认 + all-features）、clippy、`mdbook build` 全绿。
- 文档同步完成：tuning.md「Worker budget across stages」重写（feeder 预留
  槽位 + 上游优先钳制 + 专用线程回退）、core-types.md（feeder 小节与
  stage_budget 段落）、StageOptions rustdoc、prelude.rs 与两处 miri 测试
  注释（死锁约束已不存在，仅剩 miri 解释执行速度约束）、fence 大输入回归
  测试的 miri 跳过理由改写（1-worker 池现在走专用线程回退，不再死锁）。
- 已提交：e4985d0 `fix(stream): enforce pool liveness budget; dedicated-thread
fallback`。

---

## 2. 流式管道高压丢 item（已修复：crossfire try_recv 误报 Disconnected；存量 bug）

### 现象

`stress_streams_plus_assist_drivers`（hybrid_assist.rs）在我的改动上 3/5 失败：
`stream(0..500).stage(x+1).run()` 收集到 <500 个结果（如 482/296）。**基线（无我的改动）
也能复现**，只是概率更低——这是存量 bug，不是本次修复引入的。

### 已排除的（有实验依据）

| 假设                         | 实验                          | 结果                             |
| ---------------------------- | ----------------------------- | -------------------------------- |
| crossfire MPSC 通道本身      | 纯 crossfire 隔离压力测试 10s | 无丢失                           |
| MPSC 环形缓冲                | 强制终端阶段改用 MPMC 通道    | 仍丢                             |
| feeder 提前退出 / stage 少收 | per-item 计数                 | stage 收到全部 500，send 全部 Ok |
| 突发排空 try_recv 单独       | 纯 try_recv 自旋 drain        | 不丢                             |
| 阻塞 recv 单独               | 纯 recv() drain               | 不丢                             |
| fused 路径 panic             | 无 panic 的 fused 负载        | 仍丢（panic 非必需）             |

### 关键定位实验

- 流式管道改用**独立 ComputePool**（与 fused 负载不共享池）：3×15s 零丢失。
- 共享全局池：每 15s 丢 1~4 次。

**结论**：bug 在池层——流式 stage worker job 与 fused 负载共享池时，job 被异常处理。
当前首要假设：**注入队列里的 JobRef 被双重执行**（第二次执行把已 move 的闭包环境再 drop
一遍 → `tx` 多减一次引用计数 → 通道提前判定 closed → collector 提前退出 → 丢 item）。
该假设能解释：无重复 item、无多收、闭包运行次数恰好 500。

### 下一步

- 给 `HeapJob::execute` 加双重执行检测（`AtomicBool` 标记 + 报告）验证假设；
  若证实，排查 vendored `concurrent-queue` 的 `push_n`/`pop`（注入器批量提交路径）。
- 注意：`ConcurrentQueue` 是 vendored fork，改动需保持可 diff 性（见 AGENTS.md）。

### 实验进展（按时间序）

1. **双重执行检测器证伪**：`HeapJob` 加 `executed: AtomicBool`（job.rs，TEMP DEBUG 标记），
   复现丢失的 4 轮运行中 **零次 DOUBLE-EXECUTE 输出**。注入器/HeapJob 双花假设排除。
   （检测器代码暂留在 job.rs，调查结束后移除。）
2. **下一步鉴别实验**：此前只校验了 stream 侧结果。改为同时校验 fused 侧
   （确定性负载，结果可与顺序执行对比）：
   - fused 也丢 → 池层 job 执行普遍受损；
   - fused 不丢 → 特定于「流式 stage worker job 与 fused chunk 共享池」的交互。

3. **fused 侧完好**：repro 里 fused 负载每轮 `assert_eq!(r.len(), 1000)`，从未触发
   （触发即 panic 退出）。→ 排除池层普遍受损，特定于流式管道。
4. **最小化 repro**（/tmp/opencode/repro）：16 个 fused churn 线程（global pool）+
   1~8 个 `stream(0..500).stage(x+1).run()` 线程。注意：**单 stream 线程 15s 无丢失**，
   8 线程可复现。
5. **丢失模式关键数据**：`got=497 calls=500 missing=[385,451,477]`（散乱、非连续尾部、
   无重复）。stage 闭包跑满 500 次 → 计算侧无损。散乱缺失说明不是「worker send 失败后
   break」（那会导致尾部连续缺失），指向**终端 bounded channel 内 item 蒸发**：
   `send` 返回 Ok 但 collector 永远收不到。
6. **待做**：手写同拓扑实验（feeder OS 线程 → crossfire mpmc bounded → 4 个池 job →
   mpsc bounded → drain_unordered 式收集），与 youpipe::stream 同进程同负载对照：
   - 手写也丢 → crossfire 在「池负载+阻塞 send」组合下的 bug，与 youpipe 无关；
   - 手写不丢 → youpipe spawn 机制问题（端点 drop 时序/worker 生命周期）。

### 根因突破（crossfire try_recv 丢 item，已隔离复现）

1. 库内插桩结果：`DRAIN-EXIT got=368 n=500 via=1` —— collector 由
   **`try_recv` 返回 `Disconnected` 提前退出**；全程 **无 SEND-ERR**（500 次 send
   全部返回 Ok）→ item 在 crossfire 通道内部蒸发。
2. 纯 crossfire 隔离 repro（/tmp/opencode/repro/cf.rs，无 youpipe 参与）：
   - mpsc bounded、mpmc bounded、mpsc **unbounded 全部丢**（两 flavor 共性 → 在
     shared/waker 层，不在队列算法）；
   - **空闲机器也丢**（SPIN=0，fails=212）→ 与负载无关，此前「负载相关」是误导；
   - **关键鉴别：纯阻塞 `recv()` 排空（RECV_ONLY=1）零丢失**；`try_recv` 突发 +
     阻塞 recv 的**混合模式**才丢 → 直指 waker 注册/注销与 sender direct-copy
     握手的竞态（纯 try_recv 从不注册 waker → 无 direct-copy 目标 → 安全；
     纯 recv 总能从自己的 waker 槽读到 → 安全；混合时存在窗口）。
3. 代码佐证（crossfire-3.1.19）：
   - `blocking_rx.rs` `_recv_blocking` 内部 pop 用 `inner.try_recv()`（pop(false)），
     而公开 `try_recv`（shared.rs:43）用 `try_recv_final()`（pop(true)）——两条
     不同的 pop 路径；
   - `blocking_tx.rs` `_send_bounded` 有 direct-copy 路径（`WakerState::Done` →
     `return_ok!`，item 不经队列直接拷贝给接收方 waker 槽）。
   - 推测丢失机制：receiver 被唤醒（state=Woken）后改为从队列 pop、尚未注销 waker
     的窗口内，另一 sender 看到已注册 waker 执行 direct-copy，item 落入旧 waker
     槽成为孤儿 → send 返回 Ok 但无人接收。（机制未完全坐实，但不影响修复方案）
4. **候选修复（youpipe 侧，无需动 crossfire）**：`drain_unordered` 里
   `try_recv` 报 `Closed` 时不直接 return，改用阻塞 `recv()` 确认（真关闭则
   recv 立即返回 Err；误报则 recv 能收到在途 item）。
5. **另一症状很可能同根因**：此前 gdb 抓到的「池 worker 全睡 + HR collector
   永久 park」挂死，可用 crossfire `recv()` 丢唤醒解释（park 后 item 到达却没
   唤醒）——待修复后回归验证。
6. 待做：验证候选修复 → 检查 async 侧（`drain_unordered_async`、fence forwarder
   等所有 try_recv+recv 混合排空点）→ 向 crossfire 上游报 issue → docs 记录。

### 修复落地与验证（已完成）

1. **修复**：`crates/youpipe/src/state/stream.rs` 全部 4 个排空循环
   （`drain_unordered` / `drain_ordered` / `drain_unordered_async` /
   `drain_ordered_async`）的 `try_recv` Closed 分支改为先用阻塞 `recv()`
   确认再退出；模块头注释完整记录了 crossfire 该竞态的机制与复现条件。
   其余排空点（stage worker、fence forwarder 等）均为纯 `recv()` 循环，不受影响。
2. **验证**：
   - 纯 crossfire 对照实验：CONFIRM（recv 确认）后 mpsc/mpmc/unbounded 三配置
     20s 零丢失（基线同配置 62 次丢失）；
   - 最小 repro（16 fused churn + 8 stream，15s）：修复前每轮 1~4 次丢失，
     修复后 90s 零丢失；
   - 原始失败测试 `stress_streams_plus_assist_drivers`：修复前 3/5 失败，
     修复后 7/7 通过；
   - `cargo test`（默认 + all-features）全绿；clippy 无新增警告
     （pipeline_integration.rs:122 的 cast 警告为存量，与本次无关）。
3. 所有临时插桩（HeapJob 双花检测、SEND-ERR/DRAIN-EXIT 打印）已全部移除。

### 新发现的独立存量 bug（B1：池丢唤醒挂死）——已结案：repro bug，非库 bug

#### 结案报告（2026-09-04）

**结论：挂死由 stress repro 自身的一个 sender 泄漏引起，youpipe 池与
crossfire 的阻塞 recv 都没有丢唤醒。** 池侧全部嫌疑经插桩实证排除。

证据链（按调查顺序）：

1. **池内状态转储**（临时插桩：registry 看门狗 + sleep 事件环，已移除）：
   挂死时 `sleeping=32 inactive=32 jec=2 injector=0`、掩码全满、全部
   worker `is_blocked=true`——**池完全空闲：注入器为空，没有任何待执行
   任务**。直接推翻「注入器里有未完成 job 而所有 worker 在睡」的原假设。
2. **gdb 全线程栈**（gdb 捕获 abort 时刻）：16 个 fused churn、8 个
   stream、4 个 HR feeder 线程**全部正常退出**；仅剩 4 个 HR 组线程 park
   在 `std::thread::park`（crossfire blocking recv 的 park 原语）+ main
   在 `scope` join。所有 32 个 worker 在 `Sleep::sleep` 的 condvar wait
   ——合法的空闲 park。
   （注：此前 gdb 里「3 个 fused churn 线程 park 在 std::thread::park /
   scoped latch」的归因很可能是误判——LockLatch 走 parking_lot condvar
   （futex），不会显示为 `std::thread::park`；显示为 std park 的是
   crossfire 的阻塞 recv/send，即 stream/HR collector 一类线程。）
3. **根因（repro bug）**：HR 组的
   `for _ in 0..4 { let (rx0, tx1) = (rx0.clone(), tx1.clone()); submit(..) }`
   只**遮蔽**（shadow）了原始 `tx1` 绑定——原始 `tx1` 活到迭代块结束
   （collector 循环之后）→ 终端通道永远不关闭 → collector 的阻塞
   `recv()` **合法地**永久 park。纯 crossfire 复现器（无 youpipe，
   collector 用 `recv_timeout(2s)` 自愈 + 通道诊断）确认同一形态：
   `got=500 closed=false len=0 tx_cnt=1`——item 全部收完、通道空、
   仅剩那个泄漏的 sender。
4. **为什么修复前后表现不同**：ed2098d 之前，collector 靠 crossfire
   谎报的 `try_recv Disconnected`（B2）侥幸逃出迭代（HR-LOSS 打印后
   继续）；B2 被防护后，每次迭代都确定性挂在泄漏的 tx1 上——挂死从
   「偶发」变「必现」纯属 B2 掩盖效应消失，基线复现也同理（谎报只在
   部分迭代触发）。
5. **修复 repro 并回归**：submit 循环后补 `drop(tx1)`；`HR=1` 3×100s
   全部正常退出（零挂死）。修复后的 repro 偶发 `HR-LOSS got=492/498`
   属预期现象——那是 B2 在 repro 手写裸 crossfire collector（无
   youpipe 的 confirm 防护）里的直接体现。
6. **池侧结论**：`new_jobs` 启发式的 `queue_was_empty`/`awake_idle`
   组合、入睡最终检查只看注入器、`wake_specific_thread` 的
   mutex/condvar 交互——纸面推演均闭合（rayon 派生的 SeqCst fence
   握手：要么 poster 看到 sleeper 的 sleeping 提交而去唤醒，要么
   sleeper 的 fence 排在 poster 的 fence 之后从而 final check 必然看到
   push）；在被怀疑的原始负载 + 全量插桩下多轮运行也从未失手。
   sleep 协议无需改动，也不需要补新的 loom 测试。

#### 现象与证据（原始记录，保留供参考）

- 高压混合负载（16 fused churn + 8 stream + 4 组手写池 job 拓扑，repro
  `HR=1` 模式）下进程静默挂死（无任何输出，超时强杀）。
- gdb 全线程栈（复现约 40s 后抓取）：
  - **全部 24 个 `yp-pool-N` worker** 停在
    `Sleep::sleep → wait_until_cold → condvar.wait_until_internal`
    （即提交睡眠后等通知）；
  - 3 个 fused churn 线程停在 `std::thread::park`（scoped latch 等待，
    release 下 latch 帧被内联）；
  - main 停在 `scoped::scope`；
  - HR collector 线程停在 crossfire 阻塞 recv 的 park（~~其池 job 未被
    执行，channel 永不关闭~~ **此推断错误**：见上「结案报告」第 1、2 条，
    池 job 全部执行完、channel 不关闭是 repro 自己泄漏的 tx1 造成的）。
- **基线（HEAD worktree，/tmp/opencode/youpipe-base）同样复现**（timeout
  exit=124）。
- 对照：无 HR 的纯 16 fused + 8 stream 跑 90s 不挂 → 现在可知：只有 HR
  拓扑泄漏了终端 tx，stream/youpipe 自身的 spawn 端点管理没有此问题。

#### 嫌疑代码路径（pool/sleep.rs，rayon 派生协议）——全部排除

- `new_jobs`（sleep.rs:419）的唤醒启发式依赖两个**先读后写**的竞态量：
  `queue_was_empty`（inject 前读）与 `num_awake_but_idle`。若两者组合误判
  「醒着的空闲线程足以消化新 job」→ 一个 sleeper 都不唤醒。
  **排除**：awake-idle 线程每轮 idle 都重新 find_work（本地→注入器→偷），
  必然看到新 job；sleep 转换路径由 JEC 失配 + SeqCst fence 握手兜底。
- worker 入睡前的最终检查（sleep.rs:376 `has_injected_jobs()`）**只看全局
  注入器，不看其他 worker 的本地 deque**——
  **排除**：本地 deque 的 job 必然有一个活着的 owner（push 后 LIFO 自取），
  owner 不可能在自有 deque 非空时入睡（idle 循环先 pop 本地）；owner 阻塞
  在 job 内时它不算空闲、也不算睡。实证：挂死时注入器=0、全部 worker
  合法 park。
- `wake_specific_thread` 的 mutex/condvar 交互（sleep.rs:448）——
  **排除**：sleeper 从 mask.set 到 `condvar.wait` 全程持有 `is_blocked`
  mutex，waker 要么看到 `is_blocked=true` 正确唤醒，要么 owner 已在
  abort 路径自行清位。loom 模型亦覆盖。

#### 排查计划（原计划，已完成，带结论）

1. ~~最小化：逐项裁掉 HR 模式的要素~~ → 纯 crossfire 复现器一击定位
   （`recv_timeout` 自愈 + 诊断打印）。
2. ~~插桩 wake 路径与挂死状态 dump~~ → 已做（事件环 + 看门狗），结论
   见「结案报告」；插桩已全部移除（pool 三文件 diff 已还原）。
3. ~~给 sleep 协议补 loom 测试~~ → 不需要：协议无缺陷（现有 loom 模型
   已覆盖 park/wake 交错）。
4. ~~复现入口~~ → `cd /tmp/opencode/repro && HR=1 ./target/release/repro`
   （repro 已修复，仅作负载回归用；`pure.rs` 为纯 crossfire 诊断版）。

### 上游修复验证与 workaround 回滚（2026-09-05，已完成）

crossfire 3.1.20（f0eb610/771d047/5e9843f，upstream issue #70）修复了该
检查顺序竞态：`ChannelShared::try_recv` 改为先判 `is_tx_closed`（SeqCst），
仅关闭时走 `try_recv_final`（SeqCst 终检 pop）；误报只会落在 `Empty`
（安全），`Disconnected` 必然经过终检。youpipe 的 confirm workaround
随依赖升级一并回滚。

验证（两侧，干净构建 A/B）：

- crossfire 侧：fork test-suite（`-F tokio,time --release`）issue #70
  相关 26 passed（含我们贡献的 test cases）；纯 crossfire repro
  （cf.rs）：3.1.19 → 87/98 次/20s 丢失，3.1.20 → mpsc bounded ×3 轮、
  mpmc、unbounded、空闲（SPIN=0）全部零丢失。
- youpipe 侧：回滚后 `stress_streams_plus_assist_drivers` 12/12 通过
  （7+5 轮），全量 `cargo test`（默认 + all-features）全绿，clippy 无
  新增警告，原 repro 负载（16 fused + 8 stream）90s 正常退出。
- bench（对比 9/2 基线 = 3.1.19 + 同形状代码）：`cpu_unbalanced_stream`
  改善 7~17%（3.1.19 的 `try_recv` 未关闭路径也走 SeqCst 终检 pop，
  3.1.20 换成轻量 pop，collector 热路径直接受益）；`channel_throughput`
  无显著变化。注意：一轮在系统残留负载下跑出的「+56%/+85% 回归」是
  污染数据（对照组 std_mpsc 同步 +63%），空闲重跑后消失——对照组建模
  的 bench 必须在 load 归零后跑。

回滚内容：

- `state/stream.rs`：4 个 terminal drain 恢复 ed2098d~1（Closed 直接
  退出，不再阻塞 recv 确认）；
- `builder/typed/stream.rs`：stage / expand / fence forwarder 三个
  burst-drain worker loop 的同类 guard 一并移除；
- `Cargo.toml`：`crossfire = "3.1.20"`（下限约束，防止旧 lockfile 回退
  重新引入 bug）；docs/dev/streaming.md 的 worker recv loop 描述同步。

### 后续事项

- ~~向 crossfire 上游报 issue~~：报告草稿已就绪——`crossfire-bug-report.md`
  （repo 根目录），含最小复现形状与检查顺序根因；发布 issue 时可直接取用。
- ~~**B1 池丢唤醒专项排查**~~：已结案（repro bug，非库 bug），见上节。
- ~~crossfire 使用约束文档~~：已完成（state/stream.rs 模块注释 +
  docs/src/dev/crossfire-try-recv-bug.md）。
- ~~死锁修复的文档同步~~：已完成（tuning.md「Worker budget across stages」
  重写、core-types.md feeder 小节与 stage_budget 段落更新、StageOptions
  rustdoc 更新、prelude.rs 与两处 miri 测试注释修正、fence 大输入回归测试的
  miri 跳过理由改写）。

---

## 操作事故记录

- 调查中途误用 `git checkout` 丢弃过一次 stream.rs 的全部改动，已凭上下文完整重写。
  教训：调试期间勤提交 wip commit。
- 2026-09-05 验证 crossfire 3.1.20 时被 shell 后台 `&` 作用域坑出一次误判：
  `cargo update -p crossfire && cmd & cargo build` 里 `&` 把 update 连同
  cmd 一起放进后台，前台 build 抢先读了**旧 lockfile**，编出的仍是
  3.1.19 二进制——后续把它跑出的 84/85 次丢失记在了 3.1.20 头上，差点
  得出「上游修复不完整」的错误结论（随后 5 种插桩/改版复现器全部触发
  Heisenbug 式消失，反而暴露了矛盾）。教训：**版本敏感的 A/B 验证必须
  `cargo clean` 后干净构建，或 `cp` 备份产物 + `cmp` 比对指纹**，绝不
  信任「update 与 build 顺序执行」的隐式假设；写复合命令时先确认 `&`
  的作用域覆盖范围。
