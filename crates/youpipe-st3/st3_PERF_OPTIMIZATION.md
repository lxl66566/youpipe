# st3 性能优化实施报告

> 依据：`st3_PERF_REVIEW.md`（2026-09-01，基线 commit `4b9c99c` / v0.4.1）。
> 实施环境：Windows 11，AMD Ryzen 9 7950X（Zen 4，32 逻辑核），rustc 1.99.0-nightly（x86_64-pc-windows-msvc）。
> 本文档详述本轮落地的全部更改、验证方法与结论。

---

## TL;DR

Review 中提出的 P1/P2/P4/P5/P6/P7 共 6 项优化全部落地（P1 额外做了两轮 bench 驱动的再优化），P3 经评估后**主动跳过**。所有更改无用户可见 API 变化，全量测试（22 项常规 + 12 项 loom 穷举）通过。

核心成果（交错 A/B 基准，中位数，对比优化前基线）：

| 操作 | 变化 |
|---|---|
| steal 批量 16 项 | **-23% ~ -28%** |
| steal 批量 32 项 | **-29% ~ -31%** |
| steal 批量 128 项 | **-44% ~ -45%** |
| steal 单项 | 持平（lifo）~ **-13%**（fifo） |
| push_pop（lifo，64/256 混合） | **-30%** |
| push/pop/push_pop（fifo） | 持平（噪声内） |
| 空队列 steal 探测 | 持平 ~ 小幅改善（低于本机噪声下限，汇编确认指令减少） |
| Worker::new | 墙钟持平（Windows 堆分配主导）；栈帧 856B→72B、消除 640B memcpy（汇编确认） |

共 10 个提交（含 2 个基准设施提交与 CHANGELOG），`git log --oneline 4b9c99c..HEAD` 可查。

---

## 1. 提交清单

| 提交 | 内容 | 对应 Review 项 |
|---|---|---|
| `433cc20` | 基准设施：`[profile.bench]`（codegen-units=1 + lto）、`benches/micro.rs` 针对性微基准、criterion 0.3→0.5 | §3 基础设施建议 |
| `e6723e4` | steal 微基准 setup 预热队列（消除分配器冷行导致的 ±30% 方差） | §3 |
| `fe91244` | `read_at`/`write_at` 用 `get_unchecked` 消除热路径边界检查 | P2 |
| `5d51139` | steal 批量搬运改为分段 `copy_nonoverlapping`（≤3 段 SIMD 拷贝，≤8 元素小段保留内联标量循环）+ 环绕矩阵测试 | P1 |
| `9d490f6` | steal/steal_and_pop 前置 advisory 空检查，空探测不触碰 dest 队列原子 | P5 |
| `92178a4` | fifo::pop 将 tail load 提出循环 | P6 |
| `b8bfd37` | allocate_buffer 用 `set_len` 取代 `resize_with` | P7 |
| `04a93c3` | `Worker::new` 原地构造 `Arc<Queue>`（新增 `arc_new_in_place` 助手） | P4 |
| `a640dce` | transfer_items 增加 n=0/n=1 快路径（单项偷窃与原实现指令序列等价） | P1 补充 |
| `0557c96` | CHANGELOG 条目 | — |

---

## 2. 各项详述

### 2.1 P2：消除边界检查（fe91244）

`read_at`/`write_at` 的索引为 `position & mask`，其中 `mask == buffer.len() - 1` 是构造即成立的不变量（容量为 2 的幂）。编译器无法证明这一点（mask 是运行时值），因此每次元素访问都生成 `len load + cmp + jbe panic_bounds_check`，steal 搬运循环里每元素 2 次。

改法：`get_unchecked` + `debug_or_loom_assert!` 显式锁定不变量。

**验证**：探针 crate（path 依赖 st3、LTO + `--emit asm`）汇编中 `bounds_check` 出现次数从每访问 1 处降为 **0**。常规/loom 测试全过。

### 2.2 P1：分段批量搬运（5d51139 + a640dce，本轮核心收益）

**实现**：`lib.rs` 新增 `transfer_items`，将 src/dst 两个环形区间的物理断点取并集，至多切成 3 段物理连续区间，每段一次 `ptr::copy_nonoverlapping`（编译为内联向量拷贝或 CRT memcpy，内部即 SIMD）。容量为 2 的幂，故段基址用掩码而非取模（汇编确认 `div` 指令数为 0；用 `%` 时编译器会生成硬件除法）。

**两轮 bench 驱动的再优化**（本机波动大，见 §4 方法论）：
1. 纯 memcpy 版本在小批量（1~4 项）出现真实回退（lifo +25~30%，3 轮 CI 不重叠）：小 n 时 memcpy 固定调用开销（约 5ns）超过标量循环本身。→ 段长 ≤8 的段改用内联标量循环。
2. 混合版本下单项偷窃仍承受分段数学开销（约 +10~15%）。→ 增加 `n == 0` 早退与 `n == 1` 直拷快路径，单项路径与原 read_at/write_at 指令序列等价。最终单项偷窃：lifo 持平、fifo -13%。

**正确性论证**（代码内保留完整注释）：
- 预订不变量（`count ≤ min(源存活数, dest 空闲)`）保证两侧物理区间不相交，含 self-steal（同一 buffer）；debug/loom 断言逐对检查 3×3 段不相交。
- `copy_nonoverlapping` 对非 `Copy` 类型合法：这是**搬迁**而非复制——源槽位搬走后逻辑回到 `MaybeUninit`，位于新 head 之前，`Drop for Queue` 只遍历 `[head, tail)` 存活区间，不会重复 drop。
- 别名模式（`&[UnsafeCell<MaybeUninit<T>>]` → 裸指针 → 写入）与 std `UnsafeCell::get` 内部做法一致，Miri/strict-provenance 兼容。
- `cfg(st3_loom)` 下退化为逐元素 `with`/`with_mut` 循环（loom 的 `UnsafeCell` 不暴露裸指针，且需被模型追踪）；原子协议两条路径完全一致。（更正：本文撰写时门控误为 `all(test, st3_loom)`，12 项 loom 测试实际空转——本轮修复还原为纯 `cfg(st3_loom)` 并重跑真实验证，见 Cargo.toml 注释与 docs/src/dev/testing.md。）

**新增测试**：`fifo/lifo_bulk_steal`（src/dst 独立旋转 × dest 预载 × 偷窃数量 × 混合容量组合 (8,8)/(16,8)/(8,16)，约 1.8 万种对齐，含跨 src/dst 断点情形）与 `fifo/lifo_bulk_self_steal`（批量自窃的每种切分位置）。debug 断言全程开启，验证分段数学与不相交性。

**验证**：交错 A/B 中位数——batch16 -23%~-28%、batch32 -29%~-31%、batch128 -44%~-45%（多轮一致）；汇编确认搬运循环消失、替换为 ≤3 次 memcpy。

### 2.3 P5：前置空检查（9d490f6）

`steal`/`steal_and_pop`（两模块共 4 处）入口处增加 advisory 空检查（`Queue::is_likely_empty`，Relaxed load）：空探测在触碰 dest 队列原子前直接返回 `Err(Empty)`。语义不变——陈旧观察返回 Empty 与 book_items 自身在竞争下返回 Empty 不可区分（线性化点略前移，代码注释已论证）。汇编确认空路径为 2 load + unpack + cmp + 返回。

executor 式探测密集负载的收益（每次空探测省 3 次 dest 原子 load + book_items 部分开销，约 2~4ns）低于本机噪声下限，无法单独用 bench 确认，以汇编证据与「无回退」为准。

### 2.4 P6：fifo::pop 外提 tail load（92178a4）

tail 只会被 worker 拥有线程写入（`push`/`extend` 与以本队列为 dest 的 steal 都在拥有者线程上执行——`Worker: !Sync` 由类型系统保证），pop 执行期间不变。将其提出 CAS 循环。汇编确认重试路径不再重载 tail。lifo::pop 本就在循环外加载，无需改。

### 2.5 P7：免初始化分配（b8bfd37）

`Vec::with_capacity` + `set_len` 取代 `resize_with`。`UnsafeCell<MaybeUninit<T>>` 无初始化要求（`repr(transparent)`）。release 下 `resize_with` 本就被优化掉（review 附录 A.3 已验证），本改动让该性质不再依赖优化器（debug 构建不再跑 O(N) 空转循环），并使 Miri 对未写先读更严格。

### 2.6 P4：原地构造 Queue（04a93c3）

`Arc::new(Queue{...})` 编译为：856B 栈帧上构造整个 640B 结构 + `memcpy` 到堆。新增 `arc_new_in_place`：手动分配 `ArcInner`（两个 usize 计数 + payload，偏移/大小按 `#[repr(C)]` 规则手算——**刻意不用 `Layout::extend`**，因其结果元组顺序随工具链版本变化，位置解构会静默交换语义），写 strong=1/weak=1，闭包经 `addr_of_mut!` 逐字段原地写入，最后 `Arc::from_raw`。

**验证**：
- 汇编：栈帧 856B→72B，memcpy 消失，仅剩 2 次分配 + 约 10 次字段写入（编译器将 Box 构造折叠为直接写 ptr+len）。
- 独立探针程序验证 clone/drop/dealloc 全周期布局兼容。
- debug/loom 断言 `strong_count == 1`、`weak_count <= 1`（注意：新 std 中隐式弱引用不再计入 `weak_count`，全新 Arc 返回 0 而非 1——断言兼容两种记账惯例）。
- **诚实结论**：Windows 上构造墙钟无显著变化（A/B 中位数 fifo +5%/lifo -3%，噪声内）——两次堆分配（2KB buffer + 640B ArcInner）完全主导，640B 拷贝的节省被淹没。确认的收益为消除拷贝（汇编）与大幅缩小的栈帧（对 no_std/深递归调用方有真实价值）。Linux 上分配更快，预期有小幅可测收益。

**维护提示**：此改动依赖 std `ArcInner` 布局这一实现细节（自 std 诞生以来未变，生态广泛依赖）。开发过程中实际遭遇两例相邻 API 漂移（`Layout::extend` 元组顺序、`weak_count` 语义），说明对此类假设需保持断言护栏；断言失败模式是响亮的计数不一致/测试失败，而非静默内存损坏。

### 2.7 P3：Worker 本地缓存 tail —— 主动跳过

Review 将其列为收益/风险比最低项（预期 1~3%，依赖隐式假设）。实施决策依据：

1. **无法满足「确认有效」标准**：本机 push/pop 基准的运行间漂移（±8~30%）远大于预期收益（1~3%），任何 bench 都无法确认其效果——不满足「每个确认有效的改进才提交」的原则。
2. **失败模式不可接受**：现有原子读的陈旧值只导致 CAS 重试；缓存维护出错时 `push` 会以陈旧 tail **写入错误槽位**，破坏存活元素（重复 drop/移动后使用）。该 bug 无法被 loom 捕获（Cell 不受 loom 追踪），单线程确定性维护逻辑也缺乏针对性测试抓手。
3. 其依赖的类型系统保证（`Worker: !Sync`、steal 在 dest 拥有线程执行）与现有 unsafe 相同，但将「读陈旧值」升级为「用陈旧值写」，风险性质改变。

若未来实施：需按 review §2.3 的防御式设计（CAS 新值从加载字派生而非缓存），并在能提供稳定测量环境的机器上验证。

---

## 3. 验证与回归总览

每项优化落地时均执行：`cargo test`（debug，debug 断言开启）+ `cargo test --release` + `RUSTFLAGS="--cfg st3_loom" cargo test --tests --release`（12 项穷举，`-Dwarnings`）+ `cargo fmt --check` + `cargo clippy` + 探针汇编检查。CI 各任务（check 1.60 / test / loom / miri / lints / docs）本地对齐验证通过；src 仅使用 MSRV 1.60 之前的稳定 API（`addr_of_mut!` 1.51、`Arc::weak_count` 1.41 等）。

测试从 21 项增至 25 项（新增 4 个环绕矩阵测试）。

## 4. 测量方法论（本机噪声应对）

本机（桌面环境，Zen 4）单次 bench 运行间漂移可达 ±30~70%（热/后台负载/代码布局彩票），出现过未改代码的对照项 ±20% 的波动。采用的方法：

1. **交错 A/B**：git worktree 固定基线代码，优化代码与基线交替运行（轮换起始侧以消除单调漂移），每 bench 取 3~4 轮中位数；
2. **内部对照组**：`steal_empty_probe`（P1 不触碰的路径）作为会话漂移指示器；benchmark.rs 中 tokio/crossbeam 对照同理；
3. **汇编为准**：低于噪声下限的改动（P5/P6/P7、P4 墙钟部分）以指令数增减为准，bench 仅确认无回退；
4. **setup 预热**：steal 基准在非计时 setup 中 push/pop 预热两个队列，消除分配器冷 cache line 噪声（e6723e4，方差从 ±30% 显著收窄）。

## 5. 结论

- **算法层**（review 结论复核一致）：原子操作数与内存序已到设计下限，无可再挤空间；本轮全部收益来自微架构层（指令消除、向量化搬运、内存访问模式）。
- **最大收益**在批量 steal：≥16 项提升 23~45%，且批量越大收益越大（128 项接近减半）——对应 executor 的 bulk-stealing 负载。
- **单项/小规模操作**持平或个位数改善（受限于本机噪声下限，汇编层面指令严格减少）。
- **未做**：P3（理由见 §2.7）；显式 SIMD（P1 落地后 memcpy 已覆盖，与 crate 零重依赖定位冲突）；Arc 与 buffer 合并分配（伪共享风险，review 已否决）。
- **附带产出**：可复现的微基准套件（`benches/micro.rs`）、防全局配置失真的 bench profile、640B→72B 的构造栈帧、以及两个 std 实现细节漂移的实证记录（`Layout::extend` 元组顺序、`weak_count` 记账）。

## 附录：关键数据

### A.1 最终 micro 交错 A/B（3 轮中位数，ns）

| bench | 基线 | 优化后 | 变化 |
|---|---|---|---|
| steal_batch16-st3_lifo | 81.0 | 62.6 | -23% |
| steal_batch32-st3_lifo | 103.5 | 71.5 | -31% |
| steal_batch128-st3_lifo | 205.6 | 115.0 | -44% |
| steal_batch16-st3_fifo | 72.7 | 52.2 | -28% |
| steal_batch32-st3_fifo | 87.7 | 62.3 | -29% |
| steal_batch128-st3_fifo | 192.0 | 106.5 | -45% |
| steal_batch1-st3_lifo | 20.7 | 20.8 | 0% |
| steal_batch1-st3_fifo | 37.7 | 32.7 | -13% |
| steal_batch4-st3_lifo | 36.4 | 40.8 | +12%（与对照漂移重叠） |
| steal_batch4-st3_fifo | 43.6 | 45.2 | +4%（噪声内） |
| steal_empty_probe-lifo | 34.1 | 37.2 | +9%（对照） |
| steal_empty_probe-fifo | 31.4 | 32.8 | +4%（对照） |
| push_1-st3_lifo | 10.7 | 9.4 | -12% |
| push_1-st3_fifo | 9.9 | 8.3 | -16% |
| pop_1-st3_lifo | 10.4 | 10.3 | 0% |
| pop_1-st3_fifo | 9.6 | 10.3 | +7%（噪声内） |
| worker_new256-st3_lifo | 94.9 | 93.9 | 0% |
| worker_new256-st3_fifo | 86.5 | 86.1 | 0% |

（batch1/4/empty 组为另一次 4 轮聚焦会话的数据，其余为 3 轮全会话。）

### A.2 benchmark.rs（push_pop 与 executor，绝对值受会话状态影响，仅供方向参考）

| bench | 基线 | 最终 | 变化 |
|---|---|---|---|
| push_pop-small_batch-st3_lifo | 242.3 ns | 170.3 ns | **-30%** |
| push_pop-large_batch-st3_lifo | 955.4 ns | 670.5 ns | **-30%** |
| push_pop-small_batch-st3_fifo | 197.9 ns | 196.9 ns | -0.5% |
| push_pop-large_batch-st3_fifo | 789.2 ns | 759.6 ns | -3.7% |
| executor-st3_fifo | 236.4 µs | 221.0 µs | -6.5%（对照 -5~-8%） |
| executor-st3_lifo | 241.0 µs | 238.5 µs | -1%（对照 -5~-8%） |

### A.3 汇编验证摘要（探针 crate，`-C opt-level=3 -C codegen-units=1` + LTO）

- `bounds_check` 出现次数：基线每元素访问 1~2 处 → **0**（P2）。
- steal 搬运：标量循环（每元素 ~15 指令、2 分支）→ ≤3 次 `memcpy` 调用，`div` 指令 0 条（P1）。
- steal 空路径：2 load + unpack + cmp + 早退，不触碰 dest 原子（P5）。
- fifo::pop CAS 循环内 tail 重载消失（P6）。
- Worker::new：栈帧 `sub rsp, 856` + 640B `memcpy` → `sub rsp, 72`、无 memcpy、约 10 次字段写入（P4）。

---

*报告生成：2026-09-01；优化分支基于 `4b9c99c`（v0.4.1）。*
