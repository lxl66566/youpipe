# crossfire 阻塞 waker 设计空间

> 承接 [crossfire-waker-cache.md](crossfire-waker-cache.md) 的考古（下称"考古"）——它论证了 40B `ArcWaker` 分配的根因、`WakerCache` 的死因，并给出复活方案（§4，本文称**设计 A**）。本文回答下一个问题：**除了复活 A，还有没有更好的设计**。
>
> 考古对象：`/root/programs/fork/crossfire-rs`。**代码基线为 HEAD `6f761e0`**（= 3.1.20 + issue #70 修复）。注意：本文写作时 fork 工作树已存在一份**未提交的**设计 A 复活实现（改 `Tx/Rx/waker.rs/waker_registry.rs` 四文件），正被并行编辑；因此本文所有行号引用以 HEAD 为准，仅作粗定位，以函数名为锚。

## TL;DR

- **设计 C（per-thread 不死 waker + seq 标记队列项）**：把 waker 所有权从 per-episode/per-handle 挪到 per-thread。比 A 更小（只动 2 个文件、调用方零改动）、更安全（无 `ArcCell` 协议、无 `weak_count==0 && strong_count==1` 门、`UnsafeCell` 句柄写一次）、覆盖更全（MTx 多线程共享、冷启动、select 全部免费）。**推荐为主方案**。
- **设计 B（intrusive 栈上等待队列，parking_lot_core 模式）**：收益上限最高（零分配 + 可删掉整个 seq 机制），但 registry 结构重写、UB 面从"良性 miss"变成"漏摘链即 use-after-free"，违背 fork 可 diff 原则。**2026-09 已在 fork `waker-intrusive` 分支完整落地（含 miri/loom 验证）并 vendor 进本仓库实测：youpipe 负载下较 C 净回归 +8~36%（waker 高频/高竞争场景最大），C 维持 vendored 形态（§12）。**
- **设计 E（per-channel epoch + futex，eventcount 模式）**：教科书级简洁，但被四个结构性事实否决（fire 在 fast path 上、async/blocking 共享队列、select 单线程等多路、`WakeResult` 语义无对应物）。
- 设计 A（考古 §4）降级为 fallback：若 upstream 对 `thread_local` 有顾虑（嵌入式/loom 场景）再提。

---

## 1. 读代码得到的关键事实

所有设计共享的约束，每条给出代码依据与设计含义。**这些事实比任何设计方案本身更有价值**——它们划死了设计空间的边界。

### F1. 阻塞 waker 的唤醒目标是每线程常量

```rust
// waker.rs:100
pub(crate) enum ThinWaker {
    Async(Waker),
    Blocking(thread::Thread),   // new_blocking() (waker.rs:72) 填的永远是 thread::current()
}
```

`WakerInner`（waker.rs:137）的 24B 载荷里，唯一的非状态部分对同一线程**永远不变**。`update_thread_handle()`（waker.rs:175）这个"复用必须刷新句柄"的 #14 教训，在 per-thread 所有权下结构性消失：句柄只写一次、发布后只读。这是设计 C 的立足点，也是评价 A 的坐标——A 每次 pop 都要做一次 `UnsafeCell` 写（`update_thread_handle`），C 一次都不用。

**含义**：per-episode 堆对象是被 async 的 per-future waker 需求带出来的惯性设计；阻塞路径的标准模型（`std::thread::park`、tokio Parker、parking-lot）全部把 park 资源建模为 per-thread。

### F2. 跨次复用协议今天就有两处活体

**(a) episode 内复用**：`RegistryMulti::_reg_waker_blocking`（waker_registry.rs:371）的 `Some(waker)` 分支——假唤醒后同一节点 `reset_init()` + 重注册，无分配。

**(b) 对象级复用**：`SelectWaker::init_blocking`（waker_registry.rs:826）——同一个 `ArcWaker`（存在 `o_waker: UnsafeCell<Option<ArcWaker>>`，waker_registry.rs:799）跨整个 `Select` 生命周期的所有 park 轮次复用，`reset_init` + `cell.replace(weak)`。

**含义**：`reset_init + mutex 内盖 seq + push weak` 这条复用协议是作者已在生产运行、被 issue #14/#22/#34/#39 反复锤炼过的（考古 §3）。设计 C 没有发明任何新状态转移，只是把 (a) 的复用作用域从"一次调用内"扩到"一个线程内"。

### F3. 同一节点的重复 weak 今天就存在

`_reg_waker_blocking` 每次调用（包括 `Some` 复用分支）都会 `reg_waker()`（waker_registry.rs:305）→ `queue.push_back(weak)` 推一个**新** weak，而旧 weak 不摘。假唤醒 N 次的 episode，队列里就有 N+1 个指向同一节点的 weak。fire 逐个 pop，对重复项：`wake()` 见状态 `≥ Woken` 返回 `Skip`（waker.rs `wake()`），fire 循环继续（waker_registry.rs:615 `fire()`）。

**含义**：**"队列里有指向存活但已重臂节点的陈旧 weak"不是设计 C 引入的新状态，是今天就在发生的常态**。区别只在重臂发生在同一个 registry（今天，无害）还是另一个 registry（C，需要 seq 检测，见 §5.4）。

### F4. 陈旧队列项的两种既有语义

| 陈旧项指向 | 检测方式 | 处理 | 代码 |
| --- | --- | --- | --- |
| 已 drop 的节点（episode 结束 o_waker drop） | `weak.upgrade()` 失败 | pop 循环继续，跳过 | `pop_first`/`pop_again` 内 `if let Some(inner) = weak.upgrade()` 的 else 分支（waker_registry.rs:389/432） |
| 存活且为本 registry 重臂的节点（F3 的重复 weak） | `wake()` 内状态检查 | `≥ Woken` → `Skip`，不重复 unpark | waker.rs `wake()` |

**含义**：弱升级失败是 registry 的**天然 GC 路径**——所有"对象死后队列里剩什么"的问题都收敛到它。设计 C 让"死节点"变少（节点不死），于是需要给"陈旧但活着"的项补一个与 upgrade-fail 同构的检测（seq 不匹配），语义学完全对齐这张表。

### F5. seq 是 mutex 内盖的全局单调版本号，队列项不携带

`reg_waker()`（waker_registry.rs:305）在 registry mutex 内 `guard.seq.wrapping_add(1)` → `waker.set_seq(seq)`（Relaxed store，waker.rs:201 附近）→ push weak。seq 属于注册事件而非节点；#39 之后 seq 只活在 mutex 里（考古 §3.7）。`_clear_wakers`（waker_registry.rs:463）用它做摘除匹配（`_seq == old_seq` 命中、`_seq > old_seq` 停止回填）。

**含义**：队列项 `VecDeque<Weak<WakerInner>>`（waker_registry.rs:261）**不记得自己注册时的 seq**，因此 fire 无法区分"weak 指向的节点当前是不是为**本 registry** 注册的"。设计 C 的唯一配套改动就是让队列项携带注册时的 seq（+4B/项）。

### F6. blocking 与 async 共享同一个 registry 队列

`_reg_waker_async` 与 `_reg_waker_blocking` push 同一个 `queue`；async waker 是 per-future 的 `ArcWaker::new_async(ctx)`，future 可在任意时刻 drop，所以队列必须持 Weak。youpipe 侧同一底层 channel 同时挂 blocking（`MTx<mpmc::Array>`）与 async（`MAsyncTx<mpmc::Array>`）两套 handle（`crates/youpipe/src/handoff/channel.rs`）。

**含义**：任何"registry 不再持 Weak"的重设计（B 的 `*mut`、E 的无对象）都必须给 async 留轨，要么双队列要么 enum 分轨——这是 B 改动量大的主因之一，也是 E 的否决理由之一。

### F7. fire() 位于无竞争 fast path 上

`send()` fast path：`try_send` 成功 → `on_send()`（shared.rs:242）→ `recvs.fire()` → `pop_first` 第一步 `state.load(SeqCst) == MULTI_EMPTY → return None`（waker_registry.rs:389/293）。即**无竞争时 fire 只花一次 SeqCst 原子读**。

**含义**：任何替代 registry 的方案必须保持"无 waiter 时近零成本"。这单一事实否决了朴素的 eventcount（见 §6 R1），也是 parking_lot 用 bucket 状态字而非全局计数器的原因。

### F8. MTx 是 Sync；youpipe 则每 worker 独享 Tx

`unsafe impl<F: Flavor> Sync for MTx<F>`（blocking_tx.rs:323）——一个 `&MTx` 可被多线程并发调用 `send`，它们共享 per-handle 的 `waker_cache` 单槽（设计 A），并发 park 时互相 pop miss，**只有至多一个线程吃到缓存**。youpipe 的 `SyncSender::clone → MTx::clone → Tx::new` 让每个 pool worker 独享 Tx（考古 §4.2），A 在 youpipe 内没问题，但 crossfire 通用用户会踩。

### F9. blocking select：一个唤醒目标注册进 N 个 registry

select/select.rs:8-38 的设计注释 + 代码：`Select` 持一个 `Arc<SelectWaker>`（内含**单个** `ArcWaker` 与单槽 `WeakCell`）；每个 channel 的 `RegistryMulti` 通过 `reg_select_waker` 把 `Arc<SelectWaker>` 的 wrapper 存进 `selectors`（与 `queue` 平行的另一个 Vec，waker_registry.rs:260-262）；任一 channel fire 时 `pop_first` 先唤醒全部 selectors，wrapper 从 `SelectWaker` 自己的 cell 里 pop weak 唤醒那唯一的 waker。

**含义**：(1) select 自带对象级复用（F2b），不依赖也不受 blocking 侧缓存/TL 方案影响；(2) "一个唤醒目标、N 个 registry"是合法形态——任何 per-thread/per-handle 假设必须容纳它；(3) 设计 B 的 intrusive 单链表节点无法同时入 N 个链表，需 per-registry 节点拷贝（§4 难点 a）。

### F10. 40B 布局算术（继承考古 §1.3）

`Arc<WakerInner>` = Arc 头 16B + `AtomicU8` + `AtomicU32` + `UnsafeCell<ThinWaker>`（16B，niche 打标）= **40B**。这是直方图上 40B 档的唯一自然解释，也是所有方案要消灭的对象。

### F11. unpark token 一次性且可"串台"

`std::thread::park/unpark` 的 token 语义：unpark 先于 park 到达则 token 保存、下次 park 立即返回；token 属于线程而非等待点。因此"唤醒一个当前不在本 channel 等待的线程"= 一次**良性假唤醒**（该线程的 park 无理由返回，循环重查条件后重新 park 或继续干活）；`std::thread::park` 文档明确允许 spurious wakeup，所以 token 串台对**遵守 park 协议的任何代码**（包括用户自己写的 park）都是合法扰动。

**含义**：设计 C 在 `RegistrySingle` 上的唯一新行为就是这种串台（§5.5），其合法性由本条兜底。

### F12. WakerState 状态机的读写方

| 状态 | 写入者 | 场景 |
| --- | --- | --- |
| `Init = 0` | owner：`reset_init()` / `reset()` | 注册前重臂（Relaxed store，锁 hb 论证见 waker.rs:201 注释） |
| `Waiting = 1` | owner：`commit_waiting()` CAS(Init→Waiting) | double-check 确认要睡之后（#22 的窗口态修复） |
| `Woken = 3` | 对端：`wake()` store(Waiting→Woken) 或 CAS(Init→Woken) | fire / close |
| `Closed = 4` | owner：`abandon()` CAS(≤Waiting→Closed)；对端：`close_wake()` | 超时取消 / 通道关闭 |
| `Done = 5` | **无人写**（direct copy 化石：只作为 `cancel_reuse_waker` 的返回值与 blocking_tx 的死分支出现） | — |

**含义**：状态机的全部转移在 C 下不变（谁写谁读照旧），这是"C 没有新状态转移"论断的核对表。

### F13. ArcCell / WeakCell 的协议（collections.rs:7/70）

- `ArcCell::pop` = SeqCst swap(null)——独占取走；`try_put` = SeqCst CAS(null→ptr)，失败即丢弃。设计 A 的缓存槽。
- `WeakCell::replace` = SeqCst swap，旧 weak 被 drop——`RegistrySingle` 的单槽注册位（F9/F5）。
- `WeakCell::pop` = CAS 取走后 `upgrade()`，可能返回 None（弱已死）。

### F14. direct copy 的化石仍在 double-check 入口

`sender_double_check`（shared.rs:196）入口仍是 `try_send_oneshot(item.as_ptr())`，但 HEAD 的实现（crossbeam/array_queue.rs:146）已是"先双 SeqCst 判满、再试推一次"的**普通推送包装**，与"receiver 替睡着的 sender 代投"无关。读代码时不要被名字骗到——`_oneshot` 的语义已经换过一次灵魂。

### F15. 每个"多余"的原子都有前科（继承考古 §3）

`reset_init` 的 Relaxed vs SeqCst 之争、seq 的非原子化、`Init` 态的存在，全部对应 issue #22/#34/#39 或提交 `70859d0` 的显式论证。**改任何一处排序前先读考古 §3 的时间线**，不要凭直觉"简化"。

---

## 2. 读代码的方法论（经验记录）

1. **先建立"谁能在什么时刻持有引用"的枚举表**，再谈 UnsafeCell 写的合法性。A 的门（`weak_count==0 && strong_count==1`）、C 的"句柄写一次"、B 的摘链纪律，本质都是同一张表的不同版本。推演时永远问：此刻全进程还有谁能碰到这个对象？
2. **弱升级失败是天然的观察点**。读到 `if let Some(inner) = weak.upgrade() { ... } else { continue }` 就标记：这里在吸收"对象已死"的陈旧性。任何让对象不死的设计（C）都要为这段补一个同构替代。
3. **找"今天就已经在发生"的行为作为新设计的合法性锚点**。F3（重复 weak）让 C 的陈旧项不是新物种；F2 让 C 的复用协议不是新发明。新设计最安全的形态 = 已存活协议的作用域扩展。
4. **先查 fast path 谁在调用，再评价 registry 替代方案**。F7 一条事实就否决了教科书方案 E 的朴素形态；不看调用方就设计数据结构是本末倒置。
5. **git 前科优先于代码注释**。这个文件里注释与实现矛盾过（考古 §3.8 的 SeqCst 残留注释），行为学证据（F3/F4）优先于注释声明。
6. **并发编辑的会话里，引用必须锚定 commit**。本次会话中 fork 工作树在两次读取之间被并行修改（`git status` 发现未提交的设计 A 实现），行号在漂移；引用一律写 `HEAD 6f761e0 + 函数名`。
7. **行为差异用交错时间线推演到"谁被偷走了一次 fire"的粒度**。良性/恶性的分界不是"会不会多唤醒"，而是"会不会让某个本该被唤醒的 waiter 少掉一次确定的重试提示"（§5.4 的 stall 推演）。

---

## 3. 设计空间的三个正交轴

| 轴 | 取值 |
| --- | --- |
| **节点所有权** | per-episode 堆（现状）｜per-handle 单槽缓存（A）｜per-thread 不死节点（C）｜per-call 栈帧（B） |
| **registry 引用** | Weak + seq 版本（现状/A/C）｜`*mut` + 摘链纪律（B）｜无对象，futex epoch（E） |
| **唤醒协议** | `WakerInner` 状态机 + park/unpark（现状/A/C）｜纯 park/unpark token｜纯 futex wait/wake（E） |

设计 = 三轴取值的组合。A/C 沿所有权轴移动而保留协议；B 同时移动所有权与引用轴；E 三轴全换。改动半径依次爆炸，这也是推荐顺序的依据。

---

## 4. 设计 A：复活 per-handle 单槽 WakerCache（基线）

考古 §4 的方案，此处只做摘要 + 补充批判性分析（实现细节看考古与工作树未提交 diff）。

### 4.1 形态

`Tx`/`Rx` 各持一个 `WakerCache(ArcCell<WakerInner>)` 字段；`reg_waker_blocking` 签名加 `cache: &WakerCache` 参数，miss 时 `Arc::new`、hit 时 `update_thread_handle() + reset()`；两个出口（wake-success 的 `return_ok!`/`on_recv_waker`、fast-cancel 的 `cancel_reuse_waker`）push 回槽，push 前过门 `weak_count == 0 && strong_count == 1`。fork 工作树当前未提交实现即此（含 SeqCst reset 保守选项）。

### 4.2 优点

- patch 最小（~30 行），与 upstream 作者留在 `_cache_waker` 的注释意图完全一致，接受度最高；
- 无陈旧 weak：过门回收 ⇒ 队列里不会留下缓存节点的 weak ⇒ 无假唤醒、无 fire 被偷（对比 C）；
- 安全论证已被考古 §4.2 完整给出（一个不变量 + 三条推论）。

### 4.3 缺点（批判性清单）

1. **冷启动与 MTx 共享的单槽竞争**（F8）：每个新 handle 第一个 contended episode 必 miss；`&MTx` 被多线程共享时，单槽 `ArcCell` 只能让一个线程命中，其余线程照旧每 episode 一次 40B 分配——恰恰是负载最重的共享发送场景。youpipe 不踩（每 worker 独享 Tx），但这是给 upstream 的 patch，通用性缺陷要在 PR 里诚实说明。
2. **协议负担落在每次 episode**：pop 是 SeqCst swap、push 是 SeqCst CAS（F13），外加 push 时的 `weak_count`/`strong_count` 两次读。对比 C 的两次无竞争原子加减，A 每 episode 的元数据开销更高。
3. **`update_thread_handle` 的 UnsafeCell 写依赖门的独占论证**：pop 后写 `UnsafeCell<ThinWaker>` 的合法性完全建立在"门保证无人可达 + ArcCell swap 全栅栏"上。这是整个设计里最精细的 miri 面（作者当年正是"until we find out miri report of race"才注释掉缓存的，考古 §3.9）。C 里该写**不存在**。
4. **per-handle 常驻内存**：每个 Tx/Rx 永久多 8B 字段 + 至多一个 40B waker 驻留。稳态分配 ≈ handle 数。
5. **fast-cancel 出口不完整**：`_clear_wakers` 摘除后直接 drop（`ChannelShared` 拿不到 per-handle cache），作者 `XXX ... future review` 标记点（waker_registry.rs:463 内）在 A 下仍要第二步 plumbing 才能覆盖。

### 4.4 定位

最小可 upstream 化的修复；若 C 因 `thread_local` 顾虑被拒，A 是 fallback。工作树中的未提交实现可直接作为 A/B 实验的载体。

---

## 5. 设计 C：per-thread 不死 waker + seq 标记队列项（推荐）

### 5.1 核心思想

F1 说阻塞 waker 的身份就是线程。那么 waker 节点的自然所有权是 **per-thread**：每个线程一个惰性创建、线程退出才销毁的 `Arc<WakerInner>`（TL 槽）。registry 侧队列项从 `Weak<WakerInner>` 变成 `(Weak<WakerInner>, u32 seq)`，fire 时 seq 不匹配的项按"死节点 upgrade 失败"同构处理（F4/F5）。

与 A 的本质区别：A 回答"episode 结束后对象去哪"（进缓存槽，下次取出重臂）；C 回答"对象根本不该死"（TL 槽里活到线程退出，重臂即可）。因此 A 需要的缓存协议、门、句柄刷新在 C 里**没有存在的前提**。

### 5.2 代码草图

```rust
// waker.rs —— 新增，全设计中唯一的新存储
thread_local! {
    // Immortal blocking waker: the wake target of a blocking context is
    // thread::current(), which never changes. The ThinWaker handle is
    // written once, before first publication; re-arming only touches
    // `state` (atomically) and `seq` (under the registry mutex).
    static BLOCKING_WAKER: Arc<WakerInner> = ArcWaker::new_blocking().to_arc();
}

#[inline(always)]
pub(crate) fn tl_blocking_waker() -> ArcWaker {
    BLOCKING_WAKER.with(|inner| ArcWaker::from_arc(inner.clone())) // strong +1, no malloc
}

// waker_registry.rs —— RegistryMultiInner
struct RegistryMultiInner {
    queue: VecDeque<(Weak<WakerInner>, u32)>,   // +4B/项：注册时盖的 seq
    selectors: Vec<SelectWakerWrapper>,
    seq: u32,
}

// reg_waker：盖 seq 后 push (weak, seq)
// _reg_waker_blocking 的 None 分支：
let waker = tl_blocking_waker();        // 替换 ArcWaker::new_blocking()
self.reg_waker(&waker);                 // trait 签名不变，调用方零改动
o_waker.replace(waker);

// pop_first / pop_again 循环内，紧跟 upgrade：
if let Some((weak, seq)) = guard.queue.pop_front() {
    has_pop = true;
    if let Some(inner) = weak.upgrade() {
        if inner.get_seq() != seq {
            continue;  // stale entry of a re-armed node — same treatment
                       // as a dead node's upgrade failure (F4)
        }
        ...
    }
}
```

`RegistrySingle::_reg_waker_blocking`（waker_registry.rs:177）同样改用 `tl_blocking_waker()`，`WeakCell` 不变。**blocking_tx.rs / blocking_rx.rs / select / shared.rs 一行不改。**

### 5.3 生命周期（对照考古 §2 的格式）

```text
fast path   try_send 成功 → 零 waker、零分配（不变，F7）
本线程全局首次 contended episode
             TL 槽惰性 Arc::new ← 40B malloc，每线程恰一次
episode 开始   o_waker = None
  reg_waker_blocking()      TL clone（两次无竞争原子加减，无分配）
  reg_waker()               mutex 内：盖 seq、push (Weak, seq)
  sender_double_check / commit_waiting / park / fire   ← 与现状逐行相同（F12）
episode 结束   o_waker drop → strong 退回 1（仅 TL 持有）；不 push、不摘链、无门
  队列残留 (Weak, seq) → 下次被 pop 到时 seq 不匹配 → skip（≈ 死节点 upgrade 失败，F4）
假唤醒       reg_waker_blocking() 复用 o_waker（现役协议，F2a）✅ 不变
线程退出     TL 析构 drop Arc → 残留 weak 升级失败 → 既有跳过路径
```

### 5.4 为什么必须带 seq 标记：stall 交错推演

没有 seq 标记时，节点不死引入一个今天不存在的窗口——**陈旧 weak 升级成功并偷走一次 fire**。逐步推演（T1/T2/T3 为线程，ch1/ch2 为 channel）：

| 步 | 线程 | 动作 | 系统状态 |
| --- | --- | --- | --- |
| 1 | T1 | 在 ch1 上 send 阻塞，注册 TL 节点 w（seq=s1），weak₁ 入 ch1.senders 队列 | ch1 队列: [weak₁(s1)] |
| 2 | T1 | 假唤醒后 episode 结束（发送成功），o_waker drop；节点存活（TL） | weak₁ 残留，无人摘 |
| 3 | T1 | 后来在 ch2 上 send 阻塞，重臂 w（reset_init，seq=s2 盖于 ch2），weak₂ 入 ch2 队列；T3 也在 ch1 阻塞等待（weak₃） | ch1: [weak₁(s1), weak₃(s3)]；w 当前 seq=s2 |
| 4 | T2 | 在 ch1 上 recv 成功 → `on_recv` → ch1.senders.fire() → pop weak₁ → **upgrade 成功**（节点活！）→ `wake()`：状态 Init（为 ch2 重臂）→ Woken + unpark T1 | fire 被消耗在 T1 身上 |
| 5 | T1 | 从 **ch2** 的 park 中醒来（unpark token 串台，F11），重查 ch2 仍满 → 重臂 → 再 park | T3 未被唤醒 |
| 6 | — | ch1 有空位，但本轮 fire 已耗尽；T3 睡到 ch1 的**下一次** on_recv/on_send 事件 | 潜伏 stall |

不是死锁（任何后续事件都会再 fire），但它把 fire 语义从"每次事件精确唤醒一个 waiter"削弱为"最终会唤醒"。加上 seq 标记后第 4 步变为：pop (weak₁, s1) → upgrade OK → `get_seq() == s2 ≠ s1` → skip → 循环 pop weak₃ → 唤醒 T3。**精确性恢复，且与今天的 upgrade-fail 处理共用同一段代码路径**（F4 的同构替换）。

注意 F3 的对照：今天的重复 weak（同 registry 重臂）被 wake 时目标是"对的 channel"，顶多多唤醒；C 引入的才是"跨 registry 重臂"，所以必须补 seq。补上之后，同 registry 的重复 weak 也走 skip（多 pop 一项、唤醒同一节点，行为等价）。

### 5.5 RegistrySingle 为什么可以不补 seq

`RegistrySingle` 只出现在单侧不可 clone 的 flavor（spsc 两侧、mpsc 的 rx 侧），单槽 `WeakCell` + 唯一 handle + `!Sync` ⇒ **该 registry 至多存在一个潜在 waiter，且永远是同一线程**（F5/F9/F13）。陈旧 weak 串台只可能假唤醒那个唯一的相关线程（F11 兜底），不存在"另一个 waiter 被偷走 fire"的对象。单槽 `replace` 也不积累陈旧项。故 Single 侧零改动。

### 5.6 并发安全论证（对照考古 §4.2）

核心不变量从 A 的"只回收全进程无人可达的 waker"换成四条更局部的：

- **N1（句柄写一次）**：`UnsafeCell<ThinWaker>` 在 TL 惰性初始化时写一次，先于任何注册发布；此后唯一写者是原子字段（`state`）与 mutex 内的 `seq`。#14 类"句柄过期"竞态**不存在**——不存在过期。对比 A：每次 pop 都要 `update_thread_handle`，其写合法性依赖门的独占论证。
- **N2（状态机协议不变）**：F12 的读写方表格逐行不变。重臂 = 现役 `reset_init`（Relaxed + 注册 mutex 的 hb，waker.rs:201 的活注释；保守选项：TL 路径单独用 SeqCst store，每 episode 一次，成本可忽略，直接消解考古 §3.6/§3.8 两段论证的张力——A 的未提交实现已采用同样的保守选项）。
- **N3（陈旧性检测完备）**：一个队列项失去效力的方式恰有两种——节点死（upgrade fail，既有路径）或节点为别处重臂（seq mismatch，新增）。两者都表现为"pop 循环 continue"，与 F4 表格对齐。seq 的 u32 回绕论证继承考古 §4.2-③。
- **N4（死亡 = 线程退出）**：TL 析构只在线程退出时发生，而线程退出时它不可能还在任何 blocking 调用内部（否则线程没退出）。析构 drop 最后一个 strong（episode 结束时 o_waker 已 drop），残留 weak 升级失败。**无析构顺序假设、无泄漏**（40B/线程，随线程回收）。

对比 A 的论证负担：A 需要论证门的读数不竞态、pop 后独占写 sound、以及所有出口诚实走门；C 的 N1–N4 每条都是局部事实，loom 枚举空间显著更小。

### 5.7 代价与残留风险（诚实清单）

1. **良性假唤醒**：`RegistrySingle` 上的 token 串台（F11），以及 `RegistryMulti` 在 seq-tag 之前的极小窗口（seq-tag 后归零）。代价是一次空转 park-return。
2. **fire 循环多一次 upgrade 原子读**：仅当 pop 到陈旧项时；陈旧项密度由 F3 的既有动态决定（episode 与 fire 频率大体相当）。
3. **高扇入 worker 的队列残留**：线程轮流在 K 个 channel 上 park 时，每个 registry 积累指向它的陈旧项，由后续 fire 自清理。若实测成问题，对策是在"重注册且 registry ≠ 上次注册的 registry"时补一次 `_clear_wakers`（仅换 registry 时触发，youpipe 拓扑下从不触发）。
4. **TLS 访问开销**：`LocalKey::with` 一次/episode（非每 spin 轮），相对 futex 可忽略。loom 有 `loom::thread_local!` 模拟；若嫌烦，测试可用显式 per-thread 节点注入替代。
5. **upstream 接受度**：`thread_local` 在某些 no-std/embedded 场景受限（crossfire 目前是 std crate，不受影响，但要在 PR 里说明）；作者对 TLS 的偏好未知——这是 A 保留为 fallback 的唯一实质理由。

### 5.8 patch 面（文件级）

| 文件 | 改动 |
| --- | --- |
| `src/waker.rs` | +TL 槽 + `tl_blocking_waker()`（~10 行） |
| `src/waker_registry.rs` | 队列项改 `(Weak, u32)`；`reg_waker` push 元组；`pop_first`/`pop_again` 加 seq 检查（~15 行）；`_clear_wakers` 的 `process!` 宏可选改用 entry seq |
| 其他 | **零**（blocking_tx/rx、select、shared、collections 均不动；`WakerCache`/`ArcCell` 维持死亡状态） |

### 5.9 与 A 的逐项对比

| 维度 | A（per-handle 缓存） | C（per-thread 不死节点） |
| --- | --- | --- |
| 稳态 40B 分配 | ≈ handle 数（每 Tx/Rx 驻留一个） | ≈ 线程数（每线程一次；youpipe: N+2 vs 2N+2） |
| 冷启动 miss | 每 handle 首个 contended episode 分配 | 线程首个 episode 后永久免分配 |
| `&MTx` 多线程共享 | 单槽互踩，miss 方照旧分配 | 各用各的节点，天然无竞争 |
| 每 episode 元数据开销 | SeqCst swap + SeqCst CAS + 门读数 | Arc clone/drop（无竞争原子加减） |
| `update_thread_handle` | 每次 pop 一次 UnsafeCell 写 | 无（N1） |
| 陈旧 weak | 无（门保证） | 有，seq-tag 拦截（F4 同构） |
| 假唤醒可能 | 无 | 良性、有界（F11） |
| 改动文件 | 4 个（含 Tx/Rx 结构体与 trait 签名） | 2 个（调用方零改动） |
| 安全论证 | 门 + 三推论（考古 §4.2） | N1–N4（局部事实） |
| fast-cancel 出口 | 需二次 plumbing | 无出口概念，天然覆盖 |

---

## 6. 设计 B：intrusive 栈上等待队列（parking_lot_core 模式）——已试，实测回归（§12）

### 6.1 参照系：parking_lot_core 怎么做

parking-lot 是这个模式最著名的实现，结构：

1. 全局 bucket 数组（按锁地址哈希），每个 bucket 一把 mutex + 一个状态字；
2. **等待节点 `Waiter` 分配在阻塞调用者的栈上**，链入 bucket 的侵入式链表；持有 bucket mutex 是触碰他人节点的唯一许可；
3. 每线程一个 `Parker`，自带 EMPTY/PARKED/NOTIFIED 三态协议——保证"注册后、futex_wait 前"到达的唤醒不丢（unpark 先行则状态变 NOTIFIED，wait 直接返回）；
4. unpark 方在 bucket 锁内完成摘链与状态转移，锁外只做 futex_wake/unpark；等待方醒来后重新拿锁确认自己已不在链上，然后才允许栈帧返回。

这解决的核心问题与 crossfire 的 #22（Init 态）、#14（句柄）完全同构——crossfire 的 `WakerInner` 状态机本来就扮演 Parker 三态的角色。差别只在节点放哪、registry 持有什么引用。

### 6.2 映射到 crossfire

- `WakerInner` 整体保持 per-waiter 状态机，**搬到 `_send_bounded`/`_recv_blocking` 的栈帧上**；
- `RegistryMultiInner::queue` 从 `VecDeque<Weak<..>>` 改为侵入式双向链表 `*mut BlockingWaiter`（链接只在 mutex 下触碰）；
- episode 的**每个**出口（成功、超时 abandon、Closed、Done、fast-cancel）在返回前拿 registry mutex 摘除自己的节点；
- fire 方在 mutex 内 pop 节点、完成状态转移并 clone `Thread` 句柄（Arc bump，无 malloc），锁外 unpark。

### 6.3 安全纪律（不变量）

- **I1**：节点字段只可被持 registry mutex 的线程解引用；
- **I2**：owner 返回前必须持锁摘链——**每个**出口，漏一个就是 use-after-free；
- **I3**：锁外只允许对已 clone 的 `Thread` 句柄 unpark（parking-lot 的锁外 futex_wake 同型）。

### 6.4 收益（为什么它是上限）

1. **零分配，无条件成立**：第一场竞争、冷 handle、MTx 共享、select 全覆盖；
2. **陈旧项在结构上不可能存在**（I2 保证队列里只有活着的 episode）→ 以下机制**全部可删**：seq 全局版本号与 `set_seq/get_seq`、`_clear_wakers` 的 seq 扫描（cancel 变 O(1) 摘链）、`pop_first`/`pop_again` 的 upgrade-fail 循环、`WeakCell` 升级失败分支；
3. 无 Arc → 无引用计数流量、无门、无 weak/strong 推理。

### 6.5 难点（为什么现在不做）

1. **UB 面升级**：A/C 的失败模式是"缓存 miss / 多一次 skip"（良性）；B 的失败模式是漏一个摘链出口 = use-after-free。出口清单分散在 blocking_tx/blocking_rx/select/shared 四处宏里，枚举完备性的审查成本高。
2. **select 的单节点进 N 个链表**（F9）：侵入式双链节点只能属于一个链表，select 需要 per-registry 节点拷贝（SmallVec 栈数组）或为 select 单独保留堆节点——协议分叉。
3. **async 分轨**（F6）：async waker 必须留在 Weak/堆世界，队列要变成 `enum { Heap(Weak<..>), Stack(*mut ..) }` 或双队列，fire 循环双轨化。
4. **违背 fork 可 diff 原则**：AGENTS.md 要求 fork crate 与 upstream 可 diff、不手改；registry 结构重写只能走"先 upstream 化、再同步"的长路径，与本项的小步目标冲突。
5. miri/loom 重验成本最高（新 UB 面 + 双轨队列）。

**定位**：作为给 upstream 的长期提案（“registry 简化 + 零分配等待”），实施前先精读 parking_lot_core 的 `park_internal`/`unpark_one_inner` 作参照。

---

## 7. 设计 E：per-channel epoch + futex（eventcount 模式）——否决

### 7.1 模式（Vyukov eventcount / C++20 `atomic_wait` 同型）

```text
channel 侧状态:  epoch: AtomicU64,  waiters: AtomicUsize
prepare_wait:    waiters += 1
check:           重试 try_send/try_recv（含 double-check）
wait:            snapshot = epoch.load(); futex_wait(&epoch, snapshot)
cancel_wait:     waiters -= 1
notify_one:      if waiters > 0 { epoch += 1; futex_wake(&epoch, 1) }
```

等待者的登记完全交给内核的 futex 队列（按 `&epoch` 地址寻址），**用户态没有任何 per-waiter 对象**——注册、唤醒、陈旧性全部消失。这是理论上最彻底的答案：不是"复用 waker"，是"没有 waker"。

### 7.2 四个否决理由

- **R1（fast path）**：`on_send/on_recv → fire()` 在无竞争 fast path 上（F7）。朴素 eventcount 的每次 notify 都要 `epoch += 1` + `futex_wake`（无 waiter 时 syscall 立即返回 EAGAIN，但仍是 syscall）；加 `waiters` 门又要一个被两侧共同敲打的原子（每次 prepare/cancel 都写），把 fast path 的单次 SeqCst 读换成跨核流量。parking-lot 的 bucket 状态字正是为规避这一点而生的复杂度——想要它就得接受 B 级别的重写。
- **R2（async 共存）**：F6。async waker 没有 futex 可等（它要 wake 的是 executor 的 Waker），统一队列没了，blocking/async 必须双轨。
- **R3（select 多路等待）**：F9。blocking select 要单线程同时等 N 个 channel 的 epoch，而 `futex_wait` 只能等一个地址（Linux 的 `FUTEX_WAIT_MULTIPLE` 从未进主线）。
- **R4（fire 语义）**：`fire()` 的 `WakeResult::{Woken, Next, Skip}` + `last_seq` 停止条件实现了"逐个唤醒直到有一个真正消费了事件"的精确控制（waker_registry.rs:615）；futex_wake 唤醒谁由内核定，`WakeResult` 语义无对应物，on_send/on_recv 的联动（如 abandon 后的 `on_recv` 补偿，shared.rs:258 附近）需要重新设计。

**复活条件**：若未来 crossfire 分裂出"纯 blocking 专用 channel"且 fire 被移出 fast path（批量唤醒/惰性 fire），E 值得重评——它删掉的东西最多。

---

## 8. 其他否决项（短）

- **F. registry 持 strong、owner 不持**：episode 结束节点仍在队列里，晚到的 fire 会 unpark 一个已回到用户代码的线程——unpark token 串进**用户的** park（仍合法，F11），但队列里堆满已死 episode 的 strong 引用，且 owner 读自己状态需要 weak。不解决任何问题，纯劣化。
- **G. per-thread 的 WakerCache**：即"TL 槽里放一个单槽缓存"——缓存协议照旧、门照旧，只是把槽从 handle 挪到线程。相比 C 多此一举（C 的节点不死，根本不需要缓存协议），是 A→C 的过渡形态，无独立价值。
- **youpipe 侧注入 API**（`send_with_waker(&mut Option<ArcWaker>)`）：把节点生命周期交给 youpipe（per-worker 常驻）。能消灭分配，但 crossfire 要暴露 `WakerCache`-ish 公共类型、youpipe 每个 send 调用点要穿透一层——为回避 C 的 10 行 TLS 引入公共 API 面，不划算。
- **不修 upstream 的替代路径**（考古 §6）：批量 payload（摊薄 park 次数，40B 流量同比例下降，见 [todo.md](../../todo.md)）、collector 换 `std sync_channel`（已证伪）、fork 到 `crates/youpipe-crossfire`。与本文方案正交，可叠加。

---

## 9. 对比总表

| 维度 | 现状 | A 缓存复活 | **C per-thread** | B intrusive | E futex |
| --- | --- | --- | --- | --- | --- |
| 稳态 40B 分配 | 每 contended episode 一次 | ≈ handle 数 | **≈ 线程数** | **0** | **0** |
| 第一场竞争分配 | 有 | 有（每 handle） | **无（线程首场后）** | **无** | **无** |
| MTx 多线程共享 | 每线程分配 | 互踩，仍分配 | **免** | **免** | **免** |
| 改动文件数 | — | 4 | **2** | 6+（重构） | 6+（重构） |
| 新协议/机制 | — | ArcCell + 门 + 出口 | **无（现役协议扩作用域）** | 摘链纪律 I1–I3 | epoch + waiters |
| 失败模式 | 分配开销 | 良性 miss | 良性 skip/假唤醒 | **UAF** | 语义重设计 |
| seq 机制 | 保留 | 保留 | 保留（角色升级为陈旧性过滤） | **可删** | **可删** |
| async 共存 | — | 兼容 | 兼容 | 需分轨 | 需分轨 |
| select 支持 | — | 兼容 | 兼容 | 需节点拷贝 | **不支持（R3）** |
| upstream 化难度 | — | 低 | 中（TLS 顾虑） | 高 | 高 |
| 可删 `WakerCache`/`ArcCell` 尸体 | — | 复活使用 | 维持死亡 | 维持死亡 | 全删 |

> B 列为设计期预期；2026-09 实测（§12）——B 的"0 分配"兑现（本就与 C 同为实质零），但墙钟在高频 episode 场景较 C 回归 +8~36%，性能列败给 C。

## 10. 建议与验证计划

**路线**：C 为主方案（可先在 fork 上以丢弃式 patch 验证，或叠加在工作树 A 实现之上做同 binary A/B/C 三方对比）；A 为 fallback（工作树实现已在）；B 作为 upstream 长期提案另开 issue；E 记录否决理由备查。

**验证**（继承考古 §4.2 验证要求）：

1. **miri**（tree-borrows）：upstream test-suite + youpipe 集成测试。C 的重点核对 N1（TL 初始化与首次注册的 hb）与 seq-tag 路径。
2. **loom**：在 A 的"push→pop→re-reg→fire"清单上追加 C 特有两组——
   - 跨 channel 陈旧 fire：§5.4 交错表的全排列（T1 换 registry 后 ch1.fire() 必须命中 T3）；
   - 线程退出残留：T1 退出后残留 weak 被 pop，upgrade 失败路径不 panic。
3. **直方图**：`tests/expand_alloc.rs` 复测，40B 档预期从背压下千级塌缩到 ≈ 线程数（A 预期 ≈ handle 数，二者同数量级，均可用同一断言）。
4. **性能**：同 binary 交错 A/B（`bench_ab.sh`），预期与考古 §5 一致——<1% 墙钟、关注 p99 方差；A 与 C 互相对比的差额（episode 元数据开销）预计在噪声内，若 C 显著更好是加分项。

---

## 11. 落地验证（2026-10，design C 已合入，取代 A）

> 结论先行：**C 按本文蓝图落地并反超 A 的验收读数**——expand fanout-9 的 40B 分配从 A 的 62–433/run（同轮实测 233–433）降到**稳态 1/run**（每 run 常数级 ≈ 新线程 TL 槽），背压场景同样归 1；改动面与 §5.8 预期一致（2 个文件，调用方零改动）。落地过程中发现蓝图的三处遗漏/修正，见 §11.2。

### 11.1 fork 与落地形态

fork 分支 `waker-tl`（基线 = `waker-cache` 分支的 HEAD，保留 crossbeam 安全文档两提交），三个提交：

| 提交 | 内容 |
| --- | --- |
| `8ee2341` | 还原设计 A（四文件回到 upstream 6f761e0 字节态），使 C 直接对 upstream 出 diff |
| `a8c731b` | **设计 C 本体**：TL 不死 waker + 队列项 `(Weak, seq)` + pop/close 的陈旧项跳过 |
| `ee91652` | `NonNull::from(Box::leak)` 安全化（与 vendored 副本逐字节对齐） |

上表为当时的提交序列；分支后来重写为直接基于 master 的线性历史，当前 HEAD = master + 8 提交：`d53c9bd`（C 本体）、`c74d6bc`（NonNull 安全化）、`098b76e`（miri 路径测试 ×5）、`2aa1b0c`（loom 模型 ×5 + `feature = "loom"` 门控）、`5d13d1d`（科普文档 `docs/waker-tl.md`），以及为 Windows 可移植性追加的 `f77428c`（Safety 文档回填 fork）/`b14d3a2`（移除 captains-log dev-dep，其 unix libc 假设挡 Windows 编译）/`cec6948`（registry mutex 在 miri 下切 `std::sync`）。vendored `crates/youpipe-crossfire/src/` 与分支 HEAD `diff -r` 逐字节一致；`WakerCache`/`ArcCell` 维持死亡（§9 表格预期）。

### 11.2 对蓝图的三处修正（实测/推演发现）

1. **`close()` 也必须做 seq 检查**（蓝图 §5.8 只列了 `pop_first`/`pop_again`）。`RegistryMulti::close` 会清空整个队列；对"已在别处重臂"的不死节点调 `close_wake()` 会把 Waiting→Closed 盖在**另一个 channel 的现役 waiter** 上，醒来后表现为虚假 `Disconnected`——这是正确性 bug 而非良性假唤醒。同构处理：seq 不匹配跳过。已加针对性单测（`test_registry_multi_close_skips_stale_entries`）。
2. **seq 戳源从 per-registry 计数器改为全局 `AtomicU32`**。蓝图 §5.4 假设重臂后的 seq（ch2 计数器值）≠ 陈旧项的 seq（ch1 计数器值），但两个计数器只各自单调、数值互不相关——流水线里两侧计数器大致同步推进时**数值相等并不罕见**，相等即陈旧项冒充现役、偷走一次 fire（§5.4 的 stall 场景复活）。全局计数器使戳全进程唯一，"不匹配"严格等价于"已重臂"；单 registry 队列内戳仍严格递增，`_clear_wakers` 的排序逻辑与 `fire()` 的 last_seq 界 unchanged。代价：每 episode 一次被 mutex 保护的 `fetch_add`（futex 级成本下可忽略）。fork 单测中两处绝对 seq 断言改为相对断言。
3. **`_clear_wakers` 维持节点当前 seq 语义**（蓝图标注"可选改用 entry seq"——不改）。推演结论：entry-seq 在"episode 内重复注册后 fast-cancel"的交错下反而少摘一个陈旧项；节点 seq 语义的残留全部有界（fire 时被 seq-tag 跳过，同 §5.7-1 良性清单）。

### 11.3 验收数据（探针同 §1.2 方法，8192 输入 × 5 连 run，同轮双测）

| 场景 | upstream 3.1.20 | 设计 A（同轮实测） | **设计 C** |
| --- | --- | --- | --- |
| 双快 stage | 2–4 | 1–107 | 1–30（含首轮 TL 预热） |
| 背压 | 2–20（尖峰 387） | 14–17 | **1–3** |
| expand fanout-9 | 高位 ~2000 | 233–433 | **1（5/5 run）** |

C 的 40B 档在稳态**结构性归零**（总量 104 次/run，对 73,728 个 item）；A 的 fast-cancel 残余（"第二步" plumbing）在 C 里没有存在前提——无出口概念，天然覆盖（§5.9 表格预期兑现）。

### 11.4 墙钟 A/B（base = A@HEAD，side = C@工作树，交错 3 轮）

`target/bench-ab/waker-tl/compare-base-tl.tsv`（expand + async_vs_tokio + mixed_load + channel_bench，taskset 1-31）。

**youpipe 行（waker 高频场景）一致偏改善：**

| id | med A | med C | Δ% | dom C |
| --- | --- | --- | --- | --- |
| channel_throughput/youpipe_mpmc/10000 | 187.9 µs | 173.6 µs | −7.6 | 9/9（spread 4.1/1.4，stable） |
| channel_throughput/youpipe_mpsc/100000 | 2.396 ms | 2.086 ms | −12.9 | 9/9 |
| channel_throughput/youpipe_mpmc/100000 | 1.98 ms | 1.231 ms | −37.8 | 9/9（base 侧双峰 79% spread，快/慢模式翻转，非真实变化） |
| stream_pipeline/single_stage_unordered/100000 | 28.92 ms | 26.74 ms | −7.5 | 8/9 |
| stream_pipeline/single_stage_unordered/1000 | 287.4 µs | 279.2 µs | −2.8 | 9/9 |
| mixed_load/youpipe_stream_cpu/* | — | — | ±0.5 | noise |

**控制行（不经过 crossfire）标定本轮噪声带——base 侧系统性偏慢：**

| id | Δ% | verdict |
| --- | --- | --- |
| channel_throughput/std_mpsc_unbounded/100000 | **−42.4** | improvement*stable*（纯 std 通道） |
| expand_heavy/rayon_flat_map_iter_fanout=64/cpu | **−12.5** | improvement*stable*（spread 3.2/2.4） |
| expand_heavy/rayon_flat_map_iter_fanout=4/cpu | −4.4 | improvement*stable* |
| mixed_load/rayon_par_iter/* | −0.3~−0.8 | noise |

**回归检查**：无 stable 回归行（`std_mpsc_bounded/100000` +40.9% 为 131% spread 的双峰噪声；`multi_stage_2/100000` +3.5%、`push_emit_fanout=4/cpu` +5.4% 均 noise verdict）。

**结论**：youpipe 行 −3~−13% 的改善方向与幅度都在本轮控制行标定的会话级噪声带内（跨二进制布局效应，方向恰好有利于 tl 侧），**墙钟判定为中性偏正、不可归因**——与 §10-4 预期（A/C 差额在噪声内）一致。C 的兑现价值仍是 §11.3 的分配结构性归零；无回归则是放行依据。

### 11.5 验证状态与遗留

- fork：`cargo check`（default/tokio/async_std/trace_log/compat）零警告；`cargo test --release` 16+43 全绿（新增 2 个 C 专属单测：跨 registry 重臂后 fire 跳过陈旧项、close 不误伤）；upstream test-suite（tokio）并行 317/334、串行 **334/334 全过**（并行失败为已知 Drop 计数竞态）。
- youpipe：`cargo test --release` 全绿（含 15 轮全套 + 50 轮 pipeline_integration 压测）；clippy/fmt 零警告。**一次未复现挂起**：首轮全套测试与 fork 串行 test-suite 并发跑时，多个测试线程停在阻塞通道 park 上（形态像丢唤醒），杀掉后 66 次复跑零复现——归因存疑（高负载饥饿 vs 真实竞态窗口），miri/loom 验证（§11.6）未发现对应反例。

### 11.6 miri + loom 验证结论（2026-09，fork `cec6948` 起 vendored 同步）

**miri（tree-borrows，`-Zmiri-ignore-leaks`）**：

- fork / vendored `youpipe-crossfire --lib`：**21 全绿**（Windows 本机；Linux 交叉确认见下）——含 5 个 TL 路径测试（TL 槽身份与强计数、跨线程节点相异、假唤醒重臂重复项被 fire 跳过、§5.4 交错的真 park/unpark 端到端、线程退出残留弱引用走既有死节点路径）。tree-borrows 核对点 N1（TL 惰性初始化的 write-once `ThinWaker` 句柄先于任何注册发布）由此覆盖。
- youpipe 侧新增集成测试 `crates/youpipe/tests/handoff_channel.rs`（8 个，miri 下 2.8s）：经 `handoff::channel` 公共 API 端到端驱动阻塞 send 满 park+唤醒、阻塞 recv 空 park+唤醒、park 中对端 drop 的两个断连方向、mpsc send 满、**close-vs-重臂陈旧项的端到端交错**（ch1 陈旧 entry + ch2 现役 waiter，ch1 关闭不得虚假 Disconnect）、多线程竞争收发（miri 下 3P×2C×8 item）、`park_timeout` 超时/提前唤醒路径（超时 API 不在包装层暴露，经 dev-dep 直连 crossfire）。已纳入 `perf/verify/miri.sh`。

**loom（`--features loom`，LOOM_MAX_PREEMPTIONS=2）**：vendored 自带 5 模型全绿，已纳入 `perf/verify/loom.sh`。测试有效性经过突变验证：临时禁用三处 seq 比较后，前三个模型（跨 registry 陈旧 fire、close 跳过、重复项重臂）全部失败——模型真的能抓住 seq 机制失效。

**已知残留的定性**：重臂与 fire 的 seq Relaxed 传播窄窗口（两把锁间无 hb 边，fire 可能读到重臂前旧戳、陈旧项偷走一次 fire）经 `loom_fire_vs_rearm_race_recovers_within_one_event` 枚举确认**真实可达且良性**：有界恢复（下一事件唤醒）、绝不 Closed；不可用内存序修复（两步操作无法原子化），也无需修复。§11.5 的一次未复现挂起无对应 miri/loom 反例，维持"高负载饥饿"归因。

**youpipe 层不新增 loom 模型的理由**：`handoff/channel.rs` 包装层（~400 行）是无同步状态机的薄封装（错误枚举映射 + 类型包装 + clone 转发），没有可交错的原语；协议交错的模型检查由 vendored crate 内 5 模型承担，youpipe 侧可观察行为由 `handoff_channel.rs` 在真实线程 + miri 下覆盖；`handoff/notify.rs` 等真正含同步原语的文件已有各自模型（testing.md）。

**Windows 可移植性接缝**（详见 testing.md）：vendored 的 `reg_lock` 接缝在 `cfg(miri)` 下切 `std::sync::Mutex`（parking_lot 的 Windows futex 路径 miri 不可解释，同 youpipe-sys 的 shim 理由）；`captains-log` dev-dep（unix libc 假设）已从 fork 与 vendored 双侧移除——fork 由此恢复 Windows 本机全量可测。
- upstream PR：C 形态（2 文件、调用方零改动、无新协议）即本分支；A 分支保留为 fallback。

## 12. 设计 B 落地与 B vs C 对比（2026-09，B 否决，C 维持 vendored）

> 结论先行：**fork `waker-intrusive` 分支的 B（栈上侵入式 waiter 队列）完整落地并 vendor 实测后，在 youpipe 全部 waker 高频场景较 C 净回归 +8~36%，B 的"无条件零分配"收益（C 稳态已是 1/run ≈ 线程惰性 TL 槽）无法抵偿；工作树回滚到 C**。B 分支（含 miri 3-bug 修复与 loom 5 模型）保留在 fork 仓库，作为后续 upstream 提案或去 L3 重试的底座。

### 12.1 B 的落地形态（fork `waker-intrusive` @ `467ccbe`）

Stage 3 按本文 §6 蓝图 + fork `docs/intrusive-waiter-queue.md` 的精馏契约（C1–C5）落地：`WaiterNode`（栈上 pinned，`state: AtomicU8` + lock-guarded `prev/next`）+ `WaiterQueue`（registry mutex 内双链表，带 `len` 作 wake 预算）+ `SingleWaiterSlot`（Single 侧 `AtomicPtr` claim-then-handshake）；blocking 走侵入式轨，async 轨保持 `VecDeque<Weak>` + seq（§6.5-3 的双轨方案 a）；select 侧信道 `selectors: Vec<SelectWakerWrapper>` 不动（F9 在侵入式轨不存在）。注册路径带 L3 自旋（`reg_blocking` 内 try_lock + 注入的 ready-probe，spin→yield→阻塞兜底）。验证：fork `make test_loom` 5/5、lib 20/20、upstream test-suite 串行 334/334；miri（tree-borrows）种子扫全绿，过程中抓到并修复 3 个 loom/miri 才可见的 bug（single-slot claim 必须**无条件 SeqCst swap**（F7 load-first 在无 hb 边时丢唤醒）、快速路径 state 读必须 **Acquire**（Relaxed 无 hb → 链表 cell 数据竞争）、claimer 侧必须**字段域访问**（整节点引用的 tree-borrows protector 与 owner drop 冲突））——详见 fork 文档 §7.4。

vendor 侧（本仓库）验证：`--lib` 20/20、loom 5/5（49.7s）、`cargo test --release` 220/220、`expand_alloc` 契约绿、clippy 零警告。

### 12.2 墙钟 A/B（base = C@HEAD `6a84865`，side = B@工作树，交错 3 轮）

`bench_ab.sh -B expand -B async_vs_tokio -B mixed_load -B channel_bench`，taskset 1-31；完整表 `target/bench-ab/waker-intrusive/compare-base-intr.tsv`（目录保留，可 `-o` 追加轮次）。

**youpipe 行（waker episode 频率递增）：**

| id | med C | med B | Δ%（正=B 慢） | verdict |
| --- | --- | --- | --- | --- |
| expand_heavy/owned_vec_fanout=64/cpu | 134.1 ms | 182.7 ms | **+36.3** | stable（spread 6.3/3.9） |
| expand_heavy/push_emit_fanout=64/cpu | 139.6 ms | 183.9 ms | **+31.7** | stable |
| expand_heavy/push_emit_fanout=64/cheap | 136.8 ms | 180.1 ms | **+31.7** | stable |
| expand_heavy/owned_vec_fanout=64/cheap | 141.6 ms | 179.2 ms | **+26.6** | stable |
| expand_heavy/push_emit_fanout=4/cpu | 9.104 ms | 11.46 ms | +25.9 | stable |
| expand_heavy/owned_vec_fanout=4/cpu | 9.140 ms | 11.00 ms | +20.4 | stable |
| channel_throughput/youpipe_mpsc/10000 | 279.1 µs | 367.3 µs | +31.6 | 0/9 dominant（b 侧 spread 26%） |
| channel_throughput/youpipe_mpsc/100000 | 2.322 ms | 3.666 ms | +57.9 | 0/9（b 侧 231% spread，11.49 ms 离群轮，仍显著慢） |
| mixed_load/youpipe_stream_cpu/1000 | 9.058 µs | 9.934 µs | +9.7 | stable |
| stream_pipeline/single_stage_unordered/1000 | 9.153 µs | 9.889 µs | +8.0 | stable |
| stream_pipeline/single_stage_ordered/1000 | 9.167 µs | 10.03 µs | +9.4 | stable |
| stream_pipeline/multi_stage_2/1000 | 9.214 µs | 10.02 µs | +8.7 | stable |
| stream_pipeline/with_fence/1000 | 1.629 ms | 1.564 ms | **−4.0** | improvement 9/9（全表唯一改善行） |

**控制行（不经过 crossfire）标定会话偏置**：`expand_heavy/rayon_flat_map_iter_fanout=64/cpu` +12.7%、`fanout=4/cpu` +4.1% 均 stable（纯 rayon）——本轮跨二进制布局噪声系统性不利于 B 侧（与 §11.4 的反向偏置同现象）；其余控制行 ±3% noise。

**判定**：扣除控制行偏置后，expand fanout=64 行净回归仍有 ~+20~30%（幅度远超偏置且 spread 仅 2~4%、8 行 stable 一致）；低竞争的 mpsc/mixed/stream 1000 档 +8~9% stable（此场景控制行近零偏置，回归为净读数）。方向全表一致（除 with_fence 一行），**B 真实回归，C 维持**。

### 12.3 机制归因（嫌疑排序，未逐项仪器化）

1. **L3 注册自旋**（B 独有）：`reg_blocking` 的 try_lock+ready-probe 自旋在 fanout=64 → 单 registry 高竞争下空转烧核、加剧锁 convoy——回归幅度与竞争度正相关（fanout=64 +27~36% vs fanout=4 +10~26%）与此吻合。fork 文档 §7.5 本就遗留"L3 contention bench"待办（§12-量不出差异警告的兑现：量出来了，是负的）。
2. **episode 元数据更重**：B 每次阻塞收尾要"栈节点构造 + `thread::current()` clone（Arc bump）+ 双链表连接 + 状态机 CAS 往返（commit/abandon/unlink）"；C 是"TL 槽 re-arm 一次 SeqCst store + push 元组"。低竞争行（mpsc/mixed/stream 1000 档）的 +8~9% 与此量级相符。
3. fire 路径两侧相近（B：锁内 pop + Thread clone + 锁外 unpark；C：锁内 pop + Weak upgrade + unpark），非主要差异源。

若后续重试 B：先去掉 L3（或降级为仅双线程探测）、再考虑砍状态机往返（Single 的 claim handshake 与 Multi 的 abandon CAS 是否能共享一次 RMW）。

### 12.4 顺带发现：§11.5 的间歇挂起与 waker 设计无关（预存在问题）

B 侧压测（`pipeline_integration` 单二进制、无外部负载、90 s 超时判定）20 轮内第 7 轮挂起；随后 C 侧（stash 对照）同法 20 轮第 16 轮挂起——**两种 waker 实现均复现**，gdb 形态同 §11.5（池 worker 停 crossfire `Tx::send` 满 park、消费端停 `recv` park、hybrid_dispatch 停 LockLatch）。§11.5/§11.6 的"高负载饥饿"归因由此修正为**未定位的预存在问题**（youpipe 自身或两个 waker 实现共享的通道语义层）；每轮 ~5–15% 概率，已立 todo 跟踪。B 侧另有一次 10 轮全套循环内的挂起（多测试并发，形态同上）。

**2026-09-25 归因进展**：挂死的一个确定性成分已定位并修复——stream run 的 liveness 预算按
"单 run 独占池"计算，多个满预算 run 并发共享池时联合超订，全部 worker park 在通道
send/recv 上、排队的 stage-worker job（及其后在 injector FIFO 里的 fused chunk）永远无法
弹出。放大复现（4 个 barrier 对齐的 2-stage run，n=5000 > buffer）5/5 挂死，修复
（池级 parking lease，见 streaming.md "Pool-wide parking lease"）后 8/8 通过、
40 轮单二进制循环零挂。**但该修复未收束全部窗口**：多测试二进制并发压测
（pipeline_integration + scope_integration + 全套 `cargo test` 同跑）仍以低概率（约
1/8 组）复现挂死，且新样本与容量无关——挂住 run 的 n=100 远小于 256 容量（send 不可能
因 Full park），其中 budget 测试线程已退出而其 stage workers 仍 park 在 send 上
（closed 通道的唤醒未达），并存在「同一通道 collector recv park（空）与上游 send park
（满）并存」的矛盾。指向 crossfire blocking send/recv 的 fire 信号经济
（`RegistryMulti::fire` 一次只保证一个等待者的重试，wake 到 stale waker 时信号被浪费）
或另一未定位调度缺陷；release 符号内联混淆（多个 `spawn_stage` 单态化折叠）使回溯无法
保真重建 job 归属。下一步：debuginfo 构建的全并发复现器 + 按线程/waker 粒度的
trace_log 插桩，重点审 `fire` 的 pop_again seq 早停与 `_clear_wakers` 的竞争。

**2026-09-25 根因实锤并修复（RegistrySingle 状态真值源分裂）**：插桩复现器
（wepipe `crossfire-trace` feature 转发 crossfire `trace_log`，per-thread 文件 logger +
通道地址标识）在 3 进程并发 × 多轮循环下稳定复现（约 1/6 组），trace 完整重放了死链：

- 线程 F 的 `fire()`（`on_send` 触发）执行 `cell.pop()`（槽位置 null）后被抢占
  **826 ms**，`waker.wake()` 迟到；
- 期间 receiver 完成"消费 token → cancel（槽位残留）→ 下一个 episode 重新
  `reg`（`tl_blocking_waker` 复位节点为 Init 并把节点 weak 重新填入槽位）→ park"；
- 迟到的 wake 把**新代**节点置 Woken 并 unpark；receiver 醒来后 `get_waker_state`
  读的是**槽位占用**（被 re-arm 重新填充 → Init）而非节点状态 → 误判虚假唤醒 →
  再次 park；
- 此后节点 state 恒为 Woken，后续所有 `fire` pop 到它都走 `Skip`（state ≥ Woken，
  不 unpark）→ 事件流停止后**永久死锁**。这同时解释了此前全部残余指纹：n=100 ≪
  容量却 send park（senders 等的是这个永不醒的 collector 消费）、budget 线程退出
  而 workers 未收到 closed（close 的 fire 同样 Skip）、「同一通道 send 满 + recv 空
  并存」（双方认知过时）。

修复（waker_registry.rs）：

1. `RegistrySingle::get_waker_state` 改读 waker 节点状态（单一真值源，与
   `RegistryMulti` 一致）——延迟落地的 wake 无论迟到多久都会被 park 返回后的
   线程观测到；
2. `RegistrySingle::_fire` 增加 Skip 重试——pop 出的句柄节点已 Woken（re-arm 前
   的过期句柄）时重新 pop 一次，把信号交给 re-arm 后的新注册，消除
   "wake 与 cancel+re-arm 竞争落在 Skip 上"的残余窗口。

回归测试 `test_single_delayed_wake_after_rearm_is_observed` /
`test_single_fire_after_rearm_wakes_new_episode` 在旧实现上失败（读槽位返回
Init）、修复后通过。验证：插桩复现器 75 组 × 3 进程零挂（修复前约 1/6 组挂）；
crossfire 25 测试 + youpipe 全量 release 测试 + 双 crate miri + clippy 全绿。
`RegistryMulti` 的 `fire` pop/wake 同为两步，但 `get_waker_state` 本就读节点状态，
同构死锁不存在；跨 registry stale entry 偷一次 fire（Relaxed seq 无 hb，
loom 契约"一个事件内恢复"）仍是已知理论残余，事件流枯竭的闭环场景由本次
Single 修复消除了主通道（collector 通道），Multi 侧未观察到实例。
