# crossfire 阻塞 waker 缓存考古

> 支撑 [todo #6](../../todo.md)：streaming 数据面在背压下每次 park 分配 40 B `ArcWaker` 的根因、upstream（crossfire 3.1.20）的设计意图与演化史、重新启用缓存需要的改动与并发安全论证、以及对 youpipe 的预期影响。考古对象：`/root/programs/fork/crossfire-rs`（本地 fork，与 crates.io 3.1.20 同源）。

## TL;DR

- waker 缓存（`WakerCache` + `ArcCell`）是 upstream **真实生效过**的优化：2025-07 加入，2026-05 随 direct-copy 特性一起被块注释。它死于"与被处决的特性共享了泛型参数"的连带伤亡，**不是自身有 bug**。
- 3.1.20 里 `WakerCache` 位于 `waker.rs` L312–399 的 `/* */` 块注释内，**不是活代码**；活下来的只有 `collections.rs` 的 `ArcCell`（`#[allow(dead_code)]`）、trait 方法名 `cancel_reuse_waker`、以及 `_clear_wakers` 里作者留的 `// XXX, it's possible to reuse the waker, leave it for future review`。
- 重新启用约 30 行改动；并发安全的全部论证收敛在一个不变量上：**只回收"全进程无人可达"的 waker**（`weak_count == 0 && strong_count == 1` 门）。在此门下，复用与重新 `Arc::new` 在并发语义上不可区分。

## 1. 症状、真实数据与归因链

### 1.1 现象

给 `expand_emit` 写分配断言时（`tests/expand_alloc.rs`），全局计数分配器的总数在同 workload 下逐 run 剧烈抖动（几十到两万），"稳态零分配"类断言根本无法收敛。这不是 expand 的问题。

### 1.2 方法与真实数据

计数不可靠就换按尺寸的直方图（无锁固定桶 + 原子计数；Mutex+BTreeMap 版本自锁死——插桶本身要分配，持锁再分配撞上非重入锁）。以下为 2026-10 实测（临时 example，3 场景 × 多连 run，8192 输入 item，`stage(+1) → stage(*2)` 与 `expand_emit(9) → stage(*3)`，各含 feeder→stage→collector 共 3 个 channel hop）：

| 场景 | 40 B 分配数（逐 run） | run 总分配 | 其他主要分配档 |
| --- | --- | --- | --- |
| 双快 stage（无竞争） | 2, 2, 3, 4 | ~57 | 32B×30、256B×5、512B×5（常量级噪声） |
| 快生产 + 慢消费（背压） | 17, 387, 2 | 72, 442, 57 | 同上，40B 以外几乎不变 |
| expand fanout-9（73,728 下游 item） | 1690, 2653, 1654 | 1794, 2757, 1758 | 32B×45、64B×15、128B×15（scratch 倍增） |

三个直接读数：(a) 无竞争时 40B 档几乎是 0（个位数）；(b) 背压下爆发且**同 pipeline 三连 run 差两个数量级**（17/387/2，双峰）；(c) expand 场景里 40B 占总分配的 94–96%，是全 pipeline 第一大分配类，第二名（scratch 倍增）差 37 倍以上。

### 1.3 归因：为什么是 crossfire，不是 youpipe 自己

四步排除法：

1. **布局算术精确命中**。40 B 只有一种自然解释：`Arc<WakerInner>`。推导：`ThinWaker = enum { Async(Waker), Blocking(Thread) }`，`Waker` 16 B、`Thread` 是 8 B 的 `Arc` 指针，niche 打标后枚举 16 B（rustc 单独编译验证过 `size_of::<ThinWaker>() == 16`）；`WakerInner = AtomicU8 + AtomicU32 + UnsafeCell<ThinWaker>` 排布后 24 B；`Arc` 头（strong/weak 两个 usize）16 B；合计 16 + 24 = **40 B，分毫不差**。
2. **youpipe 热路径没有 40B 的产生者**。streaming 数据面自身的分配点只有：stage worker 的 scratch `Vec` 倍增（8 B 元素走 32/64/128 B 容量档，永远不是 40）、collector 输出 `Vec`（大块）、`ReorderBuffer` ring（建 ring 时一次性预分配）。feeder 与 stage 之间逐 item 转发本身无分配。
3. **行为学吻合 park-only 语义**。40B 计数与 item 数无关、与竞争强相关：无竞争时个位数（fast path `try_send` 无锁直投，根本不碰 waker），背压下千级。如果它是 per-item 路径的，无竞争时也应该是每 item 一次。
4. **源码对账**。crossfire 3.1.20 的 `blocking_tx.rs::_send_bounded` / `blocking_rx.rs::_recv_blocking` 里，`o_waker` 是每次调用从 `None` 起步的局部变量，spin 失败进入 park 前调 `reg_waker_blocking()` → `ArcWaker::new_blocking()`（恰好一次 `Arc::new`，40 B），episode 结束 drop。每 contended send/recv 尝试恰好一次，与直方图完全对得上。

### 1.4 结论的边界（诚实声明）

以上证明了"**背压下第一分配流量来自 crossfire 的 park 路径**"，这是可复现、可归因的事实。但它**尚未**证明是墙钟瓶颈：单次 malloc+free（glibc tcache 命中约 20–50 ns）相对于 park 的 futex 系统调用（µs 级）是小头。墙钟/尾延迟影响是 todo #6 的待办项（同 binary 交错 A/B）。本考古解决的是"这个分配该不该存在"——它本来就不该存在，upstream 自己写好了缓存又注释掉了。

## 2. 现状生命周期（3.1.20，复用已死版）

一次 contended send 的完整路径（rx 对称）：

```text
fast path  try_send 成功 → 零 waker、零分配
backoff    spin/yield 若干轮 → 仍满 ↓
episode 开始   o_waker = None
  reg_waker_blocking()      ArcWaker::new_blocking()  ← 40B malloc（缓存复活点 A）
  reg_waker()               mutex 内：seq 盖戳、push Weak 进队列
  sender_double_check()
  ├─ 有空位（假性竞争）→ cancel_reuse_waker()
  │    └─ _clear_wakers() 摘除 Weak → o_waker.take() → drop  ← 40B free（潜在 push 点 B）
  └─ 仍满 → commit_waiting()（Init→Waiting CAS）
       └─ park()（futex）… 对端 on_recv() → fire() → pop_first() 摘 Weak
            → waker.wake()：Waiting→Woken store + unpark
       └─ 醒来重试 try_send 成功 → return_ok!() → drop ← 40B free（注释掉的 push 点 C）
episode 内假唤醒：reg_waker_blocking() 复用 o_waker（reset_init + 重注册，无分配）✅ 仍活着
episode 结束：末个 waker drop
```

注意 episode **内**的复用今天还在（假唤醒后同一个 waker 重置重注册）；死掉的只是**跨 blocking 调用**的桥接——A↔（B/C）。

## 3. 时间线：作者每一步为什么这样改

按"作者当时遇到什么问题 → 做了什么 → 为什么"展开。所有引用来自 fork 仓库 git log 原文。

### 3.1 2025-07-07 `b7d259d`：缓存诞生

> "Add cache for LockedWaker in blocking context — Implement ArcCell"

阻塞式 send/recv 在 channel 满时必须睡觉，睡前要往注册表里放一个"叫我起来的句柄"（waker）。句柄是 `Arc` 包装的堆对象，拥塞下每轮 park 都造一个新的、醒来又扔掉——分配开销随拥塞程度放大。作者的解法就是标准的对象池：`Tx`/`Rx` 各持一个单槽缓存（新写的 `ArcCell`，一个 `AtomicPtr` 的 swap/CAS 单槽协议），miss 才 `Arc::new`，两类出口（假性竞争取消、唤醒后发送成功）都 push 回去：

```rust
// b7d259d 的 blocking_rx.rs（节选）
let waker = cache.new_blocking();          // miss 时才 Arc::new
loop {
    if let Some(item) = shared.try_recv() {
        shared.on_recv();
        cache.push(waker);                 // 出口①：假性竞争（注册后发现其实有空位）
        return Ok(item);
    }
    // … park、被唤醒 …  出口②：wake-success 后 push（tx 侧仅在 is_full() 时，见 §4.3）
}
```

### 3.2 2025-07-16 issue #14：虚假唤醒后 waker 失效（死锁前科 ①）

单任务 tokio runtime 的压测里，`ReceiveFuture` 被 runtime 虚假唤醒后**没有重新注册 waker**；sender 后来 `fire()` 时按旧句柄去 wake，唤不醒。作者的修法（`95f6623`）：每次轮询断言 `will_wake()`，不满足就重注册。对复用方案的教训：**唤醒目标（线程句柄 / Waker）会过期，复用必须刷新**。

### 3.3 2025-07-18 `8369154` issue #22：注册与入睡之间的窗口态（死锁前科 ②）

注册完 waker、还没确认"确实要睡"的窗口里，对方可能恰好投递并 wake。若此时 waker 已处于"可唤醒"态，唤醒会被这次投递消耗掉，而本线程随后才去 park——错过唤醒，永久睡死。修法：引入临时态 `Init`（"已注册、未提交"），`commit_waiting()` 用 CAS 把 `Init → Waiting` 放在 double-check（确认没有错过投递）**之后**。这就是今天 `WakerState::Init = 0 // A temporary state` 的来历。

### 3.4 2025-07-18 `967c4c9`：direct copy 诞生（后来的祸根）

> "Implement direct copy — When on_recv() see a ptr, it will try_send for sender. Which can help alot on congestion, can lower the failure order"

拥塞时 sender 睡着也是白睡——它醒来才能重试投递。direct copy 的思路：sender 睡前把 payload 的**指针**留在 waker 里；receiver 腾出空位后**替沉睡的 sender 把消息写进去**，唤醒时消息已经投递完成（`WakerState::Done`），直接少一轮完整的唤醒-重试。为了让 waker 携带 `*const T`，`WakerInner` 获得了 `<P>` 泛型，发送侧缓存类型从此变成 `WakerCache<*const T>`——**缓存与 payload 纠缠在一起，这是后面一切的关键**。

### 3.5 2025-08-29 `06b17a1` / `5454248` issue #34：miri 抓到挂起（死锁前科 ③）

> "waker: Fix reset_init() ordering to SeqCst — to prevent reorder and delaying, the otherside could see Waked and ignored the waker."

复用路径上 reset 的排序太弱，对端可能读到陈旧的"已唤醒"值而跳过本该做的唤醒 → 挂死。修法把 `reset_init` 升到 SeqCst（同日另一提交修 `cancel_waker` 的 miri 挂起报告）。

### 3.6 2025-09-11 `70859d0`：作者亲自论证排序要求（重要证据）

> "locked_waker: Argue about ordering — wake() & wake_or_copy can use Relaxed. **reset() & reset_init() should use SeqCst to clear cpu-cache due to reusing the waker.**"

此时缓存是活的，作者明确写下：**因为存在池复用，reset 必须用 SeqCst 清其他核的 store 缓冲**。这是论证复用安全性的原始依据，§4.2 会调和它与后来代码的差异。

### 3.7 2025-09-18 `0c4b824` issue #39：aarch64 上的 seq 怪象（前科 ④）

release 构建、aarch64-apple-darwin 上，`get_seq/set_seq`（当时还是 Acquire/Release）出现重复序号的日志，疑似编译器重排。修法干脆把 seq **非原子化**（当前 master 里 seq 的 load/store 都是 Relaxed，只靠 mutex 内使用保证一致）。教训：这个文件里每个看似多余的原子都是踩坑换来的；seq 从此与"原子性"脱钩，只与注册表 mutex 绑定。

### 3.8 2026-01 registry 重构：`reset_init` 降回 Relaxed

`68d0388/cf20832/fb4305a` 系列重构（`Registry` enum → trait、`locked_waker.rs` → `waker.rs`）把 `reset()` 并入 `reset_init()` 并降为 Relaxed，新论证写在代码里：

```rust
pub fn reset_init(&self) {
    // this is before we put into registry (which will extablish happen-before relationship),
    // it safe to use Relaxed
    self.state.store(WakerState::Init as u8, Ordering::Relaxed);
}
```

注意此时缓存**仍然接线**（`reg_waker_blocking(&mut o_waker, &self.waker_cache, …)`），即作者在复用仍存在的前提下接受了这个论证；但死代码里"should use SeqCst fence"的旧注释没删，两段论证的调和见 §4.2。

### 3.9 2026-01-17 → 2026-05-14 issue #54：direct copy 处决，缓存连坐

```text
f036dd5 (2026-01-17)  Disable direct copy for now — To make miri happy — issue #54
a2ed53d (2026-01-18)  Reenable direct copy
fb1af1e (2026-05-14)  waker: Remove generic from WakerInner
                      Since direct copy is removed (due to issue #54)   ← 此版本即 3.1.20
```

direct copy 本身有 miri 报的 UB（issue #54；本地仓库只有提交信息引用，细节在 upstream issue 里）。1 月禁用、次日想修又启用，最终 5 月彻底放弃。`fb1af1e` 的连锁动作：

```diff
-    waker_cache: WakerCache<*const F::Item>,      // Tx 的缓存字段删除
-    shared.recvs.reg_waker_blocking(&mut o_waker, &self.waker_cache);
+    //                self.recvs.cache_waker(o_waker, &self.waker_cache);   // 调用点注释掉
+/*                                                    ← waker.rs L312 块注释开始
+impl<T> WakerInner<*const T> { … wake_or_copy … }
+pub struct WakerCache<P: Copy>(ArcCell<WakerInner<P>>); …   ← 缓存实现整段进去
+*/                                                    ← L399 结束
```

**为什么删 direct copy 会杀死缓存**：`WakerInner` 去掉 `<P>` 泛型后，`WakerCache<*const T>`（发送侧缓存的类型）与 `reset(payload)`（复用时重置 payload）失去了存在的类型基础，编译不过。作者面前有两条路：把缓存按新类型重写，或者连同 direct copy 的尸体一起注释掉。他选了后者——缓存不是被证伪的，只是**没有大到值得单独救**。证据是尸堆上的活注释：

```rust
// waker_registry.rs（活代码）——fast-cancel 摘除 waker 时作者自己标的复用候选点：
if _seq == old_seq {
    trace_log!("{}: clear {:?} hit", self._tag, waker);
    // XXX, it's possible to reuse the waker, leave it for future review
    true
}
```

外加 trait 方法名 `cancel_reuse_waker`（"reuse" 是化石）和注释里保留的旧调用签名——作者明确把复用留作 future review，这正是提 patch 的切入点。

### 3.10 时间线总结

| 时间 | 提交 | 作者遇到的问题 | 动作 | 对缓存的影响 |
| --- | --- | --- | --- | --- |
| 2025-07-07 | `b7d259d` | 拥塞下每次 park 造/扔一个 waker，分配开销放大 | 加 `WakerCache`（per-Tx/Rx 单槽）+ `ArcCell` | **诞生，全接线** |
| 2025-07-16 | `95f6623` (#14) | 虚假唤醒后旧 waker 唤不醒人（死锁） | `will_wake()` 断言 + 重注册 | 间接：复用必须刷新句柄 |
| 2025-07-18 | `8369154` (#22) | 注册与入睡间有窗口，唤醒被提前消耗（死锁） | 引入 `Init` 态 + `commit_waiting()` CAS | 间接：复用必须重置回 `Init` |
| 2025-07-18 | `967c4c9` | 拥塞下 sender 睡着白睡，醒来才能重试 | direct copy：waker 携带 `*const T`，receiver 代投 | `WakerInner<P>` 泛型化，**缓存类型与之纠缠** |
| 2025-08-29 | `06b17a1` (#34) | reset 排序弱，对端读到陈旧 Woken 跳过唤醒（挂死，miri 抓到） | `reset_init` 升 SeqCst | 修的正是复用路径 |
| 2025-09-11 | `70859d0` | （无 bug，主动整理） | 写下"复用 ⇒ reset 必须 SeqCst"论证 | 复用排序要求的**原始依据** |
| 2025-09-18 | `0c4b824` (#39) | arm release 下 seq 出现重复序号 | seq 非原子化，只靠 mutex | seq 与原子性/分配身份解耦 |
| 2026-01 | `68d0388`…`fb4305a` | registry 结构性重构 | `reset_init` 降回 Relaxed（"注册前无 hb 需求"） | 缓存仍接线，排序论证变更 |
| 2026-01-17/18 | `f036dd5`/`a2ed53d` (#54) | direct copy 触发 miri UB | 禁用 → 次日修复启用 | 暂无影响 |
| 2026-05-14 | `fb1af1e` (#54) | direct copy 的 UB 修不干净，放弃 | 永久移除 direct copy，`WakerInner` 去泛型 | **缓存编译不过 → 整段块注释**（连带伤亡） |

一句话总结整条线：**缓存生于"park 不该分配"，缠身于"direct copy 需要 payload"，殉葬于"direct copy 被 miri 处决"**。四个 ordering bug（#14/#22/#34/#39）没有一个指向缓存本身的逻辑错误，全是 wake 生命周期的一般性问题，且各自的修法恰好构成复用方案的安全清单。

## 4. 重新启用：改哪里，怎么保证并发安全

### 4.1 需要的改动（4 处，约 30 行）

```rust
// ① waker.rs：从块注释复活 WakerCache 并去掉 <P> 泛型（payload 已不存在）：
pub struct WakerCache(ArcCell<WakerInner>);
impl WakerCache {
    pub fn new_blocking(&self) -> ArcWaker {
        if let Some(inner) = self.0.pop() {
            inner.update_thread_handle();   // 刷新线程句柄（#14 教训）
            inner.reset_init();             // 重置回 Init（#22 教训）
            return ArcWaker::from_arc(inner);
        }
        ArcWaker::new_blocking()
    }
    pub(crate) fn push(&self, waker: ArcWaker) {
        debug_assert!(waker.get_state() >= WakerState::Woken as u8);
        let a = waker.to_arc();
        if Arc::weak_count(&a) == 0 && Arc::strong_count(&a) == 1 {
            self.0.try_put(a);              // 承重门，见 §4.2
        }
    }
}
// ② blocking_tx.rs / blocking_rx.rs：Tx/Rx 恢复 waker_cache 字段（fb1af1e 删掉的）。
// ③ waker_registry.rs：_reg_waker_blocking 的 else 分支从缓存取（trait 签名加 &WakerCache 参数
//    ——注释里保留的旧调用签名就是这个意图）。
// ④ 两个出口取消注释 push：rx 的 on_recv_waker 宏、tx 的 return_ok! 宏。
```

### 4.2 并发安全：一个不变量 + 三条推论

**核心不变量（`push` 的门）**：只回收满足 `Arc::weak_count == 0 && Arc::strong_count == 1` 的 waker——即全进程**无人可达**这个对象：注册队列里的 weak 要么已被 `pop_first` 摘除（wake 路径，fire 摘 weak 在 unpark 之前），要么已被 `_clear_wakers` 摘除（假性竞争的 fast-cancel 路径）；调用线程持有唯一 strong。这个门成立时，**复用与重新 `Arc::new` 在并发语义上不可区分**，一切安全性论证都从它推出来：

1. **门的读数本身会不会竞态？** weak 只能由持有 strong 的 `Arc::downgrade` 产生（注册发生在 mutex 内的 `waker.weak()`）。push 时刻队列无 weak、他人无 strong，因此不存在任何线程能在读数后凭空造出新引用；两次独立原子读不构成窗口。唯一要求是所有出口都诚实走门：wake-success 与 fast-cancel 满足；超时/弃置路径 waker 直接 drop 不入缓存；`debug_assert!(state >= Woken)` 拦截未完成生命周期的入池。这是纯推理结论，patch 中必须以注释固化，并用 loom 枚举交错验证（loom 的 Arc 实现有等价的 weak_count 语义；若断言不便，可用镜像不变量"队列中不存在 strong>1 waker 的 weak"替代）。
2. **陈旧状态/陈旧句柄会不会被读到？** 门 ⇒ 无并发观察者。pop 是 `ArcCell` 的 SeqCst swap（全栅栏、独占取走），随后 `update_thread_handle` 经 `UnsafeCell` 的写、`reset_init` 的 Relaxed store 都发生在"独占所有权"下——sound。下一个可能的读者（fire 线程）只能在我们重新注册（mutex 下盖 seq + push weak）之后从队列拿到引用，mutex 提供 happens-before，所有写对它可见。这也调和了 §3.6 与 §3.8 的两段排序论证：`70859d0` 的"复用必须 SeqCst"是在门与 `ArcCell` 全栅栏语义未被明说时的保守写法；2026-01 的 Relaxed 论证在门的保护下成立。**保守选项**（推荐 patch 采用）：池复用路径单独加一次 SeqCst store，每 episode 一次、非每轮，成本可忽略，直接消解两段论证的冲突，也顺手覆盖 #34 的历史教训。
3. **seq 会不会 ABA？** seq 不是 waker 的属性，是注册表在 mutex 内盖的**全局单调版本号**（`reg_waker` 里 `guard.seq + 1 → waker.set_seq()`）。复用的 waker 重新注册必然拿到严格更大的新 seq；`_clear_wakers` 的 `_seq == old_seq` 命中语义在复用下不变；u32 回绕需要 2^32 次注册（每次对应一次 park episode，物理不可达，且遍历停止条件用 `>` 不用 `==`）。这是 #39 之后"seq 只活在 mutex 里"的直接红利。

**多线程共享**：`MTx` 虽是 `Sync`（可被 `&` 共享），但 `ArcCell` 的 pop（SeqCst swap）/try_put（SeqCst CAS）是原子单槽协议，并发下至多一个线程 pop 到对象，最坏只是命中率抖动，无正确性问题。youpipe 侧更干净：`SyncSender::clone → MTx::clone → Tx::new` 让每个 pool worker 独享一个 Tx，缓存天然线程私有，句柄刷新形同虚设。

**出口覆盖**：tx 侧 push 在 `if shared.is_full()` 内是**原设计有意为之**——channel 不满时下一轮大概率走 fast path 不再需要 waker，缓存无收益还多一次 CAS；持续满载（正是 40B 爆发的场景）才复用。fast-cancel 出口（`_clear_wakers` 摘除后目前直接 drop）的入池要动 `sender_double_check` 的调用方（`ChannelShared` 拿不到 per-handle 缓存），patch 第二步可做，也正好实现作者 XXX 注释标记的 "future review" 点。

**验证要求**：miri（tree-borrows）跑 upstream test-suite + youpipe 集成测试；loom 覆盖"push→pop→re-reg→fire"交错；`tests/expand_alloc.rs` 直方图确认 40B 档消失；同 binary 交错 A/B（`bench_ab.sh`）确认无回归——cross-binary 结果不可信（重编译布局噪声 ±30%，`sync_for_each` 曾测到 +44% 假回归、rayon 对照行 −13% 稳定偏移的教训）。

## 5. 修掉之后对 youpipe 的预期影响

**可确定（直接测量）**：

- 40B 分配类从背压下的每 run 千级（1690–2653/73K item）塌缩到 **≈ handle 数**（每 Tx/Rx 一个 waker 长期驻留缓存，个位数/run），§1.2 的直方图可直接复测验收；
- 分配计数类测试从"必须按尺寸过滤才能断言"变成"可以无过滤断言"——`expand_alloc.rs` 头注释里记录的 flaky 根源消失，youpipe 的分配契约测试面扩大。

**有界且偏小（算术）**：省掉的是每 contended attempt 一次 malloc + 一次 free（glibc tcache 命中约 20–50 ns）+ `Arc` 构造写 40 B + drop 的两次原子减。按实测最差 run 2653 次折算，总省约 0.1–0.3 ms 的分配器工时，摊到 ms 级的 run 上是 **<1% 墙钟**量级。不要指望吞吐大数字——这不是吞吐修复。

**待验证（可能才是主收益）**：

- 尾延迟与方差：40B 计数的双峰抖动（17/387/2）与调度抖动同现。去掉分配流量本身不改变 park 次数（那是 todo #10 批量 payload 的活），但消除了"分配器工作嵌在 park 关键路径上"这一扰动源，streaming bench 的 p99 稳定性是否改善需要 A/B；
- 分配器全局压力：tcache/arena 的额外流量减少，对同进程里用户态分配（scratch 倍增、用户 stage 内分配）的干扰降低。

**不会变的**：fast path（本就零分配零 waker）、park 次数、futex 系统调用成本、调度行为。若 A/B 显示墙钟无显著变化，该 patch 的价值仍然是"卫生 + 可测性 + 消除一个可疑的尾部扰动源"，且代价极小（30 行、每 episode 一次 CAS）。

## 6. 备选路径

- **不修 upstream**：与 todo #10 的批量 payload（channel 携带 `(seq, Vec<N>)`）合流，每 hop 的 park 次数按批大小摊薄，40B 流量同比例下降；
- **collector 侧换 `std sync_channel`**（todo #5 联动）：暴露面缩小到 stage 间 MPMC；
- upstream 拒收 patch 则 fork 到 `crates/youpipe-crossfire`（与 `youpipe-concurrent-queue` 同策略，保持可 diff 维护）。

## 7. 落地验证（2026-10，已合入）

> 结论先行：**已按 §6 备选路径 fork 落地**（`crates/youpipe-crossfire`，源分支 `waker-cache`），并按实测修正了蓝图的两处假设。40B 分配峰值降 90–97%，墙钟 A/B 中性（在对照行标定的噪声带内），价值兑现为卫生性 + 可测性——与 §5 预测一致。

### 7.1 fork 与 patch

上游补丁在 `/root/programs/fork/crossfire-rs` 分支 `waker-cache`（基线 6f761e0 = 3.1.20 + 两笔上游后续修复），五个提交：

| 提交 | 内容 |
| --- | --- |
| `ba71d40` | 复活 `WakerCache`（去 `<P>` 泛型）+ Tx/Rx 字段 + `reg_waker_blocking(&WakerCache)` + 两个 wake-success 出口 push（§4.1 蓝图） |
| `7dab8f7` / `f29ab77` | crossbeam `try_push_oneshot`/`push_with_ptr` 补 `# Safety` 文档（满足 unsafe 边界文档规则；注意须紧贴 fn 放置，中间的 attribute 会阻断规则的注释回扫） |
| `374c312` | **修蓝图遗漏**：`RegistrySend for RegistryMulti` 与 `RegistrySingle` 两个方向的 `cache_waker` 实现——trait 默认是 no-op，缺失时对应 handle 永不入池 |
| `5bb6ccf` | **修正原设计**：tx wake-success 出口去掉 `is_full()` 门，无条件 push |

vendored 副本 `crates/youpipe-crossfire/` 与 fork 分支逐字节一致（`diff -r` 验证），manifest 自含（lints、dev-deps 同 upstream，另加 `missing_safety_doc = "allow"`，同 youpipe-concurrent-queue 策略）。

### 7.2 两处对蓝图的实测修正（重要经验）

1. **§4.3 的 "is_full() 门是有意为之" 被实测证伪**。出口流量插桩（hit/miss/push + 各出口计数）显示：fanout-9 expand 场景下 89% 的 waker 丢失发生在 wake-success 但 channel 瞬时不满的出口（唤醒方腾出的空位多于一个，自己发送后又没填满）。原论证"下一轮大概率走 fast path"在饱和场景恰好不成立——miss 计数本身就是反证。去掉门后单槽缓存让**同一个 waker 循环服务数千 episode**（插桩 run 实测 hit=1643 / miss=239），40B 计数从 ~1000–1800/run 再降到 62–255/run。代价仅是每 episode 一次 CAS。
2. **蓝图漏写 `RegistrySend` / `RegistrySingle` 的 `cache_waker` 实现**。trait 默认方法体是空的；只复活 `RegistryRecv for RegistryMulti` 时插桩显示 push=0 / hit=0——tx 侧（背压主战场）完全没接上，缓存形同虚设。教训：复活被注释的调用点时，**trait 默认实现会静默吞掉缺失的 impl**，必须插桩确认数据真的流起来，不能只看编译通过。

### 7.3 验收数据

探针（临时 example，size-40 计数分配器，与 §1.2 同 workload shape，各 5 run）：

| 场景 | 3.1.20（before） | fork（after） |
| --- | --- | --- |
| 双快 stage | 2–4 | 2–18 |
| 背压 | 2–20（历史尖峰 387） | 1–18（尖峰消失） |
| expand fanout-9 | 17–2461（高位 ~2000） | **62–255**（峰值 −90~−97%） |

残余 = fast-cancel 出口（`cancel_reuse_waker` / `cancel_waker` 的 take+drop，§2 图中 push 点 B），与 miss 计数精确线性相关（txcdrop 4–207 ↔ miss 10–240）。要消掉它需把缓存句柄穿透 `ChannelShared::sender_double_check`（registry 拿不到 per-handle 缓存），即 §4.1 说的"第二步"；预期残余再降一个数量级（240→~30/run），暂不做。

### 7.4 墙钟 A/B（`bench_ab.sh`，3+5 轮交错，taskset 1-31）

- **streaming 大 N 行**（waker 高频场景）一致小幅改善：`stream_pipeline/single_stage_ordered/100000` −4.1%（9/9 dominant）、`expand_heavy/push_emit_fanout=64/cheap` −5.0%（9/9）、`mixed_load/youpipe_stream_cpu` −1.2~−1.9%；
- **channel 微基准**（1P1C）：`youpipe_mpmc/10000` +6.3% dominant、`youpipe_mpmc/100000` +152%（模式翻转）。**base-vs-base 校准**显示 `mpmc/100000` 同 binary 自身双峰（base 侧三轮 1.38/1.20/3.01 ms）、`mpsc/100000` 校准噪声 ±8.6%——两行都不能作回归证据；
- **rayon 对照行**（不经过 crossfire）+7~10% 稳定偏移：本次 A/B 的跨二进制布局噪声带标定，上述 youpipe 行 ±5% 级的波动都在带内；
- 结论：**墙钟中性**——无超出噪声带的回归，也无显著提升，与 §5 "<1% 墙钟，价值在卫生 + 可测性" 的预测吻合。

### 7.5 后续待办

- miri（tree-borrows）跑 upstream test-suite + youpipe 集成测试（§4.2 验证要求，本次按约定跳过）；loom 覆盖 push→pop→re-reg→fire 交错；
- 向 upstream 提 PR（作者留的 `// XXX, it's possible to reuse the waker` 正是这个切入点）；拒收则维持 fork；
- `expand_alloc.rs` 的按尺寸过滤可考虑收紧，但 40B 档未归零（fast-cancel 残余），无过滤断言仍会 flaky。
