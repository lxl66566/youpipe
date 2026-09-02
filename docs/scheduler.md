# ComputePool — Work-Stealing Scheduler

> [← Documentation index](README.md)

### Architecture

```
Injector (global queue)
    ↓ steal
Worker₀ ←→ Stealer₀
Worker₁ ←→ Stealer₁
Worker₂ ←→ Stealer₂
Worker₃ ←→ Stealer₃
```

- Built on `st3` (bounded lock-free LIFO deque): each worker has a local LIFO deque (FIFO stealing); other workers steal via `Stealer`
- Global injector is a lock-free `concurrent_queue::ConcurrentQueue` (unbounded) that accepts externally submitted tasks and local-queue overflow
- `EventCount`-style packed atomic counters (`pool/sleep.rs`) wake idle workers
- `SleepMask` (`pool/sleep_mask.rs`): fixed-size inline `[AtomicU64; N]` bitmask tracking which workers are parked, so `wake_any_threads` jumps straight to set bits instead of linearly locking every worker's `is_blocked` mutex. `THREADS_BITS` is sized so the mask fits in one cache line (8 words = 64 B on 64-bit, covering up to 511 workers). The single-`AtomicUsize` predecessor silently aliased bits for `worker_index >= 64` (`1usize << 64` wraps to bit 0 in Rust) and deadlocked under heavy oversubscription; see `sleep_mask.rs` module doc.

#### Vendored scheduler dependencies (`vendor/`)

The two lock-free primitives the pool is built on are **vendored forks**, so
their hot paths can be tuned in-tree without waiting on upstream releases:

- `vendor/youpipe-st3` (package `youpipe-st3`, lib name `st3`) — fork of
  [asynchronics/st3](https://github.com/asynchronics/st3) v0.4.1. Carries an
  optimization line on top of upstream: bulk segmented steal copies
  (`transfer::transfer_items` splits both ring ranges at the union of their
  wrap points and relocates up to 3 chunks with `copy_nonoverlapping`; ≤8-item
  chunks keep an inline scalar loop — measured −22…−51 % on 16–128-item
  steals), hot-path bounds-check removal, fast empty check before touching
  destination atomics, hoisted tail load out of the pop CAS loop,
  uninitialized buffer allocation, in-place `Arc` construction, and a
  single-item transfer fast path. Provenance + per-commit detail live in the
  vendored `Cargo.toml` header and `st3_PERF_OPTIMIZATION.md`.
- `vendor/youpipe-concurrent-queue` (package `youpipe-concurrent-queue`, lib
  name `concurrent_queue`) — fork of
  [smol-rs/concurrent-queue](https://github.com/smol-rs/concurrent-queue)
  v2.5.0 with the adaptive-backoff change (contention paths spin with
  exponential `pause` via `crossbeam_utils::Backoff` instead of bare CAS
  retries / immediate yield; measured 2–5.6× on contended paths) plus the
  remaining crossbeam-queue micro-optimizations from
  `concurrent-queue-PERFORMANCE_REVIEW.md`: bounds-check-free slot addressing
  on the bounded push/pop hot path (`get_unchecked` + `debug_assert`; the
  wrap invariant is not compiler-provable because the mask width
  `(cap+1).next_power_of_two()` can exceed `cap`), zeroed (`alloc_zeroed`)
  SegQueue block allocation (all-zero bytes are a valid block; skips the
  per-slot const-array copy and the memset for fresh pages), and a
  `needs_drop` guard that skips `Bounded`'s per-slot drop sweep for
  destructor-free `T`. Additionally `Unbounded::push_n` (exposed as
  `ConcurrentQueue::push_n`, per-item fallback on the other variants)
  reserves a contiguous run of up to 31 tail slots with **one** CAS and fills
  it with plain stores — segment semantics identical to `push` (WRITE-flag
  `Release` publication, block-boundary install order, closed check before
  each reservation). `Registry::inject_batch` uses it, collapsing the fused
  dispatcher's `num_threads−1 ≤ 31` chunk-job CASes into one per dispatch
  (−0.5…−2.6 % on the cpu_heavy fused path); the streaming `submit_batch`
  benefits identically. The review also records what was tried and rejected —
  e.g. x86 `lock not` SeqCst fences are a
  ~8–10 % pessimization on modern LLVM (it already lowers `fence(SeqCst)` to
  `lock or`).

  Why not crossbeam-queue directly: its `SegQueue::push` is a SeqCst CAS with
  block allocation every `LAP − 1` (31) items, and the pool's stream-dispatch
  path does exactly one high-frequency `inject` per task — the exact regime
  where concurrent-queue's design wins (replacing concurrent-queue with
  crossbeam-queue wholesale regressed the stream dispatch path 4–7 %, per-item
  `inject` punishing SegQueue's SeqCst-CAS push + per-31-items block churn).
  The forks keep concurrent-queue's push/pop shape and backfill the
  waiting-strategy gap instead.

`crossfire` stays a normal crates.io dependency (nothing left to squeeze).
The rename (`youpipe-*`) exists purely to prevent version confusion with the
crates.io releases; both crates keep their upstream lib names, so call sites
don't change. The vendored crates are excluded from youpipe's own lint/
profile config except where noted in their `Cargo.toml` headers.

### Task Submission Flow

1. `pool.submit(job)` boxes the closure in a `HeapJob`, type-erases it to a `JobRef`, and calls `inject_or_push` — external callers go to the global injector, an on-pool caller pushes its own local deque
2. `Sleep::new_injected_jobs` bumps the packed atomic counters and wakes parked workers via `wake_any_threads`
3. Worker wakes → `find_work()` searches by priority

### Work Search Strategy

`find_work()` tries sources in priority order:

1. `local.pop()` — own LIFO deque
2. `injector.steal()` — global queue (cheap CAS-free dequeue, checked before peers since external submits arrive here)
3. peer stealers — randomized full scan with `steal_and_pop`

The yield/spin/sleep backoff is **not** in `find_work()`; it lives in the idle
loop of `wait_until_cold`; each round that finds no work calls
`Sleep::no_work_found`, which ramps from `spin_loop` → `thread::yield_now` →
parking on the `EventCount`-style counters.

#### Dispatch granularity — cost-adaptive chunk counts (tried, rejected)

Hypothesis (2026-09): at small batches (1 K `cpu_heavy`), the hybrid
dispatcher's fixed cost is dominated by the inject + wake cascade, so
shrinking the top-level chunk count from `num_threads` to
`ceil(estimated_total_work / 8 µs)` — estimated via a per-pool chunk-cost EMA
sampled by `ChunkJob::execute` — should cut overhead. **Rejected by
measurement**: p50 wall at 1 K went 32.1 → 37.7 µs (+17 %), consistently
across interleaved runs. The causal read: hotpath showed ~13 workers per
batch reach the condvar park *between* back-to-back batches, and each park's
wake latency is skewed; `num_threads` fine-grained chunks are precisely the
load-balancing unit that hides that skew (late-waking workers pick up
remaining chunks instead of gating the batch with their wake latency).
Fewer, meatier chunks hand the batch's tail to whichever workers wake last.
The remaining wall-time floor at tiny batches is worker wake latency, whose
backoff constants are already A/B-tuned (see `sleep.rs` — widening the spin
or yield windows was measured as a global regression in 2026-06).

### Graceful Shutdown

`ComputePool::Drop` calls `Registry::terminate()`, which decrements a ref-count
(`terminate_count`); when the last clone drops (count 1→0) it sets each worker's
`terminate` OnceLatch and tickles it awake. Each worker's `wait_until_out_of_work`
then drains its remaining local-deque work, sets its `stopped` latch, and exits;
`Registry::Drop` blocks on every worker's `stopped` before returning.
