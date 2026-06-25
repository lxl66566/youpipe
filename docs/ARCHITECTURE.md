# youpipe v0.2.0 — Internal Architecture

This document is for contributors and developers who want to understand how youpipe works internally, why certain design decisions were made, and how to extend the system.

---

## 1. Design Philosophy

### Compile-Time Pipeline Fusion

youpipe uses **generic nested types** for compile-time pipeline fusion — similar to the iterator `Map<Filter<Iter, F1>, F2>` pattern. When the user chains `.map().filter().map()`, there are no intermediate `Vec`s or virtual dispatch overhead:

```rust
Pipeline::new()
    .map(|x: i32| x + 1)      // SyncMap<Identity, F1>
    .filter(|x: &i32| *x > 0) // Filter<SyncMap<Identity, F1>, F2>
    .map(|x: i32| x * 2)      // SyncMap<Filter<...>, F3>
    .collect(items)
```

The compiler monomorphizes all stages into a single concrete `FusedStage::apply()` call with zero indirection.

### Data-First

All data flows through typed channels (`crossfire` MPMC). Each stage owns its input receiver and output sender — no shared-memory consensus objects.

### Non-`'static` Lifetime Support

The `scope()` API allows closures to borrow stack-local variables without `'static` bounds. The `'env` lifetime is threaded through `ScopedPipeline` and propagated to `ComputePool::join`, whose `Registry::in_worker_cold` blocks the calling thread until every spawned sub-task finishes — guaranteeing borrowed references outlive the pool's access to them.

### Runtime Agnostic

Core modules (`builder/`, `handoff/`, `executor/`, `state/`, `scope/`) have zero dependency on any async runtime. The `runtime/` module defines a `Runtime` trait with `TokioRuntime` as the only implementation (behind the `tokio-runtime` feature).

---

## 2. Module Architecture

```
src/
├── builder/          # Strongly-typed Pipeline API + compile-time fusion + StreamPipeline
│   ├── mod.rs        # Public re-exports
│   ├── config.rs     # PipelineConfig, Workload enum
│   └── typed.rs      # Pipeline<S,T>, par_map(), StreamPipeline, FusedStage,
│                     # Slots<T> index-based parallel map core, RangeOp
├── executor/
│   ├── compute/      # st3 work-stealing CPU thread pool
│   │   ├── mod.rs    # ComputePool unit tests
│   │   └── worker.rs # ComputePool: Injector/Stealer/EventCount wake/graceful shutdown/join
│   ├── async_pool/   # Tokio async task pool (feature-gated)
│   │   ├── mod.rs
│   │   └── driver.rs # AsyncPool (tokio::runtime::Handle wrapper)
│   └── mod.rs
├── handoff/          # Data transfer layer
│   ├── channel.rs    # MPMC channels (crossfire wrapper: sync + async)
│   ├── ring_buffer.rs # Lock-free SPSC ring buffer (power-of-2, cache-line padded)
│   ├── batcher.rs    # Auto-batching layer (SharedRingBuffer + BatchConfig)
│   ├── notify.rs     # EventCount (condvar notify), WaitGroup (counter barrier)
│   └── mod.rs
├── state/            # Ordered output & streaming execution
│   ├── reorder.rs    # ReorderBuffer<T> (min-heap for restoring ordered output)
│   ├── fence.rs      # FenceBarrier<T> (configurable chunk_size barrier)
│   ├── stream.rs     # run_ordered_collect helper
│   └── mod.rs
├── scope/            # Non-'static lifetime support
│   ├── pipeline_scope.rs # scope(), PipelineScope, ScopedPipeline (work-stealing, 'env closures)
│   └── mod.rs
├── sync/             # Synchronization primitives
│   ├── cancel.rs     # CancellationToken (Arc<AtomicBool>)
│   ├── sys.rs        # Miri-transparent Mutex/Condvar (parking_lot ↔ std)
│   └── mod.rs
├── runtime/          # Async runtime abstraction
│   ├── traits.rs     # Runtime trait (spawn / spawn_blocking / block_on)
│   ├── tokio_impl.rs # TokioRuntime implementation
│   └── mod.rs
├── pool/             # Rayon-style work-stealing scheduler core
│   ├── registry.rs   # Registry, WorkerThread, find_work, steal
│   ├── sleep.rs      # AtomicCounters sleep/wake governance
│   ├── latch.rs      # CoreLatch / SpinLatch / LockLatch / CountLatch
│   ├── job.rs        # JobRef (type-erased), StackJob, HeapJob
│   ├── join.rs       # fork-join
│   ├── unwind.rs     # AbortIfPanic, halt/resume_unwinding
│   └── mod.rs
└── util.rs           # CachePadded<T>
```

---

## 3. Core Types

### 3.1 `Workload` — Scheduling Strategy Hint

```rust
pub enum Workload {
    Balanced,    // 4× oversplit via recursive join
    Unbalanced,  // 8× oversplit for finer-grained stealing
}
```

Both variants use the same recursive `join`-based index splitting (see 3.2).
The oversplit factor controls how many leaf tasks the recursion produces:
`Unbalanced` creates more, smaller leaves so that a thread blocked on a slow
item can have its remaining leaves stolen by idle workers.

### 3.2 `Slots<T>` — Index-Based Zero-Copy Buffers

```rust
pub(crate) struct Slots<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
}
```

The parallel map/collect core never copies data between recursive levels. Two
`Slots` buffers are allocated once:

- **input** (`from_vec`): reinterprets the user's `Vec<T>` in place — items are
  not moved, only the allocation's type is reinterpreted. `read(i)` does a
  `ptr::read`, leaving slot `i` uninit.
- **output** (`uninit(n)`): a `with_capacity(n) + set_len(n)` box of
  uninitialized slots (no O(n) init loop). `write(i, val)` marks slot `i` init.

Recursive `join` splits the **index range** `[0, n)`, not the data. Each leaf
reads `input[i]`, applies the transform, writes `output[i]`. No `split_off`,
no `extend`, no per-level reallocation — this is the key difference from a
naïve recursive `Vec` split, and the reason the warm-input throughput is
competitive with rayon's pre-allocated `collect`.

Panic safety: leaves wrap their loop in `catch_unwind`; on panic, a leaf drops
exactly the slots it touched (`output[start..i)` written, `input[i+1..end)`
unread). Internal nodes propagate the first `Err` and drop the
already-completed sibling's output range. `MAY_FILTER = false` guarantees
written ranges have no holes, so `drop_range` is sound without per-slot
validity tracking. Miri (tree-borrows) passes on all paths.

### 3.3 `Pipeline<S, T>` — Compile-Time Fused Pipeline

`Pipeline` has two generic parameters:

- `S`: The stage chain (nested `SyncMap` / `Filter` / `Fence` / `Ordered` / `Identity`)
- `T`: The current output type

```rust
pub struct Pipeline<S = Identity, T = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<T>,
}
```

**Type transition chain**:

| Method call | Type change |
|---|---|
| `Pipeline::new()` | `Pipeline<Identity, T>` |
| `.map(\|x\| f(x))` | `Pipeline<SyncMap<Identity, F>, O>` |
| `.filter(\|x\| p(x))` | `Pipeline<Filter<SyncMap<...>, F>, T>` |
| `.fence()` | `Pipeline<Fence<...>, T>` |
| `.ordered()` | `Pipeline<Ordered<...>, T>` |

### 3.4 `FusedStage` Trait — Zero-Dispatch Execution

```rust
pub trait FusedStage<T> {
    type Output;
    /// Whether the chain can drop items (contains a `Filter`).
    const MAY_FILTER: bool = false;
    fn apply(&self, item: T) -> Option<Self::Output>;
}
```

- `SyncMap::apply()` → `self.prev.apply(item).map(|v| (self.f)(v))`
- `Filter::apply()` → `self.prev.apply(item).filter(|v| (self.f)(v))` (sets `MAY_FILTER = true`)
- `Fence::apply()` → passthrough (fence semantics handled by `StreamPipeline` at runtime)
- `Ordered::apply()` → passthrough (ordering handled by `ReorderBuffer`)

`MAY_FILTER` is propagated through `SyncMap` / `Fence` / `Ordered` from the
preceding stage. `collect()` uses it as a compile-time switch: when `false`,
the stage chain is driven by the index-based `Slots` fast path (output
cardinality equals input cardinality); when `true`, it falls back to the
per-leaf-`Vec` merge path (filters change cardinality, so fixed-index writes
are impossible). Returning `Option` lets filter semantics integrate naturally
into the fusion chain.

### 3.5 `par_map()` — Convenience Parallel Map

```rust
pub fn par_map<I, F, R>(iter: I, f: F) -> Vec<R>
pub fn par_map_with_workload<I, F, R>(iter: I, f: F, workload: Workload) -> Vec<R>
pub fn par_chunks_map<I, F, R>(iter: I, chunk_size: usize, f: F) -> Vec<R>
pub fn try_par_map<I, F, R, E>(iter: I, f: F) -> Result<Vec<R>, E>
```

- `par_map`: delegates to `par_map_with_workload(..., Balanced)`
- `par_map_with_workload`: allocates input + output `Slots` once, then drives
  the recursive index-based core (`par_index_rec`) via `FnMap` (a never-filter
  `RangeOp`). Workload only changes the oversplit factor.
- `par_chunks_map`: splits items into worker ranges, each subdivides into chunk_size sub-chunks. Preserves LLVM SIMD auto-vectorization.
- `try_par_map`: still uses the recursive `Vec`-merge path (early-error return
  with `join`); not on the hot benchmark path.

### 3.6 `StreamPipeline` — Streaming Multi-Stage Pipeline

When a pipeline contains `fence`, `ordered`, or other runtime semantics, stages are connected via channels:

| Method | Description |
|---|---|
| `run()` | Single-stage streaming execution |
| `run_multi_stage()` | Two-stage pipeline (stage1 → channel → stage2) |
| `run_with_fence()` | Configurable isolation (`FenceMode`): `Barrier` (stage1 fully drains before stage2 starts) or `Chunked(k)` (forward every k items, stages overlap) |
| `run_nested()` | Expand mode: outer_stage 1:N expansion → inner_stage parallel processing |

All streaming methods accept `ordered: bool`, using `ReorderBuffer` to restore original order. Optional `CancellationToken` enables cooperative cancellation — feeder and workers check `is_cancelled()` per iteration.

### 3.7 `ScopedPipeline` — Non-`'static` Pipeline

```rust
youpipe::scope(|s| {
    let factor = 10;
    s.pipeline()
        .map(|x: i32| x * factor)   // borrows stack-local factor
        .collect(items)
})
```

Mirrors `Pipeline`'s compile-time-fused stage chain (`SyncMap` / `Filter` /
etc.) but with `'env` (non-`'static`) closure bounds. `.collect()` drives the
same recursive work-stealing `par_index_collect` core as `Pipeline::collect`
— exposed via the `pub(crate) fused_collect_scoped` entry point — so the
soundness story rests on `ComputePool::join`: the calling thread blocks in
`Registry::in_worker_cold` until every sub-task finishes, which guarantees
every `'env` reference captured by a scoped closure outlives the pool's
access to it. (No `std::thread::scope` or per-chunk `Mutex<Vec<T>>` is
involved — the previous design paid for both.)

---

## 4. ComputePool — Work-Stealing Thread Pool

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
- Global injector (mutex-protected `VecDeque`) accepts externally submitted tasks and local-queue overflow
- `EventCount` (condvar) wakes idle workers

### Task Submission Flow

1. `pool.submit(job)` → `injector.push(Box::new(job))`
2. If `idle_count > 0` → `event.notify_one()` wakes a worker
3. Worker wakes → `find_work()` searches by priority

### Work Search Strategy

```
find_work():
    1. local.pop()           → return if hit
    2. injector.steal()      → return if hit
    3. peer stealers         → return if hit
    4. cpu_since_yield++
       if > MAX_SPINS(64):
           yield_now()
           reset counter
```

### Graceful Shutdown

`Drop` impl: set `shutdown` flag → `event.notify()` wake all workers → `drain_remaining()` processes leftover tasks → join all threads.

---

## 5. Data Transfer Layer (`handoff/`)

### 5.1 MPMC Channels (`channel.rs`)

Wraps `crossfire` with a unified API:

| Type | Implementation |
|---|---|
| `SyncSender<T>` / `SyncReceiver<T>` | `crossfire::mpmc::bounded_blocking` |
| `AsyncSender<T>` / `AsyncReceiver<T>` | `crossfire::mpmc::bounded_async` |

An additional `closed: Arc<AtomicBool>` flag provides early termination without racing with crossfire's internal disconnect detection.

### 5.2 SPSC Ring Buffer (`ring_buffer.rs`)

Lock-free SPSC ring buffer features:

- Capacity must be a power of 2 (masking instead of modulo)
- `CachePadded<AtomicUsize>` separates head/tail to different cache lines — avoids false sharing
- `push_batch()` / `pop_batch()` for bulk operations
- `Drop` impl correctly drops unconsumed elements

### 5.3 EventCount (`notify.rs`)

Condvar wrapper for ComputePool worker wake-up:

- `notify()` / `notify_one()` → increment state + condvar wake
- `wait()` → remember current state key, condvar wait until key changes
- Uses `parking_lot::{Condvar, Mutex}` directly (the `cfg(miri)` mutex switch lives in `pool/sys`, used by the injector)

### 5.4 WaitGroup (`notify.rs`)

Counter barrier: `add(n)` increments, `done()` decrements, `wait()` blocks until zero. When count transitions 1→0, condvar broadcasts.

---

## 6. Ordered Output (`state/reorder.rs`)

`ReorderBuffer<T>` uses a min-heap to restore original element order after parallel processing:

1. Each element is sent with a sequence number `(seq, item)`
2. `insert(seq, item)` pushes onto the min-heap
3. `flush_ready()` pops contiguous elements from the top starting at `next_expected`
4. `flush_remaining()` handles tail remainder (sorted return)

---

## 7. Fence Barrier (`state/fence.rs` + `StreamPipeline::run_with_fence`)

A fence lets the caller decide how strictly two adjacent stages are isolated, via `FenceMode`:

- **`FenceMode::Barrier`** — hard isolation: stage 1 must fully drain before stage 2 receives any item.
- **`FenceMode::Chunked(k)`** — soft batching: forward a batch of `k` items as soon as it accumulates, so stage 2 overlaps stage 1 (the right default for mixed CPU/IO workloads).

Data flow:

1. Stage1 workers pull from `in_rx` → process → send to `mid_tx`
2. Fence thread **eagerly drains** `mid_rx` into a `FenceBarrier<T>`, releasing batches to `fenced_tx` per `mode` (immediately in `Chunked`, or all at once on disconnect in `Barrier`)
3. Stage2 workers pull from `fenced_rx` → process → send to `out_tx`

Stage completion is signalled purely by channel disconnect (all sender clones dropped) — no `WaitGroup` is needed. Eager draining is essential: it prevents stage 1 from blocking on a full `mid` channel, which previously deadlocked when `items.len()` exceeded the channel buffer.

---

## 8. Miri Compatibility

The `pool/sys` module provides a unified `Mutex` API via `cfg(miri)`:

| Environment | Injector mutex |
|---|---|
| Production | `parking_lot::Mutex` (zero-cost re-export — fairer, no poisoning) |
| Miri | `std::sync::Mutex` wrapped in a newtype exposing the same infallible `lock()` |

`parking_lot_core` resolves `WaitOnAddress` through `GetModuleHandleA`, a Windows foreign function Miri cannot emulate, whereas the std mutex/condvar are natively supported. The unified API lets callers write `mutex.lock()` once and stay transparent to which backend is active.

---

## 9. Performance Benchmarks

> All numbers below are from a 32-core AMD (Zen) Linux machine, `criterion`
> `--sample-size 30 --measurement-time 5`. Methodology note: `par_map` takes
> ownership of the input, so a benchmark iteration must rebuild the input
> (`warm_clone`). glibc's large `memcpy` uses non-temporal stores that bypass
> the cache, so a naïve `data.clone()` arrives **cold-from-RAM** — measuring
> allocator/memory latency rather than the framework. The `sync_vs_rayon` bench
> therefore warms the input in the (untimed) setup so the timed region is a
> fair, like-for-like comparison with rayon's warm `par_iter` borrow. A
> `_cold` variant is kept for the lightweight group to document the one-shot
> cold-memory cost.

### CPU-Heavy par_map vs rayon (`sync_cpu_heavy`, 100 iters/item, warm input)

| Size | youpipe | rayon | Result |
|---|---|---|---|
| 1K | ~21 µs | ~39 µs | **youpipe ~1.8× faster** |
| 10K | ~85 µs | ~90 µs | youpipe slightly faster |
| 100K | ~440 µs | ~322 µs | rayon ~1.4× faster |

### Pipeline Fusion (3 stages) vs rayon chain (`pipeline_fusion`, warm input)

| Size | youpipe fused | rayon chain | Result |
|---|---|---|---|
| 10K | ~133 µs | ~70 µs | rayon ~1.9× faster |
| 100K | ~166 µs | ~113 µs | rayon ~1.5× faster |

The fused stage chain still trails rayon's `par_iter` because rayon's consumer
is a highly-tuned length-splitting fold/collect; youpipe drives the index-based
`Slots` core through a generic `RangeOp`. Closing this gap is the next
optimization target — see `§3.2` for the current `&[T]`/`&mut [R]` leaf view
that already closed most of the lightweight-`par_map` gap.

### Lightweight par_map vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (warm) | youpipe (cold) | rayon |
|---|---|---|---|
| 10K | ~38 µs | ~38 µs | ~68 µs |
| 100K | ~124 µs | ~172 µs | ~114 µs |
| 1M | **~390 µs** | ~4.3 ms | ~290 µs |

Warm-input lightweight 1M improved from ~1.9 ms (pre-`Slots`) → ~730 µs
(after `Slots`) → **~390 µs** after switching the leaf loop to a `&[T]` /
`&mut [R]` slice view (`Slots::as_slice` / `as_mut_slice`). That last step
closed a 2.5× gap with rayon down to ~1.35×: the previous `&Slots<u64>` for
both input and output blocked LLVM's auto-vectorizer because the alias
analysis could not prove the two `UnsafeCell`-wrapped buffers were disjoint.
The cold variant documents the read-compute-write sensitivity to
cold-from-RAM input (glibc's non-temporal memcpy bypasses the cache); rayon
on an equally-cold clone measures ~800 µs at 1M.

### Mixed Load — StreamPipeline vs `tokio::spawn_blocking` (`mixed_load`)

| Size | youpipe stream | spawn_blocking | rayon (CPU-only) | Result |
|---|---|---|---|---|
| 100 | ~129 µs | ~229 µs | ~19 µs | **youpipe ~1.8× faster** than tokio |
| 500 | ~418 µs | ~1.25 ms | ~29 µs | **youpipe ~3× faster** |
| 1000 | ~1.0 ms | ~2.5 ms | ~37 µs | **youpipe ~2.5× faster** |

StreamPipeline comfortably beats `tokio::spawn_blocking` (the design target for
mixed CPU/IO). `rayon::par_iter` is fastest here because this benchmark is
pure-CPU and rayon's direct fork-join skips channel handoff entirely.

### Channel Throughput

| Size | crossfire | crossbeam-channel | std_mpsc | Result |
|---|---|---|---|---|
| 10K | 45.5 Melem/s | 46.5 Melem/s | 62.4 Melem/s | Tie (MPMC) |
| 100K | 76.6 Melem/s | 74.8 Melem/s | 125.5 Melem/s | **crossfire 1.02× vs crossbeam** |

---

## 10. Extending the System

### Adding a New Fused Stage

1. Define a stage struct implementing `StageMarker<T>` and `FusedStage<T>`
2. Add a builder method on `Pipeline<S, T>` returning `Pipeline<NewStage<S, ...>, O>`
3. In `FusedStage::apply()`, compose `self.prev.apply(item)` with the new logic

### Adding a New Runtime

Implement the `Runtime` trait:

```rust
pub trait Runtime: Send + Sync + 'static {
    fn spawn<F>(&self, future: F) where F: Future<Output = ()> + Send + 'static;
    fn spawn_blocking<F, R>(&self, f: F) -> Pin<Box<dyn Future<Output = R> + Send + 'static>>
    where F: FnOnce() -> R + Send + 'static, R: Send + 'static;
    fn block_on<F>(&self, future: F) -> F::Output where F: Future;
}
```

Add a feature flag in `Cargo.toml` and an implementation module under `runtime/`.
