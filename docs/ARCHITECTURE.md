# youpipe v0.2.0 — Internal Architecture

This document is for contributors and developers who want to understand how youpipe works internally, why certain design decisions were made, and how to extend the system.

---

## 1. Design Philosophy

### Compile-Time Pipeline Fusion

youpipe uses **generic nested types** for compile-time pipeline fusion — similar to the iterator `Map<Filter<Iter, F1>, F2>` pattern. When the user chains `.map().filter().map()`, there are no intermediate `Vec`s or virtual dispatch overhead:

```rust
Pipeline::from_vec(items)
    .map(|x: i32| x + 1)      // SyncMap<Identity, F1>
    .filter(|x: &i32| *x > 0) // Filter<SyncMap<Identity, F1>, F2>
    .map(|x: i32| x * 2)      // SyncMap<Filter<...>, F3>
    .collect(items)
```

The compiler monomorphizes all stages into a single concrete `FusedStage::apply()` call with zero indirection.

### Data-First

All data flows through typed channels (`crossfire` MPMC). Each stage owns its input receiver and output sender — no shared-memory consensus objects.

### Non-`'static` Lifetime Support

The `scope()` API is built on `std::thread::scope`, allowing closures to borrow stack-local variables without `'static` bounds.

### Runtime Agnostic

Core modules (`builder/`, `handoff/`, `executor/`, `state/`, `scope/`) have zero dependency on any async runtime. The `runtime/` module defines a `Runtime` trait with `TokioRuntime` as the only implementation (behind the `tokio-runtime` feature).

---

## 2. Module Architecture

```
src/
├── builder/          # Strongly-typed Pipeline API + compile-time fusion + StreamPipeline
│   ├── mod.rs        # Public re-exports
│   ├── config.rs     # PipelineConfig, Workload enum
│   └── typed.rs      # Pipeline<S,T>, par_map(), StreamPipeline, FusedStage, ConsumedBuffer
├── executor/
│   ├── compute/      # crossbeam-deque work-stealing CPU thread pool
│   │   ├── mod.rs    # ComputePool unit tests
│   │   └── worker.rs # ComputePool: Injector/Stealer/EventCount wake/graceful shutdown/join
│   ├── async_pool/   # Tokio async task pool (feature-gated)
│   │   ├── mod.rs
│   │   └── driver.rs # AsyncPool (tokio::runtime::Handle wrapper)
│   ├── scheduler.rs  # SchedulerConfig
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
│   ├── stream.rs     # StreamExecutor, run_sync_stage, run_ordered/unordered_collect, feed_items
│   └── mod.rs
├── scope/            # Non-'static lifetime support
│   ├── pipeline_scope.rs # scope(), PipelineScope, ScopedPipeline (std::thread::scope)
│   └── mod.rs
├── sync/             # Synchronization primitives
│   ├── cancel.rs     # CancellationToken (Arc<AtomicBool>)
│   ├── sys.rs        # cfg(miri) abstraction: parking_lot in prod, std::sync under Miri
│   └── mod.rs
├── runtime/          # Async runtime abstraction
│   ├── traits.rs     # Runtime trait (spawn / spawn_blocking / block_on)
│   ├── tokio_impl.rs # TokioRuntime implementation
│   └── mod.rs
└── graph/            # Logical pipeline DAG representation (future use)
    ├── node.rs
    ├── edge.rs
    └── mod.rs
```

---

## 3. Core Types

### 3.1 `Workload` — Scheduling Strategy Hint

```rust
pub enum Workload {
    Balanced,    // static range assignment, zero atomics
    Unbalanced,  // adaptive fetch-add with 4× oversplit
}
```

- `Balanced`: items split into N contiguous ranges (`start = id * base + remainder.min(id)`). Each worker iterates its range via `ptr::read`. Zero atomic ops per item.
- `Unbalanced`: uses `ConsumedBuffer` with `AtomicUsize` stride counter. Workers `fetch_add(stride)` to claim work dynamically. 4× oversplit ensures good load balance for skewed workloads.

### 3.2 `ConsumedBuffer<T>` — Zero-Copy Input Buffer

```rust
struct ConsumedBuffer<T> {
    ptr: NonNull<T>,
    cap: usize,
}
```

Wraps a `Vec<T>` via `ManuallyDrop`. Workers consume items via `ptr::read` (no clone, no allocation). Drop only deallocates memory — items must be fully consumed or explicitly dropped.

Used by `par_map_fine`, `par_map_adaptive`, `try_par_map`, and `collect_adaptive`.

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
| `Pipeline::from_vec(items)` | `Pipeline<Identity, T>` |
| `.map(\|x\| f(x))` | `Pipeline<SyncMap<Identity, F>, O>` |
| `.filter(\|x\| p(x))` | `Pipeline<Filter<SyncMap<...>, F>, T>` |
| `.fence()` | `Pipeline<Fence<...>, T>` |
| `.ordered()` | `Pipeline<Ordered<...>, T>` |

`collect()` requires `S: FusedStage<T> + Send + Clone + 'static`. The stage is cloned to each worker.

### 3.4 `FusedStage` Trait — Zero-Dispatch Execution

```rust
pub trait FusedStage<T> {
    type Output;
    fn apply(&self, item: T) -> Option<Self::Output>;
}
```

- `SyncMap::apply()` → `self.prev.apply(item).map(|v| (self.f)(v))`
- `Filter::apply()` → `self.prev.apply(item).filter(|v| (self.f)(v))`
- `Fence::apply()` → passthrough (fence semantics handled by `StreamPipeline` at runtime)
- `Ordered::apply()` → passthrough (ordering handled by `ReorderBuffer`)

Returning `Option` allows filter semantics to integrate naturally into the fusion chain.

### 3.5 `par_map()` — Convenience Parallel Map

```rust
pub fn par_map<I, F, R>(iter: I, f: F) -> Vec<R>
pub fn par_map_with_workload<I, F, R>(iter: I, f: F, workload: Workload) -> Vec<R>
pub fn par_chunks_map<I, F, R>(iter: I, chunk_size: usize, f: F) -> Vec<R>
pub fn try_par_map<I, F, R, E>(iter: I, f: F) -> Result<Vec<R>, E>
```

- `par_map`: delegates to `par_map_with_workload(..., Balanced)`
- `par_map_with_workload`: dispatches to `par_map_fine` (ConsumedBuffer + static range) or `par_map_adaptive` (ConsumedBuffer + fetch-add) based on Workload
- `par_chunks_map`: splits into worker ranges, each subdivides into chunk_size sub-chunks. Preserves LLVM SIMD auto-vectorization.
- `try_par_map`: `AtomicBool` error flag + `Mutex<Option<E>>` error slot. Workers check flag per item; first error stored, remaining items dropped. Uses `ConsumedBuffer` + static range.

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
    s.pipeline(items)
        .map(|x: i32| x * factor)   // borrows stack-local factor
        .collect()
})
```

Uses `std::thread::scope` internally. Closures are `Box<dyn FnOnce() + Send + 'env>`.

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

- Built on `crossbeam-deque`: each worker has a local FIFO deque, other workers steal via `Stealer`
- Global `Injector` accepts externally submitted tasks
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
- `cfg(miri)` branch uses `std::sync` instead of `parking_lot`

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

The `sync::sys` module provides a unified API via `cfg(miri)`:

| Environment | Mutex | Condvar |
|---|---|---|
| Production | `parking_lot::Mutex` | `parking_lot::Condvar` |
| Miri | `std::sync::Mutex` (wrapped with `Option<MutexGuard>`) | `std::sync::Condvar` |

`parking_lot` uses Windows FFI (`GetModuleHandleA`) that Miri cannot handle. The unified API makes both implementations transparent to callers.

---

## 9. Performance Benchmarks

### CPU-Heavy par_map vs rayon

| Size | youpipe | rayon | Result |
|---|---|---|---|
| 1K | ~130µs | ~130µs | Tie |
| 10K | ~1.1ms | ~1.1ms | Tie |
| 100K | ~10.6ms | ~10.6ms | Tie |

### Pipeline Fusion (3 stages) vs rayon chain

| Size | youpipe fused | rayon chain | Result |
|---|---|---|---|
| 10K | 3.2µs | 10.6µs | **youpipe 3.3x faster** |
| 100K | 32µs | 105µs | **youpipe 3.3x faster** |

### Mixed Load vs tokio::spawn_blocking

| Size | youpipe stream | spawn_blocking | Result |
|---|---|---|---|
| 100 | 1.29ms | 1.42ms | 1.1x faster |
| 500 | 2.32ms | 4.58ms | **2.0x faster** |
| 1000 | 3.07ms | 8.93ms | **2.9x faster** |

### Channel Throughput

| Size | crossfire | crossbeam-channel | std_mpsc | Result |
|---|---|---|---|---|
| 10K | 45.5 Melem/s | 46.5 Melem/s | 62.4 Melem/s | Tie (MPMC) |
| 100K | 76.6 Melem/s | 74.8 Melem/s | 125.5 Melem/s | **crossfire 1.02x vs crossbeam** |

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
