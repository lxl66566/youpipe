# youpipe internal

This document is for contributors and developers who want to understand how youpipe works internally, why certain design decisions were made, and how to extend the system.

---

## 1. Design Philosophy

### Data-First

youpipe's public API is **data-first**: items enter the pipeline at the front
(`pipe(items)` / `stream(items)` / `scope(|s| s.pipe(items))`), stages chain via
builder methods, and a single terminal call (`.collect()` / `.run()`) executes
the whole chain. This mirrors the mental model of `iter().map().collect()` —
data flows left-to-right, never "define the pipeline, then feed data at the end".

### Compile-Time Pipeline Fusion

youpipe uses **generic nested types** for compile-time pipeline fusion — similar to the iterator `Map<Filter<Iter, F1>, F2>` pattern. When the user chains `.map().filter().map()`, there are no intermediate `Vec`s or virtual dispatch overhead:

```rust
pipe(0..1000)                // Pipe<Identity, i32, i32>
    .map(|x: i32| x + 1)     // Pipe<SyncMap<Identity, F1>, i32, i32>
    .filter(|x: &i32| *x > 0) // Pipe<Filter<SyncMap<...>, F2>, i32, i32>
    .map(|x: i32| x * 2)     // Pipe<SyncMap<Filter<...>, F3>, i32, i32>
    .collect()               // executes the chain → Vec<i32>
```

The compiler monomorphizes all stages into a single concrete `FusedStage::apply()` call with zero indirection.

### Streaming for the cases fusion can't cover

The fused `Pipe` is CPU-only and `'static`. For workloads that need
channel-connected stages, async IO, cancellation, fences, or 1-to-N expansion,
the data-first `stream(items)` builder assembles a `StreamPipe` whose stages are
linked by MPMC channels at `.run()` time. The two engines are deliberately
separate (work-stealing join vs channel handoff) — see §3.6.

### Non-`'static` Lifetime Support

The `scope()` API allows closures to borrow stack-local variables without `'static` bounds. The `'env` lifetime is threaded through `ScopedPipe` and the underlying `fused_collect_scoped` drives the same `ComputePool::join` work-stealing core — whose `Registry::in_worker_cold` blocks the calling thread until every spawned sub-task finishes — guaranteeing borrowed references outlive the pool's access to them.

### Runtime Agnostic

Core modules (`builder/`, `handoff/`, `executor/`, `state/`, `scope/`) have zero dependency on any async runtime. The `runtime/` module defines a `Runtime` trait with `TokioRuntime` as the only implementation (behind the `tokio-runtime` feature).

---

## 2. Module Architecture

```
src/
├── builder/          # Strongly-typed data-first API + compile-time fusion + StreamPipe
│   ├── mod.rs        # Public re-exports
│   ├── config.rs     # PipelineConfig, Workload enum
│   └── typed/        # Pipe / TryPipe / StreamPipe builder core
│       ├── mod.rs    # Re-exports
│       ├── fused.rs  # pipe(), Pipe<S,I,O>, TryPipe<S,I,O,E>, par_index_* core,
│       │             #   fused_collect_scoped (pub(crate) entry for scope)
│       ├── stream.rs # stream(), StreamPipe<S,I,O>, StageSpawn typestate chain
│       ├── traits.rs # FusedStage / FusedTryStage / RangeOp / stage markers
│       │             #   (SyncMap / Filter / TryMap / MapErr / InfallibleChain)
│       └── slots.rs  # Slots<T> index-based zero-copy buffer
├── executor/
│   ├── compute/      # st3 work-stealing CPU thread pool
│   │   ├── mod.rs    # ComputePool unit tests
│   │   └── worker.rs # ComputePool: Injector/Stealer/sleep counters wake/graceful shutdown/join
│   ├── async_pool/   # Tokio async task pool (feature-gated)
│   │   ├── mod.rs
│   │   └── driver.rs # AsyncPool (tokio::runtime::Handle wrapper)
│   └── mod.rs
├── handoff/          # Data transfer layer
│   ├── channel.rs    # MPMC channels (crossfire wrapper: sync + async)
│   ├── notify.rs     # WaitGroup (counter barrier for stage synchronization)
│   └── mod.rs
├── state/            # Ordered output & streaming execution
│   ├── reorder.rs    # ReorderBuffer<T> (bitmask slot array for restoring ordered output)
│   ├── fence.rs      # FenceBarrier<T> (configurable chunk_size barrier)
│   ├── stream.rs     # run_ordered_collect helper
│   └── mod.rs
├── scope/            # Non-'static lifetime support
│   ├── pipeline_scope.rs # scope(), PipelineScope, ScopedPipe (work-stealing, 'env closures)
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
    Balanced,    // up to 4× oversplit via recursive join (adaptive, see below)
    Unbalanced,  // 8× oversplit for finer-grained stealing
}
```

Both variants use the same recursive `join`-based index splitting (see 3.2).
The oversplit factor controls how many leaf tasks the recursion produces;
`Unbalanced` creates more, smaller leaves so that a thread blocked on a slow
item can have its remaining leaves stolen by idle workers.

`Balanced` is adaptive: when the batch is small enough that per-leaf work is
sub-microsecond (`n / num_threads ≤ 1024`), it drops to `oversplit = 1` to
avoid paying fork/join dispatch overhead for stealing slack it does not need;
above that threshold it uses 4×. `Unbalanced` always uses 8× because its tail
latency needs the stealing slack regardless of batch size.

### 3.2 `Slots<T>` — Index-Based Zero-Copy Buffers

```rust
pub(crate) struct Slots<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
}
```

The parallel map/collect core never copies data between recursive levels. Two
`Slots` buffers are allocated once:

- input (`from_vec`): reinterprets the user's `Vec<T>` in place — items are
  not moved, only the allocation's type is reinterpreted.
- output (`uninit(n)`): a `with_capacity(n) + set_len(n)` box of
  uninitialized slots (no O(n) init loop).

`Slots` exposes `as_slice(start, end)` / `as_mut_slice(start, end)` to borrow a
range as a plain `&[T]` / `&mut [T]`, plus `drop_range` for panic cleanup. The
leaf loop pulls these slice views and runs `ptr::read` / `ptr::write` over them;
handing LLVM a normal slice reference (rather than `&Slots` with `UnsafeCell`
interior mutability) is what lets the auto-vectorizer prove the input and
output buffers are disjoint.

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

### 3.3 `Pipe<S, I, O>` — Data-First Fused Pipeline

`Pipe` is the data-first fused pipeline. Built by `pipe(items)`, it carries the
input `Vec<I>` inside the builder so the chain reads naturally left-to-right and
`.collect()` takes no arguments. Three generic parameters:

- `S`: The stage chain (nested `SyncMap` / `Filter` / `Identity`)
- `I`: The pipeline input type (fixed by `pipe()`)
- `O`: The current output type (the input to the next stage)

```rust
pub struct Pipe<S = Identity, I = (), O = ()> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<O>,
}
```

`I` and `O` are separate type parameters so type-changing maps compile: the
input type `I` stays fixed while `O` tracks the latest transform's output, so
`.map(i32 -> String)` then `.map(String -> usize)` type-checks end to end.

Type transition chain (`I₀` = initial input):

| Method call           | Type change                                                                   |
| --------------------- | ----------------------------------------------------------------------------- |
| `pipe(items)`         | `Pipe<Identity, I₀, I₀>`                                                      |
| `.map(\|x\| f(x))`    | `Pipe<SyncMap<Identity, F>, I₀, O>`                                           |
| `.map(\|x\| g(x))`    | `Pipe<SyncMap<...>, I₀, N>` (output type changes)                             |
| `.filter(\|x\| p(x))` | `Pipe<Filter<...>, I₀, O>` (output unchanged)                                 |
| `.try_map(\|x\| …)`   | `TryPipe<TryMap<InfallibleChain<S, E>, F>, I₀, N, E>` (infallible → fallible) |

`ScopedPipe<'env, S, I, O>` mirrors this exactly with `'env` (non-`'static`)
closure bounds; `TryPipe<S, I, O, E>` adds the fixed error type `E` and exposes
`.try_map()` / `.map_err()` for further fallible chaining.

### 3.4 `FusedStage` / `FusedTryStage` Traits — Zero-Dispatch Execution

```rust
pub trait FusedStage<T> {
    type Output;
    /// Whether the chain can drop items (contains a `Filter`).
    const MAY_FILTER: bool = false;
    fn apply(&self, item: T) -> Option<Self::Output>;
    /// Branch-free variant used by the index-based hot path; sound only when
    /// `MAY_FILTER == false` throughout the chain.
    fn apply_pure(&self, item: T) -> Self::Output;
}
```

- `SyncMap::apply()` → `self.prev.apply(item).map(|v| (self.f)(v))` (also overrides `apply_pure` to thread `prev.apply_pure`, no `Option`)
- `Filter::apply()` → `self.prev.apply(item).filter(|v| (self.f)(v))` (sets `MAY_FILTER = true`; never on the pure path)
- `Identity::apply()` → `Some(item)` (the `pipe()` seed)

`MAY_FILTER` is propagated through `SyncMap` from the preceding stage.
`.collect()` uses it as a compile-time switch: when `false`, the stage chain is
driven by the index-based `Slots` fast path via the `RangeOp` wrapper `FusedOp`
(output cardinality equals input cardinality, branch-free leaf loop); when
`true`, it falls back to the per-leaf-`Vec` merge path (`join_fused_collect`).
The `apply_pure` fast path is what keeps the leaf vectorizable — it never
constructs an `Option`.

`FusedTryStage` is the fallible counterpart (returns
`Result<Option<Output>, Error>`): `TryMap` threads `Result` via `?`,
`InfallibleChain` adapts an infallible `FusedStage` chain to `FusedTryStage` at
the `.try_map()` boundary, and `MapErr` converts the error type. Driven by
`join_fused_try_collect` (always the `Vec`-merge path, since fallible +
filtering can't assume fixed cardinality).

### 3.5 `Pipe::collect()` / `TryPipe::try_collect()` — Execution

```rust
pub fn pipe<I, It>(items: It) -> Pipe<Identity, I, I>
impl<S, I, O> Pipe<S, I, O> {
    pub fn map<N>(...)  -> Pipe<SyncMap<S, ...>, I, N>
    pub fn filter(...)  -> Pipe<Filter<S, ...>, I, O>
    pub fn try_map<N, E>(...) -> TryPipe<TryMap<InfallibleChain<S, E>, ...>, I, N, E>
    pub fn collect(self) -> Vec<O>
}
```

`.collect()` dispatches on `S::MAY_FILTER`:

- **`MAY_FILTER == false`** — the index-based fast path. Input + output `Slots`
  are allocated once, then `par_index_rec` recursively splits the **index range**
  `[0, n)` (not the data) via `ComputePool::join`. Each leaf receives `&[T]` /
  `&mut [R]` slice views and runs the `RangeOp` (`FusedOp(stages)`) through
  `apply_pure` — branch-free and vectorizable. Workload selects the oversplit
  factor per §3.1.
- **`MAY_FILTER == true`** — `join_fused_collect` recursively halves the `Vec`,
  each leaf filters into a per-leaf `Vec`, results merged by `extend`.

`.try_collect()` dispatches on `S::MAY_FILTER`:

- **`MAY_FILTER == false`** — the index-based fast path (`par_index_try_collect`),
  mirroring `collect()`'s zero-allocation strategy but with `RangeTryOp` /
  `FusedTryOp` wrappers that short-circuit on `Err`. Each leaf's `TryLeafGuard`
  cleans up partial output on both panic (unwind) and error (explicit) paths.
- **`MAY_FILTER == true`** — `join_fused_try_collect` (Vec-merge fallback),
  short-circuiting on the first `Err` via `?` and honouring `Filter`.

#### `Pipe::for_each()` / `ScopedPipe::for_each()` — Side-Effect Terminal

```rust
impl<S, I, O> Pipe<S, I, O> {
    pub fn for_each<F>(self, f: F) where F: Fn(O) + Send + Sync + 'static;
}
impl<S, I, O> ScopedPipe<'_, S, I, O> {
    pub fn for_each<F>(self, f: F) where F: Fn(O) + Sync;
}
```

The counterpart of rayon's `par_iter().for_each(..)`. Allocates **no output
buffer** — the sink-only `par_for_each` core (`par_for_each_rec` /
`par_for_each_leaf`) drives only an input `Slots<T>` through the same
recursive `ComputePool::join` tree as `collect`, but each leaf applies the
fused chain via `FusedSink(stages, f)` (the `SinkOp` wrapper) and discards
each result. This is the structural fix for pure-side-effect pipelines: a
`.map(f).collect::<Vec<()>>()` would otherwise pay for an `n`-slot output
buffer + `n` writes for data nobody reads.

Panic safety is the input-tail mirror of `LeafGuard`: each leaf's
`ForEachGuard` drops `input[pos+1..]` on unwind (item `pos` was consumed by
`op` and is gone), then `mem::forget`s on success. There is no output to
clean up. Filter stages are honoured — `SinkOp::consume` dispatches on the
compile-time `MAY_FILTER` constant, so the pure path stays branch-free for
chains without `Filter`.

#### Borrowed input: `s.pipe(&[T])`

`PipelineScope::pipe` accepts any `IntoIterator`, and `&[T]: IntoIterator<Item = &T>`
— so `s.pipe(&files)` yields `ScopedPipe<'env, _, &'env T, &'env T>` with no
clone of `T`. The only allocation is one `Vec<&T>` of `n` pointers (the
youpipe counterpart of rayon's `slice.par_iter()`). This is the right entry
point when `T` is expensive to clone (e.g. `PathBuf`, `String`) and the
pipeline only reads each item by reference. For zero input allocation, pass
indices: `s.pipe(0..slice.len()).for_each(|i| f(&slice[i]))`.

### 3.6 `StreamPipe` — Streaming Multi-Stage Pipeline

For workloads that need channel-connected stages, async IO, cancellation,
fences, or 1-to-N expansion, `stream(items)` builds a `StreamPipe` whose stages
chain via builder methods and assemble a channel topology at `.run()` time:

```rust
stream(items)                       // StreamPipe<StreamStart, I, I>
    .stage(|x| f(x))                //   → SyncStage (compute pool workers)
    .expand(|x| vec![...])          //   → ExpandStage (1-to-N)
    .fence(FenceMode::Chunked(k))   //   → FenceLink (batching barrier thread)
    .stage_async(|x| async { .. })  //   → AsyncStage (tokio tasks, M:N)
    .ordered()                      // restore input order via ReorderBuffer
    .with_cancel(token)             // cooperative cancellation
    .run()                          // execute → Vec<O>
```

| Builder method        | Runtime topology                                                          |
| --------------------- | ------------------------------------------------------------------------- |
| `.stage(f)`           | `parallelism` compute-pool workers pull, apply `f`, forward               |
| `.expand(f)`          | like `.stage` but each input → `Vec<N>` outputs (inherits parent's `seq`) |
| `.fence(mode)`        | dedicated forwarder thread batching between adjacent stages               |
| `.stage_async(f)`     | `io_concurrency` tokio tasks on the async runtime (M:N)                   |
| `.ordered()`          | feeder tags each item with `seq`; collector reorders via `ReorderBuffer`  |
| `.with_cancel(token)` | feeder/workers/bridges check `is_cancelled()` per iteration               |

The stage chain is a typestate (`SyncStage<FenceLink<SyncStage<StreamStart,…>>>`)
walked by the `StageSpawn` trait — `spawn` recurses inside-out (older stages
first) so the data-flow direction matches. `worker_stages()` counts compute-pool
stages so `.run()` divides `compute_workers` across sync stages, preventing the
"stage 1 fills the pool → stage 2 starves → deadlock" failure mode.

#### Async IO stages

`.stage_async()` is gated behind the `tokio-runtime` feature. It runs an IO
stage as **`io_concurrency` async tasks** on a tokio runtime
([`AsyncPool`]). The runtime's M:N scheduler multiplexes those tasks over
`async_workers` OS threads: each task yields its thread back to the runtime while
it awaits (e.g. `tokio::time::sleep`, real network/disk IO), so concurrency is
bounded by `io_concurrency` — **not** by the thread count.

This is the right tool when IO waits actually yield. For work that _blocks_ the
OS thread (e.g. `std::thread::sleep`), a sync `.stage()` is preferable: a
blocking call inside an async task stalls a runtime worker and forfeits the M:N
advantage (blocking concurrency is then capped at the thread count).

A mixed sync-CPU + async-IO chain keeps the CPU stage on the sync compute pool
(rayon-style, sized to cores) and the IO stage on the async runtime; the two
overlap with the CPU stage's workers writing **directly** into the IO stage's
input channel — no bridge thread. The pools do not contend: CPU uses
`compute_workers` OS threads, IO uses `async_workers` OS threads multiplexing
`io_concurrency` tasks.

Every sync→async edge uses crossfire's mixed-mode channel (`SyncSender` +
`AsyncReceiver` sharing one `mpmc::Array` — `bounded_blocking_async`).
`StageSpawn::spawn_for_async` lets each stage pick the channel kind that lets
its producers run with least friction: sync stages (sync / expand / fence)
override it so their ComputePool workers write the `SyncSender` directly
(backpressure parks the worker on `Full` — correct, since they're OS threads),
while the async consumers `recv().await` from the _same_ queue. One channel,
zero forwarding threads — for `stream(..).stage_async(..)` *and*
`stream(..).stage(cpu).stage_async(io)` alike.

A bridge thread survives only on the `spawn_async_feeder` path — chains whose
*first* stage is async (e.g. `..stage_async(f1).stage(f2).stage_async(f3)`),
where a sync stage reached through an async→sync conversion feeds a trailing
async stage. Keeping the blocking `send` off the tokio worker avoids the
"one thread is both async driver and blocking worker" anti-pattern: a
`SyncSender::send` inside a `tokio::spawn` task would park the runtime worker
under backpressure, stalling every other task on it (or deadlocking a
single-worker runtime — covered by the
`test_sync_to_async_does_not_stall_tokio_driver` regression test).

An [`AsyncPool`] may be attached via `.with_async_pool(...)` and reused across
runs; otherwise a transient runtime is built per call (simpler, but pays
~ms runtime construction each time — avoid inside tight loops).

### 3.7 `ScopedPipe` — Non-`'static` Pipeline

```rust
youpipe::scope(|s| {
    let factor = 10;
    s.pipe(0..100)                 // data-first, like pipe()
        .map(|x: i32| x * factor)  // borrows stack-local factor
        .collect()                 // → Vec<i32>
})
```

Mirrors `Pipe`'s compile-time-fused stage chain (`SyncMap` / `Filter` /
etc.) but with `'env` (non-`'static`) closure bounds. `.collect()` drives the
same recursive work-stealing `par_index_collect` core as `Pipe::collect`
— exposed via the `pub(crate) fused_collect_scoped` entry point — so the
soundness story rests on `ComputePool::join`: the calling thread blocks in
`Registry::in_worker_cold` until every sub-task finishes, which guarantees
every `'env` reference captured by a scoped closure outlives the pool's
access to it.

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
- Global injector is a lock-free `concurrent_queue::ConcurrentQueue` (unbounded) that accepts externally submitted tasks and local-queue overflow
- `EventCount`-style packed atomic counters (`pool/sleep.rs`) wake idle workers

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

### Graceful Shutdown

`ComputePool::Drop` calls `Registry::terminate()`, which decrements a ref-count
(`terminate_count`); when the last clone drops (count 1→0) it sets each worker's
`terminate` OnceLatch and tickles it awake. Each worker's `wait_until_out_of_work`
then drains its remaining local-deque work, sets its `stopped` latch, and exits;
`Registry::Drop` blocks on every worker's `stopped` before returning.

---

## 5. Data Transfer Layer (`handoff/`)

### 5.1 MPMC Channels (`channel.rs`)

Wraps `crossfire` with a unified API:

| Type                                  | Implementation                      |
| ------------------------------------- | ----------------------------------- |
| `SyncSender<T>` / `SyncReceiver<T>`   | `crossfire::mpmc::bounded_blocking` |
| `AsyncSender<T>` / `AsyncReceiver<T>` | `crossfire::mpmc::bounded_async`    |

Closure detection is delegated entirely to crossfire: `send`/`recv` return
`Closed` once crossfire's internal disconnect logic observes that all peers
have been dropped. No extra flag is maintained.

### 5.2 WaitGroup (`notify.rs`)

Counter barrier: `add(n)` increments, `done()` decrements, `wait()` blocks until zero. When count transitions 1→0, condvar broadcasts. Used internally by streaming stages to track worker completion.

---

## 6. Ordered Output (`state/reorder.rs`)

`ReorderBuffer<T>` restores original element order after parallel processing. It is a fixed-size array of `2^k` slots addressed by bitmask: `seq & mask` maps a sequence number to its slot.

1. Each element is sent with a sequence number `(seq, item)`
2. `insert(seq, item)` writes the item directly into slot `seq & mask` (constant time, no comparison)
3. `flush_ready()` walks contiguous slots starting at `next_expected`, draining any prefix that has arrived in order; returns the drained items
4. `flush_remaining()` collects whatever is still outstanding (e.g. on disconnect) and returns it sorted by `seq` — the only path that pays for a comparison sort

Capacity contract: because of the bitmask mapping, the number of simultaneously outstanding (un-flushed) items must stay below the slot count or two distinct `seq`s alias the same slot and the older item is dropped. Callers size the buffer to at least the maximum out-of-order window; the streaming collectors clamp it to `[1 Ki, 1 Mi]` slots.

---

## 7. Fence Barrier (`state/fence.rs` + `StreamPipe::fence`)

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

| Environment | Injector mutex                                                                |
| ----------- | ----------------------------------------------------------------------------- |
| Production  | `parking_lot::Mutex` (zero-cost re-export — fairer, no poisoning)             |
| Miri        | `std::sync::Mutex` wrapped in a newtype exposing the same infallible `lock()` |

`parking_lot_core` resolves `WaitOnAddress` through `GetModuleHandleA`, a Windows foreign function Miri cannot emulate, whereas the std mutex/condvar are natively supported. The unified API lets callers write `mutex.lock()` once and stay transparent to which backend is active.

---

## 9. Performance Benchmarks

> All numbers below are from a 32-core AMD (Zen) Linux machine, `criterion`
> `--sample-size 30 --measurement-time 5`. Methodology note: `pipe()` takes
> ownership of the input, so a benchmark iteration must rebuild the input
> (`warm_clone`). glibc's large `memcpy` uses non-temporal stores that bypass
> the cache, so a naïve `data.clone()` arrives **cold-from-RAM** — measuring
> allocator/memory latency rather than the framework. The `sync_vs_rayon` bench
> therefore warms the input in the (untimed) setup so the timed region is a
> fair, like-for-like comparison with rayon's warm `par_iter` borrow. A
> `_cold` variant is kept for the lightweight group to document the one-shot
> cold-memory cost.

### CPU-Heavy `pipe()` vs rayon (`sync_cpu_heavy`, 100 iters/item, warm input)

| Size | youpipe | rayon   |
| ---- | ------- | ------- |
| 1K   | ~34 µs  | ~40 µs  |
| 10K  | ~66 µs  | ~73 µs  |
| 100K | ~106 µs | ~147 µs |

### Pipeline Fusion (3 stages) vs rayon chain (`pipeline_fusion`, warm input)

| Size | youpipe fused | rayon chain |
| ---- | ------------- | ----------- |
| 10K  | ~65 µs        | ~70 µs      |
| 100K | ~90 µs        | ~104 µs     |

The fused stage chain trailed rayon at every size in mid-2026 and now wins or
draws at every size after four changes: a sleeping-bitmask rewrite of
`wake_any_threads` that directed wakes only at parked workers, moving the
`condvar.notify_one` outside the `is_blocked` mutex (which had been serialising
every woken thread's re-acquire), a `.cargo/config.toml` override that ensures
the perf-friendly `opt-level=3`/`panic=unwind` regardless of the host's global
cargo profile, and adaptive oversplit (`workload_oversplit`) that drops to
`oversplit = 1` for small batches (≤ 1024 items/worker) — trimming ~95 fork/join
internal nodes from 10 k batches and flipping the 10 k case from trailing rayon
to beating it. The remaining per-call scheduler fixed cost (~50 µs) is now only
visible on batches far below the serial short-circuit threshold.

### Lightweight `pipe()` vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (warm) | youpipe (cold) | rayon   |
| ---- | -------------- | -------------- | ------- |
| 10K  | ~64 µs         | ~71 µs         | ~69 µs  |
| 100K | ~85 µs         | ~131 µs        | ~110 µs |
| 1M   | ~606 µs        | ~4.15 ms       | ~272 µs |

Warm-input lightweight 1M improved from ~1.9 ms (pre-`Slots`) → ~730 µs
(after `Slots`) → ~390 µs (after switching the leaf loop to a `&[T]` /
`&mut [R]` slice view) → ~574 µs after the perf-config fix + the
sleeping-bitmask wake rewrite + the notify-outside-lock fix. The slice view
step closed the gap with rayon: the previous `&Slots<u64>` for both input and
output blocked LLVM's auto-vectorizer because the alias analysis could not
prove the two `UnsafeCell`-wrapped buffers were disjoint. The 1 M case still
trails rayon because the leaf work itself is so cheap (~0.12 ns/item) that
scheduling overhead dominates; at 10 k and 100 k youpipe now beats rayon
because the leaf amortises the overhead better. The cold variant documents the
read-compute-write sensitivity to cold-from-RAM input (glibc's non-temporal
memcpy bypasses the cache); rayon on an equally-cold clone measures ~800 µs
at 1M.

### Fallible `try_map().try_collect()` vs rayon (`try_collect`, warm input)

When the chain has `MAY_FILTER == false`, `try_collect` uses the same
zero-allocation index-based fast path as `collect` — pre-allocating the output
buffer and writing at known indices instead of the `Vec`-merge fallback.

| Size | youpipe try_map | rayon   |
| ---- | --------------- | ------- |
| 10K  | ~64 µs          | ~68 µs  |
| 100K | ~88 µs          | ~101 µs |

### Mixed Load — `stream()` vs `tokio::spawn_blocking` (`mixed_load`)

| Size | youpipe stream | spawn_blocking | rayon (CPU-only) |
| ---- | -------------- | -------------- | ---------------- |
| 1K   | ~832 µs        | ~2.92 ms       | ~38 µs           |
| 10K  | ~9.5 ms        | ~27.4 ms       | ~68 µs           |
| 100K | ~95.9 ms       | ~239 ms        | ~111 µs          |

`StreamPipe` beats `tokio::spawn_blocking` (the design target for mixed CPU/IO)
at every size, with the margin widest at smaller sizes where per-task spawn
overhead dominates tokio's cost, and narrowing at larger sizes where channel
bandwidth becomes the bottleneck. `rayon::par_iter` is fastest here because
this benchmark is pure-CPU and rayon's direct fork-join skips channel handoff
entirely. All youpipe variants use `warm_clone` (cache-warmed input) for fair
comparison against rayon's warm borrow.

### Async IO — `.stage_async()` (`io_async`, yielding IO)

Simulated IO uses `tokio::time::sleep` (90% × 1 ms, 10% × 8 ms tail) — a wait
that _yields_ the OS thread, the regime where M:N async concurrency beats the
blocking-thread-per-core model. `io_concurrency = 512`, 32-core machine.

#### Pure IO (`io_async_pure`)

| Size | youpipe_async | youpipe_blocking | youpipe_blocking_oversub | tokio_async_native | tokio_spawn_blocking |
| ---- | ------------- | ---------------- | ------------------------ | ------------------ | -------------------- |
| 200  | ~9.32 ms      | ~16.56 ms        | ~11.31 ms                | ~9.16 ms           | ~8.38 ms             |
| 500  | ~9.65 ms      | ~33.08 ms        | ~19.46 ms                | ~9.30 ms           | ~8.83 ms             |

`youpipe_async` matches `tokio_async_native` (the async ceiling) within ~3% and
stays well ahead of `youpipe_blocking`. `tokio_spawn_blocking` edges it via
tokio's 512-thread blocking pool — aggressive OS-thread oversubscription that
only pays off for pure-sleep (no CPU) work. The gap to the async ceiling shrank
after three changes: eliminating the sync→async bridge thread for
`stream(..).stage_async(..)` (the feeder pushes into a mixed-mode `SyncSender`
+ `AsyncReceiver` channel that the AsyncStage consumes directly — see §3.6),
and replacing the collector's per-item `recv().await` with a `try_recv`
burst-drain that absorbs tokio's timer-tick completion bursts without per-item
waker overhead.

`youpipe_blocking_oversub` uses `.with_compute_pool(ComputePool::new(512))`
to match tokio's 512-thread blocking pool, narrowing the gap substantially.
The remaining gap is streaming infrastructure overhead (channel handoff,
injector scheduling) — the tradeoff for backpressure, ordering, and
multi-stage composition that raw `spawn_blocking` doesn't provide. For
blocking IO, `.stage_async()` remains the recommended tool.

#### Mixed CPU (sync) + IO (`io_async_mixed`)

| Size | youpipe_mixed_async | youpipe_mixed_blocking | tokio_mixed_blocking |
| ---- | ------------------- | ---------------------- | -------------------- |
| 200  | ~9.48 ms            | ~27.3 ms               | ~8.93 ms             |
| 500  | ~9.97 ms            | ~60.0 ms               | ~10.1 ms             |

`youpipe_mixed_async` stays well ahead of the all-blocking two-stage baseline,
and at size 500 edges out `tokio_mixed_blocking` by ~150 µs: the async path
overlaps the CPU and IO stages on separate pools, whereas the all-blocking path
splits one compute pool between two blocking stages. At size 200 the fixed
per-run setup cost (feeder thread, channel allocation, runtime entry) is a
larger fraction of the ~9 ms total, so tokio's simpler spawn-per-item model
still leads there.

### Channel Throughput

| Size | crossfire    | crossbeam-channel | std_mpsc     |
| ---- | ------------ | ----------------- | ------------ |
| 10K  | 27.1 Melem/s | 20.2 Melem/s      | 37.0 Melem/s |
| 100K | 34.7 Melem/s | 17.3 Melem/s      | 44.6 Melem/s |

---

## 10. Extending the System

### Adding a New Fused Stage

1. Define a stage struct implementing `StageMarker<T>` and `FusedStage<T>`
2. Add a builder method on `Pipe<S, I, O>` returning `Pipe<NewStage<S, ...>, I, NewO>`
3. In `FusedStage::apply()` (and `apply_pure` if the stage can't filter), compose `self.prev.apply(item)` with the new logic

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
