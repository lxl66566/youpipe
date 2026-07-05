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

### Async runtime

Async stages (`stage_async`) run on tokio via [`AsyncPool`] (a
`tokio::runtime::Handle` wrapper). The runtime is feature-gated behind
`tokio-runtime` (the default); building without it produces a sync-only
crate that exposes no async APIs (`AsyncStage`, `AsyncPool`, `stage_async`
all disappear). Callers can attach a managed runtime via `.with_async_pool`
or let the pipeline build a transient one per `run()` call.

There is intentionally no `Runtime` trait abstraction: the streaming code
calls tokio APIs directly (e.g. `tokio::spawn`, `Handle::block_on`), and
introducing a trait would either leak tokio types through it or force a
wrapper that loses tokio's specific capabilities. `AsyncPool::new(handle, n)`
already lets a caller hand in any `tokio::runtime::Handle` (including one
built externally or shared across runs), which covers the realistic
"runtime-agnostic" use cases without an under-used abstraction layer.

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

### 3.1 `Workload` — Per-Item Cost Distribution Hint

```rust
pub enum Workload {
    Balanced,    // default; adaptive oversplit (1× for small batches, 4× for large)
    Unbalanced,  // 8× oversplit for finer-grained stealing of skewed tails
}
```

A hint about how skewed each item's wall-clock cost is **within a single
`pipe(..).collect()` / `for_each()` run** (not how items are spread across
streaming stages). It selects the fork/join oversplit factor
(`workload_oversplit`):

- `Balanced` (the default) — items cost roughly the same. Adaptive: when the
  batch is small enough that per-leaf work is sub-microsecond
  (`n / num_threads ≤ 1024`), it drops to `oversplit = 1` to avoid paying
  fork/join dispatch overhead for stealing slack it does not need; above that
  threshold it uses 4×.
- `Unbalanced` — a few items are far slower than the rest (skewed tail). Always
  uses 8× oversplit so an idle worker can steal a slow sibling's remaining
  leaves, shrinking tail latency. Opt in only when the tail is genuinely uneven.

**Scope.** Only the fused path (`pipe` / `scope` / `try_map`) consults this.
The streaming path (`stream(..)`) ignores it: streaming already load-balances
per-item skew through its MPMC channel + `per_stage_parallelism` workers (a
stalled worker simply stops draining while peers keep consuming), and there is
no fork/join oversplit decision to tune. To control streaming tail latency,
raise `compute_workers` / `per_stage_parallelism`.

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
    pub fn with_compute_pool(pool: ComputePool) -> Self
    pub fn collect(self) -> Vec<O>
}
```

`.collect()` dispatches on `S::MAY_FILTER`:

- **`MAY_FILTER == false`** — the index-based fast path. Input + output `Slots`
  are allocated once, then the top-level dispatcher splits `[0, n)` into
  **`num_threads` contiguous chunks** stored in a single `Box<[ChunkJob]>` (one
  heap allocation for all chunks, not per-chunk `Box`es) and injects them in a
  single `inject_batch` (hybrid flat/tree dispatch — see `hybrid_dispatch`).
  Every pool worker pops a chunk on its first `find_work`, so all workers are
  busy from t≈0 — no fork/join ramp-up. Each chunk then recurses via
  `ComputePool::join` (the per-chunk tree uses distributed local deques +
  stealing, avoiding the single-injector MPMC contention that sank pure flat
  dispatch). Each leaf receives `&[T]` / `&mut [R]` slice views and runs the
  `RangeOp` (`FusedOp(stages)`) through `apply_pure` — branch-free and
  vectorizable. Workload selects the oversplit factor per §3.1. The hybrid
  path is skipped when `.collect()` is reached from inside a worker of the
  *same* pool (e.g. nested `scope`), where the `CountLatch` park would
  deadlock — the single-tree `par_index_rec` runs instead. A worker of a
  *different* pool can safely take the hybrid path.
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

**Hybrid dispatch.** `par_for_each` shares the exact same
`hybrid_dispatch` machinery as `collect` — `num_threads` broad top-level
chunks injected in one `inject_batch`, every worker busy at t≈0, each chunk
recursing via the tree for distributed stealing. The only two differences
from `collect` (no output buffer, no per-chunk panic cleanup) are abstracted
behind the `SinkStrategy` impl of the `HybridStrategy` trait, so the
chunk-layout / inject / `CountLatch::wait_spin` / panic-funnel code is
written once and monomorphized per terminal (no vtable cost). When reached
from a worker of the *same* pool (nested `scope`), the hybrid `CountLatch`
park would deadlock, so it falls back to the single-tree `par_for_each_rec`.

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

#### `with_compute_pool` — Oversubscription for Blocking IO

All three fused builders (`Pipe`, `TryPipe`, `ScopedPipe`) accept a custom
`ComputePool` via `.with_compute_pool(pool)`. When omitted, the pipeline runs
on the global pool (one thread per core).

The primary use case is **blocking-IO sync workloads** — e.g. file
encryption/decryption where each leaf does `read → crypto → write`. The global
pool's `num_cpus` threads cap blocking concurrency at the core count: when a
leaf blocks on a syscall, its core sits idle with no stealable work to fill the
gap (all remaining leaves are being processed by other blocked workers). This
is the "cores idle during IO stalls" regime where wall time exceeds rayon
despite youpipe's better per-CPU efficiency.

An oversubscribed pool (e.g. `ComputePool::new(num_cpus * 2)`) lets other
threads use those idle cores for CPU work while blocked threads wait — the same
technique tokio's `spawn_blocking` and `StreamPipe::with_compute_pool` use.
Benchmarked (`fused_oversubscribe`, 32-core): a mixed CPU+IO `for_each` over
1000 items (90% × 100µs IO, 10% × 2ms tail) ran ~1.8× faster with 2×
oversubscription than the global pool, soundly beating rayon's global pool.

`ComputePool` is cheap to clone (`Arc` + one atomic), so the pool can be
created once and reused across many `collect()` / `for_each()` calls — important
for tight loops where per-call pool construction (~ms) would dominate.

**Convenience: `with_oversubscribe(factor)`.** All three builders also accept
`.with_oversubscribe(factor)` — a one-liner that internally creates a pool
sized to `factor × num_cpus` at execution time. This is the shortest path for
one-shot blocking-IO pipelines:

```rust
pipe(files)
    .with_oversubscribe(2)   // ← factor × num_cpus threads
    .for_each(|f| { /* read → crypto → write */ });
```

The pool is **transient** — created at `.collect()` / `.for_each()` time and
dropped when the terminal returns. For repeated calls in a tight loop,
pre-create the pool and use `.with_compute_pool(pool.clone())` instead (clone
is cheap: `Arc` + one atomic). If both are set, `with_compute_pool` takes
precedence.

**Do not** use oversubscription for pure-CPU workloads — extra threads beyond
the core count only add context-switch overhead and cache thrashing (measured
10–30 % regression on CPU benchmarks).

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
access to it. `.with_compute_pool(pool)` is supported — the headline use case
is oversubscribing threads for blocking-IO workloads while still borrowing
stack-local data (key caches, lookup tables) by reference.

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

### 5.2 MPSC Channels — Collector Optimisation

The streaming pipeline's collector is always the **sole consumer** of the final
output channel (multiple worker producers → one collector). This is an MPSC
topology, yet crossfire's `mpmc` module uses a CAS-based ring buffer
(`lock cmpxchg` on every dequeue) and a `Mutex<VecDeque>` waker registry —
both unnecessary for single-consumer patterns.

crossfire ships an `mpsc` module whose receiver uses:

- **`store`-based dequeue** instead of `lock cmpxchg` (single consumer → no
  contention to CAS against). Profiling showed the MPMC ring-buffer CAS
  dominates per-item cost (~20–40 % of channel throughput depending on
  contention).
- **`WeakCell` waker registry** (lock-free) instead of `Mutex<VecDeque>`.

youpipe exposes this via `MpscSender<T>` / `MpscReceiver<T>` (sync sender +
sync receiver), `mpsc_sync_async_channel` (sync sender + async receiver),
and `mpsc_async_channel` (`MpscAsyncSender` + `MpscAsyncReceiver` — both
ends async, used when async-stage consumer tasks feed the sole async
collector). The `StageSpawn` trait gains a `spawn_single` method that
creates the terminal stage's output channel as MPSC instead of MPMC;
`StreamPipe::try_run` calls `spawn_single` for the terminal path —
covering sync stages, fence links, expand, and `AsyncStage` (whose
`spawn_single` override routes the consumer fan-out into an
`mpsc_async_channel`). Intermediate stage channels remain MPMC (their
receivers are shared across multiple worker threads via `clone`).

The collector itself is generic over a `RecvItem` (sync) or `AsyncRecvItem`
(async) trait, so `collect_sync` / `collect_async` drain either the MPMC or
MPSC backing with one implementation.

`SendItem<T>` / `RecvItem<T>` / `AsyncRecvItem<T>` traits abstract over the
channel backings (MPMC vs MPSC, sync vs async) so `spawn_stage` and the
collector functions are generic without virtual dispatch.

### 5.3 WaitGroup (`notify.rs`)

Counter barrier: `add(n)` increments, `done()` decrements, `wait()` blocks until zero. When count transitions 1→0, condvar broadcasts. Used internally by streaming stages to track worker completion.

---

## 6. Ordered Output (`state/reorder.rs`)

`ReorderBuffer<T>` restores original element order after parallel processing. It is a fixed-size array of `2^k` slots addressed by bitmask: `seq & mask` maps a sequence number to its slot.

1. Each element is sent with a sequence number `(seq, item)`
2. `insert_into(seq, item, &mut Vec<T>)` writes the item directly into slot
   `seq & mask` (constant time, no comparison), then drains any contiguous run
   ready at the tail **straight into the caller's sink** — zero per-item
   allocation. (The older `insert` returned a fresh `Vec<T>` per call; in the
   in-order steady state that returned `Vec` had length 1, so the ordered
   collector paid a `malloc` + `free` per item purely to move a single value.
   `insert_into` is the hot-path variant; `insert` remains as a thin wrapper
   for tests / ergonomic callers.)
3. `flush_remaining()` collects whatever is still outstanding (e.g. on disconnect) and returns it sorted by `seq` — the only path that pays for a comparison sort

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

The `util/sys` module provides a unified `Mutex` API via `cfg(miri)`:

| Environment | Injector mutex                                                                |
| ----------- | ----------------------------------------------------------------------------- |
| Production  | `parking_lot::Mutex` (zero-cost re-export — fairer, no poisoning)             |
| Miri        | `std::sync::Mutex` wrapped in a newtype exposing the same infallible `lock()` |

`parking_lot_core` resolves `WaitOnAddress` through `GetModuleHandleA`, a Windows foreign function Miri cannot emulate, whereas the std mutex/condvar are natively supported. The unified API lets callers write `mutex.lock()` once and stay transparent to which backend is active.

### Build-profile guard (`lib.rs`)

youpipe ships a `.cargo/config.toml` override (`opt-level=3`, `panic=unwind`) that applies inside its workspace but is **not** inherited by downstream crates. Two downstream profile settings are known to be harmful; one is detected at compile time, the other is documented only:

- `panic = "abort"` — disables `catch_unwind`, so the `LeafGuard` / `ForEachGuard` panic-safety paths never run; any panic inside a pool worker aborts the process instead of propagating. Detected in `lib.rs` via `#[cfg(panic = "abort")]` (the deprecated-const warning trick) — this is accurate inside the library compilation, unlike cargo's build-script `CARGO_CFG_PANIC` env var which mirrors the build-script's own panic strategy (always `unwind`), not the target crate's.
- `opt-level = "s"` / `"z"` — disables the leaf-loop auto-vectorizer (~2× regression on the lightweight warm path). No longer detected at compile time: the rationale and the `[build] rustflags = ["-C", "opt-level=3"]` override recipe live in youpipe's own `.cargo/config.toml` comment for downstream users to copy.

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
| 1K   | ~59 µs  | ~38 µs  |
| 10K  | ~61 µs  | ~69 µs  |
| 100K | ~102 µs | ~137 µs |

The 1K case still trails rayon but the gap narrowed from ~33 µs to ~21 µs after
two changes that together removed ~11 µs of fixed overhead:

1. **`Workload::Balanced` is now the default** (was `Unbalanced` → oversplit 8).
   For 1K/10K batches `n / num_threads ≤ 1024`, so the adaptive path picks
   `oversplit = 1` (32 leaves) instead of 8 (256 leaves) — far fewer internal
   nodes to dispatch.
2. **Spin-then-park for the off-pool wait** (`CountLatch::wait_spin`): the
   hybrid driver tight-spins on the `counter` atomic for a bounded budget
   (4096 PAUSE iters ≈ 100–150 µs) before acquiring the latch's mutex. In the
   short-wait regime the last chunk's `fetch_sub` lands inside the spin window,
   so the condvar park/notify syscall (~10–20 µs of fixed overhead) is skipped
   entirely; long waits still fall through to the condvar. The mutex acquire is
   load-bearing for soundness — it serializes against the last chunk's
   in-flight `LockLatch::set`, preventing a use-after-free (spinning on the
   counter and returning directly would race the latch free against that
   access; observed as SIGSEGV).

The residual ~21 µs is no longer the condvar handshake — it is the inject+
wake cascade (pushing `num_threads` JobRefs through the injector + waking the
workers). An attempt to close it by injecting a single root job (mirroring
rayon's `join` unfold) **regressed** — the log2(num_threads) ramp-up via
work-stealing cost more than the per-chunk overhead it saved (hotpath
confirmed `steal` at ~98 ns is not the bottleneck; the inject + wake cascade
is). The real wins were:

1. **`Box<[ChunkJob]>` consolidation**: all `num_threads` chunks share one
   heap allocation (1 instead of N), saving the per-chunk malloc/free.
2. **Driver-inline participation** (mirrors rayon's calling thread): chunk 0
   runs on the off-pool driver while the pool handles chunks 1..N. This saves
   1 injector push and reduces the condvar wake cascade by 1. Guarded by
   `chunk_splits == 0` (small/medium batches where the chunk is a single leaf)
   to avoid memory-bandwidth contention on large memory-bound workloads.

Together these shaved ~5–8 % off 1K–10K `collect` batches.

An earlier version silently routed small batches to a serial loop to win this
benchmark, but that was deceptive (the API promises parallelism) and
catastrophic for expensive per-item work (file IO, crypto) whose small batches
would be wrongly serialized. The heuristic was removed — see `prefers_serial`
in `src/builder/typed/fused.rs`.

### Pipeline Fusion (3 stages) vs rayon chain (`pipeline_fusion`, warm input)

| Size | youpipe fused | rayon chain |
| ---- | ------------- | ----------- |
| 10K  | ~59 µs        | ~66 µs      |
| 100K | ~79 µs        | ~100 µs     |

The fused stage chain now beats rayon at every size after five changes: the
sleeping-bitmask rewrite of `wake_any_threads`, moving the `condvar.notify_one`
outside the `is_blocked` mutex, the `.cargo/config.toml` perf-friendly
`opt-level=3`/`panic=unwind` override, adaptive oversplit (`workload_oversplit`,
which drops to `oversplit = 1` for small batches ≤ 1024 items/worker), and
**hybrid flat/tree dispatch** (`par_index_collect_hybrid`) — injecting
`num_threads` broad top-level chunks so every worker starts busy at t≈0 with no
fork/join ramp-up, while each chunk recurses via the tree for distributed
stealing. The hybrid alone measured −6.5 % @ 10 k and −6.7 % @ 100 k.

### Lightweight `pipe()` vs rayon (`sync_lightweight`, `x+1`)

| Size | youpipe (warm) | youpipe (cold) | rayon   |
| ---- | -------------- | -------------- | ------- |
| 10K  | ~59 µs         | ~67 µs         | ~64 µs  |
| 100K | ~77 µs         | ~120 µs        | ~105 µs |
| 1M   | ~540 µs        | ~4.23 ms       | ~273 µs |

Warm-input lightweight improved ~1.9 ms (pre-`Slots`) → ~730 µs (after `Slots`)
→ ~390 µs (slice view) → ~570 µs (after perf-config + sleeping-bitmask wake +
notify-outside-lock) → **~516 µs after hybrid flat/tree dispatch** (which alone
shaved −9.6 % / −55 µs by eliminating fork/join ramp-up). The 1 M case still
trails rayon because the leaf work itself is so cheap (~0.12 ns/item) that the
off-pool spin/mutex wait + per-chunk tree fixed cost dominate; at 10 k and
100 k youpipe beats rayon because the leaf amortises the overhead better.

### Fallible `try_map().try_collect()` vs rayon (`try_collect`, warm input)

When the chain has `MAY_FILTER == false`, `try_collect` uses the same
zero-allocation index-based fast path as `collect` — pre-allocating the output
buffer and writing at known indices instead of the `Vec`-merge fallback.

| Size | youpipe try_map | rayon   |
| ---- | --------------- | ------- |
| 10K  | ~64 µs          | ~66 µs  |
| 100K | ~85 µs          | ~98 µs  |

### `for_each()` vs rayon (`sync_for_each`, cpu_heavy per item, warm input)

| Size | youpipe `for_each` | rayon `for_each` |
| ---- | ------------------ | ---------------- |
| 1K   | ~62 µs             | ~47 µs           |
| 10K  | ~183 µs            | ~203 µs          |
| 100K | ~1.54 ms           | ~1.49 ms         |

`for_each` was the last fused terminal still on the single-tree path — it
never went through `hybrid_dispatch`'s `inject_batch` +
`CountLatch::wait_spin` pattern, so it paid the full fork/join ramp-up cost.
Porting it to the shared `hybrid_dispatch` (via the `SinkStrategy` impl of
`HybridStrategy`) measured **−8.7 % @ 1K, −7.2 % @ 10K, −5.0 % @ 100K** vs the
prior tree-only `par_for_each`. At 10K youpipe now beats rayon; the 1K case
still trails because the off-pool driver blocks instead of participating the
way rayon's `par_iter` runs inline on the caller (a known remaining gap —
see `docs/ARCHITECTURE.md` "the off-pool driver blocks" note under
`sync_cpu_heavy`). A subsequent change consolidated all `num_threads` chunk
jobs into a single `Box<[ChunkJob]>` (1 heap allocation instead of N+1),
which shaved a further **~3 % @ 1K–10K** by eliminating the per-chunk
malloc/free overhead. An attempt to instead inject a single root job (rayon's
`join`-unfold pattern) **regressed** — the work-stealing ramp-up cost exceeded
the per-chunk savings on youpipe's scheduler, so the hybrid chunk strategy was
kept.

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
