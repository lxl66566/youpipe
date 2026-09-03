# Design Philosophy & Module Map

> [← Documentation index](README.md)

### Data-First

youpipe's public API is **data-first**: items enter the pipeline at the front
(`pipe(items)` / `pipe_ref(&items)` / `stream(items)` / `scope(|s| s.pipe(items))`),
stages chain via builder methods, and a single terminal call (`.collect()` /
`.run()`) executes the whole chain — mirroring `iter().map().collect()`, not
"define the pipeline, then feed data at the end".

**Two fused entry points, split by input shape** (mirroring rayon's
`into_par_iter` vs `par_iter`):

- `pipe(items)` — the *general* entry: any `IntoIterator` (ranges, generators,
  owned `Vec`s). Takes ownership; closures receive `T`; element bound
  `T: Send`.
- `pipe_ref(&items)` — the *borrowed* entry: `&[T]` only. Reads in place;
  closures receive `&T`; nothing is consumed, materialized, or freed; element
  bound `T: Sync`. The natural shape for read-only transforms over existing
  data (the dominant rayon `par_iter` use case) and the basis of the
  like-for-like "idiomatic vs idiomatic" bench rows.

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
separate (work-stealing join vs channel handoff) — see the `StreamPipe`
section of [core-types.md](core-types.md#streampipe--streaming-multi-stage-pipeline).

### Non-`'static` Lifetime Support

The `scope()` API allows closures to borrow stack-local variables without `'static` bounds. The `'env` lifetime is threaded through `ScopedPipe` and the underlying `fused_collect_scoped` drives the same `ComputePool::join` work-stealing core — whose `Registry::in_worker_cold` blocks the calling thread until every spawned sub-task finishes — guaranteeing borrowed references outlive the pool's access to them.

`pipe_ref` gets the same property without `scope`: the `'a` input borrow brands
every `PipeRef` closure, and the terminal blocks until every worker finishes —
borrowed inputs and captured stack locals are both covered.

### Async runtime

Async stages (`stage_async`) run on a pluggable [`AsyncRuntime`] backend.
**tokio is the only concrete backend today** (`tokio-runtime` feature, the
default; [`TokioPool`] wraps a `tokio::runtime::Handle`), but the runtime is
abstracted so the streaming code never calls tokio APIs directly. A future
non-tokio backend slots in as one more `AsyncRuntime` impl without touching
`stream.rs`.

`StreamPipe<S, I, O, R: AsyncRuntime = DefaultRuntime>` is generic over the
backend so every `spawn` / `block_on` monomorphises to the concrete type —
zero virtual dispatch. Sync-only chains never instantiate `R`, so the
abstraction is free for them. Callers attach a managed runtime via
`.with_async_pool` (changing `R` to the pool's type) or let the pipeline build
a transient one per `run()` call.

The old `Handle::enter` TLS guard was dropped entirely: `Handle::spawn` needs
no current-runtime context, and a future backend whose context mechanism has no
RAII guard would not have been representable alongside it. The channel layer
(`crossfire`) keeps sync and async stages decoupled from the executor.

> **`block_on`'s `!Send` bound.** `collect_async` borrows crossfire's MPSC async
> receiver (`!Sync`), so its future is `!Send`; `block_on` therefore deliberately
> omits `F: Send` (tokio's `Handle::block_on` runs inline on the calling thread
> and matches). A backend driving `block_on` on another thread would have to
> reconcile this with the `!Send` collector future.

---

```
src/
├── builder/          # Strongly-typed data-first API + compile-time fusion + StreamPipe
│   ├── mod.rs        # Public re-exports
│   ├── config.rs     # PipelineConfig, Workload enum
│   └── typed/        # Pipe / TryPipe / StreamPipe builder core
│       ├── mod.rs    # Re-exports
│       ├── fused.rs  # pipe(), Pipe<S,I,O>, TryPipe<S,I,O,E>, par_index_* core,
│       │             #   fused_collect_scoped (pub(crate) entry for scope)
│       ├── stream.rs # stream(), StreamPipe<S,I,O,R>, StageSpawn typestate chain
│       ├── traits.rs # FusedStage / FusedTryStage / RangeOp / stage markers
│       │             #   (SyncMap / Filter / TryMap / MapErr / InfallibleChain)
│       └── slots.rs  # Slots<T> index-based zero-copy buffer
├── executor/
│   ├── compute/      # st3 work-stealing CPU thread pool
│   │   ├── mod.rs    # ComputePool unit tests
│   │   └── worker.rs # ComputePool: Injector/Stealer/sleep counters wake/graceful shutdown/join
│   └── mod.rs
├── handoff/          # Data transfer layer
│   ├── channel.rs    # MPMC channels (crossfire wrapper: sync + async)
│   ├── notify.rs     # WaitGroup (counter barrier for stage synchronization)
│   └── mod.rs
├── runtime/          # Pluggable async-runtime backend for streaming .stage_async
│   ├── mod.rs        # AsyncRuntime trait, NoRuntime, DefaultRuntime alias
│   └── tokio.rs      # TokioPool (tokio::runtime::Handle wrapper)
├── state/            # Ordered output & streaming execution
│   ├── reorder.rs    # ReorderBuffer<T> (bitmask slot array for restoring ordered output)
│   ├── fence.rs      # FenceBarrier<T> (configurable chunk_size barrier)
│   ├── stream.rs     # run_ordered_collect helper
│   └── mod.rs
├── scope/            # Non-'static lifetime support
│   ├── pipeline_scope.rs # scope(), PipelineScope, ScopedPipe (work-stealing, 'env closures)
│   └── mod.rs
├── sync/             # Synchronization primitives
│   └── cancel.rs     # CancellationToken (Arc<AtomicBool>)
├── pool/             # Rayon-style work-stealing scheduler core
│   ├── registry.rs   # Registry, WorkerThread, find_work, steal
│   ├── sleep.rs      # AtomicCounters sleep/wake governance
│   ├── sleep_mask.rs # SleepMask: fixed inline [AtomicU64; N] sleeping bitmask
│   ├── latch.rs      # CoreLatch / SpinLatch / LockLatch / CountLatch
│   ├── job.rs        # JobRef (type-erased), StackJob, HeapJob
│   ├── join.rs       # fork-join
│   ├── unwind.rs     # AbortIfPanic, halt/resume_unwinding
│   └── mod.rs
└── ...
```

The miri/loom-transparent primitive layer (former `src/util/`) lives in its
own workspace crate, [`crates/youpipe-sys`](../crates/youpipe-sys):
`CachePadded<T>` plus the `Mutex`/`Condvar`/atomics/`thread_yield` shims that
switch backend by compilation context (parking_lot / miri-std / `--cfg loom`).
`pool/` and `handoff/` source their primitives from there.

---

### Adding a New Fused Stage

1. Define a stage struct implementing `StageMarker<T>` and `FusedStage<T>`
2. Add a builder method on `Pipe<S, I, O>` returning `Pipe<NewStage<S, ...>, I, NewO>`
3. In `FusedStage::apply()` (and `apply_pure` if the stage can't filter), compose `self.prev.apply(item)` with the new logic
