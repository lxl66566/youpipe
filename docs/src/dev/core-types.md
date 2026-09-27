# Core Types & Execution Paths


### `Workload` — Per-Item Cost Distribution Hint

```rust
pub enum Workload {
    Balanced,               // default; adaptive oversplit (1× for small batches, 4× for large)
    Unbalanced,             // 8× oversplit for finer-grained stealing of skewed tails
    Custom(NonZeroUsize),   // pin the oversplit factor manually
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
- `Custom(n)` — pin the oversplit factor regardless of batch size: full manual
  control for benchmarking or known-skew profiles outside the two presets
  (`1` = coarsest tree, `16` = very fine-grained stealing).

Do not confuse `Workload` (task-split **granularity**, same thread count) with
`Pipe::with_oversubscribe` (thread-count **multiplier** for blocking-IO sync
workloads) — see the `with_oversubscribe` doc for the factor guidance table.

**Scope.** Only the fused path (`pipe` / `scope` / `try_map`) consults this.
The streaming path (`stream(..)`) ignores it: streaming already load-balances
per-item skew through its MPMC channel + per-stage workers (a stalled worker
simply stops draining while peers keep consuming), and there is no fork/join
oversplit decision to tune. To control streaming tail latency, raise
`compute_workers` or pin a stage's `StageOptions::workers`.

### `Slots<T>` — Index-Based Zero-Copy Buffers

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

### `Pipe<S, I, O>` — Data-First Fused Pipeline

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

### `PipeRef<'a, S, T, O>` — Borrowed-Input Fused Pipeline

`pipe_ref(&data)` builds the borrowed counterpart of rayon's
`slice::par_iter()`: the input `&'a [T]` is read in place — never consumed,
materialized into a `Vec<&T>`, or freed inside the terminal — and items flow
through the chain as `&'a T` (closures destruct with `|&x|`).

```rust
pub fn pipe_ref<T: Sync>(items: &[T]) -> PipeRef<'_, Identity, T, &T>
//                                              element type   ^ current output = item type
```

| vs `pipe(items)`          | `pipe(items)`                | `pipe_ref(&items)` |
| ------------------------- | ---------------------------- | ------------------ |
| input                     | any `IntoIterator`           | `&[T]`             |
| item type in closures     | `T` (owned)                  | `&T` (borrowed)    |
| freed inside terminal     | input buffer (consumed)      | nothing            |
| element bound             | `T: Send` (items move)       | `T: Sync` (shared) |

Dispatch is shared with the owned core: `hybrid_dispatch` is generic over an
input handle (`IN = Slots<T>` owned, `IN = &'i [E]` borrowed — a reference *to*
the slice, so `'i` rides through `HybridStrategy`/`ChunkJob` and satisfies the
invariant `RangeOp<&'i E>` bound without transmutes). The borrowed leaves are
`par_index_*_leaf_by_ref`; panic cleanup is structurally halved — a borrowed
input is always init and never ours to drop, so only output-side guards remain
(`RefLeafGuard` / `TryRefLeafGuard`; `for_each` has no guard at all).

The `'a` borrow brands every closure, so closures may borrow additional
stack-local data without `scope` — the terminal blocks until every worker is
done (same soundness invariant as `ScopedPipe`). `TryPipeRef` is the fallible
counterpart (`E: 'static`, same erased-failure-slot caveat as
`ScopedTryPipe`).

### `RangePipe<S, O>` — Generated-Index Fused Pipeline

`pipe_range(0..n)` is the zero-materialization entry: the item at index `i`
IS the index, generated inside the leaves, so no input buffer ever exists
(`pipe(0..n)` pays a serial O(n) iota fill + buffer lifecycle on the calling
thread — 56–70 % of the whole owned call at 1 M/4 M; see
[benchmarks.md](benchmarks.md) "Input materialization"). Input type is fixed
to `usize`, which is also what lets the terminals dispatch to the generation
core statically (`S: FusedStage<usize, Output = O>`).

| vs `pipe(items)`            | `pipe(items)`             | `pipe_range(range)`          |
| --------------------------- | ------------------------- | ---------------------------- |
| input                       | any `IntoIterator`        | `Range<usize>`               |
| input buffer                | materialized `Vec`        | none (items generated)      |
| `filter` chains / `try_map` | native paths              | materialize at the terminal |
| `collect`/`for_each` (no filter) | `par_index_collect`  | `par_range_gen_collect`      |

Dispatch rides the same `hybrid_dispatch` with a third input handle — the
zero-sized `IN = ()` (the dispatcher only hands the input to strategy
leaves; the generation strategies ignore it). `RangeGenStrategy` /
`RangeGenSinkStrategy` are the `CollectStrategy` / `SinkStrategy` twins with
the input half elided: `par_range_gen_leaf`'s `GenLeafGuard` drops only the
partial output range on unwind (a generated item is never stored), and the
sink core has no guard at all. NT-store tiering is shared
(`nt_store_enabled::<R>` per whole-batch output size).

`RangePipe` duplicates `Pipe`'s builder surface by delegation-free field
moves (the #16 setter-macro consolidation subsumes it when that lands);
`try_map` transitions into a `TryPipe` over the materialized indices.

### `FusedStage` / `FusedTryStage` Traits — Zero-Dispatch Execution

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
`true`, it falls back to the range-tree merge path (`fused_filter_collect`).
The `apply_pure` fast path is what keeps the leaf vectorizable — it never
constructs an `Option`.

`FusedTryStage` is the fallible counterpart (returns
`Result<Option<Output>, Error>`): `TryMap` threads `Result` via `?`,
`InfallibleChain` adapts an infallible `FusedStage` chain to `FusedTryStage` at
the `.try_map()` boundary, and `MapErr` converts the error type. Driven by
`fused_try_filter_collect` when the chain filters (fallible + filtering can't
assume fixed cardinality), or the index-based fast path otherwise.

### `Pipe::collect()` / `TryPipe::try_collect()` — Execution

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
  vectorizable. Workload selects the oversplit factor per
[`Workload`](#workload--per-item-cost-distribution-hint). The hybrid
[`Workload`](#workload--per-item-cost-distribution-hint). On-pool callers
  (a worker of the *same* pool — `pool.submit` tasks, stream stage closures,
  nested `scope`/`run()`) take the same hybrid path for large batches
  (`chunk_splits > 0`): the dispatcher hands
  `ComputePool::on_this_pool_owner` to `CountLatch::with_count`, whose
  `Stealing` variant waits through the work-stealing `wait_until` loop
  (parking, if at all, via the sleep module's latch protocol) instead of a
  condvar the caller's own pool would have to service. Small batches keep
  the single-tree shortcut inside the dispatcher — P concurrent nested
  small batches would otherwise flood the global injector (+430 % measured;
  see the regime comment in `hybrid_dispatch`), while a single nested large
  batch wins −3.5 % (same-binary knob A/B). `YOUPIPE_ONPOOL_HYBRID=0`
  restores the always-tree behaviour.
  The dispatcher is generic only over the item type: the per-terminal
  strategy (`CollectStrategy` / `SinkStrategy` / `TryStrategy`) crosses a
  type-erased `ErasedStrategy` boundary (three fn pointers + a context
  pointer). This is deliberate — a per-strategy monomorphized dispatcher
  measurably regressed the untouched collect path via codegen-layout shifts
  (+16…30 % at 10k–100k, A/B-measured), while the erased dispatcher compiles
  once and its ~`num_threads` indirect calls per run are far off the hot
  path.
- **`MAY_FILTER == true`** — `fused_filter_collect` claims disjoint ranges of a
  shared `Slots` input, each leaf filters into a per-leaf `Vec`, results merged
  by `extend`. (Replaced the old `Vec::split_off` tree — one allocation +
  memcpy per internal node — measured −6…−9 % at 100 k.)

`.try_collect()` dispatches on `S::MAY_FILTER`:

- **`MAY_FILTER == false`** — the index-based fast path (`par_index_try_collect`),
  mirroring `collect()`'s zero-allocation strategy and hybrid flat/tree dispatch
  (via the `TryStrategy` impl of `HybridStrategy`) but with `RangeTryOp` /
  `FusedTryOp` wrappers that short-circuit on `Err`. Each leaf's `TryLeafGuard`
  cleans up partial output on both panic (unwind) and error (explicit) paths.
  The first `Err(e)` lands in the shared failure slot (first writer wins;
  a panic outranks it, mirroring the tree path's unwind-through-match
  semantics). Measured (criterion, 32-core): −48 % @ 10 k, −17 % @ 100 k vs
  the previous single-tree path.
- **`MAY_FILTER == true`** — `fused_try_filter_collect`, the fallible
  counterpart of `fused_filter_collect` (shared `Slots` + range tree, per-leaf
  `Vec` merge), short-circuiting on the first `Err` and honouring `Filter`.
  Replaced the last `Vec::split_off` tree here: measured −18 % at 10 k /
  −20 % at 100 k on `try_collect/youpipe_try_filter_owned` (5 interleaved
  rounds, 32-core).

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
`hybrid_dispatch` machinery as `collect` via the `SinkStrategy` impl of the
`HybridStrategy` trait — chunk-layout / inject / `CountLatch::wait_spin` /
panic-funnel code is written once and shared with `try_collect`'s
`TryStrategy` too (no vtable cost — see the `ErasedStrategy` note under
`collect`). On-pool callers take the same path via the `Stealing` latch (see
`collect` above).

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
pre-create the pool and use `.with_compute_pool(pool.clone())` instead. If
both are set, `with_compute_pool` takes precedence.

**Do not** use oversubscription for pure-CPU workloads — extra threads beyond
the core count only add context-switch overhead and cache thrashing (measured
10–30 % regression on CPU benchmarks).

### `StreamPipe` — Streaming Multi-Stage Pipeline

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
| `.stage_with(opts,f)` | same, with `opts.workers` / `opts.buffer` pinning that stage              |
| `.expand(f)`          | like `.stage` but each input → `Vec<N>` outputs (inherits parent's `seq`) |
| `.fence(mode)`        | dedicated forwarder thread batching between adjacent stages               |
| `.stage_async(f)`     | `io_concurrency` tokio tasks on the async runtime (M:N)                   |
| `.stage_async_with(o,f)` | same, with `opts.io_concurrency` / `opts.buffer` pinning that stage    |
| `.ordered()`          | feeder tags each item with `seq`; collector reorders via `ReorderBuffer`  |
| `.with_cancel(token)` | feeder/workers/bridges check `is_cancelled()` per iteration               |
| `.for_each(f)`        | side-effect terminal: drains item-by-item, no output `Vec` materialised   |

The stage chain is a typestate (`SyncStage<FenceLink<SyncStage<StreamStart,…>>>`)
walked by the `StageSpawn` trait — `spawn` recurses inside-out (older stages
first) so the data-flow direction matches. `stage_budget()` reports the number
of compute-pool stages plus any `StageOptions::workers` pins. `.run()` then
enforces the liveness invariant `feeder(≤1) + Σ stage workers ≤ pool_threads`:
the feeder reserves its slot first, explicit pins are granted upstream-first
(clamped to what remains, one slot held back per not-yet-spawned sync stage),
and the rest is divided equally across unpinned stages — every sync stage
keeps ≥ 1 resident worker, preventing the "stage 1 fills the pool → stage 2
starves → deadlock" failure mode. When even 1 worker per sync stage does not
fit, or `run()` executes on a worker of the same pool (nested pipelines park
that worker in the collector for the whole run), the run switches to
dedicated-thread mode: stage workers and the feeder are plain OS threads and
the pool is left untouched. Per-stage `buffer` / `io_concurrency` pins
replace the global `buffer_size` (and its `downstream_workers × 4` floor) /
`io_concurrency` for that stage only.

#### Async IO stages

`.stage_async()` is gated behind the `tokio-runtime` feature. It runs an IO
stage as **`io_concurrency` async tasks** on the [`AsyncRuntime`] backend
([`TokioPool`] — a `tokio::runtime::Handle` wrapper). The runtime's M:N
work-stealing scheduler multiplexes those tasks over `async_workers` OS
threads: each task yields its thread back to the runtime while it awaits (e.g.
`tokio::time::sleep`, real network/disk IO), so concurrency is bounded by
`io_concurrency` — **not** by the thread count.

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

Bridge threads survive only at **mode-conversion points** on the
`spawn_async_feeder` path (chains whose *first* stage is async), and each
conversion costs exactly one thread regardless of chain length:

- async → sync (`bridge_async_to_sync`, reached via `finalize_prev_rx`): every
  built-in stage overrides `spawn_async_feeder` to recurse through
  `prev.spawn_async_feeder(..)`, so the feeder's async channel stays async
  until the first stage that actually needs a sync receiver — `.stage_async(a)
  .stage(s).stage(t)` pays one bridge thread, not one per level. (The default
  trait impl — bridge up front, then plain `spawn` — previously stacked 3
  bridge threads + an async→sync→async round-trip on that shape.)
- sync → async (`bridge_final_rx_to_async`, the `FinalRx::Sync` arm): a
  trailing async stage fed by a sync stage, e.g. `..stage_async(f1).stage(f2)
  .stage_async(f3)`.

Keeping the blocking `send` off the tokio worker avoids the
"one thread is both async driver and blocking worker" anti-pattern: a
`SyncSender::send` inside a `tokio::spawn` task would park the runtime worker
under backpressure, stalling every other task on it (or deadlocking a
single-worker runtime — covered by the
`test_sync_to_async_does_not_stall_tokio_driver` regression test).

#### The feeder — inline, pool job, or dedicated thread

`feed_items` pushes the input into the first stage's channel. When
`items.len() ≤ buffer` it pushes inline from the calling thread (it can never
block on `Full`, so no deadlock). Beyond that, the push loop runs as **a job
on the compute pool** in pool mode — workers are long-lived, so the ~30-80 µs
spawn/join per run disappears while the effective thread count is unchanged
(one feeder alongside the stage workers); the feeder then **reserves one slot
in the pool's liveness budget** (see "Worker budget" below), so it is always
schedulable. In dedicated-thread mode the pool cannot be relied on at all, so
the feeder gets a **dedicated OS thread** instead. Both detached paths wrap
the loop in `catch_unwind` (an uncaught panic in a pool job aborts via the
worker's `AbortIfPanic`) and store the payload; `Feeder::finish` re-raises it
on the caller after the collect returns — the store precedes the sender drop
whose channel-close releases the collector, so the payload is always
observed.

A managed runtime may be attached via `.with_async_pool(...)` and reused across
runs; otherwise a transient runtime is built per call (simpler, but pays
~ms runtime construction each time — avoid inside tight loops).

### `ScopedPipe` — Non-`'static` Pipeline

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
