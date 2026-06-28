use std::{any::Any, marker::PhantomData, panic};

use super::{
    slots::Slots,
    traits::{
        Filter, FusedOp, FusedStage, FusedTryOp, FusedTryStage, Identity, InfallibleChain, MapErr,
        RangeOp, RangeTryOp, StageMarker, SyncMap, TryMap,
    },
};
use crate::{
    builder::config::{PipelineConfig, Workload},
    executor::compute::ComputePool,
};

type PanicPayload = Box<dyn Any + Send>;

/// Recursive index-based parallel fill. Each leaf claims a disjoint index range
/// `[start, end)` and writes outputs into `output[start..end)` by index — no
/// `split_off`, no `extend`, no per-level reallocation.
///
/// Panic safety: a panicking leaf catches the unwind, drops the partial state
/// of its own range (outputs written so far + unread inputs), and returns
/// `Err`. Internal nodes propagate the first `Err`, dropping the
/// already-completed sibling's output range. On return, the whole `[start,
/// end)` range is fully resolved: every output slot is either init (success
/// path) or dropped, and every input slot is consumed.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_rec<T, R, OP>(
    input: &Slots<T>,
    output: &Slots<R>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
) -> Result<(), PanicPayload>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)` exclusively
        // (par_index_rec splits never overlap). input[start..end) is fully
        // init, output[start..end) is fully uninit.
        let in_slice = unsafe { input.as_slice(start, end) };
        let out_slice = unsafe { output.as_mut_slice(start, end) };
        par_index_leaf(in_slice, out_slice, op);
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = ComputePool::global().join(
        || par_index_rec(input, output, start, mid, op, splits_left - 1),
        || par_index_rec(input, output, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(p), Ok(())) => {
            // SAFETY: right sibling completed without filter (RangeOp never
            // filters), so [mid, end) is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(p)
        }
        (Ok(()), Err(p)) => {
            unsafe { output.drop_range(start, mid) };
            Err(p)
        }
        (Err(p), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(p)
        }
    }
}

/// Process `[start, end)` sequentially on the current thread.
///
/// Panic safety uses a stack-local `LeafGuard` whose `Drop` runs only on
/// unwind. Compared to wrapping the loop in `panic::catch_unwind`, this lets
/// LLVM keep the loop index / written/consumed counters in registers when the
/// per-item op provably cannot panic (e.g. `|x| x + 1`): `catch_unwind`'s
/// `AssertUnwindSafe` forces the closure's `&mut i` capture to live in memory
/// for the whole loop, adding a stack spill+reload per iteration.
///
/// **Optimization note.** The leaf receives `&[T]` / `&mut [R]` *slice
/// references*, not the parent's `&Slots` cells. This is critical: with
/// `&Slots<u64>` for both input and output, LLVM cannot prove the two buffers
/// don't alias (both are `&` to the same opaque `UnsafeCell`-wrapped type), so
/// the auto-vectorizer bails out and we measure a ~2.6× regression on the 1 M
/// warm `par_map` path. Slice references carry Rust's noalias guarantees into
/// LLVM, which is what unlocks the same per-item throughput rayon's
/// `par_iter().collect()` achieves.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_leaf<T, R, OP>(input: &[T], output: &mut [R], op: &OP)
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    /// RAII guard that drops the partial slot state on unwind. `Drop` only
    /// fires if the loop panics; the success path calls `mem::forget`.
    ///
    /// `written` tracks the count of fully completed iterations (read +
    /// applied + written). At the panic point in `op.apply(item)` for iter
    /// `i = written`, item `i` has been moved into `op` (so `input[i+1..]` is
    /// still init and must be dropped) and `output[..i]` is init (must be
    /// dropped); `output[i..]` is uninit and item `i` is gone with the panic.
    /// `consumed` is therefore always `written + 1` at the panic point, so we
    /// don't track it separately — one less store per iteration on the hot
    /// path (helps the vectorizer keep the index in a register).
    struct LeafGuard<'a, T, R> {
        input: &'a [T],
        output: &'a mut [R],
        written: usize,
    }

    impl<T, R> Drop for LeafGuard<'_, T, R> {
        fn drop(&mut self) {
            // SAFETY: `written` reflects the actual completed-iteration count
            // at the unwind point. `RangeOp` never filters, so output[..written)
            // has no holes — every slot there is init and must be dropped.
            // input[written+1..] is still init (untouched), must be dropped.
            // Item `written` itself was moved into `op` and is gone with the
            // panic, so we don't drop input[written].
            unsafe {
                let i = self.written;
                let out_live = self.output.as_mut_ptr();
                for j in 0..i {
                    std::ptr::drop_in_place(out_live.add(j));
                }
                let in_live = self.input.as_ptr();
                for j in (i + 1)..self.input.len() {
                    std::ptr::drop_in_place(in_live.add(j).cast_mut());
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    // Capture raw pointers up front so the loop can mutate `g.written`
    // (which borrows `&mut g`) without re-borrowing `input` /
    // `output` (already borrowed by `g`).
    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafGuard {
        input,
        output,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input) / uninit (output).
        let item = unsafe { std::ptr::read(in_ptr.add(i)) };
        let out = op.apply(item);
        unsafe { std::ptr::write(out_ptr.add(i), out) };
        g.written = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
}

/// Drive `par_index_rec` over `[0, n)` and convert the output buffer into a
/// `Vec<R>`. Propagates panics after dropping all partial state.
///
/// # Panics
///
/// Propagates any panic raised by `op`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_collect<T, R, OP>(items: Vec<T>, op: &OP, splits: usize) -> Vec<R>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let input = Slots::from_vec(items);
    let output = Slots::<R>::uninit(n);
    let result = par_index_rec(&input, &output, 0, n, op, splits);
    match result {
        Ok(()) => {
            // Input fully consumed (all uninit): dropping the box just frees
            // memory. Output fully init: transmute into the result Vec.
            drop(input);
            output.into_vec()
        }
        Err(p) => {
            // Recursion already dropped every live slot; freeing buffers is safe.
            drop(input);
            drop(output);
            panic::resume_unwind(p);
        }
    }
}

// ── Index-based fast path for fallible (`try_collect`) pipelines ──
//
// When `FusedTryStage::MAY_FILTER == false`, output cardinality equals input
// cardinality (every item either succeeds or aborts the whole pipeline with an
// error). This lets us pre-allocate the output `Slots<R>` and write results at
// known indices — the same zero-allocation strategy `par_index_collect` uses
// for infallible pipelines. The `Vec`-merge path (`join_fused_try_collect`)
// remains the fallback for chains containing `Filter`.

/// Recursive divide-and-conquer for fallible stages. Returns `Err(e)` on the
/// first error; on error, all init output slots in the error branch are
/// cleaned up by the leaf, and sibling ranges are dropped by this function.
///
/// Panics propagate naturally through `join`'s `halt_unwinding`/`resume_unwind`
/// (re-raised past the match). The leaf's `TryLeafGuard` handles panic cleanup
/// of the leaf's own partial range, identical to `LeafGuard` in
/// `par_index_leaf`.
fn par_index_try_rec<T, R, E, OP>(
    input: &Slots<T>,
    output: &Slots<R>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
) -> Result<(), E>
where
    T: Send,
    R: Send,
    E: Send,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: disjoint range — this leaf owns `[start, end)` exclusively.
        let in_slice = unsafe { input.as_slice(start, end) };
        let out_slice = unsafe { output.as_mut_slice(start, end) };
        par_index_try_leaf(in_slice, out_slice, op)?;
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = ComputePool::global().join(
        || par_index_try_rec(input, output, start, mid, op, splits_left - 1),
        || par_index_try_rec(input, output, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) => {
            // SAFETY: right sibling completed without filter (RangeTryOp never
            // filters), so [mid, end) is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(e)
        }
        (Ok(()), Err(e)) => {
            unsafe { output.drop_range(start, mid) };
            Err(e)
        }
        (Err(e), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(e)
        }
    }
}

/// Process `[start, end)` sequentially, short-circuiting on the first `Err`.
///
/// On error: drops `output[..written]` (init from prior iterations) and
/// `input[written+1..]` (still init — untouched), then returns `Err`. Item
/// `written` was consumed by `try_apply` and is gone.
///
/// A `TryLeafGuard` runs the same cleanup on **panic** (unwind), disarmed by
/// `mem::forget` on both the `Ok` and `Err` return paths — identical structure
/// to `LeafGuard` in `par_index_leaf`.
fn par_index_try_leaf<T, R, E, OP>(input: &[T], output: &mut [R], op: &OP) -> Result<(), E>
where
    T: Send,
    R: Send,
    E: Send,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    /// RAII guard mirroring `LeafGuard`: drops the partial slot state on
    /// unwind. `Drop` only fires on panic; both success and error paths call
    /// `mem::forget`.
    struct TryLeafGuard<'a, T, R> {
        input: &'a [T],
        output: &'a mut [R],
        written: usize,
    }

    impl<T, R> Drop for TryLeafGuard<'_, T, R> {
        fn drop(&mut self) {
            // SAFETY: same reasoning as `LeafGuard::drop` — `written` reflects
            // completed iterations at the unwind point.
            unsafe {
                let i = self.written;
                let out_live = self.output.as_mut_ptr();
                for j in 0..i {
                    std::ptr::drop_in_place(out_live.add(j));
                }
                let in_live = self.input.as_ptr();
                for j in (i + 1)..self.input.len() {
                    std::ptr::drop_in_place(in_live.add(j).cast_mut());
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = TryLeafGuard {
        input,
        output,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input) / uninit (output).
        let item = unsafe { std::ptr::read(in_ptr.add(i)) };
        match op.try_apply(item) {
            Ok(out) => {
                unsafe { std::ptr::write(out_ptr.add(i), out) };
                g.written = i + 1;
            }
            Err(e) => {
                // Error path: run the same cleanup the guard would do on
                // panic, then disarm (forget) so Drop doesn't double-clean.
                // Item `i` was consumed by `try_apply` and is gone.
                unsafe {
                    for j in 0..i {
                        std::ptr::drop_in_place(out_ptr.add(j));
                    }
                    for j in (i + 1)..n {
                        std::ptr::drop_in_place(in_ptr.add(j).cast_mut());
                    }
                }
                std::mem::forget(g);
                return Err(e);
            }
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(())
}

/// Drive `par_index_try_rec` over `[0, n)` and convert the output buffer into
/// a `Vec<R>`. On error, the recursion has already dropped all init output
/// slots; on panic, the panic propagates (and the output buffer's init slots
/// may leak, same as `par_index_collect`).
fn par_index_try_collect<T, R, E, OP>(items: Vec<T>, op: &OP, splits: usize) -> Result<Vec<R>, E>
where
    T: Send,
    R: Send,
    E: Send,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let input = Slots::from_vec(items);
    let output = Slots::<R>::uninit(n);
    let result = par_index_try_rec(&input, &output, 0, n, op, splits);
    match result {
        Ok(()) => {
            drop(input);
            Ok(output.into_vec())
        }
        Err(e) => {
            // Recursion already dropped every live output slot.
            drop(input);
            drop(output);
            Err(e)
        }
    }
}

//
// Hypothesis (from hotpath): ~60% of stolen `join` B-jobs force the origin
// worker into `wait_until_cold`, so a *flat* dispatcher — N disjoint leaf-jobs
// injected at once into the pool's global queue, each writing its own output
// range, synchronized by one `CountLatch` — should win by eliminating the
// join-wait entirely.
//
// Result (A/B vs this tree, 32-core): genuinely faster at small/medium N, but
// regresses at large N — a net wash, so the code was reverted:
//
//   sync_cpu_heavy    10 k: −6.8 %      100 k:  ~0 % (noise)
//   sync_lightweight  10 k: −15 %       100 k:  −8 %      1 M: +14 %  ←
// regression
//
// Why it helps small/medium: no fork/join tree ⇒ no "run A inline then wait for
// the stolen B" idle-search; the ~120 µs fixed dispatch overhead shrinks.
//
// Why it regresses at large N: all N leaf-jobs funnel through the *single*
// global injector queue (`concurrent_queue`), and 32 workers contending on one
// MPMC queue for 128+ pops is slower than the tree's distributed model (each
// worker pushes to its own LIFO deque, peers steal — far less coherence traffic
// on a single cache line). The bottleneck is fundamental, not tunable away.
//
// It also has a panic-semantics snag: a panicking flat job propagates into the
// worker's `AbortIfPanic` (process abort) instead of the tree's
// `halt_unwinding`/`resume_unwind` propagation, so panic-safe flat dispatch
// needs extra plumbing (a Drop-guard that always decrements the latch + a
// shared panic slot + per-chunk success flags to drop siblings on unwind).
//
// Conclusion: flat dispatch is a small/medium-N win but a large-N loss. The
// promising unattempted direction is a *hybrid*: inject `num_threads` broad
// top-level chunks (low injector contention, no ramp-up) and let each chunk
// recurse via the tree (distributed deques + stealing). That needs the same
// scoped-latch plumbing as full flat, so it is left for a follow-up.

// ── Join-based parallel helpers ──

/// Items-per-worker below which a batch is routed to the calling thread
/// instead of the work-stealing pool.
///
/// The fork/join path pays a **per-call** fixed cost (external-thread job
/// injection, latch handoff, worker spin/park rounds, condvar wakeups) that is
/// independent of `n`. For small `n` that fixed cost dwarfs the per-item
/// compute, and a plain sequential loop is faster — including faster than the
/// pathological case where the scheduler's park/wake heuristic thrashes.
///
/// See `docs/PERF_NOTES.md` (#1): on a 32-core box with `cpu_heavy` work (100
/// iterations/item), the measured serial↔parallel crossover is ~3 k items
/// (1 k: parallel 137 µs vs serial 46 µs; 10 k: parallel 152 µs vs serial
/// ~460 µs). `64` items/thread lands at ~2 k on 32 cores — below the
/// crossover, so the 1 k regression is recovered while 100 k–1 M batches
/// (50–500× above the threshold) are completely unaffected.
///
/// This is a heuristic, not a cliff: it only redirects *small* batches to the
/// cheap serial path. It cannot starve the pool because sub-threshold batches
/// submit zero jobs, and it is monotonic in the per-item cost (a more
/// expensive item lowers the real crossover, so serializing even fewer
/// batches would be safe — the constant is deliberately conservative).
const SERIAL_ITEMS_PER_THREAD: usize = 64;

/// Whether a batch of `n` items should run sequentially on the calling thread
/// instead of being split across the pool. `num_threads` is read once by the
/// caller and passed in to avoid a second `ComputePool::global()` TLS hit.
///
/// The serial↔parallel crossover is `parallel_fixed_overhead / per_item_cost`.
/// We don't know `per_item_cost`, so the `Workload` hint selects a per-thread
/// item count that the batch must exceed before we pay the pool's ~120 µs
/// fixed dispatch cost:
///
/// - `Balanced`: uniform items, low average cost → high threshold
///   (`SERIAL_ITEMS_PER_THREAD`, ~2 k on 32 cores). Measured cpu_heavy
///   crossover is ~3 k; below it the pool is slower than a plain loop.
/// - `Unbalanced`: carries an expensive tail, so the average per-item cost is
///   higher and the crossover lands at a smaller `n` (measured on a 1000×-
///   spread skewed workload: crossover ~450). Serialize only truly tiny batches
///   (`/8`). The expensive tail makes *over*-serializing catastrophic — a 1 k
///   skewed batch is ~7× slower serially than in parallel — so the Unbalanced
///   multiplier stays small.
fn prefers_serial(n: usize, num_threads: usize, workload: Workload) -> bool {
    if n <= 1 || num_threads <= 1 {
        return true;
    }
    let per_thread = match workload {
        Workload::Balanced => SERIAL_ITEMS_PER_THREAD,
        Workload::Unbalanced => SERIAL_ITEMS_PER_THREAD / 8,
    };
    n <= num_threads * per_thread
}

/// Compute the number of recursive split levels. Aiming at ~`oversplit` tasks
/// per thread gives good work-stealing without excessive task overhead.
fn split_depth(n: usize, num_threads: usize, oversplit: usize) -> usize {
    let desired_tasks = (num_threads * oversplit).max(1);
    let by_threads = desired_tasks.next_power_of_two().trailing_zeros() as usize;
    let by_len = n.max(1).next_power_of_two().trailing_zeros() as usize;
    by_threads.min(by_len).max(1)
}

/// Recursive merge-based collect for fused stages that may filter. Used only as
/// the `MAY_FILTER == true` fallback; output cardinality is unknown up front so
/// each leaf produces its own `Vec` and results are concatenated.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn join_fused_collect<S, T>(mut items: Vec<T>, stages: &S, splits_left: usize) -> Vec<S::Output>
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
{
    if splits_left == 0 || items.len() <= 1 {
        return items
            .into_iter()
            .filter_map(|item| stages.apply(item))
            .collect();
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_fused_collect(items, stages, splits_left - 1),
        || join_fused_collect(right, stages, splits_left - 1),
    );
    let mut result = left_r;
    result.extend(right_r);
    result
}

/// Recursive merge-based collect for fallible fused stages. Short-circuits on
/// the first `Err`; on success honours `Filter` (drops `None` items).
///
/// Uses the `Vec`-merge path rather than the index-based fast path because
/// fallible + filter pipelines cannot assume fixed output cardinality. The
/// infallible/no-filter fast path is reserved for `Pipe::collect`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn join_fused_try_collect<S, T, E>(
    mut items: Vec<T>,
    stages: &S,
    splits_left: usize,
) -> Result<Vec<S::Output>, E>
where
    S: FusedTryStage<T, Error = E> + Sync,
    T: Send,
    S::Output: Send,
    E: Send,
{
    if splits_left == 0 || items.len() <= 1 {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if let Some(o) = stages.try_apply(item)? {
                out.push(o);
            }
        }
        return Ok(out);
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_fused_try_collect(items, stages, splits_left - 1),
        || join_fused_try_collect(right, stages, splits_left - 1),
    );
    match (left_r, right_r) {
        (Ok(mut l), Ok(r)) => {
            l.extend(r);
            Ok(l)
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

// ── Pipe (data-first fused pipeline) ──

/// Data-first entry point. Builds a fused pipeline that consumes `items` when
/// `.collect()` is called.
///
/// ```rust
/// # use youpipe::pipe;
/// let result: Vec<i32> = pipe(0..1000)
///     .map(|x: i32| x + 1)
///     .filter(|x: &i32| x % 2 == 0)
///     .map(|x: i32| x * 10)
///     .collect();
/// ```
pub fn pipe<I, It>(items: It) -> Pipe<Identity, I, I>
where
    It: IntoIterator<Item = I>,
    I: Send + 'static,
{
    Pipe {
        items: items.into_iter().collect(),
        stages: Identity,
        config: PipelineConfig::default(),
        _marker: PhantomData,
    }
}

/// A type-state, data-first fused pipeline. Stages chained via `.map()` /
/// `.filter()` are compiled into a single closure per worker — zero
/// intermediate allocations when no `filter` is present.
///
/// Three type parameters:
/// - `S` — the stage chain (nested `SyncMap` / `Filter` / `Identity`).
/// - `I` — the pipeline **input** type (fixed by `pipe()`).
/// - `O` — the **current output** type (the input to the next stage).
///
/// Separating `I` and `O` is what lets type-changing maps like
/// `.map(i32 -> String)` then `.map(String -> usize)` type-check end to end.
pub struct Pipe<S = Identity, I = (), O = ()> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<O>,
}

impl<S, I, O> Pipe<S, I, O> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Tune the workload split factor. Default is [`Workload::Balanced`].
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.config.workload = workload;
        self
    }

    /// Append a synchronous map stage: `Fn(O) -> N`.
    ///
    /// The output type changes to `N`; the pipeline input `I` is unchanged.
    /// Type-changing maps (e.g. `i32 -> String`) are supported because `I` and
    /// `O` are tracked as separate type parameters.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> Pipe<SyncMap<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N>
    where
        S: StageMarker<I, Output = O>,
        O: Send + 'static,
        N: Send + 'static,
    {
        Pipe {
            items: self.items,
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage. Keeps items where `f` returns `true`.
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Send + Sync + 'static,
    ) -> Pipe<Filter<S, impl Fn(&O) -> bool + Send + Sync + 'static>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        Pipe {
            items: self.items,
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a fallible map stage: `Fn(O) -> Result<N, E>`. Transitions the
    /// pipeline into a [`TryPipe`] whose `.try_collect()` returns
    /// `Result<Vec<N>, E>`. The first `Err` short-circuits the chain.
    ///
    /// `Filter` is honoured even after a `try_map` boundary — items dropped by
    /// an upstream filter are simply not passed to `f`.
    #[allow(clippy::type_complexity)] // the return type encodes the typestate
    // chain (`InfallibleChain` wraps the infallible prefix so it impls
    // `FusedTryStage<Error = E>`); there is no shorter spelling that preserves
    // the compile-time-fusion guarantee.
    pub fn try_map<N, E>(
        self,
        f: impl Fn(O) -> Result<N, E> + Send + Sync + 'static,
    ) -> TryPipe<
        TryMap<InfallibleChain<S, E>, impl Fn(O) -> Result<N, E> + Send + Sync + 'static>,
        I,
        N,
        E,
    >
    where
        S: StageMarker<I, Output = O>,
        O: Send + 'static,
        N: Send + 'static,
        E: Send + 'static,
    {
        TryPipe {
            items: self.items,
            stages: TryMap {
                prev: InfallibleChain(self.stages, PhantomData),
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

impl<S, I, O> Pipe<S, I, O>
where
    S: FusedStage<I, Output = O> + Send + Sync + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    /// Execute the fused pipeline and collect results.
    ///
    /// Uses the index-based range core (pre-allocated output, no per-level
    /// `split_off`/`extend`) when the stage chain cannot filter
    /// (`S::MAY_FILTER == false`), and falls back to the recursive merge path
    /// otherwise (filters change output cardinality, so fixed-index writes are
    /// not possible).
    ///
    /// Small batches (≤ `num_workers * SERIAL_ITEMS_PER_THREAD`) run
    /// sequentially on the calling thread — see [`prefers_serial`] and
    /// `docs/PERF_NOTES.md` (#1).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn collect(self) -> Vec<O> {
        let items = self.items;
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let num_threads = ComputePool::global().num_workers();
        if prefers_serial(n, num_threads, self.config.workload) {
            // Dispatch on `MAY_FILTER`: skip the `Option` wrapping on the
            // pure path so the serial fallback matches a hand-written
            // `iter().map().collect()`. With `black_box(cpu_work(x))` at 1 k
            // items this trims ~5 µs of `Option`-discriminant + branch
            // overhead (the `Identity::apply(item).map(f)` wrapper LLVM cannot
            // always elide through the trait call), narrowing the gap to the
            // raw sequential baseline.
            if S::MAY_FILTER {
                return items
                    .into_iter()
                    .filter_map(|item| self.stages.apply(item))
                    .collect();
            }
            return items
                .into_iter()
                .map(|item| self.stages.apply_pure(item))
                .collect();
        }

        // `oversplit` = tasks-per-worker for the fork/join tree. A/B-tuned
        // (2026-06, 32-core): Balanced `1` regressed cpu_heavy ~+18 % (too few
        // leaves ⇒ poor load balancing, longer tail), `8` regressed ~+5.5 %
        // (too many nodes ⇒ per-node dispatch overhead). `4` (128 leaves on 32
        // cores) is the sweet spot. Unbalanced keeps `8` for the stealing slack
        // its expensive tail needs.
        let oversplit = match self.config.workload {
            Workload::Balanced => 4,
            Workload::Unbalanced => 8,
        };
        let splits = split_depth(n, num_threads, oversplit);

        if S::MAY_FILTER {
            join_fused_collect(items, &self.stages, splits)
        } else {
            let op = FusedOp(self.stages);
            par_index_collect(items, &op, splits)
        }
    }
}

// ── TryPipe (fallible fused pipeline) ──

/// A data-first fused pipeline whose stages may fail. Obtained from
/// [`Pipe::try_map`]; call `.try_collect()` to execute and get a `Result`.
///
/// The error type `E` is fixed across the chain — every subsequent `try_map`
/// must produce the same `E` (use `.map_err()` to convert). `map` and `filter`
/// are also supported: their effects compose with `Result` via `?`.
pub struct TryPipe<S = Identity, I = (), O = (), E = std::convert::Infallible> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<(O, E)>,
}

impl<S, I, O, E> TryPipe<S, I, O, E> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Tune the workload split factor. Default is [`Workload::Balanced`].
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.config.workload = workload;
        self
    }

    /// Append an infallible map stage. The error type `E` is unchanged.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> TryPipe<SyncMap<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N, E>
    where
        S: StageMarker<I, Output = O>,
        O: Send + 'static,
        N: Send + 'static,
    {
        TryPipe {
            items: self.items,
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage. Items where `f` returns `false` are dropped from
    /// the output (no error is signalled).
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Send + Sync + 'static,
    ) -> TryPipe<Filter<S, impl Fn(&O) -> bool + Send + Sync + 'static>, I, O, E>
    where
        S: StageMarker<I, Output = O>,
    {
        TryPipe {
            items: self.items,
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append another fallible map stage. The closure must produce the same
    /// error type `E` (use `.map_err()` upstream if a different `E2` is
    /// needed).
    #[allow(clippy::type_complexity)] // typestate chain return — see `Pipe::try_map`.
    pub fn try_map<N>(
        self,
        f: impl Fn(O) -> Result<N, E> + Send + Sync + 'static,
    ) -> TryPipe<TryMap<S, impl Fn(O) -> Result<N, E> + Send + Sync + 'static>, I, N, E>
    where
        S: StageMarker<I, Output = O> + FusedTryStage<I, Error = E>,
        O: Send + 'static,
        N: Send + 'static,
    {
        TryPipe {
            items: self.items,
            stages: TryMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Convert the error type from `E` to `E2`. Useful when chaining multiple
    /// `try_map` calls whose closures return different error types.
    pub fn map_err<E2>(
        self,
        f: impl Fn(E) -> E2 + Send + Sync + 'static,
    ) -> TryPipe<MapErr<S, impl Fn(E) -> E2 + Send + Sync + 'static>, I, O, E2>
    where
        E: Send + 'static,
        E2: Send + 'static,
    {
        TryPipe {
            items: self.items,
            stages: MapErr {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

impl<S, I, O, E> TryPipe<S, I, O, E>
where
    S: FusedTryStage<I, Output = O, Error = E> + Send + Sync + 'static,
    I: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    /// Execute the fused fallible pipeline, short-circuiting on the first
    /// error. `Filter` stages drop items from the success output.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn try_collect(self) -> Result<Vec<O>, E> {
        let items = self.items;
        let n = items.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let num_threads = ComputePool::global().num_workers();
        if prefers_serial(n, num_threads, self.config.workload) {
            let mut out = Vec::with_capacity(n);
            for item in items {
                if let Some(o) = self.stages.try_apply(item)? {
                    out.push(o);
                }
            }
            return Ok(out);
        }

        let oversplit = match self.config.workload {
            Workload::Balanced => 4,
            Workload::Unbalanced => 8,
        };
        let splits = split_depth(n, num_threads, oversplit);
        if S::MAY_FILTER {
            join_fused_try_collect(items, &self.stages, splits)
        } else {
            // Fast path: no filter → output cardinality == input cardinality.
            // Pre-allocate the output buffer and write at known indices,
            // avoiding the per-split `Vec::split_off` allocations of the
            // merge path.
            let op = FusedTryOp(self.stages);
            par_index_try_collect(items, &op, splits)
        }
    }
}

// ── pub(crate) scoped entry point ──

/// `pub(crate)` entry point for scoped pipelines. Identical dispatch logic to
/// `Pipe::collect` but without `'static` bounds — driven by
/// `crate::scope::ScopedPipeline`, whose closure/stage lifetime is `'env`
/// (the surrounding `scope` block).
///
/// Soundness rests on the same `ComputePool::join` invariant that rayon-style
/// scoped parallelism relies on: the calling thread blocks inside
/// `Registry::in_worker_cold` until every recursively spawned sub-task
/// finishes, so every `'env` reference captured by `stages` outlives the
/// pool's access to them.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn fused_collect_scoped<S, T>(
    items: Vec<T>,
    stages: S,
    workload: Workload,
) -> Vec<S::Output>
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
{
    let n = items.len();
    if n == 0 {
        return Vec::new();
    }
    let num_threads = ComputePool::global().num_workers();
    if prefers_serial(n, num_threads, workload) {
        if S::MAY_FILTER {
            return items
                .into_iter()
                .filter_map(|item| stages.apply(item))
                .collect();
        }
        return items
            .into_iter()
            .map(|item| stages.apply_pure(item))
            .collect();
    }
    let oversplit = match workload {
        Workload::Balanced => 4,
        Workload::Unbalanced => 8,
    };
    let splits = split_depth(n, num_threads, oversplit);
    if S::MAY_FILTER {
        join_fused_collect(items, &stages, splits)
    } else {
        let op = FusedOp(stages);
        par_index_collect(items, &op, splits)
    }
}
