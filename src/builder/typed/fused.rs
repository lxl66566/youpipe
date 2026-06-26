use std::{any::Any, marker::PhantomData, panic};

use super::{
    slots::Slots,
    traits::{
        Fence, Filter, FnMap, FusedOp, FusedStage, Identity, Ordered, RangeOp, StageMarker, SyncMap,
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
fn par_index_leaf<T, R, OP>(input: &[T], output: &mut [R], op: &OP)
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    /// RAII guard that drops the partial slot state on unwind. `Drop` only
    /// fires if the loop panics; the success path calls `mem::forget`.
    ///
    /// `consumed` = count of input items already moved out (logically uninit).
    /// `written`  = count of output items already initialized.
    /// At the panic point in `op.apply(item)`: `consumed == i + 1` (item `i`
    /// moved into `op`), `written == i` (output[i] still uninit).
    struct LeafGuard<'a, T, R> {
        input: &'a [T],
        output: &'a mut [R],
        consumed: usize,
        written: usize,
    }

    impl<T, R> Drop for LeafGuard<'_, T, R> {
        fn drop(&mut self) {
            // SAFETY: `consumed`/`written` reflect the actual init state at
            // the unwind point. `RangeOp` never filters, so output[..written)
            // has no holes — every slot there is init and must be dropped.
            // input[consumed..] is still init (untouched), must be dropped.
            // We use `ptr::read` to consume each live slot exactly once.
            unsafe {
                let out_live = self.output.as_mut_ptr();
                for i in 0..self.written {
                    std::ptr::drop_in_place(out_live.add(i));
                }
                let in_live = self.input.as_ptr();
                for i in self.consumed..self.input.len() {
                    std::ptr::drop_in_place(in_live.add(i).cast_mut());
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    // Capture raw pointers up front so the loop can mutate `g.written` /
    // `g.consumed` (which borrow `&mut g`) without re-borrowing `input` /
    // `output` (already borrowed by `g`).
    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafGuard {
        input,
        output,
        consumed: 0,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input) / uninit (output).
        let item = unsafe { std::ptr::read(in_ptr.add(i)) };
        g.consumed = i + 1;
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

// ── Join-based parallel helpers ──

/// Compute the number of recursive split levels. Aiming for ~`oversplit` tasks
/// per thread gives good work-stealing without excessive task overhead.
fn split_depth(n: usize, num_threads: usize, oversplit: usize) -> usize {
    let desired_tasks = (num_threads * oversplit).max(1);
    let by_threads = desired_tasks.next_power_of_two().trailing_zeros() as usize;
    let by_len = n.max(1).next_power_of_two().trailing_zeros() as usize;
    by_threads.min(by_len).max(1)
}

/// Fallible recursive join-based map. Returns the first error encountered.
fn join_try_map<T, R, E, F>(mut items: Vec<T>, f: &F, splits_left: usize) -> Result<Vec<R>, E>
where
    T: Send,
    R: Send,
    E: Send,
    F: Fn(T) -> Result<R, E> + Sync,
{
    if splits_left == 0 || items.len() <= 1 {
        return items.into_iter().map(f).collect();
    }
    let mid = items.len() / 2;
    let right = items.split_off(mid);
    let (left_r, right_r) = ComputePool::global().join(
        || join_try_map(items, f, splits_left - 1),
        || join_try_map(right, f, splits_left - 1),
    );
    match (left_r, right_r) {
        (Ok(mut l), Ok(r)) => {
            l.extend(r);
            Ok(l)
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// Parallel map over an iterator. Uses [`Workload::Balanced`] by default.
///
/// Internally uses recursive `join`-based splitting (like rayon) which enables
/// work-stealing of sub-tasks. This handles both balanced and skewed workloads
/// well without per-item atomics.
pub fn par_map<I, F, R>(iter: I, f: F) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    par_map_with_workload(iter, f, Workload::Balanced)
}

/// Parallel map with explicit [`Workload`] hint.
///
/// Uses the index-based range core (pre-allocated output, no `split_off` /
/// `extend`) driven by recursive `join` splitting. `Unbalanced` creates more
/// split points (finer task granularity) so that slow items spread across more
/// leaves and can be stolen by idle workers.
pub fn par_map_with_workload<I, F, R>(iter: I, f: F, workload: Workload) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 {
        return Vec::new();
    }
    let num_threads = ComputePool::global().num_workers();
    if n <= 1 || num_threads <= 1 {
        return items.into_iter().map(f).collect();
    }

    // Oversplit: more leaves than threads so work-stealing can balance skewed
    // loads. Unbalanced uses a higher factor for finer granularity.
    let oversplit = match workload {
        Workload::Balanced => 4,
        Workload::Unbalanced => 8,
    };
    let splits = split_depth(n, num_threads, oversplit);

    par_index_collect(items, &FnMap(f), splits)
}

/// Parallel chunked map — splits items into `chunk_size` slices and calls `f`
/// on each slice, collecting all results.
pub fn par_chunks_map<I, F, R>(iter: I, chunk_size: usize, f: F) -> Vec<R>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(&[I::Item]) -> Vec<R> + Send + Sync + 'static,
    R: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 || chunk_size == 0 {
        return Vec::new();
    }

    let num_threads = ComputePool::global().num_workers();
    let num_chunks = n.div_ceil(chunk_size);
    if num_chunks <= 1 || num_threads <= 1 {
        return items.chunks(chunk_size).flat_map(&f).collect();
    }

    let splits = split_depth(num_chunks, num_threads, 4);
    join_chunks_map(items, chunk_size, &f, splits)
}

/// Recursive join-based chunked map.
fn join_chunks_map<T, R, F>(
    mut items: Vec<T>,
    chunk_size: usize,
    f: &F,
    splits_left: usize,
) -> Vec<R>
where
    T: Send,
    R: Send,
    F: Fn(&[T]) -> Vec<R> + Sync,
{
    if splits_left == 0 || items.len() <= chunk_size {
        return items.chunks(chunk_size).flat_map(f).collect();
    }
    // Split at a chunk boundary.
    let mid = ((items.len() / 2) / chunk_size) * chunk_size;
    let mid = mid.max(chunk_size);
    let right = items.split_off(mid);

    let (left_r, right_r) = ComputePool::global().join(
        || join_chunks_map(items, chunk_size, f, splits_left - 1),
        || join_chunks_map(right, chunk_size, f, splits_left - 1),
    );
    let mut result = left_r;
    result.extend(right_r);
    result
}

/// Fallible parallel map. Both branches always execute (join guarantee); the
/// first error is returned.
pub fn try_par_map<I, F, R, E>(iter: I, f: F) -> Result<Vec<R>, E>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> Result<R, E> + Send + Sync + 'static,
    R: Send + 'static,
    E: Send + 'static,
{
    let items: Vec<I::Item> = iter.into_iter().collect();
    let n = items.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let num_threads = ComputePool::global().num_workers();
    if n <= 1 || num_threads <= 1 {
        return items.into_iter().map(f).collect();
    }

    let splits = split_depth(n, num_threads, 4);
    join_try_map(items, &f, splits)
}

// ── Pipeline (main user-facing type) ──

/// A type-state pipeline builder. Stages are fused at compile time into a
/// single pass over the data when possible (no `fence` / `ordered` boundaries).
///
/// Two type parameters carry the live element type through the chain:
/// - `I` — the pipeline **input** type (fixed by the first `.map` closure).
/// - `O` — the **current output** type (the input to the *next* stage).
///
/// Separating them (previously a single `T` overloaded both roles) is what
/// lets a type-changing `.map(i32 -> String)` compile: the input type `I`
/// stays `i32` while `O` tracks the latest transform's output.
///
/// Use [`Pipeline::new`] to start, chain `.map()` / `.filter()` calls,
/// then call `.collect(items)` to execute.
pub struct Pipeline<S = Identity, I = (), O = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<(I, O)>,
}

impl<T: Send + 'static> Pipeline<Identity, T, T> {
    /// Create a new pipeline (type-state entry point).
    ///
    /// `T` is inferred from the first staged method (e.g. `.map(|x: i32|
    /// ...)`), so callers do not need to spell it out — the previous
    /// `from_vec(vec![])` entry point existed only as a type hint and
    /// silently discarded its argument, which was both wasteful and
    /// confusing.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: Identity,
            config: PipelineConfig::default(),
            _marker: PhantomData,
        }
    }
}

impl<T: Send + 'static> Default for Pipeline<Identity, T, T> {
    /// `Pipeline: Default` lets downstream code write
    /// `Pipeline::<T>::default()` or rely on type inference from the first
    /// `.map` / `.filter` call.
    fn default() -> Self {
        Self::new()
    }
}

impl<S, I, O> Pipeline<S, I, O> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Append a synchronous map stage: `Fn(O) -> N`.
    ///
    /// The output type changes to `N`; the pipeline input `I` is unchanged.
    /// Type-changing maps (e.g. `i32 -> String`) are supported because `I` and
    /// `O` are tracked as separate type parameters.
    pub fn map<N: Send + 'static>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> Pipeline<SyncMap<S, impl Fn(O) -> N + Send + Sync + 'static>, I, N>
    where
        S: StageMarker<I, Output = O>,
        O: Send + 'static,
    {
        Pipeline {
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
    ) -> Pipeline<Filter<S, impl Fn(&O) -> bool + Send + Sync + 'static>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        Pipeline {
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a fence (materialization barrier).
    pub fn fence(self) -> Pipeline<Fence<S>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        Pipeline {
            stages: Fence {
                prev: self.stages,
                chunk_size: None,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a chunked fence with the given chunk size.
    pub fn fence_chunked(self, chunk_size: usize) -> Pipeline<Fence<S>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        Pipeline {
            stages: Fence {
                prev: self.stages,
                chunk_size: Some(chunk_size),
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Mark the output as order-preserving.
    pub fn ordered(self) -> Pipeline<Ordered<S>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        Pipeline {
            stages: Ordered { prev: self.stages },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

// ── Collect for fully-fused sync pipelines ──

impl<S, I, O> Pipeline<S, I, O>
where
    S: FusedStage<I, Output = O> + Send + Sync + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    /// Execute the fused pipeline over `items` and collect results.
    ///
    /// Uses the index-based range core (pre-allocated output, no per-level
    /// `split_off`/`extend`) when the stage chain cannot filter
    /// (`S::MAY_FILTER == false`), and falls back to the recursive merge path
    /// otherwise (filters change output cardinality, so fixed-index writes are
    /// not possible).
    pub fn collect<It: IntoIterator<Item = I>>(self, items: It) -> Vec<O> {
        let items: Vec<I> = items.into_iter().collect();
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let num_threads = ComputePool::global().num_workers();
        if n <= 1 || num_threads <= 1 {
            return items
                .into_iter()
                .filter_map(|item| self.stages.apply(item))
                .collect();
        }

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

/// `pub(crate)` entry point for scoped pipelines. Identical to
/// `Pipeline::collect` but without `'static` bounds — driven by
/// `crate::scope::ScopedPipeline`, whose closure/stage lifetime is `'env`
/// (the surrounding `scope` block).
///
/// Soundness rests on the same `ComputePool::join` invariant that rayon-style
/// scoped parallelism relies on: the calling thread blocks inside
/// `Registry::in_worker_cold` until every recursively spawned sub-task
/// finishes, so every `'env` reference captured by `stages` outlives the
/// pool's access to them.
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
    if n <= 1 || num_threads <= 1 {
        return items
            .into_iter()
            .filter_map(|item| stages.apply(item))
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

/// Recursive merge-based collect for fused stages that may filter. Used only as
/// the `MAY_FILTER == true` fallback; output cardinality is unknown up front so
/// each leaf produces its own `Vec` and results are concatenated.
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
