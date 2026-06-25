use std::{
    any::Any,
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    panic,
    sync::Arc,
};

use super::config::{PipelineConfig, Workload};
use crate::{
    executor::compute::ComputePool,
    handoff::{Receiver, Sender, SharedWaitGroup, channel::channel},
    state::{FenceBarrier, FenceMode, run_ordered_collect},
    sync::CancellationToken,
};

// ── Slots: index-addressable buffer for zero-copy parallel map ──

/// Boxed slot array backing the range-based parallel map.
///
/// Each slot is `UnsafeCell<MaybeUninit<T>>`. The `MaybeUninit` layer
/// suppresses item drops when the box itself is dropped, so the box's `Drop`
/// only frees memory — every slot that holds a live `T` must be dropped by the
/// caller before the buffer goes out of scope (the recursion in
/// [`par_index_rec`] guarantees this on both the success and panic paths).
///
/// Ranges processed by different worker threads are disjoint, so non-atomic
/// `read`/`write`/`drop_range` on disjoint indices is sound. `Sync` is sound
/// because items (`T: Send`) may legitimately move between threads.
pub(crate) struct Slots<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: access is governed by the disjoint-index discipline documented on
// `Slots`. Items of type `T` may cross threads, so we require `T: Send`.
unsafe impl<T: Send> Send for Slots<T> {}
unsafe impl<T: Send> Sync for Slots<T> {}

impl<T> Slots<T> {
    /// Take ownership of a `Vec<T>` and re-interpret it as an all-init slot
    /// array. Items are not moved — only the allocation's type is
    /// reinterpreted.
    fn from_vec(vec: Vec<T>) -> Self {
        let len = vec.len();
        let box_t: Box<[T]> = vec.into_boxed_slice();
        // SAFETY: `[T]` and `[UnsafeCell<MaybeUninit<T>>]` are layout-identical:
        // `UnsafeCell` is `#[repr(transparent)]` over its field, and
        // `MaybeUninit<T>` has the same size/align/ABI as `T`.
        let ptr = Box::into_raw(box_t).cast::<UnsafeCell<MaybeUninit<T>>>();
        let buf = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) };
        Slots { buf }
    }

    /// Allocate an all-uninit slot array of length `n`.
    ///
    /// Uses `set_len` after `with_capacity` so we never touch the backing
    /// memory — the slots are `MaybeUninit`, so uninitialized is a valid state.
    /// A `.collect()`-based init here would be a sequential O(n) loop that
    /// dominates lightweight workloads (measured: ~2 ms for 1 M slots).
    fn uninit(n: usize) -> Self {
        let mut v: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(n);
        // SAFETY: the capacity is `n` and `MaybeUninit<T>` is valid uninitialized,
        // so the slots do not need to be written before being read via `read`.
        unsafe { v.set_len(n) };
        Slots {
            buf: v.into_boxed_slice(),
        }
    }

    /// Drop slots `[start, end)`. All of them must be init.
    ///
    /// # Safety
    ///
    /// Every slot in `[start, end)` must hold a live `T`. Only valid for ranges
    /// produced by operations that never filter (see `RangeOp::MAY_FILTER`).
    #[inline]
    unsafe fn drop_range(&self, start: usize, end: usize) {
        for i in start..end {
            unsafe { (*self.buf.get_unchecked(i).get()).assume_init_drop() };
        }
    }

    /// View slots `[start, end)` as an all-init `&[T]` slice.
    ///
    /// Used by the leaf loop so LLVM sees a plain slice reference (noalias
    /// guarantees via Rust's borrow rules) instead of `&Slots` with
    /// `UnsafeCell` interior-mutability — that aliasing opacity is what stalls
    /// the auto-vectorizer and inflates the 1 M warm `par_map` cost ~2.6×.
    ///
    /// # Safety
    ///
    /// * Slots `[start, end)` must all be init.
    /// * Caller must ensure no `&mut` alias to the same range is live.
    #[inline]
    unsafe fn as_slice(&self, start: usize, end: usize) -> &[T] {
        debug_assert!(start <= end && end <= self.buf.len());
        // SAFETY: `[UnsafeCell<MaybeUninit<T>>]` is layout-identical to `[T]`;
        // caller guarantees the range is init and exclusively accessible.
        unsafe {
            let ptr = self.buf.as_ptr().cast::<T>().add(start);
            std::slice::from_raw_parts(ptr, end - start)
        }
    }

    /// View slots `[start, end)` as an all-uninit `&mut [T]` slice.
    ///
    /// Counterpart to [`Slots::as_slice`] for the output buffer. The caller is
    /// responsible for fully writing the slice before anyone reads it.
    ///
    /// # Safety
    ///
    /// * Slots `[start, end)` must all be uninit (no `T` to drop).
    /// * Caller must ensure no alias to the same range is live.
    #[inline]
    #[allow(clippy::mut_from_ref)] // Governed by Slots' disjoint-index discipline
    unsafe fn as_mut_slice(&self, start: usize, end: usize) -> &mut [T] {
        debug_assert!(start <= end && end <= self.buf.len());
        // SAFETY: same layout argument as `as_slice`; interior mutability via
        // `UnsafeCell` lets us produce `&mut [T]` from `&self`. The slice is
        // exclusively ours for the leaf's lifetime (disjoint-index discipline).
        unsafe {
            let ptr = self.buf.as_ptr().cast::<T>().add(start) as *mut T;
            std::slice::from_raw_parts_mut(ptr, end - start)
        }
    }

    /// Reclaim the buffer as a `Vec<T>` without dropping any slot. All slots
    /// must be init and owned by the caller.
    fn into_vec(self) -> Vec<T> {
        let len = self.buf.len();
        let ptr = Box::into_raw(self.buf).cast::<T>();
        // SAFETY: layout-identical to `[T]` (see `from_vec`); all slots are init
        // by contract. Rebuild as a boxed slice and convert via the idiomatic
        // `Box::into_vec` (cap == len, exactly matching the boxed slice).
        let boxed: Box<[T]> =
            unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) };
        boxed.into_vec()
    }
}

// ── RangeOp: how a leaf transforms an input item ──
//
/// Compile-time-fused transform applied to every item by the range-based core.
///
/// The leaf loop calls `apply` directly (no `Option`/branch) — this is critical
/// for vectorizing the lightweight `x + 1`-style hot loop, where an `Option`
/// discriminant + branch cuts LLVM's auto-vectorizer and costs ~2.5× on the
/// 1 M warm `par_map` path (measured: 710 µs → 290 µs, matching rayon).
///
/// `RangeOp` is therefore only ever constructed for stages whose
/// `FusedStage::MAY_FILTER == false`; the filtering path uses the per-leaf
/// `Vec` merge in `join_fused_collect` instead. This invariant is what makes
/// `Slots::drop_range` sound over arbitrary sub-ranges in the panic cleanup:
/// every output slot the leaf visits is unconditionally written.
trait RangeOp<T>: Sync {
    type Out: Send;
    fn apply(&self, item: T) -> Self::Out;
}

/// Map closure wrapper (`Fn(T) -> R`). Equivalent to `SyncMap<Identity, F>`,
/// kept as a thin separate type so `par_map` doesn't pay for the `Identity`
/// passthrough call in its monomorphized leaf.
struct FnMap<F>(F);

impl<T, R, F> RangeOp<T> for FnMap<F>
where
    F: Fn(T) -> R + Sync,
    R: Send,
{
    type Out = R;
    #[inline(always)]
    fn apply(&self, item: T) -> R {
        (self.0)(item)
    }
}

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
        return par_index_leaf(in_slice, out_slice, op);
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
fn par_index_leaf<T, R, OP>(
    input: &[T],
    output: &mut [R],
    op: &OP,
) -> Result<(), PanicPayload>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    debug_assert_eq!(input.len(), output.len());
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
                    std::ptr::drop_in_place(in_live.add(i) as *mut T);
                }
            }
        }
    }

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
    Ok(())
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

// ── Marker traits ──

/// Type-level marker for a pipeline stage. Maps `Input` to `Self::Output`.
pub trait StageMarker<Input> {
    type Output;
}

/// Identity stage — passes items through unchanged.
#[derive(Clone)]
pub struct Identity;

impl<T> StageMarker<T> for Identity {
    type Output = T;
}

/// Synchronous map stage: `Fn(T) -> O`.
#[derive(Clone)]
pub struct SyncMap<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I, O> StageMarker<I> for SyncMap<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
}

/// Filter stage: keeps items where `Fn(&T) -> bool` returns `true`.
#[derive(Clone)]
pub struct Filter<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I> StageMarker<I> for Filter<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
}

/// Barrier / fence stage. Forces a materialization boundary in the streaming
/// pipeline.
#[derive(Clone)]
pub struct Fence<Prev> {
    prev: Prev,
    #[allow(dead_code)]
    chunk_size: Option<usize>,
}

impl<Prev, I> StageMarker<I> for Fence<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

/// Ordered output stage. Preserves input ordering in the final collection.
#[derive(Clone)]
pub struct Ordered<Prev> {
    prev: Prev,
}

impl<Prev, I> StageMarker<I> for Ordered<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

// ── FusedStage trait ──

/// Compile-time fused stage: applies multiple pipeline stages in a single pass
/// without intermediate allocations.
pub trait FusedStage<T> {
    type Output;

    /// Whether `apply` may return `None` for an input it received (i.e. the
    /// stage chain contains a `Filter`). When `false`, the index-based collect
    /// fast path can assume every output slot it visits is init, which makes
    /// panic cleanup trivially sound (no per-slot validity tracking).
    const MAY_FILTER: bool = false;

    /// Apply the full fused chain. `Filter` stages may return `None`.
    fn apply(&self, item: T) -> Option<Self::Output>;

    /// Apply the full fused chain without the `Option` wrapper.
    ///
    /// Used by the hot path (`RangeOp` → `par_index_leaf`) so the leaf loop
    /// stays branch-free and vectorizable. Default impl extracts the `Option`
    /// payload, which is sound IFF the entire chain has `MAY_FILTER = false`.
    ///
    /// Each stage overrides this to thread the value through `prev.apply_pure`
    /// so no `Option` is ever constructed on the pure path.
    ///
    /// # Panics
    ///
    /// May panic (caught by the leaf's `catch_unwind`).
    #[inline(always)]
    fn apply_pure(&self, item: T) -> Self::Output {
        // SAFETY: contract — only call `apply_pure` when `Self::MAY_FILTER`
        // is false throughout the chain. `Pipeline::collect` enforces this.
        match self.apply(item) {
            Some(v) => v,
            // SAFETY: caller guarantees `MAY_FILTER = false`, so this is
            // unreachable.
            None => unsafe { std::hint::unreachable_unchecked() },
        }
    }
}

impl<T> FusedStage<T> for Identity {
    type Output = T;
    fn apply(&self, item: T) -> Option<T> {
        Some(item)
    }
    #[inline(always)]
    fn apply_pure(&self, item: T) -> T {
        item
    }
}

impl<Prev, F, I, O> FusedStage<I> for SyncMap<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<O> {
        self.prev.apply(item).map(|v| (self.f)(v))
    }
    #[inline(always)]
    fn apply_pure(&self, item: I) -> O {
        let v = self.prev.apply_pure(item);
        (self.f)(v)
    }
}

impl<Prev, F, I> FusedStage<I> for Filter<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
    // A filter can drop items, so the fast path cannot assume all slots init.
    const MAY_FILTER: bool = true;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item).filter(|v| (self.f)(v))
    }
    // No `apply_pure` override: `Filter` always has `MAY_FILTER = true`, so
    // the pure path is never taken through a `Filter` chain.
}

impl<Prev, I> FusedStage<I> for Fence<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
    #[inline(always)]
    fn apply_pure(&self, item: I) -> Prev::Output {
        self.prev.apply_pure(item)
    }
}

impl<Prev, I> FusedStage<I> for Ordered<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
    #[inline(always)]
    fn apply_pure(&self, item: I) -> Prev::Output {
        self.prev.apply_pure(item)
    }
}

// ── Pipeline (main user-facing type) ──

/// A type-state pipeline builder. Stages are fused at compile time into a
/// single pass over the data when possible (no `fence` / `ordered` boundaries).
///
/// Use [`Pipeline::new`] to start, chain `.map()` / `.filter()` calls,
/// then call `.collect(items)` to execute.
pub struct Pipeline<S = Identity, T = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<T>,
}

impl<T: Send + 'static> Pipeline<Identity, T> {
    /// Create a new pipeline (type-state entry point).
    ///
    /// `T` is inferred from the first staged method (e.g. `.map(|x: i32| ...)`),
    /// so callers do not need to spell it out — the previous `from_vec(vec![])`
    /// entry point existed only as a type hint and silently discarded its
    /// argument, which was both wasteful and confusing.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: Identity,
            config: PipelineConfig::default(),
            _marker: PhantomData,
        }
    }
}

impl<T: Send + 'static> Default for Pipeline<Identity, T> {
    /// `Pipeline: Default` lets downstream code write `Pipeline::<T>::default()`
    /// or rely on type inference from the first `.map` / `.filter` call.
    fn default() -> Self {
        Self::new()
    }
}

impl<S, T> Pipeline<S, T> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Append a synchronous map stage.
    pub fn map<O: Send + 'static>(
        self,
        f: impl Fn(T) -> O + Send + Sync + 'static,
    ) -> Pipeline<SyncMap<S, impl Fn(T) -> O + Send + Sync + 'static>, O>
    where
        S: StageMarker<T>,
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
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Pipeline<Filter<S, impl Fn(&T) -> bool + Send + Sync + 'static>, T>
    where
        S: StageMarker<T>,
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
    pub fn fence(self) -> Pipeline<Fence<S>, T>
    where
        S: StageMarker<T, Output = T>,
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
    pub fn fence_chunked(self, chunk_size: usize) -> Pipeline<Fence<S>, T>
    where
        S: StageMarker<T, Output = T>,
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
    pub fn ordered(self) -> Pipeline<Ordered<S>, T>
    where
        S: StageMarker<T, Output = T>,
    {
        Pipeline {
            stages: Ordered { prev: self.stages },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

// ── Collect for fully-fused sync pipelines ──

/// `RangeOp` wrapper around a `FusedStage` so the index-based core can drive
/// the compile-time-fused stage chain.
///
/// Only constructable when `S::MAY_FILTER == false` (enforced by
/// `Pipeline::collect`'s dispatch on `S::MAY_FILTER`). The `RangeOp::apply`
/// impl goes through `FusedStage::apply_pure`, which avoids constructing an
/// `Option` at all — keeping the leaf loop branch-free for the vectorizer.
struct FusedOp<S>(S);

impl<S, T> RangeOp<T> for FusedOp<S>
where
    S: FusedStage<T> + Sync,
    S::Output: Send,
{
    type Out = S::Output;
    #[inline(always)]
    fn apply(&self, item: T) -> S::Output {
        self.0.apply_pure(item)
    }
}

impl<S, T> Pipeline<S, T>
where
    S: FusedStage<T> + Send + Sync + 'static,
    T: Send + 'static,
    S::Output: Send + 'static,
{
    /// Execute the fused pipeline over `items` and collect results.
    ///
    /// Uses the index-based range core (pre-allocated output, no per-level
    /// `split_off`/`extend`) when the stage chain cannot filter
    /// (`S::MAY_FILTER == false`), and falls back to the recursive merge path
    /// otherwise (filters change output cardinality, so fixed-index writes are
    /// not possible).
    pub fn collect<I: IntoIterator<Item = T>>(self, items: I) -> Vec<S::Output> {
        let items: Vec<T> = items.into_iter().collect();
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

// ── Streaming pipeline (for stages that break fusion: async, fence, ordered)
// ──

/// Streaming pipeline for workloads that cannot be fused at compile time
/// (async stages, fences, ordered output, multi-stage).
///
/// Data flows through channels between feeder → workers → collector.
pub struct StreamPipeline {
    config: PipelineConfig,
    cancel: Option<CancellationToken>,
}

// ── Streaming stage helpers (used by the fence pipeline) ──

/// True iff `cancel` is set and the pipeline should stop feeding new work.
#[inline]
fn cancel_active(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(CancellationToken::is_cancelled)
}

/// Spawn `parallelism` workers on `pool` that pull from `rx`, apply `stage`,
/// and forward to `tx`. Each worker loops until its receiver disconnects or the
/// supplied cancellation token (if any) is signalled.
///
/// The supplied `rx`/`tx` are consumed: clones are handed to workers and the
/// originals dropped here, so channel close propagates correctly once all
/// workers exit (no external `WaitGroup` needed for completion signalling).
#[allow(clippy::needless_pass_by_value)] // ownership transfer is intentional:
// taking the endpoints by value ensures the caller cannot retain a clone that
// would keep the channel open after the workers have finished.
fn spawn_stage<I, O>(
    pool: &ComputePool,
    rx: Receiver<I>,
    tx: Sender<O>,
    parallelism: usize,
    cancel: Option<CancellationToken>,
    stage: impl Fn(I) -> O + Send + Sync + 'static,
) where
    I: Send + 'static,
    O: Send + 'static,
{
    let stage = Arc::new(stage);
    for _ in 0..parallelism {
        let stage = stage.clone();
        let rx = rx.clone();
        let tx = tx.clone();
        let cancel = cancel.clone();
        pool.submit(move || {
            while let Ok(item) = rx.recv() {
                if cancel_active(cancel.as_ref()) {
                    break;
                }
                if tx.send(stage(item)).is_err() {
                    break;
                }
            }
        });
    }
}

/// Fence forwarder: drains `mid_rx` into a [`FenceBarrier`] and releases
/// batches to `fenced_tx` according to `mode`.
///
/// In [`FenceMode::Barrier`] mode nothing is forwarded until `mid_rx`
/// disconnects (stage 1 fully done) — a hard barrier. In
/// [`FenceMode::Chunked`] mode batches flow as they accumulate, letting
/// stage 2 overlap stage 1.
///
/// Draining `mid_rx` eagerly (rather than waiting on a separate barrier
/// first) is what keeps stage 1 from blocking on a full channel: this is the
/// fix for the previous wait-before-drain deadlock.
#[allow(clippy::needless_pass_by_value)] // runs inside a `thread::spawn(move …)`:
// the endpoints must be owned so they are dropped (closing the channels) when
// forwarding completes.
fn forward_fenced<M>(mid_rx: Receiver<M>, fenced_tx: Sender<M>, mode: FenceMode)
where
    M: Send + 'static,
{
    let mut fence = FenceBarrier::<M>::new(mode);
    while let Ok(item) = mid_rx.recv() {
        if let Some(batch) = fence.push(item) {
            for it in batch {
                if fenced_tx.send(it).is_err() {
                    return;
                }
            }
        }
    }
    if let Some(remaining) = fence.flush() {
        for it in remaining {
            if fenced_tx.send(it).is_err() {
                return;
            }
        }
    }
}

impl StreamPipeline {
    /// Create a new streaming pipeline with the given config.
    #[must_use]
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            cancel: None,
        }
    }

    /// Attach a [`CancellationToken`] for cooperative cancellation.
    #[must_use]
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Run a single stage over `items`. If `ordered` is true, output preserves
    /// input order.
    pub fn run<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_ordered(items, stage)
        } else {
            self.run_unordered(items, stage)
        }
    }

    fn run_ordered<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);
        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder_handle = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let stage = Arc::new(stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let stage = stage.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let output = stage(item);
                    if tx.send((seq, output)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let collector_handle = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder_handle.join().unwrap();
        wg.wait();
        collector_handle.join().unwrap()
    }

    fn run_unordered<I, O>(
        &self,
        items: Vec<I>,
        stage: impl Fn(I) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);
        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder_handle = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let stage = Arc::new(stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let stage = stage.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let output = stage(item);
                    if tx.send(output).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder_handle.join().unwrap();
        wg.wait();
        results
    }

    /// Run two stages in sequence (stage1 → stage2).
    pub fn run_multi_stage<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_multi_stage_ordered(items, stage1, stage2)
        } else {
            self.run_multi_stage_unordered(items, stage1, stage2)
        }
    }

    fn run_multi_stage_ordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        // Floor each stage at one worker so a small `compute_workers` (e.g. a
        // single-threaded pool) still runs both stages instead of silently
        // starving one of them.
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = s(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        for _ in 0..par2 {
            let s = s2.clone();
            let rx = mid_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = s(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        drop(in_rx);
        drop(mid_tx);
        drop(mid_rx);
        drop(out_tx);

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        wg.wait();
        collector.join().unwrap()
    }

    fn run_multi_stage_unordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        // Floor each stage at one worker so a small `compute_workers` (e.g. a
        // single-threaded pool) still runs both stages instead of silently
        // starving one of them.
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let s1 = Arc::new(stage1);
        let s2 = Arc::new(stage2);
        let wg = SharedWaitGroup::new();
        wg.add(par1 + par2);

        for _ in 0..par1 {
            let s = s1.clone();
            let rx = in_rx.clone();
            let tx = mid_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = s(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        for _ in 0..par2 {
            let s = s2.clone();
            let rx = mid_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = s(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }

        drop(in_rx);
        drop(mid_tx);
        drop(mid_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder.join().unwrap();
        wg.wait();
        results
    }

    /// Run two stages separated by a fence whose isolation strength is
    /// controlled by `mode`.
    ///
    /// [`FenceMode::Barrier`] fully drains stage 1 before stage 2 sees any
    /// item (hard isolation). [`FenceMode::Chunked`] releases batches as they
    /// form so the stages overlap — the right default for mixed CPU/IO loads.
    pub fn run_with_fence<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
        mode: FenceMode,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_with_fence_ordered(items, stage1, stage2, mode)
        } else {
            self.run_with_fence_unordered(items, stage1, stage2, mode)
        }
    }

    fn run_with_fence_ordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
        mode: FenceMode,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        // Split workers across the two stages so the total count of blocking
        // pool jobs never exceeds the pool size. Submitting `parallelism`
        // workers per stage (2× oversubscription) starves the pool: stage 2
        // jobs can't get scheduled while stage 1 jobs block, deadlocking the
        // pipeline. This mirrors `run_multi_stage`'s invariant.
        let parallelism = self.config.compute_workers.min(n);
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<(u64, I)>(buffer_size);
        let (mid_tx, mid_rx) = channel::<(u64, M)>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<(u64, M)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for (seq, item) in items.into_iter().enumerate() {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send((seq as u64, item)).is_err() {
                    break;
                }
            }
        });

        // Thread seq through the tuple so input order survives the fence.
        spawn_stage(
            pool,
            in_rx,
            mid_tx,
            par1,
            self.cancel.clone(),
            move |(seq, item): (u64, I)| (seq, stage1(item)),
        );

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(
            pool,
            fenced_rx,
            out_tx,
            par2,
            self.cancel.clone(),
            move |(seq, mid): (u64, M)| (seq, stage2(mid)),
        );

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        fence_thread.join().unwrap();
        collector.join().unwrap()
    }

    fn run_with_fence_unordered<I, M, O>(
        &self,
        items: Vec<I>,
        stage1: impl Fn(I) -> M + Send + Sync + 'static,
        stage2: impl Fn(M) -> O + Send + Sync + 'static,
        mode: FenceMode,
    ) -> Vec<O>
    where
        I: Send + 'static,
        M: Send + 'static,
        O: Send + 'static,
    {
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        // See `run_with_fence_ordered`: keep total blocking jobs ≤ pool size.
        let parallelism = self.config.compute_workers.min(n);
        let par1 = (parallelism / 2).max(1);
        let par2 = parallelism.saturating_sub(par1).max(1);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(par1.max(par2) * 4);

        let (in_tx, in_rx) = channel::<I>(buffer_size);
        let (mid_tx, mid_rx) = channel::<M>(buffer_size);
        let (fenced_tx, fenced_rx) = channel::<M>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in items {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        spawn_stage(pool, in_rx, mid_tx, par1, self.cancel.clone(), stage1);

        let fence_thread = std::thread::spawn(move || forward_fenced(mid_rx, fenced_tx, mode));

        spawn_stage(pool, fenced_rx, out_tx, par2, self.cancel.clone(), stage2);

        let collector = std::thread::spawn(move || {
            let mut results = Vec::with_capacity(n);
            while let Ok(item) = out_rx.recv() {
                results.push(item);
            }
            results
        });

        feeder.join().unwrap();
        fence_thread.join().unwrap();
        collector.join().unwrap()
    }
}

impl StreamPipeline {
    /// Run a nested (1-to-N) pipeline: `outer_stage` expands each item,
    /// `inner_stage` maps each expanded item.
    pub fn run_nested<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
        ordered: bool,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        if ordered {
            self.run_nested_ordered(items, outer_stage, inner_stage)
        } else {
            self.run_nested_unordered(items, outer_stage, inner_stage)
        }
    }

    fn run_nested_ordered<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        let expanded: Vec<(u64, N)> = items
            .into_iter()
            .enumerate()
            .flat_map(|(seq, item)| {
                let nested = outer_stage(item);
                nested.into_iter().map(move |n| (seq as u64, n))
            })
            .collect();

        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);

        let (in_tx, in_rx) = channel::<(u64, N)>(buffer_size);
        let (out_tx, out_rx) = channel::<(u64, O)>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok((seq, item)) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = inner(item);
                    if tx.send((seq, out)).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let collector = std::thread::spawn(move || run_ordered_collect(&out_rx, n));

        feeder.join().unwrap();
        wg.wait();
        collector.join().unwrap()
    }

    fn run_nested_unordered<I, O, N>(
        &self,
        items: Vec<I>,
        outer_stage: impl Fn(I) -> Vec<N> + Send + Sync + 'static,
        inner_stage: impl Fn(N) -> O + Send + Sync + 'static,
    ) -> Vec<O>
    where
        I: Send + 'static,
        N: Send + 'static,
        O: Send + 'static,
    {
        let expanded: Vec<N> = items.into_iter().flat_map(outer_stage).collect();

        let n = expanded.len();
        if n == 0 {
            return Vec::new();
        }
        let parallelism = self.config.compute_workers.min(n);
        let pool = ComputePool::global();
        let buffer_size = self.config.buffer_size.max(parallelism * 4);

        let (in_tx, in_rx) = channel::<N>(buffer_size);
        let (out_tx, out_rx) = channel::<O>(buffer_size);

        let feeder_cancel = self.cancel.clone();
        let feeder = std::thread::spawn(move || {
            for item in expanded {
                if cancel_active(feeder_cancel.as_ref()) {
                    break;
                }
                if in_tx.send(item).is_err() {
                    break;
                }
            }
        });

        let inner = Arc::new(inner_stage);
        let wg = SharedWaitGroup::new();
        wg.add(parallelism);
        for _ in 0..parallelism {
            let inner = inner.clone();
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let wg = wg.clone();
            let worker_cancel = self.cancel.clone();
            pool.submit(move || {
                while let Ok(item) = rx.recv() {
                    if cancel_active(worker_cancel.as_ref()) {
                        break;
                    }
                    let out = inner(item);
                    if tx.send(out).is_err() {
                        break;
                    }
                }
                wg.done();
            });
        }
        drop(in_rx);
        drop(out_tx);

        let mut results = Vec::with_capacity(n);
        while let Ok(item) = out_rx.recv() {
            results.push(item);
        }

        feeder.join().unwrap();
        wg.wait();
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fused_sync_collect() {
        let items: Vec<i32> = (0..100).collect();
        let result = Pipeline::new()
            .map(|x: i32| x * 2)
            .map(|x: i32| x + 1)
            .collect(items);
        let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, expected);
    }

    #[test]
    fn test_fused_filter() {
        let items: Vec<i32> = (0..20).collect();
        let result = Pipeline::new()
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .collect(items);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, vec![0, 20, 40, 60, 80, 100, 120, 140, 160, 180]);
    }

    #[test]
    fn test_empty_input() {
        let items: Vec<i32> = vec![];
        let result = Pipeline::new()
            .map(|x: i32| x * 2)
            .collect(items);
        assert!(result.is_empty());
    }

    #[test]
    fn test_par_map() {
        let items: Vec<i32> = (0..100).collect();
        let result = par_map(items, |x: i32| x * 3);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    /// Correctness on a large input that exercises the recursive index split
    /// across many leaves.
    #[test]
    fn test_par_map_large() {
        let n: usize = 200_000;
        let items: Vec<u64> = (0..n).map(|x| x as u64).collect();
        let result = par_map(items.clone(), |x: u64| x.wrapping_mul(3).wrapping_add(1));
        assert_eq!(result.len(), n);
        for (i, r) in result.iter().enumerate() {
            assert_eq!(*r, (i as u64).wrapping_mul(3).wrapping_add(1));
        }
    }

    /// Validates that input items are consumed exactly once and output slots
    /// hold the right values, using a Drop type. A double-free or
    /// use-after-free would surface under Miri or as a wrong count.
    #[test]
    fn test_par_map_drop_type() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        #[derive(Debug)]
        struct Tracker(Arc<AtomicUsize>);
        impl PartialEq for Tracker {
            fn eq(&self, other: &Self) -> bool {
                Arc::ptr_eq(&self.0, &other.0)
            }
        }
        impl Drop for Tracker {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let items: Vec<Tracker> = (0..5000).map(|_| Tracker(counter.clone())).collect();
        let arcs: Vec<Arc<AtomicUsize>> = par_map(items, |t| {
            let c = t.0.clone();
            drop(t);
            c
        })
        .into_iter()
        .collect();
        assert_eq!(arcs.len(), 5000);
        // All input Trackers have been dropped (moved into the closure and consumed).
        assert_eq!(counter.load(Ordering::SeqCst), 5000);
        // The returned Arcs are still live — dropping them must not touch counter.
        drop(arcs);
        assert_eq!(counter.load(Ordering::SeqCst), 5000);
    }

    /// Panic propagation + cleanup for the index-based par_map path. Uses a
    /// Drop-tracking type so a leak or double-free shows up as a wrong drop
    /// count (and as UB under Miri).
    #[test]
    fn test_par_map_panic_safety() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Tracker {
            idx: usize,
            counter: Arc<AtomicUsize>,
        }
        impl Drop for Tracker {
            fn drop(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let panic_idx: usize = 1500;
        let n = 4000;
        let items: Vec<Tracker> = (0..n)
            .map(|idx| Tracker {
                idx,
                counter: counter.clone(),
            })
            .collect();

        let panic_idx_closure = panic_idx;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            par_map(items, move |t| {
                let idx = t.idx;
                drop(t);
                assert!(idx != panic_idx_closure, "induced panic at idx {idx}");
                idx as u64
            });
        }));
        assert!(result.is_err(), "par_map should propagate the panic");
        // Every input Tracker must have been dropped exactly once: the ones
        // consumed before the panic, plus the ones cleaned up by the recursion.
        assert_eq!(
            counter.load(Ordering::SeqCst),
            n,
            "expected all {n} Trackers dropped exactly once"
        );
    }

    /// Panic safety for the fused collect fast path (no filter).
    #[test]
    fn test_fused_collect_panic_safety() {
        let items: Vec<i32> = (0..2000).collect();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pipeline::new()
                .map(|x: i32| if x == 1500 { panic!("boom") } else { x + 1 })
                .collect(items);
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_single_stage_unordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run(items, |x: i32| x * 2, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_single_stage_ordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let result = sp.run(items, |x: i32| x * 2, true);
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_multi_stage() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run_multi_stage(items, |x: i32| x + 1, |x: i32| x * 3, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| (x + 1) * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_ok() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x * 3) });
        let mut r = result.unwrap();
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_err() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, String> {
            if x == 50 {
                Err(format!("bad: {x}"))
            } else {
                Ok(x * 2)
            }
        });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "bad: 50");
    }

    #[test]
    fn test_try_par_map_empty() {
        let items: Vec<i32> = vec![];
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x) });
        assert_eq!(result.unwrap(), Vec::<i32>::new());
    }

    #[test]
    fn test_stream_cancel_unordered() {
        let token = crate::sync::CancellationToken::new();
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config).with_cancel(token.clone());
        let items: Vec<i32> = (0..1000).collect();
        token.cancel();
        let result = sp.run(
            items,
            |x: i32| -> i32 {
                std::thread::sleep(std::time::Duration::from_micros(100));
                x * 2
            },
            false,
        );
        assert!(result.len() < 1000);
    }

    #[test]
    fn test_stream_no_cancel() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..50).collect();
        let result = sp.run(items, |x: i32| x * 2, false);
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn test_stream_nested() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..5).collect();
        let mut result = sp.run_nested(items, |x: i32| vec![x, x + 100], |x: i32| x * 2, false);
        result.sort_unstable();
        let mut expected: Vec<i32> = (0..5).flat_map(|x| vec![x * 2, (x + 100) * 2]).collect();
        expected.sort_unstable();
        assert_eq!(result, expected);
    }

    /// Regression: `with_cancel` previously only worked for `run`. The
    /// multi-stage / fence / nested paths ignored the token; this test guards
    /// against that regression by exercising all three with a pre-cancelled
    /// token + per-item sleep so that under cancellation none of them should
    /// process the full input.
    #[test]
    fn test_stream_cancel_all_variants() {
        use std::{num::NonZeroUsize, time::Duration};

        fn sleep_map<T: Copy>(x: T) -> T {
            std::thread::sleep(Duration::from_micros(50));
            x
        }

        let mk = || {
            let token = crate::sync::CancellationToken::new();
            let sp = StreamPipeline::new(PipelineConfig::default()).with_cancel(token.clone());
            (token, sp)
        };
        let items: Vec<i32> = (0..1000).collect();

        // multi_stage
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_multi_stage(items.clone(), sleep_map, sleep_map, false);
            assert!(r.len() < 1000, "multi_stage cancel failed: {}", r.len());
        }
        // with_fence
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_with_fence(
                items.clone(),
                sleep_map,
                sleep_map,
                FenceMode::Chunked(NonZeroUsize::new(32).unwrap()),
                false,
            );
            assert!(r.len() < 1000, "with_fence cancel failed: {}", r.len());
        }
        // nested
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_nested(items, |x| vec![x, x + 1], sleep_map, false);
            assert!(r.len() < 2000, "nested cancel failed: {}", r.len());
        }
    }
}
