use std::{
    any::Any,
    marker::PhantomData,
    panic,
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use super::{
    slots::Slots,
    traits::{
        Filter, FusedOp, FusedSink, FusedStage, FusedTryOp, FusedTryStage, Identity,
        InfallibleChain, MapErr, RangeOp, RangeTryOp, SinkOp, StageMarker, SyncMap, TryMap,
    },
};
use crate::{
    builder::config::{PipelineConfig, Workload},
    executor::compute::ComputePool,
    pool::{
        job::{Job, JobRef},
        latch::{CountLatch, Latch},
        registry::WorkerThread,
        unwind,
    },
};

type PanicPayload = Box<dyn Any + Send>;
/// Shared first-panic slot for hybrid dispatch. `halt_unwinding` catches each
/// chunk's panic before it reaches the lock, so the mutex is never poisoned.
type PanicSlot = Mutex<Option<PanicPayload>>;

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
    let num_threads = ComputePool::global().num_workers();
    let input = Slots::from_vec(items);
    let output = Slots::<R>::uninit(n);

    // Hybrid dispatch when called from outside the pool (the common
    // `.collect()` case): inject `num_threads` broad top-level chunks into the
    // global injector so every worker grabs one immediately — no fork/join
    // ramp-up. Each chunk then recurses via the tree (distributed deques +
    // stealing). See the "flat dispatch" post-mortem above for why pure flat
    // was a wash; hybrid keeps its small/medium-N win (parallel ramp-up) while
    // avoiding its large-N regression (only `num_threads` items through the
    // injector, not `N`).
    //
    // Fall back to the single-tree path when already on a pool worker: the
    // hybrid path blocks the caller on a `CountLatch`/`LockLatch`, which would
    // deadlock a pool worker (it must steal while waiting, not park).
    let on_pool = !WorkerThread::current().is_null();
    let result = if on_pool {
        par_index_rec(&input, &output, 0, n, op, splits)
    } else {
        par_index_collect_hybrid(&input, &output, n, op, splits, num_threads)
    };
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

// ── Hybrid flat/tree top-level dispatch ──
//
// Hypothesis: the single-tree `par_index_rec` grows parallelism one level at a
// time — the externally-injected top job runs on ONE worker, which runs its A
// inline and pushes B; only after B is stolen does a second worker join, and so
// on. That ramp-up costs ~log2(num_threads) join levels before every worker is
// busy, and is the bulk of the ~120 µs fixed dispatch overhead that dominates
// small/medium batches (notably the 1 K `cpu_heavy` case trailing rayon).
//
// Hybrid injects `num_threads` disjoint top-level chunks into the injector in
// one `inject_batch` (one JEC bump, one wake cascade). Every worker pops a
// chunk on its first `find_work`, so all workers are busy from t≈0. Each chunk
// then builds its own mini-tree via `par_index_rec`, so within-chunk stealing
// still uses the distributed local deques (no single-queue contention at large
// N, which is what sank pure flat dispatch).
//
// Panic plumbing: injected jobs must NOT let a panic reach the worker's
// `AbortIfPanic`. Each chunk's body is wrapped in `halt_unwinding`; the first
// panic is funnelled into a shared `PanicSlot`, every chunk (success or panic)
// decrements the `CountLatch`, and the driver — after `wait()` — drops the
// output ranges of successful chunks (failed chunks already cleaned their own
// ranges inside `par_index_rec`) and resumes the captured panic.

/// One top-level chunk of a hybrid-dispatch parallel index collect. Heap-
/// allocated (stable address); referenced by the injected `JobRef`. Carries
/// raw pointers to the shared `Slots`/`op`/`latch`/`panic_slot`, which all
/// live on the driver's stack frame — sound because the driver blocks on the
/// `CountLatch` until every chunk has executed.
struct ChunkJob<T, R, OP> {
    input: *const Slots<T>,
    output: *const Slots<R>,
    op: *const OP,
    start: usize,
    end: usize,
    splits: usize,
    /// Shared count latch; decremented on completion (success or panic).
    latch: *const CountLatch,
    /// Shared first-panic slot.
    panic_slot: *const PanicSlot,
    /// Set `true` on success. On panic, stays `false` (the range is already
    /// cleaned up by `par_index_rec`, so the driver skips it during the
    /// Err-path output teardown). Written before `latch.set`; the driver reads
    /// it after `latch.wait` returns (the latch's SeqCst provides the
    /// happens-before edge).
    succeeded: AtomicBool,
}

// SAFETY: the raw pointers reference data owned by the driver's stack frame;
// the driver blocks on the CountLatch until every chunk finishes, so the
// pointed-to data outlives every `execute` call. The shared `Slots`/`op`/
// `latch`/`panic_slot` are accessed from distinct workers but over disjoint
// index ranges (`Slots`) or through `Sync` types (`OP: Sync`, `CountLatch`,
// `Mutex`); each `ChunkJob` itself is touched by exactly one worker (the one
// that pops its `JobRef`).
unsafe impl<T: Send, R: Send, OP: Sync> Send for ChunkJob<T, R, OP> {}

impl<T, R, OP> Job for ChunkJob<T, R, OP>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    unsafe fn execute(this: *const ()) {
        unsafe {
            let this = &*this.cast::<Self>();
            // Catch any panic so it never reaches the worker's `AbortIfPanic`.
            // `par_index_rec` returns `Result<(), PanicPayload>` AND `join` may
            // resume-unwrap a deeper panic through it — so `halt_unwinding`
            // yields a nested Result that we flatten: both the propagated
            // (outer Err) and returned (inner Err) panic payloads land in the
            // shared slot.
            let r = unwind::halt_unwinding(|| {
                par_index_rec(&*this.input, &*this.output, this.start, this.end, &*this.op, this.splits)
            });
            match r {
                Ok(Ok(())) => this.succeeded.store(true, Ordering::Release),
                Ok(Err(p)) | Err(p) => {
                    // First writer wins; `halt_unwinding` caught any panic
                    // before we touched the lock, so the mutex is never
                    // poisoned. `unwrap_or_else(into_inner)` keeps us robust
                    // even if a future change violates that invariant.
                    let mut slot = (*this.panic_slot)
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if slot.is_none() {
                        *slot = Some(p);
                    }
                }
            }
            // Always signal completion so the driver wakes exactly once the
            // last chunk finishes, regardless of success/panic mix.
            CountLatch::set(this.latch);
        }
    }
}

/// Hybrid top-level dispatcher. Splits `[0, n)` into `num_chunks` contiguous
/// ranges, injects one `ChunkJob` per range, and blocks until all complete.
/// Returns `Err(first_panic)` if any chunk panicked (after dropping the
/// successful chunks' output ranges so the caller can free the buffers without
/// double-drop).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_collect_hybrid<T, R, OP>(
    input: &Slots<T>,
    output: &Slots<R>,
    n: usize,
    op: &OP,
    splits: usize,
    num_threads: usize,
) -> Result<(), PanicPayload>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    // One chunk per worker → instant parallel ramp-up. Round up the split
    // depth reduction so the per-chunk tree is shallower: total leaf count
    // stays ≈ num_threads * oversplit (matching the single-tree path), just
    // distributed across the chunks instead of grown from one root.
    let num_chunks = Ord::min(num_threads, n).max(1);
    let chunk_log2 = num_chunks.next_power_of_two().trailing_zeros() as usize;
    let chunk_splits = splits.saturating_sub(chunk_log2);

    let panic_slot: PanicSlot = Mutex::new(None);
    let latch = CountLatch::with_count(num_chunks, None);

    // Build contiguous ranges as evenly as possible (first `rem` chunks get one
    // extra item). ChunkJobs are boxed so their addresses stay pinned
    // regardless of the Vec reallocating its backing buffer.
    let chunk = n / num_chunks;
    let rem = n % num_chunks;
    let mut jobs: Vec<Box<ChunkJob<T, R, OP>>> = Vec::with_capacity(num_chunks);
    let mut start = 0;
    for i in 0..num_chunks {
        let size = chunk + usize::from(i < rem);
        let end = start + size;
        jobs.push(Box::new(ChunkJob {
            input: ptr::from_ref(input),
            output: ptr::from_ref(output),
            op: ptr::from_ref(op),
            start,
            end,
            splits: chunk_splits,
            latch: ptr::from_ref(&latch),
            panic_slot: ptr::from_ref(&panic_slot),
            succeeded: AtomicBool::new(false),
        }));
        start = end;
    }
    debug_assert_eq!(start, n);

    // One batched inject: a single JEC increment + a single wake cascade,
    // regardless of `num_chunks`. Every idle worker pops a chunk on its next
    // `find_work` → all workers busy from t≈0.
    let registry = ComputePool::global().registry();
    let job_refs: Vec<JobRef> = jobs
        .iter()
        .map(|j| unsafe { JobRef::new(std::ptr::from_ref(&**j)) })
        .collect();
    registry.inject_batch(job_refs.into_iter());

    // Block the external thread until every chunk has signalled. `CountLatch`
    // with no owner uses a `LockLatch` (parking-lot condvar) — correct for an
    // off-pool caller (a pool worker must NOT take this path; see the guard in
    // `par_index_collect`).
    latch.wait();

    // After `wait` returns every chunk's `execute` has run `CountLatch::set`;
    // the SeqCst fence there carries the `succeeded` Release store into our
    // Acquire load below.
    let panic_payload = panic_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(p) = panic_payload {
        // Drop output ranges of successful chunks (failed chunks already
        // cleaned their own ranges inside `par_index_rec`'s internal-node /
        // leaf-guard cleanup). After this, every output slot is either init
        // (no — we just dropped them) or uninit, so the caller can free the
        // buffers safely.
        for j in &jobs {
            if j.succeeded.load(Ordering::Acquire) {
                unsafe { output.drop_range(j.start, j.end) };
            }
        }
        return Err(p);
    }
    Ok(())
}

// ── Index-based parallel sink (`for_each`) — no output buffer ──
//
// The `for_each` terminal applies the fused chain + user closure for side
// effects only. Unlike `par_index_collect`, it allocates **no output `Slots`**:
// the leaf reads each input item, runs the chain, hands the result to the
// closure, and discards it. This is the structural fix for the
// `par_iter().for_each()` workload shape where `.map(f).collect::<Vec<()>>()`
// would pay for a pointless n-slot output buffer + n writes.

/// Recursive divide-and-conquer sink. Each leaf claims a disjoint input range
/// `[start, end)` and consumes it via `op`; no output is written.
///
/// Panic safety mirrors `par_index_rec`'s input half: a panicking leaf's
/// `ForEachGuard` drops the unread tail of its own range, internal nodes
/// propagate the first `Err`, and the panic-free sibling's range is already
/// fully consumed (every read slot is uninit, nothing to drop). On return,
/// every slot in `[start, end)` is either consumed (read) or dropped.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_for_each_rec<T, OP>(
    input: &Slots<T>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
) -> Result<(), PanicPayload>
where
    T: Send,
    OP: SinkOp<T>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)` exclusively.
        // input[start..end) is fully init; nothing else is touched.
        let in_slice = unsafe { input.as_slice(start, end) };
        par_for_each_leaf(in_slice, op);
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = ComputePool::global().join(
        || par_for_each_rec(input, start, mid, op, splits_left - 1),
        || par_for_each_rec(input, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        // The completed sibling fully consumed its own range (every slot read
        // → uninit, nothing to drop). The panicking sibling's ForEachGuard
        // already dropped its unread tail, so no per-range cleanup is needed
        // here — unlike par_index_rec, there is no output buffer to drop.
        (Err(p), _) | (_, Err(p)) => Err(p),
    }
}

/// Consume `[start, end)` sequentially on the current thread, applying `op`
/// for its side effect.
///
/// Panic safety uses a stack-local `ForEachGuard` whose `Drop` runs only on
/// unwind — the input-tail mirror of `LeafGuard` (without the output half,
/// since `for_each` allocates no output buffer). At the panic point in
/// `op.consume(item)` for iter `i = pos`, item `i` has been moved into `op`
/// (gone with the panic), `input[i+1..]` is still init (untouched, must be
/// dropped); `input[..i]` was already moved-out in prior iterations.
fn par_for_each_leaf<T, OP>(input: &[T], op: &OP)
where
    T: Send,
    OP: SinkOp<T>,
{
    /// RAII guard that drops the unread input tail on unwind. Counterpart to
    /// `LeafGuard` with the output half elided (no output buffer exists).
    ///
    /// `pos` tracks the count of fully consumed iterations at the unwind
    /// point. Item `pos` was moved into `op` and is gone with the panic, so
    /// we drop `input[pos+1..]` only.
    struct ForEachGuard<'a, T> {
        input: &'a [T],
        pos: usize,
    }

    impl<T> Drop for ForEachGuard<'_, T> {
        fn drop(&mut self) {
            // SAFETY: `pos` reflects the actual consumed-iteration count at
            // the unwind point. Items `..pos` were already moved out (uninit);
            // item `pos` was consumed by `op` and is gone; `input[pos+1..]`
            // is still init and must be dropped.
            unsafe {
                let in_live = self.input.as_ptr();
                for j in (self.pos + 1)..self.input.len() {
                    std::ptr::drop_in_place(in_live.add(j).cast_mut());
                }
            }
        }
    }

    let in_ptr = input.as_ptr();
    let n = input.len();

    let mut g = ForEachGuard { input, pos: 0 };

    while g.pos < n {
        let i = g.pos;
        // SAFETY: disjoint index; slot i is init (input). The read moves the
        // item out of the slot, leaving it uninit — never re-read.
        let item = unsafe { std::ptr::read(in_ptr.add(i)) };
        op.consume(item);
        g.pos = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
}

/// Drive `par_for_each_rec` over `[0, n)`. Propagates panics after the
/// recursion's `ForEachGuard` has dropped every unread input slot.
///
/// # Panics
///
/// Propagates any panic raised by `op`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_for_each<T, OP>(items: Vec<T>, op: &OP, splits: usize)
where
    T: Send,
    OP: SinkOp<T>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let input = Slots::from_vec(items);
    let result = par_for_each_rec(&input, 0, n, op, splits);
    match result {
        Ok(()) => {
            // All input slots consumed (read → uninit): dropping the box just
            // frees memory, no per-slot drops.
            drop(input);
        }
        Err(p) => {
            // Recursion already dropped every live (unread) input slot.
            drop(input);
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
// promising direction was a *hybrid*: inject `num_threads` broad top-level
// chunks (low injector contention, no ramp-up) and let each chunk recurse via
// the tree (distributed deques + stealing).
//
// ✅ DONE (2026-07): `par_index_collect_hybrid` below implements exactly this.
// A/B vs the single-tree baseline (32-core, sample-size 30, measurement-time 5):
//
//   sync_cpu_heavy       1 k: −2.8 %     10 k: −3.6 %     100 k: −1.1 %
//   pipeline_fusion     10 k: −6.5 %     100 k: −6.7 %
//   sync_lightweight    10 k: −4.0 %     100 k: −9.2 %      1 M: −9.6 % ←
//   sync_lightweight_cold 100 k: −5.9 %   1 M: −5.1 %
//   try_collect         100 k: −5.7 %
//
// Every size improved or held; the 1 M lightweight case that pure flat
// regressed by +14 % now *improves* by −9.6 % — the hybrid's
// `num_threads`-item inject never hits the single-injector MPMC contention
// that sank pure flat. The small/medium-N win is smaller than pure flat's
// (−15 % at 10 k) because each chunk still builds a mini-tree (some ramp-up
// inside the chunk), but avoiding the large-N cliff is the decisive win. The
// remaining ~120 µs fixed cost on tiny batches (1 k cpu_heavy still trails
// rayon) is now dominated by the off-pool `CountLatch`/condvar handoff, not
// ramp-up — a separate problem.

// ── Join-based parallel helpers ──

/// Whether a batch of `n` items should run sequentially on the calling thread
/// instead of being split across the pool. `num_threads` is read once by the
/// caller and passed in to avoid a second `ComputePool::global()` TLS hit.
///
/// Only the trivial cases short-circuit to serial: an empty or single-item
/// batch (no parallelism to exploit), or a single-threaded pool (nowhere to
/// steal to). Everything else goes through the fork/join tree.
///
/// # Why this no longer guesses based on batch size
///
/// An earlier version routed small batches (`n ≤ num_threads × k`) to a serial
/// loop to avoid the pool's ~120 µs fixed dispatch overhead (external-thread
/// job injection + `LockLatch` handoff + worker wake). That was tuned against
/// the `cpu_heavy` benchmark (~30 ns/item), whose serial↔parallel crossover is
/// ~3 k items.
///
/// The heuristic was **deceptive**: `.collect()` / `.for_each()` advertise
/// parallelism, but silently ran serially for small batches. Since the
/// crossover is `fixed_overhead / per_item_cost` and the framework cannot know
/// `per_item_cost`, the same `n` could mean microseconds of work or seconds
/// (file IO, crypto, network). A 100-item batch of file encryptions would be
/// serialized — turning a 4 s parallel run into a 100 s serial one.
///
/// The asymmetry is decisive: wrongly parallelizing a cheap small batch costs
/// ~120 µs (imperceptible); wrongly serializing an expensive small batch costs
/// the entire batch wall-time. If a user wants serial execution, that is their
/// decision to make explicitly — the framework's job is to parallelize, not to
/// second-guess the workload. The ~120 µs overhead on cheap small batches is
/// accepted as the price of honesty; the right long-term fix is to lower the
/// cold-inject cost itself (hybrid dispatch — see the flat-dispatch comment
/// above), not to silently downgrade to serial.
fn prefers_serial(n: usize, num_threads: usize, _workload: Workload) -> bool {
    n <= 1 || num_threads <= 1
}

/// Compute the number of recursive split levels. Aiming at ~`oversplit` tasks
/// per thread gives good work-stealing without excessive task overhead.
fn split_depth(n: usize, num_threads: usize, oversplit: usize) -> usize {
    let desired_tasks = (num_threads * oversplit).max(1);
    let by_threads = desired_tasks.next_power_of_two().trailing_zeros() as usize;
    let by_len = n.max(1).next_power_of_two().trailing_zeros() as usize;
    by_threads.min(by_len).max(1)
}

/// Items-per-worker (at oversplit = 1) below which the fork/join tree is built
/// with `oversplit = 1` instead of [`BALANCED_OVERSPLIT`].
///
/// Each internal node of the tree costs ~60-100 ns of dispatch overhead
/// (StackJob/Latch creation, deque push, `catch_unwind`, probe loop). With
/// `oversplit = 4` a 32-core pool builds 127 internal nodes — that fixed cost
/// dominates batches whose own leaf work is sub-microsecond.
///
/// When `n / num_threads` is small the per-leaf wall time is short enough that
/// tail latency from a single slow leaf is negligible, so the extra
/// stealing slack from `oversplit = 4` is pure overhead. Dropping to
/// `oversplit = 1` (32 leaves on 32 cores) trims ~95 nodes and measured
/// −8…−14 % on 10 k batches across `sync_cpu_heavy`, `sync_lightweight`, and
/// `pipeline_fusion`.
///
/// Above this threshold the leaves become long enough (measured cpu_heavy
/// crossover ~3 k items ⇒ ~150 µs/leaf) that scheduling jitter on the last
/// finishing worker stretches the tail; reverting to `oversplit = 1` at 100 k
/// cpu_heavy regressed +12.6 %.
const LOW_OVERSPLIT_ITEMS_PER_THREAD: usize = 1024;

/// Default oversplit factor for `Workload::Balanced`. A/B-tuned (2026-06,
/// 32-core): `1` regressed cpu_heavy ~+18 % (too few leaves ⇒ poor load
/// balancing, longer tail), `8` regressed ~+5.5 % (too many nodes ⇒ per-node
/// dispatch overhead). `4` (128 leaves on 32 cores) is the sweet spot.
const BALANCED_OVERSPLIT: usize = 4;

/// Oversplit factor for the fork/join tree, adapting to batch size.
///
/// See [`LOW_OVERSPLIT_ITEMS_PER_THREAD`] for the rationale. `Unbalanced`
/// always uses `8` for the stealing slack its expensive tail needs.
fn workload_oversplit(n: usize, num_threads: usize, workload: Workload) -> usize {
    match workload {
        Workload::Balanced => {
            if n / num_threads.max(1) <= LOW_OVERSPLIT_ITEMS_PER_THREAD {
                1
            } else {
                BALANCED_OVERSPLIT
            }
        }
        Workload::Unbalanced => 8,
    }
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
    /// Only trivially-empty batches (0-1 items or a single-threaded pool) run
    /// sequentially — see [`prefers_serial`] for why batch-size guessing was
    /// removed.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn collect(self) -> Vec<O> {
        let items = self.items;
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let num_threads = ComputePool::global().num_workers();
        if prefers_serial(n, num_threads, self.config.workload) {
            // Trivial case (n == 1 or single-threaded pool): skip the pool
            // entirely. Dispatch on `MAY_FILTER` so the pure path matches a
            // hand-written `iter().map().collect()` — no `Option` wrapper.
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

        // `oversplit` = tasks-per-worker for the fork/join tree. Adaptive:
        // small batches (≤ `LOW_OVERSPLIT_ITEMS_PER_THREAD` per worker) use
        // `1` to minimise join-dispatch overhead; larger batches use
        // `BALANCED_OVERSPLIT` for stealing slack. See `workload_oversplit`.
        let oversplit = workload_oversplit(n, num_threads, self.config.workload);
        let splits = split_depth(n, num_threads, oversplit);

        if S::MAY_FILTER {
            join_fused_collect(items, &self.stages, splits)
        } else {
            let op = FusedOp(self.stages);
            par_index_collect(items, &op, splits)
        }
    }

    /// Execute the fused pipeline, applying `f` to each output for its side
    /// effect. Returns `()`.
    ///
    /// The equivalent of rayon's `par_iter().for_each(..)`. Unlike
    /// [`.collect()`](Self::collect), **no output `Vec` is allocated**: the
    /// `for_each` terminal discards each transformed item after invoking `f`.
    /// For pipelines whose last step is a side effect (file writes, mutation
    /// of shared state, logging), this avoids the structural cost of a
    /// pointless `Vec<()>` (or `Vec<O>`) output buffer plus `n` slot writes.
    ///
    /// Filter stages are honoured: items dropped by an upstream filter are
    /// simply not passed to `f`.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `f` (after the leaf's
    /// cleanup guard drops unread input slots).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn for_each<F>(self, f: F)
    where
        F: Fn(O) + Send + Sync + 'static,
    {
        let items = self.items;
        let n = items.len();
        if n == 0 {
            return;
        }
        let num_threads = ComputePool::global().num_workers();
        if prefers_serial(n, num_threads, self.config.workload) {
            // Trivial case (n == 1 or single-threaded pool): run inline, no
            // output buffer. Dispatch on `MAY_FILTER` to keep the pure path
            // branch-free.
            if S::MAY_FILTER {
                for item in items {
                    if let Some(o) = self.stages.apply(item) {
                        f(o);
                    }
                }
            } else {
                for item in items {
                    let o = self.stages.apply_pure(item);
                    f(o);
                }
            }
            return;
        }

        let oversplit = workload_oversplit(n, num_threads, self.config.workload);
        let splits = split_depth(n, num_threads, oversplit);
        let op = FusedSink(self.stages, f);
        par_for_each(items, &op, splits);
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

        let oversplit = workload_oversplit(n, num_threads, self.config.workload);
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
    let oversplit = workload_oversplit(n, num_threads, workload);
    let splits = split_depth(n, num_threads, oversplit);
    if S::MAY_FILTER {
        join_fused_collect(items, &stages, splits)
    } else {
        let op = FusedOp(stages);
        par_index_collect(items, &op, splits)
    }
}

/// `pub(crate)` entry point for the scoped `for_each` terminal. Identical
/// dispatch logic to `Pipe::for_each` but without `'static` bounds — driven
/// by `crate::scope::ScopedPipe::for_each`, whose closure lifetime is `'env`.
///
/// Soundness rests on the same `ComputePool::join` invariant as
/// [`fused_collect_scoped`]: the calling thread blocks inside
/// `Registry::in_worker_cold` until every sub-task finishes, so every `'env`
/// reference captured by `stages` / `f` outlives the pool's access to them.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn fused_for_each_scoped<S, T, F>(items: Vec<T>, stages: S, f: F, workload: Workload)
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
    F: Fn(S::Output) + Sync,
{
    let n = items.len();
    if n == 0 {
        return;
    }
    let num_threads = ComputePool::global().num_workers();
    if prefers_serial(n, num_threads, workload) {
        if S::MAY_FILTER {
            for item in items {
                if let Some(o) = stages.apply(item) {
                    f(o);
                }
            }
        } else {
            for item in items {
                let o = stages.apply_pure(item);
                f(o);
            }
        }
        return;
    }
    let oversplit = workload_oversplit(n, num_threads, workload);
    let splits = split_depth(n, num_threads, oversplit);
    let op = FusedSink(stages, f);
    par_for_each(items, &op, splits);
}
