// miri has no `movnti` semantics; other arches lack the intrinsics — both
// fall back to plain stores (see `nt_store`).
#[cfg(all(target_arch = "x86_64", not(miri)))]
use core::arch::x86_64::{_mm_sfence, _mm_stream_si64};
use std::{
    any::Any,
    marker::PhantomData,
    num::NonZeroUsize,
    panic, ptr,
    sync::{
        Mutex, OnceLock,
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
        unwind,
    },
};

type PanicPayload = Box<dyn Any + Send>;

// ── Pool resolution for the fused path ──
//
// Four sources of a compute pool, checked in priority order:
//   1. `with_compute_pool(pool)` — explicit, always wins.
//   2. `with_oversubscribe(factor)` — a hint that creates a transient pool sized to
//      `compute_workers × factor` at execution time.
//   3. `with_compute_workers(n)` with `n ≠ num_cpus` — a transient pool of `n` threads.
//   4. Neither → the global pool (one thread per core).
//
// The transient pools from (2)/(3) are owned by `ExecPool::Owned` and live on
// the stack frame of the terminal method (`.collect()` / `.for_each()` / …),
// outliving all uses of the `&ComputePool` reference it hands out. Dropping
// it at the end of the terminal call tears down the worker threads — correct
// for a one-shot pipeline, but a per-call ~ms cost that tight loops should
// avoid by pre-creating a pool and using `with_compute_pool` instead.

/// The compute pool that a fused terminal (`.collect()` / `.for_each()` / …)
/// drives its fork-join work through.
pub(crate) enum ExecPool<'a> {
    /// A borrowed reference — either the global pool or a user-supplied pool.
    Ref(&'a ComputePool),
    /// A transient pool created from an oversubscribe factor or a non-default
    /// worker budget. Owned so it is dropped (and its worker threads joined)
    /// when the terminal returns.
    Owned(ComputePool),
}

impl ExecPool<'_> {
    pub(crate) fn as_pool(&self) -> &ComputePool {
        match self {
            ExecPool::Ref(p) => p,
            ExecPool::Owned(p) => p,
        }
    }
}

/// Resolve the pool for a fused terminal call.
///
/// Precedence: explicit `compute_pool` > `oversubscribe` factor >
/// non-default `compute_workers` > global pool.
pub(crate) fn resolve_exec_pool(
    compute_pool: Option<&ComputePool>,
    oversubscribe: Option<NonZeroUsize>,
    compute_workers: usize,
) -> ExecPool<'_> {
    if let Some(p) = compute_pool {
        return ExecPool::Ref(p);
    }
    // Same fallback as `global_registry` / `PipelineConfig::default` so a
    // default-sized budget compares equal to the global pool's thread count.
    // Cached: an uncached `available_parallelism()` here cost ~26 µs/call
    // (cgroup file reads) — visible on every fused terminal (see `num_cpus`).
    let ncpus = crate::num_cpus();
    if let Some(factor) = oversubscribe {
        // Base = the configured budget (== ncpus when untouched), so
        // `with_compute_workers(n).with_oversubscribe(2)` yields n×2 threads.
        return ExecPool::Owned(ComputePool::new(
            compute_workers.saturating_mul(factor.get()),
        ));
    }
    // A budget that differs from the machine default cannot run on the global
    // pool (sized to ncpus) — honour it with a transient pool. Equal values
    // keep the shared global pool and pay no per-call construction cost.
    if compute_workers != ncpus {
        return ExecPool::Owned(ComputePool::new(compute_workers));
    }
    ExecPool::Ref(ComputePool::global())
}

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
    pool: &ComputePool,
    input: &Slots<T>,
    output: &Slots<R>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
    nt: bool,
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
        par_index_leaf(in_slice, out_slice, op, nt);
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_index_rec(pool, input, output, start, mid, op, splits_left - 1, nt),
        || par_index_rec(pool, input, output, mid, end, op, splits_left - 1, nt),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(p), Ok(())) => {
            // SAFETY: right sibling completed without filter (RangeOp never
            // filters), so [mid, end) is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(p)
        },
        (Ok(()), Err(p)) => {
            unsafe { output.drop_range(start, mid) };
            Err(p)
        },
        (Err(p), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(p)
        },
    }
}

// ── Non-temporal output stores for fused-collect leaves ──

/// Process-wide NT-store policy, parsed once from `YOUPIPE_NT_STORE`:
/// unset → `Auto`, `"0"` → force off, `"1"` → force on. Any other value
/// panics at first use — the previous any-non-"0"-means-on semantics once
/// read `YOUPIPE_NT_STORE=off` as ON on both sides of an A/B and measured
/// +0 % everywhere; failing loudly turns that trap into an immediate
/// error (see docs/src/dev/benchmarks.md "NT-store attribution").
#[derive(Clone, Copy, PartialEq, Eq)]
enum NtStorePolicy {
    /// Never use NT stores.
    Off,
    /// Always use NT stores for eligible outputs.
    On,
    /// NT stores for collect outputs of at least [`NT_AUTO_MIN_BYTES`]
    /// (the default).
    Auto,
}

/// [`NtStorePolicy::Auto`]'s threshold: whole-batch output bytes from which
/// non-temporal stores are a measured win in *both* consumer shapes
/// (write-only drop and immediate read-back). Fixed, not probed from cache
/// geometry: the win is per-line RFO elimination, and below the threshold
/// (100 K items = 0.8 MB: +9–11 %) gains shrink toward the ±2 % noise floor
/// where the balance gets machine-dependent.
const NT_AUTO_MIN_BYTES: usize = 8 << 20;

fn nt_store_policy() -> NtStorePolicy {
    static POLICY: OnceLock<NtStorePolicy> = OnceLock::new();
    *POLICY.get_or_init(|| match std::env::var("YOUPIPE_NT_STORE") {
        Err(_) => NtStorePolicy::Auto,
        Ok(v) => match v.as_str() {
            "0" => NtStorePolicy::Off,
            "1" => NtStorePolicy::On,
            other => panic!(
                "YOUPIPE_NT_STORE: invalid value {other:?} (leave unset for auto, \"0\" to force \
                 off, \"1\" to force on)"
            ),
        },
    })
}

/// Whether fused-collect leaves write eligible 8-byte outputs with
/// non-temporal (streaming) stores instead of plain `ptr::write`.
///
/// Mechanism: streaming stores bypass the cache hierarchy, so a
/// write-once/never-read output buffer (`.collect()` whose `Vec` the
/// consumer only drops) costs no read-for-ownership traffic and evicts
/// nothing from L3. The expected flip side — a consumer reading the output
/// right after collect pays a DRAM round-trip instead of a cache hit —
/// measured NOT to bite: horizontal `cpu_balanced_readback` (fold over the
/// output inside the timed region), same-binary two-process A/B, 5
/// alternating pairs × 2 rounds, rayon drift control ±0.5 %, NT-on
/// +11…+15 % wall at 100 K–4 M (both tables in docs/src/dev/benchmarks.md
/// "NT-store attribution"). The parallel phase's RFO elimination outweighs
/// the consumer's prefetched sequential read-back, hence the `Auto`
/// default. Write-once shape: +9/+12/+14/+18 % at 100 K/1 M/2 M/4 M
/// (NT-on also beats rayon at 2 M and 4 M, reversing the former
/// −12…−14 % deficit); 1 K/10 K ±2 % (output fits cache — nothing to
/// bypass), which is what the 8 MiB auto threshold leaves off.
///
/// Decision inputs: the process policy (env, cached) and the
/// *whole-batch* output size — resolved once per collect call in
/// `par_index_collect*` and threaded down as a leaf `bool`, because a
/// leaf only sees its own chunk and would undercount the batch.
///
/// Eligibility (`R` exactly 8 bytes, aligned ≥ 8) is a compile-time
/// constant, so other types compile the NT branch below away entirely;
/// eligible types pay one initialized-`OnceLock` load per collect call.
fn nt_store_enabled<R>(output_items: usize) -> bool {
    const fn eligible<R>() -> bool {
        size_of::<R>() == 8 && align_of::<R>() >= 8
    }
    if !eligible::<R>() {
        return false;
    }
    match nt_store_policy() {
        NtStorePolicy::Off => false,
        NtStorePolicy::On => true,
        NtStorePolicy::Auto => output_items.saturating_mul(size_of::<R>()) >= NT_AUTO_MIN_BYTES,
    }
}

/// Write one output slot with a streaming (non-temporal) store — x86_64
/// `movnti` (SSE2 baseline); plain `ptr::write` under miri / other arches.
///
/// Bitwise-moves `val` through an `i64`: Rust moves are bitwise
/// relocations, so this is indistinguishable from `ptr::write` for any
/// 8-byte type; `mem::forget` hands drop responsibility to the
/// destination slot (exactly one drop on every path).
///
/// NT stores are weakly ordered — a later release-store (latch `set`) does
/// NOT publish them to other threads. Every exit of an NT leaf must run
/// [`nt_fence`] (via [`NtFenceOnDrop`]) before the leaf's completion
/// becomes visible. The ordering chain: NT stores → `sfence` → latch
/// `set` (`SeqCst` swap, `pool::latch`) → waiter `Acquire` probe →
/// reader. Same-thread read-back (panic-cleanup `drop_in_place` of the
/// partial range) needs no fence: loads are never reordered with older
/// stores to the same location from the same thread.
///
/// # Safety
///
/// * `out_ptr` must be valid for one `R` write, exclusively owned, and 8-byte aligned (guaranteed
///   by [`nt_store_enabled`] eligibility plus the slot array layout: base aligned to
///   `align_of::<R>() ≥ 8`, slots exactly 8 bytes).
// A real call frame per 8-byte slot would swamp the store itself.
#[allow(clippy::inline_always)]
#[inline(always)]
unsafe fn nt_store<R>(out_ptr: *mut R, val: R) {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    {
        // SAFETY: eligibility makes the i64 reinterpretation a bitwise move.
        unsafe {
            let bits = ptr::read(ptr::addr_of!(val).cast::<i64>());
            std::mem::forget(val);
            _mm_stream_si64(out_ptr.cast::<i64>(), bits);
        }
    }
    #[cfg(any(not(target_arch = "x86_64"), miri))]
    // SAFETY: plain fallback store, same contract as `ptr::write`.
    unsafe {
        ptr::write(out_ptr, val)
    }
}

/// Order prior streaming stores before all subsequent stores (the
/// completion signal); no-op where NT stores are unavailable.
#[allow(clippy::inline_always)] // one instruction; a call would cost more
#[inline(always)]
fn nt_fence() {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    // SAFETY: `sfence` has no preconditions beyond x86_64 SSE2.
    unsafe {
        _mm_sfence();
    }
}

/// Runs [`nt_fence`] on drop — the NT leaf's fence for every exit at
/// once: normal scope end (success) and panic unwind. Declare it after
/// the leaf's cleanup guard: reverse-order unwind drops the fence first,
/// then the guard's partial-output drops; the success path drops it at
/// the `if` block's end, before the leaf returns into the latch-setting
/// callers (`par_index_rec`'s `join` / `hybrid_dispatch`'s `CountLatch`).
struct NtFenceOnDrop;

impl Drop for NtFenceOnDrop {
    #[inline]
    fn drop(&mut self) {
        nt_fence();
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
fn par_index_leaf<T, R, OP>(input: &[T], output: &mut [R], op: &OP, nt: bool)
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
    ///
    /// Stores raw pointers (not `&mut [R]`) so that `mem::forget(g)` on the
    /// success path doesn't conflict with the raw-pointer writes under
    /// Tree Borrows: a `&mut [R]` field in the guard would be disabled by
    /// the foreign write through `out_ptr`, making the `forget` access UB.
    /// Raw pointers carry no borrow tags, so there is nothing to disable.
    struct LeafGuard<T, R> {
        in_ptr: *const T,
        out_ptr: *mut R,
        n: usize,
        written: usize,
    }

    impl<T, R> Drop for LeafGuard<T, R> {
        fn drop(&mut self) {
            // SAFETY: `written` reflects the actual completed-iteration count
            // at the unwind point. `RangeOp` never filters, so output[..written)
            // has no holes — every slot there is init and must be dropped.
            // input[written+1..] is still init (untouched), must be dropped.
            // Item `written` itself was moved into `op` and is gone with the
            // panic, so we don't drop input[written].
            unsafe {
                let i = self.written;
                for j in 0..i {
                    ptr::drop_in_place(self.out_ptr.add(j));
                }
                for j in (i + 1)..self.n {
                    ptr::drop_in_place(self.in_ptr.add(j).cast_mut());
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafGuard {
        in_ptr,
        out_ptr,
        n,
        written: 0,
    };

    // The NT branch stays a separate loop copy: keeping the plain loop
    // untouched preserves its codegen (register-allocated `written`,
    // auto-vectorization for vectorizable ops) when NT is off.
    if nt {
        let _fence = NtFenceOnDrop;
        while g.written < n {
            let i = g.written;
            // SAFETY: same disjoint-index discipline as the plain loop
            // below; `nt_store`'s alignment/size requirements come from
            // `nt_store_enabled`'s eligibility check.
            let item = unsafe { ptr::read(in_ptr.add(i)) };
            let out = op.apply(item);
            unsafe { nt_store(out_ptr.add(i), out) };
            g.written = i + 1;
        }
    } else {
        while g.written < n {
            let i = g.written;
            // SAFETY: disjoint index; slot i is init (input) / uninit (output).
            let item = unsafe { ptr::read(in_ptr.add(i)) };
            let out = op.apply(item);
            unsafe { ptr::write(out_ptr.add(i), out) };
            g.written = i + 1;
        }
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
fn par_index_collect<T, R, OP>(
    items: Vec<T>,
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Vec<R>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let input = Slots::from_vec(items);
    let output = Slots::<R>::uninit(n);

    // Hybrid dispatch: inject `num_threads` broad top-level chunks into the
    // global injector so every worker grabs one immediately — no fork/join
    // ramp-up. Each chunk then recurses via the tree (distributed deques +
    // stealing). See the "flat dispatch" post-mortem below for why pure flat
    // was a wash; hybrid keeps its small/medium-N win (parallel ramp-up) while
    // avoiding its large-N regression (only `num_threads` items through the
    // injector, not `N`). Works for on-pool callers too: the dispatcher picks
    // a work-stealing `Stealing` latch for them (see `hybrid_dispatch`).
    let strategy = CollectStrategy {
        output: &output,
        op,
        nt: nt_store_enabled::<R>(n),
    };
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err();
    if let Some(f) = result {
        // Recursion already dropped every live slot; freeing buffers is safe.
        drop(input);
        drop(output);
        resume_panic(f);
    }
    // Input fully consumed (all uninit): dropping the box just frees memory.
    // Output fully init: transmute into the result Vec.
    drop(input);
    output.into_vec()
}

/// Fused-core entry for the streaming pass-through
/// (`StageSpawn::fuse_exec`): collect `op` over `items` exactly like
/// `Pipe::collect` does for a no-filter chain — trivial-batch serial
/// shortcut, then `SplitPlan` + hybrid-dispatch index core.
///
/// Streaming callers reach this only after the pass-through eligibility
/// guards passed (pure `SyncStage` chain, no cancellation, no per-stage
/// pins), so the `Vec`-in/`Vec`-out contract here is the whole story.
pub(super) fn fused_pass_collect<T, R, OP>(
    items: Vec<T>,
    op: &OP,
    workload: Workload,
    pool: &ComputePool,
) -> Vec<R>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    let n = items.len();
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        // Same trivial path as `Pipe::collect`: plain sequential map, no
        // output-buffer machinery.
        return items.into_iter().map(|item| op.apply(item)).collect();
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    par_index_collect(items, op, plan, pool)
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
// failure (panic, or the fallible op's first `Err`) is funnelled into a shared
// failure slot, every chunk (success or failure) decrements the `CountLatch`,
// and the driver — after `wait_spin()` — drops the output ranges of
// successful chunks (failed chunks already cleaned their own ranges inside
// the recursion) and resumes the captured failure.

// ── Strategy abstraction: collect / for_each / try_collect share one
// dispatcher ──
//
// The hybrid dispatcher's machinery (chunk layout, single `inject_batch`,
// `CountLatch::wait_spin`, shared failure-slot funnel) is identical for every
// terminal. The strategies differ only in:
//
//   1. The recursive chunk driver — `par_index_rec` writes to a shared output `Slots<R>`
//      (`collect`); `par_for_each_rec` is sink-only (`for_each`); `par_index_try_rec`
//      short-circuits into a shared error slot (`try_collect`'s no-filter fast path).
//   2. The failure cleanup — `collect`/`try_collect` must drop successful chunks' output ranges so
//      the caller can free the buffers; `for_each` has nothing to clean (the failed chunk's
//      `ForEachGuard` already dropped its own unread input tail).
//
// [`HybridStrategy`] abstracts exactly those differences so the dispatcher is
// written once as [`hybrid_dispatch`]. The strategy crosses into the
// dispatcher behind the type-erased [`ErasedStrategy`] boundary (see its doc
// for why — tl;dr: monomorphizing the dispatcher per strategy measurably
// regressed the untouched collect path via codegen layout shifts).

/// How a hybrid-dispatched chunk (or the driver chunk) can fail.
///
/// For the infallible strategies (`CollectStrategy` / `SinkStrategy`) only the
/// `Panic` variant is reachable. The fallible strategy (`TryStrategy`)
/// additionally carries the op's first `Err(e)`.
///
/// A panic outranks an op failure: the single-tree path propagates a panicking
/// leaf's unwind straight through the parent's `Result` match (discarding any
/// sibling `Err` it was about to return), so "panic wins" reproduces the
/// tree's observable semantics.
enum ErasedFailure {
    /// Op-level failure, type-erased (only produced by fallible strategies).
    /// Boxed once per failed run — failures are cold, the boxing cost is
    /// irrelevant.
    Op(Box<dyn Any + Send>),
    /// A panic payload.
    Panic(PanicPayload),
}

impl ErasedFailure {
    /// First-writer-wins record, except a panic always displaces a previously
    /// recorded op failure (see the type doc for why).
    fn record(slot: &Mutex<Option<Self>>, failure: Self) {
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (&mut *slot, failure) {
            (None, f) | (Some(ErasedFailure::Op(_)), f @ ErasedFailure::Panic(_)) => {
                // First failure wins — except a late panic, which displaces an
                // earlier recorded op failure.
                *slot = Some(f);
            },
            // First failure wins otherwise.
            (Some(_), _) => {},
        }
    }
}

/// First-failure kind for the fallible hybrid strategy.
enum TryFailure<E> {
    Error(E),
    Panic(PanicPayload),
}

/// Per-operation execution strategy for hybrid flat/tree top-level dispatch.
///
/// Implemented by [`CollectStrategy`] (`.collect()`), [`SinkStrategy`]
/// (`.for_each()`), and [`TryStrategy`] (`.try_collect()`, the no-filter fast
/// path). Each bundles the operation + any per-op shared state (the output
/// buffer for collect) and exposes the recursive chunk driver, the sequential
/// leaf runner (for driver-inline participation), and the successful-chunk
/// failure cleanup.
///
/// Handed to [`hybrid_dispatch`] behind the type-erased [`ErasedStrategy`]
/// boundary — see [`ErasedStrategy`] for why the dispatcher must stay
/// non-generic.
///
/// Generic over the **input handle** `IN` (not the item type): the dispatcher
/// only needs to hand chunks a shared view of the input buffer — owned runs
/// use `IN = Slots<T>` (move-out semantics), borrowed runs use `IN = [E]`
/// (shared-read semantics, `pipe_ref`). Item-type concerns live inside each
/// strategy's `run_chunk`/`run_sequential`.
trait HybridStrategy<IN: ?Sized>: Sync {
    /// What a failed chunk produces. Must be `Any + Send` so the erased
    /// boundary can box it into [`ErasedFailure::Op`] and the caller can
    /// downcast it back.
    type Failure: Any + Send;

    /// Recursively drive chunk `[start, end)`, returning `Err(failure)`.
    /// The strategy's recursion must catch its own panics (via
    /// `unwind::halt_unwinding`) so a panicking chunk never reaches the
    /// worker's `AbortIfPanic`.
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &IN,
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), Self::Failure>;

    /// Run `[start, end)` sequentially on the current thread — no `pool.join`,
    /// no scheduling. Used by the off-pool driver to participate in the work:
    /// it processes one chunk inline while the pool handles the rest (mirrors
    /// rayon's calling-thread participation). Panics propagate naturally to the
    /// caller's `halt_unwinding`; op failures come back as `Err`.
    fn run_sequential(&self, input: &IN, start: usize, end: usize) -> Result<(), Self::Failure>;

    /// Drop resources held by a *successful* chunk when some other chunk
    /// failed, so the caller can free the shared buffers without leak or
    /// double-drop. No-op for sink-only.
    ///
    /// # Safety
    ///
    /// `run_chunk` or `run_sequential` must have fully completed `[start, end)`
    /// without panic.
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize);
}

/// Signature of [`HybridStrategy::run_chunk`] behind the erased boundary.
type ErasedRunChunk<IN> =
    unsafe fn(*const (), &ComputePool, &IN, usize, usize, usize) -> Result<(), ErasedFailure>;

/// Type-erased [`HybridStrategy`] handle passed to [`hybrid_dispatch`].
///
/// `ctx` points at the concrete strategy living on the caller's stack frame;
/// the fn pointers know the concrete type and cast it back. Non-capturing
/// closures coerce to `unsafe fn` pointers, so each strategy pays one
/// trampoline that boxes failures into [`ErasedFailure`].
///
/// Keeping the dispatcher non-generic matters: the indirect calls happen once
/// per chunk (~`num_threads` per run), far off the per-item hot path, while
/// monomorphizing per terminal measurably regressed the *untouched* collect
/// path +16…30 % at 10k–100k via codegen layout shifts (measured; this
/// project is acutely layout-sensitive — see the `codegen-units = 1` note in
/// `Cargo.toml`).
// Manual impls: derived ones would add undesired `IN: Copy` bounds — the
// parameter only appears in fn-pointer signatures.
impl<IN: ?Sized> Clone for ErasedStrategy<IN> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<IN: ?Sized> Copy for ErasedStrategy<IN> {}

struct ErasedStrategy<IN: ?Sized> {
    ctx: *const (),
    /// SAFETY contracts mirror the `HybridStrategy` methods.
    run_chunk: ErasedRunChunk<IN>,
    run_sequential: unsafe fn(*const (), &IN, usize, usize) -> Result<(), ErasedFailure>,
    cleanup_success: unsafe fn(*const (), usize, usize),
}

// SAFETY: the fn pointers only ever dereference `ctx`, which points at a
// `HybridStrategy` (whose impl requires `Sync`) that outlives the dispatch.
unsafe impl<IN: ?Sized> Sync for ErasedStrategy<IN> {}

impl<IN: ?Sized, S> From<&S> for ErasedStrategy<IN>
where
    S: HybridStrategy<IN>,
{
    fn from(strategy: &S) -> Self {
        let ctx = ptr::from_ref(strategy).cast::<()>();
        ErasedStrategy {
            ctx,
            // SAFETY: `ctx` is a valid `&S` for the duration of the dispatch
            // (the caller's frame blocks in `hybrid_dispatch`).
            run_chunk: |ctx, pool, input, start, end, splits| unsafe {
                (*ctx.cast::<S>())
                    .run_chunk(pool, input, start, end, splits)
                    .map_err(|f| ErasedFailure::Op(Box::new(f)))
            },
            run_sequential: |ctx, input, start, end| unsafe {
                (*ctx.cast::<S>())
                    .run_sequential(input, start, end)
                    .map_err(|f| ErasedFailure::Op(Box::new(f)))
            },
            cleanup_success: |ctx, start, end| unsafe {
                (*ctx.cast::<S>()).cleanup_success_chunk(start, end);
            },
        }
    }
}

/// Hybrid strategy for `.collect()`: writes outputs into a shared `Slots<R>`
/// at known indices, and on panic drops successful chunks' output ranges so
/// the caller can free the buffers.
///
/// Holds references into the caller's (`par_index_collect`) stack frame; sound
/// because `hybrid_dispatch` blocks on the `CountLatch` until every chunk has
/// executed, so the borrowed `output` / `op` outlive every chunk access.
struct CollectStrategy<'a, R, OP> {
    output: &'a Slots<R>,
    op: &'a OP,
    /// Whole-batch NT decision (see [`nt_store_enabled`]) — a leaf only
    /// sees its own chunk, so the auto tier must be resolved against the
    /// full output size by the driver.
    nt: bool,
}

impl<T, R, OP> HybridStrategy<Slots<T>> for CollectStrategy<'_, R, OP>
where
    T: Send,
    R: Send,
    OP: RangeOp<T, Out = R>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &Slots<T>,
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        par_index_rec(
            pool,
            input,
            self.output,
            start,
            end,
            self.op,
            splits,
            self.nt,
        )
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &Slots<T>,
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the caller (driver or leaf) owns
        // `[start, end)` exclusively. Input slots are init; output slots
        // are uninit.
        let in_slice = unsafe { input.as_slice(start, end) };
        let out_slice = unsafe { self.output.as_mut_slice(start, end) };
        par_index_leaf(in_slice, out_slice, self.op, self.nt);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize) {
        // SAFETY: caller guarantees `run_chunk` returned `Ok(())` for
        // `[start, end)`, so those output slots are fully init and safe to
        // drop. After this the range is uninit, letting the caller free the
        // backing buffer without double-drop.
        unsafe { self.output.drop_range(start, end) };
    }
}

/// Hybrid strategy for `.for_each()`: sink-only, no output buffer. On panic
/// there is nothing to clean — the failed chunk's `ForEachGuard` already
/// dropped its own unread input tail, and successful chunks fully consumed
/// their input ranges.
struct SinkStrategy<'a, OP> {
    op: &'a OP,
}

impl<T, OP> HybridStrategy<Slots<T>> for SinkStrategy<'_, OP>
where
    T: Send,
    OP: SinkOp<T>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &Slots<T>,
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        par_for_each_rec(pool, input, start, end, self.op, splits)
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &Slots<T>,
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the caller owns `[start, end)` exclusively.
        let in_slice = unsafe { input.as_slice(start, end) };
        par_for_each_leaf(in_slice, self.op);
        Ok(())
    }

    #[inline]
    /// # Safety
    ///
    /// Nothing to drop for a sink-only strategy.
    unsafe fn cleanup_success_chunk(&self, _start: usize, _end: usize) {
        // No-op: `for_each` allocates no output buffer; the failed chunk's
        // `ForEachGuard` already dropped its own unread input tail inside
        // `par_for_each_rec`, and successful chunks fully consumed theirs.
    }
}

/// Hybrid strategy for `.try_collect()`'s no-filter fast path: like
/// [`CollectStrategy`] it writes outputs into a shared `Slots<R>` at known
/// indices, but the chunk driver is fallible — the first `Err(e)` is funnelled
/// into the shared [`TryFailure`] slot instead of a panic-only slot.
///
/// Range resolution on each failure kind:
/// - `Ok(())` chunk — output range fully init; the driver drops it via `cleanup_success_chunk` when
///   some other chunk failed.
/// - `Err(e)` chunk — `par_index_try_rec`'s leaf/internal-node cleanup has already dropped every
///   live output slot and consumed every input slot in the chunk's range, so nothing remains to
///   clean (mirrors the panicked chunk of the infallible strategies).
/// - Panicking chunk — unwinds through the recursion (leaf guard cleans its own partial range;
///   sibling ranges may leak, same documented behaviour as the single-tree path).
struct TryStrategy<'a, R, E, OP> {
    output: &'a Slots<R>,
    op: &'a OP,
    _marker: PhantomData<fn(E)>,
}

impl<T, R, E, OP> HybridStrategy<Slots<T>> for TryStrategy<'_, R, E, OP>
where
    T: Send,
    R: Send,
    E: Send + 'static,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    type Failure = TryFailure<E>;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &Slots<T>,
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), TryFailure<E>> {
        par_index_try_rec(pool, input, self.output, start, end, self.op, splits)
            .map_err(TryFailure::Error)
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &Slots<T>,
        start: usize,
        end: usize,
    ) -> Result<(), TryFailure<E>> {
        // SAFETY: disjoint range — the caller (driver) owns `[start, end)`
        // exclusively. Input slots are init; output slots are uninit.
        let in_slice = unsafe { input.as_slice(start, end) };
        let out_slice = unsafe { self.output.as_mut_slice(start, end) };
        par_index_try_leaf(in_slice, out_slice, self.op).map_err(TryFailure::Error)
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize) {
        // SAFETY: caller guarantees the chunk returned `Ok(())`, so those
        // output slots are fully init and safe to drop.
        unsafe { self.output.drop_range(start, end) };
    }
}

/// One top-level chunk of a hybrid-dispatched parallel operation. Stored in a
/// single contiguous `Box<[ChunkJob]>` shared by all chunks (not individually
/// boxed); referenced by the injected `JobRef`. Carries raw pointers to the
/// shared input view (`Slots` for owned runs, `[E]` for borrowed runs) /
/// `ErasedStrategy` / `latch` / failure slot, which all live on the driver's
/// stack frame — sound because the driver blocks on the `CountLatch`
/// until every chunk has executed.
struct ChunkJob<IN: ?Sized> {
    input: *const IN,
    strategy: ErasedStrategy<IN>,
    start: usize,
    end: usize,
    splits: usize,
    /// The compute pool to use for within-chunk recursion. Raw pointer to the
    /// `ComputePool` on the driver's stack frame; valid because the driver
    /// blocks on the `CountLatch` until every chunk finishes.
    pool: *const ComputePool,
    /// Shared count latch; decremented on completion (success or panic).
    latch: *const CountLatch,
    /// Shared first-failure slot (panic, and for fallible strategies the op's
    /// first `Err`).
    fail_slot: *const Mutex<Option<ErasedFailure>>,
    /// Set `true` on success. On failure, stays `false` (the range is already
    /// cleaned up by the strategy's recursion, so the driver skips it during
    /// the Err-path teardown). Written before `latch.set`; the driver reads
    /// it after `latch.wait` returns (the latch's SeqCst provides the
    /// happens-before edge).
    succeeded: AtomicBool,
}

// SAFETY: the raw pointers reference data owned by the driver's stack frame;
// the driver blocks on the CountLatch until every chunk finishes, so the
// pointed-to data outlives every `execute` call. The shared input view /
// `strategy` / `pool` / `latch` / `fail_slot` are accessed from distinct
// workers but over disjoint index ranges (`Slots` / `[E]`) or through `Sync`
// types (`ErasedStrategy: Sync`, `ComputePool: Sync`, `CountLatch`, `Mutex`);
// each `ChunkJob` itself is executed by exactly one thread (a pool worker or
// the off-pool driver — whoever pops its `JobRef` from the injector).
unsafe impl<IN: ?Sized + Sync> Send for ChunkJob<IN> {}

impl<IN: ?Sized + Sync> Job for ChunkJob<IN> {
    unsafe fn execute(this: *const ()) {
        unsafe {
            let this = &*this.cast::<Self>();
            // Catch any panic so it never reaches the worker's `AbortIfPanic`.
            // The erased `run_chunk` returns `Result<(), ErasedFailure>` AND
            // `join` may resume-unwrap a deeper panic through it — both the
            // propagated (outer `Err`) and returned (inner `Err`) failures
            // land in the shared slot, panic outranking op failures.
            let r = unwind::halt_unwinding(|| {
                (this.strategy.run_chunk)(
                    this.strategy.ctx,
                    &*this.pool,
                    &*this.input,
                    this.start,
                    this.end,
                    this.splits,
                )
            });
            match r {
                Ok(Ok(())) => this.succeeded.store(true, Ordering::Release),
                Ok(Err(f)) => ErasedFailure::record(&*this.fail_slot, f),
                Err(p) => ErasedFailure::record(&*this.fail_slot, ErasedFailure::Panic(p)),
            }
            // Always signal completion so the driver wakes exactly once the
            // last chunk finishes, regardless of success/failure mix.
            CountLatch::set(this.latch);
        }
    }
}

/// Tail chunks withheld from the injector for the hybrid driver's work-assist
/// (small batches only — see `hybrid_dispatch`). The driver runs chunk 0
/// inline plus this many reserve chunks from its wait loop; workers execute
/// the rest. Each unit absorbs one slowest-to-wake worker's worth of tail
/// latency. A/B (3+2 interleaved rounds, 2026-09): no measurable wall-time
/// change at 1 or 4 — under back-to-back benchmark loops most workers stay
/// ready, so the rescue value only shows when workers are absent (see
/// `test_hybrid_driver_assists_when_workers_busy`); 1 stays the conservative
/// default because raising it shifts more of the batch onto the (otherwise
/// spinning) driver at the cost of parallelism when workers ARE available.
const ASSIST_RESERVE_CHUNKS: usize = 1;

/// Which dispatch surface on-pool hybrid callers use (large batches only —
/// see the regime comment in `hybrid_dispatch`). Runtime-overridable via
/// `YOUPIPE_ONPOOL_HYBRID` so A/B benchmarks compare the same binary:
/// recompiles swing tight benchmarks by tens of percent through pure
/// code-layout shifts (the established methodology — see
/// `YOUPIPE_OVERSPLIT`). A/B scripts must pin each side's value (`0`, `1`,
/// `2`); any other non-"0" value means level 1, the pre-levels semantics.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OnpoolHybrid {
    /// Every on-pool caller takes the pre-hybrid single tree.
    Off,
    /// Chunks dispatched through the global injector (wake cascade).
    Injector,
    /// The on-pool driver pushes its chunks onto its OWN local LIFO deque;
    /// peers steal them from the FIFO end instead of all funnelling through
    /// the one global injector (st3 semantics, same as `join`'s B branch).
    LocalDeque,
}

fn onpool_hybrid_mode() -> OnpoolHybrid {
    static MODE: OnceLock<OnpoolHybrid> = OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("YOUPIPE_ONPOOL_HYBRID").as_deref() {
        Err(_) | Ok("0") => OnpoolHybrid::Off,
        Ok("2") => OnpoolHybrid::LocalDeque,
        Ok(_) => OnpoolHybrid::Injector,
    })
}

/// Hybrid top-level dispatcher. Splits `[0, n)` into `num_chunks` contiguous
/// ranges (`num_threads + plan.chunk_slack`, see [`UNBALANCED_CHUNK_SLACK`]).
/// Chunk 0 is run **inline on the driver thread** (mirrors rayon's
/// off-pool path where the calling thread participates; see the inline comment
/// in the body for the ramp-up/wake-cascade rationale); chunks
/// `1..num_chunks - reserve` are injected as `ChunkJob`s and the driver blocks
/// until all complete. The `reserve` tail chunks (small batches only) are
/// never injected — the driver executes them from its wait loop instead.
///
/// On-pool callers (a worker of this pool) take the hybrid path only for
/// large batches (`chunk_splits > 0`), waiting through a work-stealing
/// `Stealing` latch; small batches shortcut to the single tree inside (see
/// the body for the measured regime split).
///
/// Returns `Err(first_failure)` if any chunk (driver or pool) failed (after
/// the strategy has cleaned up the successful chunks' per-chunk resources so
/// the caller can free the shared buffers). The caller downcasts the erased
/// failure back to its concrete kind.
///
/// Non-generic over the strategy (see [`ErasedStrategy`]) so this dispatcher
/// and `ChunkJob` compile once per input handle `IN`, serving `.collect()`,
/// `.for_each()`, and `.try_collect()` alike, for both owned (`Slots<T>`) and
/// borrowed (`[E]`) input views.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn hybrid_dispatch<IN>(
    pool: &ComputePool,
    input: &IN,
    strategy: &ErasedStrategy<IN>,
    n: usize,
    plan: SplitPlan,
    num_threads: usize,
) -> Result<(), ErasedFailure>
where
    IN: ?Sized + Sync,
{
    // Caller context decides how the driver waits: a worker of THIS pool gets
    // a `Stealing` `CountLatch` (waits via the work-stealing `wait_until`
    // loop, parking through the sleep module's latch protocol — never a
    // condvar it would have to service itself); an off-pool thread (or a
    // worker of a different pool) gets the spin-then-condvar `Blocking`
    // latch. This is what lets on-pool callers — `pool.submit` tasks, stream
    // stage closures, nested `.run()` collectors — take the hybrid path
    // instead of paying a log2(num_threads) fork/join ramp-up per nested
    // terminal.
    let owner = pool.on_this_pool_owner();

    // One chunk per worker → instant parallel ramp-up; `chunk_slack` adds
    // extra chunks that persist in the injector for late-arriving workers
    // (see `UNBALANCED_CHUNK_SLACK`). Round the split depth reduction so the
    // per-chunk tree is shallower: total leaf count stays ≈ num_threads *
    // oversplit (matching the single-tree path), just distributed across the
    // chunks instead of grown from one root.
    let num_chunks = Ord::min(num_threads + plan.chunk_slack, n).max(1);
    // With slack, use floor(log2(num_chunks)): a slack-sized count (33..48)
    // keeps the same per-chunk tree depth as 32 chunks, preserving the total
    // leaf budget fine-grained stealing needs (ceil would coarsen one level —
    // measured +3 pt on uniform zstd). Without slack, keep the historical
    // next-power-of-two rounding bit-for-bit (identical for the power-of-two
    // chunk counts of `num_chunks == num_threads`, different only for
    // `n < num_threads` batches where the old behavior is the tuned one).
    let chunk_log2 = if plan.chunk_slack == 0 {
        num_chunks.next_power_of_two().trailing_zeros() as usize
    } else {
        (usize::BITS - 1 - num_chunks.leading_zeros()) as usize
    };
    let chunk_splits = plan.depth.saturating_sub(chunk_log2);

    let chunk = n / num_chunks;
    let rem = n % num_chunks;

    // Driver participation (small batches, `chunk_splits == 0`): the calling
    // thread (off-pool external thread or on-pool worker alike) runs chunk 0
    // inline while the pool handles the rest — mirroring rayon's off-pool
    // path. Beyond chunk 0 (off-pool only), `ASSIST_RESERVE_CHUNKS`
    // tail chunks are withheld from the injector and executed by the driver
    // from its wait loop (`wait_spin_assist`): a parked worker takes ~µs-scale
    // to wake and the slowest-to-wake worker gates the batch tail, so letting
    // the otherwise-spinning driver absorb the last chunk(s) closes exactly
    // that tail.
    //
    // The reserve is kept OUT of the injector on purpose — the driver must not
    // interact with the pool-global queue at all. Two pop-based variants were
    // tried and reverted: (a) executing any popped job breaks on foreign
    // worker-only jobs (`in_worker_cold`'s `StackJob` asserts on the worker
    // TLS and unwinds straight through the driver frame, skipping the latch
    // wait); (b) popping, identity-checking and re-queueing foreign jobs
    // reorders the injector's FIFO, which foreign submitters depend on — a
    // stream feeder pushed behind resident stage-worker jobs starves, and
    // with every worker parked on an empty channel recv the whole pool
    // deadlocks (reproduced under the parallel test suite).
    //
    // Guarded by `chunk_splits == 0` so the driver only runs single-leaf
    // chunks (≈ n/num_threads items, sub-microsecond for the small/medium
    // batches this targets). For large batches (`chunk_splits > 0`) the
    // driver chunk would be a multi-leaf range processed sequentially — its
    // memory traffic competes with the pool workers' bandwidth on
    // memory-bound workloads, which regressed `sync_lightweight` 1 M by
    // ~3 %.
    let driver_participates = chunk_splits == 0;
    let first_pool_chunk = usize::from(driver_participates);
    // Small batches (`chunk_splits == 0`) on-pool keep the single tree.
    // Measured 2026-07 on `sync_nested_on_pool` (32 cores):
    //   * `nested_saturated/1K` +430 % under hybrid (recompile A/B): P concurrent nested batches
    //     flood the global injector with ~P×P tiny chunks, and every driver's stealing wait then
    //     pops through that one contended MPMC — the same single-injector collapse that sank flat
    //     dispatch at large N. The tree distributes via local deques + peer stealing instead.
    //   * `nested_single/1K` +3 % under hybrid (recompile A/B): with P−1 workers parked, the tree's
    //     incremental local-deque pushes (one wake per join) edge out the inject_batch wake
    //     cascade.
    // Large batches keep the hybrid path: same-binary knob A/B
    // (`YOUPIPE_ONPOOL_HYBRID`, 5 interleaved rounds) shows
    // `nested_single/100K` −3.5 % (25/25 dominant) — ramp-up dominates and
    // the injector round trips amortize; `nested_saturated/100K` is a
    // +2 %-lean wash (every worker nesting large batches is exotic, and
    // even there hybrid is within spread of the tree). Level 2
    // (`LocalDeque`, 2026-09-25, 5 interleaved rounds) removes that
    // saturation lean: vs level 1 `nested_single/100K` −2.9 % (24/25),
    // `nested_saturated/100K` −1.7 % (25/25); vs the tree the margin is
    // session-unstable (±3–6 %, sign flips across sessions), so the
    // default stays 0. Ungating this small-batch shortcut under level 2
    // (`YOUPIPE_ONPOOL_HYBRID_SMALL`, experiment, reverted) still collapses
    // the parked-peers regime: `nested_single/1K` +80 % (0/25) — only
    // `nested_saturated/1K` improves (−8 %, 25/25); the gate stays (see
    // docs/src/dev/scheduler.md "on-pool callers").
    //
    // SAFETY: single-tree execution of `[0, n)` with the full split budget
    // — the same call the pre-hybrid on-pool path made directly. Panics
    // unwind out of `run_chunk` (leaf guards clean partial state) exactly
    // as they did through the `par_*_rec` calls; op failures return through
    // the erased boundary, which the call sites already downcast (the
    // `Op(Box<TryFailure<E>>)` shape is the try strategies' own).
    let mode = onpool_hybrid_mode();
    if owner.is_some() && (driver_participates || mode == OnpoolHybrid::Off) {
        return unsafe { (strategy.run_chunk)(strategy.ctx, pool, input, 0, n, plan.depth) };
    }
    // From here on every on-pool caller is in the large-batch regime with the
    // knob enabled (`!driver_participates`), so `reserve` is 0 for them via
    // the else branch — and the `Stealing` wait never needs the assist hook.
    let reserve = if driver_participates {
        ASSIST_RESERVE_CHUNKS.min(num_chunks - first_pool_chunk - 1)
    } else {
        0
    };
    let pool_chunks = num_chunks - first_pool_chunk;
    debug_assert!(pool_chunks >= 1, "prefers_serial guarantees num_chunks ≥ 2");
    let fail_slot: Mutex<Option<ErasedFailure>> = Mutex::new(None);
    // The latch waits for every non-inline chunk, including the off-pool
    // driver's reserve chunks (their execution decrements it like a worker's
    // would).
    let latch = CountLatch::with_count(pool_chunks, owner);

    // Build pool chunk jobs (chunks `first_pool_chunk..num_chunks`). All
    // ChunkJobs share ONE heap allocation (`Box<[ChunkJob]>`, frozen via
    // `into_boxed_slice` so element addresses are stable for the injected
    // `JobRef`s). The borrowed `strategy` lives on the caller's stack frame
    // (which blocks on this call until `wait_spin` returns), so the raw
    // context pointer inside is valid for every chunk's `execute`.
    //
    // Chunk boundaries: chunk i covers `[i*chunk + min(i,rem), next)`. The
    // driver (if participating) owns chunk 0 = `[0, chunk + usize::from(rem >
    // 0)]`; pool chunks follow contiguously.
    let (driver_start, driver_end) = if driver_participates {
        (0, chunk + usize::from(rem > 0))
    } else {
        (0, 0)
    };
    let mut jobs_vec: Vec<ChunkJob<IN>> = Vec::with_capacity(pool_chunks);
    let mut start = driver_end;
    for i in first_pool_chunk..num_chunks {
        let size = chunk + usize::from(i < rem);
        let end = start + size;
        jobs_vec.push(ChunkJob {
            input: ptr::from_ref(input),
            strategy: *strategy,
            start,
            end,
            splits: chunk_splits,
            pool: ptr::from_ref(pool),
            latch: ptr::from_ref(&latch),
            fail_slot: ptr::from_ref(&fail_slot),
            succeeded: AtomicBool::new(false),
        });
        start = end;
    }
    debug_assert_eq!(start, n);
    let jobs: Box<[ChunkJob<IN>]> = jobs_vec.into_boxed_slice();
    // The tail `reserve` jobs are driver-owned (never injected); only
    // `jobs[..injected_len]` get JobRefs.
    let injected_len = jobs.len() - reserve;

    // Dispatch pool chunks. Off-pool (and `Injector` level): one injector
    // batch — a single JEC increment + a single wake cascade; every idle
    // worker pops a chunk on its next `find_work`. `LocalDeque` level
    // (on-pool only): the driver pushes the whole batch onto its OWN local
    // LIFO deque, so P concurrent nested batches distribute across P deques
    // instead of converging on the one global MPMC — the distributed
    // dispatch surface that keeps the single tree flat under saturation.
    // The driver's own wait consumes from the LIFO end while peers steal
    // from the FIFO end, so driver and stealers never touch the same slot;
    // overflow beyond the deque capacity spills to the injector. Consumption
    // semantics are identical on every surface: each JobRef's `execute`
    // decrements the latch from within, so `counter == 0` still implies
    // every JobRef was fully consumed (the driver's own wait pops its chunks
    // exactly like `join`'s B branch). The JobRefs are produced lazily from
    // the boxed slice (no intermediate `Vec<JobRef>` allocation).
    let registry = pool.registry();
    let job_refs = jobs[..injected_len]
        .iter()
        .map(|j| unsafe { JobRef::new(ptr::from_ref(j)) });
    if mode == OnpoolHybrid::LocalDeque && owner.is_some() {
        registry.push_local_batch(job_refs);
    } else {
        registry.inject_batch(job_refs);
    }

    // Run chunk 0 inline on the driver thread, concurrently with the pool
    // (only when `driver_participates`; otherwise `driver_end == 0` and the
    // call is a no-op on an empty range). Any panic is caught by
    // `halt_unwinding` and funnelled into the shared failure slot; an op
    // failure (fallible strategies) is recorded the same way. `driver_ok`
    // tracks success for the failure-cleanup path below.
    let mut driver_ok = false;
    if driver_participates {
        let driver_result = unwind::halt_unwinding(|| unsafe {
            (strategy.run_sequential)(strategy.ctx, input, driver_start, driver_end)
        });
        match driver_result {
            Ok(Ok(())) => driver_ok = true,
            Ok(Err(f)) => ErasedFailure::record(&fail_slot, f),
            Err(p) => ErasedFailure::record(&fail_slot, ErasedFailure::Panic(p)),
        }
    }

    // Wait until every pool chunk has signalled. Off-pool (`Blocking`
    // latch): `wait_spin` — spin-then-park, because the condvar park/notify
    // handshake is ~10–20 µs of fixed overhead per batch (two syscalls + a
    // wake cascade); for small/medium batches whose own parallel work is only
    // tens of µs that handshake dominated the wall time. Spinning on the
    // SeqCst counter for a bounded budget lets the last chunk's decrement
    // land inside the spin window and skips the syscall; long waits still
    // fall through to the condvar. See `CountLatch::wait_spin` for the
    // synchronization argument.
    //
    // Work-assist (off-pool, driver-participates regime, `reserve > 0`):
    // while waiting, the driver runs the batch's withheld reserve chunks
    // (see the reserve block above for why they never touch the injector).
    // `counter == 0` still implies every *injected* `JobRef` was fully
    // consumed (the completion `set` lives inside `execute`), which is
    // load-bearing for the teardown below: once `wait` returns and this
    // frame (owning the `Box<[ChunkJob]>`) goes away, no worker can touch a
    // freed chunk.
    //
    // On-pool (`Stealing` latch): `wait_spin` routes to the work-stealing
    // `wait_until` loop — the driver keeps executing (its own injected
    // chunks, foreign jobs, steals) instead of burning a core, and parks
    // only through the sleep module's latch protocol.
    if reserve > 0 {
        let mut reserve_next = injected_len;
        let assist = || {
            if reserve_next >= jobs.len() {
                return false;
            }
            let j = &jobs[reserve_next];
            reserve_next += 1;
            // SAFETY: reserve chunks are never injected — this driver is
            // their sole executor, and its frame outlives the latch wait.
            // Runs the same `execute` a worker would (panic capture, failure
            // recording, latch decrement).
            unsafe { <ChunkJob<IN> as Job>::execute(ptr::from_ref(j).cast::<()>()) };
            true
        };
        latch.wait_spin_assist(assist);
    } else {
        latch.wait_spin();
    }

    // After `wait_spin` returns every pool chunk's `execute` has run
    // `CountLatch::set`; the SeqCst fence there carries the `succeeded`
    // Release store into our Acquire load below.
    let failure = fail_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(f) = failure {
        // Let the strategy clean up each successful chunk's per-chunk state
        // (failed chunks already cleaned their own inside the recursion's
        // internal-node / leaf-guard cleanup). After this the caller can free
        // the shared buffers safely.
        for j in &jobs {
            if j.succeeded.load(Ordering::Acquire) {
                // SAFETY: `succeeded` is set only after `run_chunk` returned
                // `Ok(())`, which is the precondition of `cleanup_success`.
                unsafe { (j.strategy.cleanup_success)(j.strategy.ctx, j.start, j.end) };
            }
        }
        // Clean up the driver chunk if it succeeded but a pool chunk failed.
        // SAFETY: `driver_ok` is set only after `run_sequential` completed
        // without panic, which fully processed `[driver_start, driver_end)`.
        if driver_ok {
            unsafe { (strategy.cleanup_success)(strategy.ctx, driver_start, driver_end) };
        }
        return Err(f);
    }
    Ok(())
}

/// Downcast an [`ErasedFailure`] back into the infallible strategies' only
/// failure kind (a panic payload) and resume the unwind.
///
/// # Panics
///
/// Panics via `resume_unwind` with the captured payload.
fn resume_panic(failure: ErasedFailure) -> ! {
    match failure {
        ErasedFailure::Panic(p) => panic::resume_unwind(p),
        // Infallible strategies never record an op failure.
        ErasedFailure::Op(_) => unreachable!("collect/for_each have no op failure"),
    }
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
    pool: &ComputePool,
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
    let (l, r) = pool.join(
        || par_for_each_rec(pool, input, start, mid, op, splits_left - 1),
        || par_for_each_rec(pool, input, mid, end, op, splits_left - 1),
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
                    ptr::drop_in_place(in_live.add(j).cast_mut());
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
        let item = unsafe { ptr::read(in_ptr.add(i)) };
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
fn par_for_each<T, OP>(items: Vec<T>, op: &OP, plan: SplitPlan, pool: &ComputePool)
where
    T: Send,
    OP: SinkOp<T>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let input = Slots::from_vec(items);

    // Hybrid dispatch: inject `num_threads` broad top-level chunks so every
    // worker is busy at t≈0 with no fork/join ramp-up — the same structural
    // win `par_index_collect` gets via `CollectStrategy`. See the "flat
    // dispatch" post-mortem below for why pure flat was a wash; hybrid keeps
    // the small/medium-N ramp-up win while each chunk recurses via the tree
    // (distributed deques + stealing), avoiding the single-injector MPMC
    // contention that sank pure flat at large N. On-pool callers get the
    // work-stealing `Stealing` latch (see `hybrid_dispatch`).
    let strategy = SinkStrategy { op };
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err();
    match result {
        Some(f) => {
            // Recursion already dropped every live (unread) input slot.
            drop(input);
            resume_panic(f);
        },
        None => {
            // All input slots consumed (read → uninit): dropping the box just
            // frees memory, no per-slot drops.
            drop(input);
        },
    }
}

// ── Index-based fast path for fallible (`try_collect`) pipelines ──
//
// When `FusedTryStage::MAY_FILTER == false`, output cardinality equals input
// cardinality (every item either succeeds or aborts the whole pipeline with an
// error). This lets us pre-allocate the output `Slots<R>` and write results at
// known indices — the same zero-allocation strategy `par_index_collect` uses
// for infallible pipelines. The range-tree path (`fused_try_filter_collect`)
// serves chains containing `Filter`.

/// Recursive divide-and-conquer for fallible stages. Returns `Err(e)` on the
/// first error; on error, all init output slots in the error branch are
/// cleaned up by the leaf, and sibling ranges are dropped by this function.
///
/// Panics propagate naturally through `join`'s `halt_unwinding`/`resume_unwind`
/// (re-raised past the match). The leaf's `TryLeafGuard` handles panic cleanup
/// of the leaf's own partial range, identical to `LeafGuard` in
/// `par_index_leaf`.
fn par_index_try_rec<T, R, E, OP>(
    pool: &ComputePool,
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
    let (l, r) = pool.join(
        || par_index_try_rec(pool, input, output, start, mid, op, splits_left - 1),
        || par_index_try_rec(pool, input, output, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) => {
            // SAFETY: right sibling completed without filter (RangeTryOp never
            // filters), so [mid, end) is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(e)
        },
        (Ok(()), Err(e)) => {
            unsafe { output.drop_range(start, mid) };
            Err(e)
        },
        (Err(e), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(e)
        },
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
    /// `mem::forget`. Uses raw pointers for the same Tree Borrows reason as
    /// `LeafGuard` — see the comment there.
    struct TryLeafGuard<T, R> {
        in_ptr: *const T,
        out_ptr: *mut R,
        n: usize,
        written: usize,
    }

    impl<T, R> Drop for TryLeafGuard<T, R> {
        fn drop(&mut self) {
            // SAFETY: same reasoning as `LeafGuard::drop` — `written` reflects
            // completed iterations at the unwind point.
            unsafe {
                let i = self.written;
                for j in 0..i {
                    ptr::drop_in_place(self.out_ptr.add(j));
                }
                for j in (i + 1)..self.n {
                    ptr::drop_in_place(self.in_ptr.add(j).cast_mut());
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = TryLeafGuard {
        in_ptr,
        out_ptr,
        n,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input) / uninit (output).
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        match op.try_apply(item) {
            Ok(out) => {
                unsafe { ptr::write(out_ptr.add(i), out) };
                g.written = i + 1;
            },
            Err(e) => {
                // Error path: run the same cleanup the guard would do on
                // panic, then disarm (forget) so Drop doesn't double-clean.
                // Item `i` was consumed by `try_apply` and is gone.
                unsafe {
                    for j in 0..i {
                        ptr::drop_in_place(out_ptr.add(j));
                    }
                    for j in (i + 1)..n {
                        ptr::drop_in_place(in_ptr.add(j).cast_mut());
                    }
                }
                std::mem::forget(g);
                return Err(e);
            },
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
///
/// Hybrid flat/tree dispatch (shared with `collect` / `for_each` via
/// [`TryStrategy`]): `num_threads` broad chunks injected in one
/// `inject_batch`, every worker busy at t≈0 — no fork/join ramp-up. On-pool
/// callers get the work-stealing `Stealing` latch (see `hybrid_dispatch`).
fn par_index_try_collect<T, R, E, OP>(
    items: Vec<T>,
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Result<Vec<R>, E>
where
    T: Send,
    R: Send,
    E: Send + 'static,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let input = Slots::from_vec(items);
    let output = Slots::<R>::uninit(n);

    let strategy = TryStrategy {
        output: &output,
        op,
        _marker: PhantomData,
    };
    // Downcast the erased op failure back to `TryFailure<E>`.
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err()
    .map(|f| match f {
        ErasedFailure::Op(b) => match b.downcast::<TryFailure<E>>() {
            Ok(tf) => *tf,
            Err(_) => unreachable!("try strategy only records TryFailure<E>"),
        },
        ErasedFailure::Panic(p) => TryFailure::Panic(p),
    });
    match result {
        None => {
            drop(input);
            Ok(output.into_vec())
        },
        Some(TryFailure::Error(e)) => {
            // Recursion already dropped every live output slot.
            drop(input);
            drop(output);
            Err(e)
        },
        Some(TryFailure::Panic(p)) => {
            // Mirrors the single-tree path: a panic unwinds past the buffer
            // management (init slots may leak, documented above).
            drop(input);
            drop(output);
            panic::resume_unwind(p);
        },
    }
}

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
// `hybrid_dispatch` (above, driven by the per-terminal strategies) implements
// exactly this.
// A/B vs the single-tree baseline (32-core, sample-size 30, measurement-time
// 5):
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
// inside the chunk), but avoiding the large-N cliff is the decisive win.
//
// The off-pool wait that hybrid introduces was originally a condvar park
// (`CountLatch`/`LockLatch`), costing ~10–20 µs of fixed overhead per batch on
// the driver thread — the dominant remaining cost on the 1 K `cpu_heavy` case.
// It is now a spin-then-park (`CountLatch::wait_spin`): a bounded tight spin on
// the SeqCst `counter` covers the small/medium-batch envelope without a
// syscall, falling through to the condvar only for genuinely long waits. See
// the `CountLatch::wait_spin` doc in `src/pool/latch.rs` for why the spin must
// still end in the mutex acquire (use-after-free avoidance).

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
/// loop to avoid the pool's fixed dispatch overhead (external-thread job
/// injection + off-pool wait + worker wake). That was tuned against the
/// `cpu_heavy` benchmark (~30 ns/item), whose serial↔parallel crossover is
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
/// only the dispatch envelope (now ~20–30 µs after hybrid dispatch +
/// `CountLatch::wait_spin`, was ~120 µs — imperceptible); wrongly serializing
/// an expensive small batch costs the entire batch wall-time. If a user wants
/// serial execution, that is their decision to make explicitly — the
/// framework's job is to parallelize, not to second-guess the workload. The
/// dispatch overhead on cheap small batches is accepted as the price of
/// honesty; the right long-term fix is to lower the cold-inject cost itself
/// (hybrid dispatch + spin-then-park — see the flat-dispatch comment above),
/// not to silently downgrade to serial.
fn prefers_serial(n: usize, num_threads: usize) -> bool {
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

/// Oversplit factor for `Workload::Unbalanced`. Unlike `Balanced`, the whole
/// point is that an idle worker must find a stealable leaf even when the
/// batch is small — the per-node dispatch overhead is the price of
/// tail-latency insurance on a skewed workload (the adaptive `Balanced`
/// path would drop to `1` below [`LOW_OVERSPLIT_ITEMS_PER_THREAD`]).
///
/// Runtime-overridable (`YOUPIPE_OVERSPLIT`) so A/B benchmarks compare the
/// same binary — compile-time flips change code layout enough on their own
/// to swing tight benchmarks by tens of percent.
fn unbalanced_oversplit() -> usize {
    static FACTOR: OnceLock<usize> = OnceLock::new();
    *FACTOR.get_or_init(|| {
        std::env::var("YOUPIPE_OVERSPLIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32)
    })
}

/// Oversplit factor for the fork/join tree, adapting to batch size.
///
/// See [`LOW_OVERSPLIT_ITEMS_PER_THREAD`] for the rationale. `Unbalanced`
/// always uses [`unbalanced_oversplit`]; `Custom(n)` pins `n` for full manual
/// control (benchmarking, known-skew workloads outside the two presets).
fn workload_oversplit(n: usize, num_threads: usize, workload: Workload) -> usize {
    match workload {
        Workload::Balanced => {
            if n / num_threads.max(1) <= LOW_OVERSPLIT_ITEMS_PER_THREAD {
                1
            } else {
                BALANCED_OVERSPLIT
            }
        },
        Workload::Unbalanced => unbalanced_oversplit(),
        Workload::Custom(factor) => factor.get(),
    }
}

/// Narrow-tier extra top-level chunks injected beyond worker count for
/// `Workload::Unbalanced` (runtime-overridable via `YOUPIPE_CHUNK_SLACK`,
/// same-binary A/B; see [`unbalanced_chunk_slack`] for the adaptive tier).
///
/// Rationale (2026-09 diagnosis, see dev/scheduler.md "latecomer slack"):
/// on an SMT machine a fully-loaded pool permanently excludes the 1–2
/// workers that lose the first scheduling round after a batch inject —
/// either their futex wake is delivered 100 µs–1.7 ms late (CFS wakeup
/// preemption under oversubscription) or they sit in the `sched_yield`
/// backoff phase, starved to one `find_work` scan per ~50–300 µs. By then
/// all `num_threads` chunks are claimed and the stealable tree subtrees
/// live only µs-scale windows (the spin-phase workers drain them at µs
/// cadence), so a slow-cadence arrival never catches one. Leftover
/// injector chunks, unlike deque subtrees, persist until popped — the one
/// work source a latecomer can always find. Measured (32 logical / 16
/// physical cores, zstd_shape, same-binary A/B): heavy-tail −7…−12 pt vs
/// rayon (ahead), uniform −2…−3 pt, capped ±1 pt; cheap skewed/log-uniform
/// n=200/5000 pay +2…+7 % but stay >2× ahead of rayon — the same
/// trade-off shape (and much smaller cost) as `UNBALANCED_OVERSPLIT` 8→32.
const UNBALANCED_CHUNK_SLACK: usize = 8;

/// Wide-tier slack for batches whose wide-tier chunks still hold enough
/// items ([`UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK`]): heavier tails also
/// suffer plain boundary luck — which fixed chunk the rare 2 MB items land
/// in decides the straggler, and more, smaller chunks scatter that luck.
/// Measured (same session as above, 6-seed cross-check, n=8000 =
/// 200→167 items/chunk): worst seed +6.9 %→+1.3 % vs rayon, mean
/// +1.1 %→−1.1 %, spread 14.6 pt→4.2 pt; n=4000 all three shapes improve
/// (heavy-tail mean −2.5 pt, uniform −2 pt). n=2000 (≈42 items/chunk)
/// first measured +2…+4 pt against the wide tier; a 6-seed re-scan found
/// it neutral instead (see [`UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK`]) —
/// that regression was seed luck. Per-chunk overhead does bite further
/// down (flat 32/64 below), hence the two tiers.
///
/// Rejected alongside: dropping the default to the 16 physical cores
/// (SMT gives zstd ~1.9×; 16 threads lose +78…+89 % wall time — see the
/// zstd_shape `threads` mode) and flat slack 32/64 (uniform n=2000 +3 %).
const UNBALANCED_CHUNK_SLACK_WIDE: usize = 16;

/// Items per wide-tier chunk (`n / (num_threads + WIDE)`) required to
/// upgrade from [`UNBALANCED_CHUNK_SLACK`] to [`UNBALANCED_CHUNK_SLACK_WIDE`].
/// Boundary scan (2026-09, criterion zstd_shape grid n=2000/3000/4000 =
/// 42/63/85 items per chunk, 6 seeds × 3 interleaved same-binary rounds of
/// flat `YOUPIPE_CHUNK_SLACK` 8/16, drift-cancelled by pairing youpipe with
/// the same pass's rayon id): 42/chunk is neutral (median −1.2…+0.5 %,
/// 3–5 of 6 seeds — below the adoption bar, stays narrow), 63/chunk favors
/// wide on every shape (heavy-tail −3.0 %, capped −1.4 %, uniform −0.3 %).
/// 48 sits between the neutral and the wide-favored point; every cheap-side
/// family (cpu_unbalanced n=200/5000, fused 200/1000) keeps its previous
/// tier bit-for-bit.
const UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK: usize = 48;

/// Adaptive [`UNBALANCED_CHUNK_SLACK`]: the wide tier once wide-tier chunks
/// still hold ≥ [`UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK`] items. `n` comes from
/// the terminal's item count — chunk boundary luck only pays once chunks
/// stay coarse enough for per-chunk overhead to amortize.
fn unbalanced_chunk_slack(n: usize, num_threads: usize) -> usize {
    static FACTOR: OnceLock<Option<usize>> = OnceLock::new();
    let factor = FACTOR.get_or_init(|| {
        std::env::var("YOUPIPE_CHUNK_SLACK")
            .ok()
            .and_then(|v| v.parse().ok())
    });
    if let Some(v) = factor {
        return *v;
    }
    let wide = num_threads.saturating_add(UNBALANCED_CHUNK_SLACK_WIDE);
    if n / wide.max(1) >= UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK {
        UNBALANCED_CHUNK_SLACK_WIDE
    } else {
        UNBALANCED_CHUNK_SLACK
    }
}

/// Per-call dispatch plan derived once from the workload kind.
///
/// `depth` is the fork/join split budget ([`split_depth`]); `chunk_slack` is
/// the extra top-level chunk count for the hybrid dispatcher
/// ([`UNBALANCED_CHUNK_SLACK`], zero unless `Workload::Unbalanced`).
/// Bundling both keeps the terminal → dispatcher signatures workload-typed
/// instead of threading loose `usize`s.
#[derive(Clone, Copy)]
struct SplitPlan {
    depth: usize,
    chunk_slack: usize,
}

impl SplitPlan {
    fn new(n: usize, num_threads: usize, workload: Workload) -> Self {
        let oversplit = workload_oversplit(n, num_threads, workload);
        Self {
            depth: split_depth(n, num_threads, oversplit),
            chunk_slack: match workload {
                Workload::Unbalanced => unbalanced_chunk_slack(n, num_threads),
                Workload::Balanced | Workload::Custom(_) => 0,
            },
        }
    }
}

// ── Filter-chain parallel collect ──
//
// Chains containing `Filter` cannot use the index-based core (output
// cardinality is unknown up front), so each leaf produces its own `Vec` and
// the tree concatenates. The ranges are disjoint indices into a shared
// `Slots` input — the replacement for the old `Vec::split_off` merge, which
// paid one allocation + memcpy per internal node.
//
// Filter chains deliberately stay on the single tree even for off-pool
// callers (unlike the no-filter terminals' hybrid dispatch). A/B (2026-09,
// 3 interleaved rounds, 32-core, the `sync_filter` bench): the hybrid
// variant — `num_threads` chunks injected flat, each publishing its `Vec`
// into a shared mutex slot, the driver sorting + concatenating after the
// latch wait — measured a stable +6.5 % at 10 k and +3.3 % at 1 k (100 k:
// noise). The tree concatenates level by level *in parallel* as workers
// finish, while the hybrid funnels every chunk `Vec` through one mutex and
// merges them all on the single driver thread; that structural merge cost
// outweighs the ramp-up win for every batch size measured.

/// RAII guard that drops the unread input tail on unwind — shared by the
/// infallible ([`filter_leaf`]) and fallible ([`filter_try_leaf`]) filter
/// leaves. The output half is elided because each leaf's `Vec` drops itself.
///
/// `pos` tracks the consumed-iteration count: items `..pos` were moved out
/// of their slots (uninit), item `pos` was moved into the stage chain and is
/// gone with it, `input[pos+1..]` is still init and must be dropped.
struct FilterGuard<'a, T> {
    input: &'a [T],
    pos: usize,
}

impl<T> FilterGuard<'_, T> {
    /// Drop the still-init tail `input[pos+1..]`. Runs on both the unwind
    /// path (`Drop`) and the fallible leaf's `Err` short-circuit — item `pos`
    /// was consumed by the stage call either way.
    ///
    /// # Safety
    ///
    /// `pos` must reflect the leaf's iteration counter at the call point;
    /// every slot in `(pos, len)` must hold a live `T`.
    unsafe fn drop_tail(&self) {
        let in_live = self.input.as_ptr();
        // SAFETY: see the contract above; `pos` items were `ptr::read` out.
        unsafe {
            for j in (self.pos + 1)..self.input.len() {
                ptr::drop_in_place(in_live.add(j).cast_mut());
            }
        }
    }
}

impl<T> Drop for FilterGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: `pos` reflects the consumed-iteration count at the unwind
        // point.
        unsafe { self.drop_tail() };
    }
}

/// Consume `input` sequentially, applying `stages` and collecting surviving
/// outputs into a fresh `Vec`.
///
/// Panic safety: a [`FilterGuard`] drops the unread input tail on unwind
/// (there is no shared output buffer; the leaf's `Vec` drops naturally).
fn filter_leaf<T, S>(input: &[T], stages: &S) -> Vec<S::Output>
where
    T: Send,
    S: FusedStage<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    let mut out = Vec::new();
    let mut g = FilterGuard { input, pos: 0 };

    while g.pos < n {
        let i = g.pos;
        // SAFETY: disjoint index; slot i is init. The read moves the item out
        // of the slot, leaving it uninit — never re-read.
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        if let Some(o) = stages.apply(item) {
            out.push(o);
        }
        g.pos = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    out
}

/// Recursive range-based filter collect. Each leaf claims the disjoint range
/// `[start, end)` and produces its own `Vec`; internal nodes concatenate.
///
/// On panic the unwinding side's guard drops its unread input tail and every
/// partial `Vec` drops naturally, so internal nodes need no cleanup match
/// (unlike the index-based core's shared output buffer).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_filter_rec<T, S>(
    pool: &ComputePool,
    input: &Slots<T>,
    start: usize,
    end: usize,
    stages: &S,
    splits_left: usize,
) -> Vec<S::Output>
where
    T: Send,
    S: FusedStage<T> + Sync,
    S::Output: Send,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        return filter_leaf(in_slice, stages);
    }
    let mid = start + (end - start) / 2;
    let (mut l, r) = pool.join(
        || par_filter_rec(pool, input, start, mid, stages, splits_left - 1),
        || par_filter_rec(pool, input, mid, end, stages, splits_left - 1),
    );
    l.extend(r);
    l
}

/// Drive the filter-chain collect over an owned batch with the single
/// range-based tree. Off-pool callers enter the pool through the first
/// `join`'s injection — the same entry the old `Vec::split_off` tree used
/// (see the section comment for why filter chains do not take the hybrid
/// dispatcher).
///
/// A/B vs the old `Vec::split_off` tree (5 interleaved rounds, 32-core,
/// `sync_filter/youpipe_filter_map_owned`): 100 k −6…−9 % (stable — every
/// round of the range tree beat every round of the split_off tree), 1 k/10 k
/// within round spread (+1…+2 %, rounds interleave). The split_off merge paid
/// one allocation + memcpy per internal node (~n·levels/2 item moves across
/// the tree); the range tree moves each surviving item exactly once, into its
/// leaf's `Vec`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn fused_filter_collect<T, S>(
    items: Vec<T>,
    stages: &S,
    splits: usize,
    pool: &ComputePool,
) -> Vec<S::Output>
where
    T: Send,
    S: FusedStage<T> + Sync,
    S::Output: Send,
{
    let n = items.len();
    debug_assert!(n > 0);
    let input = Slots::from_vec(items);
    let out = par_filter_rec(pool, &input, 0, n, stages, splits);
    // Input fully consumed (every slot read → uninit): freeing just drops
    // the buffer.
    drop(input);
    out
}

/// Fallible counterpart of [`filter_leaf`]: consume `input` sequentially,
/// applying `stages` (which may filter) and collecting surviving outputs into
/// a fresh `Vec`. Short-circuits on the first `Err`.
///
/// On `Err`, outputs produced so far drop with `out` and the unread input
/// tail drops via [`FilterGuard::drop_tail`] (item `pos` was consumed by
/// `try_apply`); the guard runs the same cleanup on **panic** (unwind).
fn filter_try_leaf<T, S>(input: &[T], stages: &S) -> Result<Vec<S::Output>, S::Error>
where
    T: Send,
    S: FusedTryStage<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    let mut out = Vec::new();
    let mut g = FilterGuard { input, pos: 0 };

    while g.pos < n {
        let i = g.pos;
        // SAFETY: disjoint index; slot i is init. The read moves the item out
        // of the slot, leaving it uninit — never re-read.
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        match stages.try_apply(item) {
            Ok(Some(o)) => out.push(o),
            Ok(None) => {},
            Err(e) => {
                // Short-circuit: same tail cleanup the guard would do on
                // unwind, then disarm it so `Drop` does not double-clean.
                // `out` drops with the return — outputs are discarded on
                // `Err`.
                // SAFETY: `pos == i`; slots `(i, n)` are still init.
                unsafe { g.drop_tail() };
                std::mem::forget(g);
                return Err(e);
            },
        }
        g.pos = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(out)
}

/// Fallible counterpart of [`par_filter_rec`]: recursive range-based
/// collect over a shared [`Slots`] input. Each leaf claims the disjoint range
/// `[start, end)` and produces its own `Vec`; internal nodes concatenate.
/// Short-circuits on the first `Err`.
///
/// On `Err` every input slot is already gone — the failing leaf dropped its
/// unread tail, and `pool.join` waits out the sibling, which either consumed
/// its whole range or dropped its tail the same way — so the root's
/// `drop(input)` needs no per-slot cleanup. Panics propagate through
/// `join`'s `halt_unwinding`/`resume_unwind` with the same property (the
/// unwinding leaf's guard drops its tail; the waited-out sibling's `Vec`
/// drops naturally), mirroring [`par_filter_rec`].
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_filter_try_rec<T, S>(
    pool: &ComputePool,
    input: &Slots<T>,
    start: usize,
    end: usize,
    stages: &S,
    splits_left: usize,
) -> Result<Vec<S::Output>, S::Error>
where
    T: Send,
    S: FusedTryStage<T> + Sync,
    S::Output: Send,
    S::Error: Send,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        return filter_try_leaf(in_slice, stages);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_filter_try_rec(pool, input, start, mid, stages, splits_left - 1),
        || par_filter_try_rec(pool, input, mid, end, stages, splits_left - 1),
    );
    match (l, r) {
        (Ok(mut l), Ok(r)) => {
            l.extend(r);
            Ok(l)
        },
        // The sibling's Ok `Vec` (if any) drops here with the match arm —
        // outputs are discarded once the batch has failed.
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// Drive the fallible filter-chain collect over an owned batch with the
/// single range-based tree — the counterpart of [`fused_filter_collect`] for
/// `MAY_FILTER` `try_collect` chains, which cannot use the index-based fast
/// path (output cardinality is unknown up front).
///
/// Replaced the old `Vec::split_off` tree (one allocation + memcpy per
/// internal node); the range tree moves each surviving item exactly once,
/// into its leaf's `Vec` — the same measured −6…−9 % shape as the infallible
/// port (see [`fused_filter_collect`]).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn fused_try_filter_collect<T, S>(
    items: Vec<T>,
    stages: &S,
    splits: usize,
    pool: &ComputePool,
) -> Result<Vec<S::Output>, S::Error>
where
    T: Send,
    S: FusedTryStage<T> + Sync,
    S::Output: Send,
    S::Error: Send,
{
    let n = items.len();
    debug_assert!(n > 0);
    let input = Slots::from_vec(items);
    let out = par_filter_try_rec(pool, &input, 0, n, stages, splits);
    // Input fully consumed on both arms (success: every slot read → uninit;
    // failure: tails dropped by the leaves/guards): freeing just drops the
    // buffer.
    drop(input);
    out
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
        compute_pool: None,
        oversubscribe: None,
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
    /// Custom compute pool. When `None`, the pipeline runs on
    /// [`ComputePool::global`] (sized to `num_cpus`). When `Some`, all
    /// fork-join work is driven through this pool instead — useful for
    /// oversubscribing threads for blocking-IO sync workloads (e.g.
    /// `ComputePool::new(num_cpus * 2)` to fill CPU gaps during IO stalls).
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor from [`Pipe::with_oversubscribe`]. Resolved to a
    /// transient `ComputePool` at execution time. Ignored when `compute_pool`
    /// is `Some` (explicit pool takes precedence).
    oversubscribe: Option<NonZeroUsize>,
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

    /// Set the compute-pool worker budget — see
    /// [`PipelineConfig::with_compute_workers`]. On the fused path this sizes
    /// the pool: a value that differs from the machine default runs the
    /// terminal on a transient pool of exactly this many threads, and
    /// [`Pipe::with_oversubscribe`] multiplies it (`budget × factor`). An
    /// explicit [`Pipe::with_compute_pool`] always takes precedence and the
    /// budget is ignored. For repeated terminal calls prefer pre-creating a
    /// pool once (transient pools pay a per-call ~ms construction cost).
    /// Streaming knobs (`buffer_size`, `io_concurrency`, …) have no effect on
    /// the fused path.
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Attach a custom [`ComputePool`] for the fused pipeline's fork-join work.
    /// When omitted, the pipeline runs on [`ComputePool::global`] (sized to
    /// `num_cpus`).
    ///
    /// The primary use case is **oversubscribing threads for blocking-IO sync
    /// workloads**. The global pool has one thread per core, so when a leaf
    /// task blocks on a syscall (file IO, network, etc.) its core sits idle
    /// with no stealable work to fill the gap. A larger pool (e.g.
    /// `ComputePool::new(num_cpus * 2)`) lets other threads use those idle
    /// cores for CPU work (crypto, compression, etc.) while blocked threads
    /// wait — the same technique that `tokio::spawn_blocking` and
    /// [`crate::StreamPipe::with_compute_pool`] use.
    ///
    /// `ComputePool` is cheap to clone (`Arc` + one atomic), so the pool can
    /// be created once and reused across many `collect()` / `for_each()`
    /// calls — important for tight loops where per-call pool construction
    /// (~ms) would dominate.
    ///
    /// (Pool sizes like 128 fit blocking-IO oversubscription; kept small
    /// here so the example also runs under miri's single emulated worker.)
    ///
    /// ```rust
    /// use youpipe::{ComputePool, pipe};
    ///
    /// let pool = ComputePool::new(4);
    /// let result: Vec<i32> = pipe(0..100)
    ///     .with_compute_pool(pool)
    ///     .map(|x: i32| x + 1)
    ///     .collect();
    /// ```
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Oversubscribe the compute pool by `factor` for **blocking-IO sync
    /// workloads** — a convenience that internally creates a pool with
    /// `factor × num_cpus` threads at execution time, so you don't have to
    /// call [`ComputePool::new`] and [`Pipe::with_compute_pool`] yourself.
    ///
    /// # When to use this
    ///
    /// The default pool (one thread per core) is optimal for **CPU-bound**
    /// work. But when each leaf blocks on a syscall — file IO, network, locks
    /// — the blocked thread's core sits idle with no stealable work to fill
    /// the gap (all remaining leaves are held by other blocked workers).
    /// Wall time then exceeds rayon despite youpipe's better per-CPU
    /// efficiency, simply because cores aren't saturated.
    ///
    /// An oversubscribed pool (`factor = 2` → 2× threads) lets other threads
    /// use those idle cores for CPU work (crypto, compression, …) while
    /// blocked threads wait. This is the same technique tokio's
    /// `spawn_blocking` pool and [`crate::StreamPipe::with_compute_pool`] use.
    ///
    /// # When NOT to use this
    ///
    /// **Do not** use this for pure-CPU workloads (in-memory transforms,
    /// number-crunching, no syscalls in the hot loop). Extra threads beyond
    /// the core count only add context-switch overhead, cache thrashing, and
    /// work-stealing contention — measured 10–30 % regression on the
    /// `sync_vs_rayon` CPU benchmarks. The default (no oversubscription) is
    /// already optimal for that case.
    ///
    /// # Choosing a factor
    ///
    /// | Workload shape | Recommended `factor` |
    /// |----------------|----------------------|
    /// | CPU + fast IO (NVMe, page cache) | 1 (no benefit) |
    /// | CPU + slow IO (HDD, cold reads) | 2–3 |
    /// | CPU + network / lock contention | 3–4 |
    /// | Mostly IO, light CPU | 4–8 |
    ///
    /// Start with `2`; if wall time is still dominated by idle cores (visible
    /// as `User ≪ wall × cores` in `time`), increase. Diminishing returns
    /// set in quickly once the IO bandwidth itself becomes the bottleneck.
    ///
    /// # `with_oversubscribe` vs `with_compute_pool`
    ///
    /// `with_oversubscribe(factor)` creates a **transient** pool at
    /// `.collect()` / `.for_each()` time and drops it when the terminal
    /// returns. That is fine for a one-shot pipeline, but in a tight loop the
    /// per-call pool construction (~ms for thread spawn + priming) dominates.
    /// For repeated calls, pre-create the pool and use
    /// [`Pipe::with_compute_pool`]:
    ///
    /// ```rust
    /// use youpipe::{ComputePool, pipe};
    ///
    /// // Pre-create once; clone is cheap (Arc + one atomic).
    /// // (Size 4 keeps the example runnable under miri; real blocking-IO
    /// // pools want 128+ threads.)
    /// let pool = ComputePool::new(4);
    /// for batch in std::iter::repeat_with(|| vec![0u64; 1000]).take(20) {
    ///     pipe(batch)
    ///         .with_compute_pool(pool.clone())
    ///         .map(|x: u64| x + 1)
    ///         .for_each(|_| ());
    /// }
    /// ```
    ///
    /// If both `with_compute_pool` and `with_oversubscribe` are set, the
    /// explicit pool wins and the factor is ignored.
    ///
    /// # Example
    ///
    /// ```rust
    /// use youpipe::pipe;
    ///
    /// // Each item does blocking IO (file read + crypto + write).
    /// // factor = 2 → 2× num_cpus threads fill IO-stall gaps with CPU work.
    /// let files: Vec<String> = (0..100).map(|i| format!("file{i}")).collect();
    /// pipe(files).with_oversubscribe(2).for_each(|f: String| {
    ///     // read(&f) → encrypt → write(out)
    ///     let _ = f;
    /// });
    /// ```
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = NonZeroUsize::new(factor.max(1));
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return Vec::new();
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            // Trivial case (n == 1 or single-threaded pool): skip the pool
            // entirely. Dispatch on `MAY_FILTER` so the pure path matches a
            // hand-written `iter().map().collect()` — no `Option` wrapper.
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

        // `oversplit` = tasks-per-worker for the fork/join tree. Adaptive:
        // small batches (≤ `LOW_OVERSPLIT_ITEMS_PER_THREAD` per worker) use
        // `1` to minimise join-dispatch overhead; larger batches use
        // `BALANCED_OVERSPLIT` for stealing slack. See `workload_oversplit`.
        let plan = SplitPlan::new(n, num_threads, self.config.workload);

        if S::MAY_FILTER {
            fused_filter_collect(items, &stages, plan.depth, pool)
        } else {
            let op = FusedOp(stages);
            par_index_collect(items, &op, plan, pool)
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
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return;
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            // Trivial case (n == 1 or single-threaded pool): run inline, no
            // output buffer. Dispatch on `MAY_FILTER` to keep the pure path
            // branch-free.
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

        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let op = FusedSink(stages, f);
        par_for_each(items, &op, plan, pool);
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
    /// Custom compute pool — see [`Pipe::with_compute_pool`].
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor — see [`Pipe::with_oversubscribe`].
    oversubscribe: Option<NonZeroUsize>,
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

    /// Set the compute-pool worker budget — see [`Pipe::with_compute_workers`].
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Attach a custom [`ComputePool`] — see [`Pipe::with_compute_pool`].
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Oversubscribe the compute pool — see [`Pipe::with_oversubscribe`] for
    /// the full guidance. Same semantics: creates a transient
    /// `factor × num_cpus` thread pool at `.try_collect()` time.
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = NonZeroUsize::new(factor.max(1));
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
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
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            let mut out = Vec::with_capacity(n);
            for item in items {
                if let Some(o) = stages.try_apply(item)? {
                    out.push(o);
                }
            }
            return Ok(out);
        }

        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        if S::MAY_FILTER {
            fused_try_filter_collect(items, &stages, plan.depth, pool)
        } else {
            // Fast path: no filter → output cardinality == input cardinality.
            // Pre-allocate the output buffer and write at known indices,
            // avoiding the per-split `Vec::split_off` allocations of the
            // merge path.
            let op = FusedTryOp(stages);
            par_index_try_collect(items, &op, plan, pool)
        }
    }
}

// ── Borrowed-input parallel core (zero-materialization) ──
//
// `pipe_ref` drives the same hybrid flat/tree dispatch as the owned path, but
// the input is a shared `&'i [E]` instead of an owned `Slots<E>`: items flow
// through the stage chain as `&E`, the input buffer is never consumed, freed,
// or materialized into a `Vec<&E>` — the borrowed counterpart of rayon's
// `slice::par_iter()`.
//
// Lifetime plumbing: the dispatch input handle is `IN = &'i [E]` (a reference
// TO the slice), so `'i` is carried in the type through `HybridStrategy` /
// `ErasedStrategy` / `ChunkJob` — the trait methods' `&IN` parameter is
// late-bound, and a bare `&[E]` would lose the association with the op's
// `RangeOp<&'i E>` bound (trait parameters are invariant, so no lifetime
// shortening would type-check).
//
// Bound shift vs the owned core: sharing `&[E]` across workers requires
// `E: Sync` (owned moves items across threads, which requires `E: Send`).
//
// Panic-safety simplification is structural: a borrowed input is always init
// and never ours to drop, so the input half of every cleanup guard
// (`LeafGuard` / `TryLeafGuard`) and the whole `ForEachGuard` disappear —
// only output slots need dropping on unwind.

/// Borrowed-input leaf: process `input` sequentially, applying `op` to each
/// `&E` and writing outputs by index. Counterpart of [`par_index_leaf`] with
/// the `ptr::read` move-out replaced by a shared borrow — LLVM sees the same
/// read-8B / compute / write-8B loop shape, so the vectorized code matches.
///
/// Panic safety: `RefLeafGuard` drops only the init `output[..written]` slots
/// (a borrowed input is always init and never ours to drop).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_leaf_by_ref<'i, E, R, OP>(input: &'i [E], output: &mut [R], op: &OP, nt: bool)
where
    OP: RangeOp<&'i E, Out = R>,
{
    /// RAII guard that drops the partial **output** range on unwind; the
    /// counterpart of `LeafGuard` with the input half elided. Raw pointers for
    /// the same Tree Borrows reason as `LeafGuard` — see the comment there.
    struct RefLeafGuard<R> {
        out_ptr: *mut R,
        written: usize,
    }

    impl<R> Drop for RefLeafGuard<R> {
        fn drop(&mut self) {
            // SAFETY: `written` reflects the completed-iteration count at the
            // unwind point. `RangeOp` never filters, so `output[..written)` is
            // fully init and must be dropped; the borrowed input needs
            // nothing.
            unsafe {
                for j in 0..self.written {
                    ptr::drop_in_place(self.out_ptr.add(j));
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = RefLeafGuard {
        out_ptr,
        written: 0,
    };

    // Separate NT loop copy — see `par_index_leaf` for why the plain loop
    // stays untouched.
    if nt {
        let _fence = NtFenceOnDrop;
        while g.written < n {
            let i = g.written;
            // SAFETY: same in-place input borrow as the plain loop below;
            // `nt_store`'s alignment/size requirements come from
            // `nt_store_enabled`'s eligibility check.
            let item = unsafe { &*in_ptr.add(i) };
            let out = op.apply(item);
            unsafe { nt_store(out_ptr.add(i), out) };
            g.written = i + 1;
        }
    } else {
        while g.written < n {
            let i = g.written;
            // SAFETY: disjoint index; the input slot is shared and read in place
            // (no move-out, nothing becomes uninit).
            let item = unsafe { &*in_ptr.add(i) };
            let out = op.apply(item);
            unsafe { ptr::write(out_ptr.add(i), out) };
            g.written = i + 1;
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
}

/// Borrowed-input recursive index-based parallel fill — counterpart of
/// [`par_index_rec`]. Each leaf claims a disjoint index range `[start, end)`;
/// a panicking leaf's guard drops its own partial output range, internal
/// nodes propagate the first `Err` and drop the completed sibling's output
/// range. A borrowed input never needs cleanup.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_rec_by_ref<'i, E, R, OP>(
    pool: &ComputePool,
    input: &'i [E],
    output: &Slots<R>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
    nt: bool,
) -> Result<(), PanicPayload>
where
    E: Sync,
    R: Send,
    OP: RangeOp<&'i E, Out = R>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively. `input[start..end)` is shared and init;
        // `output[start..end)` is uninit.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let out_slice = unsafe { output.as_mut_slice(start, end) };
        par_index_leaf_by_ref(in_slice, out_slice, op, nt);
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_index_rec_by_ref(pool, input, output, start, mid, op, splits_left - 1, nt),
        || par_index_rec_by_ref(pool, input, output, mid, end, op, splits_left - 1, nt),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(p), Ok(())) => {
            // SAFETY: right sibling completed without filter (RangeOp never
            // filters), so [mid, end) is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(p)
        },
        (Ok(()), Err(p)) => {
            unsafe { output.drop_range(start, mid) };
            Err(p)
        },
        (Err(p), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(p)
        },
    }
}

/// Hybrid strategy for the borrowed-input `.collect()` — counterpart of
/// [`CollectStrategy`] over the `&'i [E]` input handle.
struct CollectByRefStrategy<'a, R, OP> {
    output: &'a Slots<R>,
    op: &'a OP,
    /// Whole-batch NT decision — see [`CollectStrategy::nt`].
    nt: bool,
}

impl<'i, E, R, OP> HybridStrategy<&'i [E]> for CollectByRefStrategy<'_, R, OP>
where
    E: Sync,
    R: Send,
    OP: RangeOp<&'i E, Out = R>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &&'i [E],
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        par_index_rec_by_ref(
            pool,
            input,
            self.output,
            start,
            end,
            self.op,
            splits,
            self.nt,
        )
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &&'i [E],
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the caller (driver or leaf) owns
        // `[start, end)` exclusively. Input is shared + init; output uninit.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let out_slice = unsafe { self.output.as_mut_slice(start, end) };
        par_index_leaf_by_ref(in_slice, out_slice, self.op, self.nt);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize) {
        // SAFETY: caller guarantees `run_chunk` returned `Ok(())` for
        // `[start, end)`, so those output slots are fully init and safe to
        // drop.
        unsafe { self.output.drop_range(start, end) };
    }
}

/// Drive `par_index_rec_by_ref` over a borrowed `&'i [E]` and convert the
/// output buffer into a `Vec<R>`. Counterpart of [`par_index_collect`] — no
/// input `Slots` is created (nothing to free on any path), the input is only
/// read.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_collect_by_ref<'i, E, R, OP>(
    input: &'i [E],
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Vec<R>
where
    E: Sync,
    R: Send,
    OP: RangeOp<&'i E, Out = R>,
{
    let n = input.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let output = Slots::<R>::uninit(n);

    // Same hybrid dispatch as the owned path — see `par_index_collect`
    // (on-pool callers included, via the `Stealing` latch).
    let strategy = CollectByRefStrategy {
        output: &output,
        op,
        nt: nt_store_enabled::<R>(n),
    };
    // The dispatcher's input handle is the slice reference itself.
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err();
    if let Some(f) = result {
        // Recursion already dropped every live output slot; freeing the
        // buffer is safe. The borrowed input needs nothing.
        drop(output);
        resume_panic(f);
    }
    output.into_vec()
}

/// Borrowed-input sink leaf — counterpart of [`par_for_each_leaf`] with **no
/// cleanup guard at all**: the input is shared (never consumed) and no output
/// exists, so a panic in `op` leaves nothing to clean in this leaf.
fn par_for_each_leaf_by_ref<'i, E, OP>(input: &'i [E], op: &OP)
where
    OP: SinkOp<&'i E>,
{
    for item in input {
        op.consume(item);
    }
}

/// Borrowed-input recursive sink — counterpart of [`par_for_each_rec`]. A
/// panicking leaf leaves no partial state, so siblings need no cleanup.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_for_each_rec_by_ref<'i, E, OP>(
    pool: &ComputePool,
    input: &'i [E],
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
) -> Result<(), PanicPayload>
where
    E: Sync,
    OP: SinkOp<&'i E>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        par_for_each_leaf_by_ref(in_slice, op);
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_for_each_rec_by_ref(pool, input, start, mid, op, splits_left - 1),
        || par_for_each_rec_by_ref(pool, input, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(p), _) | (_, Err(p)) => Err(p),
    }
}

/// Hybrid strategy for the borrowed-input `.for_each()` — counterpart of
/// [`SinkStrategy`] over the `&'i [E]` input handle. Sink-only: no output
/// buffer, nothing to clean on any path.
struct SinkByRefStrategy<'a, OP> {
    op: &'a OP,
}

impl<'i, E, OP> HybridStrategy<&'i [E]> for SinkByRefStrategy<'_, OP>
where
    E: Sync,
    OP: SinkOp<&'i E>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &&'i [E],
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        par_for_each_rec_by_ref(pool, input, start, end, self.op, splits)
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &&'i [E],
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the caller (driver) owns `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        par_for_each_leaf_by_ref(in_slice, self.op);
        Ok(())
    }

    #[inline]
    /// # Safety
    ///
    /// Nothing to drop for a sink-only strategy.
    unsafe fn cleanup_success_chunk(&self, _start: usize, _end: usize) {
        // No-op: sink-only, nothing to clean (mirrors `SinkStrategy`).
    }
}

/// Drive `par_for_each_rec_by_ref` over a borrowed `&'i [E]`. Counterpart of
/// [`par_for_each`].
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_for_each_by_ref<'i, E, OP>(input: &'i [E], op: &OP, plan: SplitPlan, pool: &ComputePool)
where
    E: Sync,
    OP: SinkOp<&'i E>,
{
    let n = input.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();

    // Same hybrid dispatch as the owned path — see `par_for_each` (on-pool
    // callers included, via the `Stealing` latch).
    let strategy = SinkByRefStrategy { op };
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err();
    if let Some(f) = result {
        resume_panic(f);
    }
}

/// Borrowed-input fallible leaf — counterpart of [`par_index_try_leaf`] with
/// the input half of every cleanup path elided (borrowed input is always
/// init). On `Err`: drops `output[..written]`, disarms the guard, returns.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_try_leaf_by_ref<'i, E, R, F, OP>(
    input: &'i [E],
    output: &mut [R],
    op: &OP,
) -> Result<(), F>
where
    OP: RangeTryOp<&'i E, Out = R, Error = F>,
{
    /// RAII guard mirroring `RefLeafGuard` for the fallible leaf — drops the
    /// init `output[..written]` slots on unwind only. Raw pointers for the
    /// same Tree Borrows reason as `LeafGuard`.
    struct TryRefLeafGuard<R> {
        out_ptr: *mut R,
        written: usize,
    }

    impl<R> Drop for TryRefLeafGuard<R> {
        fn drop(&mut self) {
            // SAFETY: `written` reflects completed iterations at the unwind
            // point; the borrowed input needs nothing.
            unsafe {
                for j in 0..self.written {
                    ptr::drop_in_place(self.out_ptr.add(j));
                }
            }
        }
    }

    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = TryRefLeafGuard {
        out_ptr,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; the input slot is shared and read in place.
        let item = unsafe { &*in_ptr.add(i) };
        match op.try_apply(item) {
            Ok(out) => {
                unsafe { ptr::write(out_ptr.add(i), out) };
                g.written = i + 1;
            },
            Err(e) => {
                // Error path: run the same output cleanup the guard would do
                // on panic, then disarm (forget) so Drop doesn't double-clean.
                unsafe {
                    for j in 0..i {
                        ptr::drop_in_place(out_ptr.add(j));
                    }
                }
                std::mem::forget(g);
                return Err(e);
            },
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(())
}

/// Borrowed-input fallible recursion — counterpart of [`par_index_try_rec`].
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_try_rec_by_ref<'i, E, R, F, OP>(
    pool: &ComputePool,
    input: &'i [E],
    output: &Slots<R>,
    start: usize,
    end: usize,
    op: &OP,
    splits_left: usize,
) -> Result<(), F>
where
    E: Sync,
    R: Send,
    F: Send,
    OP: RangeTryOp<&'i E, Out = R, Error = F>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively. Input shared + init; output uninit.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let out_slice = unsafe { output.as_mut_slice(start, end) };
        par_index_try_leaf_by_ref(in_slice, out_slice, op)?;
        return Ok(());
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_index_try_rec_by_ref(pool, input, output, start, mid, op, splits_left - 1),
        || par_index_try_rec_by_ref(pool, input, output, mid, end, op, splits_left - 1),
    );
    match (l, r) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) => {
            // SAFETY: right sibling completed without filter, so [mid, end)
            // is fully init and safe to drop.
            unsafe { output.drop_range(mid, end) };
            Err(e)
        },
        (Ok(()), Err(e)) => {
            unsafe { output.drop_range(start, mid) };
            Err(e)
        },
        (Err(e), Err(_)) => {
            unsafe {
                output.drop_range(start, mid);
                output.drop_range(mid, end);
            }
            Err(e)
        },
    }
}

/// Hybrid strategy for the borrowed-input `.try_collect()` fast path —
/// counterpart of [`TryStrategy`] over the `&'i [E]` input handle.
struct TryByRefStrategy<'a, R, E, OP> {
    output: &'a Slots<R>,
    op: &'a OP,
    _marker: PhantomData<fn(E)>,
}

impl<'i, E, R, F, OP> HybridStrategy<&'i [E]> for TryByRefStrategy<'_, R, F, OP>
where
    E: Sync,
    R: Send,
    F: Send + 'static,
    OP: RangeTryOp<&'i E, Out = R, Error = F>,
{
    type Failure = TryFailure<F>;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &&'i [E],
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), TryFailure<F>> {
        par_index_try_rec_by_ref(pool, input, self.output, start, end, self.op, splits)
            .map_err(TryFailure::Error)
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &&'i [E],
        start: usize,
        end: usize,
    ) -> Result<(), TryFailure<F>> {
        // SAFETY: disjoint range — the caller (driver) owns `[start, end)`
        // exclusively. Input shared + init; output uninit.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let out_slice = unsafe { self.output.as_mut_slice(start, end) };
        par_index_try_leaf_by_ref(in_slice, out_slice, self.op).map_err(TryFailure::Error)
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize) {
        // SAFETY: caller guarantees the chunk returned `Ok(())`, so those
        // output slots are fully init and safe to drop.
        unsafe { self.output.drop_range(start, end) };
    }
}

/// Drive `par_index_try_rec_by_ref` over a borrowed `&'i [E]`. Counterpart of
/// [`par_index_try_collect`].
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_try_collect_by_ref<'i, E, R, F, OP>(
    input: &'i [E],
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Result<Vec<R>, F>
where
    E: Sync,
    R: Send,
    F: Send + 'static,
    OP: RangeTryOp<&'i E, Out = R, Error = F>,
{
    let n = input.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let output = Slots::<R>::uninit(n);

    // Same hybrid dispatch as the owned path — see `par_index_try_collect`
    // (on-pool callers included, via the `Stealing` latch).
    let strategy = TryByRefStrategy {
        output: &output,
        op,
        _marker: PhantomData,
    };
    let result = hybrid_dispatch(
        pool,
        &input,
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err()
    .map(|f| match f {
        ErasedFailure::Op(b) => match b.downcast::<TryFailure<F>>() {
            Ok(tf) => *tf,
            Err(_) => unreachable!("try strategy only records TryFailure<F>"),
        },
        ErasedFailure::Panic(p) => TryFailure::Panic(p),
    });
    match result {
        None => Ok(output.into_vec()),
        Some(TryFailure::Error(e)) => {
            // Recursion already dropped every live output slot.
            drop(output);
            Err(e)
        },
        Some(TryFailure::Panic(p)) => {
            // Mirrors the owned path: a panic unwinds past the buffer
            // management (init slots may leak, documented there).
            drop(output);
            panic::resume_unwind(p);
        },
    }
}

/// Borrowed-input merge-based collect for fused stages that may filter —
/// counterpart of [`join_fused_collect`] with `Vec::split_off` replaced by
/// index ranges (no per-level reallocation; the input is shared).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn join_fused_collect_by_ref<'i, S, E>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    start: usize,
    end: usize,
    splits_left: usize,
) -> Vec<S::Output>
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    if splits_left == 0 || end - start <= 1 {
        return input[start..end]
            .iter()
            .filter_map(|item| stages.apply(item))
            .collect();
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || join_fused_collect_by_ref(pool, input, stages, start, mid, splits_left - 1),
        || join_fused_collect_by_ref(pool, input, stages, mid, end, splits_left - 1),
    );
    let mut result = l;
    result.extend(r);
    result
}

/// Borrowed-input merge-based collect for fallible fused stages — counterpart
/// of [`fused_try_filter_collect`] over index ranges (the input is a shared
/// slice, so no `Slots` reinterpretation is needed).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn join_fused_try_collect_by_ref<'i, S, E, F>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    start: usize,
    end: usize,
    splits_left: usize,
) -> Result<Vec<S::Output>, F>
where
    S: FusedTryStage<&'i E, Error = F> + Sync,
    E: Sync,
    S::Output: Send,
    F: Send,
{
    if splits_left == 0 || end - start <= 1 {
        let mut out = Vec::with_capacity(end - start);
        for item in &input[start..end] {
            if let Some(o) = stages.try_apply(item)? {
                out.push(o);
            }
        }
        return Ok(out);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || join_fused_try_collect_by_ref(pool, input, stages, start, mid, splits_left - 1),
        || join_fused_try_collect_by_ref(pool, input, stages, mid, end, splits_left - 1),
    );
    match (l, r) {
        (Ok(mut l), Ok(r)) => {
            l.extend(r);
            Ok(l)
        },
        (Err(e), _) | (_, Err(e)) => Err(e),
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
    pool: &ComputePool,
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
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
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
    let plan = SplitPlan::new(n, num_threads, workload);
    if S::MAY_FILTER {
        fused_filter_collect(items, &stages, plan.depth, pool)
    } else {
        let op = FusedOp(stages);
        par_index_collect(items, &op, plan, pool)
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
pub(crate) fn fused_for_each_scoped<S, T, F>(
    items: Vec<T>,
    stages: S,
    f: F,
    workload: Workload,
    pool: &ComputePool,
) where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
    F: Fn(S::Output) + Sync,
{
    let n = items.len();
    if n == 0 {
        return;
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
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
    let plan = SplitPlan::new(n, num_threads, workload);
    let op = FusedSink(stages, f);
    par_for_each(items, &op, plan, pool);
}

/// `pub(crate)` entry point for the scoped fallible terminal
/// (`ScopedTryPipe::try_collect`). Identical dispatch logic to
/// `TryPipe::try_collect` but without `'static` bounds on the stage chain —
/// the closure/stage lifetime is `'env`.
///
/// `E` still requires `'static`: the fast (no-filter) path routes through the
/// hybrid dispatcher, whose panic/failure payloads are type-erased
/// `Box<dyn Any>` and downcast back by concrete type. Error types that borrow
/// scope-local data therefore cannot use the scoped fallible path — in
/// practice fallible closures borrow *inputs* (`'env` on the closure) while
/// the error is an owned/`'static` type, so this rarely bites.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn fused_try_collect_scoped<S, T, E>(
    items: Vec<T>,
    stages: S,
    workload: Workload,
    pool: &ComputePool,
) -> Result<Vec<S::Output>, E>
where
    S: FusedTryStage<T, Error = E> + Sync,
    T: Send,
    S::Output: Send,
    E: Send + 'static,
{
    let n = items.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        let mut out = Vec::with_capacity(n);
        for item in items {
            if let Some(o) = stages.try_apply(item)? {
                out.push(o);
            }
        }
        return Ok(out);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    if S::MAY_FILTER {
        fused_try_filter_collect(items, &stages, plan.depth, pool)
    } else {
        let op = FusedTryOp(stages);
        par_index_try_collect(items, &op, plan, pool)
    }
}

// ── pub(super) borrowed-input entry points ──
//
// Driven by `crate::builder::PipeRef` (`pipe_ref`). Identical dispatch logic
// to `Pipe::collect` / `Pipe::for_each` / `TryPipe::try_collect`, but the
// input is a shared `&'i [E]` — see the borrowed-core section comment above
// for the lifetime plumbing and the `E: Sync` bound shift.

/// Entry point for the borrowed-input `.collect()`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) fn fused_collect_by_ref<'i, S, E>(
    input: &'i [E],
    stages: S,
    workload: Workload,
    pool: &ComputePool,
) -> Vec<S::Output>
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    let n = input.len();
    if n == 0 {
        return Vec::new();
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        if S::MAY_FILTER {
            return input.iter().filter_map(|item| stages.apply(item)).collect();
        }
        return input.iter().map(|item| stages.apply_pure(item)).collect();
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    if S::MAY_FILTER {
        join_fused_collect_by_ref(pool, input, &stages, 0, n, plan.depth)
    } else {
        let op = FusedOp(stages);
        par_index_collect_by_ref(input, &op, plan, pool)
    }
}

/// Entry point for the borrowed-input `.for_each()`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) fn fused_for_each_by_ref<'i, S, E, F>(
    input: &'i [E],
    stages: S,
    f: F,
    workload: Workload,
    pool: &ComputePool,
) where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
    F: Fn(S::Output) + Sync,
{
    let n = input.len();
    if n == 0 {
        return;
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        if S::MAY_FILTER {
            for item in input {
                if let Some(o) = stages.apply(item) {
                    f(o);
                }
            }
        } else {
            for item in input {
                let o = stages.apply_pure(item);
                f(o);
            }
        }
        return;
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let op = FusedSink(stages, f);
    par_for_each_by_ref(input, &op, plan, pool);
}

/// Entry point for the borrowed-input `.try_collect()`. `E` (the error type)
/// still requires `'static`: the fast path routes failures through the hybrid
/// dispatcher's type-erased slots, which downcast by concrete type — same
/// caveat as `fused_try_collect_scoped`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) fn fused_try_collect_by_ref<'i, S, E, F>(
    input: &'i [E],
    stages: S,
    workload: Workload,
    pool: &ComputePool,
) -> Result<Vec<S::Output>, F>
where
    S: FusedTryStage<&'i E, Error = F> + Sync,
    E: Sync,
    S::Output: Send,
    F: Send + 'static,
{
    let n = input.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        let mut out = Vec::with_capacity(n);
        for item in input {
            if let Some(o) = stages.try_apply(item)? {
                out.push(o);
            }
        }
        return Ok(out);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    if S::MAY_FILTER {
        join_fused_try_collect_by_ref(pool, input, &stages, 0, n, plan.depth)
    } else {
        let op = FusedTryOp(stages);
        par_index_try_collect_by_ref(input, &op, plan, pool)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::pipe_ref;

    /// Pool resolution precedence + the compute-workers semantics that the
    /// fused setters promise (an earlier version silently ignored the budget:
    /// `resolve_exec_pool` looked only at the pool handle and the oversubscribe
    /// factor, while the docs claimed the budget sized the pool).
    #[test]
    fn test_resolve_exec_pool_precedence() {
        let ncpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);

        // 1. Explicit pool always wins, regardless of budget/oversubscribe.
        let pool = ComputePool::new(3);
        let exec = resolve_exec_pool(Some(&pool), None, ncpus + 1);
        assert_eq!(exec.as_pool().num_workers(), 3);

        // 2. Oversubscribe multiplies the budget (not the machine default).
        let factor = NonZeroUsize::new(2).unwrap();
        let exec = resolve_exec_pool(None, Some(factor), 3);
        assert_eq!(exec.as_pool().num_workers(), 6);

        // 3. A non-default budget creates a transient pool of exactly n.
        let exec = resolve_exec_pool(None, None, 3);
        assert_eq!(exec.as_pool().num_workers(), 3);

        // 4. The untouched default keeps the shared global pool.
        let exec = resolve_exec_pool(None, None, ncpus);
        assert_eq!(
            exec.as_pool().num_workers(),
            ComputePool::global().num_workers()
        );
    }

    #[test]
    fn test_with_compute_pool_collect() {
        let pool = ComputePool::new(4);
        let result: Vec<i32> = pipe(0..1000)
            .with_compute_pool(pool)
            .map(|x: i32| x * 2)
            .collect();
        let expected: Vec<i32> = (0..1000).map(|x| x * 2).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_with_compute_pool_for_each() {
        let pool = ComputePool::new(4);
        let sum = Arc::new(AtomicUsize::new(0));
        let s = sum.clone();
        pipe(0u32..1000)
            .with_compute_pool(pool)
            .for_each(move |x: u32| {
                s.fetch_add(x as usize, Ordering::Relaxed);
            });
        let expected: usize = (0..1000usize).sum();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }

    #[test]
    fn test_with_compute_pool_filter_chain() {
        let pool = ComputePool::new(4);
        let result: Vec<i32> = pipe(0..1000)
            .with_compute_pool(pool)
            .map(|x: i32| x + 1)
            .filter(|x: &i32| *x % 3 == 0)
            .map(|x: i32| x * 10)
            .collect();
        let expected: Vec<i32> = (1..=1000).filter(|x| x % 3 == 0).map(|x| x * 10).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_with_compute_pool_try_collect() {
        let pool = ComputePool::new(4);
        let result: Result<Vec<i32>, &'static str> = pipe(0..1000)
            .with_compute_pool(pool)
            .try_map(|x: i32| Ok::<i32, &str>(x + 1))
            .try_collect();
        let expected: Vec<i32> = (1..=1000).collect();
        assert_eq!(result.unwrap(), expected);
    }

    /// A 2-thread custom pool must cap concurrency far below the global
    /// pool's thread count — proving the fused path dispatches to the
    /// user-supplied pool, not the global one.
    #[test]
    fn test_with_compute_pool_limits_parallelism() {
        let pool = ComputePool::new(2);
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let m = max_active.clone();

        pipe(0..2000)
            .with_compute_pool(pool)
            .with_workload(Workload::Balanced)
            .for_each(move |x: i32| {
                let cur = a.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(cur, Ordering::SeqCst);
                // Enough work per item to guarantee overlap on a 2-thread pool.
                std::thread::sleep(std::time::Duration::from_micros(50));
                a.fetch_sub(1, Ordering::SeqCst);
                std::hint::black_box(x);
            });

        let max = max_active.load(Ordering::SeqCst);
        // 2 pool workers → at most ~3 concurrent (the off-pool driver may
        // briefly participate via the hybrid path's tree). The global pool
        // (32 threads) would show 20+.
        assert!(
            max <= 4,
            "expected ≤4 concurrent on a 2-thread pool, got {max} — custom pool not used?"
        );
    }

    #[test]
    fn test_with_oversubscribe_collect() {
        let result: Vec<i32> = pipe(0..1000)
            .with_oversubscribe(2)
            .map(|x: i32| x * 3)
            .collect();
        let expected: Vec<i32> = (0..1000).map(|x| x * 3).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_with_oversubscribe_for_each() {
        let sum = Arc::new(AtomicUsize::new(0));
        let s = sum.clone();
        pipe(0u32..1000)
            .with_oversubscribe(2)
            .for_each(move |x: u32| {
                s.fetch_add(x as usize, Ordering::Relaxed);
            });
        let expected: usize = (0..1000usize).sum();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }

    /// `with_compute_pool` takes precedence over `with_oversubscribe`: when
    /// both are set, the explicit pool wins and the factor is ignored.
    #[test]
    fn test_compute_pool_precedence_over_oversubscribe() {
        let pool = ComputePool::new(1);
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let m = max_active.clone();

        pipe(0..500)
            // Both set — the 1-thread pool must win over the factor.
            .with_compute_pool(pool)
            .with_oversubscribe(100)
            .for_each(move |x: i32| {
                let cur = a.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(cur, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_micros(100));
                a.fetch_sub(1, Ordering::SeqCst);
                std::hint::black_box(x);
            });

        let max = max_active.load(Ordering::SeqCst);
        // 1-thread pool → at most 2 concurrent (1 worker + possible driver).
        // If the oversubscribe factor (100× num_cpus) had won, this would
        // be 20+.
        assert!(
            max <= 2,
            "expected ≤2 concurrent on 1-thread pool, got {max} — oversubscribe factor overrode \
             the explicit pool?"
        );
    }

    #[test]
    fn test_with_oversubscribe_try_collect() {
        let result: Result<Vec<i32>, &'static str> = pipe(0..1000)
            .with_oversubscribe(2)
            .try_map(|x: i32| Ok::<i32, &str>(x + 1))
            .try_collect();
        let expected: Vec<i32> = (1..=1000).collect();
        assert_eq!(result.unwrap(), expected);
    }

    /// `nt_store_enabled`'s Auto tier: threshold math and eligibility. Runs
    /// under the default env (unset = Auto); On/Off are process-global
    /// (OnceLock) and stay covered by the horizontal A/B instead.
    #[test]
    fn test_nt_store_auto_decision() {
        // Eligible 8-byte type: below / at / above the whole-batch threshold.
        let t = NT_AUTO_MIN_BYTES / size_of::<u64>();
        assert!(!nt_store_enabled::<u64>(0));
        assert!(!nt_store_enabled::<u64>(t - 1));
        assert!(nt_store_enabled::<u64>(t));
        assert!(nt_store_enabled::<u64>(usize::MAX));
        // Ineligible sizes / alignments never take the NT path.
        assert!(!nt_store_enabled::<u32>(usize::MAX));
        assert!(!nt_store_enabled::<(u64, u64)>(usize::MAX));
    }

    /// A whole-batch output at the Auto threshold actually exercises the NT
    /// leaf path (streaming stores + `sfence` publication) end-to-end and
    /// stays bit-correct, for both the owned and borrowed collect cores.
    /// `not(miri)`: 1M interpreted iterations would dominate the miri run,
    /// and under miri `nt_store` is a plain `ptr::write` anyway.
    #[cfg(not(miri))]
    #[test]
    fn test_collect_nt_auto_threshold_correct() {
        let n = NT_AUTO_MIN_BYTES / size_of::<u64>();
        let data: Vec<u64> = (0..n as u64).collect();
        let f = |x: u64| x.wrapping_mul(3).wrapping_add(1);
        let expected: u64 = (0..n as u64).map(f).sum();

        let owned: Vec<u64> = pipe(data.clone()).map(f).collect();
        assert_eq!(owned.len(), n);
        assert_eq!(owned.iter().copied().sum::<u64>(), expected);

        let borrowed: Vec<u64> = pipe_ref(&data).map(|&x| f(x)).collect();
        assert_eq!(borrowed.iter().copied().sum::<u64>(), expected);
    }
}
