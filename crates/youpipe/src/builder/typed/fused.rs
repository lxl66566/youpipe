// miri has no `movnti` semantics; other arches lack the intrinsics — both
// fall back to plain stores (see `nt_store`).
#[cfg(all(target_arch = "x86_64", not(miri)))]
use core::arch::x86_64::{_mm_sfence, _mm_stream_si64};
use std::{
    any::Any,
    cell::UnsafeCell,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Range,
    panic, ptr,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use super::{
    slots::Slots,
    traits::{
        CountReducer, Filter, FoldReducer, FusedOp, FusedReduce, FusedSink, FusedStage, FusedTryOp,
        FusedTryReduce, FusedTryStage, Identity, InfallibleChain, MapErr, OptionReducer, RangeOp,
        RangeReduce, RangeTryOp, ReduceOp, Reducer, SinkOp, StageMarker, SumReducer, SyncMap,
        TryMap, TryReduceOp,
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
// one at the end of the terminal call parks it in the process-wide
// recycling cache (see `ComputePool::new`) instead of joining the workers,
// so the next same-sized terminal reuses it for an `Arc` clone — tight
// loops no longer pay per-call construction. Pools whose threads must
// really be gone go through `ComputePool::clear_cached_pools` or a
// user-owned `with_compute_pool` handle.

/// The compute pool that a fused terminal (`.collect()` / `.for_each()` / …)
/// drives its fork-join work through.
pub(crate) enum ExecPool<'a> {
    /// A borrowed reference — either the global pool or a user-supplied pool.
    Ref(&'a ComputePool),
    /// A transient pool created from an oversubscribe factor or a non-default
    /// worker budget. Dropped when the terminal returns — through
    /// `ComputePool::new` the drop parks it in the process-wide recycling
    /// cache rather than joining the workers.
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

/// Recursive index-based parallel core — the divide-and-conquer shared by
/// every fused terminal's chunk driver (the former per-terminal
/// `par_*_rec` copies). Each leaf claims a disjoint index range
/// `[start, end)` and runs it through `leaf`; no `split_off`, no `extend`,
/// no per-level reallocation.
///
/// Hooks (passed as closure references, monomorphized per terminal — the
/// leaf loops stay independently inlined so the auto-vectorization argument
/// on `par_index_leaf` keeps holding):
///
/// * `leaf` runs `[start, end)` sequentially on the current thread. Op failures come back as `Err`;
///   panics are captured by the join plumbing (the leaf guard has already dropped the partial state
///   of its own range) and reach the internal-node match below as values.
/// * `drop_success_range` drops the resources a *successfully completed* range holds in shared
///   buffers (the completed sibling's output slots), so the caller can free those buffers without
///   leak or double-drop — the internal-node granularity of
///   [`HybridStrategy::cleanup_success_chunk`]. Only ever invoked for ranges whose subtree returned
///   `Ok(())`; no-op for sink-only terminals.
///
/// Panic safety: a panicking leaf never unwinds past this recursion — it is
/// captured at each join boundary and handed back as `Err` (a self-run B's
/// escape is covered by the unwind-only `SiblingGuard`), so the internal-node
/// match always runs and drops every completed sibling's range via
/// `drop_success_range`. On return, the whole `[start, end)` range is fully
/// resolved: every slot is either init (success path) or dropped, and every
/// input slot is consumed.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_tree_rec<IN: ?Sized + Sync, E, L, D>(
    pool: &ComputePool,
    input: &IN,
    start: usize,
    end: usize,
    splits_left: usize,
    leaf: &L,
    drop_success_range: &D,
) -> Result<(), E>
where
    E: Send + From<PanicPayload>,
    L: Fn(&IN, usize, usize) -> Result<(), E> + Sync,
    D: Fn(usize, usize) + Sync,
{
    if splits_left == 0 || end - start <= 1 {
        return leaf(input, start, end);
    }
    let mid = start + (end - start) / 2;
    // Unwind backstop for the one panic `join_captured` lets escape: a
    // self-run B (see `join_on_captured`'s self-pop branch). Any panic that
    // unwinds past the join call below therefore implies A completed
    // successfully — the guard drops exactly the left range, then lets the
    // unwind continue toward the chunk boundary's `halt_unwinding`. Every
    // other panic (A's, a stolen B's, a leaf's) is captured inside the join
    // machinery and comes back as a value through the match below.
    let g = SiblingGuard {
        start,
        mid,
        drop_success_range,
    };
    let (l, r) = pool.join_captured(
        || {
            par_tree_rec(
                pool,
                input,
                start,
                mid,
                splits_left - 1,
                leaf,
                drop_success_range,
            )
        },
        || {
            par_tree_rec(
                pool,
                input,
                mid,
                end,
                splits_left - 1,
                leaf,
                drop_success_range,
            )
        },
    );
    std::mem::forget(g);
    let l_ok = matches!(l, Ok(Ok(())));
    let r_ok = matches!(r, Ok(Ok(())));
    if l_ok && r_ok {
        return Ok(());
    }
    // SAFETY (hook contract): a non-success branch's range was fully cleaned
    // inside its own recursion (internal-node match + leaf guard), so only
    // the completed siblings' ranges hold live shared resources here (e.g.
    // fully-init output slots — the no-filter cores never leave holes).
    if l_ok {
        drop_success_range(start, mid);
    }
    if r_ok {
        drop_success_range(mid, end);
    }
    // Both branches completed, each with its own failure. A panic outranks
    // an op failure; among equal kinds the left (first) failure wins — the
    // same ordering `ErasedFailure::record` enforces at chunk level.
    Err(match (l, r) {
        (Err(p), _) | (_, Err(p)) => E::from(p),
        (Ok(Err(e)), _) | (Ok(Ok(())), Ok(Err(e))) => e,
        (Ok(Ok(())), Ok(Ok(()))) => unreachable!("handled by the early return above"),
    })
}

/// Unwind-only sibling cleanup for [`par_tree_rec`] (see its comment for the
/// escape-path invariant). Forgotten on every normal return.
struct SiblingGuard<'a, D: Fn(usize, usize)> {
    start: usize,
    mid: usize,
    drop_success_range: &'a D,
}

impl<D: Fn(usize, usize)> Drop for SiblingGuard<'_, D> {
    fn drop(&mut self) {
        // SAFETY (hook contract): the escaping unwind implies the join's A
        // side completed, so `[start, mid)` is fully init and droppable.
        (self.drop_success_range)(self.start, self.mid);
    }
}

/// Generic leaf cleanup guard — the one RAII shape behind the former
/// per-leaf guards (`LeafGuard` / `TryLeafGuard` / `RefLeafGuard` /
/// `TryRefLeafGuard` / `ForEachGuard` / `FilterGuard` / `PlaceLeafGuard` /
/// `GenLeafGuard`). On unwind it drops the init **output prefix**
/// `[0, written)` (when `OUT`) and the still-init **input tail**
/// `(written, n)` (when `IN`); dead halves compile away.
///
/// `written` counts fully completed iterations (read + applied + written).
/// At the panic point in the op for iter `i = written`: `output[..i]` is init
/// (drop when `OUT`), item `i` was moved into the op and is gone with the
/// panic (never dropped by the guard), `input[i+1..]` is still init (drop
/// when `IN`).
///
/// Stores raw pointers (not slices) so `mem::forget(g)` on the success path
/// cannot conflict with the raw-pointer writes under Tree Borrows: a
/// `&mut [R]` field would be disabled by the foreign write through
/// `out_ptr`, making the `forget` access UB. Raw pointers carry no borrow
/// tags, so there is nothing to disable. Dead-half pointers are never
/// dereferenced (the matching const half gates every drop loop).
struct LeafCleanup<T, R, const OUT: bool, const IN: bool> {
    in_ptr: *const T,
    out_ptr: *mut R,
    n: usize,
    written: usize,
}

impl<T, R, const OUT: bool, const IN: bool> LeafCleanup<T, R, OUT, IN> {
    /// Run the cleanup now instead of on unwind — the fallible leaves'
    /// `Err` short-circuit path (item `written` was consumed by the op
    /// either way), after which the caller `mem::forget`s the guard so
    /// `Drop` does not double-clean.
    ///
    /// # Safety
    ///
    /// `written` must be the guard's live iteration counter; every slot in
    /// the enabled halves must hold a live value (`output[..written]` init,
    /// `input(written..n]` init).
    unsafe fn cleanup(&self) {
        let i = self.written;
        // SAFETY: contract above — the same drops the unwind path performs.
        unsafe {
            if OUT {
                for j in 0..i {
                    ptr::drop_in_place(self.out_ptr.add(j));
                }
            }
            if IN {
                for j in (i + 1)..self.n {
                    ptr::drop_in_place(self.in_ptr.add(j).cast_mut());
                }
            }
        }
    }
}

impl<T, R, const OUT: bool, const IN: bool> Drop for LeafCleanup<T, R, OUT, IN> {
    fn drop(&mut self) {
        // SAFETY: `written` reflects the actual completed-iteration count at
        // the unwind point.
        unsafe { self.cleanup() };
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
/// callers (`par_tree_rec`'s `join` / `hybrid_dispatch`'s `CountLatch`).
struct NtFenceOnDrop;

impl Drop for NtFenceOnDrop {
    #[inline]
    fn drop(&mut self) {
        nt_fence();
    }
}

/// Process `[start, end)` sequentially on the current thread.
///
/// Panic safety uses a stack-local [`LeafCleanup`] guard whose `Drop` runs
/// only on unwind. Compared to wrapping the loop in `panic::catch_unwind`,
/// this lets LLVM keep the loop index / written counters in registers when the
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
    // Unwind cleanup via the generic `LeafCleanup` guard: `output[..written]`
    // is init (`RangeOp` never filters, no holes), `input[written+1..]` is
    // still init; item `written` itself is gone with the panic (moved into
    // `op`), so nobody drops it. `consumed == written + 1`, hence the single
    // counter — one store per iteration, letting the vectorizer keep the
    // index in a register (see the guard's Tree Borrows note for why raw
    // pointers, not slices).
    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafCleanup::<T, R, true, true> {
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

/// Drive the index core over `[0, n)` and convert the output buffer into a
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

/// Fused-core entry for the streaming reduce pass-through
/// (`StageSpawn::fuse_exec_reduce`): fold `op` over `items` with `reducer`
/// exactly like `Pipe::reduce` does for a no-filter chain — trivial-batch
/// serial shortcut, then `SplitPlan` + the hybrid-dispatch reduce core.
///
/// Streaming callers reach this only after the pass-through eligibility
/// guards passed (pure `SyncStage` chain, no cancellation, no per-stage
/// pins), same contract as [`fused_pass_collect`].
pub(super) fn fused_pass_reduce<T, R, OP>(
    items: Vec<T>,
    op: &OP,
    reducer: &R,
    workload: Workload,
    pool: &ComputePool,
) -> R::Acc
where
    T: Send,
    OP: RangeOp<T>,
    R: Reducer<OP::Out>,
{
    let n = items.len();
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        // Covers the empty batch too: the reducer's identity IS the fold of
        // zero items.
        let mut acc = reducer.identity();
        for item in items {
            acc = reducer.fold(acc, op.apply(item));
        }
        return acc;
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = RangeReduce(op, reducer);
    par_reduce(items, &rop, plan, pool)
}

// ── Hybrid flat/tree top-level dispatch ──
//
// Hypothesis: the single tree grows parallelism one level at a
// time — the externally-injected top job runs on ONE worker, which runs its A
// inline and pushes B; only after B is stolen does a second worker join, and so
// on. That ramp-up costs ~log2(num_threads) join levels before every worker is
// busy, and is the bulk of the ~120 µs fixed dispatch overhead that dominates
// small/medium batches (notably the 1 K `cpu_heavy` case trailing rayon).
//
// Hybrid injects `num_threads` disjoint top-level chunks into the injector in
// one `inject_batch` (one JEC bump, one wake cascade). Every worker pops a
// chunk on its first `find_work`, so all workers are busy from t≈0. Each chunk
// then builds its own mini-tree via `par_tree_rec`, so within-chunk stealing
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
//   1. The recursive chunk driver — each strategy supplies `par_tree_rec`'s leaf +
//      `drop_success_range` hooks: `collect` writes to a shared output `Slots<R>`, `for_each` is
//      sink-only, `try_collect`'s no-filter fast path short-circuits into a shared error slot.
//   2. The failure cleanup — `collect`/`try_collect` must drop successful chunks' output ranges so
//      the caller can free the buffers; `for_each` has nothing to clean (the failed chunk's leaf
//      guard already dropped its own unread input tail).
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
/// A panic outranks an op failure: the tree recursion hands a panicking leaf
/// back as an `Err` whose payload displaces any op failure recorded earlier
/// (see [`ErasedFailure::record`]), so "panic wins" reproduces the tree's
/// observable semantics.
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

impl<E> From<PanicPayload> for TryFailure<E> {
    fn from(p: PanicPayload) -> Self {
        TryFailure::Panic(p)
    }
}

/// Classify a strategy failure into the erased sum: a panic keeps its payload
/// unboxed (`Panic`), everything else is the op failure (`Op`). Needed because
/// the tree recursion now hands panics back as `Err` values (see
/// [`par_tree_rec`]) instead of unwinding through the erased boundary.
trait ErasedFailureFrom: Sized {
    fn into_erased(self) -> ErasedFailure;
}

impl ErasedFailureFrom for PanicPayload {
    fn into_erased(self) -> ErasedFailure {
        ErasedFailure::Panic(self)
    }
}

impl<E: Any + Send> ErasedFailureFrom for TryFailure<E> {
    fn into_erased(self) -> ErasedFailure {
        match self {
            TryFailure::Panic(p) => ErasedFailure::Panic(p),
            TryFailure::Error(e) => ErasedFailure::Op(Box::new(e)),
        }
    }
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
    S::Failure: ErasedFailureFrom,
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
                    .map_err(ErasedFailureFrom::into_erased)
            },
            run_sequential: |ctx, input, start, end| unsafe {
                (*ctx.cast::<S>())
                    .run_sequential(input, start, end)
                    .map_err(ErasedFailureFrom::into_erased)
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
        let leaf = |input: &Slots<T>, start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively. Input slots are init; output
            // slots are uninit.
            let in_slice = unsafe { input.as_slice(start, end) };
            let out_slice = unsafe { self.output.as_mut_slice(start, end) };
            par_index_leaf(in_slice, out_slice, self.op, self.nt);
            Ok(())
        };
        let drop_success_range = |start: usize, end: usize| {
            // SAFETY (hook contract): only called for completed ranges, so
            // those output slots are fully init and safe to drop.
            unsafe { self.output.drop_range(start, end) };
        };
        par_tree_rec(pool, input, start, end, splits, &leaf, &drop_success_range)
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
        let leaf = |input: &Slots<T>, start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively.
            let in_slice = unsafe { input.as_slice(start, end) };
            par_for_each_leaf(in_slice, self.op);
            Ok(())
        };
        // Sink-only: the panicking sibling's guard drops its own input tail,
        // the completed sibling fully consumed its range — nothing to clean.
        let noop = |_start: usize, _end: usize| {};
        par_tree_rec(pool, input, start, end, splits, &leaf, &noop)
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
        // leaf guard already dropped its own unread input tail inside the
        // tree, and successful chunks fully consumed theirs.
    }
}

// ── Zero-materialization generation core (`pipe_range`) ──
//
// `pipe(items)` materializes any non-`Vec` input on the calling thread
// before the parallel phase: `(0..n).collect::<Vec<u64>>()` is a serial O(n)
// fill (measured 167 µs @ 1 M / 1.1 ms @ 4 M, plus the buffer's
// alloc/read/free lifecycle), which is 56–70 % of the whole owned call at
// those sizes (input-materialize caliber, docs/src/dev/benchmarks.md). The
// generation core removes the input buffer entirely: the item at index `i`
// IS the index, so each leaf computes `op.apply(base + i)` straight into the
// output slot — no input allocation, no fill, no input cache traffic.
//
// Reuses `hybrid_dispatch` behind the same erased-strategy boundary with the
// zero-sized input handle `IN = ()` (the dispatcher never dereferences the
// input; only strategy leaves do, and these ignore it).

/// Hybrid strategy for `RangePipe::collect()`: the generated-input
/// counterpart of [`CollectStrategy`] — same output contract (shared
/// `Slots<R>`, drop successful chunks' ranges on failure), minus the input
/// buffer.
struct RangeGenStrategy<'a, R, OP> {
    /// Absolute value of the first generated item (the range's start).
    base: usize,
    output: &'a Slots<R>,
    op: &'a OP,
    /// Whole-batch NT decision (see [`nt_store_enabled`]).
    nt: bool,
}

impl<R, OP> HybridStrategy<()> for RangeGenStrategy<'_, R, OP>
where
    R: Send,
    OP: RangeOp<usize, Out = R>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &(),
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        let leaf = |_input: &(), start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively; output slots are uninit.
            let out_slice = unsafe { self.output.as_mut_slice(start, end) };
            par_range_gen_leaf(self.base + start, out_slice, self.op, self.nt);
            Ok(())
        };
        let drop_success_range = |start: usize, end: usize| {
            // SAFETY (hook contract): only called for completed ranges, so
            // those output slots are fully init and safe to drop.
            unsafe { self.output.drop_range(start, end) };
        };
        par_tree_rec(pool, input, start, end, splits, &leaf, &drop_success_range)
    }

    #[inline]
    fn run_sequential(&self, _input: &(), start: usize, end: usize) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the caller (driver or leaf) owns
        // `[start, end)` exclusively; output slots are uninit.
        let out_slice = unsafe { self.output.as_mut_slice(start, end) };
        par_range_gen_leaf(self.base + start, out_slice, self.op, self.nt);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, end: usize) {
        // SAFETY: caller guarantees `run_chunk` returned `Ok(())` for
        // `[start, end)`, so those output slots are fully init and safe to
        // drop; after this the range is uninit and the buffer frees cleanly.
        unsafe { self.output.drop_range(start, end) };
    }
}

/// Generate `[base, base + output.len())` sequentially into `output`.
///
/// The [`par_index_leaf`] counterpart with the input half elided: the guard
/// drops only the partial OUTPUT range on unwind (a generated item is never
/// stored, so there is no input tail to drop). Same NT-store branch shape —
/// the plain loop's codegen (register-allocated `written`,
/// auto-vectorizable `op`) must stay untouched when NT is off.
fn par_range_gen_leaf<R, OP>(base: usize, output: &mut [R], op: &OP, nt: bool)
where
    R: Send,
    OP: RangeOp<usize, Out = R>,
{
    // Unwind cleanup — `LeafCleanup` with the output half only: a generated
    // item is never stored, so no input tail can exist (`output[..written]`
    // has no holes — `RangeOp` never filters). The dead input pointer is
    // never dereferenced (`IN = false`).
    let n = output.len();
    let out_ptr = output.as_mut_ptr();
    let mut g = LeafCleanup::<R, R, true, false> {
        in_ptr: out_ptr.cast_const(),
        out_ptr,
        n,
        written: 0,
    };

    // The NT branch stays a separate loop copy (see `par_index_leaf`).
    if nt {
        let _fence = NtFenceOnDrop;
        while g.written < n {
            let i = g.written;
            let out = op.apply(base + i);
            // SAFETY: disjoint index; slot i is uninit. `nt_store`'s
            // alignment/size requirements come from `nt_store_enabled`'s
            // eligibility check.
            unsafe { nt_store(out_ptr.add(i), out) };
            g.written = i + 1;
        }
    } else {
        while g.written < n {
            let i = g.written;
            let out = op.apply(base + i);
            // SAFETY: disjoint index; slot i is uninit.
            unsafe { ptr::write(out_ptr.add(i), out) };
            g.written = i + 1;
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
}

/// Drive the generation core over a whole range and convert the output
/// buffer into a `Vec<R>` — the generation twin of [`par_index_collect`].
///
/// # Panics
///
/// Propagates any panic raised by `op`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_range_gen_collect<R, OP>(
    range: Range<usize>,
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Vec<R>
where
    R: Send,
    OP: RangeOp<usize, Out = R>,
{
    let n = range.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let output = Slots::<R>::uninit(n);
    let strategy = RangeGenStrategy {
        base: range.start,
        output: &output,
        op,
        nt: nt_store_enabled::<R>(n),
    };
    let result = hybrid_dispatch(
        pool,
        &(),
        &ErasedStrategy::from(&strategy),
        n,
        plan,
        num_threads,
    )
    .err();
    if let Some(f) = result {
        // Successful chunks' output ranges were dropped by the strategy;
        // freeing the box just frees memory.
        drop(output);
        resume_panic(f);
    }
    // Output fully init: transmute into the result Vec.
    output.into_vec()
}

/// Hybrid strategy for `RangePipe::for_each()`: sink-only, no output buffer,
/// no input buffer — nothing to clean on any path.
struct RangeGenSinkStrategy<'a, OP> {
    base: usize,
    op: &'a OP,
}

impl<OP> HybridStrategy<()> for RangeGenSinkStrategy<'_, OP>
where
    OP: SinkOp<usize>,
{
    type Failure = PanicPayload;

    #[inline]
    fn run_chunk(
        &self,
        pool: &ComputePool,
        input: &(),
        start: usize,
        end: usize,
        splits: usize,
    ) -> Result<(), PanicPayload> {
        let leaf = |_input: &(), start: usize, end: usize| {
            for item in (self.base + start)..(self.base + end) {
                self.op.consume(item);
            }
            Ok(())
        };
        // Sink-only and buffer-free: generated items are never stored, so
        // no completed range holds anything to drop.
        let noop = |_start: usize, _end: usize| {};
        par_tree_rec(pool, input, start, end, splits, &leaf, &noop)
    }

    #[inline]
    fn run_sequential(&self, _input: &(), start: usize, end: usize) -> Result<(), PanicPayload> {
        for item in (self.base + start)..(self.base + end) {
            self.op.consume(item);
        }
        Ok(())
    }

    #[inline]
    /// # Safety
    ///
    /// Nothing to drop for a sink-only, buffer-free strategy.
    unsafe fn cleanup_success_chunk(&self, _start: usize, _end: usize) {
        // No-op: no output buffer, and generated items are never stored.
    }
}

/// Drive the generation sink core over a whole range.
///
/// # Panics
///
/// Propagates any panic raised by `op`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_range_gen_for_each<OP>(range: Range<usize>, op: &OP, plan: SplitPlan, pool: &ComputePool)
where
    OP: SinkOp<usize>,
{
    let n = range.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let strategy = RangeGenSinkStrategy {
        base: range.start,
        op,
    };
    let result = hybrid_dispatch(
        pool,
        &(),
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

/// Hybrid strategy for `.try_collect()`'s no-filter fast path: like
/// [`CollectStrategy`] it writes outputs into a shared `Slots<R>` at known
/// indices, but the chunk driver is fallible — the first `Err(e)` is funnelled
/// into the shared [`TryFailure`] slot instead of a panic-only slot.
///
/// Range resolution on each failure kind:
/// - `Ok(())` chunk — output range fully init; the driver drops it via `cleanup_success_chunk` when
///   some other chunk failed.
/// - `Err(e)` chunk — the fallible tree's leaf/internal-node cleanup has already dropped every live
///   output slot and consumed every input slot in the chunk's range, so nothing remains to clean
///   (mirrors the panicked chunk of the infallible strategies).
/// - Panicking chunk — comes back as `TryFailure::Panic` through the recursion's per-level halt
///   (leaf guard cleans its own partial range; completed sibling ranges are dropped at each
///   internal node), so nothing leaks either.
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
        let leaf = |input: &Slots<T>, start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively. Input slots are init; output
            // slots are uninit.
            let in_slice = unsafe { input.as_slice(start, end) };
            let out_slice = unsafe { self.output.as_mut_slice(start, end) };
            par_index_try_leaf(in_slice, out_slice, self.op).map_err(TryFailure::Error)
        };
        let drop_success_range = |start: usize, end: usize| {
            // SAFETY (hook contract): only called for completed ranges, so
            // those output slots are fully init and safe to drop.
            unsafe { self.output.drop_range(start, end) };
        };
        par_tree_rec(pool, input, start, end, splits, &leaf, &drop_success_range)
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

/// Top-level chunk count of a hybrid dispatch — shared by `hybrid_dispatch`
/// and the reduce strategies' slot geometry (see [`ChunkSlots`]): the two
/// must agree, or a chunk's start index would map to the wrong slot.
fn hybrid_num_chunks(n: usize, plan: SplitPlan, num_threads: usize) -> usize {
    Ord::min(num_threads + plan.chunk_slack, n).max(1)
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
    let num_chunks = hybrid_num_chunks(n, plan, num_threads);
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

/// Consume `[start, end)` sequentially on the current thread, applying `op`
/// for its side effect.
///
/// Panic safety uses a stack-local [`LeafCleanup`] guard (input half only —
/// `for_each` allocates no output buffer) whose `Drop` runs only on unwind.
/// At the panic point in `op.consume(item)` for iter `i = written`, item `i`
/// has been moved into `op` (gone with the panic), `input[i+1..]` is still
/// init (untouched, must be dropped); `input[..i]` was already moved-out in
/// prior iterations.
fn par_for_each_leaf<T, OP>(input: &[T], op: &OP)
where
    T: Send,
    OP: SinkOp<T>,
{
    // Unwind cleanup — `LeafCleanup` with the input half only (no output
    // buffer exists): items `..written` were already moved out (uninit), item
    // `written` is gone with the panic, `input[written+1..]` must be dropped.
    // The dead output pointer is never dereferenced (`OUT = false`).
    let in_ptr = input.as_ptr();
    let n = input.len();

    let mut g = LeafCleanup::<T, (), false, true> {
        in_ptr,
        out_ptr: ptr::null_mut(),
        n,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input). The read moves the
        // item out of the slot, leaving it uninit — never re-read.
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        op.consume(item);
        g.written = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
}

/// Drive the sink core over `[0, n)`. Propagates panics after the tree's
/// leaf guards have dropped every unread input slot.
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

// ── Reduce core (reduce / fold / sum / count / min / max) ──
//
// The aggregation counterpart of `for_each`'s structural win: a reduce
// terminal never allocates an output buffer. Each leaf folds its disjoint
// range into one partial accumulator, and the tree combines partials
// bottom-up through `join` returns — the combine runs on whatever worker
// finishes each subtree, in parallel. Contrast with the collect path, where
// a `.map(f).sum()` shape must pay an n-slot `Slots` allocation + n slot
// writes + a serial fold over the materialized `Vec`.
//
// Chunk-level integration reuses `hybrid_dispatch` unchanged:
// `ReduceStrategy`'s chunk driver runs `par_reduce_rec` and publishes the
// chunk's partial into its one-shot [`ChunkSlots`] cell; the driver folds
// the cells in chunk order (= input order, deterministic left-to-right)
// after the latch. Publication is a plain store to the chunk's own cell —
// no shared lock (a `Mutex<Vec>` here serialized the tail of small batches;
// see the `ChunkSlots` doc for the measured numbers).
//
// Panic safety is structural for the tree itself — a partial is a plain
// local value moved through `join`, so a panicking leaf's partial drops
// with the unwind and the waited-out sibling's partial drops inside
// `join`'s machinery. The one cleanup hook is `cleanup_success_chunk` ->
// `ChunkSlots::drop_published`, eagerly dropping each successful chunk's
// published partial on failure paths (the slot box's own drop is the
// backstop). The leaf guard owns only the input tail (owned input) —
// exactly the `for_each` leaf shape.

/// Owned-input reduce leaf: fold `input` into one partial accumulator.
fn reduce_leaf<T, OP>(input: &[T], op: &OP) -> OP::Acc
where
    T: Send,
    OP: ReduceOp<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    // Unwind cleanup — input half only (no output buffer exists; the
    // partial `acc` is a local and drops naturally).
    let mut g = LeafCleanup::<T, (), false, true> {
        in_ptr,
        out_ptr: ptr::null_mut(),
        n,
        written: 0,
    };

    let mut acc = op.identity();
    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input). The read moves the
        // item out of the slot, leaving it uninit — never re-read.
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        acc = op.fold(acc, item);
        g.written = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    acc
}

/// Owned-input fallible reduce leaf — short-circuits on the first `Err`.
/// On `Err`, the accumulator was moved into `try_fold` (gone with it), and
/// the guard drops the unread input tail before disarming. Panics unwind
/// through the guard the same way (the partial `acc` drops as a local).
fn reduce_try_leaf<T, OP>(input: &[T], op: &OP) -> Result<OP::Acc, OP::Error>
where
    T: Send,
    OP: TryReduceOp<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    // Unwind cleanup — input half only (see `reduce_leaf`).
    let mut g = LeafCleanup::<T, (), false, true> {
        in_ptr,
        out_ptr: ptr::null_mut(),
        n,
        written: 0,
    };

    let mut acc = op.identity();
    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init (input).
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        match op.try_fold(acc, item) {
            Ok(a) => {
                acc = a;
                g.written = i + 1;
            },
            Err(e) => {
                // SAFETY: `written == i`; slots `(i, n)` are still init.
                unsafe { g.cleanup() };
                std::mem::forget(g);
                return Err(e);
            },
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(acc)
}

/// Borrowed-input reduce leaf — no guard at all: the input is shared
/// (never consumed) and no output buffer exists, so a panic in `op` leaves
/// nothing to clean in this leaf.
fn reduce_leaf_by_ref<'i, E, OP>(input: &'i [E], op: &OP) -> OP::Acc
where
    E: Sync,
    OP: ReduceOp<&'i E>,
{
    let mut acc = op.identity();
    for item in input {
        acc = op.fold(acc, item);
    }
    acc
}

/// Borrowed-input fallible reduce leaf.
fn reduce_try_leaf_by_ref<'i, E, OP>(input: &'i [E], op: &OP) -> Result<OP::Acc, OP::Error>
where
    E: Sync,
    OP: TryReduceOp<&'i E>,
{
    let mut acc = op.identity();
    for item in input {
        acc = op.try_fold(acc, item)?;
    }
    Ok(acc)
}

/// Value-carrying divide-and-conquer over the owned input. Unlike
/// [`par_tree_rec`] (unit results + failure-cleanup hooks), the reduce
/// tree's `join` returns both child partials and the node combines them —
/// there is no shared buffer, so there is no sibling-drop path at all; a
/// panicking subtree's unwind propagates and every partial (a local value)
/// drops naturally.
fn par_reduce_rec<T, OP>(
    pool: &ComputePool,
    input: &Slots<T>,
    op: &OP,
    start: usize,
    end: usize,
    splits_left: usize,
) -> OP::Acc
where
    T: Send,
    OP: ReduceOp<T>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        return reduce_leaf(in_slice, op);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_reduce_rec(pool, input, op, start, mid, splits_left - 1),
        || par_reduce_rec(pool, input, op, mid, end, splits_left - 1),
    );
    op.combine(l, r)
}

/// Borrowed-input counterpart of [`par_reduce_rec`].
fn par_reduce_rec_by_ref<'i, E, OP>(
    pool: &ComputePool,
    input: &'i [E],
    op: &OP,
    start: usize,
    end: usize,
    splits_left: usize,
) -> OP::Acc
where
    E: Sync,
    OP: ReduceOp<&'i E>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: disjoint range — this leaf owns `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        return reduce_leaf_by_ref(in_slice, op);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_reduce_rec_by_ref(pool, input, op, start, mid, splits_left - 1),
        || par_reduce_rec_by_ref(pool, input, op, mid, end, splits_left - 1),
    );
    op.combine(l, r)
}

/// Fallible owned-input tree. On `Err` every partial is already gone (the
/// failing side's partial dropped inside its leaf, the `Ok` sibling's
/// dropped as a moved value in the match arm below) and every input slot of
/// the failing side is resolved by its leaf cleanup; the `Ok` sibling fully
/// consumed its own range. Panics unwind (same as [`par_reduce_rec`]).
fn par_reduce_try_rec<T, OP>(
    pool: &ComputePool,
    input: &Slots<T>,
    op: &OP,
    start: usize,
    end: usize,
    splits_left: usize,
) -> Result<OP::Acc, OP::Error>
where
    T: Send,
    OP: TryReduceOp<T>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: this leaf owns the disjoint range `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        return reduce_try_leaf(in_slice, op);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_reduce_try_rec(pool, input, op, start, mid, splits_left - 1),
        || par_reduce_try_rec(pool, input, op, mid, end, splits_left - 1),
    );
    match (l, r) {
        (Ok(l), Ok(r)) => Ok(op.combine(l, r)),
        // The `Ok` sibling's partial (if any) drops here as a moved value.
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// Borrowed-input fallible tree — counterpart of [`par_reduce_try_rec`].
fn par_reduce_try_rec_by_ref<'i, E, OP>(
    pool: &ComputePool,
    input: &'i [E],
    op: &OP,
    start: usize,
    end: usize,
    splits_left: usize,
) -> Result<OP::Acc, OP::Error>
where
    E: Sync,
    OP: TryReduceOp<&'i E>,
{
    if splits_left == 0 || end - start <= 1 {
        // SAFETY: disjoint range — this leaf owns `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        return reduce_try_leaf_by_ref(in_slice, op);
    }
    let mid = start + (end - start) / 2;
    let (l, r) = pool.join(
        || par_reduce_try_rec_by_ref(pool, input, op, start, mid, splits_left - 1),
        || par_reduce_try_rec_by_ref(pool, input, op, mid, end, splits_left - 1),
    );
    match (l, r) {
        (Ok(l), Ok(r)) => Ok(op.combine(l, r)),
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// One-shot mailbox per top-level chunk: `Empty` until the chunk's tree
/// finishes, then `Full(partial)` — the reduce core's publication surface.
///
/// Zero shared contention by construction: every chunk writes exactly its
/// own cell (a plain store, sequenced before its latch `set`), and only the
/// driver reads cells, after the latch wait. The first design used a
/// `Mutex<Vec<(start, Acc)>>` with one push per chunk — correct, but the
/// ~`num_threads` pushes pile up on the single lock when the per-chunk work
/// is short, serializing the batch tail: measured +80…+112 % vs
/// collect-then-sum at 1K and +40…+73 % at 10K (the win at 100K/1M, where
/// publication overlaps real compute, was −40 %/−80 %). Per-chunk cells
/// remove the shared line entirely; unpublished (`Empty`) cells are simply
/// skipped by the combine, which also covers the on-pool single-tree path
/// (one `run_chunk` over `[0, n)` publishes only chunk 0's cell).
struct ChunkSlots<A> {
    slots: Box<[UnsafeCell<ChunkCell<A>>]>,
    /// Chunk geometry of the dispatch (`n = num_chunks * chunk + rem`):
    /// chunk `i` covers `[i*chunk + min(i, rem), …)` — the driver's boundary
    /// formula, inverted by [`ChunkSlots::chunk_of`].
    chunk: usize,
    rem: usize,
}

enum ChunkCell<A> {
    Empty,
    Full(A),
}

// SAFETY: access is governed by the one-shot mailbox discipline documented
// on `ChunkSlots` — each cell is written by exactly one chunk (plain store
// before its SeqCst latch `set`) and read only by the driver after the
// latch wait; `A: Send` covers the partial crossing threads.
unsafe impl<A: Send> Send for ChunkSlots<A> {}
unsafe impl<A: Send> Sync for ChunkSlots<A> {}

impl<A> ChunkSlots<A> {
    /// Allocate `num_chunks` empty cells for an `n`-item dispatch (geometry
    /// from [`hybrid_num_chunks`], matching the driver's split).
    fn new(n: usize, num_chunks: usize) -> Self {
        Self {
            slots: (0..num_chunks)
                .map(|_| UnsafeCell::new(ChunkCell::Empty))
                .collect(),
            chunk: n / num_chunks,
            rem: n % num_chunks,
        }
    }

    /// The top-level chunk ordinal owning `start` — the inverse of the
    /// driver's boundary formula `start(i) = i*chunk + min(i, rem)`.
    fn chunk_of(&self, start: usize) -> usize {
        // `chunk >= 1` always (num_chunks <= n), so both divisors are
        // non-zero; the front block holds the `rem` wider chunks.
        let front = self.rem * (self.chunk + 1);
        if start < front {
            start / (self.chunk + 1)
        } else {
            self.rem + (start - front) / self.chunk
        }
    }

    /// Publish a completed chunk's partial (plain store to the chunk's own
    /// cell; the latch publishes it to the driver).
    fn publish(&self, start: usize, acc: A) {
        let i = self.chunk_of(start);
        // SAFETY: this chunk's cell is exclusively ours until the driver
        // reads it (one-shot discipline); the previous state is `Empty`
        // (no value to drop).
        unsafe { *self.slots[i].get() = ChunkCell::Full(acc) };
    }

    /// Drop a published partial — the failure-path cleanup for a successful
    /// chunk (failed chunks never published).
    ///
    /// # Safety
    ///
    /// The chunk owning `start` must have published exactly once and must
    /// not be read afterwards.
    unsafe fn drop_published(&self, start: usize) {
        let i = self.chunk_of(start);
        // SAFETY: contract above; the take leaves `Empty`, so the box's own
        // drop never sees a live value again (no double-drop).
        let cell = unsafe { &mut *self.slots[i].get() };
        if matches!(*cell, ChunkCell::Full(_)) {
            *cell = ChunkCell::Empty;
        }
    }

    /// Fold every published partial left-to-right (chunk ordinal = input
    /// order). Runs once, on the driver, after the latch. Each cell is
    /// swapped to `Empty` as it is consumed, so the box's own drop never
    /// sees a live value (no double-drop for `Drop` accumulators).
    fn combine(self, mut f: impl FnMut(A, A) -> A) -> A {
        let mut acc: Option<A> = None;
        for cell in &self.slots {
            // SAFETY: every published cell was written before its chunk's
            // latch `set`; the driver's wait orders those stores before
            // this read (one-shot: `combine` consumes the slots, so each
            // cell is touched exactly once). `Empty` cells — chunks that
            // never ran, only possible via the on-pool single-tree
            // shortcut — are skipped.
            match unsafe { ptr::replace(cell.get(), ChunkCell::Empty) } {
                ChunkCell::Full(a) => {
                    acc = Some(match acc {
                        Some(x) => f(x, a),
                        None => a,
                    });
                },
                ChunkCell::Empty => {},
            }
        }
        acc.expect("hybrid_dispatch always runs at least one chunk")
    }
}

/// Hybrid strategy for the owned-input reduce terminals: each chunk's tree
/// produces one partial, published into the chunk's [`ChunkSlots`] cell.
struct ReduceStrategy<'a, T, OP>
where
    OP: ReduceOp<T>,
{
    op: &'a OP,
    /// Per-chunk partial cells (see [`ChunkSlots`]); the driver combines
    /// them in chunk order after the latch. On failure,
    /// `cleanup_success_chunk` drops exactly the published cells.
    slots: ChunkSlots<OP::Acc>,
    _marker: PhantomData<fn(&T)>,
}

impl<T, OP> HybridStrategy<Slots<T>> for ReduceStrategy<'_, T, OP>
where
    T: Send,
    OP: ReduceOp<T>,
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
        let acc = par_reduce_rec(pool, input, self.op, start, end, splits);
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &Slots<T>,
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the driver owns `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        let acc = reduce_leaf(in_slice, self.op);
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, _end: usize) {
        // Drop this successful chunk's published partial so the caller can
        // free the slot box without leaking accumulator state (a failed
        // batch has no user-visible accumulator).
        // SAFETY: the dispatcher only calls this for chunks whose
        // `run_chunk` returned `Ok(())` — exactly the publishers.
        unsafe { self.slots.drop_published(start) };
    }
}

/// Hybrid strategy for the borrowed-input reduce terminals — the
/// [`ReduceStrategy`] counterpart over the `&'i [E]` input handle.
struct ReduceByRefStrategy<'a, 'i, E, OP>
where
    OP: ReduceOp<&'i E>,
{
    op: &'a OP,
    /// See [`ReduceStrategy::slots`].
    slots: ChunkSlots<OP::Acc>,
    _marker: PhantomData<fn(&'i E)>,
}

impl<'i, E, OP> HybridStrategy<&'i [E]> for ReduceByRefStrategy<'_, 'i, E, OP>
where
    E: Sync,
    OP: ReduceOp<&'i E>,
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
        let acc = par_reduce_rec_by_ref(pool, input, self.op, start, end, splits);
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &&'i [E],
        start: usize,
        end: usize,
    ) -> Result<(), PanicPayload> {
        // SAFETY: disjoint range — the driver owns `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let acc = reduce_leaf_by_ref(in_slice, self.op);
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, _end: usize) {
        // SAFETY: dispatcher contract — successful publishers only.
        unsafe { self.slots.drop_published(start) };
    }
}

/// Hybrid strategy for the owned-input fallible reduce terminals
/// (`try_reduce` / `try_fold`): the tree short-circuits on the first `Err`
/// exactly like [`TryStrategy`]'s, and only successful chunks publish.
struct ReduceTryStrategy<'a, T, E, OP>
where
    OP: TryReduceOp<T, Error = E>,
{
    op: &'a OP,
    /// See [`ReduceStrategy::slots`]; a chunk's partial is published only
    /// after its tree returned `Ok`.
    slots: ChunkSlots<OP::Acc>,
    _marker: PhantomData<fn(&T, E)>,
}

impl<T, E, OP> HybridStrategy<Slots<T>> for ReduceTryStrategy<'_, T, E, OP>
where
    T: Send,
    E: Send + 'static,
    OP: TryReduceOp<T, Error = E>,
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
        let acc = par_reduce_try_rec(pool, input, self.op, start, end, splits)
            .map_err(TryFailure::Error)?;
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &Slots<T>,
        start: usize,
        end: usize,
    ) -> Result<(), TryFailure<E>> {
        // SAFETY: disjoint range — the driver owns `[start, end)`
        // exclusively; the slots are init.
        let in_slice = unsafe { input.as_slice(start, end) };
        let acc = reduce_try_leaf(in_slice, self.op).map_err(TryFailure::Error)?;
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, _end: usize) {
        // SAFETY: dispatcher contract — successful publishers only.
        unsafe { self.slots.drop_published(start) };
    }
}

/// Hybrid strategy for the borrowed-input fallible reduce terminals.
struct ReduceTryByRefStrategy<'a, 'i, E, F, OP>
where
    OP: TryReduceOp<&'i E, Error = F>,
{
    op: &'a OP,
    /// See [`ReduceStrategy::slots`].
    slots: ChunkSlots<OP::Acc>,
    _marker: PhantomData<fn(&'i E, E, F)>,
}

impl<'i, E, F, OP> HybridStrategy<&'i [E]> for ReduceTryByRefStrategy<'_, 'i, E, F, OP>
where
    E: Sync,
    F: Send + 'static,
    OP: TryReduceOp<&'i E, Error = F>,
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
        let acc = par_reduce_try_rec_by_ref(pool, input, self.op, start, end, splits)
            .map_err(TryFailure::Error)?;
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    fn run_sequential(
        &self,
        input: &&'i [E],
        start: usize,
        end: usize,
    ) -> Result<(), TryFailure<F>> {
        // SAFETY: disjoint range — the driver owns `[start, end)`
        // exclusively; the input slice is shared and init.
        let in_slice = unsafe { input.get_unchecked(start..end) };
        let acc = reduce_try_leaf_by_ref(in_slice, self.op).map_err(TryFailure::Error)?;
        self.slots.publish(start, acc);
        Ok(())
    }

    #[inline]
    unsafe fn cleanup_success_chunk(&self, start: usize, _end: usize) {
        // SAFETY: dispatcher contract — successful publishers only.
        unsafe { self.slots.drop_published(start) };
    }
}

/// Drive the reduce core over an owned batch, then combine the published
/// chunk partials in input order.
///
/// # Panics
///
/// Propagates any panic raised by the op (after the leaf guards dropped
/// every unread input slot; published partials drop with the strategy).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_reduce<T, OP>(items: Vec<T>, op: &OP, plan: SplitPlan, pool: &ComputePool) -> OP::Acc
where
    T: Send,
    OP: ReduceOp<T>,
{
    let n = items.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let input = Slots::from_vec(items);
    let strategy = ReduceStrategy {
        op,
        slots: ChunkSlots::new(n, hybrid_num_chunks(n, plan, num_threads)),
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
    .err();
    // Every input slot is resolved on both paths: successful chunks consumed
    // their whole range; failed chunks' guards dropped their unread tails.
    drop(input);
    if let Some(f) = result {
        // Published partials were dropped by the strategy's cleanup hook.
        resume_panic(f);
    }
    strategy.slots.combine(|l, r| op.combine(l, r))
}

/// Drive the reduce core over a borrowed `&'i [E]` — counterpart of
/// [`par_reduce`].
///
/// # Panics
///
/// Propagates any panic raised by the op.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_reduce_by_ref<'i, E, OP>(
    input: &'i [E],
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> OP::Acc
where
    E: Sync,
    OP: ReduceOp<&'i E>,
{
    let n = input.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let strategy = ReduceByRefStrategy {
        op,
        slots: ChunkSlots::new(n, hybrid_num_chunks(n, plan, num_threads)),
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
    .err();
    if let Some(f) = result {
        resume_panic(f);
    }
    strategy.slots.combine(|l, r| op.combine(l, r))
}

/// Drive the fallible reduce core over an owned batch.
///
/// # Panics
///
/// Propagates any panic raised by the op (mirrors `par_index_try_collect`'s
/// panic path).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_reduce_try<T, OP>(
    items: Vec<T>,
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Result<OP::Acc, OP::Error>
where
    T: Send,
    OP: TryReduceOp<T>,
    OP::Error: 'static,
{
    let n = items.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let input = Slots::from_vec(items);
    let strategy = ReduceTryStrategy {
        op,
        slots: ChunkSlots::new(n, hybrid_num_chunks(n, plan, num_threads)),
        _marker: PhantomData,
    };
    // Downcast the erased op failure back to `TryFailure<OP::Error>`.
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
        ErasedFailure::Op(b) => match b.downcast::<OP::Error>() {
            Ok(e) => TryFailure::Error(*e),
            Err(_) => unreachable!("try reduce strategy only records op failures"),
        },
        ErasedFailure::Panic(p) => TryFailure::Panic(p),
    });
    match result {
        None => {
            drop(input);
            Ok(strategy.slots.combine(|l, r| op.combine(l, r)))
        },
        Some(TryFailure::Error(e)) => {
            // Published partials were dropped by the cleanup hook; the rest
            // of the batch's state resolved inside the trees.
            drop(input);
            Err(e)
        },
        Some(TryFailure::Panic(p)) => {
            drop(input);
            panic::resume_unwind(p);
        },
    }
}

/// Drive the fallible reduce core over a borrowed `&'i [E]`.
///
/// # Panics
///
/// Propagates any panic raised by the op.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_reduce_try_by_ref<'i, E, OP>(
    input: &'i [E],
    op: &OP,
    plan: SplitPlan,
    pool: &ComputePool,
) -> Result<OP::Acc, OP::Error>
where
    E: Sync,
    OP: TryReduceOp<&'i E>,
    OP::Error: 'static,
{
    let n = input.len();
    debug_assert!(n > 0);
    let num_threads = pool.num_workers();
    let strategy = ReduceTryByRefStrategy {
        op,
        slots: ChunkSlots::new(n, hybrid_num_chunks(n, plan, num_threads)),
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
        ErasedFailure::Op(b) => match b.downcast::<OP::Error>() {
            Ok(e) => TryFailure::Error(*e),
            Err(_) => unreachable!("try reduce strategy only records op failures"),
        },
        ErasedFailure::Panic(p) => TryFailure::Panic(p),
    });
    match result {
        None => Ok(strategy.slots.combine(|l, r| op.combine(l, r))),
        Some(TryFailure::Error(e)) => Err(e),
        Some(TryFailure::Panic(p)) => panic::resume_unwind(p),
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

/// Process `[start, end)` sequentially, short-circuiting on the first `Err`.
///
/// On error: drops `output[..written]` (init from prior iterations) and
/// `input[written+1..]` (still init — untouched), then returns `Err`. Item
/// `written` was consumed by `try_apply` and is gone.
///
/// A [`LeafCleanup`] guard runs the same cleanup on **panic** (unwind),
/// disarmed by `mem::forget` on both the `Ok` and `Err` return paths —
/// identical structure to the guard in `par_index_leaf`.
fn par_index_try_leaf<T, R, E, OP>(input: &[T], output: &mut [R], op: &OP) -> Result<(), E>
where
    T: Send,
    R: Send,
    E: Send,
    OP: RangeTryOp<T, Out = R, Error = E>,
{
    // Unwind cleanup — the shared `LeafCleanup` guard with both halves live,
    // same contract as `par_index_leaf`'s (see the comment there).
    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafCleanup::<T, R, true, true> {
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
                // SAFETY: `written == i`; `output[..i)` and `input(i..n)`
                // hold live values.
                unsafe { g.cleanup() };
                std::mem::forget(g);
                return Err(e);
            },
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(())
}

/// Drive the fallible index core over `[0, n)` and convert the output buffer into
/// a `Vec<R>`. On error or panic, the recursion has already dropped all init
/// output slots (panics travel the same value channel as op errors — see
/// [`par_tree_rec`]).
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
        ErasedFailure::Op(b) => match b.downcast::<E>() {
            Ok(e) => TryFailure::Error(*e),
            Err(_) => unreachable!("try strategy only records op failures of E"),
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
            // Recursion already dropped every live output slot; resume the
            // panic for the caller.
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
// paid one allocation + memcpy of the *input* half per internal node. The
// output side is NOT single-move: every internal node's `l.extend(r)`
// reserves and memcpys one child's `Vec` into the other's, so a surviving
// item is moved O(depth) times (once into its leaf's `Vec`, then once per
// merge level above it).
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

/// Consume `input` sequentially, applying `stages` and collecting surviving
/// outputs into a fresh `Vec`.
///
/// Panic safety: a [`LeafCleanup`] guard (input half only) drops the unread
/// input tail on unwind (there is no shared output buffer; the leaf's `Vec`
/// drops naturally).
fn filter_leaf<T, S>(input: &[T], stages: &S) -> Vec<S::Output>
where
    T: Send,
    S: FusedStage<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    // Pre-allocate for the all-survive worst case (rayon's filter does the
    // same); when most items are filtered out the over-allocation is bounded
    // by the leaf input length and avoids log-many reallocs + partial memcpys.
    let mut out = Vec::with_capacity(n);
    // Unwind cleanup — input half only (the leaf's `Vec` drops itself).
    let mut g = LeafCleanup::<T, (), false, true> {
        in_ptr,
        out_ptr: ptr::null_mut(),
        n,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
        // SAFETY: disjoint index; slot i is init. The read moves the item out
        // of the slot, leaving it uninit — never re-read.
        let item = unsafe { ptr::read(in_ptr.add(i)) };
        if let Some(o) = stages.apply(item) {
            out.push(o);
        }
        g.written = i + 1;
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    out
}

/// Recursive range-based filter collect. Each leaf claims the disjoint range
/// `[start, end)` and produces its own `Vec`; internal nodes concatenate via
/// `l.extend(r)` — one reserve + memcpy of one side per node, i.e. O(depth)
/// moves per surviving item (see the section comment).
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
/// within round spread (+1…+2 %, rounds interleave). The split_off tree paid
/// one allocation + memcpy of the *input* half per internal node (~n·levels/2
/// input item moves before any stage work); the range tree shares the input
/// via `Slots`, so the input is never copied. Outputs are still concatenated
/// level by level — `l.extend(r)` moves one side per internal node, so each
/// surviving item is moved O(depth) times, not once; the measured win comes
/// from eliminating the input-side copies, not single-move outputs.
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
/// tail drops via the guard's cleanup (item `pos` was consumed by
/// `try_apply`); the guard runs the same cleanup on **panic** (unwind).
fn filter_try_leaf<T, S>(input: &[T], stages: &S) -> Result<Vec<S::Output>, S::Error>
where
    T: Send,
    S: FusedTryStage<T>,
{
    let in_ptr = input.as_ptr();
    let n = input.len();

    // Pre-allocate for the all-survive worst case (rayon's filter does the
    // same); when most items are filtered out the over-allocation is bounded
    // by the leaf input length and avoids log-many reallocs + partial memcpys.
    let mut out = Vec::with_capacity(n);
    // Unwind cleanup — input half only (the leaf's `Vec` drops itself).
    let mut g = LeafCleanup::<T, (), false, true> {
        in_ptr,
        out_ptr: ptr::null_mut(),
        n,
        written: 0,
    };

    while g.written < n {
        let i = g.written;
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
                // SAFETY: `written == i`; slots `(i, n)` are still init.
                unsafe { g.cleanup() };
                std::mem::forget(g);
                return Err(e);
            },
        }
        g.written = i + 1;
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
/// Replaced the old `Vec::split_off` tree (one allocation + input-half memcpy
/// per internal node); same shape as [`fused_filter_collect`] — the input is
/// shared via ranges (never copied), outputs still concatenate level by level
/// (`extend` moves one side per internal node, O(depth) moves per survivor).
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
    /// budget is ignored. Transient pools resolve through the process-wide
    /// recycling cache ([`ComputePool::new`](crate::ComputePool::new)):
    /// dropped pools park instead of joining, and
    /// `ComputePool::clear_cached_pools` reclaims their threads when they
    /// must really be gone. Streaming knobs (`buffer_size`,
    /// `io_concurrency`, …) have no effect on the fused path.
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
    /// calls. A transient pool is also cheap after its first use (recycled
    /// through the cache, see [`ComputePool::new`](crate::ComputePool::new));
    /// an explicit pool still wins when several pipelines should share one
    /// budget or outlive the cache's parking window.
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
    /// `.collect()` / `.for_each()` time and parks it in the process-wide
    /// recycling cache when the terminal returns (see
    /// [`ComputePool::new`](crate::ComputePool::new)) — after the first call
    /// the same-sized pool is reused, so tight loops no longer pay thread
    /// spawn + priming. An explicit [`Pipe::with_compute_pool`] is still the
    /// right tool when several pipelines should share one budget or the
    /// parked threads must be reclaimed deterministically:
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
    ///
    /// # Panics
    ///
    /// Panics if `factor == 0` — a zero oversubscription factor is never
    /// meaningful (see `PipelineConfig::require_nonzero`).
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = Some(PipelineConfig::require_nonzero(
            factor,
            "with_oversubscribe",
        ));
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

    /// Execute the fused pipeline and reduce the outputs into a single
    /// value — **no output `Vec` is allocated**.
    ///
    /// The aggregation counterpart of [`for_each`](Self::for_each)'s
    /// structural win: each parallel leaf folds its range into one partial
    /// accumulator and the tree combines partials in parallel, instead of
    /// materializing an `n`-slot output buffer (`Vec<O>`) just to fold it
    /// away again. For `.map(f).sum()`-shaped workloads this removes the
    /// output allocation, the `n` slot writes, and the serial fold.
    ///
    /// Filter stages are honoured: items dropped by an upstream filter are
    /// simply not folded.
    ///
    /// `op` should be **associative** — outputs are combined as a
    /// deterministic tree over input order, but the exact association
    /// depends on the split layout (batch size, worker count), so a
    /// non-associative `op` (e.g. float `+` under reordering) may produce
    /// run-to-run differences.
    ///
    /// Returns `None` for an empty input (or when a filter drops every
    /// item).
    ///
    /// ```rust
    /// # use youpipe::pipe;
    /// let max = pipe(0..1000).map(|x: i64| x * 3).reduce(i64::max);
    /// assert_eq!(max, Some(3 * 999));
    /// ```
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `op`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn reduce<F>(self, op: F) -> Option<O>
    where
        F: Fn(O, O) -> O + Send + Sync + 'static,
    {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return None;
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            // Trivial case: plain sequential reduce, no chunk machinery.
            if S::MAY_FILTER {
                return items
                    .into_iter()
                    .filter_map(|item| stages.apply(item))
                    .reduce(op);
            }
            return items
                .into_iter()
                .map(|item| stages.apply_pure(item))
                .reduce(op);
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedReduce(stages, OptionReducer(op));
        par_reduce(items, &rop, plan, pool)
    }

    /// Execute the fused pipeline, folding outputs into an accumulator of a
    /// **different type** — no output `Vec` is allocated (see
    /// [`reduce`](Self::reduce) for the core's shape).
    ///
    /// Each parallel leaf seeds a partial from `init.clone()` and folds its
    /// range with `f`; the tree combines partials with `combine`, and the
    /// driver finally combines the per-chunk partials left-to-right in
    /// input order. For the result to be independent of the split layout,
    /// `f`/`combine` must form an associative pair over the lifted domain —
    /// e.g. `(0, |a, x| a + x, |a, b| a + b)` for sums, or `String`
    /// concatenation pairs if partial order matters (the driver's
    /// left-to-right order keeps concatenation correct for a commutative
    /// combine, and for append-only shapes when each leaf's fold is
    /// order-preserving).
    ///
    /// Returns `init` unchanged for an empty input.
    ///
    /// ```rust
    /// # use youpipe::pipe;
    /// let lens = pipe(["alpha".to_string(), "beta".to_string()]).fold(
    ///     0usize,
    ///     |a, s: String| a + s.len(),
    ///     |a, b| a + b,
    /// );
    /// assert_eq!(lens, 9);
    /// ```
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain, `f` or `combine`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn fold<A, F, C>(self, init: A, f: F, combine: C) -> A
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, O) -> A + Send + Sync + 'static,
        C: Fn(A, A) -> A + Send + Sync + 'static,
    {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if n == 0 || prefers_serial(n, num_threads) {
            // Serial fold: a single accumulator, no per-leaf `init` clones.
            let mut acc = init;
            if S::MAY_FILTER {
                for item in items {
                    if let Some(o) = stages.apply(item) {
                        acc = f(acc, o);
                    }
                }
            } else {
                for item in items {
                    acc = f(acc, stages.apply_pure(item));
                }
            }
            return acc;
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedReduce(stages, FoldReducer { init, f, combine });
        par_reduce(items, &rop, plan, pool)
    }

    /// Execute the fused pipeline and sum the outputs — the zero-copy
    /// `.map(f).sum()` shape (see [`reduce`](Self::reduce)).
    ///
    /// Empty input folds to the additive identity (`0` for numeric types —
    /// [`Sum`](std::iter::Sum) over an empty iterator).
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn sum(self) -> O
    where
        O: std::iter::Sum + Send,
    {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if n == 0 || prefers_serial(n, num_threads) {
            if S::MAY_FILTER {
                return items
                    .into_iter()
                    .filter_map(|item| stages.apply(item))
                    .sum();
            }
            return items.into_iter().map(|item| stages.apply_pure(item)).sum();
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedReduce(stages, SumReducer(PhantomData));
        par_reduce(items, &rop, plan, pool)
    }

    /// Execute the fused pipeline and count the outputs (post-filter) — no
    /// output buffer (see [`reduce`](Self::reduce)). The stage chain still
    /// runs on every item.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn count(self) -> usize {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return 0;
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            if S::MAY_FILTER {
                return items
                    .into_iter()
                    .filter_map(|item| stages.apply(item))
                    .count();
            }
            // The chain still runs on every item (side-effecting maps);
            // `map(..).count()` would draw a clippy flag for ignoring the
            // mapped values, so spell the loop out.
            let mut c = 0usize;
            for item in items {
                let _ = stages.apply_pure(item);
                c += 1;
            }
            return c;
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedReduce(stages, CountReducer);
        par_reduce(items, &rop, plan, pool)
    }

    /// Execute the fused pipeline and return the minimum output —
    /// `reduce(Ord::min)` without naming the op (see [`reduce`](Self::reduce)
    /// for the shape and associativity notes). Returns `None` for an empty
    /// (or fully filtered) input.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain.
    pub fn min(self) -> Option<O>
    where
        O: Ord,
    {
        self.reduce(Ord::min)
    }

    /// Execute the fused pipeline and return the maximum output —
    /// `reduce(Ord::max)` (see [`min`](Self::min)).
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain.
    pub fn max(self) -> Option<O>
    where
        O: Ord,
    {
        self.reduce(Ord::max)
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
    ///
    /// # Panics
    ///
    /// Panics if `factor == 0` (see [`Pipe::with_oversubscribe`]).
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = Some(PipelineConfig::require_nonzero(
            factor,
            "with_oversubscribe",
        ));
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

    /// Execute the fused fallible pipeline and reduce the outputs,
    /// short-circuiting on the first error — the fallible counterpart of
    /// [`Pipe::reduce`] (no output `Vec`; partials combined up the tree).
    ///
    /// On `Err`, every partial accumulator is dropped (the failing chunk's
    /// partials inside its tree, the completed chunks' partials with the
    /// strategy) and the input's unread slots are dropped by the leaf
    /// guards — nothing leaks to the caller. Filter stages are honoured:
    /// items dropped by a filter are not folded and cause no error. `op`
    /// should be associative (see [`Pipe::reduce`]).
    ///
    /// Returns `Ok(None)` for an empty input (or a fully filtered one).
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `op`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn try_reduce<F>(self, op: F) -> Result<Option<O>, E>
    where
        F: Fn(O, O) -> O + Send + Sync + 'static,
    {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        if n == 0 {
            return Ok(None);
        }
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if prefers_serial(n, num_threads) {
            let mut acc: Option<O> = None;
            for item in items {
                if let Some(o) = stages.try_apply(item)? {
                    acc = Some(match acc {
                        Some(a) => op(a, o),
                        None => o,
                    });
                }
            }
            return Ok(acc);
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedTryReduce(stages, OptionReducer(op));
        par_reduce_try(items, &rop, plan, pool)
    }

    /// Execute the fused fallible pipeline, folding outputs into an
    /// accumulator of a different type — the fallible counterpart of
    /// [`Pipe::fold`] (see it for the associativity contract). The first
    /// `Err` short-circuits (see [`try_reduce`](Self::try_reduce) for the
    /// failure-path cleanup).
    ///
    /// Returns `Ok(init)` for an empty input.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain, `f` or `combine`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn try_fold<A, F, C>(self, init: A, f: F, combine: C) -> Result<A, E>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, O) -> A + Send + Sync + 'static,
        C: Fn(A, A) -> A + Send + Sync + 'static,
    {
        let items = self.items;
        let stages = self.stages;
        let n = items.len();
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        let num_threads = pool.num_workers();
        if n == 0 || prefers_serial(n, num_threads) {
            // Serial fold: a single accumulator, no per-leaf `init` clones.
            let mut acc = init;
            for item in items {
                if let Some(o) = stages.try_apply(item)? {
                    acc = f(acc, o);
                }
            }
            return Ok(acc);
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let rop = FusedTryReduce(stages, FoldReducer { init, f, combine });
        par_reduce_try(items, &rop, plan, pool)
    }
}

// ── RangePipe (zero-materialization pipeline over a generated index range) ──

/// Data-first entry point over an index range — the zero-materialization
/// counterpart of `pipe(range)`. Items are *generated* inside the parallel
/// leaves (the item at index `i` is `i`), so no input `Vec` is ever
/// allocated or serially filled on the calling thread: `pipe(0..n)` pays a
/// serial O(n) iota fill before the parallel phase starts, which measures
/// 56–70 % of the whole owned call at 1 M/4 M items (see
/// docs/src/dev/benchmarks.md "Input materialization").
///
/// ```rust
/// # use youpipe::pipe_range;
/// let result: Vec<usize> = pipe_range(0..1000).map(|i: usize| i * 2).collect();
/// assert_eq!(result.len(), 1000);
/// assert_eq!(result[7], 14);
/// ```
#[must_use]
pub fn pipe_range(range: Range<usize>) -> RangePipe<Identity, usize> {
    RangePipe {
        range,
        stages: Identity,
        config: PipelineConfig::default(),
        compute_pool: None,
        oversubscribe: None,
        _marker: PhantomData,
    }
}

/// [`pipe_range`]'s builder. Mirrors [`Pipe`]'s builder surface (`.map` /
/// `.filter` / `.try_map` / tuning setters — identical semantics, input type
/// fixed to `usize`), but the filter-free `.collect()` / `.for_each()`
/// terminals run the generation core: items are produced inside the leaves
/// and no input buffer exists. Chains that *can* filter, and the fallible
/// `.try_collect()` terminal, materialize the indices once at the terminal —
/// the same serial fill `pipe(range)` always paid.
pub struct RangePipe<S = Identity, O = usize> {
    range: Range<usize>,
    stages: S,
    config: PipelineConfig,
    /// Custom compute pool — see [`Pipe::with_compute_pool`].
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor — see [`Pipe::with_oversubscribe`].
    oversubscribe: Option<NonZeroUsize>,
    _marker: PhantomData<O>,
}

impl<S, O> RangePipe<S, O> {
    /// Override the default [`PipelineConfig`] — see [`Pipe::with_config`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Tune the workload split factor — see [`Pipe::with_workload`].
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

    /// Oversubscribe the compute pool — see [`Pipe::with_oversubscribe`].
    ///
    /// # Panics
    ///
    /// Panics if `factor == 0` (see [`Pipe::with_oversubscribe`]).
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = Some(PipelineConfig::require_nonzero(
            factor,
            "with_oversubscribe",
        ));
        self
    }
}

impl<S, O> RangePipe<S, O>
where
    S: StageMarker<usize, Output = O>,
    O: Send + 'static,
{
    /// Append a synchronous map stage: `Fn(O) -> N`. The output type
    /// changes to `N`; the input stays the generated index.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Send + Sync + 'static,
    ) -> RangePipe<SyncMap<S, impl Fn(O) -> N + Send + Sync + 'static>, N>
    where
        N: Send + 'static,
    {
        RangePipe {
            range: self.range,
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

    /// Append a filter stage — see [`Pipe::filter`]. Keeps items where `f`
    /// returns `true`. Filter chains materialize the indices at the
    /// terminal (see the [`RangePipe`] type doc).
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Send + Sync + 'static,
    ) -> RangePipe<Filter<S, impl Fn(&O) -> bool + Send + Sync + 'static>, O> {
        RangePipe {
            range: self.range,
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

    /// Append a fallible map stage — see [`Pipe::try_map`]. Transitions into
    /// a [`TryPipe`] whose items are the materialized indices (the fallible
    /// terminal has no generation core yet — the fallback equals
    /// `pipe(range).try_map(..)`).
    #[allow(clippy::type_complexity)] // mirrors `Pipe::try_map`'s typestate chain
    pub fn try_map<N, E>(
        self,
        f: impl Fn(O) -> Result<N, E> + Send + Sync + 'static,
    ) -> TryPipe<
        TryMap<InfallibleChain<S, E>, impl Fn(O) -> Result<N, E> + Send + Sync + 'static>,
        usize,
        N,
        E,
    >
    where
        E: Send + 'static,
        N: Send + 'static,
    {
        TryPipe {
            items: self.range.collect(),
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

impl<S, O> RangePipe<S, O>
where
    S: FusedStage<usize, Output = O> + Send + Sync + 'static,
    O: Send + 'static,
{
    /// Execute the fused pipeline over the generated indices and collect the
    /// results — the generation core (no input buffer) when the chain cannot
    /// filter, the materialized [`Pipe::collect`] path otherwise.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain (after the leaves'
    /// cleanup guards dropped all partial state).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn collect(self) -> Vec<O> {
        let n = self.range.len();
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
            // Trivial case — mirrors `Pipe::collect`'s serial arm, items
            // generated inline by the range iterator itself.
            if S::MAY_FILTER {
                return self.range.filter_map(|i| self.stages.apply(i)).collect();
            }
            return self.range.map(|i| self.stages.apply_pure(i)).collect();
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        if S::MAY_FILTER {
            // Filters change output cardinality (the range-tree path needs
            // materialized items): fill once, then the shared filter core.
            fused_filter_collect(self.range.collect(), &self.stages, plan.depth, pool)
        } else {
            let op = FusedOp(self.stages);
            par_range_gen_collect(self.range, &op, plan, pool)
        }
    }

    /// Execute the fused pipeline over the generated indices, applying `f` to
    /// each output for its side effect — no output `Vec`, no input buffer.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `f`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn for_each<F>(self, f: F)
    where
        F: Fn(O) + Send + Sync + 'static,
    {
        let n = self.range.len();
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
            if S::MAY_FILTER {
                for i in self.range {
                    if let Some(o) = self.stages.apply(i) {
                        f(o);
                    }
                }
            } else {
                for i in self.range {
                    let o = self.stages.apply_pure(i);
                    f(o);
                }
            }
            return;
        }
        let plan = SplitPlan::new(n, num_threads, self.config.workload);
        let op = FusedSink(self.stages, f);
        if S::MAY_FILTER {
            // `FusedSink::consume` honours filters, but the sink generation
            // core has no input buffer to read items from — materialize.
            par_for_each(self.range.collect(), &op, plan, pool);
        } else {
            par_range_gen_for_each(self.range, &op, plan, pool);
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
// Panic-safety simplification is structural: a borrowed input is always
// init and never ours to drop, so every leaf guard runs `LeafCleanup` with
// its input half disabled — only output slots need dropping on unwind.

/// Borrowed-input leaf: process `input` sequentially, applying `op` to each
/// `&E` and writing outputs by index. Counterpart of [`par_index_leaf`] with
/// the `ptr::read` move-out replaced by a shared borrow — LLVM sees the same
/// read-8B / compute / write-8B loop shape, so the vectorized code matches.
///
/// Panic safety: the [`LeafCleanup`] guard drops only the init
/// `output[..written]` slots (a borrowed input is always init and never ours
/// to drop).
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn par_index_leaf_by_ref<'i, E, R, OP>(input: &'i [E], output: &mut [R], op: &OP, nt: bool)
where
    OP: RangeOp<&'i E, Out = R>,
{
    // Unwind cleanup — `LeafCleanup` with the output half only (a borrowed
    // input is always init and never ours to drop); `output[..written]` has
    // no holes because `RangeOp` never filters.
    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafCleanup::<E, R, true, false> {
        in_ptr,
        out_ptr,
        n,
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
        let leaf = |input: &&'i [E], start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively. Input is shared + init;
            // output is uninit.
            let in_slice = unsafe { input.get_unchecked(start..end) };
            let out_slice = unsafe { self.output.as_mut_slice(start, end) };
            par_index_leaf_by_ref(in_slice, out_slice, self.op, self.nt);
            Ok(())
        };
        let drop_success_range = |start: usize, end: usize| {
            // SAFETY (hook contract): only called for completed ranges, so
            // those output slots are fully init and safe to drop.
            unsafe { self.output.drop_range(start, end) };
        };
        par_tree_rec(pool, input, start, end, splits, &leaf, &drop_success_range)
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

/// Drive the borrowed index core over a `&'i [E]` and convert the
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
        let leaf = |input: &&'i [E], start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively; the input slice is shared and
            // init.
            let in_slice = unsafe { input.get_unchecked(start..end) };
            par_for_each_leaf_by_ref(in_slice, self.op);
            Ok(())
        };
        // Sink-only: no output buffer, borrowed input needs nothing.
        let noop = |_start: usize, _end: usize| {};
        par_tree_rec(pool, input, start, end, splits, &leaf, &noop)
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

/// Drive the borrowed sink core over a `&'i [E]`. Counterpart of
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
    // Unwind cleanup — same output-only `LeafCleanup` as the infallible
    // borrowed leaf (see `par_index_leaf_by_ref`).
    debug_assert_eq!(input.len(), output.len());

    let in_ptr = input.as_ptr();
    let out_ptr = output.as_mut_ptr();
    let n = input.len();

    let mut g = LeafCleanup::<E, R, true, false> {
        in_ptr,
        out_ptr,
        n,
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
                // SAFETY: `written == i`; `output[..i)` holds live values.
                unsafe { g.cleanup() };
                std::mem::forget(g);
                return Err(e);
            },
        }
    }

    // Success: disarm the cleanup Drop.
    std::mem::forget(g);
    Ok(())
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
        let leaf = |input: &&'i [E], start: usize, end: usize| {
            // SAFETY: disjoint range — the caller (driver or internal node)
            // owns `[start, end)` exclusively. Input is shared + init;
            // output is uninit.
            let in_slice = unsafe { input.get_unchecked(start..end) };
            let out_slice = unsafe { self.output.as_mut_slice(start, end) };
            par_index_try_leaf_by_ref(in_slice, out_slice, self.op).map_err(TryFailure::Error)
        };
        let drop_success_range = |start: usize, end: usize| {
            // SAFETY (hook contract): only called for completed ranges, so
            // those output slots are fully init and safe to drop.
            unsafe { self.output.drop_range(start, end) };
        };
        par_tree_rec(pool, input, start, end, splits, &leaf, &drop_success_range)
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

/// Drive the borrowed fallible index core over a `&'i [E]`. Counterpart of
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
        ErasedFailure::Op(b) => match b.downcast::<F>() {
            Ok(e) => TryFailure::Error(*e),
            Err(_) => unreachable!("try strategy only records op failures of F"),
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
            // Recursion already dropped every live output slot; resume the
            // panic for the caller.
            drop(output);
            panic::resume_unwind(p);
        },
    }
}

/// Borrowed-input merge-based collect for fused stages that may filter —
/// counterpart of [`fused_filter_collect`] with the input shared by index
/// ranges instead of `Vec::split_off`. The output side still concatenates
/// level by level (`extend` may reserve + memcpy one side per internal node,
/// O(depth) moves per surviving item).
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
        // Pre-allocate for the all-survive worst case, matching the owned
        // leaves and `join_fused_try_collect_by_ref` (rayon does the same).
        let mut out = Vec::with_capacity(end - start);
        for item in &input[start..end] {
            if let Some(o) = stages.apply(item) {
                out.push(o);
            }
        }
        return out;
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

// ── By-ref filter collect variants: merge tree / count-then-place /
//    write-then-compact (YOUPIPE_FILTER_COLLECT) ──
//
// Count-then-place (ctp): two-pass alternative to the merge tree for
// `pipe_ref(..).filter(..)` collects. Pass 1 counts survivors per leaf, a
// sequential scan turns the counts into output offsets, pass 2 re-runs the
// chain and writes survivors straight into ONE exactly-sized output buffer
// — every survivor moves exactly once, no per-leaf `Vec` allocations, no
// tree merges. Trade-offs:
//   * stage closures run TWICE over the input — a loss for expensive stages and an observable
//     difference for side-effecting (interior-mutable) closures;
//   * two fork/join waves instead of one;
//   * flat in selectivity: the merge tree's cost scales with survivors (O(depth) `extend` moves
//     each), this path's with the input.
//   Panic safety mirrors the index cores: pass 2's tree runs on
//   `join_captured` + `SiblingGuard` and drops each completed sibling's leaf
//   range (a contiguous output image via the prefix-sum `bounds` with a
//   `total` sentinel) before letting a panic ride on; pass 1 owns no shared
//   buffer (survivor temporaries drop in the leaf loop), so its plain `join`
//   needs no cleanup.
// Same-binary knob A/B (5 interleaved rounds, 32-core, 100K borrowed):
// keep90 −50.5 %, keep50 −37.1 %, keep33 −31.1 % (all 25/25 stable); keep10
// +41.3 %, every 10K shape +28…+75 % (all 0/25) — crossover ≈ 25 %
// selectivity at 100K. Owned/try filter paths keep the merge tree: their
// stages consume items by value, so a count pass cannot re-run them. See
// docs/src/dev/benchmarks.md (filter-chain collect).

/// Which implementation by-ref filter collects use. Parsed once from
/// `YOUPIPE_FILTER_COLLECT`: unset/`"merge"` → merge tree (default), `"ctp"`
/// → count-then-place, `"wtc"` → write-then-compact. Invalid values panic at
/// first use — see `nt_store_policy` for why failing loudly beats silently
/// misreading a knob.
///
/// Shape guidance (three-sided A/B, docs/src/dev/benchmarks.md filter-chain
/// collect): merge wins small/mid batches at mid/high selectivity; wtc wins
/// ≥100K batches at any selectivity and small batches; ctp wins ≥100K
/// batches keeping ≳30 % — the merge default is only wrong for ≥100K, which
/// is exactly where the knobs are for.
#[derive(Clone, Copy)]
enum FilterCollectVariant {
    Merge,
    Ctp,
    Wtc,
}

fn filter_collect_variant() -> FilterCollectVariant {
    static VARIANT: OnceLock<FilterCollectVariant> = OnceLock::new();
    *VARIANT.get_or_init(|| match std::env::var("YOUPIPE_FILTER_COLLECT") {
        Err(_) => FilterCollectVariant::Merge,
        Ok(v) => match v.as_str() {
            "merge" => FilterCollectVariant::Merge,
            "ctp" => FilterCollectVariant::Ctp,
            "wtc" => FilterCollectVariant::Wtc,
            other => panic!(
                "YOUPIPE_FILTER_COLLECT: invalid value {other:?} (leave unset or \"merge\" for \
                 the merge tree, \"ctp\" for count-then-place, \"wtc\" for write-then-compact)"
            ),
        },
    })
}

/// Number of leaves the range-split recursion produces for a `len`-item
/// range with `splits` levels left — mirrors the `mid = len / 2` split of
/// [`par_filter_rec`] exactly, so leaf ordinals can be threaded top-down
/// (left subtree first) without touching the tree itself.
fn split_leaf_count(len: usize, splits: usize) -> usize {
    if splits == 0 || len <= 1 {
        1
    } else {
        split_leaf_count(len - len / 2, splits - 1) + split_leaf_count(len / 2, splits - 1)
    }
}

/// Pass 1: count this subtree's survivors into `counts[leaf_base + i]` (one
/// Relaxed store per leaf — disjoint indices, synchronized by `pool.join`
/// before the scan reads them). Outputs produced by `apply` drop here.
fn count_filter_rec<'i, S, E>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    start: usize,
    end: usize,
    splits_left: usize,
    counts: &[AtomicUsize],
    leaf_base: usize,
) -> usize
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
{
    if splits_left == 0 || end - start <= 1 {
        let mut c = 0;
        for item in &input[start..end] {
            if stages.apply(item).is_some() {
                c += 1;
            }
        }
        counts[leaf_base].store(c, Ordering::Relaxed);
        return c;
    }
    let mid = start + (end - start) / 2;
    let left_leaves = split_leaf_count(mid - start, splits_left - 1);
    let (l, r) = pool.join(
        || {
            count_filter_rec(
                pool,
                input,
                stages,
                start,
                mid,
                splits_left - 1,
                counts,
                leaf_base,
            )
        },
        || {
            count_filter_rec(
                pool,
                input,
                stages,
                mid,
                end,
                splits_left - 1,
                counts,
                leaf_base + left_leaves,
            )
        },
    );
    l + r
}

/// Pass 2: write leaf `leaf_base`'s survivors into the leaf's slice of the
/// shared output buffer (`bounds`/`counts` from the scan). Panic safety (same
/// shape as [`par_tree_rec`]): a panicking leaf drops its own written prefix
/// via the guard, the internal-node match below drops every completed
/// sibling's leaf range before letting the panic ride on, and the unwind-only
/// [`SiblingGuard`] covers the one self-run-B escape — so by the time a panic
/// leaves this recursion, the subtree's output slots are all uninit again and
/// the driver's `Slots` drop (a memory free) leaks nothing.
struct FilterPlaceCtx<'i, 'a, S, E>
where
    S: FusedStage<&'i E>,
{
    input: &'i [E],
    stages: &'a S,
    output: &'a Slots<S::Output>,
    /// `bounds[i]` = leaf i's output start, `bounds[leaves]` = `total` — the
    /// sentinel closes the last leaf, so any leaf range `[lo, hi)` maps to the
    /// contiguous output range `[bounds[lo], bounds[hi])` (prefix-sum
    /// geometry). That contiguous image is what the panic cleanup drops.
    bounds: &'a [usize],
    counts: &'a [usize],
}

/// Recursive invariant: if a panic escapes [`place_filter_rec`] for the leaf
/// range `[leaf_base, leaf_base + leaves)`, every output slot that subtree
/// ever wrote has been dropped (leaf guard for the partial one, the join match
/// for completed siblings) — the buffer is left as if the subtree never ran.
fn place_filter_rec<'i, S, E>(
    pool: &ComputePool,
    ctx: &FilterPlaceCtx<'i, '_, S, E>,
    start: usize,
    end: usize,
    splits_left: usize,
    leaf_base: usize,
) where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    let (input, stages, output, bounds, counts) =
        (ctx.input, ctx.stages, ctx.output, ctx.bounds, ctx.counts);
    if splits_left == 0 || end - start <= 1 {
        let off = bounds[leaf_base];
        let cnt = counts[leaf_base];
        // SAFETY: this leaf owns the disjoint output range
        // `[off, off + cnt)` exclusively; the slots are uninit.
        // Raw pointer only (no `out_slice[...]` accesses): the guard's
        // `out_ptr` derives from the `&mut [R]`, and any write through the
        // slice itself is a FOREIGN write that disables that derived tag
        // under Tree Borrows — the unwind cleanup would then reborrow-UB
        // (found by miri on the panic-accounting test; same reason the
        // other leaves stick to raw pointers).
        let out_ptr = unsafe { output.as_mut_slice(off, off + cnt) }.as_mut_ptr();
        let in_ptr = input[start..end].as_ptr();
        // Unwind cleanup — output half only, same shape as the borrowed
        // collect leaf (`LeafCleanup`, see `par_index_leaf_by_ref`).
        let mut g = LeafCleanup::<E, S::Output, true, false> {
            in_ptr,
            out_ptr,
            n: end - start,
            written: 0,
        };
        for i in 0..(end - start) {
            // SAFETY: shared read of input slot i (borrowed input, no moves).
            let item = unsafe { &*in_ptr.add(i) };
            if let Some(o) = stages.apply(item) {
                // Bounds-checked RAW write (NOT `=`): slice assignment drops
                // the old value first, but these slots are uninit — a Drop
                // output type would run drop glue over garbage bits (SIGSEGV,
                // found by the panic-accounting test). The bounds check
                // makes a non-deterministic predicate that keeps more items
                // in pass 2 than pass 1 counted panic here, not write past
                // the leaf's slice.
                assert!(
                    g.written < cnt,
                    "non-deterministic filter predicate: pass 2 kept more items than pass 1 \
                     counted"
                );
                // SAFETY: slot `written` is inside the leaf's `[0, cnt)`
                // slice (asserted above) and still uninit.
                unsafe { ptr::write(out_ptr.add(g.written), o) };
                g.written += 1;
            }
        }
        debug_assert_eq!(g.written, cnt);
        // Success: disarm the cleanup Drop.
        std::mem::forget(g);
        return;
    }
    let mid = start + (end - start) / 2;
    let left_leaves = split_leaf_count(mid - start, splits_left - 1);
    let leaves = left_leaves + split_leaf_count(end - mid, splits_left - 1);
    // Drop the completed subtree's slice of the shared buffer. Shared by the
    // unwind backstop and the match below (the `par_tree_rec` sibling-cleanup
    // contract, mapped from index ranges to leaf ranges).
    let drop_leaf_range = |lo: usize, hi: usize| {
        // SAFETY (leaf geometry): `bounds` is a prefix sum with a `total`
        // sentinel, so `[bounds[lo], bounds[hi])` is exactly leaf range
        // `[lo, hi)`'s contiguous slice; a subtree that returned Ok filled
        // every slot in it (deterministic predicate contract — pass 1 and
        // pass 2 observe the same survivors).
        unsafe { output.drop_range(bounds[lo], bounds[hi]) };
    };
    // Unwind backstop for the one panic `join_captured` lets escape: a
    // self-run B (see `join_on_captured`'s self-pop branch). Any panic that
    // unwinds past the join call below therefore implies A completed
    // successfully; the guard drops exactly the left leaf range, then lets
    // the unwind continue (B's own recursion already zeroed its side — the
    // invariant on `place_filter_rec`).
    let g = SiblingGuard {
        start: leaf_base,
        mid: leaf_base + left_leaves,
        drop_success_range: &drop_leaf_range,
    };
    let (l, r) = pool.join_captured(
        || place_filter_rec(pool, ctx, start, mid, splits_left - 1, leaf_base),
        || {
            place_filter_rec(
                pool,
                ctx,
                mid,
                end,
                splits_left - 1,
                leaf_base + left_leaves,
            );
        },
    );
    std::mem::forget(g);
    match (l, r) {
        (Ok(()), Ok(())) => {},
        // Panicking sides zeroed themselves (invariant); a completed sibling
        // is dropped here before the panic rides on. Among two panics the
        // left (first) payload wins — join's own both-panic ordering.
        (Err(p), Err(_)) => unwind::resume_unwinding(p),
        (Err(p), Ok(())) => {
            drop_leaf_range(leaf_base + left_leaves, leaf_base + leaves);
            unwind::resume_unwinding(p);
        },
        (Ok(()), Err(p)) => {
            drop_leaf_range(leaf_base, leaf_base + left_leaves);
            unwind::resume_unwinding(p);
        },
    }
}

/// Drive the two-pass count-then-place filter collect (see the section
/// comment). `depth` is the same split budget the merge tree would use.
fn fused_filter_collect_by_ref_ctp<'i, S, E>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    depth: usize,
) -> Vec<S::Output>
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    let n = input.len();
    let leaves = split_leaf_count(n, depth);
    let counts: Vec<AtomicUsize> = (0..leaves).map(|_| AtomicUsize::new(0)).collect();
    count_filter_rec(pool, input, stages, 0, n, depth, &counts, 0);

    // Sequential scan over ≤ a few hundred leaf counts: bounds[i] is leaf
    // i's start in the shared output buffer, `total` its exact length. The
    // trailing `total` sentinel closes the last leaf (see
    // `FilterPlaceCtx::bounds`) so the panic cleanup can drop any leaf range
    // as one contiguous `drop_range` call.
    let mut total = 0;
    let mut bounds: Vec<usize> = Vec::with_capacity(leaves + 1);
    for c in &counts {
        bounds.push(total);
        total += c.load(Ordering::Relaxed);
    }
    bounds.push(total);
    let counts: Vec<usize> = counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();

    let output = Slots::uninit(total);
    let ctx = FilterPlaceCtx {
        input,
        stages,
        output: &output,
        bounds: &bounds,
        counts: &counts,
    };
    place_filter_rec(pool, &ctx, 0, n, depth, 0);
    // Every output slot is init (each leaf filled exactly its count).
    output.into_vec()
}

// ── Write-then-compact by-ref filter collect (YOUPIPE_FILTER_COLLECT=wtc) ──
//
// Single-pass alternative to both the merge tree and count-then-place: every
// leaf runs the chain exactly ONCE over its input range `[start, end)` and
// writes its survivors contiguously into the LOW end of its slice of ONE
// n-slot output buffer (`[start, start + kept)`), recording `(start, kept)`
// per leaf. A compaction pass then prefix-sums the kept counts into output
// offsets and block-copies each leaf's contiguous segment down to its final
// position (`ptr::copy`, memmove semantics — one segment's dst/src ranges
// can overlap); a segment whose offset already equals its start (nothing
// before it was filtered out) skips the copy entirely, so an all-survive
// chain compacts to zero copies. The copies run sequentially on the driver
// for small payloads and through a parallel join-tree wave for large ones
// ([`WTC_PARALLEL_COMPACT_MIN_BYTES`]): the sequential sweep is bound by
// cross-core cache-line transfers (the segments were just written by every
// worker, so the driver pulls each line from a remote cache — measured
// ~4-5 µs per 72 KB, ~55 µs per 720 KB), and distributing the segment
// copies over the pool amortizes that below one extra wave. The parallel
// wave copies into a SEPARATE exactly-sized destination buffer — an
// in-place downward parallel sweep is NOT interleaving-safe: a later
// segment's destination can land inside an earlier segment's still-unread
// source whenever that earlier leaf kept fewer items than the gaps before
// it (sequential in-order sweeps are safe because sources are consumed
// before later writes; found by test, not by review).
//
//   * vs the merge tree: ONE allocation instead of one per leaf plus extend reallocs; each survivor
//     is written once and moved at most once (the tree moves it O(depth) times through
//     `l.extend(r)`).
//   * vs count-then-place: the chain runs ONCE — no observable double side effects, no second
//     fork/join wave (ctp's 10K and low-selectivity regressions) — at the price of an n-sized (not
//     survivor-sized) output buffer and one survivor-payload memmove on the driver thread. Output
//     stores are plain (never NT): the compaction re-reads them immediately.
//
// Final three-sided same-binary knob A/B (5 interleaved rounds, 32-core,
// rayon drift controls within ±4 %): vs the merge tree wtc wins at 100K
// every selectivity (keep33 −24 %, keep50 −31 %, keep90 −38 %, all 25/25;
// keep10 −0.1 % noise) and at 1K (−12.8 %), ties at 10K low selectivity,
// and regresses at 10K mid/high selectivity (keep90 +25 %, 0/25 — the
// survivor payload sits under the parallel-compaction threshold, so the
// driver-sequential sweep pays cross-core transfers for lines the workers
// just wrote; a size gate would buy only that window). ctp keeps the ≥100K
// mid/high-selectivity crown (−10…−23 % vs wtc) and loses everywhere else.
// Verdict: merge stays the DEFAULT; all three live behind the
// `YOUPIPE_FILTER_COLLECT` knob (merge for small/mid batches at mid/high
// selectivity, wtc for ≥100K at any selectivity and for small batches,
// ctp for ≥100K batches keeping ≳30 %). See docs/src/dev/benchmarks.md
// (filter-chain collect).
//
// Survivors land leaf-contiguous, NOT at their exact input indices. An
// exact-index scheme would scatter writes (at low selectivity nearly every
// survivor would touch a fresh cache line) and its compaction could not
// recover segment boundaries from per-leaf counts alone — it would need
// per-run metadata. Leaf-contiguous writes keep every leaf's stores
// sequential (cache-line dense at any selectivity) and make the compaction
// blockwise: the only memory-bandwidth cost of low selectivity is the
// n-sized buffer allocation itself, never touched beyond `kept` slots.
//
// Owned/try filter paths keep the merge tree. Owned is mechanically
// possible (a wtc leaf would `ptr::read` each input slot once and panic
// cleanup mirrors `FilterGuard`) but is deferred: this round pins down the
// borrowed caliber, where the merge tree's output-side cost was measured.
// The try path additionally entangles `Err` short-circuit with the meta
// cleanup walk — the merge tree already short-circuits cheaply there.

/// Per-leaf record for the compaction sweep: where the leaf's input range
/// starts and how many survivors it wrote at the low end of that range.
/// Each slot is written by exactly one leaf — the `split_at_mut` threading
/// makes the disjoint ownership a compile-time property — and read by the
/// driver only after the join tree completed, so plain non-atomic fields
/// are race-free (the join latch provides the happens-before edge).
#[derive(Clone, Copy)]
struct WtcLeafMeta {
    start: usize,
    kept: usize,
}

/// Leaf guard: drops this leaf's written survivor prefix on unwind. The
/// panicking leaf never reaches its meta store (stored only after the loop),
/// so the driver's panic-path meta walk skips it — no double drop. Raw
/// pointer for the same Tree Borrows reason as `PlaceLeafGuard`.
struct WtcLeafGuard<R> {
    out_ptr: *mut R,
    written: usize,
}

impl<R> Drop for WtcLeafGuard<R> {
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

/// The single stage pass: run the chain over `input[start..end)` and write
/// survivors contiguously into `output[start..start + kept)`, recording
/// `(start, kept)` in `meta`. `meta` covers exactly this subtree's leaves in
/// input order; the root pass owns the whole array and each internal node
/// splits it in half alongside the input range.
fn write_filter_rec<'i, S, E>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    start: usize,
    end: usize,
    splits_left: usize,
    output: &Slots<S::Output>,
    meta: &mut [WtcLeafMeta],
) where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    if splits_left == 0 || end - start <= 1 {
        let in_slice = &input[start..end];
        // SAFETY: this leaf owns the disjoint output range `[start, end)`
        // exclusively; the slots are uninit.
        let out_slice = unsafe { output.as_mut_slice(start, end) };
        let out_ptr = out_slice.as_mut_ptr();
        let mut g = WtcLeafGuard {
            out_ptr,
            written: 0,
        };
        for item in in_slice {
            if let Some(o) = stages.apply(item) {
                // SAFETY: `written` counts survivors among the items scanned
                // so far, so the store stays inside this leaf's slice with
                // no bounds check — even a keep-everything predicate cannot
                // overflow it (written <= items scanned <= slice len).
                // Slot `written` is uninit; disjoint index.
                unsafe { ptr::write(g.out_ptr.add(g.written), o) };
                g.written += 1;
            }
        }
        meta[0] = WtcLeafMeta {
            start,
            kept: g.written,
        };
        // Success: disarm the cleanup Drop.
        std::mem::forget(g);
        return;
    }
    let mid = start + (end - start) / 2;
    let left_leaves = split_leaf_count(mid - start, splits_left - 1);
    let (lmeta, rmeta) = meta.split_at_mut(left_leaves);
    pool.join(
        || {
            write_filter_rec(
                pool,
                input,
                stages,
                start,
                mid,
                splits_left - 1,
                output,
                lmeta,
            );
        },
        || {
            write_filter_rec(
                pool,
                input,
                stages,
                mid,
                end,
                splits_left - 1,
                output,
                rmeta,
            );
        },
    );
}

/// [`WTC_PARALLEL_COMPACT_MIN_BYTES`]'s survivor-payload threshold, from
/// which the compaction copies go through a parallel join-tree wave instead
/// of the driver-sequential sweep. Fixed, not probed: the sequential sweep's
/// cost is cross-core cache-line transfers (~15-20 GB/s effective), the
/// parallel wave's is one fork/join ramp (~10 µs measured on this class of
/// tree) — break-even sits near a few hundred KB.
const WTC_PARALLEL_COMPACT_MIN_BYTES: usize = 256 << 10;

/// One leaf's compaction assignment: move `len` survivors from the leaf's
/// write position `src` down to its final position `dst` (`dst <= src`
/// always — offsets trail input positions by the filtered-out count).
#[derive(Clone, Copy)]
struct WtcSeg {
    src: usize,
    dst: usize,
    len: usize,
}

/// Parallel block-copy wave of the compaction (large payloads): each leaf
/// copies its batch of [`WtcSeg`]s from the stage pass's n-slot `src_buffer`
/// into the separate exactly-sized `dst_buffer`. Distinct allocations make
/// the segment copies unconditionally interleaving-safe (an in-place
/// downward sweep is not — see the section comment); the copies cannot
/// panic, so no cleanup exists.
fn compact_filter_rec<R>(
    pool: &ComputePool,
    src_buffer: &Slots<R>,
    dst_buffer: &Slots<R>,
    segs: &[WtcSeg],
    start: usize,
    end: usize,
    splits_left: usize,
) where
    R: Send,
{
    if splits_left == 0 || end - start <= 1 {
        let src_base = src_buffer.base_ptr();
        let dst_base = dst_buffer.base_ptr();
        for s in &segs[start..end] {
            if s.len > 0 {
                // SAFETY: `[s.src, s.src + s.len)` is fully init (completed
                // leaf's survivors); the destinations of distinct segments
                // are disjoint, and src/dst live in distinct allocations.
                unsafe {
                    ptr::copy_nonoverlapping(src_base.add(s.src), dst_base.add(s.dst), s.len);
                };
            }
        }
        return;
    }
    let mid = start + (end - start) / 2;
    pool.join(
        || {
            compact_filter_rec(
                pool,
                src_buffer,
                dst_buffer,
                segs,
                start,
                mid,
                splits_left - 1,
            );
        },
        || {
            compact_filter_rec(
                pool,
                src_buffer,
                dst_buffer,
                segs,
                mid,
                end,
                splits_left - 1,
            );
        },
    );
}

/// Drive the single-pass write-then-compact filter collect (see the section
/// comment). `depth` is the same split budget the merge tree would use.
///
/// # Panics
///
/// Propagates any panic raised by `stages` after dropping every live output
/// slot (unlike ctp's place pass, where completed siblings' outputs leak —
/// see the walk below for why wtc can afford full cleanup).
fn fused_filter_collect_by_ref_wtc<'i, S, E>(
    pool: &ComputePool,
    input: &'i [E],
    stages: &S,
    depth: usize,
) -> Vec<S::Output>
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
{
    let n = input.len();
    let leaves = split_leaf_count(n, depth);
    let output = Slots::uninit(n);
    let mut meta = vec![WtcLeafMeta { start: 0, kept: 0 }; leaves];

    // `join` re-raises the first leaf panic only after every sibling
    // completed, so at the catch point each leaf has EITHER stored its meta
    // (fully written survivors) OR unwound through its guard (partial prefix
    // dropped, meta still the zero initial value — `kept == 0` makes the
    // walk skip it without touching `start`). The walk therefore drops
    // exactly the live survivors: nothing leaks, nothing drops twice.
    // ctp could not do this cheaply because its place pass has no record of
    // WHICH leaves finished before the panic; wtc's meta store IS that
    // record.
    let result = unwind::halt_unwinding(|| {
        write_filter_rec(pool, input, stages, 0, n, depth, &output, &mut meta);
    });
    if let Err(p) = result {
        for m in &meta {
            // SAFETY: a stored meta means the leaf completed —
            // `[m.start, m.start + m.kept)` is fully init.
            unsafe { output.drop_range(m.start, m.start + m.kept) };
        }
        // All live slots dropped: freeing the buffer just frees memory.
        drop(output);
        unwind::resume_unwinding(p);
    }

    // Compaction: prefix-sum the kept counts into final offsets (`dst`
    // trails `src` monotonically — after the first filtered-out item, every
    // later segment shifts down by the cumulative filtered count), then move
    // the segments. Sequential on the driver for small payloads; one
    // parallel copy wave for large ones (see the section comment).
    let mut total = 0;
    let segs: Vec<WtcSeg> = meta
        .iter()
        .map(|m| {
            let s = WtcSeg {
                src: m.start,
                dst: total,
                len: m.kept,
            };
            total += m.kept;
            s
        })
        .collect();

    if total.saturating_mul(size_of::<S::Output>()) >= WTC_PARALLEL_COMPACT_MIN_BYTES {
        let dest = Slots::uninit(total);
        compact_filter_rec(pool, &output, &dest, &segs, 0, segs.len(), depth);
        // The n-slot buffer now holds only moved-from stale bits — freeing
        // it drops nothing. The destination is exactly survivor-sized.
        drop(output);
        return dest.into_vec();
    }
    // Sequential in-place sweep, safe because the segments are consumed in
    // input order: each copy's write region ends at `s.dst + s.len`, at most
    // the next segment's `src`, so nothing still to be read is clobbered.
    let base = output.base_ptr();
    for s in &segs {
        if s.len > 0 && s.dst != s.src {
            // SAFETY: `[s.src, s.src + s.len)` is fully init (completed
            // leaf); `ptr::copy` is memmove — one segment's own dst/src
            // ranges can overlap.
            unsafe { ptr::copy(base.add(s.src), base.add(s.dst), s.len) };
        }
    }
    // `[0, total)` holds the survivors in input order; the tail is
    // moved-from stale bits that `into_vec_with_len` never drops. The Vec
    // keeps the n-slot capacity (len == survivors) — shrink is left to the
    // caller; a copy to shrink would defeat the single-move design.
    output.into_vec_with_len(total)
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

/// `pub(crate)` entry point for the scoped reduce terminal — the
/// [`fused_collect_scoped`] counterpart: same dispatch logic as
/// `Pipe::reduce` without the `'static` bounds on the stage chain (driven
/// by `crate::scope::ScopedPipe::reduce`). Soundness rests on the same
/// `ComputePool::join` invariant as [`fused_collect_scoped`].
pub(crate) fn fused_reduce_scoped<S, T, F>(
    items: Vec<T>,
    stages: S,
    op: F,
    workload: Workload,
    pool: &ComputePool,
) -> Option<S::Output>
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
    F: Fn(S::Output, S::Output) -> S::Output + Sync,
{
    let n = items.len();
    if n == 0 {
        return None;
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        if S::MAY_FILTER {
            return items
                .into_iter()
                .filter_map(|item| stages.apply(item))
                .reduce(op);
        }
        return items
            .into_iter()
            .map(|item| stages.apply_pure(item))
            .reduce(op);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedReduce(stages, OptionReducer(op));
    par_reduce(items, &rop, plan, pool)
}

/// `pub(crate)` entry point for the scoped fold terminal — the
/// [`fused_reduce_scoped`] counterpart over a different accumulator type
/// (driven by `crate::scope::ScopedPipe::fold`).
pub(crate) fn fused_fold_scoped<S, T, A, F, C>(
    items: Vec<T>,
    stages: S,
    init: A,
    f: F,
    combine: C,
    workload: Workload,
    pool: &ComputePool,
) -> A
where
    S: FusedStage<T> + Sync,
    T: Send,
    S::Output: Send,
    A: Clone + Send + Sync,
    F: Fn(A, S::Output) -> A + Sync,
    C: Fn(A, A) -> A + Sync,
{
    let n = items.len();
    let num_threads = pool.num_workers();
    if n == 0 || prefers_serial(n, num_threads) {
        let mut acc = init;
        for item in items {
            if let Some(o) = stages.apply(item) {
                acc = f(acc, o);
            }
        }
        return acc;
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedReduce(stages, FoldReducer { init, f, combine });
    par_reduce(items, &rop, plan, pool)
}

/// `pub(crate)` entry point for the scoped fallible reduce terminal
/// (`ScopedTryPipe::try_reduce`). Same dispatch logic as
/// `TryPipe::try_reduce` minus the `'static` bounds on the stage chain;
/// `E` keeps its `'static` bound (the hybrid dispatcher's type-erased
/// failure slot downcasts by concrete type — same caveat as
/// [`fused_try_collect_scoped`]).
pub(crate) fn fused_try_reduce_scoped<S, T, E, F>(
    items: Vec<T>,
    stages: S,
    op: F,
    workload: Workload,
    pool: &ComputePool,
) -> Result<Option<S::Output>, E>
where
    S: FusedTryStage<T, Error = E> + Sync,
    T: Send,
    S::Output: Send,
    E: Send + 'static,
    F: Fn(S::Output, S::Output) -> S::Output + Sync,
{
    let n = items.len();
    if n == 0 {
        return Ok(None);
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        let mut acc: Option<S::Output> = None;
        for item in items {
            if let Some(o) = stages.try_apply(item)? {
                acc = Some(match acc {
                    Some(a) => op(a, o),
                    None => o,
                });
            }
        }
        return Ok(acc);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedTryReduce(stages, OptionReducer(op));
    par_reduce_try(items, &rop, plan, pool)
}

/// `pub(crate)` entry point for the scoped fallible fold terminal
/// (`ScopedTryPipe::try_fold`).
pub(crate) fn fused_try_fold_scoped<S, T, A, E, F, C>(
    items: Vec<T>,
    stages: S,
    init: A,
    f: F,
    combine: C,
    workload: Workload,
    pool: &ComputePool,
) -> Result<A, E>
where
    S: FusedTryStage<T, Error = E> + Sync,
    T: Send,
    S::Output: Send,
    A: Clone + Send + Sync,
    E: Send + 'static,
    F: Fn(A, S::Output) -> A + Sync,
    C: Fn(A, A) -> A + Sync,
{
    let n = items.len();
    let num_threads = pool.num_workers();
    if n == 0 || prefers_serial(n, num_threads) {
        let mut acc = init;
        for item in items {
            if let Some(o) = stages.try_apply(item)? {
                acc = f(acc, o);
            }
        }
        return Ok(acc);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedTryReduce(stages, FoldReducer { init, f, combine });
    par_reduce_try(items, &rop, plan, pool)
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
        return match filter_collect_variant() {
            FilterCollectVariant::Merge => {
                join_fused_collect_by_ref(pool, input, &stages, 0, n, plan.depth)
            },
            FilterCollectVariant::Ctp => {
                fused_filter_collect_by_ref_ctp(pool, input, &stages, plan.depth)
            },
            FilterCollectVariant::Wtc => {
                fused_filter_collect_by_ref_wtc(pool, input, &stages, plan.depth)
            },
        };
    }
    let op = FusedOp(stages);
    par_index_collect_by_ref(input, &op, plan, pool)
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

/// Entry point for the borrowed-input reduce terminals, generic over the
/// accumulator [`Reducer`] (driven by `PipeRef::reduce` / `fold` / `sum` /
/// `count` in borrowed.rs — the conveniences are one-line reducer choices).
pub(super) fn fused_reduce_by_ref<'i, S, E, R>(
    input: &'i [E],
    stages: S,
    reducer: R,
    workload: Workload,
    pool: &ComputePool,
) -> R::Acc
where
    S: FusedStage<&'i E> + Sync,
    E: Sync,
    S::Output: Send,
    R: Reducer<S::Output>,
{
    let n = input.len();
    if n == 0 {
        return reducer.identity();
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        let mut acc = reducer.identity();
        if S::MAY_FILTER {
            for item in input {
                if let Some(o) = stages.apply(item) {
                    acc = reducer.fold(acc, o);
                }
            }
        } else {
            for item in input {
                acc = reducer.fold(acc, stages.apply_pure(item));
            }
        }
        return acc;
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedReduce(stages, reducer);
    par_reduce_by_ref(input, &rop, plan, pool)
}

/// Entry point for the borrowed-input fallible reduce terminals
/// (`TryPipeRef::try_reduce` / `try_fold`). `E` (the error type) keeps its
/// `'static` bound — the hybrid dispatcher's type-erased failure slot
/// downcasts by concrete type (same caveat as `fused_try_collect_by_ref`).
pub(super) fn fused_try_reduce_by_ref<'i, S, E, F, R>(
    input: &'i [E],
    stages: S,
    reducer: R,
    workload: Workload,
    pool: &ComputePool,
) -> Result<R::Acc, F>
where
    S: FusedTryStage<&'i E, Error = F> + Sync,
    E: Sync,
    S::Output: Send,
    F: Send + 'static,
    R: Reducer<S::Output>,
{
    let n = input.len();
    if n == 0 {
        return Ok(reducer.identity());
    }
    let num_threads = pool.num_workers();
    if prefers_serial(n, num_threads) {
        let mut acc = reducer.identity();
        for item in input {
            if let Some(o) = stages.try_apply(item)? {
                acc = reducer.fold(acc, o);
            }
        }
        return Ok(acc);
    }
    let plan = SplitPlan::new(n, num_threads, workload);
    let rop = FusedTryReduce(stages, reducer);
    par_reduce_try_by_ref(input, &rop, plan, pool)
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

    /// Count-then-place by-ref filter collect: identical output to the merge
    /// tree across selectivity shapes, split depths (including depths that
    /// hit the `len <= 1` early exit) and the empty-output edge (0%
    /// survival → `Slots::uninit(0)`). Drives the ctp driver directly — the
    /// env knob is a process-global `OnceLock` and is covered by the
    /// same-binary knob A/B instead.
    #[test]
    fn test_filter_ctp_by_ref_matches_merge_tree() {
        let pool = ComputePool::global();
        let data: Vec<u64> = (0..10_007).collect();

        // keep: all / none / ~1/3 — `(x + 1) % k == 0` after the first map.
        for (name, keep) in [("all", 1u64), ("none", 10_000), ("third", 3)] {
            let stages = SyncMap {
                prev: Filter {
                    prev: SyncMap {
                        prev: Identity,
                        f: |x: &u64| x + 1,
                    },
                    f: move |&x: &u64| x % keep == 0,
                },
                f: |x: u64| x * 2,
            };
            let expected: Vec<u64> = data
                .iter()
                .map(|&x| x + 1)
                .filter(|&x| x % keep == 0)
                .map(|x| x * 2)
                .collect();

            for depth in [0, 1, 6, 20] {
                let merged = join_fused_collect_by_ref(pool, &data, &stages, 0, data.len(), depth);
                let ctp = fused_filter_collect_by_ref_ctp(pool, &data, &stages, depth);
                assert_eq!(merged, expected, "merge tree, keep={name} depth={depth}");
                assert_eq!(ctp, expected, "count-then-place, keep={name} depth={depth}");
            }
        }
    }

    /// Write-then-compact by-ref filter collect: identical output to the
    /// merge tree across selectivity shapes, split depths (including depths
    /// that hit the `len <= 1` early exit) and the empty-output edge. Drives
    /// the wtc driver directly — the env knob is a process-global `OnceLock`
    /// and is covered by the same-binary knob A/B instead.
    #[test]
    fn test_filter_wtc_by_ref_matches_merge_tree() {
        let pool = ComputePool::global();
        // Small prime under miri: the 10K shape costs >20 interpreted
        // minutes at depth 20 (10K single-item leaves); 503 keeps the same
        // split shapes (multi-leaf trees AND the `len <= 1` early exit).
        let n: u64 = if cfg!(miri) {
            503
        } else {
            10_007
        };
        let data: Vec<u64> = (0..n).collect();

        for (name, keep) in [("all", 1u64), ("none", 10_000), ("third", 3)] {
            let stages = SyncMap {
                prev: Filter {
                    prev: SyncMap {
                        prev: Identity,
                        f: |x: &u64| x + 1,
                    },
                    f: move |&x: &u64| x % keep == 0,
                },
                f: |x: u64| x * 2,
            };
            let expected: Vec<u64> = data
                .iter()
                .map(|&x| x + 1)
                .filter(|&x| x % keep == 0)
                .map(|x| x * 2)
                .collect();

            for depth in [0, 1, 6, 20] {
                let merged = join_fused_collect_by_ref(pool, &data, &stages, 0, data.len(), depth);
                let wtc = fused_filter_collect_by_ref_wtc(pool, &data, &stages, depth);
                assert_eq!(merged, expected, "merge tree, keep={name} depth={depth}");
                assert_eq!(
                    wtc, expected,
                    "write-then-compact, keep={name} depth={depth}"
                );
                // The compacted Vec is exactly survivor-sized (capacity may
                // keep the n-slot buffer — documented behavior).
                assert_eq!(
                    wtc.len(),
                    expected.len(),
                    "wtc len, keep={name} depth={depth}"
                );
            }
        }
    }

    /// Large-payload caliber: the survivor bytes cross
    /// `WTC_PARALLEL_COMPACT_MIN_BYTES`, so the compaction takes the parallel
    /// copy wave (native-only — miri has no throughput caliber to offer, and
    /// the wave's correctness argument is interleaving-independent anyway).
    #[cfg(not(miri))]
    #[test]
    fn test_filter_wtc_parallel_compact_matches_merge_tree() {
        let pool = ComputePool::global();
        let n: u64 = (WTC_PARALLEL_COMPACT_MIN_BYTES / size_of::<u64>()) as u64 * 3 / 2;
        let data: Vec<u64> = (0..n).collect();
        let stages = SyncMap {
            prev: Filter {
                prev: SyncMap {
                    prev: Identity,
                    f: |x: &u64| x + 1,
                },
                f: |&x: &u64| x % 4 != 0, // keep 75 %
            },
            f: |x: u64| x * 2,
        };
        let expected: Vec<u64> = data
            .iter()
            .map(|&x| x + 1)
            .filter(|&x| x % 4 != 0)
            .map(|x| x * 2)
            .collect();
        // 384 KB of survivors on 8-byte items — above the threshold.
        assert!(expected.len() * size_of::<u64>() >= WTC_PARALLEL_COMPACT_MIN_BYTES);

        for depth in [3, 6] {
            let merged = join_fused_collect_by_ref(pool, &data, &stages, 0, data.len(), depth);
            let wtc = fused_filter_collect_by_ref_wtc(pool, &data, &stages, depth);
            assert_eq!(merged, expected, "merge tree, depth={depth}");
            assert_eq!(wtc, expected, "write-then-compact wave, depth={depth}");
        }
    }

    /// Panic accounting for the wtc driver: after the re-raised panic every
    /// produced output instance is dropped exactly once — the panicking
    /// leaf's guard drops its own written prefix, the driver's meta walk
    /// drops every completed leaf's survivors (ctp's place pass leaks those;
    /// wtc's meta store makes the exact walk possible). Exercises both a
    /// multi-leaf tree (panic mid-tree, siblings complete around it) and the
    /// single-leaf depth (guard-only cleanup).
    #[test]
    fn test_filter_wtc_panic_drop_accounting() {
        use std::sync::Arc;

        struct DropCounter {
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        let n: u64 = if cfg!(miri) {
            500
        } else {
            20_000
        };
        // After the `x + 1` map below; even so the `x % 2 == 0` filter
        // actually lets it through to the panicking stage.
        let panic_marker = ((n / 3) + 2) & !1;
        let data: Vec<u64> = (0..n).collect();

        for depth in [3, 0] {
            let created = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let (c_create, d_drop) = (created.clone(), dropped.clone());
            let stages = SyncMap {
                prev: Filter {
                    prev: SyncMap {
                        prev: Identity,
                        f: |x: &u64| x + 1,
                    },
                    f: |&x: &u64| x % 2 == 0,
                },
                f: move |x: u64| {
                    assert!(x != panic_marker, "boom");
                    c_create.fetch_add(1, Ordering::Relaxed);
                    DropCounter {
                        dropped: d_drop.clone(),
                    }
                },
            };
            let r = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                fused_filter_collect_by_ref_wtc(ComputePool::global(), &data, &stages, depth)
            }));
            assert!(
                r.is_err(),
                "panic must propagate through the wtc tree (depth={depth})"
            );
            let (c, d) = (
                created.load(Ordering::Relaxed),
                dropped.load(Ordering::Relaxed),
            );
            assert_eq!(
                c, d,
                "every produced output must drop exactly once (depth={depth}): created={c}, \
                 dropped={d}"
            );
            // The panicking item's leaf stops AT it and every other leaf
            // completes (`join` runs both sides), so at least the items
            // before the leaf boundary must have been produced.
            assert!(c > 0, "some outputs must precede the panic (depth={depth})");
        }
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
