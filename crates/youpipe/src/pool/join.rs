//! Fork-join parallelism. Adapted from rayon-core's `join`.
//!
//! When `join` is called from a pool worker, the first closure runs inline on
//! the current thread while the second is pushed to the local deque. If the
//! second closure is stolen by another worker, the current thread will steal
//! other work while waiting for it to complete. This is the core work-stealing
//! strategy.

use std::{any::Any, sync::Arc};

/// A join result with a captured panic as the `Err` side (see
/// [`join_captured`]).
pub(crate) type Captured<T> = Result<T, Box<dyn Any + Send>>;

use super::{
    job::StackJob,
    latch::{AsCoreLatch, SpinLatch},
    registry::{Registry, WorkerThread},
    unwind,
};

/// Takes two closures and *potentially* runs them in parallel. Returns both
/// results.
///
/// When called from a pool worker thread, `a` runs on the current thread while
/// `b` is advertised for stealing. When called from an external thread, the
/// pool handles injection.
///
/// # Panics
///
/// Both closures always execute. If either panics, that panic is propagated. If
/// both panic, the first closure's panic wins.
pub(crate) fn join<A, B, RA, RB>(registry: &Arc<Registry>, oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    registry.in_worker(|worker_thread, injected| unsafe {
        join_on(worker_thread, injected, oper_a, oper_b)
    })
}

/// Value-returning [`join`]: a panicking closure comes back as
/// `Err(payload)` instead of the panic resuming through this frame.
///
/// Callers that must clean up per-branch state on failure (the fused tree
/// recursion's sibling cleanup) need this: a resumed unwind skips every
/// statement between the `join` call and the caller's own unwinder, so the
/// completed sibling's cleanup would be silently bypassed.
pub(crate) fn join_captured<A, B, RA, RB>(
    registry: &Arc<Registry>,
    oper_a: A,
    oper_b: B,
) -> (Captured<RA>, Captured<RB>)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    registry.in_worker(|worker_thread, _injected| unsafe {
        join_on_captured(worker_thread, oper_a, oper_b)
    })
}

/// Join implementation assuming we're already on `worker_thread`.
///
/// # Safety
///
/// `worker_thread` must be the current thread's `WorkerThread`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) unsafe fn join_on<A, B, RA, RB>(
    worker_thread: &WorkerThread,
    _injected: bool,
    oper_a: A,
    oper_b: B,
) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    let (a, b) = unsafe { join_on_captured(worker_thread, oper_a, oper_b) };
    // `resume_unwinding` never returns, so each closure body runs only on the
    // Err path; the Ok path is a plain unwrap.
    (
        a.unwrap_or_else(|p| unwind::resume_unwinding(p)),
        b.unwrap_or_else(|p| unwind::resume_unwinding(p)),
    )
}

/// [`join_on`]'s value-returning core (see [`join_captured`]).
///
/// # Safety
///
/// Same contract as [`join_on`]: `worker_thread` must be the current thread's
/// `WorkerThread`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) unsafe fn join_on_captured<A, B, RA, RB>(
    worker_thread: &WorkerThread,
    oper_a: A,
    oper_b: B,
) -> (Captured<RA>, Captured<RB>)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    // Create job B as a StackJob with a SpinLatch. It lives on this stack frame
    // until we extract its result.
    let job_b = StackJob::new(
        move |_stolen| oper_b(),
        SpinLatch::new(worker_thread.registry(), worker_thread.index()),
    );
    let job_b_ref = unsafe { job_b.as_job_ref() };
    let job_b_id = job_b_ref.id();

    // Push B to local deque; it becomes available for stealing.
    unsafe { worker_thread.push(job_b_ref) };

    // Execute A inline. Hopefully B gets stolen in the meantime.
    let result_a = unwind::halt_unwinding(oper_a);

    // Now try to pop and run B, or wait for it if stolen. Even when A panicked
    // we must run this to completion: B (its StackJob on this frame) may hold
    // references into our stack, so this frame cannot unwind past it until B
    // has finished — the panic rides back as a value instead.
    let result_b = loop {
        if job_b.latch.probe() {
            break unsafe { job_b.into_result_captured() };
        }
        if let Some(job) = worker_thread.try_pop_local() {
            if job_b_id == job.id() {
                // Found B — run it inline. Two shapes by A's outcome:
                //
                // * A ok (the hot case): run B directly. A panicking B is
                //   then the ONE panic that escapes `join_on_captured` as a
                //   live unwind — safe because the caller (the fused tree
                //   recursion) parks a sibling-cleanup guard at the join
                //   call that knows A completed. Wrapping this call in
                //   `catch_unwind` instead (even isolated in a cold
                //   helper) regressed sync_cpu_heavy/100K by +33 % in
                //   same-session A/B vs a rayon control — this branch is
                //   that layout-sensitive.
                // * A failed (cold, batch already doomed): capture B's
                //   panic as a value so the caller's match sees both
                //   failures instead of losing A's result to the unwind.
                break if result_a.is_ok() {
                    Ok(unsafe { job_b.run_inline(false) })
                } else {
                    unsafe { job_b.run_captured(false) }
                };
            }
            unsafe { WorkerThread::execute(job) };
        } else {
            // Local deque empty (B was stolen). Steal work while waiting.
            unsafe { worker_thread.wait_until(job_b.latch.as_core_latch()) };
            debug_assert!(job_b.latch.probe());
            break unsafe { job_b.into_result_captured() };
        }
    };

    (result_a, result_b)
}
