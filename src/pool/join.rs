//! Fork-join parallelism. Adapted from rayon-core's `join`.
//!
//! When `join` is called from a pool worker, the first closure runs inline on
//! the current thread while the second is pushed to the local deque. If the
//! second closure is stolen by another worker, the current thread will steal
//! other work while waiting for it to complete. This is the core work-stealing
//! strategy.

use std::{any::Any, sync::Arc};

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

/// Join implementation assuming we're already on `worker_thread`.
///
/// # Safety
///
/// `worker_thread` must be the current thread's `WorkerThread`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) unsafe fn join_on<A, B, RA, RB>(
    worker_thread: &WorkerThread,
    injected: bool,
    oper_a: A,
    oper_b: B,
) -> (RA, RB)
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
    let status_a = unwind::halt_unwinding(oper_a);
    let result_a = match status_a {
        Ok(v) => v,
        Err(err) => unsafe { join_recover_from_panic(worker_thread, &job_b.latch, err) },
    };

    // Now try to pop and run B, or wait for it if stolen.
    while !job_b.latch.probe() {
        if let Some(job) = worker_thread.try_pop_local() {
            if job_b_id == job.id() {
                // Found B! Run it inline.
                let result_b = unsafe { job_b.run_inline(injected) };
                return (result_a, result_b);
            }
            unsafe { WorkerThread::execute(job) };
        } else {
            // Local deque empty (B was stolen). Steal work while waiting.
            unsafe { worker_thread.wait_until(job_b.latch.as_core_latch()) };
            debug_assert!(job_b.latch.probe());
            break;
        }
    }

    (result_a, unsafe { job_b.into_result() })
}

/// If A panics, we still must wait for B to complete (it may hold references
/// into our stack frame).
#[cold]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn join_recover_from_panic(
    worker_thread: &WorkerThread,
    job_b_latch: &SpinLatch<'_>,
    err: Box<dyn Any + Send>,
) -> ! {
    unsafe { worker_thread.wait_until(job_b_latch.as_core_latch()) };
    unwind::resume_unwinding(err)
}
