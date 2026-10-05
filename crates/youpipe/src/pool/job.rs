//! Type-erased jobs for the work-stealing scheduler. Adapted from rayon-core.
//!
//! A `JobRef` is a pair of (data pointer, execute function pointer) — a
//! type-erased `Job` with no vtable indirection. Jobs may live on the stack
//! (`StackJob`) or the heap (`HeapJob`).

use std::{any::Any, cell::UnsafeCell, mem};

use super::{latch::Latch, unwind};

/// Result of executing a job's closure.
pub(crate) enum JobResult<T> {
    None,
    Ok(T),
    Panic(Box<dyn Any + Send>),
}

/// Trait implemented by concrete job types. `execute` is stored as a function
/// pointer in `JobRef` for direct dispatch (no vtable).
///
/// # Safety
///
/// `execute` may be called from a different thread than the one which
/// scheduled the job, so the implementer must ensure appropriate `Send`/`Sync`.
///
/// # Panics
///
/// Implementations must not let a panic escape `execute` — capture it into a
/// result/failure slot or abort. Consumers pop jobs bare (join's wait loop has
/// no panic guard of its own), and a live unwind through them both bypasses
/// callers' unwind cleanups and, for stack-allocated jobs still queued behind
/// it, leaves their `JobRef`s dangling in the deques (UB).
pub(crate) trait Job {
    /// # Safety
    ///
    /// `this` must point to a valid, unexecuted instance of `Self` that stays
    /// alive for the whole call, and must be executed exactly once.
    unsafe fn execute(this: *const ());
}

/// Type-erased job reference. Each `JobRef` **must** be executed exactly once,
/// or data may leak.
pub(crate) struct JobRef {
    pointer: *const (),
    execute_fn: unsafe fn(*const ()),
}

unsafe impl Send for JobRef {}
unsafe impl Sync for JobRef {}

impl JobRef {
    /// # Safety
    ///
    /// Caller asserts that `data` will remain valid until the job is executed.
    pub(crate) unsafe fn new<T>(data: *const T) -> JobRef
    where
        T: Job,
    {
        JobRef {
            pointer: data.cast::<()>(),
            execute_fn: <T as Job>::execute,
        }
    }

    /// Opaque identity for comparison (used by `join` to detect self-popped
    /// job).
    #[inline]
    pub(crate) fn id(&self) -> (usize, usize) {
        (self.pointer as usize, self.execute_fn as usize)
    }

    /// # Safety
    ///
    /// Same contract as [`Job::execute`]: `self` must be a valid, unexecuted
    /// job whose data is still alive; executed exactly once.
    #[inline]
    pub(crate) unsafe fn execute(self) {
        unsafe { (self.execute_fn)(self.pointer) };
    }
}

/// A job that lives in a stack slot. When it executes it does not free any heap
/// data — cleanup happens when the stack frame is popped.
///
/// `F` receives a `bool` indicating whether the job was stolen (executed on a
/// different thread).
pub(crate) struct StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send,
    R: Send,
{
    pub(crate) latch: L,
    func: UnsafeCell<Option<F>>,
    result: UnsafeCell<JobResult<R>>,
}

impl<L, F, R> StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send,
    R: Send,
{
    pub(crate) fn new(func: F, latch: L) -> StackJob<L, F, R> {
        StackJob {
            latch,
            func: UnsafeCell::new(Some(func)),
            result: UnsafeCell::new(JobResult::None),
        }
    }

    /// # Safety
    ///
    /// `self` (the whole `StackJob`, latch included) must remain alive and
    /// unmutated until the returned `JobRef` is executed exactly once.
    pub(crate) unsafe fn as_job_ref(&self) -> JobRef {
        unsafe { JobRef::new(self) }
    }

    // `run_inline` was removed with join's self-pop switch to
    // `JobRef::execute`: it bypassed the job's `halt_unwinding`, so a self-run
    // B's panic escaped as a live unwind instead of a value (and re-adding a
    // catch at the call site cost +33 % on sync_cpu_heavy/100K — see
    // `join_on_captured`).

    /// # Safety
    ///
    /// The job must never have been scheduled: no concurrent `execute` may run
    /// or be pending, otherwise the closure is consumed twice.
    pub(crate) unsafe fn run_inline(self, stolen: bool) -> R {
        self.func.into_inner().unwrap()(stolen)
    }

    /// [`run_inline`](Self::run_inline) with the panic captured as a value —
    /// used by `join_on_captured`'s self-pop branch only when the OTHER side
    /// already failed (a cold, already-doomed batch), so both failures reach
    /// the caller's match instead of the unwind eating A's result.
    ///
    /// `cold` + `inline(never)` are load-bearing: the `catch_unwind`
    /// landingpad must stay out of the callers' codegen (see
    /// `join_on_captured`'s self-pop comment for the measured cost).
    ///
    /// # Safety
    ///
    /// Same contract as [`run_inline`](Self::run_inline).
    #[cold]
    #[inline(never)]
    pub(crate) unsafe fn run_captured(&self, stolen: bool) -> Result<R, Box<dyn Any + Send>> {
        // SAFETY: contract above — sole owner, never scheduled concurrently.
        let func = unsafe { (*self.func.get()).take().unwrap() };
        unwind::halt_unwinding(move || func(stolen))
    }

    /// # Safety
    ///
    /// The job must have been executed exactly once (latch set), so the result
    /// slot is populated and no other thread can still access it.
    pub(crate) unsafe fn into_result(self) -> R {
        self.result.into_inner().into_return_value()
    }

    /// [`into_result`](Self::into_result) without the resume: a stored panic
    /// comes back as `Err(payload)` for [`join_captured`](crate::pool::join::join_captured)
    /// callers that handle panics as values.
    ///
    /// # Safety
    ///
    /// Same contract as [`into_result`](Self::into_result).
    pub(crate) unsafe fn into_result_captured(self) -> Result<R, Box<dyn Any + Send>> {
        match self.result.into_inner() {
            JobResult::None => unreachable!(),
            JobResult::Ok(x) => Ok(x),
            JobResult::Panic(p) => Err(p),
        }
    }
}

impl<L, F, R> Job for StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send,
    R: Send,
{
    unsafe fn execute(this: *const ()) {
        unsafe {
            let this = &*this.cast::<Self>();
            let abort = unwind::AbortIfPanic;
            let func = (*this.func.get()).take().unwrap();
            (*this.result.get()) = JobResult::call(func);
            Latch::set(&raw const this.latch);
            mem::forget(abort);
        }
    }
}

/// A job stored on the heap. Used by `scope` and `submit`.
pub(crate) struct HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    job: BODY,
}

impl<BODY> HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    #[allow(clippy::unnecessary_box_returns)]
    pub(crate) fn new(job: BODY) -> Box<Self> {
        Box::new(HeapJob { job })
    }

    /// Erases lifetimes. Caller must ensure the `JobRef` doesn't outlive the
    /// job's data.
    ///
    /// # Safety
    ///
    /// The returned `JobRef` must be executed before `self` is dropped.
    pub(crate) unsafe fn into_job_ref(self: Box<Self>) -> JobRef {
        unsafe { JobRef::new(Box::into_raw(self)) }
    }

    /// Creates a static `JobRef`.
    pub(crate) fn into_static_job_ref(self: Box<Self>) -> JobRef
    where
        BODY: 'static,
    {
        unsafe { self.into_job_ref() }
    }
}

impl<BODY> Job for HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    unsafe fn execute(this: *const ()) {
        unsafe {
            let this = Box::from_raw(this as *mut Self);
            // A HeapJob has no result slot to capture a panic into, so it
            // aborts (the `Job::execute` no-unwind contract). Unlike rayon,
            // whose `spawn` always injects to the global queue, our on-pool
            // submit fast path parks HeapJobs in the local deque, whose
            // consumer — join's wait loop — executes them bare: a live unwind
            // there destroyed the join frame while its stack-allocated `job_b`
            // ref still sat in the deque → SIGSEGV on the next pop/steal.
            // Abort also unifies submit-panic semantics with the main loop
            // and `wait_until_cold` (their own `AbortIfPanic` guards already
            // aborted it) and with rayon `spawn`. The alternative — capturing
            // at the join call site and resuming after B — was rejected: it
            // re-runs the measured +33 % hot-path catch regression (see
            // `StackJob::run_inline`'s note) and a resumed foreign unwind
            // escapes `join_captured`'s value contract, bypassing the fused
            // tree's sibling cleanup (the leak class fixed before it).
            let abort = unwind::AbortIfPanic;
            (this.job)();
            mem::forget(abort);
        }
    }
}

impl<T> JobResult<T> {
    fn call(func: impl FnOnce(bool) -> T) -> Self {
        match unwind::halt_unwinding(|| func(true)) {
            Ok(x) => JobResult::Ok(x),
            Err(x) => JobResult::Panic(x),
        }
    }

    pub(crate) fn into_return_value(self) -> T {
        match self {
            JobResult::None => unreachable!(),
            JobResult::Ok(x) => x,
            JobResult::Panic(x) => unwind::resume_unwinding(x),
        }
    }
}
