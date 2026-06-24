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
pub(crate) trait Job {
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

    pub(crate) unsafe fn as_job_ref(&self) -> JobRef {
        unsafe { JobRef::new(self) }
    }

    pub(crate) unsafe fn run_inline(self, stolen: bool) -> R {
        self.func.into_inner().unwrap()(stolen)
    }

    pub(crate) unsafe fn into_result(self) -> R {
        self.result.into_inner().into_return_value()
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
            (this.job)();
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
