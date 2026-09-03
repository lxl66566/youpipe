//! Package up unwind recovery. Adapted from rayon-core.

use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    thread,
};

/// Executes `f` and captures any panic, translating it into an `Err`.
pub(crate) fn halt_unwinding<F, R>(func: F) -> thread::Result<R>
where
    F: FnOnce() -> R,
{
    panic::catch_unwind(AssertUnwindSafe(func))
}

pub(crate) fn resume_unwinding(payload: Box<dyn Any + Send>) -> ! {
    panic::resume_unwind(payload)
}

/// Guard that aborts the process if dropped during a panic.
/// Call `mem::forget` on success paths.
pub(crate) struct AbortIfPanic;

impl Drop for AbortIfPanic {
    fn drop(&mut self) {
        eprintln!("youpipe: detected unexpected panic in pool worker; aborting");
        ::std::process::abort();
    }
}
