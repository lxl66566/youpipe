//! Loom primitive shims.
//!
//! NOTE: the gating below must stay a plain `cfg(st3_loom)` — do NOT add a
//! `test` conjunct. The integration loom tests link this lib as an rlib
//! compiled *without* cfg(test), so `all(test, st3_loom)` would select the
//! real atomics for exactly those tests: loom::model then tracks none of the
//! queue's operations, explores a single schedule, and every model passes
//! vacuously in ~0.00s (upstream PR #10 "loom-as-dev-dep" introduced this
//! form; measured on the 2026-10 fix). The plain cfg in turn requires loom
//! to be a regular target dependency — see Cargo.toml.
#[cfg(st3_loom)]
#[allow(unused_imports)]
pub(crate) mod sync {
    pub(crate) mod atomic {
        #[cfg(not(target_has_atomic = "64"))]
        pub(crate) use loom::sync::atomic::AtomicU16;
        pub(crate) use loom::sync::atomic::AtomicU32;
        #[cfg(target_has_atomic = "64")]
        pub(crate) use loom::sync::atomic::AtomicU64;
    }
}
#[cfg(not(st3_loom))]
#[allow(unused_imports)]
pub(crate) mod sync {
    pub(crate) mod atomic {
        #[cfg(not(target_has_atomic = "64"))]
        pub(crate) use core::sync::atomic::AtomicU16;
        pub(crate) use core::sync::atomic::AtomicU32;
        #[cfg(target_has_atomic = "64")]
        pub(crate) use core::sync::atomic::AtomicU64;
    }
}

#[cfg(st3_loom)]
pub(crate) mod cell {
    pub(crate) use loom::cell::UnsafeCell;
}
#[cfg(not(st3_loom))]
pub(crate) mod cell {
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(core::cell::UnsafeCell<T>);

    #[allow(dead_code)]
    impl<T> UnsafeCell<T> {
        pub(crate) fn new(data: T) -> UnsafeCell<T> {
            UnsafeCell(core::cell::UnsafeCell::new(data))
        }
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[allow(unused_macros)]
macro_rules! debug_or_loom_assert {
    ($($arg:tt)*) => (if cfg!(any(debug_assertions, st3_loom)) { assert!($($arg)*); })
}
#[allow(unused_macros)]
macro_rules! debug_or_loom_assert_eq {
    ($($arg:tt)*) => (if cfg!(any(debug_assertions, st3_loom)) { assert_eq!($($arg)*); })
}
#[allow(unused_imports)]
pub(crate) use debug_or_loom_assert;
#[allow(unused_imports)]
pub(crate) use debug_or_loom_assert_eq;
