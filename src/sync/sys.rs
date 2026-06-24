//! Miri-transparent Mutex + Condvar abstraction.
//!
//! In production this is a zero-cost re-export of `parking_lot::{Mutex,
//! Condvar}`, which are fairer than their std counterparts and never poison —
//! so there is no panic path on the (cold) injector lock.
//!
//! Under Miri we instead back the same API with `std::sync::{Mutex, Condvar}`,
//! because `parking_lot_core` resolves `WaitOnAddress` through
//! `GetModuleHandleA`, a Windows foreign function Miri cannot emulate, whereas
//! the std primitives are natively supported by the interpreter.
//!
//! Both paths expose identical, infallible APIs so callers never branch on
//! `cfg`.

#[cfg(not(miri))]
#[allow(unused_imports)]
pub(crate) use parking_lot::{Condvar, Mutex, MutexGuard};

#[cfg(miri)]
pub(crate) use self::shim::{Condvar, Mutex, MutexGuard};

#[cfg(miri)]
mod shim {
    use std::{sync as s, time::Duration};

    pub(crate) struct Mutex<T: ?Sized>(s::Mutex<T>);
    pub(crate) struct MutexGuard<'a, T: ?Sized>(s::MutexGuard<'a, T>);
    pub(crate) struct Condvar(s::Condvar);

    impl<T> Mutex<T> {
        #[inline]
        pub(crate) const fn new(value: T) -> Self {
            Self(s::Mutex::new(value))
        }
    }

    impl<T: ?Sized> Mutex<T> {
        #[inline]
        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap_or_else(|e| e.into_inner()))
        }
    }

    impl<T: ?Sized> std::ops::Deref for MutexGuard<'_, T> {
        type Target = T;
        #[inline]
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<T: ?Sized> std::ops::DerefMut for MutexGuard<'_, T> {
        #[inline]
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl Condvar {
        #[inline]
        pub(crate) fn new() -> Self {
            Self(s::Condvar::new())
        }

        #[inline]
        pub(crate) fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
            let _ = self.0.wait(&mut guard.0).unwrap_or_else(|e| e.into_inner());
        }

        #[inline]
        pub(crate) fn wait_for<'a, T>(
            &self,
            guard: &mut MutexGuard<'a, T>,
            timeout: Duration,
        ) -> bool {
            match self.0.wait_timeout(&mut guard.0, timeout) {
                Ok((_, result)) => !result.timed_out(),
                Err(e) => {
                    let (_, result) = e.into_inner();
                    !result.timed_out()
                }
            }
        }

        #[inline]
        pub(crate) fn notify_one(&self) {
            self.0.notify_one();
        }

        #[inline]
        pub(crate) fn notify_all(&self) {
            self.0.notify_all();
        }
    }
}
