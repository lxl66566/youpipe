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

    impl<T: Default + Send> Default for Mutex<T> {
        #[inline]
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T> std::fmt::Debug for Mutex<T>
    where
        s::Mutex<T>: std::fmt::Debug,
    {
        #[inline]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
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

    impl Default for Condvar {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    impl std::fmt::Debug for Condvar {
        #[inline]
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }

    impl Condvar {
        #[inline]
        pub(crate) fn new() -> Self {
            Self(s::Condvar::new())
        }

        /// Park the current thread until notified.
        ///
        /// Matches the `parking_lot::Condvar::wait(&self, &mut MutexGuard)`
        /// signature even though `std::sync::Condvar::wait` consumes the
        /// guard and returns it. We move the inner std guard out via
        /// `ptr::read`, hand it to std by value, then write the returned
        /// guard back through the same reference.
        ///
        /// # Safety of the move
        ///
        /// Between the `ptr::read` and `ptr::write`, `guard.0` is logically
        /// uninitialized but we never observe it. `std::Condvar::wait` only
        /// returns `Err` on poison (which we unwrap back into a usable
        /// guard), so a panic here would leak the original guard rather than
        /// double-free — acceptable for the test-only Miri path.
        #[inline]
        pub(crate) fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
            // SAFETY: see method-level comment.
            let taken = unsafe { std::ptr::read(&guard.0) };
            let returned = self.0.wait(taken).unwrap_or_else(|e| e.into_inner());
            // SAFETY: see method-level comment.
            unsafe { std::ptr::write(&mut guard.0, returned) };
        }

        /// Park the current thread until notified or `timeout` elapses.
        /// Returns `true` if notified before the timeout, `false` otherwise.
        /// See [`Self::wait`] for the move-dance rationale.
        #[inline]
        pub(crate) fn wait_for<'a, T>(
            &self,
            guard: &mut MutexGuard<'a, T>,
            timeout: Duration,
        ) -> bool {
            // SAFETY: see `wait` method-level comment.
            let taken = unsafe { std::ptr::read(&guard.0) };
            let (returned, result) = match self.0.wait_timeout(taken, timeout) {
                Ok((g, r)) => (g, r),
                Err(e) => {
                    let (g, r) = e.into_inner();
                    (g, r)
                }
            };
            // SAFETY: see `wait` method-level comment.
            unsafe { std::ptr::write(&mut guard.0, returned) };
            !result.timed_out()
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
