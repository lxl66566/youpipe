//! Miri- and loom-transparent Mutex + Condvar + atomics abstraction.
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
//! Under `--cfg loom` (concurrency-model testing, see `Cargo.toml`) everything
//! is backed by `loom::sync` / `loom::sync::atomic` so the model checker
//! observes the atomics and lock/condvar interleavings instead of the real OS
//! primitives. The modules under test (`pool/sleep.rs`, `pool/latch.rs`,
//! `pool/sleep_mask.rs`, `handoff/notify.rs`) source their atomics from here
//! for exactly this reason.
//!
//! All paths expose identical, infallible APIs so callers never branch on
//! `cfg`.

#[cfg(all(not(miri), not(loom)))]
#[allow(unused_imports)]
pub(crate) use parking_lot::{Condvar, Mutex, MutexGuard};

#[cfg(loom)]
pub(crate) use self::loom_shim::{Condvar, Mutex};
#[cfg(miri)]
pub(crate) use self::shim::{Condvar, Mutex, MutexGuard};

/// loom-backed shim matching the infallible `parking_lot` API shape (loom's
/// `Mutex::lock` returns a `Result` like std's; the model never poisons, so
/// the wrapper just unwraps it away).
#[cfg(loom)]
mod loom_shim {
    use std::ops::{Deref, DerefMut};

    use loom::sync::{Condvar as LCondvar, Mutex as LMutex, MutexGuard as LMutexGuard};

    pub(crate) struct Mutex<T: ?Sized>(LMutex<T>);
    pub(crate) struct MutexGuard<'a, T: ?Sized>(Option<LMutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        #[inline]
        pub(crate) fn new(value: T) -> Self {
            Self(LMutex::new(value))
        }
    }

    impl<T: Default> Default for Mutex<T> {
        #[inline]
        fn default() -> Self {
            Self(LMutex::default())
        }
    }

    impl<T: ?Sized> Mutex<T> {
        #[inline]
        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ))
        }
    }

    impl<T> std::fmt::Debug for Mutex<T>
    where
        T: std::fmt::Debug,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }

    impl<'a, T: ?Sized> Deref for MutexGuard<'a, T> {
        type Target = T;

        #[inline]
        fn deref(&self) -> &T {
            self.0.as_ref().expect("guard moved into condvar wait")
        }
    }

    impl<'a, T: ?Sized> DerefMut for MutexGuard<'a, T> {
        #[inline]
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("guard moved into condvar wait")
        }
    }

    pub(crate) fn condvar_wait<'a, T>(cv: &LCondvar, guard: &mut MutexGuard<'a, T>) {
        let taken = guard.0.take().expect("guard already in condvar wait");
        let returned = cv
            .wait(taken)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.0 = Some(returned);
    }

    impl Default for Condvar {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    pub(crate) struct Condvar(LCondvar);

    impl std::fmt::Debug for Condvar {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }

    impl Condvar {
        #[inline]
        pub(crate) fn new() -> Self {
            Self(LCondvar::new())
        }

        /// Park the current thread until notified.
        ///
        /// Matches the `parking_lot::Condvar::wait(&self, &mut MutexGuard)`
        /// signature even though loom's (like std's) consumes the guard and
        /// returns it. The guard temporarily moves out of the wrapper via
        /// `Option::take` so loom's model tracks the whole hand-off (a raw
        /// `ptr::read`/`ptr::write` dance would bypass its bookkeeping).
        #[inline]
        pub(crate) fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
            super::loom_shim::condvar_wait(&self.0, guard);
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

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

/// Atomics routed through loom under `--cfg loom` so the model checker sees
/// them; plain std atomics otherwise.
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

/// Cooperative yield for idle backoff loops. Under loom this MUST be
/// `loom::thread::yield_now` so the model checker can switch threads — a
/// `std::thread::yield_now` would be a real syscall inside the model and
/// break exploration.
#[inline]
pub(crate) fn thread_yield() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    std::thread::yield_now();
}

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
                },
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
