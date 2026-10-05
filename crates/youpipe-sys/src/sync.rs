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
//! Under `--cfg loom` (concurrency-model testing, switched by the rustflag —
//! see youpipe's `Cargo.toml` for the rationale) everything is backed by
//! `loom::sync` / `loom::sync::atomic` so the model checker observes the
//! atomics and lock/condvar interleavings instead of the real OS primitives.
//! youpipe's synchronization cores (`pool/sleep.rs`, `pool/latch.rs`,
//! `pool/sleep_mask.rs`, `handoff/notify.rs`) source their primitives from
//! here for exactly this reason.
//!
//! All paths expose identical, infallible APIs so callers never branch on
//! `cfg`.

#[cfg(all(not(miri), not(loom)))]
#[allow(unused_imports)]
pub use parking_lot::{Condvar, Mutex, MutexGuard};

#[cfg(loom)]
pub use self::loom_shim::{Condvar, Mutex, MutexGuard};
#[cfg(miri)]
pub use self::shim::{Condvar, Mutex, MutexGuard};

/// loom-backed shim matching the infallible `parking_lot` API shape (loom's
/// `Mutex::lock` returns a `Result` like std's; the model never poisons, so
/// the wrapper just unwraps it away).
#[cfg(loom)]
mod loom_shim {
    use std::ops::{Deref, DerefMut};

    use loom::sync::{Condvar as LCondvar, Mutex as LMutex, MutexGuard as LMutexGuard};

    pub struct Mutex<T: ?Sized>(LMutex<T>);
    pub struct MutexGuard<'a, T: ?Sized>(Option<LMutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        #[inline]
        pub fn new(value: T) -> Self {
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
        pub fn lock(&self) -> MutexGuard<'_, T> {
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

    pub fn condvar_wait<'a, T>(cv: &LCondvar, guard: &mut MutexGuard<'a, T>) {
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

    pub struct Condvar(LCondvar);

    impl std::fmt::Debug for Condvar {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.0, f)
        }
    }

    impl Condvar {
        #[inline]
        pub fn new() -> Self {
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
        pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
            super::loom_shim::condvar_wait(&self.0, guard);
        }

        #[inline]
        pub fn notify_one(&self) {
            self.0.notify_one();
        }

        #[inline]
        pub fn notify_all(&self) {
            self.0.notify_all();
        }
    }
}

#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

/// Atomics routed through loom under `--cfg loom` so the model checker sees
/// them; plain std atomics otherwise.
#[cfg(loom)]
pub use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

/// Cooperative yield for idle backoff loops. Under loom this MUST be
/// `loom::thread::yield_now` so the model checker can switch threads — a
/// `std::thread::yield_now` would be a real syscall inside the model and
/// break exploration.
#[inline]
pub fn thread_yield() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    std::thread::yield_now();
}

#[cfg(miri)]
mod shim {
    use std::sync as s;

    pub struct Mutex<T: ?Sized>(s::Mutex<T>);
    pub struct MutexGuard<'a, T: ?Sized>(Option<s::MutexGuard<'a, T>>);
    pub struct Condvar(s::Condvar);

    impl<T> Mutex<T> {
        #[inline]
        pub const fn new(value: T) -> Self {
            Self(s::Mutex::new(value))
        }
    }

    impl<T: ?Sized> Mutex<T> {
        #[inline]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(self.0.lock().unwrap_or_else(|e| e.into_inner())))
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
            self.0.as_ref().expect("guard moved into condvar wait")
        }
    }

    impl<T: ?Sized> std::ops::DerefMut for MutexGuard<'_, T> {
        #[inline]
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().expect("guard moved into condvar wait")
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
        pub fn new() -> Self {
            Self(s::Condvar::new())
        }

        /// Park the current thread until notified.
        ///
        /// Matches the `parking_lot::Condvar::wait(&self, &mut MutexGuard)`
        /// signature even though std's (like loom's) consumes the guard and
        /// returns it. The guard temporarily moves out of the wrapper via
        /// `Option::take`, same shape as the loom shim. The previous
        /// `ptr::read`/`ptr::write` hand-off was unsound on the unwind
        /// path: had `wait` panicked after taking ownership, std would have
        /// dropped the guard during its unwind, the `ptr::write` would
        /// never run, and the wrapper would hold a bitwise duplicate — a
        /// double unlock/drop that Miri flags as UB. `Option` keeps the
        /// wrapper's slot honestly empty instead.
        #[inline]
        pub fn wait<'a, T>(&self, guard: &mut MutexGuard<'a, T>) {
            let taken = guard.0.take().expect("guard already in condvar wait");
            let returned = self.0.wait(taken).unwrap_or_else(|e| e.into_inner());
            guard.0 = Some(returned);
        }

        #[inline]
        pub fn notify_one(&self) {
            self.0.notify_one();
        }

        #[inline]
        pub fn notify_all(&self) {
            self.0.notify_all();
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Exercises the `Option::take` hand-off end to end: lock, wait
        /// under contention, deref the re-installed guard. The shim's only
        /// consumer (`youpipe`'s pool) is not compiled in this crate's
        /// tests, so without this the path is miri-dark.
        #[test]
        fn condvar_wait_moves_guard_out_and_back() {
            let mutex = Mutex::new(0u8);
            let condvar = Condvar::new();

            std::thread::scope(|s| {
                let signer = s.spawn(|| {
                    *mutex.lock() = 1;
                    condvar.notify_one();
                });
                let mut guard = mutex.lock();
                while *guard == 0 {
                    condvar.wait(&mut guard);
                }
                assert_eq!(*guard, 1);
                signer.join().unwrap();
            });
        }
    }
}
