//! Latches: signaling primitives adapted from rayon-core.
//!
//! A latch starts as false. Eventually `set()` makes it true. Once `probe()`
//! returns true, all memory effects from before `set()` are visible.
//!
//! Atomics/Mutex/Condvar come from `youpipe-sys` so `--cfg loom`
//! can swap them for simulated ones (see `sys.rs`).

use std::{marker::PhantomData, ops::Deref, sync::Arc};

use youpipe_sys::{AtomicUsize, Condvar, Mutex, Ordering};

use super::registry::Registry;

/// Trait for latches that can be set. Operates on `*const Self` to allow the
/// latch to become dangling during `set` (the waiter may wake and deallocate).
pub(crate) trait Latch {
    /// # Safety
    ///
    /// `this` must be valid on entry. It may be invalidated during the call by
    /// the owning thread waking up, so no further field accesses are allowed
    /// after the internal `set` succeeds.
    unsafe fn set(this: *const Self);
}

pub(crate) trait AsCoreLatch {
    fn as_core_latch(&self) -> &CoreLatch;
}

// ── State encoding ──

/// Latch is not set, owning thread is awake.
const UNSET: usize = 0;
/// Latch is not set, owning thread is going to sleep.
const SLEEPY: usize = 1;
/// Latch is not set, owning thread is asleep and must be awoken.
const SLEEPING: usize = 2;
/// Latch is set.
const SET: usize = 3;

/// Spin latch: the simplest, most efficient kind. No `wait()` operation —
/// callers busy-loop or steal work while probing. The 4-state encoding lets
/// the sleep module coordinate wake-ups without a separate flag.
#[derive(Debug)]
pub(crate) struct CoreLatch {
    state: AtomicUsize,
}

impl CoreLatch {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(UNSET),
        }
    }

    /// Invoked by owning thread as it prepares to sleep. Returns `true` if it
    /// may proceed to fall asleep, `false` if the latch was set in the
    /// meantime.
    #[inline]
    pub(crate) fn get_sleepy(&self) -> bool {
        self.state
            .compare_exchange(UNSET, SLEEPY, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Invoked by owning thread as it falls asleep. Returns `true` if it should
    /// block, `false` if the latch was set in the meantime.
    #[inline]
    pub(crate) fn fall_asleep(&self) -> bool {
        self.state
            .compare_exchange(SLEEPY, SLEEPING, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Invoked by owning thread when it wakes up or decides not to sleep.
    #[inline]
    pub(crate) fn wake_up(&self) {
        if !self.probe() {
            let _ =
                self.state
                    .compare_exchange(SLEEPING, UNSET, Ordering::SeqCst, Ordering::Relaxed);
        }
    }

    /// Set the latch. Returns `true` if the owning thread was sleeping and must
    /// be awoken.
    ///
    /// # Safety
    ///
    /// After this returns `true`, `this` may be invalidated by the woken
    /// thread.
    #[inline]
    pub(crate) unsafe fn set(this: *const Self) -> bool {
        let old_state = unsafe { (*this).state.swap(SET, Ordering::SeqCst) };
        old_state == SLEEPING
    }

    #[inline]
    pub(crate) fn probe(&self) -> bool {
        self.state.load(Ordering::Acquire) == SET
    }
}

impl AsCoreLatch for CoreLatch {
    #[inline]
    fn as_core_latch(&self) -> &CoreLatch {
        self
    }
}

/// Spin latch bound to a specific worker thread. Used by `join` to signal
/// completion of a stolen job back to the originating worker.
pub(crate) struct SpinLatch<'r> {
    core: CoreLatch,
    registry: &'r Arc<Registry>,
    target_worker_index: usize,
}

impl<'r> SpinLatch<'r> {
    #[inline]
    pub(crate) fn new(registry: &'r Arc<Registry>, target_worker_index: usize) -> Self {
        SpinLatch {
            core: CoreLatch::new(),
            registry,
            target_worker_index,
        }
    }

    #[inline]
    pub(crate) fn probe(&self) -> bool {
        self.core.probe()
    }
}

impl AsCoreLatch for SpinLatch<'_> {
    #[inline]
    fn as_core_latch(&self) -> &CoreLatch {
        &self.core
    }
}

impl Latch for SpinLatch<'_> {
    #[inline]
    unsafe fn set(this: *const Self) {
        unsafe {
            // Read all needed fields BEFORE the set, because `set` may
            // invalidate `this` once the owning thread wakes.
            let registry = (*this).registry;
            let registry = Arc::clone(registry);
            let target_worker_index = (*this).target_worker_index;

            if CoreLatch::set(&raw const (*this).core) {
                registry.notify_worker_latch_is_set(target_worker_index);
            }
        }
    }
}

/// Latch backed by a Mutex+Condvar. Supports a blocking `wait()`. Used when an
/// external (non-pool) thread must block.
#[derive(Debug)]
pub(crate) struct LockLatch {
    m: Mutex<bool>,
    v: Condvar,
}

impl LockLatch {
    #[inline]
    pub(crate) fn new() -> LockLatch {
        LockLatch {
            m: Mutex::new(false),
            v: Condvar::new(),
        }
    }

    /// Block until latch is set, then reset so it can be reused.
    pub(crate) fn wait_and_reset(&self) {
        let mut guard = self.m.lock();
        while !*guard {
            self.v.wait(&mut guard);
        }
        *guard = false;
    }

    pub(crate) fn wait(&self) {
        let mut guard = self.m.lock();
        while !*guard {
            self.v.wait(&mut guard);
        }
    }
}

impl Latch for LockLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        unsafe {
            let mut guard = (*this).m.lock();
            *guard = true;
            (*this).v.notify_all();
        }
    }
}

/// One-time blocking latch, used for thread termination.
#[derive(Debug)]
pub(crate) struct OnceLatch {
    core: CoreLatch,
}

impl OnceLatch {
    #[inline]
    pub(crate) fn new() -> OnceLatch {
        Self {
            core: CoreLatch::new(),
        }
    }

    /// Set the latch and wake the specific worker if it was sleeping.
    ///
    /// # Safety
    ///
    /// `this` must point to a valid `OnceLatch` that is set exactly once, and
    /// must outlive the waiter's wake-up path (the waiter only reads state
    /// before returning from `wait_until`).
    #[inline]
    pub(crate) unsafe fn set_and_tickle_one(
        this: *const Self,
        registry: &Registry,
        target_worker_index: usize,
    ) {
        if unsafe { CoreLatch::set(&raw const (*this).core) } {
            registry.notify_worker_latch_is_set(target_worker_index);
        }
    }
}

impl AsCoreLatch for OnceLatch {
    #[inline]
    fn as_core_latch(&self) -> &CoreLatch {
        &self.core
    }
}

/// Counting latch used by `scope`. Tracks a counter; only "set" when the
/// counter reaches zero.
pub(crate) struct CountLatch {
    counter: AtomicUsize,
    kind: CountLatchKind,
}

enum CountLatchKind {
    /// A latch for scopes created on a pool worker thread which will
    /// participate in work stealing while it waits.
    Stealing {
        latch: CoreLatch,
        registry: Arc<Registry>,
        worker_index: usize,
    },
    /// A latch for scopes created on a non-pool thread which will block.
    Blocking { latch: LockLatch },
}

#[cfg(all(test, loom))]
mod loom_tests {
    use std::{ptr, sync::Arc};

    use youpipe_sys::AtomicUsize;

    use super::*;

    /// The 4-state transition protocol: a sleeper may only be told "you were
    /// sleeping" (`set` returns true) if it had actually reached SLEEPING,
    /// and after `set` + join the latch must read SET regardless of the
    /// interleaving.
    #[test]
    fn core_latch_sleep_wake_race() {
        loom::model(|| {
            let latch = Arc::new(CoreLatch::new());
            let l2 = Arc::clone(&latch);
            let sleeper = loom::thread::spawn(move || {
                if l2.get_sleepy() && l2.fall_asleep() {
                    // The setter always runs, so the probe eventually flips.
                    while !l2.probe() {
                        loom::hint::spin_loop();
                    }
                    l2.wake_up();
                }
            });
            let was_sleeping = unsafe { CoreLatch::set(ptr::from_ref(&*latch)) };
            sleeper.join().unwrap();
            assert!(latch.probe(), "latch must end SET");
            // No assertion on `was_sleeping` alone (either transition is
            // legal), but if set() saw SLEEPING the sleeper must observe SET
            // before returning — checked by the spin + probe above.
            let _ = was_sleeping;
        });
    }

    /// `wait_spin_assist` (Blocking variant): one external setter plus one
    /// "reserve chunk" executed by the waiter's own hook. The hook's
    /// decrement participates in the same counter, so the wait must return
    /// only after both sides completed, with both sides' writes visible —
    /// the hook runs on the waiting thread, but its store must be ordered by
    /// the counter's SeqCst chain exactly like a remote setter's.
    #[test]
    fn count_latch_assist_hook_decrements_like_a_setter() {
        loom::model(|| {
            use std::cell::Cell;

            struct Hooked {
                latch: CountLatch,
                flag: AtomicUsize,
            }

            let hl = Arc::new(Hooked {
                latch: CountLatch::with_count(2, None),
                flag: AtomicUsize::new(0),
            });

            let a = Arc::clone(&hl);
            loom::thread::spawn(move || {
                a.flag.fetch_or(1, Ordering::Release);
                unsafe { CountLatch::set(ptr::from_ref(&a.latch)) };
            });

            let b = Arc::clone(&hl);
            let ran = Cell::new(false);
            b.latch.wait_spin_assist(|| {
                if ran.get() {
                    return false;
                }
                ran.set(true);
                b.flag.fetch_or(2, Ordering::Release);
                unsafe { CountLatch::set(ptr::from_ref(&b.latch)) };
                true
            });

            assert_eq!(
                hl.flag.load(Ordering::Acquire),
                3,
                "both the setter's and the hook's writes visible"
            );
        });
    }

    /// `CountLatch` (Blocking variant): two setters + a `wait_spin` waiter.
    /// Relaxed data flags published by each setter must be visible after the
    /// wait returns — the counter's SeqCst fetch_sub + LockLatch mutex must
    /// establish the happens-before chain (loom explores the relaxed loads).
    #[test]
    fn count_latch_blocking_publishes_setter_writes() {
        loom::model(|| {
            let latch = Arc::new(CountLatch::with_count(2, None));
            let flag_a = Arc::new(AtomicUsize::new(0));
            let flag_b = Arc::new(AtomicUsize::new(0));

            let la = Arc::clone(&latch);
            let fa = Arc::clone(&flag_a);
            let t1 = loom::thread::spawn(move || {
                fa.store(1, Ordering::Release);
                unsafe { CountLatch::set(ptr::from_ref(&*la)) };
            });
            let lb = Arc::clone(&latch);
            let fb = Arc::clone(&flag_b);
            let t2 = loom::thread::spawn(move || {
                fb.store(1, Ordering::Release);
                unsafe { CountLatch::set(ptr::from_ref(&*lb)) };
            });
            latch.wait_spin();
            t1.join().unwrap();
            t2.join().unwrap();
            assert_eq!(flag_a.load(Ordering::Acquire), 1);
            assert_eq!(flag_b.load(Ordering::Acquire), 1);
        });
    }
}

impl std::fmt::Debug for CountLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            CountLatchKind::Stealing { .. } => f.debug_tuple("Stealing").finish(),
            CountLatchKind::Blocking { .. } => f.debug_tuple("Blocking").finish(),
        }
    }
}

impl CountLatch {
    pub(crate) fn new(owner: Option<(&Arc<Registry>, usize)>) -> Self {
        Self::with_count(1, owner)
    }

    pub(crate) fn with_count(count: usize, owner: Option<(&Arc<Registry>, usize)>) -> Self {
        Self {
            counter: AtomicUsize::new(count),
            kind: match owner {
                Some((registry, worker_index)) => CountLatchKind::Stealing {
                    latch: CoreLatch::new(),
                    registry: Arc::clone(registry),
                    worker_index,
                },
                None => CountLatchKind::Blocking {
                    latch: LockLatch::new(),
                },
            },
        }
    }

    #[inline]
    pub(crate) fn increment(&self) {
        let old = self.counter.fetch_add(1, Ordering::Relaxed);
        debug_assert!(old != 0);
    }

    pub(crate) fn wait(&self) {
        match &self.kind {
            CountLatchKind::Stealing {
                latch,
                registry,
                worker_index,
            } => {
                debug_assert!(registry.num_threads() > *worker_index);
                Registry::wait_until_worker(latch);
            },
            CountLatchKind::Blocking { latch } => latch.wait(),
        }
    }

    /// Spin-then-park wait for the off-pool (`Blocking`) variant.
    ///
    /// Tight-spins on `counter` for a bounded budget first, so that by the time
    /// we acquire the latch's mutex (below) the last chunk has — in the common
    /// short-wait case — already finished `LockLatch::set`, meaning `*guard` is
    /// `true` and we return without a condvar park syscall. That syscall
    /// (`futex`/`pthread_cond_signal`) is ~10–20 µs of fixed overhead per batch
    /// and was the bulk of the remaining gap to rayon on the 1 K `cpu_heavy`
    /// case. For long waits we exhaust the spin budget and the mutex path
    /// transparently parks on the condvar, releasing the core.
    ///
    /// # Why the mutex acquire is load-bearing (not just the spin)
    ///
    /// The spin may *not* return directly. The last chunk, after
    /// `counter.fetch_sub` brings it to 0, still has to run `LockLatch::set`
    /// (lock → store → notify → unlock) — which dereferences the latch via the
    /// raw pointer the chunk holds. If the driver observed `counter == 0` and
    /// freed the latch (by returning), the chunk's in-flight `LockLatch::set`
    /// would touch freed memory (observed as SIGSEGV). Going through the mutex
    /// fixes this: `LockLatch::set` holds the very same mutex while touching
    /// the latch, so our `lock()` cannot return until the setter has released
    /// it (finished touching the latch). Once we see `*guard == true` and
    /// return, the setter is provably done — no use-after-free. The mutex
    /// lock/unlock on an uncontended `parking_lot` mutex is ~10–30 ns
    /// (adaptive spin, no syscall) — ~1000× cheaper than the condvar park it
    /// replaces.
    ///
    /// # Synchronization
    ///
    /// `Latch::set` does `counter.fetch_sub(SeqCst)`; the last decrement (1→0)
    /// additionally runs `LockLatch::set`. An `Acquire` load that reads `0`
    /// synchronizes with that final `SeqCst` decrement, and the mutex
    /// acquire/release pairs every prior chunk write (e.g. the hybrid driver's
    /// `succeeded` flags) into the driver's view — the same guarantee the
    /// condvar path relies on.
    ///
    /// No-stealing note: the caller is an off-pool thread with no worker deque,
    /// so unlike the `Stealing` variant it cannot help by stealing from peers
    /// while it waits — a bounded spin is strictly better than parking for
    /// short waits. (The hybrid dispatcher layers work-*assisting* on top via
    /// [`wait_spin_assist`]; the `Stealing` variant keeps its work-stealing
    /// `wait()`.)
    ///
    /// [`wait_spin_assist`]: CountLatch::wait_spin_assist
    pub(crate) fn wait_spin(&self) {
        match &self.kind {
            CountLatchKind::Stealing {
                latch,
                registry,
                worker_index,
            } => {
                // On-pool: keep the work-stealing wait — the worker can steal
                // while it waits, which is strictly better than spinning.
                debug_assert!(registry.num_threads() > *worker_index);
                Registry::wait_until_worker(latch);
            },
            CountLatchKind::Blocking { latch } => {
                // Tier 1: tight spin (PAUSE on x86). Each iteration is ~10–40 ns;
                // this budget covers ~100–150 µs, enough to catch any batch whose
                // parallel time fits inside the small/medium dispatch envelope.
                // We only `break` (not `return`): see the method doc on why the
                // mutex acquire below is mandatory for soundness.
                for _ in 0..OFF_POOL_SPIN_ITERS {
                    if self.counter.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    std::hint::spin_loop();
                }
                // Tier 2: condvar park. In the common case the spin above let
                // the setter finish `LockLatch::set`, so `*guard == true` here
                // and we return without parking (no syscall). If the budget ran
                // out (genuinely long wait), this parks until the setter's
                // notify — releasing the core. Either way the mutex serializes
                // us against the setter's in-flight latch access.
                latch.wait();
            },
        }
    }

    /// [`wait_spin`](Self::wait_spin) with a work-assist hook: between spins,
    /// the off-pool driver runs one of its **reserve chunks** via `try_work`,
    /// and productive work resets the spin budget.
    ///
    /// The hybrid dispatcher keeps a small tail of chunks out of the injector
    /// (see `hybrid_dispatch`); `try_work` returns `true` while any remain,
    /// `false` once the local reserve is exhausted (monotone — the driver is
    /// their only executor). This lets the driver absorb the wake-latency tail
    /// — chunks that slow-to-wake workers would otherwise gate the batch on —
    /// without the driver ever touching the pool-global injector (whose FIFO
    /// order foreign submitters, e.g. stream feeders, depend on; an earlier
    /// pop-and-requeue assist variant reordered those jobs and deadlocked
    /// resident stream workers parked on empty channels).
    ///
    /// Tier structure and the mutex-serialization soundness argument are
    /// identical to [`wait_spin`](Self::wait_spin); the only additions are the
    /// hook call and the budget reset on productive work.
    ///
    /// Liveness: `try_work` returning `false` means every remaining chunk was
    /// injected (and is popped/executing on a worker) — parking is safe, the
    /// last executor's `LockLatch::set` notifies us.
    pub(crate) fn wait_spin_assist(&self, mut try_work: impl FnMut() -> bool) {
        match &self.kind {
            CountLatchKind::Stealing { .. } => {
                // Unreachable from the hybrid dispatcher (on-pool small
                // batches take the single-tree shortcut before the latch
                // is even built; large batches reserve 0): the
                // work-stealing wait cannot run the assist hook (the
                // reserve chunks would have no executor). Kept as a safe
                // fallback for other callers.
                self.wait_spin();
            },
            CountLatchKind::Blocking { latch } => {
                let mut spins_left = OFF_POOL_SPIN_ITERS;
                loop {
                    if try_work() {
                        // Productive work resets the budget: a driver that
                        // keeps running its reserve chunks is making progress,
                        // not spinning, and must not fall through to the
                        // condvar while reserve work remains.
                        spins_left = OFF_POOL_SPIN_ITERS;
                        if self.counter.load(Ordering::Acquire) == 0 {
                            break;
                        }
                        continue;
                    }
                    if self.counter.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    if spins_left == 0 {
                        // Tier 2: condvar park (see `wait_spin`). Returns only
                        // with the latch set — terminal state.
                        latch.wait();
                        return;
                    }
                    spins_left -= 1;
                    std::hint::spin_loop();
                }
                // Broke out of the loop on counter == 0: still serialize
                // against the last setter's in-flight `LockLatch::set` via the
                // mutex acquire (see `wait_spin`'s soundness doc). In the
                // common case `*guard` is already `true` — no park.
                latch.wait();
            },
        }
    }
}

/// Tight-spin iteration budget for [`CountLatch::wait_spin`]'s tier 1.
///
/// Tuned for the off-pool hybrid-dispatch driver: large enough to absorb the
/// full small/medium-batch dispatch envelope (~100–150 µs of parallel work +
/// wake latency) without falling through to the condvar, small enough that a
/// truly long wait (ms+) only burns ~100 µs of one calling-thread core before
/// parking. See the `par_index_collect_hybrid` comment in `fused.rs`.
///
/// Under `loom` the budget shrinks to keep the model's state space small —
/// the spin's only synchronization role is the final mutex acquire, which the
/// model exercises regardless of the iteration count.
#[cfg(not(loom))]
const OFF_POOL_SPIN_ITERS: usize = 4096;
#[cfg(loom)]
const OFF_POOL_SPIN_ITERS: usize = 2;

impl Latch for CountLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        unsafe {
            if (*this).counter.fetch_sub(1, Ordering::SeqCst) == 1 {
                match &(*this).kind {
                    CountLatchKind::Stealing {
                        latch,
                        registry,
                        worker_index,
                    } => {
                        let registry = Arc::clone(registry);
                        let worker_index = *worker_index;
                        if CoreLatch::set(latch) {
                            registry.notify_worker_latch_is_set(worker_index);
                        }
                    },
                    CountLatchKind::Blocking { latch } => LockLatch::set(latch),
                }
            }
        }
    }
}

/// `&L` without `dereferenceable`, for passing to `Latch::set`.
pub(crate) struct LatchRef<'a, L> {
    inner: *const L,
    _marker: PhantomData<&'a L>,
}

impl<L> LatchRef<'_, L> {
    pub(crate) fn new(inner: &L) -> LatchRef<'_, L> {
        LatchRef {
            inner,
            _marker: PhantomData,
        }
    }
}

unsafe impl<L: Sync> Sync for LatchRef<'_, L> {}

impl<L> Deref for LatchRef<'_, L> {
    type Target = L;

    fn deref(&self) -> &L {
        // SAFETY: while &self exists, the inner latch is alive.
        unsafe { &*self.inner }
    }
}

impl<L: Latch> Latch for LatchRef<'_, L> {
    #[inline]
    unsafe fn set(this: *const Self) {
        unsafe { L::set((*this).inner) };
    }
}
