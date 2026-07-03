//! Sleep / wake governance. Packs three counters into one `AtomicUsize` so that
//! the fast path (posting work while threads are awake) is pure atomics — no
//! Mutex/Condvar in the hot path. Adapted from rayon-core (RFC #5).

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

use super::latch::CoreLatch;
use crate::{
    sync::sys::{Condvar, Mutex},
    util::CachePadded,
};

// ── Packed counter layout ──

/// Number of bits used for each thread counter field.
#[cfg(target_pointer_width = "64")]
const THREADS_BITS: usize = 16;

#[cfg(target_pointer_width = "32")]
const THREADS_BITS: usize = 8;

/// Max number of threads the pool can handle.
pub(crate) const THREADS_MAX: usize = (1 << THREADS_BITS) - 1;

const SLEEPING_SHIFT: usize = 0;
const INACTIVE_SHIFT: usize = THREADS_BITS;
const JEC_SHIFT: usize = 2 * THREADS_BITS;

/// Add one sleeping thread.
const ONE_SLEEPING: usize = 1;
/// Add one inactive thread (idle, sleepy, or sleeping).
const ONE_INACTIVE: usize = 1 << INACTIVE_SHIFT;
/// Add one to the jobs event counter.
const ONE_JEC: usize = 1 << JEC_SHIFT;

#[inline]
fn select_thread(word: usize, shift: usize) -> usize {
    (word >> shift) & THREADS_MAX
}

#[inline]
fn select_jec(word: usize) -> usize {
    word >> JEC_SHIFT
}

/// Atomic counters packing sleeping-threads, inactive-threads, and JEC.
#[allow(dead_code)]
pub(crate) struct AtomicCounters {
    value: AtomicUsize,
}

#[derive(Copy, Clone)]
pub(crate) struct Counters {
    word: usize,
}

/// A value read from the Jobs Event Counter (JEC).
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub(crate) struct JobsEventCounter(usize);

impl JobsEventCounter {
    const DUMMY: JobsEventCounter = JobsEventCounter(usize::MAX);

    #[inline]
    fn is_sleepy(self) -> bool {
        (self.0 & 1) == 0
    }

    #[inline]
    fn is_active(self) -> bool {
        !self.is_sleepy()
    }
}

impl AtomicCounters {
    #[inline]
    pub(crate) fn new() -> AtomicCounters {
        AtomicCounters {
            value: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> Counters {
        Counters {
            word: self.value.load(ordering),
        }
    }

    #[inline]
    fn try_exchange(&self, old: Counters, new: Counters, ordering: Ordering) -> bool {
        self.value
            .compare_exchange(old.word, new.word, ordering, Ordering::Relaxed)
            .is_ok()
    }

    /// Adds an inactive thread. Invoked when a thread enters its idle loop.
    #[inline]
    pub(crate) fn add_inactive_thread(&self) {
        self.value.fetch_add(ONE_INACTIVE, Ordering::SeqCst);
    }

    /// Increments the JEC if `when` holds for the current value.
    #[allow(private_bounds)]
    pub(crate) fn increment_jobs_event_counter_if(
        &self,
        when: impl Fn(JobsEventCounter) -> bool,
    ) -> Counters {
        loop {
            let old = self.load(Ordering::SeqCst);
            if when(old.jobs_counter()) {
                let new = Counters {
                    word: old.word.wrapping_add(ONE_JEC),
                };
                if self.try_exchange(old, new, Ordering::SeqCst) {
                    return new;
                }
                std::hint::spin_loop();
            } else {
                return old;
            }
        }
    }

    /// Subtracts an inactive thread (when it finds work). Returns the number of
    /// sleeping threads to wake up.
    #[inline]
    pub(crate) fn sub_inactive_thread(&self) -> usize {
        let old = Counters {
            word: self.value.fetch_sub(ONE_INACTIVE, Ordering::SeqCst),
        };
        // Heuristic: when an inactive thread becomes active, wake up to 2
        // sleeping threads (to keep the pipeline fed).
        Ord::min(old.sleeping_threads(), 2)
    }

    /// Subtracts a sleeping thread.
    #[inline]
    pub(crate) fn sub_sleeping_thread(&self) {
        self.value.fetch_sub(ONE_SLEEPING, Ordering::SeqCst);
    }

    #[inline]
    #[allow(private_interfaces)]
    pub(crate) fn try_add_sleeping_thread(&self, old: Counters) -> bool {
        let new = Counters {
            word: old.word + ONE_SLEEPING,
        };
        self.try_exchange(old, new, Ordering::SeqCst)
    }
}

impl Counters {
    #[inline]
    fn jobs_counter(self) -> JobsEventCounter {
        JobsEventCounter(select_jec(self.word))
    }

    #[inline]
    fn inactive_threads(self) -> usize {
        select_thread(self.word, INACTIVE_SHIFT)
    }

    #[inline]
    fn awake_but_idle_threads(self) -> usize {
        self.inactive_threads() - self.sleeping_threads()
    }

    #[inline]
    fn sleeping_threads(self) -> usize {
        select_thread(self.word, SLEEPING_SHIFT)
    }
}

// ── Sleep state machine ──

pub(crate) struct Sleep {
    /// Per-worker sleep state.
    worker_sleep_states: Vec<crate::util::CachePadded<WorkerSleepState>>,
    counters: AtomicCounters,
    /// Bitmask of currently-sleeping workers (bit `i` set iff worker `i` is
    /// parked in `condvar.wait`). Lets `wake_any_threads` jump directly to
    /// sleeping workers instead of doing a rotating linear scan that locks
    /// every awake worker's `is_blocked` mutex along the way.
    ///
    /// Under the fork/join `join` pattern, when most workers are awake and
    /// scanning, each `work_found`/`new_internal_jobs` wake attempt used to
    /// scan several awake workers (each lock is ~70 ns L3 hit uncontended),
    /// and under contention the p99 `work_found` reached ~100 µs as 32
    /// workers piled on the same victim mutex. The mask collapses the scan
    /// to exactly the set bits, so awake workers are never touched.
    ///
    /// The mask is racy by design (set in `sleep()` under the worker's own
    /// mutex, cleared in `wake_specific_thread` under the same mutex); a
    /// stale set bit just causes one redundant lock attempt that returns
    /// `false`. A stale clear bit just causes a missed wake, which is
    /// recovered by the existing JEC/`increment_jobs_event_counter_if`
    /// retry loop (sleepers re-check `jobs_counter` before parking).
    sleeping_mask: CachePadded<AtomicUsize>,
}

#[derive(Default)]
struct WorkerSleepState {
    is_blocked: Mutex<bool>,
    condvar: Condvar,
}

/// Created when a thread becomes idle. Consumed when work is found.
pub(crate) struct IdleState {
    worker_index: usize,
    rounds: u32,
    jobs_counter: JobsEventCounter,
}

/// Idle rounds spent busy-spinning (the `pause` instruction) before yielding
/// the OS thread.
///
/// Stolen `join` work typically arrives within microseconds. A `sched_yield`
/// here costs ~1 µs *and* may migrate the worker off its warm core (dropping
/// its cache), and with ~400 k idle rounds per 100 k-item run
/// (hotpath-measured) that syscall overhead was the dominant gap vs rayon,
/// which also busy-spins before yielding. Burn a little CPU instead — the pool
/// parks for real one round past `ROUNDS_UNTIL_SLEEPY`, so long-idle CPU cost
/// stays bounded.
///
/// Measured (32-core, A/B vs the all-yield predecessor): `sync_cpu_heavy`
/// youpipe `pipe().map().collect()` improved −14 % @ 10 k, −18 % @ 100 k;
/// `pipeline_fusion` and `sync_lightweight` improved −5…−13 %. (The 1 k batch
/// used to hit a serial short-circuit and never reach here; that heuristic was
/// later removed — see `prefers_serial` in `builder/typed/fused.rs`.)
///
/// Widening the spin window further (A/B tried 128 in 2026-06) regressed
/// everything by +20-36 %: 32 workers all spinning burn enough coherence
/// traffic on the deque / counter cache lines to throttle the cores, and the
/// longer pause window delays the first round of `find_work` after a stolen
/// job arrives. 32 stays the sweet spot.
const ROUNDS_SPIN: u32 = 32;
/// Idle rounds spent in `sched_yield` (cooperate but stay runnable) after the
/// busy-spin phase. At this round the worker announces "sleepy" (bumps the JEC
/// so a later poster can detect it), yields once more, and then the next idle
/// round falls through to the actual `condvar` park in `sleep()`.
///
/// Widening the yield window (A/B tried 64 and 96 in 2026-06) lifted the
/// 10 k and 1 M `sync_lightweight` cases by 5-30 % — workers stay runnable
/// across `criterion`'s ~30-100 µs `iter_batched` gap and skip the next iter's
/// `condvar.notify_one` wake cascade — but consistently regressed the 100 k
/// variants (especially `_cold`) by 4-12 % because the longer yield window
/// keeps workers in syscall overhead during the closure's intra-iter idle
/// rounds. 32 (matching rayon) stays the global sweet spot.
const ROUNDS_UNTIL_SLEEPY: u32 = ROUNDS_SPIN + 32;

impl Sleep {
    pub(crate) fn new(n_threads: usize) -> Sleep {
        assert!(n_threads <= THREADS_MAX);
        Sleep {
            worker_sleep_states: (0..n_threads).map(|_| CachePadded::default()).collect(),
            counters: AtomicCounters::new(),
            sleeping_mask: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub(crate) fn start_looking(&self, worker_index: usize) -> IdleState {
        self.counters.add_inactive_thread();
        IdleState {
            worker_index,
            rounds: 0,
            jobs_counter: JobsEventCounter::DUMMY,
        }
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn work_found(&self) {
        let threads_to_wake = self.counters.sub_inactive_thread();
        // `sub_inactive_thread` returns at most 2, safe to truncate
        self.wake_any_threads(threads_to_wake as u32);
    }

    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn no_work_found(
        &self,
        idle: &mut IdleState,
        latch: &CoreLatch,
        has_injected_jobs: impl FnOnce() -> bool,
    ) {
        if idle.rounds < ROUNDS_SPIN {
            // Busy-spin phase: stay on-core, keep cache warm, no syscall.
            std::hint::spin_loop();
            idle.rounds += 1;
        } else if idle.rounds < ROUNDS_UNTIL_SLEEPY {
            // Yield phase: cooperate with the OS scheduler but stay runnable.
            thread::yield_now();
            idle.rounds += 1;
        } else if idle.rounds == ROUNDS_UNTIL_SLEEPY {
            idle.jobs_counter = self.announce_sleepy();
            idle.rounds += 1;
            thread::yield_now();
        } else {
            self.sleep(idle, latch, has_injected_jobs);
        }
    }

    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn announce_sleepy(&self) -> JobsEventCounter {
        self.counters
            .increment_jobs_event_counter_if(JobsEventCounter::is_active)
            .jobs_counter()
    }

    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn sleep(
        &self,
        idle: &mut IdleState,
        latch: &CoreLatch,
        has_injected_jobs: impl FnOnce() -> bool,
    ) {
        let worker_index = idle.worker_index;

        if !latch.get_sleepy() {
            return;
        }

        let sleep_state = &self.worker_sleep_states[worker_index];
        let mut is_blocked = sleep_state.is_blocked.lock();

        if !latch.fall_asleep() {
            idle.wake_fully();
            return;
        }

        // Pre-publish our sleeping bit *before* the counter commit. A waker
        // that observes our `sleeping_threads` increment also observes the bit
        // (the bit's `Release` is ordered before the counter's `SeqCst`
        // exchange in program order; the waker reads counter with `SeqCst`
        // then mask with `Acquire`, so the synchronizes-with pair carries the
        // bit write into the waker's observation). Setting the bit after the
        // commit instead opens a race where the waker sees the counter but
        // not the bit, skips us, and leaves us parked with no notifier.
        //
        // We hold `is_blocked`'s mutex throughout the commit, so any concurrent
        // `wake_specific_thread` blocks here until our `condvar.wait` releases
        // the mutex (by which point `*is_blocked == true`, so the waker finds
        // us rather than missing the wake).
        let bit = 1usize << worker_index;
        self.sleeping_mask.fetch_or(bit, Ordering::Release);

        loop {
            let counters = self.counters.load(Ordering::SeqCst);

            // JEC changed since we got sleepy — new work was posted. Search again.
            if counters.jobs_counter() != idle.jobs_counter {
                self.sleeping_mask.fetch_and(!bit, Ordering::Release);
                idle.wake_partly();
                latch.wake_up();
                return;
            }

            if self.counters.try_add_sleeping_thread(counters) {
                break;
            }
        }

        // Final check for injected jobs to prevent deadlock.
        std::sync::atomic::fence(Ordering::SeqCst);
        if has_injected_jobs() {
            self.counters.sub_sleeping_thread();
            // We never reached `condvar.wait`, so no waker cleared our bit.
            self.sleeping_mask.fetch_and(!bit, Ordering::Release);
        } else {
            // If we don't see an injected job (the normal case), then flag
            // ourselves as asleep and wait till we are notified.
            *is_blocked = true;
            while *is_blocked {
                sleep_state.condvar.wait(&mut is_blocked);
            }
            // Woken by `wake_specific_thread`, which already cleared our bit.
        }

        idle.wake_fully();
        latch.wake_up();
    }

    /// Notify a specific worker that its latch has been set.
    pub(crate) fn notify_worker_latch_is_set(&self, target_worker_index: usize) {
        self.wake_specific_thread(target_worker_index);
    }

    /// New jobs were injected from outside the pool.
    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn new_injected_jobs(&self, num_jobs: u32, queue_was_empty: bool) {
        // Fence guarantees sleepy/sleeping threads observe injected work.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.new_jobs(num_jobs, queue_was_empty);
    }

    /// New jobs were pushed onto a thread's local deque.
    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn new_internal_jobs(&self, num_jobs: u32, queue_was_empty: bool) {
        self.new_jobs(num_jobs, queue_was_empty);
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn new_jobs(&self, num_jobs: u32, queue_was_empty: bool) {
        let counters = self
            .counters
            .increment_jobs_event_counter_if(JobsEventCounter::is_sleepy);
        // Both values are bounded by THREADS_MAX (≤65535 on 64-bit), safe to truncate
        let num_awake_but_idle = counters.awake_but_idle_threads() as u32;
        let num_sleepers = counters.sleeping_threads() as u32;

        if num_sleepers == 0 {
            return;
        }

        if !queue_was_empty {
            let num_to_wake = Ord::min(num_jobs, num_sleepers);
            self.wake_any_threads(num_to_wake);
        } else if num_awake_but_idle < num_jobs {
            let num_to_wake = Ord::min(num_jobs - num_awake_but_idle, num_sleepers);
            self.wake_any_threads(num_to_wake);
        }
    }

    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn wake_any_threads(&self, mut num_to_wake: u32) {
        if num_to_wake == 0 {
            return;
        }
        // Snapshot the sleeping mask and walk only set bits. Each bit is
        // racing with the worker's own sleep/wake transitions, so the mask
        // may be stale — `wake_specific_thread` re-checks `is_blocked` under
        // the worker's mutex and returns `false` for any bit we no longer
        // own. Reload the mask after a failed wake to pick up bits the
        // racing sleeper just published.
        let mut mask = self.sleeping_mask.load(Ordering::Acquire);
        while num_to_wake > 0 && mask != 0 {
            let i = mask.trailing_zeros() as usize;
            mask &= !(1usize << i);
            if self.wake_specific_thread(i) {
                num_to_wake -= 1;
            } else {
                // Bit was stale; reload in case a fresh sleeper published.
                mask = self.sleeping_mask.load(Ordering::Acquire);
            }
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn wake_specific_thread(&self, index: usize) -> bool {
        let sleep_state = &self.worker_sleep_states[index];
        // Hold the mutex only long enough to flip `is_blocked` and clear the
        // sleeping bit/counter, then drop it *before* `condvar.notify_one`.
        // Holding the mutex across notify serialises the woken thread's
        // re-acquire (it must wait for us to release), and on a contended
        // wake path that latency piles up — measured p99 `work_found` was
        // ~270 µs at sync_lightweight 1 M when notify was inside the lock;
        // dropping the lock first cuts the tail substantially.
        let woken = {
            let mut is_blocked = sleep_state.is_blocked.lock();
            if *is_blocked {
                *is_blocked = false;
                // Clear the sleeping bit before notifying so concurrent
                // `wake_any_threads` scanners don't pile onto this worker.
                self.sleeping_mask
                    .fetch_and(!(1 << index), Ordering::Release);
                // Decrement sleeping counter here (not in the woken thread)
                // so other posters see the updated count sooner.
                self.counters.sub_sleeping_thread();
                true
            } else {
                false
            }
        };
        if woken {
            sleep_state.condvar.notify_one();
        }
        woken
    }
}

impl IdleState {
    fn wake_fully(&mut self) {
        self.rounds = 0;
        self.jobs_counter = JobsEventCounter::DUMMY;
    }

    fn wake_partly(&mut self) {
        self.rounds = ROUNDS_UNTIL_SLEEPY;
        self.jobs_counter = JobsEventCounter::DUMMY;
    }
}
