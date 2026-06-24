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

const ROUNDS_UNTIL_SLEEPY: u32 = 32;
const ROUNDS_UNTIL_SLEEPING: u32 = ROUNDS_UNTIL_SLEEPY + 1;

impl Sleep {
    pub(crate) fn new(n_threads: usize) -> Sleep {
        assert!(n_threads <= THREADS_MAX);
        Sleep {
            worker_sleep_states: (0..n_threads).map(|_| CachePadded::default()).collect(),
            counters: AtomicCounters::new(),
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
    pub(crate) fn work_found(&self) {
        let threads_to_wake = self.counters.sub_inactive_thread();
        // `sub_inactive_thread` returns at most 2, safe to truncate
        self.wake_any_threads(threads_to_wake as u32);
    }

    #[inline]
    pub(crate) fn no_work_found(
        &self,
        idle: &mut IdleState,
        latch: &CoreLatch,
        has_injected_jobs: impl FnOnce() -> bool,
    ) {
        if idle.rounds < ROUNDS_UNTIL_SLEEPY {
            thread::yield_now();
            idle.rounds += 1;
        } else if idle.rounds == ROUNDS_UNTIL_SLEEPY {
            idle.jobs_counter = self.announce_sleepy();
            idle.rounds += 1;
            thread::yield_now();
        } else if idle.rounds < ROUNDS_UNTIL_SLEEPING {
            idle.rounds += 1;
            thread::yield_now();
        } else {
            self.sleep(idle, latch, has_injected_jobs);
        }
    }

    #[cold]
    fn announce_sleepy(&self) -> JobsEventCounter {
        self.counters
            .increment_jobs_event_counter_if(JobsEventCounter::is_active)
            .jobs_counter()
    }

    #[cold]
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

        loop {
            let counters = self.counters.load(Ordering::SeqCst);

            // JEC changed since we got sleepy — new work was posted. Search again.
            if counters.jobs_counter() != idle.jobs_counter {
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
        } else {
            *is_blocked = true;
            while *is_blocked {
                sleep_state.condvar.wait(&mut is_blocked);
            }
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
    pub(crate) fn new_injected_jobs(&self, num_jobs: u32, queue_was_empty: bool) {
        // Fence guarantees sleepy/sleeping threads observe injected work.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.new_jobs(num_jobs, queue_was_empty);
    }

    /// New jobs were pushed onto a thread's local deque.
    #[inline]
    pub(crate) fn new_internal_jobs(&self, num_jobs: u32, queue_was_empty: bool) {
        self.new_jobs(num_jobs, queue_was_empty);
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
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
    fn wake_any_threads(&self, mut num_to_wake: u32) {
        if num_to_wake > 0 {
            for i in 0..self.worker_sleep_states.len() {
                if self.wake_specific_thread(i) {
                    num_to_wake -= 1;
                    if num_to_wake == 0 {
                        return;
                    }
                }
            }
        }
    }

    fn wake_specific_thread(&self, index: usize) -> bool {
        let sleep_state = &self.worker_sleep_states[index];
        let mut is_blocked = sleep_state.is_blocked.lock();
        if *is_blocked {
            *is_blocked = false;
            sleep_state.condvar.notify_one();
            // Decrement sleeping counter here (not in the woken thread) so other
            // posters see the updated count sooner.
            self.counters.sub_sleeping_thread();
            true
        } else {
            false
        }
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
