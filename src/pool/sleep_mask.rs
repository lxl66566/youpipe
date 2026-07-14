//! Fixed-size inline bitmask tracking which pool workers are parked in
//! `condvar.wait`.
//!
//! A compile-time `[AtomicU64; N]` array — no heap allocation, no pointer
//! indirection. `THREADS_BITS` is sized (9 on 64-bit, 8 on 32-bit) so the
//! entire mask fits in one cache line, and `SleepMask` lives inside the
//! `Arc<Registry>` allocation right next to the hot atomic `counters`.
//!
//! For the common case (≤64 workers) only `words[0]` is ever touched, and it
//! typically shares a cache line with `counters`, so the wake path is
//! cache-hot with zero indirection.
//!
//! # Historical note
//!
//! The predecessor was a single `AtomicUsize`. Rust masks shift amounts
//! modulo the bit width (defined behaviour, not a panic), so
//! `1usize << worker_index` for `worker_index >= 64` silently wrapped to
//! bit 0. Workers 0 and 64 thus aliased bit 0, causing an unrecoverable
//! infinite loop in `wake_any_threads` under heavy oversubscription
//! (`ComputePool::new(128)` on a 4-core CI runner). The fixed multi-word
//! array gives every worker a unique bit with zero runtime cost.

use std::sync::atomic::{AtomicU64, Ordering};

use super::sleep::THREADS_MAX;

const BITS_PER_WORD: usize = 64;

/// Number of 64-bit words needed to cover `THREADS_MAX` workers.
/// 64-bit: ceil(511 / 64) = 8 words = 64 B (one cache line).
/// 32-bit: ceil(255 / 64) = 4 words = 32 B.
const MASK_WORDS: usize = THREADS_MAX.div_ceil(BITS_PER_WORD);

/// Atomic bitmask tracking which workers are parked in `condvar.wait`.
///
/// Each 64-bit word covers 64 worker slots: workers 0..63 in `words[0]`,
/// 64..127 in `words[1]`, etc. Set bit `i` ⟹ worker `i` is sleeping.
/// The mask is racy by design (set in `sleep()` under the worker's own
/// mutex, cleared in `wake_specific_thread` under the same mutex); a stale
/// set bit just causes one redundant lock attempt that returns `false`.
pub(crate) struct SleepMask {
    words: [AtomicU64; MASK_WORDS],
    /// Active words (ceil(n_threads / 64)). Words beyond this are always
    /// zero and never scanned, keeping the wake loop tight for small pools.
    n_words: usize,
}

impl SleepMask {
    pub(crate) fn new(n_threads: usize) -> Self {
        Self {
            words: std::array::from_fn(|_| AtomicU64::new(0)),
            n_words: n_threads.div_ceil(BITS_PER_WORD),
        }
    }

    /// Atomically set the bit for `worker_index`.
    #[inline]
    pub(crate) fn set(&self, worker_index: usize) {
        let (word, bit) = split_index(worker_index);
        self.words[word].fetch_or(bit, Ordering::Release);
    }

    /// Atomically clear the bit for `worker_index`.
    #[inline]
    pub(crate) fn clear(&self, worker_index: usize) {
        let (word, bit) = split_index(worker_index);
        self.words[word].fetch_and(!bit, Ordering::Release);
    }

    /// Scan all set bits, invoking `wake_fn(worker_index)` for each.
    ///
    /// Returns the number of workers for which `wake_fn` returned `true`.
    /// Stops early once `target` successful wakes have been recorded.
    ///
    /// When `wake_fn` returns `false` (stale bit from a racing sleep/wake
    /// transition), the scan restarts from the first word — reloading fresh
    /// values to pick up any bits a racing sleeper just published. With
    /// per-worker unique bits this loop always terminates: a persistently
    /// stale bit cannot exist because the owning worker clears its own bit
    /// on every exit path from `sleep()`.
    pub(crate) fn wake_scan(&self, target: u32, mut wake_fn: impl FnMut(usize) -> bool) -> u32 {
        if target == 0 {
            return 0;
        }
        let mut woken = 0u32;
        'outer: while woken < target {
            for w in 0..self.n_words {
                let mut bits = self.words[w].load(Ordering::Acquire);
                while bits != 0 {
                    let bit_pos = bits.trailing_zeros() as usize;
                    bits &= !(1u64 << bit_pos);
                    let worker_index = w * BITS_PER_WORD + bit_pos;
                    if wake_fn(worker_index) {
                        woken += 1;
                        if woken >= target {
                            return woken;
                        }
                    } else {
                        // Stale bit — reload all words and restart.
                        continue 'outer;
                    }
                }
            }
            // Scanned every active word; no more set bits.
            break;
        }
        woken
    }
}

/// Split `worker_index` into (word_index, bit_mask).
#[inline]
fn split_index(worker_index: usize) -> (usize, u64) {
    let word = worker_index / BITS_PER_WORD;
    let bit = 1u64 << (worker_index % BITS_PER_WORD);
    (word, bit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_clear_single_word() {
        let mask = SleepMask::new(50);
        mask.set(0);
        mask.set(49);
        let woken = mask.wake_scan(2, |i| {
            assert!(i == 0 || i == 49);
            true
        });
        assert_eq!(woken, 2);
    }

    #[test]
    fn set_clear_cross_word_boundary() {
        // 128 threads → words[0] (0..63) + words[1] (64..127).
        let mask = SleepMask::new(128);
        mask.set(63);
        mask.set(64);
        mask.set(127);

        let mut visited = Vec::new();
        let woken = mask.wake_scan(3, |i| {
            visited.push(i);
            true
        });
        assert_eq!(woken, 3);
        assert!(visited.contains(&63));
        assert!(visited.contains(&64));
        assert!(visited.contains(&127));

        // Clear and verify scan finds nothing.
        mask.clear(63);
        mask.clear(64);
        mask.clear(127);
        let woken2 = mask.wake_scan(3, |_| true);
        assert_eq!(woken2, 0);
    }

    #[test]
    fn stale_bit_triggers_rescan() {
        // With unique per-worker bits, a stale bit is always transient: the
        // owning worker clears it on every exit path. This test simulates that
        // by clearing the bit inside wake_fn on rejection.
        let mask = SleepMask::new(130);
        mask.set(5);
        mask.set(70);

        let mut woken_indices = Vec::new();
        let woken = mask.wake_scan(2, |i| {
            if i == 5 {
                // Simulate "worker 5 already awake" — clear stale bit, reject.
                mask.clear(5);
                false
            } else {
                woken_indices.push(i);
                true
            }
        });
        assert_eq!(woken, 1);
        assert_eq!(woken_indices, [70]);
    }

    #[test]
    fn wake_scan_stops_at_target() {
        let mask = SleepMask::new(200);
        for i in 0..10 {
            mask.set(i);
        }
        let woken = mask.wake_scan(3, |_| true);
        assert_eq!(woken, 3);
    }

    #[test]
    fn empty_mask_wakes_none() {
        let mask = SleepMask::new(64);
        let woken = mask.wake_scan(5, |_| true);
        assert_eq!(woken, 0);
    }

    #[test]
    fn cross_word_bits_are_distinct() {
        // Worker 64's bit is in words[1], distinct from worker 0's bit in
        // words[0]. The original single-word bug aliased these two — this
        // test is the regression guard.
        let mask = SleepMask::new(130);
        mask.set(64); // words[1] bit 0
        assert_eq!(mask.words[0].load(Ordering::Relaxed), 0);
        assert_eq!(mask.words[1].load(Ordering::Relaxed), 1);
        mask.set(0); // words[0] bit 0
        assert_eq!(mask.words[0].load(Ordering::Relaxed), 1);

        // wake_scan must reach worker 64 (in the second word) without
        // spinning on worker 0's bit. wake_fn returns `false` for worker 0
        // only after clearing its bit — matching the real
        // `wake_specific_thread` contract (a `false` return always coincides
        // with the bit being cleared by the owning worker).
        let woken = mask.wake_scan(1, |i| {
            if i == 0 {
                mask.clear(0);
                false
            } else {
                assert_eq!(i, 64);
                true
            }
        });
        assert_eq!(woken, 1);
    }
}
