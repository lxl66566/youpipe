//! Modify by frostyplanet@gmail.com for the crossfire crate:
//!
//!   - Optimise for MPSC scenario;
//!   - Pack head/tail for cache efficiency;
//!   - Add token interface according to crossbeam-channel
//!   - Modified push() to push_with_ptr();
//!   - Add try_push_oneshot();
//!   - youpipe fork extension (2026-09): pop_batch() — claim a run of ready
//!     items with ONE `recv` SeqCst store, amortizing the per-item cursor
//!     update over the batch (see docs/src/dev/streaming.md "Batched ring
//!     operations" in the youpipe repo).
//!
//! Fork from crossbeam-queue crate commit 5a154def002304814d50f3c7658bd30eb46b2fad
//!
//! The MIT License (MIT)
//!
//! Copyright (c) 2025, 2026 frostyplanet@gmail.com
//!
//! Copyright (c) 2019 The Crossbeam Project Developers
//!
//! Permission is hereby granted, free of charge, to any
//! person obtaining a copy of this software and associated
//! documentation files (the "Software"), to deal in the
//! Software without restriction, including without
//! limitation the rights to use, copy, modify, merge,
//! publish, distribute, sublicense, and/or sell copies of
//! the Software, and to permit persons to whom the Software
//! is furnished to do so, subject to the following
//! conditions:
//!
//! The above copyright notice and this permission notice
//! shall be included in all copies or substantial portions
//! of the Software.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
//! ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
//! TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//! PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
//! SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
//! CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
//! OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
//! IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
//! DEALINGS IN THE SOFTWARE.
//!
//! The implementation is based on Dmitry Vyukov's bounded MPMC queue.
//!
//! Source:
//!   - <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>

use core::cell::UnsafeCell;

use crate::flavor::Token;
use core::mem::{self, MaybeUninit};
use core::panic::{RefUnwindSafe, UnwindSafe};
use core::ptr;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crossbeam_utils::{Backoff, CachePadded};

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    stamp: AtomicUsize,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded multi-producer single-consumer queue.
pub struct ArrayQueueMpsc<T> {
    /// The sender state.
    ///
    /// High bits: head_cached
    /// Low bits: tail
    sender: CachePadded<AtomicU64>,

    /// The receiver state.
    ///
    /// High bits: tail_cached
    /// Low bits: head
    recv: CachePadded<AtomicU64>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: u32,
}

unsafe impl<T> Sync for ArrayQueueMpsc<T> {}
unsafe impl<T> Send for ArrayQueueMpsc<T> {}

impl<T> UnwindSafe for ArrayQueueMpsc<T> {}
impl<T> RefUnwindSafe for ArrayQueueMpsc<T> {}

impl<T> ArrayQueueMpsc<T> {
    /// Creates a new bounded queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity is zero.
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "capacity must be non-zero");
        assert!(cap < (1 << 31), "capacity too large for u32 logic");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized
        // with stamps.
        let buffer: Box<[Slot<T>]> = (0..cap)
            .map(|i| {
                // Set the stamp to `i`.
                Slot { stamp: AtomicUsize::new(i), value: UnsafeCell::new(MaybeUninit::uninit()) }
            })
            .collect();

        // One lap is the smallest power of two greater than `cap`.
        let one_lap = (cap + 1).next_power_of_two() as u32;

        Self {
            buffer,
            one_lap,
            recv: CachePadded::new(AtomicU64::new(((tail as u64) << 32) | (head as u64))),
            sender: CachePadded::new(AtomicU64::new(((head as u64) << 32) | (tail as u64))),
        }
    }

    #[inline(always)]
    fn _try_push(
        &self, sender_val: u64, tail: u32, head_cached: u32, value: *const T,
    ) -> Result<bool, u64> {
        let index = (tail & (self.one_lap - 1)) as usize;
        let new_tail = if index + 1 < self.buffer.len() {
            tail + 1
        } else {
            let lap = tail & !(self.one_lap - 1);
            lap.wrapping_add(self.one_lap)
        };
        let new_sender_val = ((head_cached as u64) << 32) | (new_tail as u64);
        match self.sender.compare_exchange_weak(
            sender_val,
            new_sender_val,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                debug_assert!(index < self.buffer.len());
                unsafe {
                    let slot = self.buffer.get_unchecked(index);

                    let item: &mut MaybeUninit<T> = &mut *slot.value.get();
                    item.write(ptr::read(value));
                    slot.stamp.store((tail as usize).wrapping_add(1), Ordering::Release);
                }
                Ok(true)
            }
            Err(current) => Err(current),
        }
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        let backoff = Backoff::new();
        let mut sender_val = self.sender.load(Ordering::Relaxed);
        loop {
            let tail = sender_val as u32;
            let mut head_cached = (sender_val >> 32) as u32;

            if head_cached.wrapping_add(self.one_lap) == tail {
                backoff.spin();
                let head = self.recv.load(Ordering::SeqCst) as u32;
                if head == head_cached {
                    return false;
                }
                head_cached = head;
            }
            match self._try_push(sender_val, tail, head_cached, value) {
                Ok(res) => return res,
                Err(current) => {
                    sender_val = current;
                    backoff.snooze();
                }
            }
        }
    }

    /// This function is optimise for channel suspected to be full,
    /// It's an equal replacement to is_full(), if not try only oneshot,
    /// return Ok(true) when push ok, Ok(false) when channel is full.
    /// None when uncertain (normally needs a loop)
    #[inline(always)]
    pub unsafe fn try_push_oneshot(&self, value: *const T) -> Option<bool> {
        let sender_val = self.sender.load(Ordering::SeqCst);
        let tail = sender_val as u32;
        let mut head_cached = (sender_val >> 32) as u32;
        if head_cached.wrapping_add(self.one_lap) == tail {
            let head = self.recv.load(Ordering::SeqCst) as u32;
            if head == head_cached {
                return Some(false);
            }
            head_cached = head;
        }
        self._try_push(sender_val, tail, head_cached, value).ok()
    }

    /// youpipe fork extension (batch send): claim a run of FREE slots with
    /// ONE `sender` CAS, then write the values. Non-blocking prefix
    /// semantics: sends `values[..n]` where `n` is bounded by both the
    /// batch length and the free run from the current tail (0 = nothing
    /// sendable right now — full, or CAS lost after bounded retries).
    /// Backpressure granularity is unchanged from per-item push: callers
    /// fall back to per-item blocking pushes for the remainder.
    ///
    /// Freeness is derived from the cached head (refreshed once from `recv`
    /// when it reports full), never from per-slot stamps: every position in
    /// `[tail, head_cached + one_lap)` was popped in the previous lap, so
    /// claiming it before writing is the same claimed-but-unpublished
    /// window a per-item push opens between its CAS and its stamp store.
    /// A torn snapshot (stale tail + fresh head, torn-snapshot note in
    /// `real_positions`) can only OVERCOUNT the free run, and the claim
    /// CAS then fails on the stale tail bits — overclaiming never lands.
    /// Concurrent producers claim disjoint runs (each CAS moves `tail`
    /// past the previous claim); the consumer's `_read` spin on a claimed
    /// slot stays bounded-wait because the claimer always completes its
    /// writes, and `pop_batch`'s stamp walk stops at unpublished slots.
    ///
    /// # Safety
    ///
    /// `values` must be fully initialized; on return the first `n` entries
    /// have been moved out of `values` (the caller must not read them
    /// again); entries from `n` on remain owned by the caller.
    #[inline]
    pub unsafe fn try_push_batch(&self, values: &mut [MaybeUninit<T>]) -> usize {
        let mut sender_val = self.sender.load(Ordering::Relaxed);
        let backoff = Backoff::new();
        // Strong CAS + bounded retries (the MPMC twin's rationale): a
        // 0-return must mean "contended/full, fall back to per-item",
        // never "spurious weak-CAS failure ate all retries" — Miri
        // simulates those.
        for _attempt in 0..4 {
            let tail = sender_val as u32;
            let mut head_cached = (sender_val >> 32) as u32;
            if head_cached.wrapping_add(self.one_lap) == tail {
                // Full per the cached head: refresh once, same as
                // `push_with_ptr`.
                let head = self.recv.load(Ordering::SeqCst) as u32;
                if head == head_cached {
                    return 0;
                }
                head_cached = head;
            }
            let free = self.real_positions(tail, head_cached.wrapping_add(self.one_lap));
            let n = values.len().min(free as usize);
            debug_assert!(n > 0);
            let mut new_tail = tail;
            for _ in 0..n {
                new_tail = self.advance(new_tail);
            }
            let new_sender_val = ((head_cached as u64) << 32) | (new_tail as u64);
            match self.sender.compare_exchange(
                sender_val,
                new_sender_val,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let mut pos = tail;
                    for i in 0..n {
                        let index = (pos & (self.one_lap - 1)) as usize;
                        debug_assert!(index < self.buffer.len());
                        let slot = self.buffer.get_unchecked(index);
                        let item: &mut MaybeUninit<T> = &mut *slot.value.get();
                        item.write(ptr::read(values[i].as_ptr()));
                        // Publication order of `_try_push`: value first,
                        // then the Release stamp.
                        slot.stamp.store((pos as usize).wrapping_add(1), Ordering::Release);
                        pos = self.advance(pos);
                    }
                    return n;
                },
                Err(current) => {
                    sender_val = current;
                    backoff.spin();
                },
            }
        }
        0
    }

    /// One position step from `pos` (`_try_push` / `pop_batch`'s lap wrap:
    /// the last buffer slot skips to the next lap base — positions
    /// `buffer.len()..one_lap` have no slots).
    #[inline]
    fn advance(&self, pos: u32) -> u32 {
        let index = pos & (self.one_lap - 1);
        if (index as usize) + 1 < self.buffer.len() {
            pos.wrapping_add(1)
        } else {
            (pos & !(self.one_lap - 1)).wrapping_add(self.one_lap)
        }
    }

    /// Count the slot-backed positions in the half-open range `[from, to)`
    /// (`to - from <= one_lap`): positions whose lap index falls in
    /// `buffer.len()..one_lap` have no slot, so a raw distance OVERCOUNTS
    /// the free run exactly there (found by `batch_push_sends_free_prefix_
    /// across_laps`: cap 3 / lap 4 claimed 4 slots and double-wrote slot 0).
    fn real_positions(&self, from: u32, to: u32) -> u32 {
        let mask = self.one_lap - 1;
        let cap = self.buffer.len() as u32;
        // NOTE: `from` (tail) and `to` (refreshed head + lap) come from
        // different moments — a producer descheduled between its sender
        // load and the head refresh can see `to - from > one_lap` when
        // other producers advanced the tail meanwhile. The overcounted
        // free run from such a torn snapshot is rejected by the caller's
        // CAS (the stale tail bits no longer match), so no span assertion
        // here (a debug_assert on the span fired spuriously in
        // batch_push_multi_producer on exactly this interleaving).
        if to == from {
            return 0;
        }
        // Real count of an index range [a, b) with 0 <= a < b <= one_lap.
        let seg_real = |a: u32, b: u32| b.min(cap).saturating_sub(a.min(cap));
        let idx_from = from & mask;
        let idx_to = to & mask;
        if to.wrapping_sub(from) == self.one_lap || idx_to <= idx_from {
            // The range crosses a lap base: [idx_from, one_lap) + [0, idx_to).
            seg_real(idx_from, self.one_lap) + seg_real(0, idx_to)
        } else {
            seg_real(idx_from, idx_to)
        }
    }

    #[inline]
    pub fn start_read(&self, final_check: bool) -> Option<Token> {
        if let Some((head, tail_cached)) = self._start_read::<true>(final_check) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            Some(Token::new(slot as *const Slot<T> as *const u8, packed_recv as usize))
        } else {
            None
        }
    }

    #[inline]
    pub fn pop(&self, final_check: bool) -> Option<T> {
        if let Some((head, tail_cached)) = self._start_read::<true>(final_check) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            let msg = unsafe { slot.value.get().read().assume_init() };
            // Update recv (which contains head) to free the slot.
            self.recv.store(packed_recv, Ordering::SeqCst);
            Some(msg)
        } else {
            None
        }
    }

    /// youpipe fork extension (batch recv): claim up to `out.len()`
    /// consecutively ready items, publishing the new head with ONE `recv`
    /// SeqCst store for the whole batch.
    ///
    /// Non-blocking: returns the ready prefix (0 = nothing ready right now;
    /// NOT an emptiness/closedness verdict — the caller distinguishes via
    /// `pop`). Per item this pays one stamp Acquire load plus the value
    /// move; per batch one `recv` load and, only when the cached tail says
    /// empty, one `sender` load. The stamp walk needs no tail bound: a slot
    /// beyond the producers' tail simply carries a stamp from an older lap
    /// and terminates the run. A slot whose producer has claimed (tail
    /// advanced) but not yet stored the value/stamp also terminates the run
    /// — `pop`'s `_read` spin keeps the bounded-wait contract for the
    /// blocking paths.
    #[inline]
    pub fn pop_batch(&self, out: &mut [MaybeUninit<T>]) -> usize {
        let recv_val = self.recv.load(Ordering::Relaxed);
        let mut head = recv_val as u32;
        let mut tail_cached = (recv_val >> 32) as u32;
        if tail_cached == head {
            // Cached tail is stale: take a fresh snapshot before declaring
            // nothing ready (SeqCst would only matter for the park-path
            // final check, which batch callers never perform).
            tail_cached = self.sender.load(Ordering::Acquire) as u32;
            if tail_cached == head {
                return 0;
            }
        }
        let mut n = 0;
        while n < out.len() {
            let index = (head & (self.one_lap - 1)) as usize;
            debug_assert!(index < self.buffer.len());
            let slot = unsafe { self.buffer.get_unchecked(index) };
            if slot.stamp.load(Ordering::Acquire) != (head as usize).wrapping_add(1) {
                break;
            }
            unsafe {
                out[n].write(slot.value.get().read().assume_init());
            }
            n += 1;
            head = if index + 1 < self.buffer.len() {
                head + 1
            } else {
                (head & !(self.one_lap - 1)).wrapping_add(self.one_lap)
            };
        }
        if n > 0 {
            // Hint invariant (`_start_read` relies on it): head <=
            // tail_cached <= real tail. The per-item path advances head one
            // slot per call, so it can never step PAST the hint (each step
            // re-runs the == emptiness check and refreshes on exact hit);
            // a batch JUMP can: the walk is stamp-bounded, not
            // tail-bounded, and may legitimately consume past a stale-low
            // hint. Storing that stale hint verbatim leaves
            // `tail_cached < head`, which `_start_read` misreads as "an
            // item exists" and `_read` then spins on a stamp from a dead
            // lap forever (reproduced 2026-09: 4 producers / batch drain,
            // recv={head=40000, tail_cached=16}, collector spinning at
            // 200% CPU). Clamp the stored hint up to the new head: hint ==
            // head reads as "maybe empty, refresh from sender" — exactly
            // the truth when the walk outran the hint — and the hint can
            // never exceed the real tail because the walk only covered
            // published slots.
            let hint = tail_cached.max(head);
            // Single cursor publication frees `n` slots for the producers
            // at once. SeqCst to match `pop` (the Miri-observed ordering).
            self.recv.store(((hint as u64) << 32) | (head as u64), Ordering::SeqCst);
        }
        n
    }

    #[inline]
    pub fn pop_cached(&self) -> Option<T> {
        if let Some((head, tail_cached)) = self._start_read::<false>(false) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            let msg = unsafe { slot.value.get().read().assume_init() };
            // Update recv (which contains head) to free the slot.
            self.recv.store(packed_recv, Ordering::SeqCst);
            Some(msg)
        } else {
            None
        }
    }

    /// return head, tail_cached
    #[inline]
    fn _start_read<const SPIN: bool>(&self, _final_check: bool) -> Option<(u32, u32)> {
        let recv_val = self.recv.load(Ordering::Relaxed);
        let head = recv_val as u32;
        let mut tail_cached = (recv_val >> 32) as u32;

        if tail_cached == head {
            if SPIN {
                core::hint::spin_loop();
                let tail = if _final_check {
                    self.sender.load(Ordering::SeqCst) as u32
                } else {
                    self.sender.load(Ordering::Acquire) as u32
                };
                if head == tail {
                    return None;
                }
                tail_cached = tail;
            } else {
                return None;
            }
        }
        Some((head, tail_cached))
    }

    #[inline]
    fn _read(&self, head: u32, tail_cached: u32) -> (&Slot<T>, u64) {
        // Deconstruct the head.
        let index = (head & (self.one_lap - 1)) as usize;
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        // Wait for stamp update
        let target_stamp = (head as usize).wrapping_add(1);
        loop {
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == target_stamp {
                break;
            }
            core::hint::spin_loop();
        }
        // Update head
        let new_head = if index + 1 < self.buffer.len() {
            head + 1
        } else {
            let lap = head & !(self.one_lap - 1);
            lap.wrapping_add(self.one_lap)
        };
        (slot, ((tail_cached as u64) << 32) | (new_head as u64))
    }

    #[inline(always)]
    pub fn read(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        let msg = unsafe { slot.value.get().read().assume_init() };
        // Do not update stamp
        self.recv.store(token.stamp as u64, Ordering::SeqCst);
        msg
    }

    /// Returns the capacity of the queue.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the queue is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head = self.recv.load(Ordering::SeqCst) as u32;
        let tail = self.sender.load(Ordering::SeqCst) as u32;
        tail == head
    }

    /// Returns `true` if the queue is full.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let tail = self.sender.load(Ordering::SeqCst) as u32;
        let head = self.recv.load(Ordering::SeqCst) as u32;
        head.wrapping_add(self.one_lap) == tail
    }

    /// Returns the number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        loop {
            let tail = self.sender.load(Ordering::SeqCst) as u32;
            let head = self.recv.load(Ordering::SeqCst) as u32;

            if self.sender.load(Ordering::SeqCst) as u32 == tail {
                let hix = head & (self.one_lap - 1);
                let tix = tail & (self.one_lap - 1);

                return if hix < tix {
                    (tix - hix) as usize
                } else if hix > tix {
                    self.capacity() - (hix - tix) as usize
                } else if tail == head {
                    0
                } else {
                    self.capacity()
                };
            }
        }
    }
}

impl<T> Drop for ArrayQueueMpsc<T> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            let recv_val = *self.recv.get_mut();
            let sender_val = *self.sender.get_mut();

            let head = recv_val as u32;
            let tail = sender_val as u32;

            let hix = head & (self.one_lap - 1);
            let tix = tail & (self.one_lap - 1);

            let len = if hix < tix {
                tix - hix
            } else if hix > tix {
                self.capacity() as u32 - hix + tix
            } else if tail == head {
                0
            } else {
                self.capacity() as u32
            };

            for i in 0..(len as usize) {
                let index = if (hix as usize) + i < self.capacity() {
                    (hix as usize) + i
                } else {
                    (hix as usize) + i - self.capacity()
                };

                unsafe {
                    debug_assert!(index < self.buffer.len());
                    let slot = self.buffer.get_unchecked_mut(index);
                    (*slot.value.get()).assume_init_drop();
                }
            }
        }
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    /// Drain via batches of at most `max`; returns when no batch makes
    /// progress. Mirrors the intended production pattern: a typed Vec plus
    /// `spare_capacity_mut()` as the uninit batch buffer.
    fn collect_batch(q: &ArrayQueueMpsc<u32>, max: usize) -> Vec<u32> {
        let mut got = Vec::new();
        let mut buf: Vec<u32> = Vec::with_capacity(max);
        loop {
            let n = q.pop_batch(buf.spare_capacity_mut());
            if n == 0 {
                return got;
            }
            // SAFETY: pop_batch initialized exactly buf[..n].
            unsafe { buf.set_len(n) };
            got.append(&mut buf);
        }
    }

    #[test]
    fn batch_claims_fifo_prefix_across_laps() {
        let q = ArrayQueueMpsc::new(3); // lap wrap exercised over 21 items
        let mut pushed = 0u32;
        for _chunk in 0..7 {
            for _ in 0..3 {
                let v = MaybeUninit::new(pushed);
                assert!(unsafe { q.push_with_ptr(v.as_ptr()) });
                pushed += 1;
            }
            // single-batch contract: a claim never exceeds the buffer
            let mut buf: Vec<u32> = Vec::with_capacity(2);
            let n = q.pop_batch(buf.spare_capacity_mut());
            assert_eq!(n, 2, "claim is capped by the buffer, not the run");
            // SAFETY: initialized by the claim above.
            unsafe { buf.set_len(n) };
            assert_eq!(&buf[..], &[pushed - 3, pushed - 2]);
            buf.clear();
            // rest of the run, then a 0-claim on the drained ring
            assert_eq!(collect_batch(&q, 8), vec![pushed - 1]);
        }
        assert_eq!(collect_batch(&q, 4), vec![]);
        // still usable after empty batches
        let v = MaybeUninit::new(99u32);
        assert!(unsafe { q.push_with_ptr(v.as_ptr()) });
        assert_eq!(collect_batch(&q, 8), vec![99]);
    }

    #[test]
    fn batch_partial_when_buffer_smaller_than_ready_run() {
        let q = ArrayQueueMpsc::new(8);
        for i in 0..8u32 {
            let v = MaybeUninit::new(i);
            assert!(unsafe { q.push_with_ptr(v.as_ptr()) });
        }
        // one claim with a 4-slot buffer stops at 4 even though 8 are ready
        let mut buf: Vec<u32> = Vec::with_capacity(4);
        let n = q.pop_batch(buf.spare_capacity_mut());
        assert_eq!(n, 4);
        // SAFETY: initialized by the claim above.
        unsafe { buf.set_len(n) };
        assert_eq!(&buf[..], (0..4).collect::<Vec<_>>().as_slice());
        buf.clear();
        assert_eq!(collect_batch(&q, 8), (4..8).collect::<Vec<_>>());
        assert_eq!(collect_batch(&q, 8), vec![]);
    }

    #[test]
    fn batch_multi_producer_no_loss_no_dup() {
        const P: usize = 4;
        // Miri runs ~100x slower; the interleaving surface (multi-producer
        // vs batch claim) is size-independent.
        const N: u32 = if cfg!(miri) { 100 } else { 2000 };
        let q = std::sync::Arc::new(ArrayQueueMpsc::<u32>::new(7));
        let mut hs = Vec::new();
        for p in 0..P {
            let q = std::sync::Arc::clone(&q);
            let base = u32::try_from(p).unwrap() * N;
            hs.push(std::thread::spawn(move || {
                for i in 0..N {
                    let v = MaybeUninit::new(base + i);
                    while !unsafe { q.push_with_ptr(v.as_ptr()) } {
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        let mut got: Vec<u32> = Vec::new();
        let mut buf: Vec<u32> = Vec::with_capacity(5);
        let mut idle = 0usize;
        while got.len() < P * N as usize {
            let n = q.pop_batch(buf.spare_capacity_mut());
            if n == 0 {
                idle += 1;
                assert!(idle < 1_000_000, "consumer stalled");
                continue;
            }
            idle = 0;
            // SAFETY: pop_batch initialized exactly buf[..n].
            unsafe { buf.set_len(n) };
            got.append(&mut buf);
        }
        for h in hs {
            h.join().unwrap();
        }
        got.sort_unstable();
        assert_eq!(got, (0..u32::try_from(P).unwrap() * N).collect::<Vec<_>>());
    }

    /// Stage `k` fresh values starting at `next` as an uninit batch.
    fn stage(k: usize, next: u32) -> Vec<MaybeUninit<u32>> {
        (next..next + k as u32).map(MaybeUninit::new).collect()
    }

    /// Batch-push the free prefix, drain interleaved, across laps — the
    /// send-side twin of `batch_claims_fifo_prefix_across_laps`: the claim
    /// must respect the buffer-end lap wrap (cap 3 has no power-of-two lap)
    /// and never exceed the free run.
    #[test]
    fn batch_push_sends_free_prefix_across_laps() {
        let q = ArrayQueueMpsc::new(3);
        let mut next = 0u32;
        // SAFETY for every call: the batch is fully initialized and the
        // sent prefix is never read again.
        let mut send = |q: &ArrayQueueMpsc<u32>, k: usize| -> usize {
            let n = unsafe { q.try_push_batch(&mut stage(k, next)) };
            next += n as u32;
            n
        };
        // first claim: whole batch fits (3 free)
        assert_eq!(send(&q, 4), 3, "claim is capped by the free run, not the batch");
        // full now: 0 until the consumer frees slots
        assert_eq!(send(&q, 2), 0);
        assert_eq!(collect_batch(&q, 8), vec![0, 1, 2]);
        // interleaved rounds exercising the lap wrap on both sides
        for r in 1..7u32 {
            assert_eq!(send(&q, 2), 2, "ring drained, both fit");
            assert_eq!(send(&q, 2), 1, "one slot left");
            assert_eq!(send(&q, 2), 0, "full");
            assert_eq!(collect_batch(&q, 8), vec![3 * r, 3 * r + 1, 3 * r + 2]);
        }
        assert_eq!(collect_batch(&q, 8), vec![]);
    }

    /// Mixed per-item pushes, batch pushes and batch pops over a lap-skip
    /// capacity: FIFO order and the stamp protocol must survive every
    /// interleaving.
    #[test]
    fn batch_push_mixed_with_per_item_across_laps() {
        let q = ArrayQueueMpsc::new(5);
        let mut expect = Vec::new();
        for round in 0..200u32 {
            let base = round * 6;
            // item per-push (drain-until-pushable: a prior round may leave
            // the ring full — there is no end-of-round drain)
            while !unsafe { q.push_with_ptr(MaybeUninit::new(base).as_ptr()) } {
                let got = collect_batch(&q, 8);
                let k = got.len();
                assert_eq!(got, expect.drain(..k).collect::<Vec<_>>());
            }
            expect.push(base);
            // batch push the rest; the unsent remainder re-pushes per-item
            // (the production callers' fallback rhythm), draining only as
            // blocked
            let mut uninit = stage(5, base + 1);
            // SAFETY: fully initialized; the sent prefix moves out.
            let sent = unsafe { q.try_push_batch(&mut uninit) };
            expect.extend((base + 1)..(base + 1 + sent as u32));
            // SAFETY: [sent..] was never moved out.
            let rest: Vec<u32> = unsafe {
                uninit[sent..].iter_mut().map(|s| s.assume_init_read()).collect()
            };
            for (j, v) in rest.into_iter().enumerate() {
                while !unsafe { q.push_with_ptr(MaybeUninit::new(v).as_ptr()) } {
                    // drain what is available; the drained prefix must
                    // match the expected FIFO prefix
                    let got = collect_batch(&q, 8);
                    let k = got.len();
                    assert_eq!(got, expect.drain(..k).collect::<Vec<_>>());
                    let _ = j;
                }
                expect.push(v);
            }
        }
        let got = collect_batch(&q, 64);
        assert_eq!(got.len(), expect.len());
        assert!(got == expect, "FIFO order must survive mixed interleavings");
    }

    /// Multi-producer batch claims must partition the stream with no loss
    /// and no duplication even when the ring is contended.
    #[test]
    fn batch_push_multi_producer_no_loss_no_dup() {
        const P: usize = 4;
        const N: u32 = if cfg!(miri) { 100 } else { 2000 };
        let q = std::sync::Arc::new(ArrayQueueMpsc::<u32>::new(7));
        let mut hs = Vec::new();
        for p in 0..P {
            let q = std::sync::Arc::clone(&q);
            let base = u32::try_from(p).unwrap() * N;
            hs.push(std::thread::spawn(move || {
                let mut i = 0u32;
                let mut idle = 0usize;
                while i < N {
                    let k = (N - i).min(5) as usize;
                    let batch: Vec<u32> = (base + i..base + i + k as u32).collect();
                    let mut uninit: Vec<MaybeUninit<u32>> =
                        batch.into_iter().map(MaybeUninit::new).collect();
                    // SAFETY: fully initialized; the sent prefix moves out.
                    let n = unsafe { q.try_push_batch(&mut uninit) };
                    if n == 0 {
                        idle += 1;
                        assert!(idle < 1_000_000, "producer stalled");
                        continue;
                    }
                    idle = 0;
                    i += n as u32;
                    // the unsent remainder drops; values are re-derived
                    // from `i` on the next attempt, so nothing is lost
                }
            }));
        }
        let mut got: Vec<u32> = Vec::new();
        let mut buf: Vec<u32> = Vec::with_capacity(5);
        let mut idle = 0usize;
        while got.len() < P * N as usize {
            let n = q.pop_batch(buf.spare_capacity_mut());
            if n == 0 {
                idle += 1;
                assert!(idle < 1_000_000, "consumer stalled");
                continue;
            }
            idle = 0;
            // SAFETY: pop_batch initialized exactly buf[..n].
            unsafe { buf.set_len(n) };
            got.append(&mut buf);
        }
        for h in hs {
            h.join().unwrap();
        }
        got.sort_unstable();
        assert_eq!(got, (0..u32::try_from(P).unwrap() * N).collect::<Vec<_>>());
    }
}
