//! Modify by frostyplanet@gmail.com for the crossfire crate:
//!
//!   - Optimise for single consumer scenario;
//!   - Add token interface according to crossbeam-channel
//!   - Modified push() to push_with_ptr();
//!   - Add try_push_oneshot() which combinds the logic of push and check_full in one step;
//!   - Remove unused functions.
//!   - youpipe fork extension (2026-09): pop_batch() / try_push_batch() —
//!     claim a run of slots with ONE cursor CAS instead of one per item
//!     (see docs/src/dev/streaming.md "Batched ring operations" in the
//!     youpipe repo).
//!   - youpipe fork extension (2026-11): the atomics come from
//!     `loom::sync::atomic` under the `loom` feature, so `mod loom_tests`
//!     model-checks the batch claim protocol.
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
#[cfg(not(feature = "loom"))]
use core::sync::atomic::{self, AtomicUsize, Ordering};
use crossbeam_utils::{Backoff, CachePadded};
#[cfg(feature = "loom")]
// Modeled atomics; the slot payloads stay in core's UnsafeCell.
use loom::sync::atomic::{self, AtomicUsize, Ordering};

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    ///
    /// If the stamp equals the tail, this node will be next written to. If it equals head + 1,
    /// this node will be next read from.
    stamp: AtomicUsize,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded multi-producer multi-consumer queue.
///
/// This queue allocates a fixed-capacity buffer on construction, which is used to store pushed
/// elements. The queue cannot hold more elements than the buffer allows. Attempting to push an
/// element into a full queue will fail. Alternatively, [`force_push`] makes it possible for
/// this queue to be used as a ring-buffer. Having a buffer allocated upfront makes this queue
/// a bit faster than [`SegQueue`].
///
/// [`SegQueue`]: super::SegQueue
pub struct ArrayQueue<T, const MP: bool, const MC: bool> {
    /// The head of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are popped from the head of the queue.
    head: CachePadded<AtomicUsize>,

    /// The tail of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are pushed into the tail of the queue.
    tail: CachePadded<AtomicUsize>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: usize,
}

unsafe impl<T, const MP: bool, const MC: bool> Sync for ArrayQueue<T, MP, MC> {}
unsafe impl<T, const MP: bool, const MC: bool> Send for ArrayQueue<T, MP, MC> {}

impl<T, const MP: bool, const MC: bool> UnwindSafe for ArrayQueue<T, MP, MC> {}
impl<T, const MP: bool, const MC: bool> RefUnwindSafe for ArrayQueue<T, MP, MC> {}

impl<T, const MP: bool, const MC: bool> ArrayQueue<T, MP, MC> {
    /// Creates a new bounded queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity is zero.
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "capacity must be non-zero");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized
        // with stamps.
        let buffer: Box<[Slot<T>]> = (0..cap)
            .map(|i| {
                // Set the stamp to `{ lap: 0, index: i }`.
                Slot { stamp: AtomicUsize::new(i), value: UnsafeCell::new(MaybeUninit::uninit()) }
            })
            .collect();

        // One lap is the smallest power of two greater than `cap`.
        let one_lap = (cap + 1).next_power_of_two();

        Self {
            buffer,
            one_lap,
            head: CachePadded::new(AtomicUsize::new(head)),
            tail: CachePadded::new(AtomicUsize::new(tail)),
        }
    }

    /// This function is optimise for channel suspected to be full,
    /// It's an equal replacement to is_full(), if not try only oneshot,
    /// return Ok(true) when push ok, Ok(false) when channel is full.
    /// None when uncertain (normally needs a loop)
    #[allow(dead_code)]
    #[inline(always)]
    pub unsafe fn try_push_oneshot(&self, value: *const T) -> Option<bool> {
        // Use two SeqCst to compare tail & head, it's an equal replacement to is_full()
        let tail = self.tail.load(Ordering::SeqCst);
        macro_rules! check_full {
            ($tail: expr) => {
                let head = self.head.load(Ordering::SeqCst);
                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == $tail {
                    // ...then the queue is full.
                    return Some(false);
                }
            };
        }
        check_full!(tail);
        match self._try_push(tail, value) {
            Ok(_) => Some(true),
            Err((_stamp, _new_tail)) => {
                // after the first check_full with both loads are SeqCst, this is unlikely full, but also a hot path
                None
            }
        }
    }

    /// return stamp, new_tail
    #[inline]
    fn _try_push(&self, tail: usize, value: *const T) -> Result<bool, (usize, Option<usize>)> {
        let cap = self.capacity();
        // Deconstruct the tail.
        let index = tail & (self.one_lap - 1);

        // Inspect the corresponding slot.
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        let stamp = slot.stamp.load(Ordering::Acquire);

        // If the tail and the stamp match, we may attempt to push.
        if tail == stamp {
            let new_tail = if index + 1 < cap {
                // Same lap, incremented index.
                // Set to `{ lap: lap, index: index + 1 }`.
                tail + 1
            } else {
                let lap = tail & !(self.one_lap - 1);
                // One lap forward, index wraps around to zero.
                // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                lap.wrapping_add(self.one_lap)
            };
            if MP {
                // Try moving the tail.
                if let Err(t) = self.tail.compare_exchange_weak(
                    tail,
                    new_tail,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    return Err((stamp, Some(t)));
                }
            } else {
                self.tail.store(new_tail, Ordering::SeqCst);
            }
            // Write the value into the slot and update the stamp.
            unsafe {
                let item: &mut MaybeUninit<T> = &mut *slot.value.get();
                item.write(ptr::read(value));
            }
            slot.stamp.store(tail + 1, Ordering::Release);
            Ok(true)
        } else {
            Err((stamp, None))
        }
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        let backoff = Backoff::new();
        let mut tail =
            if MP { self.tail.load(Ordering::Relaxed) } else { self.tail.load(Ordering::Acquire) };
        macro_rules! check_full {
            ($tail: expr) => {
                let head = if MP || MC {
                    // NOTE: The fence is preventing livestock
                    atomic::fence(Ordering::SeqCst);
                    self.head.load(Ordering::Relaxed)
                } else {
                    self.head.load(Ordering::SeqCst)
                };
                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == $tail {
                    // ...then the queue is full.
                    return false;
                }
            };
        }
        loop {
            match self._try_push(tail, value) {
                Ok(res) => return res,
                Err((stamp, new_tail)) => {
                    if let Some(_tail) = new_tail {
                        tail = _tail;
                        backoff.spin();
                        continue;
                    }
                    if stamp.wrapping_add(self.one_lap) == tail + 1 {
                        check_full!(tail);
                    }
                    backoff.snooze();
                    if MP {
                        tail = self.tail.load(Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// youpipe fork extension (batch recv): CAS the head ONCE for a run of
    /// consecutively ready slots instead of once per item.
    ///
    /// The stamp walk is read-only and reuses `_start_read`'s per-item
    /// readiness rule (`stamp == position + 1`); a single
    /// `compare_exchange` on the walked-from head then makes the whole
    /// claim atomic: any interfering consumer moves the head and fails the
    /// CAS, and a producer cannot touch a slot whose stamp equals
    /// `head + 1` (its claim stamp is a full lap away), so the checked
    /// values are stable until popped. Non-blocking: returns the ready
    /// prefix (0 = nothing claimable right now — empty, first slot
    /// in-flight, or CAS lost after bounded retries; the caller
    /// distinguishes via `pop`).
    #[inline]
    pub fn pop_batch(&self, out: &mut [MaybeUninit<T>]) -> usize {
        let order = if MC { Ordering::Relaxed } else { Ordering::Acquire };
        let mut head = self.head.load(order);
        let backoff = Backoff::new();
        // Bounded retries keep the call non-blocking under consumer
        // contention; the per-item `pop` path keeps the unbounded loop for
        // the blocking flavors.
        for _attempt in 0..4 {
            let mut n = 0;
            let mut pos = head;
            while n < out.len() {
                let index = pos & (self.one_lap - 1);
                debug_assert!(index < self.buffer.len());
                let slot = unsafe { self.buffer.get_unchecked(index) };
                if slot.stamp.load(Ordering::Acquire) != pos.wrapping_add(1) {
                    break;
                }
                pos = Self::advance(pos, index, self.one_lap, self.capacity());
                n += 1;
            }
            if n == 0 {
                return 0;
            }
            match self.head.compare_exchange(head, pos, Ordering::SeqCst, Ordering::Relaxed) {
                Ok(_) => {
                    let mut p = head;
                    for slot_out in out.iter_mut().take(n) {
                        let index = p & (self.one_lap - 1);
                        let slot = unsafe { self.buffer.get_unchecked(index) };
                        unsafe {
                            slot_out.write(slot.value.get().read().assume_init());
                        }
                        // Free the slot for the next lap (same Release the
                        // per-item `pop` performs).
                        slot.stamp.store(p.wrapping_add(self.one_lap), Ordering::Release);
                        p = Self::advance(p, index, self.one_lap, self.capacity());
                    }
                    return n;
                }
                Err(h) => {
                    head = h;
                    backoff.spin();
                }
            }
        }
        0
    }

    /// youpipe fork extension (batch send): claim a run of consecutively
    /// FREE slots with ONE tail CAS, then write the values. Non-blocking
    /// prefix semantics: sends `values[..n]` where `n` is the longest free
    /// run found (0 = nothing sendable from this snapshot — full, a stale
    /// `Relaxed` tail read whose walk lands on a slot a peer has already
    /// published, or CAS lost after bounded retries). Backpressure
    /// granularity is unchanged from per-item send: callers fall back to
    /// per-item blocking sends for the remainder, which re-reads the tail
    /// and is what makes the stale-snapshot 0 harmless.
    ///
    /// # Safety
    ///
    /// `values` must be fully initialized; on return the first `n` entries
    /// have been moved out of `values` (the caller must not read them
    /// again); entries from `n` on remain owned by the caller.
    #[inline]
    pub unsafe fn try_push_batch(&self, values: &mut [MaybeUninit<T>]) -> usize {
        let mut tail =
            if MP { self.tail.load(Ordering::Relaxed) } else { self.tail.load(Ordering::Acquire) };
        let backoff = Backoff::new();
        for _attempt in 0..4 {
            let mut n = 0;
            let mut pos = tail;
            while n < values.len() {
                let index = pos & (self.one_lap - 1);
                debug_assert!(index < self.buffer.len());
                let slot = unsafe { self.buffer.get_unchecked(index) };
                if slot.stamp.load(Ordering::Acquire) != pos {
                    break;
                }
                pos = Self::advance(pos, index, self.one_lap, self.capacity());
                n += 1;
            }
            if n == 0 {
                return 0;
            }
            if MP {
                // Strong CAS on purpose (unlike `_try_push`'s weak loop):
                // a 0-return must mean "contended/full, fall back to
                // per-item", never "spurious weak-CAS failure ate all
                // bounded retries" — Miri simulates those (it ate the
                // bounded retries in batch_send_claims_only_free_prefix).
                match self.tail.compare_exchange(tail, pos, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(_) => {
                        self.write_claimed(tail, values, n);
                        return n;
                    }
                    Err(t) => {
                        tail = t;
                        backoff.spin();
                    }
                }
            } else {
                self.tail.store(pos, Ordering::SeqCst);
                self.write_claimed(tail, values, n);
                return n;
            }
        }
        0
    }

    /// Write the claimed run `values[..n]` starting at ring position `pos`
    /// (value first, then the Release stamp — the per-item `_try_push`
    /// publication order).
    #[inline]
    unsafe fn write_claimed(&self, pos: usize, values: &mut [MaybeUninit<T>], n: usize) {
        let mut p = pos;
        for i in 0..n {
            let index = p & (self.one_lap - 1);
            let slot = self.buffer.get_unchecked(index);
            let item: &mut MaybeUninit<T> = &mut *slot.value.get();
            item.write(std::ptr::read(values[i].as_ptr()));
            slot.stamp.store(p.wrapping_add(1), Ordering::Release);
            p = Self::advance(p, index, self.one_lap, self.capacity());
        }
    }

    /// Lap-aware position advance shared by the batch walks (same wrap rule
    /// as `_try_push`/`_start_read`).
    #[inline(always)]
    fn advance(pos: usize, index: usize, one_lap: usize, cap: usize) -> usize {
        if index + 1 < cap {
            pos + 1
        } else {
            (pos & !(one_lap - 1)).wrapping_add(one_lap)
        }
    }

    #[inline]
    pub fn start_read(&self, final_check: bool) -> Option<Token> {
        if let Some((slot, stamp)) = self._start_read(final_check) {
            Some(Token::new(slot as *const Slot<T> as *const u8, stamp))
        } else {
            None
        }
    }

    #[inline]
    pub fn pop(&self, final_check: bool) -> Option<T> {
        if let Some((slot, stamp)) = self._start_read(final_check) {
            let msg = unsafe { slot.value.get().read().assume_init() };
            slot.stamp.store(stamp, Ordering::Release);
            Some(msg)
        } else {
            None
        }
    }

    #[inline]
    fn _start_read(&self, final_check: bool) -> Option<(&Slot<T>, usize)> {
        let mut head;
        if final_check {
            // because we need to check is_empty before park,
            // use SeqCst to make Miri happy
            head = self.head.load(Ordering::SeqCst);
        } else {
            let order = if MC { Ordering::Relaxed } else { Ordering::Acquire };
            head = self.head.load(order);
        }
        let backoff = Backoff::new();
        loop {
            // Deconstruct the head.
            let index = head & (self.one_lap - 1);
            // Inspect the corresponding slot.
            debug_assert!(index < self.buffer.len());
            let slot = unsafe { self.buffer.get_unchecked(index) };
            let stamp = slot.stamp.load(Ordering::Acquire);

            // If the stamp is ahead of the head by 1, we may attempt to pop.
            if head + 1 == stamp {
                let new = if index + 1 < self.capacity() {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, index: index + 1 }`.
                    head + 1
                } else {
                    let lap = head & !(self.one_lap - 1);
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };
                if MC {
                    // Try moving the head.
                    if let Err(new_head) = self.head.compare_exchange_weak(
                        head,
                        new,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        head = new_head;
                        backoff.spin();
                        continue;
                    }
                } else {
                    self.head.store(new, Ordering::SeqCst);
                }
                let new_head = head.wrapping_add(self.one_lap);
                return Some((slot, new_head));
            } else {
                if stamp == head {
                    // Check full
                    let tail = if MP || MC {
                        // NOTE: The fence is preventing live lock
                        atomic::fence(Ordering::SeqCst);
                        self.tail.load(Ordering::Relaxed)
                    } else {
                        self.tail.load(Ordering::SeqCst)
                    };
                    // If the tail equals the head, that means the channel is empty.
                    if tail == head {
                        return None;
                    }
                    backoff.spin();
                } else {
                    // Snooze because we need to wait for the stamp to get updated.
                    backoff.snooze();
                }
                if MC {
                    head = self.head.load(Ordering::Relaxed);
                }
                continue;
            }
        }
    }

    #[inline(always)]
    pub fn read(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        let msg = unsafe { slot.value.get().read().assume_init() };
        slot.stamp.store(token.stamp, Ordering::Release);
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
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);

        // Is the tail lagging one lap behind head?
        // Is the tail equal to the head?
        //
        // Note: If the head changes just before we load the tail, that means there was a moment
        // when the channel was not empty, so it is safe to just return `false`.
        tail == head
    }

    /// Returns `true` if the queue is full.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::SeqCst);
        let head = self.head.load(Ordering::SeqCst);

        // Is the head lagging one lap behind tail?
        //
        // Note: If the tail changes just before we load the head, that means there was a moment
        // when the queue was not full, so it is safe to just return `false`.
        head.wrapping_add(self.one_lap) == tail
    }

    /// youpipe fork extension (ring recycling): restore construction state
    /// (head = tail = 0, every slot stamp = its index) so endpoints can be
    /// rebuilt over the same allocation. Exclusive access is the caller's
    /// contract (`ChannelShared::try_reset` claims both endpoint refcounts
    /// first, so no push/pop/claim can be in flight; a claim abandoned by
    /// a panicked producer leaves head != tail and fails below). Returns
    /// false when the ring still holds items — leftover values must go
    /// through the Drop path, not be buried under fresh stamps.
    pub fn try_reset(&self) -> bool {
        if !self.is_empty() {
            return false;
        }
        for (i, slot) in self.buffer.iter().enumerate() {
            slot.stamp.store(i, Ordering::Release);
        }
        self.head.store(0, Ordering::Release);
        self.tail.store(0, Ordering::Release);
        true
    }

    /// Returns the number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        loop {
            // Load the tail, then load the head.
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);

            // If the tail didn't change, we've got consistent values to work with.
            if self.tail.load(Ordering::SeqCst) == tail {
                let hix = head & (self.one_lap - 1);
                let tix = tail & (self.one_lap - 1);

                return if hix < tix {
                    tix - hix
                } else if hix > tix {
                    self.capacity() - hix + tix
                } else if tail == head {
                    0
                } else {
                    self.capacity()
                };
            }
        }
    }
}

/// Reads a cursor through `&mut self`: the std atomics expose `get_mut`,
/// loom's modeled ones only a relaxed load.
#[cfg(not(feature = "loom"))]
#[inline]
fn final_cursor(cursor: &mut CachePadded<AtomicUsize>) -> usize {
    *cursor.get_mut()
}

#[cfg(feature = "loom")]
#[inline]
fn final_cursor(cursor: &mut CachePadded<AtomicUsize>) -> usize {
    cursor.load(Ordering::Relaxed)
}

impl<T, const MP: bool, const MC: bool> Drop for ArrayQueue<T, MP, MC> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // Get the index of the head.
            let head = final_cursor(&mut self.head);
            let tail = final_cursor(&mut self.tail);

            let hix = head & (self.one_lap - 1);
            let tix = tail & (self.one_lap - 1);

            let len = if hix < tix {
                tix - hix
            } else if hix > tix {
                self.capacity() - hix + tix
            } else if tail == head {
                0
            } else {
                self.capacity()
            };

            // Loop over all slots that hold a message and drop them.
            for i in 0..len {
                // Compute the index of the next slot holding a message.
                let index =
                    if hix + i < self.capacity() { hix + i } else { hix + i - self.capacity() };

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

    #[test]
    fn batch_push_pop_roundtrip_across_laps() {
        let q = ArrayQueue::<u32, true, true>::new(3);
        // 5 items per round through the 3-slot ring, 8 rounds: whenever a
        // send batch claims nothing (ring full mid-chunk), everything
        // committed so far is consumed first — the feeder's exact
        // batch-prefix + per-item-fallback shape, across many lap wraps.
        let mut next_in = 0u32;
        let mut next_out = 0u32;
        for _round in 0..8 {
            let mut batch: Vec<MaybeUninit<u32>> =
                (next_in..next_in + 5).map(MaybeUninit::new).collect();
            next_in += 5;
            let mut sent = 0usize;
            let mut got: Vec<u32> = Vec::new();
            let mut buf: Vec<u32> = Vec::with_capacity(4);
            while sent < 5 {
                // SAFETY: `batch` is fully initialized; the sent prefix is
                // moved out and only the unsent tail is re-sliced.
                let n = unsafe { q.try_push_batch(&mut batch[sent..]) };
                sent += n;
                // drain whatever is committed so far
                loop {
                    let m = q.pop_batch(buf.spare_capacity_mut());
                    if m == 0 {
                        match q.pop(false) {
                            Some(v) => got.push(v),
                            None => break,
                        }
                    } else {
                        // SAFETY: pop_batch initialized exactly buf[..m].
                        unsafe { buf.set_len(m) };
                        got.append(&mut buf);
                    }
                }
            }
            assert_eq!(got, (next_out..next_out + 5).collect::<Vec<_>>());
            next_out += 5;
        }
    }

    #[test]
    fn batch_send_claims_only_free_prefix() {
        let q = ArrayQueue::<u32, true, true>::new(4);
        let mut pre: Vec<MaybeUninit<u32>> = (0..3).map(MaybeUninit::new).collect();
        assert_eq!(unsafe { q.try_push_batch(&mut pre) }, 3);
        // 1 free slot: a 4-item batch must claim only the free prefix
        let mut batch: Vec<MaybeUninit<u32>> = (10..14).map(MaybeUninit::new).collect();
        assert_eq!(unsafe { q.try_push_batch(&mut batch) }, 1);
        // full now: 0 claimable; the unsent tail stays owned by the caller
        assert_eq!(unsafe { q.try_push_batch(&mut batch[1..]) }, 0);
        let mut got: Vec<u32> = Vec::new();
        let mut buf: Vec<u32> = Vec::with_capacity(4);
        loop {
            let n = q.pop_batch(buf.spare_capacity_mut());
            if n == 0 {
                break;
            }
            // SAFETY: pop_batch initialized exactly buf[..n].
            unsafe { buf.set_len(n) };
            got.append(&mut buf);
        }
        assert_eq!(got, vec![0, 1, 2, 10]);
    }

    #[test]
    fn batch_mpmc_no_loss_no_dup() {
        const P: usize = 4;
        const C: usize = 3;
        // Miri runs ~100x slower; the interleaving surface (contended batch
        // CAS claims) is size-independent.
        const N: u32 = if cfg!(miri) { 100 } else { 3000 };
        let q = std::sync::Arc::new(ArrayQueue::<u32, true, true>::new(8));
        let collected = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Liveness budget: every spin loop below checks the clock on its
        // idle path, so a lost-wakeup bug fails fast instead of burning a
        // core forever (a pre-budget stress shape once ran 9 min at 200%
        // CPU). Miri gets a generous wall clock — the budget is a safety
        // net, not a perf gate.
        let budget = std::time::Duration::from_secs(if cfg!(miri) { 300 } else { 30 });
        let mut hs = Vec::new();
        for p in 0..P {
            let q = std::sync::Arc::clone(&q);
            let base = u32::try_from(p).unwrap() * N;
            hs.push(std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + budget;
                for i in 0..N {
                    let mut v = [MaybeUninit::new(base + i)];
                    while unsafe { q.try_push_batch(&mut v) } == 0 {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "producer {p} stalled on a full ring"
                        );
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        let total = P * N as usize;
        let mut consumers = Vec::new();
        for c in 0..C {
            let q = std::sync::Arc::clone(&q);
            let collected = std::sync::Arc::clone(&collected);
            consumers.push(std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + budget;
                let mut got: Vec<u32> = Vec::new();
                let mut buf: Vec<u32> = Vec::with_capacity(6);
                while collected.load(std::sync::atomic::Ordering::Relaxed) < total {
                    let n = q.pop_batch(buf.spare_capacity_mut());
                    if n == 0 {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "consumer {c} stalled on a drained ring"
                        );
                        std::hint::spin_loop();
                        continue;
                    }
                    // SAFETY: pop_batch initialized exactly buf[..n].
                    unsafe { buf.set_len(n) };
                    got.append(&mut buf);
                    collected.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                }
                got
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let mut all = Vec::new();
        for c in consumers {
            all.append(&mut c.join().unwrap());
        }
        all.sort_unstable();
        // exactly-once: any duplicate or loss breaks both checks
        assert_eq!(all.len(), total);
        assert_eq!(all, (0..u32::try_from(P).unwrap() * N).collect::<Vec<_>>());
    }
}

// Model-checked batch claim tests. Run with:
//   cargo test -p youpipe-crossfire --features loom --lib --release -- loom_
//
// The seam above swaps only the atomics, so a slot's payload write is not a
// modeled access and a model can only switch threads on a modeled atomic.
// `write_claimed` has none between its two halves (`value` store, then the
// Release stamp), so the window those two halves order is not schedulable
// here: what the models assert is that every claim a consumer makes carries
// a tag its producer published and that each tag is claimed exactly once,
// which is what a claim taken through a published stamp implies. The
// consumer-visible side of slot reuse (a head CAS that lost, a 0 return, a
// re-claim of a run) is fully modeled.
//
// `pop_batch`'s "four CAS retries exhausted" exit needs four preemptions
// inside one call and is not schedulable at the suite's preemption budget
// (0 of them reached at LOOM_MAX_PREEMPTIONS=2 and 3, measured); both zero
// returns leave the head unstored, so a 0 return claiming nothing is pinned
// by the partial drain that follows the joins.
//
// The producer side (`try_push_batch` + `write_claimed`) has the same seam
// shape: one modeled tail CAS, then a `value` store and a Release stamp per
// slot with no modeled access in between, so the model cannot order a slot's
// two halves either. What it does pin is the pair of contracts the call
// makes: the claimed prefix is exactly what the consumers receive (no split,
// no duplicate, no phantom claim) and the values left in `values[n..]` drop
// exactly once each, the split `flush_staged`'s `assume_init_read` loop
// depends on.
//
// `try_push_batch`'s `0` returns, measured at the suite's budget over the
// three models below (6496 / 13142 / 10390 schedules explored): the
// four-retry-exhausted exit is never reached — with two producers committing
// one claim each, a producer can lose at most one tail CAS, so that exit is
// structurally unreachable. Two claim-free exits are reachable: the `n == 0`
// walk over a genuinely occupied prefix, and a `Relaxed` tail read stale by
// at least one peer claim — the walk then starts before the peer's run and
// breaks on its published stamp, so a ring with free slots reports 0 (in
// `loom_try_push_batch_claims_publish_their_prefix`, whose two batches fit
// the ring, every observed zero return is that stale one). Neither advances a
// cursor, which the consumed multiset and the quiescent `len()` pin; in both
// cases the caller's per-item fallback re-reads the tail.
//
// Falsification, temporary patches not committed: an unconditional
// `tail.store(pos)` in place of the MP claim CAS makes all three models report
// a lost and a duplicated tag, so the CAS is load-bearing under the model;
// publishing a run's stamps only after writing every value survives all
// three, which is exactly the window this seam cannot see.
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use std::sync::Arc;

    /// Never a published tag, so a buffer slot pop_batch wrote always
    /// differs from one it left alone.
    const SENTINEL: u32 = u32::MAX;

    /// Polls per consumer. A consumer that spins on a 0 return explodes the
    /// state space (every poll is a preemption point), so the remainder is
    /// drained single-threaded after the joins instead.
    const POLLS: usize = 3;

    /// One poll: asserts the prefix contract and returns the claimed tags.
    /// `n <= buf.len()`, `buf[..n]` holds queue-published tags, `buf[n..]` is
    /// untouched.
    fn poll_once(
        queue: &ArrayQueue<u32, true, true>, buf: &mut [MaybeUninit<u32>], total: u32,
    ) -> Vec<u32> {
        for slot in buf.iter_mut() {
            *slot = MaybeUninit::new(SENTINEL);
        }
        let n = queue.pop_batch(buf);
        assert!(n <= buf.len(), "claimed {n} slots into a {}-slot buffer", buf.len());
        let mut got = Vec::with_capacity(n);
        for slot in buf.iter().take(n) {
            // SAFETY: pop_batch initialized exactly buf[..n].
            let tag = *unsafe { slot.assume_init_ref() };
            assert!(tag < total, "read tag {tag} outside the published range");
            got.push(tag);
        }
        for slot in buf.iter().skip(n) {
            // SAFETY: the sentinel loop above initialized the whole buffer.
            assert_eq!(*unsafe { slot.assume_init_ref() }, SENTINEL, "pop_batch touched buf[n..]");
        }
        got
    }

    /// One consumer: `POLLS` polls with a yield between them so the producer
    /// can publish. A 0 return is legal and claims nothing; what proves
    /// nothing was stranded is the drain that follows the joins.
    fn consumer_polls(queue: &ArrayQueue<u32, true, true>, k: usize, total: u32) -> Vec<u32> {
        let mut buf = vec![MaybeUninit::new(SENTINEL); k];
        let mut got = Vec::new();
        for _ in 0..POLLS {
            got.extend(poll_once(queue, &mut buf, total));
            loom::thread::yield_now();
        }
        got
    }

    /// Drains what the consumers left behind. Runs after every thread joined,
    /// so the ring is quiescent and a 0 return means empty: an item stranded
    /// by a claimed-but-unpublished slot cannot be recovered here.
    fn drain_rest(queue: &ArrayQueue<u32, true, true>, total: u32) -> Vec<u32> {
        let mut buf = [MaybeUninit::new(SENTINEL); 4];
        let mut got = Vec::new();
        loop {
            let tags = poll_once(queue, &mut buf, total);
            if tags.is_empty() {
                return got;
            }
            got.extend(tags);
        }
    }

    fn assert_conserved(mut tags: Vec<u32>, total: u32) {
        tags.sort_unstable();
        assert_eq!(tags, (0..total).collect::<Vec<u32>>(), "a claim was split or duplicated");
    }

    #[test]
    fn loom_pop_batch_conserves_a_single_batch_claim() {
        // One producer claims three slots in one CAS and publishes them one
        // by one; two consumers of different batch widths poll the same head
        // through that window.
        loom::model(|| {
            const TOTAL: u32 = 3;
            let queue = Arc::new(ArrayQueue::<u32, true, true>::new(4));
            let producer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    let mut batch =
                        [MaybeUninit::new(0u32), MaybeUninit::new(1u32), MaybeUninit::new(2u32)];
                    // SAFETY: `batch` is fully initialized and the ring is empty.
                    assert_eq!(unsafe { queue.try_push_batch(&mut batch) }, TOTAL as usize);
                })
            };
            let wide = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };
            let narrow = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 1, TOTAL))
            };

            producer.join().unwrap();
            let mut tags = wide.join().unwrap();
            tags.extend(narrow.join().unwrap());
            tags.extend(drain_rest(&queue, TOTAL));
            assert_conserved(tags, TOTAL);
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    fn loom_pop_batch_interleaves_item_and_batch_publication() {
        // Per-item pushes and a two-slot batch claim fill the same ring
        // behind one consumer: each writer stamps its slot only after
        // writing it, so the ring order the consumer claims has no hole it
        // could read through.
        loom::model(|| {
            const TOTAL: u32 = 4;
            let queue = Arc::new(ArrayQueue::<u32, true, true>::new(4));
            let batch = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    let mut batch = [MaybeUninit::new(0u32), MaybeUninit::new(1u32)];
                    // SAFETY: `batch` is fully initialized; the claimed prefix
                    // is moved out and the written tail stays owned here.
                    let n = unsafe { queue.try_push_batch(&mut batch) };
                    // A tail load that predates the peer's claim makes the walk
                    // come back short (documented: the caller falls back to
                    // per-item pushes); the ring never fills, so they succeed.
                    for tag in n..2 {
                        let value = MaybeUninit::new(tag as u32);
                        // SAFETY: TOTAL <= capacity, so the ring never fills.
                        assert!(unsafe { queue.push_with_ptr(value.as_ptr()) });
                    }
                })
            };
            let item = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    for tag in 2..TOTAL {
                        let value = MaybeUninit::new(tag);
                        // SAFETY: TOTAL <= capacity, so the ring never fills.
                        assert!(unsafe { queue.push_with_ptr(value.as_ptr()) });
                    }
                })
            };
            let consumer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };

            batch.join().unwrap();
            item.join().unwrap();
            let mut tags = consumer.join().unwrap();
            tags.extend(drain_rest(&queue, TOTAL));
            assert_conserved(tags, TOTAL);
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    fn loom_pop_batch_racing_runs_never_reclaim_a_lost_run() {
        // One four-slot claim published slot by slot while two consumers
        // claim two slots per call: whoever loses the head CAS must retry
        // from the winner's head and never re-claim the run it lost.
        loom::model(|| {
            const TOTAL: u32 = 4;
            let queue = Arc::new(ArrayQueue::<u32, true, true>::new(4));
            let producer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    let mut batch = [
                        MaybeUninit::new(0u32),
                        MaybeUninit::new(1u32),
                        MaybeUninit::new(2u32),
                        MaybeUninit::new(3u32),
                    ];
                    // SAFETY: `batch` is fully initialized; the ring is empty
                    // and holds exactly these four entries.
                    assert_eq!(unsafe { queue.try_push_batch(&mut batch) }, TOTAL as usize);
                })
            };
            let first = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };
            let second = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };

            producer.join().unwrap();
            let mut tags = first.join().unwrap();
            tags.extend(second.join().unwrap());
            tags.extend(drain_rest(&queue, TOTAL));
            assert_conserved(tags, TOTAL);
            assert_eq!(queue.len(), 0);

            // A 0 return claimed nothing, so the drained ring holds no
            // phantom claim: both read facades agree it is empty and a fresh
            // claim publishes over the same slots.
            let mut out = [MaybeUninit::new(SENTINEL)];
            assert_eq!(queue.pop_batch(&mut out), 0);
            assert!(queue.pop(false).is_none());
            let mut fresh = [MaybeUninit::new(TOTAL)];
            // SAFETY: `fresh` is fully initialized and the ring is drained.
            assert_eq!(unsafe { queue.try_push_batch(&mut fresh) }, 1);
            assert_eq!(queue.pop_batch(&mut out), 1);
            assert_eq!(*unsafe { out[0].assume_init_ref() }, TOTAL);
        });
    }

    /// One producer: one batch claim, then the documented per-item fallback
    /// for the tail the claim did not move (`flush_staged`'s shape).
    fn push_batch_then_items(queue: &ArrayQueue<u32, true, true>, tags: &[u32]) {
        let mut values: Vec<MaybeUninit<u32>> =
            tags.iter().copied().map(MaybeUninit::new).collect();
        // SAFETY: `values` is fully initialized; the claimed prefix is moved
        // out of it and the tail is still owned here.
        let n = unsafe { queue.try_push_batch(&mut values) };
        assert!(n <= tags.len(), "claimed {n} slots of a {}-value batch", tags.len());
        for slot in &mut values[n..] {
            // SAFETY: the two batches fit the ring together, so the per-item
            // push always finds a free slot and never parks.
            assert!(unsafe { queue.push_with_ptr(slot.as_ptr()) });
        }
    }

    #[test]
    fn loom_try_push_batch_claims_publish_their_prefix() {
        // Two producers claim a run each in one tail CAS and push the rest per
        // item; one consumer polls two slots per call through both publication
        // windows.
        loom::model(|| {
            const TOTAL: u32 = 4;
            let queue = Arc::new(ArrayQueue::<u32, true, true>::new(4));
            let first = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || push_batch_then_items(&queue, &[0, 1]))
            };
            let second = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || push_batch_then_items(&queue, &[2, 3]))
            };
            let consumer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };

            first.join().unwrap();
            second.join().unwrap();
            let mut tags = consumer.join().unwrap();
            tags.extend(drain_rest(&queue, TOTAL));
            // A claim is only counted once the consumer can see its slots, so
            // a split claim, a re-claim and a claim the fallback then pushed
            // again all break this multiset.
            assert_conserved(tags, TOTAL);
            assert_eq!(queue.len(), 0);
        });
    }

    /// Payload whose drop bumps a per-tag counter: the counter is the
    /// ownership witness (2 = double drop, 0 = leak).
    struct Tagged {
        tag: u32,
        drops: Arc<Vec<AtomicUsize>>,
    }

    impl Drop for Tagged {
        fn drop(&mut self) {
            self.drops[self.tag as usize].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn new_drop_log(total: u32) -> Arc<Vec<AtomicUsize>> {
        Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect())
    }

    /// Claims as much of `tags` as the ring has free slots and drops the tail
    /// it kept. Returns `(claimed tags, tail tags)`.
    fn push_tagged_batch(
        queue: &ArrayQueue<Tagged, true, true>, tags: &[u32], drops: &Arc<Vec<AtomicUsize>>,
    ) -> (Vec<u32>, Vec<u32>) {
        let mut values: Vec<MaybeUninit<Tagged>> = tags
            .iter()
            .map(|&tag| MaybeUninit::new(Tagged { tag, drops: Arc::clone(drops) }))
            .collect();
        // SAFETY: `values` is fully initialized; entries [..n] are moved out
        // and entries [n..] stay owned here.
        let n = unsafe { queue.try_push_batch(&mut values) };
        assert!(n <= tags.len(), "claimed {n} slots of a {}-value batch", tags.len());
        let mut kept = Vec::new();
        for slot in values.iter_mut().skip(n) {
            // SAFETY: try_push_batch left entries [n..] initialized.
            let value = unsafe { slot.assume_init_read() };
            kept.push(value.tag);
        }
        (tags[..n].to_vec(), kept)
    }

    /// Pops up to `rounds` batches and drops every claimed value (that drop is
    /// the consumer half of the exactly-once count).
    fn consume_tagged(queue: &ArrayQueue<Tagged, true, true>, rounds: usize) -> Vec<u32> {
        let mut buf: Vec<MaybeUninit<Tagged>> = (0..4).map(|_| MaybeUninit::uninit()).collect();
        let mut got = Vec::new();
        for _ in 0..rounds {
            let n = queue.pop_batch(&mut buf);
            for slot in buf.iter_mut().take(n) {
                // SAFETY: pop_batch initialized exactly buf[..n].
                let value = unsafe { slot.assume_init_read() };
                got.push(value.tag);
            }
            loom::thread::yield_now();
        }
        got
    }

    fn drain_tagged(queue: &ArrayQueue<Tagged, true, true>) -> Vec<u32> {
        let mut got = Vec::new();
        loop {
            let round = consume_tagged(queue, 1);
            if round.is_empty() {
                return got;
            }
            got.extend(round);
        }
    }

    #[test]
    fn loom_try_push_batch_owns_only_the_moved_prefix() {
        // Five values against a four-slot ring: whichever producer claims
        // second gets a short claim and the tail it walked past stays its own
        // to drop, while every value the ring took is dropped by the consumer.
        loom::model(|| {
            const TOTAL: u32 = 5;
            let drops = new_drop_log(TOTAL);
            let queue = Arc::new(ArrayQueue::<Tagged, true, true>::new(4));
            let three = {
                let (queue, drops) = (Arc::clone(&queue), Arc::clone(&drops));
                loom::thread::spawn(move || push_tagged_batch(&queue, &[0, 1, 2], &drops))
            };
            let two = {
                let (queue, drops) = (Arc::clone(&queue), Arc::clone(&drops));
                loom::thread::spawn(move || push_tagged_batch(&queue, &[3, 4], &drops))
            };
            let consumer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consume_tagged(&queue, POLLS))
            };

            let (claimed_three, kept_three) = three.join().unwrap();
            let (claimed_two, kept_two) = two.join().unwrap();
            let mut consumed = consumer.join().unwrap();
            consumed.extend(drain_tagged(&queue));

            // Every value is either consumed from the ring or dropped by the
            // producer that kept it, and never both.
            let mut accounted = claimed_three.clone();
            accounted.extend(claimed_two.iter().copied());
            accounted.extend(kept_three.iter().copied());
            accounted.extend(kept_two.iter().copied());
            accounted.sort_unstable();
            assert_eq!(accounted, (0..TOTAL).collect::<Vec<u32>>(), "a value was claimed twice");

            let mut claimed = claimed_three;
            claimed.extend(claimed_two);
            claimed.sort_unstable();
            consumed.sort_unstable();
            assert_eq!(consumed, claimed, "the ring handed back something other than the claim");

            for (tag, count) in drops.iter().enumerate() {
                assert_eq!(count.load(Ordering::Relaxed), 1, "tag {tag} not dropped exactly once");
            }
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    fn loom_try_push_batch_zero_return_claims_nothing() {
        // One producer claims the whole four-slot ring, so the peer's
        // single-value batch either lands in the slot a consumer freed or
        // walks into a full ring and returns 0. The tags consumed are exactly
        // the claimed ones in either case, and after a 0 return the documented
        // per-item fallback still places the value.
        loom::model(|| {
            const TOTAL: u32 = 5;
            let queue = Arc::new(ArrayQueue::<u32, true, true>::new(4));
            let wide = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    let mut values: Vec<MaybeUninit<u32>> = (0..4).map(MaybeUninit::new).collect();
                    // SAFETY: `values` is fully initialized; the unsent tail
                    // stays owned here and is simply dropped (u32 has none).
                    unsafe { queue.try_push_batch(&mut values) }
                })
            };
            let narrow = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || {
                    let mut values = [MaybeUninit::new(4u32)];
                    // SAFETY: `values` is fully initialized.
                    unsafe { queue.try_push_batch(&mut values) }
                })
            };
            let consumer = {
                let queue = Arc::clone(&queue);
                loom::thread::spawn(move || consumer_polls(&queue, 2, TOTAL))
            };

            let wide_claimed = wide.join().unwrap();
            let narrow_claimed = narrow.join().unwrap();
            let mut tags = consumer.join().unwrap();
            tags.extend(drain_rest(&queue, TOTAL));

            // The narrow claim runs second in the full-ring interleavings, so
            // it either takes the one slot a consumer freed or nothing at all.
            assert!(narrow_claimed <= 1, "narrow claimed {narrow_claimed} of one value");
            assert!(wide_claimed <= 4, "wide claimed {wide_claimed} of four values");
            let mut expected: Vec<u32> = (0..u32::try_from(wide_claimed).unwrap()).collect();
            if narrow_claimed == 1 {
                expected.push(4);
            }
            tags.sort_unstable();
            assert_eq!(tags, expected, "a 0 return claimed a slot nothing published");
            assert_eq!(queue.len(), 0);

            if narrow_claimed == 0 {
                // Documented fallback: the caller retries per item. The retry
                // is applied on the quiescent ring, because a per-item push
                // issued right after the 0 return has to wait for a consumer to
                // free a slot, and that blocking spin never terminates on the
                // branch where the producer is the last runnable thread.
                let mut fresh = [MaybeUninit::new(4u32)];
                // SAFETY: `fresh` is fully initialized; the ring is drained.
                assert_eq!(unsafe { queue.try_push_batch(&mut fresh) }, 1);
                let mut out = [MaybeUninit::new(SENTINEL)];
                assert_eq!(queue.pop_batch(&mut out), 1);
                assert_eq!(*unsafe { out[0].assume_init_ref() }, 4);
                assert_eq!(queue.len(), 0);
            }
        });
    }
}
