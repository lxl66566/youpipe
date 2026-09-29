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
use core::sync::atomic::{self, AtomicUsize, Ordering};
use crossbeam_utils::{Backoff, CachePadded};

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
    /// run found (0 = nothing sendable right now — full or CAS lost after
    /// bounded retries). Backpressure granularity is unchanged from
    /// per-item send: callers fall back to per-item blocking sends for the
    /// remainder.
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
                match self.tail.compare_exchange(
                    tail,
                    pos,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
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
        if index + 1 < cap { pos + 1 } else { (pos & !(one_lap - 1)).wrapping_add(one_lap) }
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

impl<T, const MP: bool, const MC: bool> Drop for ArrayQueue<T, MP, MC> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // Get the index of the head.
            let head = *self.head.get_mut();
            let tail = *self.tail.get_mut();

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
