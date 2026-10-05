use alloc::alloc::{alloc_zeroed, handle_alloc_error};
use alloc::boxed::Box;
use core::alloc::Layout;
use core::mem::{MaybeUninit, size_of};
use core::ptr;

use crossbeam_utils::CachePadded;

use crate::const_fn;
use crate::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use crate::sync::cell::UnsafeCell;
#[allow(unused_imports)]
use crate::sync::prelude::*;
use crate::sync::Backoff;
use crate::{PopError, PushError};

// Bits indicating the state of a slot:
// * If a value has been written into the slot, `WRITE` is set.
// * If a value has been read from the slot, `READ` is set.
// * If the block is being destroyed, `DESTROY` is set.
const WRITE: usize = 1;
const READ: usize = 2;
const DESTROY: usize = 4;

// Each block covers one "lap" of indices.
const LAP: usize = 32;
// The maximum number of items a block can hold.
const BLOCK_CAP: usize = LAP - 1;
// How many lower bits are reserved for metadata.
const SHIFT: usize = 1;
// Has two different purposes:
// * If set in head, indicates that the block is not the last one.
// * If set in tail, indicates that the queue is closed.
const MARK_BIT: usize = 1;

/// A slot in a block.
struct Slot<T> {
    /// The value.
    value: UnsafeCell<MaybeUninit<T>>,

    /// The state of the slot.
    state: AtomicUsize,
}

impl<T> Slot<T> {
    #[cfg(loom)]
    fn uninit_block() -> [Slot<T>; BLOCK_CAP] {
        // Repeat this expression 31 times.
        // Update if we change BLOCK_CAP
        macro_rules! repeat_31 {
            ($e: expr) => {
                [
                    $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e,
                    $e, $e, $e, $e, $e, $e, $e, $e, $e, $e, $e,
                ]
            };
        }

        repeat_31!(Slot {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicUsize::new(0),
        })
    }

    /// Waits until a value is written into the slot.
    fn wait_write(&self) {
        let backoff = Backoff::new();
        while self.state.load(Ordering::Acquire) & WRITE == 0 {
            backoff.snooze();
        }
    }
}

/// A block in a linked list.
///
/// Each block in the list can hold up to `BLOCK_CAP` values.
struct Block<T> {
    /// The next block in the linked list.
    next: AtomicPtr<Block<T>>,

    /// Slots for values.
    slots: [Slot<T>; BLOCK_CAP],
}

impl<T> Block<T> {
    const LAYOUT: Layout = {
        let layout = Layout::new::<Self>();
        assert!(
            layout.size() != 0,
            "Block should never be zero-sized, as it has an AtomicPtr field"
        );
        layout
    };

    /// Creates an empty block.
    #[cfg(not(loom))]
    fn new() -> Box<Block<T>> {
        // All-zero bytes are a valid `Block`: `next` is a null pointer,
        // `state` is 0 (no WRITE/READ/DESTROY bits set) and the `MaybeUninit`
        // value slots are uninitialized by definition. A zeroed allocation
        // lets the allocator serve fresh pages without a memset and skips
        // the per-slot const-array copy of the previous `Box::new(Block)`
        // (see concurrent-queue-PERFORMANCE_REVIEW.md P2; mirrors
        // crossbeam's SegQueue).
        let raw = unsafe { alloc_zeroed(Self::LAYOUT) };
        if raw.is_null() {
            handle_alloc_error(Self::LAYOUT);
        }
        // Safety: `raw` is a live, zero-initialized allocation for `Self`.
        unsafe { Box::from_raw(raw.cast()) }
    }

    /// Creates an empty block (loom build: loom's tracked cell types must be
    /// constructed directly, not synthesized from zeroed memory).
    #[cfg(loom)]
    fn new() -> Box<Block<T>> {
        Box::new(Block {
            next: AtomicPtr::new(ptr::null_mut()),
            slots: Slot::uninit_block(),
        })
    }

    /// Waits until the next pointer is set.
    fn wait_next(&self) -> *mut Block<T> {
        let backoff = Backoff::new();
        loop {
            let next = self.next.load(Ordering::Acquire);
            if !next.is_null() {
                return next;
            }
            backoff.snooze();
        }
    }

    /// Sets the `DESTROY` bit in slots starting from `start` and destroys the block.
    unsafe fn destroy(this: *mut Block<T>, start: usize) {
        // It is not necessary to set the `DESTROY` bit in the last slot because that slot has
        // begun destruction of the block.
        for i in start..BLOCK_CAP - 1 {
            let slot = (*this).slots.get_unchecked(i);

            // Mark the `DESTROY` bit if a thread is still using the slot.
            if slot.state.load(Ordering::Acquire) & READ == 0
                && slot.state.fetch_or(DESTROY, Ordering::AcqRel) & READ == 0
            {
                // If a thread is still using the slot, it will continue destruction of the block.
                return;
            }
        }

        // No thread is using the block, now it is safe to destroy it.
        drop(Box::from_raw(this));
    }
}

/// A position in a queue.
struct Position<T> {
    /// The index in the queue.
    index: AtomicUsize,

    /// The block in the linked list.
    block: AtomicPtr<Block<T>>,
}

/// An unbounded queue.
pub struct Unbounded<T> {
    /// The head of the queue.
    head: CachePadded<Position<T>>,

    /// The tail of the queue.
    tail: CachePadded<Position<T>>,
}

impl<T> Unbounded<T> {
    const_fn!(
        const_if: #[cfg(not(loom))];
        /// Creates a new unbounded queue.
        pub const fn new() -> Unbounded<T> {
            Unbounded {
                head: CachePadded::new(Position {
                    block: AtomicPtr::new(ptr::null_mut()),
                    index: AtomicUsize::new(0),
                }),
                tail: CachePadded::new(Position {
                    block: AtomicPtr::new(ptr::null_mut()),
                    index: AtomicUsize::new(0),
                }),
            }
        }
    );

    /// Pushes an item into the queue.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        let mut tail = self.tail.index.load(Ordering::Acquire);
        let mut block = self.tail.block.load(Ordering::Acquire);
        let mut next_block = None;
        let backoff = Backoff::new();

        loop {
            // Check if the queue is closed.
            if tail & MARK_BIT != 0 {
                return Err(PushError::Closed(value));
            }

            // Calculate the offset of the index into the block.
            let offset = (tail >> SHIFT) % LAP;

            // If we reached the end of the block, wait until the next one is installed.
            if offset == BLOCK_CAP {
                backoff.snooze();
                tail = self.tail.index.load(Ordering::Acquire);
                block = self.tail.block.load(Ordering::Acquire);
                continue;
            }

            // If we're going to have to install the next block, allocate it in advance in order to
            // make the wait for other threads as short as possible.
            if offset + 1 == BLOCK_CAP && next_block.is_none() {
                next_block = Some(Block::<T>::new());
            }

            // If this is the first value to be pushed into the queue, we need to allocate the
            // first block and install it.
            if block.is_null() {
                let new = Box::into_raw(Block::<T>::new());

                if self
                    .tail
                    .block
                    .compare_exchange(block, new, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    self.head.block.store(new, Ordering::Release);
                    block = new;
                } else {
                    next_block = unsafe { Some(Box::from_raw(new)) };
                    tail = self.tail.index.load(Ordering::Acquire);
                    block = self.tail.block.load(Ordering::Acquire);
                    continue;
                }
            }

            let new_tail = tail + (1 << SHIFT);

            // Try advancing the tail forward.
            match self.tail.index.compare_exchange_weak(
                tail,
                new_tail,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    // If we've reached the end of the block, install the next one.
                    if offset + 1 == BLOCK_CAP {
                        let next_block = Box::into_raw(next_block.unwrap());
                        self.tail.block.store(next_block, Ordering::Release);
                        self.tail.index.fetch_add(1 << SHIFT, Ordering::Release);
                        (*block).next.store(next_block, Ordering::Release);
                    }

                    // Write the value into the slot.
                    let slot = (*block).slots.get_unchecked(offset);
                    slot.value.with_mut(|slot| {
                        slot.write(MaybeUninit::new(value));
                    });
                    slot.state.fetch_or(WRITE, Ordering::Release);
                    return Ok(());
                },
                Err(t) => {
                    backoff.spin();
                    tail = t;
                    block = self.tail.block.load(Ordering::Acquire);
                }
            }
        }
    }

    // Items wider than this skip the stack-staged segment in `push_n` and
    // take the per-item `push` fallback instead: staging is `BLOCK_CAP`
    // items wide, and 31 x 256 B keeps the stage within ~8 KiB of stack.
    const MAX_STAGED_ITEM_SIZE: usize = 256;

    /// Pushes a batch of items with one tail CAS per block segment.
    ///
    /// Whereas [`push`](Self::push) performs a `compare_exchange` on the tail
    /// index for every item, `push_n` reserves a contiguous run of `n` slots
    /// with a single CAS (`n` capped at the current block's remaining space,
    /// i.e. at most `BLOCK_CAP`), then fills the reserved slots with plain
    /// stores. For callers that inject whole batches at once this removes the
    /// per-item serialization of `lock cmpxchg` on the shared tail cache line
    /// (N contended RMWs → N/BLOCK_CAP + 1).
    ///
    /// Segment semantics are identical to `push`: a successful CAS owns
    /// `[tail, tail+n)`; values become visible to consumers via per-slot
    /// `WRITE` flags set with `Release` ordering; when a segment exactly fills
    /// a block the caller installs and links the next block exactly like
    /// `push` does at the block boundary.
    ///
    /// Segment lengths derive from the items actually pulled out of the
    /// iterator, never from `ExactSizeIterator::len()` alone: `len()` is a
    /// safe hint (only the unstable `TrustedLen` is a contract), and
    /// reserving slots that end up unfilled would leave the queue with
    /// reserved-but-unwritten slots — `pop` would then spin on `wait_write`
    /// forever and the queue's `Drop` would run `drop_in_place` on
    /// uninitialized values. An iterator that over-reports its length simply
    /// ends the batch early.
    ///
    /// If the queue is closed before a segment is reserved, no further items
    /// are written (values still inside the iterator are dropped) — this is
    /// the batch analogue of `push` returning `PushError::Closed`, except that
    /// already-written segments remain readable by consumers. Returns the
    /// number of items actually written.
    ///
    /// Unlike `push`, a concurrent `close()` between segments cannot hand the
    /// unwritten values back to the caller (a partially consumed iterator
    /// cannot be reconstructed); use per-item `push` if item-granular recovery
    /// is required.
    pub fn push_n<I>(&self, values: I) -> usize
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut iter = values.into_iter();
        let len = iter.len();
        if len == 0 {
            return 0;
        }

        // A stack-staged segment is `BLOCK_CAP` items wide; for very large
        // `T` that could overflow small thread stacks, so fall back to
        // per-item `push` (same semantics, one CAS per item).
        if size_of::<T>() > Self::MAX_STAGED_ITEM_SIZE {
            let mut written = 0;
            for _ in 0..len {
                let Some(value) = iter.next() else { break };
                // The unbounded queue only fails `push` when closed; the
                // value is dropped together with the error and the iterator
                // drops the rest.
                if self.push(value).is_err() {
                    break;
                }
                written += 1;
            }
            return written;
        }

        let mut tail = self.tail.index.load(Ordering::Acquire);
        let mut block = self.tail.block.load(Ordering::Acquire);
        let mut next_block: Option<Box<Block<T>>> = None;
        let backoff = Backoff::new();
        let mut written = 0usize;

        // Values pulled from the iterator but not yet written into the queue,
        // live in `[stage_start, stage_end)`. An iterator cannot be rewound,
        // so the stage must survive CAS retries and the first-block-install
        // race below.
        let mut stage: [MaybeUninit<T>; BLOCK_CAP] = core::array::from_fn(|_| MaybeUninit::uninit());
        let mut stage_start = 0usize;
        let mut stage_end = 0usize;

        loop {
            // Check if the queue is closed.
            if tail & MARK_BIT != 0 {
                // SAFETY: every slot in `[stage_start, stage_end)` holds a
                // value pulled from the iterator by the staging loop below;
                // the iterator can no longer drop them for us.
                for slot in &mut stage[stage_start..stage_end] {
                    unsafe { slot.assume_init_drop() };
                }
                return written;
            }

            // Calculate the offset of the index into the block.
            let offset = (tail >> SHIFT) % LAP;

            // If we reached the end of the block, wait until the next one is
            // installed by the thread that filled the previous slot.
            if offset == BLOCK_CAP {
                backoff.snooze();
                tail = self.tail.index.load(Ordering::Acquire);
                block = self.tail.block.load(Ordering::Acquire);
                continue;
            }

            // (Re)fill the stage from the iterator. `len` is only a hint —
            // an over-reporting `ExactSizeIterator` is plain safe code — so
            // the iterator may dry up before `written == len`; an empty stage
            // then ends the batch. (This is also why there is no
            // `debug_assert` on `iter.len()` anymore: the lie itself is the
            // gracefully handled case, not an invariant violation.)
            if stage_start == stage_end {
                let cap = (BLOCK_CAP - offset).min(len - written);
                let mut m = 0;
                while m < cap {
                    match iter.next() {
                        Some(value) => {
                            stage[m].write(value);
                            m += 1;
                        }
                        None => break,
                    }
                }
                if m == 0 {
                    return written;
                }
                stage_start = 0;
                stage_end = m;
            }

            // Reserve exactly what the stage holds. A CAS retry may have
            // landed on an offset with less remaining space than the stage
            // was filled for, so cap by the current block's remainder; the
            // surplus stays staged for the next iteration.
            let n = (stage_end - stage_start).min(BLOCK_CAP - offset);

            // If this segment is going to fill the block, allocate the next
            // one in advance so the wait for other threads is as short as
            // possible (mirrors `push`).
            if offset + n == BLOCK_CAP && next_block.is_none() {
                next_block = Some(Block::<T>::new());
            }

            // If this is the first push into the queue, allocate the first
            // block and install it.
            if block.is_null() {
                let new = Box::into_raw(Block::<T>::new());

                if self
                    .tail
                    .block
                    .compare_exchange(block, new, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    self.head.block.store(new, Ordering::Release);
                    block = new;
                } else {
                    next_block = unsafe { Some(Box::from_raw(new)) };
                    tail = self.tail.index.load(Ordering::Acquire);
                    block = self.tail.block.load(Ordering::Acquire);
                    continue;
                }
            }

            // Try reserving the whole segment in one CAS.
            let new_tail = tail + (n << SHIFT);
            match self.tail.index.compare_exchange_weak(
                tail,
                new_tail,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    // Fill the reserved slots. SAFETY: the CAS granted
                    // exclusive ownership of `[offset, offset+n)` in `block`,
                    // and the stage holds `n` initialized values in
                    // `[stage_start, stage_start + n)`; each `assume_init_read`
                    // moves a value out, leaving that stage slot logically
                    // uninitialized.
                    for k in 0..n {
                        let value = stage[stage_start + k].assume_init_read();
                        let slot = (*block).slots.get_unchecked(offset + k);
                        slot.value.with_mut(|slot| {
                            slot.write(MaybeUninit::new(value));
                        });
                        slot.state.fetch_or(WRITE, Ordering::Release);
                    }
                    stage_start += n;
                    written += n;

                    // If the segment filled the block, install the next one.
                    if offset + n == BLOCK_CAP {
                        let next = Box::into_raw(next_block.take().unwrap());
                        self.tail.block.store(next, Ordering::Release);
                        self.tail.index.fetch_add(1 << SHIFT, Ordering::Release);
                        (*block).next.store(next, Ordering::Release);
                        block = next;
                        tail = new_tail + (1 << SHIFT);
                    } else {
                        tail = new_tail;
                    }

                    if stage_start == stage_end && written == len {
                        return written;
                    }
                },
                Err(t) => {
                    backoff.spin();
                    tail = t;
                    block = self.tail.block.load(Ordering::Acquire);
                }
            }
        }
    }
    /// Pops an item from the queue.
    pub fn pop(&self) -> Result<T, PopError> {
        let mut head = self.head.index.load(Ordering::Acquire);
        let mut block = self.head.block.load(Ordering::Acquire);
        let backoff = Backoff::new();

        loop {
            // Calculate the offset of the index into the block.
            let offset = (head >> SHIFT) % LAP;

            // If we reached the end of the block, wait until the next one is installed.
            if offset == BLOCK_CAP {
                backoff.snooze();
                head = self.head.index.load(Ordering::Acquire);
                block = self.head.block.load(Ordering::Acquire);
                continue;
            }

            let mut new_head = head + (1 << SHIFT);

            if new_head & MARK_BIT == 0 {
                crate::full_fence();
                let tail = self.tail.index.load(Ordering::Relaxed);

                // If the tail equals the head, that means the queue is empty.
                if head >> SHIFT == tail >> SHIFT {
                    // Check if the queue is closed.
                    if tail & MARK_BIT != 0 {
                        return Err(PopError::Closed);
                    } else {
                        return Err(PopError::Empty);
                    }
                }

                // If head and tail are not in the same block, set `MARK_BIT` in head.
                if (head >> SHIFT) / LAP != (tail >> SHIFT) / LAP {
                    new_head |= MARK_BIT;
                }
            }

            // The block can be null here only if the first push operation is in progress.
            if block.is_null() {
                backoff.snooze();
                head = self.head.index.load(Ordering::Acquire);
                block = self.head.block.load(Ordering::Acquire);
                continue;
            }

            // Try moving the head index forward.
            match self.head.index.compare_exchange_weak(
                head,
                new_head,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => unsafe {
                    // If we've reached the end of the block, move to the next one.
                    if offset + 1 == BLOCK_CAP {
                        let next = (*block).wait_next();
                        let mut next_index = (new_head & !MARK_BIT).wrapping_add(1 << SHIFT);
                        if !(*next).next.load(Ordering::Relaxed).is_null() {
                            next_index |= MARK_BIT;
                        }

                        self.head.block.store(next, Ordering::Release);
                        self.head.index.store(next_index, Ordering::Release);
                    }

                    // Read the value.
                    let slot = (*block).slots.get_unchecked(offset);
                    slot.wait_write();
                    let value = slot.value.with_mut(|slot| slot.read().assume_init());

                    // Destroy the block if we've reached the end, or if another thread wanted to
                    // destroy but couldn't because we were busy reading from the slot.
                    if offset + 1 == BLOCK_CAP {
                        Block::destroy(block, 0);
                    } else if slot.state.fetch_or(READ, Ordering::AcqRel) & DESTROY != 0 {
                        Block::destroy(block, offset + 1);
                    }

                    return Ok(value);
                },
                Err(h) => {
                    backoff.spin();
                    head = h;
                    block = self.head.block.load(Ordering::Acquire);
                }
            }
        }
    }

    /// Returns the number of items in the queue.
    pub fn len(&self) -> usize {
        loop {
            // Load the tail index, then load the head index.
            let mut tail = self.tail.index.load(Ordering::SeqCst);
            let mut head = self.head.index.load(Ordering::SeqCst);

            // If the tail index didn't change, we've got consistent indices to work with.
            if self.tail.index.load(Ordering::SeqCst) == tail {
                // Erase the lower bits.
                tail &= !((1 << SHIFT) - 1);
                head &= !((1 << SHIFT) - 1);

                // Fix up indices if they fall onto block ends.
                if (tail >> SHIFT) & (LAP - 1) == LAP - 1 {
                    tail = tail.wrapping_add(1 << SHIFT);
                }
                if (head >> SHIFT) & (LAP - 1) == LAP - 1 {
                    head = head.wrapping_add(1 << SHIFT);
                }

                // Rotate indices so that head falls into the first block.
                let lap = (head >> SHIFT) / LAP;
                tail = tail.wrapping_sub((lap * LAP) << SHIFT);
                head = head.wrapping_sub((lap * LAP) << SHIFT);

                // Remove the lower bits.
                tail >>= SHIFT;
                head >>= SHIFT;

                // Return the difference minus the number of blocks between tail and head.
                return tail - head - tail / LAP;
            }
        }
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let head = self.head.index.load(Ordering::SeqCst);
        let tail = self.tail.index.load(Ordering::SeqCst);
        head >> SHIFT == tail >> SHIFT
    }

    /// Returns `true` if the queue is full.
    pub fn is_full(&self) -> bool {
        false
    }

    /// Closes the queue.
    ///
    /// Returns `true` if this call closed the queue.
    pub fn close(&self) -> bool {
        let tail = self.tail.index.fetch_or(MARK_BIT, Ordering::SeqCst);
        tail & MARK_BIT == 0
    }

    /// Returns `true` if the queue is closed.
    pub fn is_closed(&self) -> bool {
        self.tail.index.load(Ordering::SeqCst) & MARK_BIT != 0
    }
}

impl<T> Drop for Unbounded<T> {
    fn drop(&mut self) {
        let Self { head, tail } = self;
        let Position { index: head, block } = &mut **head;

        head.with_mut(|&mut mut head| {
            tail.index.with_mut(|&mut mut tail| {
                // Erase the lower bits.
                head &= !((1 << SHIFT) - 1);
                tail &= !((1 << SHIFT) - 1);

                unsafe {
                    // Drop all values between `head` and `tail` and deallocate the heap-allocated blocks.
                    while head != tail {
                        let offset = (head >> SHIFT) % LAP;

                        if offset < BLOCK_CAP {
                            // Drop the value in the slot.
                            block.with_mut(|block| {
                                let slot = (**block).slots.get_unchecked(offset);
                                slot.value.with_mut(|slot| {
                                    let value = &mut *slot;
                                    value.as_mut_ptr().drop_in_place();
                                });
                            });
                        } else {
                            // Deallocate the block and move to the next one.
                            block.with_mut(|block| {
                                let next_block = (**block).next.with_mut(|next| *next);
                                drop(Box::from_raw(*block));
                                *block = next_block;
                            });
                        }

                        head = head.wrapping_add(1 << SHIFT);
                    }

                    // Deallocate the last remaining block.
                    block.with_mut(|block| {
                        if !block.is_null() {
                            drop(Box::from_raw(*block));
                        }
                    });
                }
            });
        });
    }
}
