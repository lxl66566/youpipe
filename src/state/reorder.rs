use std::mem::MaybeUninit;

struct Slot<T> {
    seq: u64,
    occupied: bool,
    item: MaybeUninit<T>,
}

/// A sequence-numbered re-sequencing buffer.
///
/// Items arrive tagged with a `u64` sequence number; `insert` returns any
/// items whose sequence numbers form a contiguous run starting from the last
/// flushed position. Out-of-order arrivals are buffered until their
/// predecessors arrive.
///
/// # Capacity precondition
///
/// The buffer uses power-of-two masking, so sequence numbers are mapped to
/// slots via `seq & mask`. If the number of *simultaneously outstanding*
/// (un-flushed) items ever exceeds `capacity`, two distinct sequence numbers
/// alias the same slot and the older item is **silently dropped**. Callers
/// must size the buffer to at least the maximum out-of-order window. The
/// streaming collectors clamp the window to `[1 Ki, 1 Mi]` slots, which is
/// ample for realistic worker counts.
///
/// The slot array is allocated lazily on the first out-of-order arrival: an
/// in-order stream (the common case) never touches it, so constructing the
/// buffer costs nothing beyond two integers and an empty `Vec`.
pub struct ReorderBuffer<T> {
    slots: Vec<Slot<T>>,
    next_expected: u64,
    len: usize,
    mask: usize,
}

impl<T> ReorderBuffer<T> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1).next_power_of_two();
        Self {
            // Lazy: `ensure_slots` materializes the array on the first
            // out-of-order arrival. In-order streams pay zero allocation
            // (and zero zero-init of up to 1 Mi slots).
            slots: Vec::new(),
            next_expected: 0,
            len: 0,
            mask: cap - 1,
        }
    }

    /// Materialize the slot array on first use. Must be called before any
    /// slot access; guarded by `len == 0 ⇒ slots untouched` in the fast path.
    fn ensure_slots(&mut self) {
        if self.slots.is_empty() {
            self.slots = (0..=self.mask)
                .map(|_| Slot {
                    seq: 0,
                    occupied: false,
                    item: MaybeUninit::uninit(),
                })
                .collect();
        }
    }

    /// Insert `item` tagged with `seq`, writing any newly-contiguous run of
    /// items directly into `sink` — the zero-allocation hot path used by the
    /// streaming ordered collectors.
    ///
    /// The `sink` out-parameter avoids the per-call `Vec` of [`insert`]: in the
    /// in-order steady state that returned `Vec` has length 1, so callers paid
    /// a `malloc` + `free` per item purely to move one value — at 100 k+ items
    /// that churn dominated the ordered collector's cost.
    pub fn insert_into(&mut self, seq: u64, item: T, sink: &mut Vec<T>) {
        // Fast path: item is exactly next expected and nothing is buffered.
        // `len == 0` implies every slot is unoccupied (occupied slots are
        // counted by `len`), so this scalar guard subsumes a defensive
        // `!slots[idx].occupied` read — and with the lazy slot array an
        // in-order stream never allocates it at all. Nothing can be flushable,
        // so the flush pass is skipped too.
        if seq == self.next_expected && self.len == 0 {
            sink.push(item);
            self.next_expected += 1;
            return;
        }
        self.ensure_slots();
        // `seq as usize` is safe across pointer widths: `& self.mask` only
        // keeps the low log2(capacity) bits, so truncation on 32-bit targets
        // is harmless (capacity is always < 2³²).
        #[allow(clippy::cast_possible_truncation)]
        let idx = (seq as usize) & self.mask;
        // Slow path: out-of-order arrival, or in-order arrival while gaps are
        // still buffered (the slot write + read-back is the 3 stores + 2 loads
        // + len bookkeeping the fast path avoids).
        let slot = &mut self.slots[idx];
        if slot.occupied {
            // Capacity precondition violated: a different seq aliases this
            // slot. The old item is dropped to avoid a leak. See the type-level
            // doc for the capacity contract.
            debug_assert_ne!(
                slot.seq, seq,
                "duplicate seq {seq} — ReorderBuffer is single-item-per-seq; use without `expand`"
            );
            unsafe { slot.item.assume_init_drop() };
            self.len -= 1;
        }
        slot.occupied = true;
        slot.seq = seq;
        slot.item.write(item);
        self.len += 1;
        self.flush_ready_into(sink);
    }

    /// Insert `item` and return any newly-contiguous run as a `Vec`.
    ///
    /// Convenience wrapper around [`insert_into`](Self::insert_into) (e.g.
    /// tests); prefer `insert_into` on hot paths to avoid the per-call
    /// allocation.
    pub fn insert(&mut self, seq: u64, item: T) -> Vec<T> {
        let mut ready = Vec::new();
        self.insert_into(seq, item, &mut ready);
        ready
    }

    fn flush_ready_into(&mut self, sink: &mut Vec<T>) {
        // `len == 0` ⇒ no occupied slots ⇒ the loop below would break on its
        // first iteration anyway. Also keeps the lazy `slots` array (empty
        // `Vec`) safely unindexed.
        if self.len == 0 {
            return;
        }
        loop {
            // See `insert_into` for why truncation is harmless.
            #[allow(clippy::cast_possible_truncation)]
            let idx = (self.next_expected as usize) & self.mask;
            if !self.slots[idx].occupied || self.slots[idx].seq != self.next_expected {
                break;
            }
            let slot = &mut self.slots[idx];
            // SAFETY: slot is occupied and init (checked above).
            let item = unsafe { slot.item.assume_init_read() };
            slot.occupied = false;
            self.len -= 1;
            self.next_expected += 1;
            sink.push(item);
        }
    }

    pub fn flush_remaining(&mut self) -> Vec<T> {
        let mut items: Vec<(u64, T)> = Vec::with_capacity(self.len);
        for slot in &mut self.slots {
            if slot.occupied {
                let item = unsafe { slot.item.assume_init_read() };
                slot.occupied = false;
                items.push((slot.seq, item));
            }
        }
        items.sort_by_key(|(seq, _)| *seq);
        self.len = 0;
        items.into_iter().map(|(_, item)| item).collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    pub fn reset(&mut self) {
        for slot in &mut self.slots {
            if slot.occupied {
                unsafe { slot.item.assume_init_drop() };
                slot.occupied = false;
            }
        }
        self.len = 0;
        self.next_expected = 0;
    }
}

impl<T> Drop for ReorderBuffer<T> {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if slot.occupied {
                unsafe { slot.item.assume_init_drop() };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_order() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        assert_eq!(buf.insert(0, 10), vec![10]);
        assert_eq!(buf.insert(1, 20), vec![20]);
        assert_eq!(buf.insert(2, 30), vec![30]);
    }

    /// The in-order steady state takes the `seq == next_expected` fast path:
    /// every item is pushed straight to the sink, no slot bookkeeping.
    #[test]
    fn test_fast_path_steady_state() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        let mut out = Vec::new();
        for i in 0..10u64 {
            buf.insert_into(i, i32::try_from(i * 10).unwrap(), &mut out);
        }
        assert_eq!(out, (0..10).map(|i| i * 10).collect::<Vec<_>>());
        assert!(buf.is_empty());
        assert_eq!(buf.next_expected(), 10);
    }

    /// A fast-path item that unblocks buffered successors must flush them.
    #[test]
    fn test_fast_path_flushes_buffered_run() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        let mut out = Vec::new();
        buf.insert_into(2, 30, &mut out); // buffered
        buf.insert_into(3, 40, &mut out); // buffered
        assert_eq!(out, Vec::<i32>::new());
        buf.insert_into(0, 10, &mut out); // fast path — immediately emitted
        assert_eq!(out, vec![10]);
        buf.insert_into(1, 20, &mut out); // fast path; flushes 2, 3
        assert_eq!(out, vec![10, 20, 30, 40]);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_out_of_order() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        assert_eq!(buf.insert(2, 30), Vec::<i32>::new());
        assert_eq!(buf.insert(0, 10), vec![10]);
        assert_eq!(buf.insert(1, 20), vec![20, 30]);
    }

    #[test]
    fn test_gap() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        assert_eq!(buf.insert(0, 10), vec![10]);
        assert_eq!(buf.insert(3, 40), Vec::<i32>::new());
        assert_eq!(buf.insert(5, 60), Vec::<i32>::new());
        assert_eq!(buf.insert(1, 20), vec![20]);
        assert_eq!(buf.insert(4, 50), Vec::<i32>::new());
    }

    #[test]
    fn test_flush_remaining() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        buf.insert(0, 10);
        buf.insert(3, 40);
        buf.insert(1, 20);
        buf.insert(5, 50);
        let remaining = buf.flush_remaining();
        assert_eq!(remaining, vec![40, 50]);
    }

    #[test]
    fn test_capacity_overflow() {
        let mut buf = ReorderBuffer::<i32>::new(2);
        assert_eq!(buf.insert(5, 50), Vec::<i32>::new());
        assert_eq!(buf.insert(3, 30), Vec::<i32>::new());
        assert_eq!(buf.insert(1, 10), Vec::<i32>::new());
        assert!(buf.len() <= 2);
    }

    /// The slot array stays unallocated while arrivals are in order: the
    /// whole reorder machinery is two integers and an empty `Vec`.
    #[test]
    fn test_lazy_slots_in_order() {
        let mut buf = ReorderBuffer::<i32>::new(1 << 20);
        let mut out = Vec::new();
        for i in 0..1000u64 {
            buf.insert_into(i, i32::try_from(i).unwrap(), &mut out);
            assert!(buf.slots.is_empty(), "in-order arrival must not allocate");
        }
        assert_eq!(out.len(), 1000);
        assert_eq!(buf.next_expected(), 1000);
        assert!(buf.is_empty());
    }

    /// The first out-of-order arrival materializes the slot array once, and
    /// re-sequencing still works after subsequent in-order arrivals.
    #[test]
    fn test_lazy_slots_allocated_once_on_reorder() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        let mut out = Vec::new();
        for i in 0..50u64 {
            buf.insert_into(i, i32::try_from(i).unwrap(), &mut out);
        }
        assert!(buf.slots.is_empty());
        buf.insert_into(52, 520, &mut out); // out of order → allocate
        let cap = buf.slots.len();
        assert!(cap >= 16);
        buf.insert_into(50, 500, &mut out); // fills the gap
        buf.insert_into(51, 510, &mut out); // flushes 50..=52
        assert_eq!(buf.slots.len(), cap, "no reallocation on later reorders");
        buf.insert_into(53, 530, &mut out); // in-order again (len == 0 fast path)
        assert_eq!(out.iter().copied().skip(50).collect::<Vec<_>>(), vec![
            500, 510, 520, 530
        ]);
    }
}
