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
        let slots = (0..cap)
            .map(|_| Slot {
                seq: 0,
                occupied: false,
                item: MaybeUninit::uninit(),
            })
            .collect();
        Self {
            slots,
            next_expected: 0,
            len: 0,
            mask: cap - 1,
        }
    }

    /// Insert `item` tagged with `seq`, writing any newly-contiguous run of
    /// items directly into `sink` without allocating a temporary `Vec`.
    ///
    /// This is the zero-allocation hot path used by the streaming ordered
    /// collectors: each item arriving in order produces exactly one
    /// `sink.push(item)` with no per-item heap traffic. Contrast with
    /// [`insert`](Self::insert), which returns a fresh `Vec<T>` per call — in
    /// the in-order steady state that returned `Vec` has length 1, so the
    /// collector pays a `malloc` + `free` per item purely to move a single
    /// value. At 100 k+ items that allocation churn dominated the ordered
    /// collector's cost; this sink variant eliminates it.
    pub fn insert_into(&mut self, seq: u64, item: T, sink: &mut Vec<T>) {
        // `seq as usize` is safe across all pointer widths: the subsequent
        // `& self.mask` only keeps the low log2(capacity) bits, so truncation
        // on 32-bit targets is harmless (capacity is always < 2³²).
        #[allow(clippy::cast_possible_truncation)]
        let idx = (seq as usize) & self.mask;
        let slot = &mut self.slots[idx];
        if slot.occupied {
            // Capacity precondition violated: a different seq aliases this
            // slot. The old item is dropped to avoid a leak. See the type-level
            // doc for the capacity contract.
            debug_assert_ne!(
                slot.seq, seq,
                "duplicate seq {seq} — ReorderBuffer is single-item-per-seq; \
                 use without `expand`"
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
    /// Convenience wrapper around [`insert_into`](Self::insert_into) for
    /// callers that prefer a returned `Vec` over an out-parameter (e.g.
    /// tests). Prefer `insert_into` on hot paths to avoid the per-call
    /// allocation.
    pub fn insert(&mut self, seq: u64, item: T) -> Vec<T> {
        let mut ready = Vec::new();
        self.insert_into(seq, item, &mut ready);
        ready
    }

    fn flush_ready_into(&mut self, sink: &mut Vec<T>) {
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

    #[test]
    fn test_out_of_order() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        assert!(buf.insert(2, 30).is_empty());
        assert_eq!(buf.insert(0, 10), vec![10]);
        assert_eq!(buf.insert(1, 20), vec![20, 30]);
    }

    #[test]
    fn test_gap() {
        let mut buf = ReorderBuffer::<i32>::new(16);
        assert_eq!(buf.insert(0, 10), vec![10]);
        assert!(buf.insert(3, 40).is_empty());
        assert!(buf.insert(5, 60).is_empty());
        assert_eq!(buf.insert(1, 20), vec![20]);
        assert!(buf.insert(4, 50).is_empty());
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
        assert!(buf.insert(5, 50).is_empty());
        assert!(buf.insert(3, 30).is_empty());
        assert!(buf.insert(1, 10).is_empty());
        assert!(buf.len() <= 2);
    }
}
