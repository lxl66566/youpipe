use std::mem::MaybeUninit;

/// Slot tag encoding: `seq.wrapping_add(1)`, so `0` marks an unoccupied
/// slot and every real seq (including `0` itself) maps to a non-zero tag.
/// Packing `occupied` into the tag keeps a slot at `max(8, size_of::<T>())`
/// bytes — for `u64` items that is 16 B instead of the 24 B a
/// `seq + bool + MaybeUninit` layout costs, i.e. +50 % window density and
/// half the bytes read per slot probe in the flush scan.
///
/// `wrapping_add` keeps the encoding total: only `seq == u64::MAX` maps to
/// tag 0, which would make that one slot look unoccupied — sequence numbers
/// count flushed items and never approach `u64::MAX` in practice.
const UNOCCUPIED: u64 = 0;

struct Slot<T> {
    tag: u64,
    item: MaybeUninit<T>,
}

/// A sequence-numbered re-sequencing buffer.
///
/// Items arrive tagged with a `u64` sequence number; `insert` returns any
/// items whose sequence numbers form a contiguous run starting from the last
/// flushed position. Out-of-order arrivals are buffered until their
/// predecessors arrive.
///
/// # Capacity, growth, and drops
///
/// The buffer maps sequence numbers to slots via `seq & mask` (power-of-two
/// masking). Aliasing — two distinct *live* (un-flushed) seqs landing on one
/// slot — happens when the live span exceeds the capacity. Instead of
/// silently overwriting, an aliasing insert first **grows** the slot array
/// (doubling, rehashing the occupied slots; see [`Self::with_max`]) up to
/// `max_capacity`; only at that caller-pinned ceiling is the older item
/// dropped and counted in [`Self::dropped`]. A duplicate seq (identical tag)
/// is dropped immediately — the buffer is single-item-per-seq.
///
/// With `max_capacity ≥ next_pow2(item count)` and seqs `0..n` assigned by
/// one FIFO feeder, growth makes drops unreachable (span bound proof in
/// `state::stream::OrderedWindow`) — that is how the ordered collectors
/// size it, so they lose no items by construction. The ordered collectors
/// additionally validate `emitted + dropped == expected` after the drain
/// (see `state::stream`).
///
/// The slot array is allocated lazily on the first out-of-order arrival: an
/// in-order stream (the common case) never touches it, so constructing the
/// buffer costs nothing beyond two integers and an empty `Vec`.
pub struct ReorderBuffer<T> {
    slots: Vec<Slot<T>>,
    next_expected: u64,
    len: usize,
    mask: usize,
    /// Hard capacity ceiling (power of two, ≥ current capacity). Aliasing
    /// inserts grow toward it; at it, the older item is dropped and counted.
    max_capacity: usize,
    /// Items dropped by occupied-slot overwrites (span overflow at the
    /// pinned `max_capacity`, or a duplicate seq). A plain `usize`: the
    /// buffer is confined to the single collector thread
    /// (`insert_into`/`flush_*` all take `&mut self` from the drain
    /// loops), so no atomic is needed on the hot path.
    dropped: usize,
}

impl<T> ReorderBuffer<T> {
    /// Fixed-capacity buffer: no growth, an aliasing insert drops the older
    /// item (the pre-growth semantics, kept for callers that pin memory
    /// explicitly).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_max_inner(capacity, None)
    }

    /// Buffer that grows on demand: the slot array starts at
    /// `initial_capacity` (materialized lazily on the first out-of-order
    /// arrival) and doubles — rehashing occupied slots, never dropping —
    /// whenever the live span would alias, up to `max_capacity` (both
    /// rounded up to powers of two; a `max_capacity` below the initial
    /// capacity disables growth).
    #[must_use]
    pub fn with_max(initial_capacity: usize, max_capacity: usize) -> Self {
        Self::with_max_inner(initial_capacity, Some(max_capacity))
    }

    fn with_max_inner(capacity: usize, max_capacity: Option<usize>) -> Self {
        let cap = capacity.max(1).next_power_of_two();
        let max = match max_capacity {
            Some(m) => m.max(1).next_power_of_two().max(cap),
            None => cap,
        };
        Self {
            // Lazy: `ensure_slots` materializes the array on the first
            // out-of-order arrival. In-order streams pay zero allocation.
            slots: Vec::new(),
            next_expected: 0,
            len: 0,
            mask: cap - 1,
            max_capacity: max,
            dropped: 0,
        }
    }

    /// Materialize the slot array on first use. Must be called before any
    /// slot access; guarded by `len == 0 ⇒ slots untouched` in the fast path.
    fn ensure_slots(&mut self) {
        if self.slots.is_empty() {
            self.slots = (0..=self.mask)
                .map(|_| Slot {
                    tag: UNOCCUPIED,
                    item: MaybeUninit::uninit(),
                })
                .collect();
        }
    }

    /// Insert `item` tagged with `seq`, passing any newly-contiguous run of
    /// items to `sink` one by one — the zero-allocation hot path used by the
    /// streaming ordered collectors.
    ///
    /// The closure sink (rather than a `&mut Vec<T>` out-parameter) lets both
    /// terminal kinds share it: `.run()` pushes into its result `Vec`,
    /// `.for_each()` invokes the user closure. Neither constructs a per-item
    /// `Vec` — the earlier `insert()`-returning-a-`Vec` shape cost a
    /// `malloc` + `free` per item purely to move one value, which at 100 k+
    /// items dominated the ordered collector's cost.
    pub fn insert_into(&mut self, seq: u64, item: T, sink: &mut impl FnMut(T)) {
        // Fast path: item is exactly next expected and nothing is buffered.
        // `len == 0` implies every slot is unoccupied (occupied slots are
        // counted by `len`), so this scalar guard subsumes a defensive
        // `!slots[idx].occupied` read — and with the lazy slot array an
        // in-order stream never allocates it at all. Nothing can be flushable,
        // so the flush pass is skipped too.
        if seq == self.next_expected && self.len == 0 {
            sink(item);
            self.next_expected += 1;
            return;
        }
        self.ensure_slots();
        let tag = seq.wrapping_add(1);
        // Slow path: out-of-order arrival, or in-order arrival while gaps
        // are still buffered (the slot write + read-back is the 3 stores + 2
        // loads + len bookkeeping the fast path avoids). Kept in the
        // historical single-pass shape — one index computation, one tag
        // load, one store set on the hot path. Measurement note (2026-10,
        // sharded_term/single_ordered_cpu): this drain is bistable on the
        // bench box (~29 ms vs ~80 ms rounds, both values reproduced from
        // UNCHANGED binaries minutes apart), which masqueraded as +26 %/+
        // 74 % regressions in cross-session criterion runs; a same-binary
        // env-knob A/B (occupancy-sized vs full pre-size, 4 interleaved
        // rounds) measured +2.5 % noise — the growth cascade is below the
        // noise floor, and this shape matches the pre-growth codegen.
        //
        // `seq as usize` is safe across pointer widths: `& self.mask` only
        // keeps the low log2(capacity) bits, so truncation on 32-bit
        // targets is harmless (capacity is always < 2³²).
        #[allow(clippy::cast_possible_truncation)]
        let mut idx = (seq as usize) & self.mask;
        if self.slots[idx].tag != UNOCCUPIED {
            if self.slots[idx].tag == tag {
                // Duplicate seq (single-item-per-seq contract, e.g. `expand`
                // combined with `ordered` — rejected by the streaming
                // runner, but this is a public type). The older item is
                // dropped and counted, keeping the collector's
                // `emitted + dropped == expected` accounting closed.
                debug_assert!(
                    false,
                    "duplicate seq {seq} — ReorderBuffer is single-item-per-seq; use without \
                     `expand`"
                );
            } else if self.grow_for(seq) {
                // Occupied by a different seq: the live span reached the
                // capacity (two seqs `capacity` apart alias one slot).
                // Grown — re-map `seq`'s slot at the new capacity.
                #[allow(clippy::cast_possible_truncation)]
                let new_idx = (seq as usize) & self.mask;
                idx = new_idx;
            }
            // No debug_assert on the at-ceiling overwrite below: the
            // ceiling is caller policy (see `OrderedWindow`), violations
            // degrade gracefully (drop + count), and the unit tests
            // exercise that path deliberately.
            if self.slots[idx].tag != UNOCCUPIED {
                // At the pinned ceiling (`grow_for` returned false — the
                // capacity, and thus `idx`, is unchanged) or a duplicate
                // seq: overwrite the older aliased item in place.
                self.dropped += 1;
                // SAFETY: occupied slot holds an init item (tag checked).
                unsafe { self.slots[idx].item.assume_init_drop() };
                self.len -= 1;
            }
        }
        let slot = &mut self.slots[idx];
        slot.tag = tag;
        slot.item.write(item);
        self.len += 1;
        self.flush_ready_into(sink);
    }

    /// Grow the slot array until `seq` maps to a free slot, or `max_capacity`
    /// is reached. Returns whether `seq`'s slot is free on return.
    ///
    /// Rehash soundness: occupied seqs are pairwise distinct modulo the
    /// current capacity (inductively — any alias that would break it
    /// triggered this growth instead of an overwrite), and distinct mod `c`
    /// implies distinct mod `2c` (congruence mod `2c` implies congruence
    /// mod `c`), so re-placing each occupied slot at `seq & new_mask` never
    /// collides. Doubling also keeps the amortized growth cost O(1) per
    /// insert.
    fn grow_for(&mut self, seq: u64) -> bool {
        loop {
            if self.mask + 1 >= self.max_capacity {
                // At the pinned ceiling — cannot grow further.
                #[allow(clippy::cast_possible_truncation)]
                return self.slots[(seq as usize) & self.mask].tag == UNOCCUPIED;
            }
            self.double();
            #[allow(clippy::cast_possible_truncation)]
            let idx = (seq as usize) & self.mask;
            if self.slots[idx].tag == UNOCCUPIED {
                return true;
            }
            // Still aliased (e.g. occupied seqs `capacity` and
            // `2·capacity` apart from `seq`): keep doubling.
        }
    }

    /// Double the slot array, re-placing occupied slots at their new
    /// indices. `Slot` moves are byte copies of the `MaybeUninit` payload;
    /// unoccupied source slots carry no item and are discarded.
    fn double(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let new_cap = (self.mask + 1) * 2;
        let mut slots: Vec<Slot<T>> = (0..new_cap)
            .map(|_| Slot {
                tag: UNOCCUPIED,
                item: MaybeUninit::uninit(),
            })
            .collect();
        for slot in old {
            if slot.tag != UNOCCUPIED {
                // seq = tag − 1 (wrapping); truncation is harmless as in
                // `insert_into`.
                #[allow(clippy::cast_possible_truncation)]
                let idx = slot.tag.wrapping_sub(1) as usize & (new_cap - 1);
                slots[idx] = slot;
            }
        }
        self.slots = slots;
        self.mask = new_cap - 1;
    }

    /// Insert `item` and return any newly-contiguous run as a `Vec`.
    ///
    /// Convenience wrapper (e.g. tests); prefer [`insert_into`](Self::insert_into)
    /// on hot paths to avoid the per-call allocation.
    pub fn insert(&mut self, seq: u64, item: T) -> Vec<T> {
        let mut ready = Vec::new();
        self.insert_into(seq, item, &mut |item| ready.push(item));
        ready
    }

    fn flush_ready_into(&mut self, sink: &mut impl FnMut(T)) {
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
            // One 8-byte tag read covers both the occupied flag and the seq
            // match (`tag == next_expected + 1`).
            if self.slots[idx].tag != self.next_expected.wrapping_add(1) {
                break;
            }
            let slot = &mut self.slots[idx];
            // SAFETY: slot is occupied and init (tag matched above).
            let item = unsafe { slot.item.assume_init_read() };
            slot.tag = UNOCCUPIED;
            self.len -= 1;
            self.next_expected += 1;
            sink(item);
        }
    }

    /// Flush every buffered item, sorted by seq. Cold path (once per run,
    /// at close). Single allocation: occupied slots are compacted to the
    /// front and sorted in place (by tag), then read into the returned
    /// `Vec` — the earlier collect-`Vec<(u64, T)>`-then-`collect()` shape
    /// paid a second heap allocation purely to carry the seq through the
    /// sort.
    pub fn flush_remaining(&mut self) -> Vec<T> {
        // Compact occupied slots to the front. Swap-based so every slot
        // stays a valid value — moving out of a `Vec` element would leave
        // an uninitialized hole behind.
        let mut write = 0;
        for read in 0..self.slots.len() {
            if self.slots[read].tag != UNOCCUPIED {
                self.slots.swap(write, read);
                write += 1;
            }
        }
        debug_assert_eq!(write, self.len);
        // Sort by tag (== seq + 1). `Slot` moves are byte copies of the
        // `MaybeUninit` payload — nothing is dropped here.
        self.slots[..write].sort_unstable_by_key(|s| s.tag);
        let mut out = Vec::with_capacity(write);
        for slot in &mut self.slots[..write] {
            // SAFETY: occupied and init — `tag != UNOCCUPIED` until the
            // read below clears it.
            out.push(unsafe { slot.item.assume_init_read() });
            slot.tag = UNOCCUPIED;
        }
        self.len = 0;
        out
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Current slot capacity (power of two). The array itself materializes
    /// lazily on the first out-of-order arrival (see [`Self::ensure_slots`]);
    /// this reports the reserved capacity regardless.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[must_use]
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    /// Items dropped by occupied-slot overwrites so far (span overflow at
    /// the pinned `max_capacity`, or a duplicate seq). Release builds
    /// expose the count without panicking; debug builds additionally assert
    /// at the duplicate-seq drop site.
    #[must_use]
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    pub fn reset(&mut self) {
        for slot in &mut self.slots {
            if slot.tag != UNOCCUPIED {
                unsafe { slot.item.assume_init_drop() };
                slot.tag = UNOCCUPIED;
            }
        }
        self.len = 0;
        self.next_expected = 0;
        self.dropped = 0;
    }
}

impl<T> Drop for ReorderBuffer<T> {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if slot.tag != UNOCCUPIED {
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
            buf.insert_into(i, i32::try_from(i * 10).unwrap(), &mut |item| {
                out.push(item);
            });
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
        buf.insert_into(2, 30, &mut |item| out.push(item)); // buffered
        buf.insert_into(3, 40, &mut |item| out.push(item)); // buffered
        assert_eq!(out, Vec::<i32>::new());
        buf.insert_into(0, 10, &mut |item| out.push(item)); // fast path — immediately emitted
        assert_eq!(out, vec![10]);
        buf.insert_into(1, 20, &mut |item| out.push(item)); // fast path; flushes 2, 3
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
        // A wrapped-window item: seq 17 aliases slot 1 (17 & 15), which was
        // already flushed — its slot index (1) is lower than the in-window
        // items' (3, 5), so sorting must order by seq, not slot index.
        buf.insert(17, 170);
        let remaining = buf.flush_remaining();
        assert_eq!(remaining, vec![40, 50, 170]);
        assert!(buf.is_empty());
        assert_eq!(buf.next_expected(), 2);
    }

    #[test]
    fn test_capacity_overflow() {
        let mut buf = ReorderBuffer::<i32>::new(2);
        assert_eq!(buf.insert(5, 50), Vec::<i32>::new());
        assert_eq!(buf.insert(3, 30), Vec::<i32>::new());
        assert_eq!(buf.insert(1, 10), Vec::<i32>::new());
        assert!(buf.len() <= 2);
    }

    /// Window smaller than the reorder span: seqs 3 and 1 alias slot 1 of a
    /// 2-slot window (3 & 1 == 1 & 1 == 1). The older item (seq 3) must be
    /// counted as dropped, and the accounting `emitted + dropped == inserted`
    /// must close — the invariant the ordered collector validates per run.
    #[test]
    fn test_alias_drop_is_counted() {
        let mut buf = ReorderBuffer::<i32>::new(2);
        let mut out = Vec::new();
        let inserted = 3;
        buf.insert_into(0, 10, &mut |i| out.push(i)); // fast path, emitted
        buf.insert_into(3, 30, &mut |i| out.push(i)); // buffered in slot 1
        assert_eq!(buf.dropped(), 0);
        buf.insert_into(1, 20, &mut |i| out.push(i)); // aliases seq 3 — old item dropped
        assert_eq!(buf.dropped(), 1);
        for item in buf.flush_remaining() {
            out.push(item);
        }
        // seq 3's item (30) is gone; 0, 1 were emitted; 3 never re-inserted.
        assert_eq!(out, vec![10, 20]);
        assert_eq!(out.len() + buf.dropped(), inserted);
        assert!(buf.is_empty());
    }

    /// Growth absorbs a span beyond the initial capacity: nothing is
    /// dropped, every item is emitted in seq order, and the capacity ends
    /// at the smallest power of two that held the span.
    #[test]
    fn test_growth_absorbs_span_overflow() {
        let mut buf = ReorderBuffer::<i32>::with_max(2, 1 << 10);
        let mut out = Vec::new();
        // Skip seq 0, buffer seqs 1..=5 (span 5 > capacity 2 → doubling).
        for seq in 1..=5u64 {
            buf.insert_into(seq, i32::try_from(seq).unwrap() * 10, &mut |i| {
                out.push(i);
            });
        }
        assert_eq!(buf.dropped(), 0);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.capacity(), 8);
        // Close the gap: everything flushes in order.
        buf.insert_into(0, 0, &mut |i| out.push(i));
        assert_eq!(out, vec![0, 10, 20, 30, 40, 50]);
        assert!(buf.is_empty());
        assert_eq!(buf.dropped(), 0);
    }

    /// Growth may need several doublings for one insert: occupied seqs at
    /// `capacity`-strides from the arriving seq re-alias at every
    /// intermediate size (distinct mod `c` does not imply distinct mod
    /// `2c` the other way), so `grow_for` must keep doubling until the
    /// arriving seq's slot is free.
    #[test]
    fn test_growth_repeated_doubling() {
        let mut buf = ReorderBuffer::<i32>::with_max(2, 1 << 10);
        let mut out = Vec::new();
        // Occupied seqs 1, 5 (5 & 1 == 1: aliased at cap 2, grew to 4).
        buf.insert_into(1, 10, &mut |i| out.push(i));
        buf.insert_into(5, 50, &mut |i| out.push(i));
        // 5 & 1 aliases seq 1 at cap 2 → doubles to 4 (5 & 3 == 1, still
        // aliased) → 8 (slot 5 free).
        assert_eq!(buf.capacity(), 8);
        // Arriving seq 9: 9 & 3 == 1 == 1 & 3 → still aliased at 4, must
        // grow to 8 (9 & 7 == 1, 1 & 7 == 1 → aliased again) … to 16.
        buf.insert_into(9, 90, &mut |i| out.push(i));
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.dropped(), 0);
        assert_eq!(buf.len(), 3);
        // Release the prefix: 0, 1 flush in order; the flush stops at the
        // seq-2 gap, leaving 5 and 9 buffered (delivered by
        // `flush_remaining` below).
        buf.insert_into(0, 0, &mut |i| out.push(i));
        assert_eq!(out, vec![0, 10]);
        assert_eq!(buf.len(), 2, "seqs 5 and 9 stay buffered behind the gap");
        let mut tail = buf.flush_remaining();
        out.append(&mut tail);
        assert_eq!(out, vec![0, 10, 50, 90]);
        assert!(buf.is_empty());
        assert_eq!(buf.dropped(), 0);
    }

    /// `with_max` pins the ceiling: a span beyond it drops (and counts) the
    /// older item exactly like the fixed-capacity buffer — the memory knob
    /// `StreamPipe::with_reorder_window` relies on.
    #[test]
    fn test_growth_stops_at_max() {
        let mut buf = ReorderBuffer::<i32>::with_max(2, 2);
        let mut out = Vec::new();
        let inserted = 3;
        buf.insert_into(0, 10, &mut |i| out.push(i)); // fast path, emitted
        buf.insert_into(3, 30, &mut |i| out.push(i)); // buffered in slot 1
        assert_eq!(buf.dropped(), 0);
        buf.insert_into(1, 20, &mut |i| out.push(i)); // aliases seq 3 at the ceiling
        assert_eq!(buf.capacity(), 2, "no growth past max_capacity");
        assert_eq!(buf.dropped(), 1);
        for item in buf.flush_remaining() {
            out.push(item);
        }
        assert_eq!(out, vec![10, 20]);
        assert_eq!(out.len() + buf.dropped(), inserted);
    }

    /// `max_capacity` below the initial capacity is clamped up: the ceiling
    /// can never sit below the starting size (growth within `[initial,
    /// max]` is then empty, i.e. disabled).
    #[test]
    fn test_with_max_clamped_to_initial() {
        let buf = ReorderBuffer::<i32>::with_max(16, 2);
        assert_eq!(buf.capacity(), 16);
    }

    /// The slot array stays unallocated while arrivals are in order: the
    /// whole reorder machinery is two integers and an empty `Vec`.
    #[test]
    fn test_lazy_slots_in_order() {
        let mut buf = ReorderBuffer::<i32>::new(1 << 20);
        let mut out = Vec::new();
        for i in 0..1000u64 {
            buf.insert_into(i, i32::try_from(i).unwrap(), &mut |item| out.push(item));
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
            buf.insert_into(i, i32::try_from(i).unwrap(), &mut |item| out.push(item));
        }
        assert!(buf.slots.is_empty());
        buf.insert_into(52, 520, &mut |item| out.push(item)); // out of order → allocate
        let cap = buf.slots.len();
        assert!(cap >= 16);
        buf.insert_into(50, 500, &mut |item| out.push(item)); // fills the gap
        buf.insert_into(51, 510, &mut |item| out.push(item)); // flushes 50..=52
        assert_eq!(buf.slots.len(), cap, "no reallocation on later reorders");
        buf.insert_into(53, 530, &mut |item| out.push(item)); // in-order again (len == 0 fast path)
        assert_eq!(out.iter().copied().skip(50).collect::<Vec<_>>(), vec![
            500, 510, 520, 530
        ]);
    }

    /// The `tag` packing must keep a slot at `max(8, size_of::<T>())` bytes
    /// — the whole point of folding `occupied` into the tag (16 B vs 24 B
    /// for `u64` items, +50 % window density). Guards against a field
    /// re-introduction regressing the layout.
    #[test]
    fn test_slot_density() {
        // 8 B tag + 8 B item, no padding — the `seq + bool` layout cost 24 B.
        assert_eq!(size_of::<Slot<u64>>(), 16);
    }
}
