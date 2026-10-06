use std::{cell::UnsafeCell, mem::MaybeUninit};

// ── Slots: index-addressable buffer for zero-copy parallel map ──

/// Boxed slot array backing the range-based parallel map.
///
/// Each slot is `UnsafeCell<MaybeUninit<T>>`. The `MaybeUninit` layer
/// suppresses item drops when the box itself is dropped, so the box's `Drop`
/// only frees memory — every slot that holds a live `T` must be dropped by the
/// caller before the buffer goes out of scope (the recursion in
/// [`par_index_rec`] guarantees this on both the success and panic paths).
///
/// Ranges processed by different worker threads are disjoint, so non-atomic
/// `read`/`write`/`drop_range` on disjoint indices is sound. `Sync` is sound
/// because items (`T: Send`) may legitimately move between threads.
pub(crate) struct Slots<T> {
    /// The WHOLE allocation, `buf.len() == capacity` slots. Constructors
    /// guarantee the box's length equals the allocation's item count, so the
    /// implicit `Drop` (a plain free) uses the true layout even when the
    /// usable `len` is smaller — a shorter fat pointer would deallocate a
    /// shrunken layout over the full allocation, which is why the spare
    /// capacity cannot be hidden behind a plain `Box<[..]>` of `len` items.
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Usable slot count (`<= buf.len()`): all init for [`Slots::from_vec`]
    /// inputs, all uninit for [`Slots::uninit`]. Range ops never address
    /// slots past `len` — under `from_vec` those are the input `Vec`'s spare
    /// capacity.
    len: usize,
}

// SAFETY: access is governed by the disjoint-index discipline documented on
// `Slots`. Items of type `T` may cross threads, so we require `T: Send`.
unsafe impl<T: Send> Send for Slots<T> {}
unsafe impl<T: Send> Sync for Slots<T> {}

impl<T> Slots<T> {
    /// Take ownership of a `Vec<T>` and re-interpret it as an all-init slot
    /// array. Items are not moved — only the allocation's type is
    /// reinterpreted.
    pub(super) fn from_vec(vec: Vec<T>) -> Self {
        // `Vec::into_raw_parts` semantics, by hand (the std API is still
        // unstable): keep the allocation AND its capacity. `into_boxed_slice`
        // would shrink-to-fit (realloc + memcpy + free) whenever
        // `cap > len` — a full extra data move the collect cores never
        // needed (review P-7).
        let mut vec = std::mem::ManuallyDrop::new(vec);
        let len = vec.len();
        let cap = vec.capacity();
        let ptr = vec.as_mut_ptr().cast::<UnsafeCell<MaybeUninit<T>>>();
        debug_assert!(len <= cap);
        // SAFETY: `[T]` and `[UnsafeCell<MaybeUninit<T>>]` are layout-identical:
        // `UnsafeCell` is `#[repr(transparent)]` over its field, and
        // `MaybeUninit<T>` has the same size/align/ABI as `T`. The allocation
        // holds `cap` items (Vec's contract), so the box owns exactly it;
        // slots `[len, cap)` are the Vec's uninitialized spare capacity.
        let buf = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, cap)) };
        Slots { buf, len }
    }

    /// Allocate an all-uninit slot array of length `n`.
    ///
    /// Uses `set_len` after `with_capacity` so we never touch the backing
    /// memory — the slots are `MaybeUninit`, so uninitialized is a valid state.
    /// A `.collect()`-based init here would be a sequential O(n) loop that
    /// dominates lightweight workloads (measured: ~2 ms for 1 M slots).
    pub(super) fn uninit(n: usize) -> Self {
        let mut v: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(n);
        // SAFETY: the capacity is `n` and `MaybeUninit<T>` is valid uninitialized,
        // so the slots do not need to be written before being read via `read`.
        unsafe { v.set_len(n) };
        // `with_capacity` guarantees `capacity() >= n` (== n in practice —
        // the request is for exactly n); taking the full capacity keeps the
        // box length equal to the allocation.
        let len = v.len();
        Slots {
            buf: v.into_boxed_slice(),
            len,
        }
    }

    /// Drop slots `[start, end)`. All of them must be init.
    ///
    /// # Safety
    ///
    /// Every slot in `[start, end)` must hold a live `T`. Only valid for ranges
    /// produced by operations that never filter (see `RangeOp::MAY_FILTER`).
    #[inline]
    pub(super) unsafe fn drop_range(&self, start: usize, end: usize) {
        debug_assert!(start <= end && end <= self.len);
        for i in start..end {
            unsafe { (*self.buf.get_unchecked(i).get()).assume_init_drop() };
        }
    }

    /// View slots `[start, end)` as an all-init `&[T]` slice.
    ///
    /// Used by the leaf loop so LLVM sees a plain slice reference (noalias
    /// guarantees via Rust's borrow rules) instead of `&Slots` with
    /// `UnsafeCell` interior-mutability — that aliasing opacity is what stalls
    /// the auto-vectorizer and inflates the 1 M warm `par_map` cost ~2.6×.
    ///
    /// # Safety
    ///
    /// * Slots `[start, end)` must all be init.
    /// * Caller must ensure no `&mut` alias to the same range is live.
    #[inline]
    pub(super) unsafe fn as_slice(&self, start: usize, end: usize) -> &[T] {
        debug_assert!(start <= end && end <= self.len);
        // SAFETY: `[UnsafeCell<MaybeUninit<T>>]` is layout-identical to `[T]`;
        // caller guarantees the range is init and exclusively accessible.
        unsafe {
            let ptr = self.buf.as_ptr().cast::<T>().add(start);
            std::slice::from_raw_parts(ptr, end - start)
        }
    }

    /// View slots `[start, end)` as an all-uninit `&mut [T]` slice.
    ///
    /// Counterpart to [`Slots::as_slice`] for the output buffer. The caller is
    /// responsible for fully writing the slice before anyone reads it.
    ///
    /// # Safety
    ///
    /// * Slots `[start, end)` must all be uninit (no `T` to drop).
    /// * Caller must ensure no alias to the same range is live.
    #[inline]
    #[allow(clippy::mut_from_ref)] // Governed by Slots' disjoint-index discipline
    pub(super) unsafe fn as_mut_slice(&self, start: usize, end: usize) -> &mut [T] {
        debug_assert!(start <= end && end <= self.len);
        // SAFETY: same layout argument as `as_slice`; interior mutability via
        // `UnsafeCell` lets us produce `&mut [T]` from `&self`. The slice is
        // exclusively ours for the leaf's lifetime (disjoint-index discipline).
        unsafe {
            let ptr = self.buf.as_ptr().cast::<T>().add(start).cast_mut();
            std::slice::from_raw_parts_mut(ptr, end - start)
        }
    }

    /// Raw base pointer of the slot array, mutable through interior
    /// mutability — for whole-buffer moves that need `ptr::copy` (memmove)
    /// semantics inside this one buffer, where the destination and source
    /// ranges can overlap and slice methods (`copy_from_slice` is
    /// `copy_nonoverlapping`) would be UB. Carries no live borrow across the
    /// call (raw pointers have no tags to disable under Tree Borrows).
    #[inline]
    #[allow(clippy::mut_from_ref)] // Same governance as `as_mut_slice`
    pub(super) fn base_ptr(&self) -> *mut T {
        self.buf.as_ptr().cast::<T>().cast_mut()
    }

    /// Reclaim the buffer as a `Vec<T>` without dropping any slot. All slots
    /// must be init and owned by the caller.
    pub(super) fn into_vec(self) -> Vec<T> {
        // Every usable slot is init by contract — `into_vec_with_len` with
        // the full usable length.
        let len = self.len;
        self.into_vec_with_len(len)
    }

    /// Reclaim the buffer as a `Vec<T>` of an explicit length `<=` the
    /// buffer's — the compaction twin of [`Slots::into_vec`]: the first
    /// `len` slots must be init, the tail's stale bits are never dropped and
    /// never read. The Vec keeps the full buffer capacity (len < cap).
    pub(super) fn into_vec_with_len(self, len: usize) -> Vec<T> {
        debug_assert!(len <= self.len);
        let cap = self.buf.len();
        let ptr = Box::into_raw(self.buf).cast::<T>();
        // SAFETY: layout-identical to `[T]` (see `from_vec`); the first `len`
        // usable slots are init by contract, the allocation holds `cap`
        // items (the box's length IS the capacity — `from_vec` keeps it,
        // `uninit` allocates exactly).
        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Review P-7: `from_vec` must keep a spare-capacity input's allocation
    /// (no shrink-to-fit realloc) and the capacity must survive the
    /// `into_vec` round trip — contents identical throughout. `String`
    /// payloads would catch any layout mis-reinterpretation.
    #[test]
    fn from_vec_keeps_spare_capacity() {
        let mut v: Vec<String> = Vec::with_capacity(64);
        for i in 0..10 {
            v.push(format!("item-{i}"));
        }
        let cap = v.capacity();
        assert!(cap > 10, "test needs spare capacity, got {cap}");

        let slots = Slots::from_vec(v);
        assert_eq!(slots.len, 10);
        {
            // All slots init — spot-check through the slot view.
            let s = unsafe { slots.as_slice(0, 10) };
            assert_eq!(s[0], "item-0");
            assert_eq!(s[9], "item-9");
        }
        let back = slots.into_vec();
        let expected: Vec<String> = (0..10).map(|i| format!("item-{i}")).collect();
        assert_eq!(back, expected);
        assert_eq!(back.capacity(), cap, "capacity must survive the round trip");
    }

    /// `uninit` + `into_vec` round trip: exact usable length and capacity.
    #[test]
    fn uninit_round_trip_exact() {
        let slots: Slots<u64> = Slots::uninit(8);
        assert_eq!(slots.len, 8);
        unsafe {
            let s = slots.as_mut_slice(0, 8);
            for (i, x) in s.iter_mut().enumerate() {
                *x = i as u64;
            }
        }
        let back = slots.into_vec();
        assert_eq!(back, (0..8).collect::<Vec<u64>>());
        assert_eq!(back.capacity(), 8);
    }
}
