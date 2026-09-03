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
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
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
        let len = vec.len();
        let box_t: Box<[T]> = vec.into_boxed_slice();
        // SAFETY: `[T]` and `[UnsafeCell<MaybeUninit<T>>]` are layout-identical:
        // `UnsafeCell` is `#[repr(transparent)]` over its field, and
        // `MaybeUninit<T>` has the same size/align/ABI as `T`.
        let ptr = Box::into_raw(box_t).cast::<UnsafeCell<MaybeUninit<T>>>();
        let buf = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) };
        Slots { buf }
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
        Slots {
            buf: v.into_boxed_slice(),
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
        debug_assert!(start <= end && end <= self.buf.len());
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
        debug_assert!(start <= end && end <= self.buf.len());
        // SAFETY: same layout argument as `as_slice`; interior mutability via
        // `UnsafeCell` lets us produce `&mut [T]` from `&self`. The slice is
        // exclusively ours for the leaf's lifetime (disjoint-index discipline).
        unsafe {
            let ptr = self.buf.as_ptr().cast::<T>().add(start).cast_mut();
            std::slice::from_raw_parts_mut(ptr, end - start)
        }
    }

    /// Reclaim the buffer as a `Vec<T>` without dropping any slot. All slots
    /// must be init and owned by the caller.
    pub(super) fn into_vec(self) -> Vec<T> {
        let len = self.buf.len();
        let ptr = Box::into_raw(self.buf).cast::<T>();
        // SAFETY: layout-identical to `[T]` (see `from_vec`); all slots are init
        // by contract. Rebuild as a boxed slice and convert via the idiomatic
        // `Box::into_vec` (cap == len, exactly matching the boxed slice).
        let boxed: Box<[T]> =
            unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) };
        boxed.into_vec()
    }
}
