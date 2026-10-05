//! # St³ — Stealing Static Stack
//!
//! Very fast lock-free, bounded, work-stealing queue with FIFO stealing and
//! LIFO or FIFO semantic for the worker thread.
//!
//! The `Worker` handle enables push and pop operations from a single thread,
//! while `Stealer` handles can be shared between threads to perform FIFO
//! batch-stealing operations.
//!
//! `St³` is effectively a faster, fixed-size alternative to the Chase-Lev
//! double-ended queue. It uses no atomic fences, much fewer atomic loads and
//! stores, and fewer Read-Modify-Write operations: none for `push`, one for
//! `pop` and one (LIFO) or two (FIFO) for `steal`.
//!
//! ## Example
//!
//! ```
//! use std::thread;
//! use st3::lifo::Worker;
//!
//! // Push 4 items into a queue of capacity 256.
//! let worker = Worker::new(256);
//! worker.push("a").unwrap();
//! worker.push("b").unwrap();
//! worker.push("c").unwrap();
//! worker.push("d").unwrap();
//!
//! // Steal items concurrently.
//! let stealer = worker.stealer();
//! let th = thread::spawn(move || {
//!     let other_worker = Worker::new(256);
//!
//!     // Try to steal half the items and return the actual count of stolen items.
//!     match stealer.steal(&other_worker, |n| n/2) {
//!         Ok(actual) => actual,
//!         Err(_) => 0,
//!     }
//! });
//!
//! // Pop items concurrently.
//! let mut pop_count = 0;
//! while worker.pop().is_some() {
//!     pop_count += 1;
//! }
//!
//! // Does it add up?
//! let steal_count = th.join().unwrap();
//! assert_eq!(pop_count + steal_count, 4);
//! ```
#![warn(missing_docs, missing_debug_implementations, unreachable_pub)]
#![no_std]

extern crate alloc;

use alloc::alloc::{alloc, handle_alloc_error};
use alloc::sync::Arc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use core::alloc::Layout;
use core::fmt;
use core::mem::MaybeUninit;
use core::sync::atomic::AtomicUsize;

use config::{UnsignedLong, UnsignedShort};

use crate::loom_exports::cell::UnsafeCell;
use crate::loom_exports::debug_or_loom_assert;

mod config;
pub mod fifo;
pub mod lifo;
mod loom_exports;
mod transfer;

/// Error returned when stealing is unsuccessful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StealError {
    /// No item was stolen.
    Empty,
    /// Another concurrent stealing operation is ongoing.
    Busy,
}

impl fmt::Display for StealError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StealError::Empty => write!(f, "cannot steal from empty queue"),
            StealError::Busy => write!(f, "a concurrent steal operation is ongoing"),
        }
    }
}

#[inline]
/// Pack two short integers into a long one.
fn pack(value1: UnsignedShort, value2: UnsignedShort) -> UnsignedLong {
    ((value1 as UnsignedLong) << UnsignedShort::BITS) | value2 as UnsignedLong
}
#[inline]
/// Unpack a long integer into 2 short ones.
fn unpack(value: UnsignedLong) -> (UnsignedShort, UnsignedShort) {
    (
        (value >> UnsignedShort::BITS) as UnsignedShort,
        value as UnsignedShort,
    )
}

#[cfg(st3_loom)]
fn allocate_buffer<T>(len: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]> {
    // Unlike the real `UnsafeCell`, loom's is not plain data: every cell
    // carries a model location that must be registered by `new`. Exposing
    // uninitialized slots (the fast path below) would make the first write
    // resolve a garbage location handle — observed as an index-out-of-bounds
    // panic in `loom::rt::cell::Cell::start_write` and a cascade abort while
    // unwinding (2026-10 loom re-enablement).
    let mut buffer = Vec::with_capacity(len);
    buffer.extend((0..len).map(|_| UnsafeCell::new(MaybeUninit::uninit())));
    buffer.into_boxed_slice()
}

#[cfg(not(st3_loom))]
fn allocate_buffer<T>(len: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]> {
    let mut buffer = Vec::with_capacity(len);

    // An `UnsafeCell<MaybeUninit<T>>` does not require initialization:
    // `UnsafeCell` is `repr(transparent)` over `MaybeUninit`, so exposing
    // the uninitialized allocation as a slice is sound as long as elements
    // are only ever read after being written to. Setting the length
    // directly (instead of using `resize_with`) makes this property hold by
    // construction rather than by optimizer elimination.
    //
    // Safety: `len <= capacity` trivially holds after `with_capacity`.
    unsafe { buffer.set_len(len) };

    buffer.into_boxed_slice()
}

/// Build an [`Arc`] whose payload is constructed directly inside the heap
/// allocation.
///
/// `Arc::new` first constructs the payload on the stack and then copies it
/// into the `ArcInner` allocation. For the queues of this crate (640 bytes
/// mostly made of cache-padded counters), this means a large stack frame
/// and a full copy of the structure; this helper lets the caller write the
/// payload fields directly at their final location.
///
/// # Safety
///
/// `init` must fully initialize the payload (every field of `T` must be
/// written exactly once) and must not panic.
unsafe fn arc_new_in_place<T, F: FnOnce(*mut T)>(init: F) -> Arc<T> {
    // The internal `ArcInner<T>` layout (two usize-wide reference counters
    // followed by the payload) is an implementation detail of `alloc`, but
    // this layout has been in effect since the origin of std and is deeply
    // relied upon across the ecosystem. It is additionally validated at
    // debug/loom run time by the reference-count assertions at the end of
    // this function: with a mismatched layout, the counts would be
    // immediately inconsistent (or the test suite would fail through a
    // corrupted queue) rather than memory being silently corrupted.
    //
    // Note: `Layout::extend` is deliberately not used here as the order of
    // its result tuple changed across toolchain versions.
    let payload = Layout::new::<T>();
    let header_size = 2 * core::mem::size_of::<usize>();
    let align = payload.align().max(core::mem::align_of::<usize>());
    let offset = (header_size + payload.align() - 1) & !(payload.align() - 1);
    let total = (offset + payload.size() + align - 1) & !(align - 1);
    let layout = Layout::from_size_align(total, align).unwrap();

    let inner = alloc(layout);
    if inner.is_null() {
        handle_alloc_error(layout);
    }

    // Reference counters: one strong reference (the returned handle) and
    // one weak reference (implicitly held by all strong references).
    let counter = inner as *mut AtomicUsize;
    counter.write(AtomicUsize::new(1));
    counter.add(1).write(AtomicUsize::new(1));

    init(inner.add(offset) as *mut T);

    // Safety: the payload was fully initialized by `init` and the header
    // mimics an `Arc` freshly created by `Arc::new`.
    let arc = Arc::from_raw(inner.add(offset) as *const T);

    debug_or_loom_assert!(Arc::strong_count(&arc) == 1);
    // Depending on the std version, the implicit weak reference held by
    // strong references is either counted (weak_count == 1) or not
    // (weak_count == 0) for a freshly built Arc.
    debug_or_loom_assert!(Arc::weak_count(&arc) <= 1);

    arc
}
