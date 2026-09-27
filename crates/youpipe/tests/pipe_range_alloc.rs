//! Verifies `pipe_range`'s zero-materialization contract with a counting
//! global allocator (own test binary — the allocator is process-global, same
//! discipline as `expand_alloc.rs` / `submit_batch_alloc.rs`).
//!
//! `pipe_range(..).map(..).collect()` may allocate the OUTPUT buffer (N × 8
//! bytes for `u64` outputs) but never an input buffer: the items are
//! generated in the leaves. The old `pipe(0..n)` path allocates both
//! (input `Vec` + output `Slots`), i.e. 2 allocations of N × 8; the
//! generation core must perform exactly 1.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use youpipe::pipe_range;

/// 100 K `u64` = 800 KB — a size neither the chunk-job box (tens of bytes)
/// nor anything else in the run produces.
const N: usize = 100_000;
const BUF_SIZE: usize = N * 8;

struct CountingAlloc;

static BUF_SIZED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == BUF_SIZE {
            BUF_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() == BUF_SIZE {
            BUF_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

#[test]
fn pipe_range_collect_allocates_no_input_buffer() {
    BUF_SIZED.store(0, Ordering::Relaxed);
    let r: Vec<u64> = pipe_range(0..N).map(|i: usize| i as u64 + 1).collect();
    assert_eq!(r.len(), N);
    assert_eq!(r[0], 1);
    assert_eq!(r[N - 1], N as u64);
    assert_eq!(
        BUF_SIZED.load(Ordering::Relaxed),
        1,
        "exactly one N-sized allocation (the output buffer) — an input buffer would be the second"
    );
}
