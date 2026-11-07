//! Verifies `pipe_range`'s zero-materialization contract with a counting
//! global allocator (own test binary — the allocator is process-global, same
//! discipline as `expand_alloc.rs` / `submit_batch_alloc.rs`).
//!
//! `pipe_range(..).map(..).collect()` may allocate the OUTPUT buffer (N × 8
//! bytes for `u64` outputs) but never an input buffer: the items are
//! generated in the leaves. The old `pipe(0..n)` path allocates both
//! (input `Vec` + output `Slots`), i.e. 2 allocations of N × 8; the
//! generation core must perform exactly 1. The filter-chain twin (todo #46)
//! holds the same contract on the merge-tree core, whose output reaches
//! N slots through per-leaf `Vec` merges — counted at `>= BUF_SIZE` so the
//! assertion is independent of the tree's exact capacity growth.
//!
//! One test function covering both cores on purpose: the counter is
//! process-global, and two `#[test]`s run concurrently in one process, so a
//! second test's output buffer could land inside the first's window.

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
        if layout.size() >= BUF_SIZE {
            BUF_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BUF_SIZE {
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
    // Filter-free core: the output `Slots` is the only N-sized allocation.
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

    // Filter-chain generation core: a keep-all filter forces the survivor
    // output to N slots, so the merge tree's output side accounts for exactly
    // one `>= N × 8` allocation; the materialized fallback's input `Vec`
    // would be a second.
    BUF_SIZED.store(0, Ordering::Relaxed);
    let r: Vec<u64> = pipe_range(0..N)
        .map(|i: usize| i as u64 + 1)
        .filter(|_: &u64| true)
        .collect();
    assert_eq!(r.len(), N);
    assert_eq!(r[0], 1);
    assert_eq!(r[N - 1], N as u64);
    assert_eq!(
        BUF_SIZED.load(Ordering::Relaxed),
        1,
        "only the output side reaches N slots — an input buffer would be a second >=N-sized one"
    );
}
