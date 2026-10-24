//! Allocation contract of the aggregation terminals on the streaming
//! topology (own test binary — the allocator is process-global). A
//! materialising fallback would build an n-item output `Vec` (one ≥1 MiB
//! allocation for the 512K-item shape); the reducing collector keeps output
//! memory O(1). The `.run()` control on the same chain must keep showing the
//! big allocation, proving the counter detects materialisation.
//!
//! Counting is filtered by size (≥1 MiB) so pool worker stacks, channel
//! rings, and reorder windows — all small or sub-megabyte — stay out of the
//! measurement. Atomics-only counting: a locking histogram self-deadlocks
//! (see `tests/expand_alloc.rs`).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use youpipe::{CancellationToken, stream};

/// 512K × 8-byte outputs = a 4 MiB output `Vec` on the materialising
/// path — still comfortably above the 1 MiB counter threshold below.
const N: u64 = 1 << 19;
const BIG: usize = 1 << 20;

struct CountingAlloc;

static BIG_ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BIG {
            BIG_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BIG {
            BIG_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= BIG && layout.size() < BIG {
            BIG_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

fn bump(x: u64) -> u64 {
    x * 2 + 1
}

fn big_allocs() -> u64 {
    BIG_ALLOCS.load(Ordering::Relaxed)
}

#[test]
#[allow(clippy::cast_possible_truncation)] // test sizes fit usize everywhere
fn streaming_aggregations_no_output_materialisation() {
    let cancel = CancellationToken::new();
    let items: Vec<u64> = (0..N).collect();

    // Warm up the global pool and channels.
    let _ = stream(items.clone())
        .with_cancel(cancel.clone())
        .stage(bump)
        .run();

    let input = items.clone();
    let before = big_allocs();
    let s: u64 = stream(input).with_cancel(cancel.clone()).stage(bump).sum();
    let sum_delta = big_allocs() - before;
    assert_eq!(s, (0..N).map(bump).sum::<u64>());
    assert_eq!(
        sum_delta, 0,
        "streaming sum materialised output ({sum_delta} allocations ≥1 MiB)"
    );

    let input = items.clone();
    let before = big_allocs();
    let c = stream(input)
        .with_cancel(cancel.clone())
        .stage(bump)
        .count();
    let count_delta = big_allocs() - before;
    assert_eq!(c, N as usize);
    assert_eq!(
        count_delta, 0,
        "streaming count materialised output ({count_delta} allocations ≥1 MiB)"
    );

    let input = items.clone();
    let before = big_allocs();
    let r = stream(input).with_cancel(cancel.clone()).stage(bump).min();
    let reduce_delta = big_allocs() - before;
    assert_eq!(r, Some(bump(0)));
    assert_eq!(
        reduce_delta, 0,
        "streaming reduce materialised output ({reduce_delta} allocations ≥1 MiB)"
    );

    // Control: the Vec-materialising terminal on the identical chain must
    // trip the counter.
    let input = items;
    let before = big_allocs();
    let v = stream(input).with_cancel(cancel).stage(bump).run();
    let run_delta = big_allocs() - before;
    assert_eq!(v.len(), N as usize);
    assert!(
        run_delta >= 1,
        "run() control lost its output allocation — the counter no longer detects materialisation"
    );
}
