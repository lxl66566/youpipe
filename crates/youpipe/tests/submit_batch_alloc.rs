//! Verifies `ComputePool::submit_batch`'s allocation contract with a counting
//! global allocator (own test binary — the allocator is process-global, so
//! allocation-sensitive assertions cannot share a binary with unrelated
//! tests, same discipline as `expand_alloc.rs`).
//!
//! `submit_batch` streams `JobRef`s straight into the injector's
//! segment-reserving `push_n` (`I::IntoIter: ExactSizeIterator`), so the
//! batch must not allocate an intermediate `Vec<JobRef>` — one allocation of
//! exactly `BATCH × 16` bytes on the old `collect()` path.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

use youpipe::ComputePool;

/// Batch size chosen so the removed intermediate `Vec<JobRef>` would be
/// exactly 128 × 16 = 2048 bytes — a size neither the per-job `HeapJob`
/// boxing (closure-sized), the queue's block segments (31 × 24 ≈ 744 B), nor
/// the test's own `Vec<u64>` output (1024 B) produces.
const BATCH: usize = 128;
const VEC_SIZE: usize = BATCH * 16;

struct CountingAlloc;

static VEC_SIZED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == VEC_SIZE {
            VEC_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() == VEC_SIZE {
            VEC_SIZED.fetch_add(1, Ordering::Relaxed);
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
fn submit_batch_allocates_no_intermediate_vec() {
    let pool = ComputePool::new(4);
    let (tx, rx) = mpsc::channel();
    VEC_SIZED.store(0, Ordering::Relaxed);
    // A lazy `map` over a range keeps `ExactSizeIterator` (`usize` — std's
    // `Range<u64>` lacks the impl, pointer-width types have it) — the bound
    // real callers (stream stage spawn) rely on.
    let jobs = (0..BATCH).map(|i: usize| {
        let tx = tx.clone();
        move || {
            tx.send(i as u64).unwrap();
        }
    });
    pool.submit_batch(jobs);
    drop(tx);

    let mut got = Vec::with_capacity(BATCH);
    for _ in 0..BATCH {
        got.push(
            rx.recv_timeout(Duration::from_secs(10))
                .expect("a batch job never ran"),
        );
    }
    got.sort_unstable();
    assert_eq!(got, (0..BATCH).map(|i| i as u64).collect::<Vec<_>>());

    assert_eq!(
        VEC_SIZED.load(Ordering::Relaxed),
        0,
        "submit_batch must not collect an intermediate Vec<JobRef>"
    );
}
