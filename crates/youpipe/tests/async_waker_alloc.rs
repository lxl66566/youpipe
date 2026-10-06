//! Wiring-level regression guard for cross-episode async waker-node reuse
//! (review P-4).
//!
//! The strict alloc-count A/B for the reuse mechanism itself lives in
//! `youpipe-crossfire/tests/waker_alloc.rs` (dedicated binary, quiet
//! allocator). This test guards the youpipe side: every parking loop in the
//! streaming data path (feeder bridges, async stage tasks, collectors, the
//! sharded async anchor) must go through the slot-based `recv_cached` /
//! `send_cached` paths. If a call site regresses to the per-call
//! `recv()`/`send()` futures, a parking-heavy run allocates one waker node
//! per park episode — hundreds for this workload — instead of one per
//! endpoint.
//!
//! The counting allocator buckets exactly `crossfire::ARC_WAKER_ALLOC_SIZE`
//! (the `Arc<WakerInner>` block); in a debug build the runtime's own
//! same-size allocations add a noisy ~100-200 on top, so the bound is loose
//! — it must sit well above the endpoint + incidental count and well below
//! the per-episode count. Both phases run sequentially in one test function
//! (the counters are process globals).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use youpipe::{stream, AsyncStageOptions};

static WAKER_CLASS: AtomicU64 = AtomicU64::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == crossfire::ARC_WAKER_ALLOC_SIZE {
            WAKER_CLASS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() == crossfire::ARC_WAKER_ALLOC_SIZE {
            WAKER_CLASS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

fn waker_class() -> u64 {
    WAKER_CLASS.load(Ordering::SeqCst)
}

#[test]
fn async_stage_waker_alloc_wiring() {
    // Parking-heavy: slow stage (CPU burn per item) + fast feeder — the
    // input channel fills (feeder parks per drain), the stage tasks and the
    // collector park on burst gaps. io_concurrency pinned so the endpoint
    // count stays single-digit.
    let n = 512_u64;
    let before = waker_class();
    let out = stream(0..n)
        .stage_async_with(
            AsyncStageOptions::new().io_concurrency(8).buffer(16),
            |x: u64| async move {
                let mut acc = x;
                for i in 0..2_000u64 {
                    acc = acc.wrapping_add(i.rotate_left(7) ^ x);
                }
                std::hint::black_box(acc);
                x + 1
            },
        )
        .run();
    let pipeline = waker_class() - before;
    assert_eq!(out.len() as u64, n);
    println!("pipeline (slot-based): waker-class allocs = {pipeline}");

    // Loose tripwire: endpoint count (~10 crossfire nodes) plus the noisy
    // debug-build incidental same-size traffic lands well under this; a
    // wiring regression to per-call futures allocates per park episode —
    // 500+ for this workload.
    assert!(
        pipeline <= 256,
        "waker-class allocations {pipeline} exceed the wiring bound; \
         a parking loop probably regressed to per-call recv()/send()"
    );

    // Floor proof: the same bucket over a plain per-call `recv()` loop
    // (no slot) with a slow producer must show real per-episode traffic —
    // otherwise the pipeline bound above could pass vacuously.
    let before = waker_class();
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        rt.block_on(async {
            let (tx, rx) = crossfire::mpmc::bounded_async::<u64>(1);
            let producer = tokio::spawn(async move {
                for i in 0..64_u64 {
                    for _ in 0..200 {
                        tokio::task::yield_now().await;
                    }
                    if tx.send(i).await.is_err() {
                        return;
                    }
                }
            });
            let mut got = 0_u64;
            while rx.recv().await.is_ok() {
                got += 1;
            }
            producer.await.unwrap();
            assert_eq!(got, 64);
        });
    }
    let plain = waker_class() - before;
    println!("plain recv() loop:    waker-class allocs = {plain}");
    assert!(
        plain >= 20,
        "plain recv() loop allocated only {plain} waker nodes; \
         the workload does not actually park, so the pipeline bound above \
         proves nothing"
    );
}
