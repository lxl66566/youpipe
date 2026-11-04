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

#![cfg(feature = "tokio-runtime")]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use youpipe::{AsyncNodeOptions, AsyncStageOptions, graph, stream};

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
    // Miri /4: only the parking pattern matters (waker-class alloc counts
    // against the <=256 bound), and each item burns 2000 interpreted
    // iterations in the slow stage — full n dominates the miri run.
    let n = if cfg!(miri) {
        128_u64
    } else {
        512_u64
    };
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
        "waker-class allocations {pipeline} exceed the wiring bound; a parking loop probably \
         regressed to per-call recv()/send()"
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
        "plain recv() loop allocated only {plain} waker nodes; the workload does not actually \
         park, so the pipeline bound above proves nothing"
    );
}

/// Graph-side twin: the async fan-in anchor (`AsyncFanIn`, two sources into
/// one `node_async`) parks its any-of-N combinator on every empty-input
/// gap, and the select cycle drops the losing futures — with cancel-path
/// slot reuse those drops retire into the slots, so the whole run stays at
/// the per-endpoint floor (node tasks x 2 input slots + the bridge), not
/// one node per park episode. Loose bound: the 48-byte bucket also
/// catches the anchor's per-episode `futs` Vec and debug-build runtime
/// noise; a wiring regression to per-episode nodes adds hundreds.
#[test]
fn graph_async_anchor_waker_alloc_wiring() {
    let n = 256_u64;
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let before = waker_class();
    let mut g = graph();
    let items: Vec<u64> = (0..n).collect();
    let all_items = Arc::new(items);
    let items = Arc::clone(&all_items);
    let source = move || {
        let k = NEXT.fetch_add(1, Ordering::Relaxed);
        std::future::ready((k < n).then(|| items[usize::try_from(k).expect("k < n fits usize")]))
    };
    let s1 = g.source_async("s1", source);
    let items2 = Arc::clone(&all_items);
    let source2 = move || {
        let k = NEXT2.fetch_add(1, Ordering::Relaxed);
        std::future::ready((k < n).then(|| items2[usize::try_from(k).expect("k < n fits usize")]))
    };
    let s2 = g.source_async("s2", source2);
    let m = g.node_async_with(
        "m",
        AsyncNodeOptions::new().io_concurrency(4).buffer(16),
        |x: u64| async move { x + 1 },
    );
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(s1, m).edge(s2, m).edge(m, out);
    g.run();
    let allocs = waker_class() - before;
    // 2 sources x (sum(0..n) + n items' +1 from the node) = n * (n + 1).
    assert_eq!(total.load(Ordering::SeqCst), n * (n + 1));
    println!("graph anchor (2x{n} items, 4 tasks): waker-class allocs = {allocs}");
    assert!(
        allocs <= 512,
        "waker-class allocations {allocs} exceed the anchor wiring bound; the          any-of-N \
         anchor probably regressed to per-episode node allocation"
    );
}

static NEXT: AtomicU64 = AtomicU64::new(0);
static NEXT2: AtomicU64 = AtomicU64::new(0);
