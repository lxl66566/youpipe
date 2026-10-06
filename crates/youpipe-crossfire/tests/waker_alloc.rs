//! Alloc-count regression for cross-episode async waker-node reuse
//! (youpipe P-4). Dedicated test binary so the `#[global_allocator]`
//! counters are not raced by unrelated tests.
//!
//! Two consumer loops run the same park-heavy workload over the same
//! channel shape; the only difference is the waker slot. The slot-less
//! `recv()` loop must allocate one `Arc<WakerInner>` per park episode; the
//! `recv_cached()` loop must allocate a fixed handful (one per endpoint,
//! first contended episode only).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use crossfire::{mpmc, AsyncWakerSlot};

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

const ITEMS: u64 = 64;

/// Producer slow enough that every consumer recv parks: the item arrives
/// only after the consumer has registered and committed Waiting.
async fn slow_producer(tx: crossfire::MAsyncTx<mpmc::Array<u64>>) {
    for i in 0..ITEMS {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        if tx.send(i).await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn recv_cached_allocates_per_endpoint_not_per_episode() {
    // ── slot-less baseline: one allocation per park episode ──
    // The consumer runs as a spawned task, mirroring every youpipe call
    // site. A loop that is the ROOT future of a current-thread
    // Runtime::block_on sees a different waker instance per wake
    // (will_wake mismatch), so reuse degrades to the rebuild path there:
    // correct, just without the allocation savings.
    let (tx, rx) = mpmc::bounded_async::<u64>(1);
    let producer = tokio::spawn(slow_producer(tx));
    let before = WAKER_CLASS.load(Ordering::SeqCst);
    let consumer = tokio::spawn(async move {
        let mut got = 0_u64;
        while rx.recv().await.is_ok() {
            got += 1;
        }
        got
    });
    producer.await.unwrap();
    let got = consumer.await.unwrap();
    assert_eq!(got, ITEMS);
    let plain = WAKER_CLASS.load(Ordering::SeqCst) - before;
    println!("plain recv():   waker-class allocs = {plain}");
    assert!(
        plain >= ITEMS / 2,
        "plain recv() allocated only {plain} waker nodes for {ITEMS} parks; \
         the workload does not actually park, so the cached bound below \
         would prove nothing"
    );

    // ── slot loop: one allocation for the endpoint, reuse afterwards ──
    let (tx, rx) = mpmc::bounded_async::<u64>(1);
    let producer = tokio::spawn(slow_producer(tx));
    let before = WAKER_CLASS.load(Ordering::SeqCst);
    let consumer = tokio::spawn(async move {
        let mut slot = AsyncWakerSlot::new();
        let mut got = 0_u64;
        while rx.recv_cached(&mut slot).await.is_ok() {
            got += 1;
        }
        got
    });
    producer.await.unwrap();
    let got = consumer.await.unwrap();
    assert_eq!(got, ITEMS);
    let cached = WAKER_CLASS.load(Ordering::SeqCst) - before;
    println!("recv_cached():  waker-class allocs = {cached}");
    assert!(
        cached <= 4,
        "recv_cached() allocated {cached} waker nodes for one endpoint; \
         cross-episode slot reuse regressed"
    );
}
