use std::sync::Arc;

#[test]
fn test_compute_pool_basic() {
    let pool = youpipe::ComputePool::new(4);
    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..10 {
        let tx = tx.clone();
        pool.submit(move || {
            tx.send(i).unwrap();
        });
    }
    drop(tx);
    let results: Vec<_> = rx.iter().collect();
    assert_eq!(results.len(), 10);
}

#[test]
fn test_compute_pool_shared() {
    let pool = Arc::new(youpipe::ComputePool::new(4));
    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..100 {
        let tx = tx.clone();
        let p = pool.clone();
        p.submit(move || {
            let mut sum = 0u64;
            // Miri: the inner arithmetic depth is irrelevant to the pool
            // machinery under test — shrink it 50x.
            let inner: u32 = if cfg!(miri) { 20 } else { 1_000 };
            for j in 0..inner {
                sum = sum.wrapping_add(j as u64);
            }
            tx.send((i, sum)).unwrap();
        });
    }
    drop(tx);
    let results: Vec<_> = rx.iter().collect();
    assert_eq!(results.len(), 100);
}

#[test]
fn test_compute_pool_many_small_tasks() {
    let pool = Arc::new(youpipe::ComputePool::new(4));
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let wg = youpipe::SharedWaitGroup::new();
    // Miri: 10k submit/wait cycles over the injector + steal path take tens
    // of interpreted minutes; 500 exercises every queue/steal/latch path
    // (the pool has 4 emulated workers) in seconds.
    let total = if cfg!(miri) { 500 } else { 10_000 };
    wg.add(total);
    for _ in 0..total {
        let counter = counter.clone();
        let wg = wg.clone();
        pool.submit(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            wg.done();
        });
    }
    wg.wait();
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), total);
}
