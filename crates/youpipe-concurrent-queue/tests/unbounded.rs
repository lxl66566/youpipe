#![allow(clippy::bool_assert_comparison)]

use concurrent_queue::{ConcurrentQueue, PopError, PushError};

#[cfg(not(target_family = "wasm"))]
use easy_parallel::Parallel;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_family = "wasm")]
use wasm_bindgen_test::wasm_bindgen_test as test;

#[test]
fn smoke() {
    let q = ConcurrentQueue::unbounded();
    q.push(7).unwrap();
    assert_eq!(q.pop(), Ok(7));

    q.push(8).unwrap();
    assert_eq!(q.pop(), Ok(8));
    assert!(q.pop().is_err());
}

#[test]
fn len_empty_full() {
    let q = ConcurrentQueue::unbounded();

    assert_eq!(q.len(), 0);
    assert_eq!(q.is_empty(), true);

    q.push(()).unwrap();

    assert_eq!(q.len(), 1);
    assert_eq!(q.is_empty(), false);

    q.pop().unwrap();

    assert_eq!(q.len(), 0);
    assert_eq!(q.is_empty(), true);
}

#[test]
fn len() {
    let q = ConcurrentQueue::unbounded();

    assert_eq!(q.len(), 0);

    for i in 0..50 {
        q.push(i).unwrap();
        assert_eq!(q.len(), i + 1);
    }

    for i in 0..50 {
        q.pop().unwrap();
        assert_eq!(q.len(), 50 - i - 1);
    }

    assert_eq!(q.len(), 0);
}

#[test]
fn close() {
    let q = ConcurrentQueue::unbounded();
    assert_eq!(q.push(10), Ok(()));

    assert!(!q.is_closed());
    assert!(q.close());

    assert!(q.is_closed());
    assert!(!q.close());

    assert_eq!(q.push(20), Err(PushError::Closed(20)));
    assert_eq!(q.pop(), Ok(10));
    assert_eq!(q.pop(), Err(PopError::Closed));
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn spsc() {
    const COUNT: usize = if cfg!(miri) { 100 } else { 100_000 };

    let q = ConcurrentQueue::unbounded();

    Parallel::new()
        .add(|| {
            for i in 0..COUNT {
                loop {
                    if let Ok(x) = q.pop() {
                        assert_eq!(x, i);
                        break;
                    }
                }
            }
            assert!(q.pop().is_err());
        })
        .add(|| {
            for i in 0..COUNT {
                q.push(i).unwrap();
            }
        })
        .run();
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn mpmc() {
    const COUNT: usize = if cfg!(miri) { 100 } else { 25_000 };
    const THREADS: usize = 4;

    let q = ConcurrentQueue::<usize>::unbounded();
    let v = (0..COUNT).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();

    Parallel::new()
        .each(0..THREADS, |_| {
            for _ in 0..COUNT {
                let n = loop {
                    if let Ok(x) = q.pop() {
                        break x;
                    }
                };
                v[n].fetch_add(1, Ordering::SeqCst);
            }
        })
        .each(0..THREADS, |_| {
            for i in 0..COUNT {
                q.push(i).unwrap();
            }
        })
        .run();

    for c in v {
        assert_eq!(c.load(Ordering::SeqCst), THREADS);
    }
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn drops() {
    const RUNS: usize = if cfg!(miri) { 20 } else { 100 };
    const STEPS: usize = if cfg!(miri) { 100 } else { 10_000 };

    static DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq)]
    struct DropCounter;

    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    for _ in 0..RUNS {
        let steps = fastrand::usize(0..STEPS);
        let additional = fastrand::usize(0..1000);

        DROPS.store(0, Ordering::SeqCst);
        let q = ConcurrentQueue::unbounded();

        Parallel::new()
            .add(|| {
                for _ in 0..steps {
                    while q.pop().is_err() {}
                }
            })
            .add(|| {
                for _ in 0..steps {
                    q.push(DropCounter).unwrap();
                }
            })
            .run();

        for _ in 0..additional {
            q.push(DropCounter).unwrap();
        }

        assert_eq!(DROPS.load(Ordering::SeqCst), steps);
        drop(q);
        assert_eq!(DROPS.load(Ordering::SeqCst), steps + additional);
    }
}

#[test]
fn push_n_basic() {
    let q = ConcurrentQueue::unbounded();
    assert_eq!(q.push_n([]), 0);
    assert_eq!(q.push_n([7, 8, 9]), 3);
    assert_eq!(q.len(), 3);
    assert_eq!(q.pop(), Ok(7));
    assert_eq!(q.pop(), Ok(8));
    assert_eq!(q.pop(), Ok(9));
    assert_eq!(q.pop(), Err(PopError::Empty));
}

/// A batch larger than one block (31 slots) spans the jump index; FIFO order
/// and `len` must hold across the boundary.
#[test]
fn push_n_spans_blocks() {
    let q = ConcurrentQueue::unbounded();
    let batch: Vec<i32> = (0..100).collect();
    assert_eq!(q.push_n(batch.iter().copied()), 100);
    assert_eq!(q.len(), 100);
    for i in 0..100 {
        assert_eq!(q.pop(), Ok(i));
    }
    assert_eq!(q.pop(), Err(PopError::Empty));
}

/// Interleaved single pushes and batches must produce one global FIFO order.
#[test]
fn push_n_interleaved_with_push() {
    let q = ConcurrentQueue::unbounded();
    q.push(0).unwrap();
    assert_eq!(q.push_n([1, 2, 3]), 3);
    q.push(4).unwrap();
    assert_eq!(q.push_n([5]), 1);
    assert_eq!(q.push_n((6..10).collect::<Vec<_>>()), 4);
    for i in 0..10 {
        assert_eq!(q.pop(), Ok(i));
    }
}

#[test]
fn push_n_after_close() {
    let q = ConcurrentQueue::unbounded();
    assert_eq!(q.push_n([1, 2]), 2);
    q.close();
    // Nothing further may be written once closed.
    assert_eq!(q.push_n([3, 4, 5]), 0);
    // Values already written before close remain readable.
    assert_eq!(q.pop(), Ok(1));
    assert_eq!(q.pop(), Ok(2));
    assert_eq!(q.pop(), Err(PopError::Closed));
}

/// MPMC with batch producers: every pushed item is popped exactly once and
/// Drop accounting is exact.
#[test]
fn push_n_mpmc_drops() {
    const RUNS: usize = if cfg!(miri) { 5 } else { 20 };
    const PRODUCERS: usize = 4;
    const BATCHES: usize = 50;
    const BATCH: usize = 33; // deliberately not block-aligned

    static DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq)]
    struct DropCounter;

    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    for _ in 0..RUNS {
        DROPS.store(0, Ordering::SeqCst);
        let q = std::sync::Arc::new(ConcurrentQueue::<DropCounter>::unbounded());
        let total = PRODUCERS * BATCHES * BATCH;

        let mut consumers = Vec::new();
        for _ in 0..2 {
            let q = std::sync::Arc::clone(&q);
            consumers.push(std::thread::spawn(move || {
                let mut popped = 0usize;
                while popped < usize::MAX {
                    match q.pop() {
                        Ok(_) => popped += 1,
                        Err(PopError::Empty) if q.is_closed() => break,
                        Err(PopError::Empty) => std::hint::spin_loop(),
                        Err(PopError::Closed) => break,
                    }
                }
                popped
            }));
        }

        let mut producers = Vec::new();
        for _ in 0..PRODUCERS {
            let q = std::sync::Arc::clone(&q);
            producers.push(std::thread::spawn(move || {
                for _ in 0..BATCHES {
                    assert_eq!(q.push_n((0..BATCH).map(|_| DropCounter)), BATCH);
                }
            }));
        }
        for p in producers {
            p.join().unwrap();
        }
        q.close();
        let popped: usize = consumers.into_iter().map(|c| c.join().unwrap()).sum();
        drop(q);

        assert_eq!(popped, total);
        assert_eq!(DROPS.load(Ordering::SeqCst), total);
    }
}
