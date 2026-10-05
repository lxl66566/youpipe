//! On-pool callers of the fused terminals: `pool.submit` tasks, stream stage
//! closures and nested `.run()` collectors that call `.collect()` / `.for_each()`
//! / `.try_collect()` from inside a worker of the same pool.
//!
//! These exercise the hybrid dispatcher's `Stealing`-latch path (the driver
//! waits via the work-stealing `wait_until` loop instead of a condvar) — the
//! regression suite for the ramp-up fix; each test would deadlock or hang if
//! that routing regressed back to a blocking wait.

use std::panic::AssertUnwindSafe;

use youpipe::{ComputePool, pipe, pipe_ref, stream};

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    let iters = if cfg!(miri) {
        2
    } else {
        100
    };
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// A nested `.collect()` inside a `pool.submit` task: the calling worker is
/// the hybrid driver, waits by stealing, and the batch must produce the exact
/// map. Covers both the driver-participates regime (1K: single-leaf chunks)
/// and the chunked regime (100K: per-chunk trees).
#[test]
fn nested_collect_inside_pool_worker() {
    let pool = ComputePool::new(8);
    for size in [1_000usize, 100_000] {
        let size = if cfg!(miri) {
            size / 10
        } else {
            size
        };
        let data: Vec<u64> = (0..size as u64).collect();
        let expected: Vec<u64> = data.iter().map(|&x| cpu_work(x)).collect();
        let (tx, rx) = std::sync::mpsc::channel();
        let p = pool.clone();
        pool.submit(move || {
            let got: Vec<u64> = pipe_ref(&data)
                .map(|&x| cpu_work(x))
                .with_compute_pool(p)
                .collect();
            tx.send(got).unwrap();
        });
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("on-pool nested collect deadlocked");
        assert_eq!(got, expected);
    }
}

/// Every worker of the pool runs a nested `.collect()` at the same time —
/// the worst case for the stealing wait: each driver's chunks are executable
/// by any other (also-waiting) driver. Must complete without deadlock and
/// with every batch correct.
#[test]
fn nested_collect_all_workers_concurrently() {
    let workers = 8usize;
    let pool = ComputePool::new(workers);
    let rounds = if cfg!(miri) {
        2
    } else {
        20
    };
    let size: u64 = if cfg!(miri) {
        256
    } else {
        4_096
    };
    for _ in 0..rounds {
        let (tx, rx) = std::sync::mpsc::channel();
        let jobs: Vec<_> = (0..workers as u64)
            .map(|w| {
                let tx = tx.clone();
                let p = pool.clone();
                move || {
                    let got: Vec<u64> = pipe(0..size)
                        .map(move |x: u64| x.wrapping_mul(3).wrapping_add(w))
                        .with_compute_pool(p)
                        .collect();
                    tx.send((w, got)).unwrap();
                }
            })
            .collect();
        pool.submit_batch(jobs);
        drop(tx);
        let mut results = std::collections::HashSet::new();
        for _ in 0..workers {
            let (w, got) = rx
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("concurrent on-pool nested collects deadlocked");
            let expected: Vec<u64> = (0..size)
                .map(|x| x.wrapping_mul(3).wrapping_add(w))
                .collect();
            assert_eq!(got, expected);
            results.insert(w);
        }
        assert_eq!(results.len(), workers);
    }
}

/// The hybrid panic path on-pool: a panicking chunk is captured, the
/// successful chunks' output ranges are cleaned up, and the panic propagates
/// out of `.collect()` — caught here inside the worker closure, which then
/// keeps submitting work (the pool must stay healthy).
#[test]
fn nested_panic_propagates_inside_pool_worker() {
    let pool = ComputePool::new(4);
    let (tx, rx) = std::sync::mpsc::channel();
    let p = pool.clone();
    pool.submit(move || {
        let n: u64 = if cfg!(miri) {
            64
        } else {
            4_096
        };
        let p2 = p.clone();
        let r = std::panic::catch_unwind(AssertUnwindSafe(move || {
            let _: Vec<u64> = pipe(0..n)
                .map(move |x| {
                    assert!(x < n / 2, "boom");
                    x + 1
                })
                .with_compute_pool(p2)
                .collect();
        }));
        assert!(r.is_err(), "panic must propagate out of the nested collect");
        // The same worker must still be able to run another on-pool batch.
        let got: Vec<u64> = pipe(0..1_000u64)
            .map(|x| x * 2)
            .with_compute_pool(p)
            .collect();
        tx.send(got).unwrap();
    });
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("worker did not survive the nested panic");
    assert_eq!(got, (0..1_000u64).map(|x| x * 2).collect::<Vec<_>>());
}

/// The fallible hybrid strategy on-pool: the first `Err` short-circuits the
/// batch (successful chunks clean their output ranges) and surfaces as the
/// terminal's `Err`.
#[test]
fn nested_try_collect_error_inside_pool_worker() {
    let pool = ComputePool::new(4);
    let (tx, rx) = std::sync::mpsc::channel();
    let p = pool.clone();
    pool.submit(move || {
        let n: u64 = if cfg!(miri) {
            128
        } else {
            8_192
        };
        let r: Result<Vec<u64>, &str> = pipe(0..n)
            .try_map(move |x: u64| {
                if x == n / 2 {
                    Err("stop")
                } else {
                    Ok(x + 1)
                }
            })
            .with_compute_pool(p)
            .try_collect();
        tx.send(r).unwrap();
    });
    let r = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("on-pool nested try_collect deadlocked");
    assert_eq!(r.unwrap_err(), "stop");
}

/// The sink strategy on-pool (no output buffer): `.for_each` inside a worker.
#[test]
fn nested_for_each_inside_pool_worker() {
    let pool = ComputePool::new(4);
    let (tx, rx) = std::sync::mpsc::channel();
    let p = pool.clone();
    pool.submit(move || {
        let n: u64 = if cfg!(miri) {
            256
        } else {
            20_000
        };
        let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        {
            let acc = std::sync::Arc::clone(&sum);
            let pipe_ = pipe(0..n).map(|x: u64| x + 1).with_compute_pool(p);
            pipe_.for_each(move |x| {
                acc.fetch_add(x, std::sync::atomic::Ordering::Relaxed);
            });
        }
        // Every chunk's RMWs happen-before the driver's post-wait (the
        // latch's SeqCst chain), so this load observes the final sum.
        tx.send(sum.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap();
    });
    let n: u64 = if cfg!(miri) {
        256
    } else {
        20_000
    };
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("on-pool nested for_each deadlocked");
    assert_eq!(got, (1..=n).sum::<u64>());
}

/// A stream stage closure running a fused terminal: the inner pass-through
/// `run()` collector executes on a pool worker of the same pool (the nested
/// `.run()` shape from `test_nested_stream_inside_pool_worker_no_deadlock`,
/// with a fused `pipe` inner pipeline instead of a streaming one).
#[test]
fn nested_pipe_inside_stream_stage_closure() {
    let pool = ComputePool::new(4);
    let outer: Vec<u64> = stream(0..8u64)
        .with_compute_pool(pool.clone())
        .stage(move |x: u64| {
            let n: u64 = if cfg!(miri) {
                512
            } else {
                8_192
            };
            let inner: Vec<u64> = pipe(0..n)
                .map(move |v: u64| v.wrapping_mul(2).wrapping_add(x))
                .with_compute_pool(pool.clone())
                .collect();
            inner.iter().sum::<u64>() + x
        })
        .run();
    let n: u64 = if cfg!(miri) {
        512
    } else {
        8_192
    };
    let mut expected: Vec<u64> = (0..8u64)
        .map(|x| (0..n).map(|v| v * 2 + x).sum::<u64>() + x)
        .collect();
    expected.sort_unstable();
    let mut outer = outer;
    outer.sort_unstable();
    assert_eq!(outer, expected);
}
