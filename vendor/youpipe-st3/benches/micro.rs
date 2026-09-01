//! Targeted micro-benchmarks for the hot paths of the LIFO and FIFO queues.
//!
//! Unlike `benchmark.rs` (which compares full queues against each other on
//! macroscopic workloads), these micro-benchmarks isolate individual operations
//! so that specific optimizations can be verified:
//!
//! - `steal_batch<N>`: bulk transfer of N items (data-movement cost),
//! - `push_1` / `pop_1`: single push/pop (worker-side hot path),
//! - `steal_empty_probe`: failed steal on an empty queue (executor-style
//!   probing),
//! - `worker_new`: queue construction.
//!
//! Routines return their queues so that dropping them is not part of the timed
//! section (`iter_batched` drops routine outputs after stopping the timer).
// Benches are dev-only targets, never built by the MSRV 1.60 CI check.
#![allow(clippy::incompatible_msrv)]
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use st3::fifo;
use st3::lifo;

/// Queue capacity used for all benchmarks (power of two, as required).
const CAP: usize = 256;

/// Steal batches of various sizes from a freshly filled source queue.
///
/// Both queues are warmed up in the (untimed) setup so that the measurement
/// reflects the transfer itself rather than the cache state of freshly
/// allocated queues.
fn steal_lifo(c: &mut Criterion) {
    for &batch in &[1usize, 4, 16, 32, 128] {
        c.bench_function(&format!("steal_batch{batch}-st3_lifo"), |b| {
            b.iter_batched(
                || {
                    let src: lifo::Worker<usize> = lifo::Worker::new(CAP);
                    let dest = lifo::Worker::new(CAP);
                    let _ = src.push(0);
                    debug_assert!(src.pop().is_some());
                    let _ = dest.push(0);
                    debug_assert!(dest.pop().is_some());
                    for i in 0..batch {
                        let _ = src.push(i);
                    }
                    (src.stealer(), src, dest)
                },
                |(stealer, src, dest)| {
                    let stolen = stealer.steal(&dest, |_| black_box(batch)).unwrap();
                    black_box(stolen);
                    (src, dest)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

fn steal_fifo(c: &mut Criterion) {
    for &batch in &[1usize, 4, 16, 32, 128] {
        c.bench_function(&format!("steal_batch{batch}-st3_fifo"), |b| {
            b.iter_batched(
                || {
                    let src: fifo::Worker<usize> = fifo::Worker::new(CAP);
                    let dest = fifo::Worker::new(CAP);
                    let _ = src.push(0);
                    debug_assert!(src.pop().is_some());
                    let _ = dest.push(0);
                    debug_assert!(dest.pop().is_some());
                    for i in 0..batch {
                        let _ = src.push(i);
                    }
                    (src.stealer(), src, dest)
                },
                |(stealer, src, dest)| {
                    let stolen = stealer.steal(&dest, |_| black_box(batch)).unwrap();
                    black_box(stolen);
                    (src, dest)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

/// Single push on a fresh queue.
fn push_lifo(c: &mut Criterion) {
    c.bench_function("push_1-st3_lifo", |b| {
        b.iter_batched(
            || lifo::Worker::new(CAP),
            |worker| {
                let _ = worker.push(black_box(42));
                worker
            },
            BatchSize::SmallInput,
        )
    });
}

fn push_fifo(c: &mut Criterion) {
    c.bench_function("push_1-st3_fifo", |b| {
        b.iter_batched(
            || fifo::Worker::new(CAP),
            |worker| {
                let _ = worker.push(black_box(42));
                worker
            },
            BatchSize::SmallInput,
        )
    });
}

/// Single pop on a queue holding exactly one item.
fn pop_lifo(c: &mut Criterion) {
    c.bench_function("pop_1-st3_lifo", |b| {
        b.iter_batched(
            || {
                let worker = lifo::Worker::new(CAP);
                let _ = worker.push(black_box(42));
                worker
            },
            |worker| {
                black_box(worker.pop());
                worker
            },
            BatchSize::SmallInput,
        )
    });
}

fn pop_fifo(c: &mut Criterion) {
    c.bench_function("pop_1-st3_fifo", |b| {
        b.iter_batched(
            || {
                let worker = fifo::Worker::new(CAP);
                let _ = worker.push(black_box(42));
                worker
            },
            |worker| {
                black_box(worker.pop());
                worker
            },
            BatchSize::SmallInput,
        )
    });
}

/// Failed steal on an empty source queue (executor-style probing).
fn steal_empty_lifo(c: &mut Criterion) {
    c.bench_function("steal_empty_probe-st3_lifo", |b| {
        b.iter_batched(
            || {
                let src: lifo::Worker<usize> = lifo::Worker::new(CAP);
                let dest = lifo::Worker::new(CAP);
                let _ = src.push(0);
                debug_assert!(src.pop().is_some());
                let _ = dest.push(0);
                debug_assert!(dest.pop().is_some());
                (src.stealer(), src, dest)
            },
            |(stealer, src, dest)| {
                let res = stealer.steal(&dest, |_| black_box(32));
                debug_assert!(matches!(res, Err(st3::StealError::Empty)));
                black_box(res.is_err());
                (src, dest)
            },
            BatchSize::SmallInput,
        )
    });
}

fn steal_empty_fifo(c: &mut Criterion) {
    c.bench_function("steal_empty_probe-st3_fifo", |b| {
        b.iter_batched(
            || {
                let src: fifo::Worker<usize> = fifo::Worker::new(CAP);
                let dest = fifo::Worker::new(CAP);
                let _ = src.push(0);
                debug_assert!(src.pop().is_some());
                let _ = dest.push(0);
                debug_assert!(dest.pop().is_some());
                (src.stealer(), src, dest)
            },
            |(stealer, src, dest)| {
                let res = stealer.steal(&dest, |_| black_box(32));
                debug_assert!(matches!(res, Err(st3::StealError::Empty)));
                black_box(res.is_err());
                (src, dest)
            },
            BatchSize::SmallInput,
        )
    });
}

/// Queue construction (buffer + Arc allocations).
fn worker_new_lifo(c: &mut Criterion) {
    c.bench_function("worker_new256-st3_lifo", |b| {
        b.iter_batched(
            || (),
            |()| lifo::Worker::<usize>::new(black_box(CAP)),
            BatchSize::SmallInput,
        )
    });
}

fn worker_new_fifo(c: &mut Criterion) {
    c.bench_function("worker_new256-st3_fifo", |b| {
        b.iter_batched(
            || (),
            |()| fifo::Worker::<usize>::new(black_box(CAP)),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    steal_lifo,
    steal_fifo,
    push_lifo,
    push_fifo,
    pop_lifo,
    pop_fifo,
    steal_empty_lifo,
    steal_empty_fifo,
    worker_new_lifo,
    worker_new_fifo,
);
criterion_main!(benches);
