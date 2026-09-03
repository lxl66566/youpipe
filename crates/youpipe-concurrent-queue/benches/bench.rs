#![allow(clippy::incompatible_msrv)] // false positive: https://github.com/rust-lang/rust-clippy/issues/12257#issuecomment-2093667187

use std::{any::type_name, fmt::Debug, hint::black_box};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use criterion::{criterion_group, criterion_main, Criterion};
use easy_parallel::Parallel;

const COUNT: usize = 100_000;
const THREADS: usize = 7;
// Small capacity for the contended benchmark, so that producers repeatedly hit
// a full queue and have to retry.
const SMALL_CAP: usize = 1024;

fn spsc<T: Default + std::fmt::Debug + Send>(recv: &ConcurrentQueue<T>, send: &ConcurrentQueue<T>) {
    Parallel::new()
        .add(|| loop {
            match recv.pop() {
                Ok(_) => (),
                Err(PopError::Empty) => (),
                Err(PopError::Closed) => break,
            }
        })
        .add(|| {
            for _ in 0..COUNT {
                send.push(T::default()).unwrap();
            }
            send.close();
        })
        .run();
}

fn mpsc<T: Default + std::fmt::Debug + Send>(recv: &ConcurrentQueue<T>, send: &ConcurrentQueue<T>) {
    Parallel::new()
        .each(0..THREADS, |_| {
            for _ in 0..COUNT {
                send.push(T::default()).unwrap();
            }
        })
        .add(|| {
            let mut recieved = 0;
            while recieved < THREADS * COUNT {
                match recv.pop() {
                    Ok(_) => recieved += 1,
                    Err(PopError::Empty) => (),
                    Err(PopError::Closed) => unreachable!(),
                }
            }
        })
        .run();
}

/// Like `mpsc`, but the queue is bounded to a small capacity so that it is full most of the
/// time and producers retry in a tight loop.
fn mpsc_contended<T: Default + std::fmt::Debug + Send>(
    recv: &ConcurrentQueue<T>,
    send: &ConcurrentQueue<T>,
) {
    Parallel::new()
        .each(0..THREADS, |_| {
            for _ in 0..COUNT {
                let mut value = T::default();
                loop {
                    value = match send.push(value) {
                        Ok(()) => break,
                        Err(PushError::Full(value)) => value,
                        Err(PushError::Closed(_)) => unreachable!(),
                    };
                }
            }
        })
        .add(|| {
            let mut recieved = 0;
            while recieved < THREADS * COUNT {
                match recv.pop() {
                    Ok(_) => recieved += 1,
                    Err(PopError::Empty) => (),
                    Err(PopError::Closed) => unreachable!(),
                }
            }
        })
        .run();
}

fn single_thread<T: Default + std::fmt::Debug>(
    recv: &ConcurrentQueue<T>,
    send: &ConcurrentQueue<T>,
) {
    for _ in 0..COUNT {
        send.push(T::default()).unwrap();
    }
    for _ in 0..COUNT {
        recv.pop().unwrap();
    }
}

// Because we can't pass generic functions as const parameters.
macro_rules! bench_all(
    ($name:ident, $f:ident) => {
        fn $name(c: &mut Criterion) {
            fn helper<T: Default + Debug + Send>(c: &mut Criterion) {
                let name = format!("unbounded_{}_{}", stringify!($f), type_name::<T>());

                c.bench_function(&name, |b| b.iter(|| {
                    let q = ConcurrentQueue::unbounded();
                    $f::<T>(black_box(&q), black_box(&q));
                }));

                let name = format!("bounded_{}_{}", stringify!($f), type_name::<T>());

                c.bench_function(&name, |b| b.iter(|| {
                    let q = ConcurrentQueue::bounded(THREADS * COUNT);
                    $f::<T>(black_box(&q), black_box(&q));
                }));
            }
            helper::<u8>(c);
            helper::<u16>(c);
            helper::<u32>(c);
            helper::<u64>(c);
            helper::<u128>(c);
        }
    }
);

bench_all!(bench_spsc, spsc);
bench_all!(bench_mpsc, mpsc);
bench_all!(bench_single_thread, single_thread);

// Only meaningful for a bounded queue: an unbounded queue is never full.
fn bench_mpsc_contended(c: &mut Criterion) {
    fn helper<T: Default + Debug + Send>(c: &mut Criterion) {
        let name = format!("bounded_mpsc_contended_{}", type_name::<T>());

        c.bench_function(&name, |b| {
            b.iter(|| {
                let q = ConcurrentQueue::bounded(SMALL_CAP);
                mpsc_contended::<T>(black_box(&q), black_box(&q));
            })
        });
    }
    helper::<u8>(c);
    helper::<u16>(c);
    helper::<u32>(c);
    helper::<u64>(c);
    helper::<u128>(c);
}

criterion_group!(
    generic_group,
    bench_single_thread,
    bench_spsc,
    bench_mpsc,
    bench_mpsc_contended
);
criterion_main!(generic_group);
