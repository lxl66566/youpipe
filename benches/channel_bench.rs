use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_channels(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_throughput");
    for size in [10_000_u64, 100_000_u64] {
        group.throughput(Throughput::Elements(size));

        group.bench_with_input(
            BenchmarkId::new("youpipe_crossfire", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let (tx, rx) = youpipe::channel::<u64>(256);
                    let producer = std::thread::spawn(move || {
                        for i in 0..size {
                            tx.send(i).unwrap();
                        }
                    });
                    let consumer = std::thread::spawn(move || {
                        let mut count = 0u64;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });
                    producer.join().unwrap();
                    let count = consumer.join().unwrap();
                    assert_eq!(count, size);
                    black_box(count);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("crossbeam_channel", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let (tx, rx) = crossbeam_channel::bounded::<u64>(256);
                    let producer = std::thread::spawn(move || {
                        for i in 0..size {
                            tx.send(i).unwrap();
                        }
                    });
                    let consumer = std::thread::spawn(move || {
                        let mut count = 0u64;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });
                    producer.join().unwrap();
                    let count = consumer.join().unwrap();
                    assert_eq!(count, size);
                    black_box(count);
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("std_mpsc", size), &size, |b, &size| {
            b.iter(|| {
                let (tx, rx) = std::sync::mpsc::channel::<u64>();
                let producer = std::thread::spawn(move || {
                    for i in 0..size {
                        tx.send(i).unwrap();
                    }
                });
                let consumer = std::thread::spawn(move || {
                    let mut count = 0u64;
                    while rx.recv().is_ok() {
                        count += 1;
                    }
                    count
                });
                producer.join().unwrap();
                let count = consumer.join().unwrap();
                black_box(count);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_channels);
criterion_main!(benches);
