mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Canonical two-thread throughput shape shared by every row: one producer
/// sends `size` items, one consumer counts recvs until the senders drop.
///
/// Macro, not a generic helper: the impls expose no common send/recv trait
/// (youpipe returns `ChannelError`, std/crossbeam their own error types), so
/// monomorphization would need a per-row adapter trait costing more code than
/// the four duplicated lines it removes.
macro_rules! row {
    ($group:expr, $name:literal, $size:expr, $mk:expr) => {
        $group.bench_with_input(BenchmarkId::new($name, $size), &$size, |b, &size| {
            b.iter(|| {
                let (tx, rx) = $mk();
                let producer = std::thread::spawn(move || {
                    for i in 0..size {
                        tx.send(i).unwrap();
                    }
                    // tx drops here: closing the channel terminates the consumer.
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
        });
    };
}

fn bench_channels(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_throughput");
    for size in [10_000_u64, 100_000_u64] {
        group.throughput(Throughput::Elements(size));

        // Inter-stage handoff caliber: bounded MPMC (crossfire mpmc::Array).
        row!(group, "youpipe_mpmc", size, || youpipe::channel::<u64>(256));
        // Terminal-collector caliber: bounded MPSC — the recv side skips the
        // MPMC CAS (see handoff/channel.rs "MPSC section").
        row!(group, "youpipe_mpsc", size, || {
            youpipe::handoff::mpsc_channel::<u64>(256)
        });
        row!(group, "crossbeam_bounded", size, || {
            crossbeam_channel::bounded::<u64>(256)
        });
        // std baselines: the bounded sync_channel is the comparable row;
        // the unbounded channel skips capacity accounting entirely and is
        // kept only to preserve the historical reference point.
        row!(group, "std_mpsc_bounded", size, || {
            std::sync::mpsc::sync_channel::<u64>(256)
        });
        row!(group, "std_mpsc_unbounded", size, || {
            std::sync::mpsc::channel::<u64>()
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_channels
}
criterion_main!(benches);
