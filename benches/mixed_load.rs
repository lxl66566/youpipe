mod common;

use std::hint::black_box as bb;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use youpipe::stream;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// The streaming engine takes ownership (no borrowed entry), so each iteration
/// rebuilds the input in the (untimed) setup and pulls it into cache — without
/// warming the fresh clone arrives cold-from-RAM and the measured time is
/// dominated by allocator/memory latency (glibc's large memcpy uses
/// non-temporal stores; see docs/benchmarks.md).
fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

fn bench_mixed_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_load");
    // 1K / 100K anchors, aligned with `async_vs_tokio` so the streaming-CPU
    // story reads off one consistent size axis across both benches. This
    // group doubles as the pure-rayon control (`rayon_par_iter` touches none
    // of youpipe's code) for detecting environment drift in full-suite runs.
    for size in [1_000usize, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_cpu", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v).stage(|x: u64| bb(cpu_work(x))).run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_spawn_blocking_cpu", size),
            &size,
            |b, &size| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(size);
                        for i in 0..size {
                            handles
                                .push(tokio::task::spawn_blocking(move || bb(cpu_work(i as u64))));
                        }
                        for h in handles {
                            bb(h.await.unwrap());
                        }
                    });
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_par_iter", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = data.par_iter().map(|&x| bb(cpu_work(x))).collect();
                    bb(r)
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_mixed_load
}
criterion_main!(benches);
