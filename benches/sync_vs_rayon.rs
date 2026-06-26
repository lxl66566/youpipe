use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Clone `src` and read every element once so the pages are faulted in AND the
/// data is warm in cache. Used as the (untimed) `iter_batched` setup so that
/// the timed region measures the framework's work, not allocator / page-fault /
/// cold-memory noise.
///
/// This is what makes the comparison fair: `par_map` takes ownership (so each
/// iteration needs a fresh `Vec`), whereas `rayon::par_iter` borrows reused
/// data. Without warming, the fresh clone arrives cold-from-RAM (glibc's large
/// memcpy uses non-temporal stores that bypass the cache) and the measured time
/// is dominated by memory latency rather than the framework — a property of the
/// allocator, not of youpipe.
fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    // Touch every element (cache-warming read) folded into a sink the optimizer
    // cannot eliminate, so the clone's non-temporal-stored bytes are pulled back
    // into cache before the timed region runs.
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    black_box(acc);
    v
}

fn bench_par_map_vs_rayon(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_cpu_heavy");
    for size in [1_000, 10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| black_box(youpipe::pipe(v).map(|x| black_box(cpu_work(x))).collect()),
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_par_iter", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = data.par_iter().map(|&x| black_box(cpu_work(x))).collect();
                    black_box(r)
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("sequential", size), &data, |b, data| {
            b.iter(|| {
                let r: Vec<u64> = data.iter().map(|&x| black_box(cpu_work(x))).collect();
                black_box(r)
            });
        });
    }
    group.finish();
}

fn bench_pipeline_fusion(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_fusion");
    for size in [10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("fused_3_stages", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        black_box(
                            youpipe::pipe(v)
                                .map(|x: u64| x + 1)
                                .map(|x: u64| x * 3)
                                .map(|x: u64| x - 2)
                                .collect(),
                        )
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("rayon_chain", size), &data, |b, data| {
            b.iter(|| {
                let r: Vec<u64> = data
                    .par_iter()
                    .map(|&x| x + 1)
                    .map(|x| x * 3)
                    .map(|x| x - 2)
                    .collect();
                black_box(r)
            });
        });

        group.bench_with_input(
            BenchmarkId::new("sequential_chain", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = data
                        .iter()
                        .map(|&x| x + 1)
                        .map(|x| x * 3)
                        .map(|x| x - 2)
                        .collect();
                    black_box(r)
                });
            },
        );
    }
    group.finish();
}

fn bench_lightweight_work(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_lightweight");
    for size in [10_000, 100_000, 1_000_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        // Warm-input variant: measures framework work in the steady-state
        // (hot-loop) regime, comparable to rayon's warm borrow.
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map_warm", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        black_box(
                            youpipe::pipe(v)
                                .map(|x| black_box(x.wrapping_add(1)))
                                .collect(),
                        )
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // Cold-input variant (fresh clone, no warming): documents the
        // one-shot-from-cold-memory cost. Every framework pays cold-read
        // latency here; this is not a like-for-like comparison with rayon's
        // warm borrow below.
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map_cold", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(
                        youpipe::pipe(data.clone())
                            .map(|x| black_box(x.wrapping_add(1)))
                            .collect(),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_par_iter", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = data.par_iter().map(|&x| black_box(x + 1)).collect();
                    black_box(r)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_par_map_vs_rayon,
    bench_pipeline_fusion,
    bench_lightweight_work
);
criterion_main!(benches);
