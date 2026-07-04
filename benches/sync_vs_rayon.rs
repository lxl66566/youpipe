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

fn bench_try_collect(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_collect");
    for size in [10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        // youpipe try_collect (success path — index-based fast path, MAY_FILTER ==
        // false)
        group.bench_with_input(
            BenchmarkId::new("youpipe_try_map_warm", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        black_box(
                            youpipe::pipe(v)
                                .try_map(|x: u64| -> Result<u64, &'static str> { Ok(x + 1) })
                                .map(|x| x * 3)
                                .try_collect()
                                .unwrap(),
                        )
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // rayon equivalent: try for each + collect
        group.bench_with_input(BenchmarkId::new("rayon_try_map", size), &data, |b, data| {
            b.iter(|| {
                let r: Vec<u64> = data.par_iter().map(|&x| x + 1).map(|x| x * 3).collect();
                black_box(r)
            });
        });
    }
    group.finish();
}

fn bench_for_each_vs_rayon(c: &mut Criterion) {
    // `for_each` exercises the sink-only hybrid dispatch path (`SinkStrategy`).
    // Mirrors `bench_par_map_vs_rayon` / `bench_lightweight_work` but ends in
    // `.for_each(..)` instead of `.collect()`, so the comparison isolates the
    // dispatch machinery (no output buffer allocation / writes) and documents
    // the ramp-up win from sharing `hybrid_dispatch` with the collect path.
    let mut group = c.benchmark_group("sync_for_each");
    for size in [1_000, 10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));

        // CPU-heavy: same per-item work as `bench_par_map_vs_rayon`. The sink
        // accumulates into a relaxed atomic so the closure is not optimised
        // away, but the atomic is uncontended (one store per item, no RMW loop).
        let sink = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        group.bench_with_input(
            BenchmarkId::new("youpipe_cpu_heavy", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let sink = sink.clone();
                        youpipe::pipe(v)
                            .map(|x| black_box(cpu_work(x)))
                            .for_each(move |r| {
                                sink.fetch_add(r, std::sync::atomic::Ordering::Relaxed);
                            });
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_cpu_heavy", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let sink = sink.clone();
                    data.par_iter()
                        .map(|&x| black_box(cpu_work(x)))
                        .for_each(move |r| {
                            sink.fetch_add(r, std::sync::atomic::Ordering::Relaxed);
                        });
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
    bench_lightweight_work,
    bench_try_collect,
    bench_for_each_vs_rayon
);
criterion_main!(benches);
