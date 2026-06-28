use std::{hint::black_box as bb, num::NonZeroUsize};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::{FenceMode, stream};

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Clone `src` and warm it into cache so the measured time reflects framework
/// overhead, not allocator/page-fault latency. See `sync_vs_rayon.rs`.
fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

fn bench_stream_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_pipeline");
    // 1K → 10K → 100K: spans the setup-dominated to channel-bandwidth-bound
    // regime. Above 100K the channel handoff cost is fully amortised and the
    // numbers stop surfacing new information, so 100K is the upper anchor.
    for size in [1_000, 10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("single_stage_unordered", size),
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
            BenchmarkId::new("single_stage_ordered", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v).stage(|x: u64| bb(cpu_work(x))).ordered().run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("multi_stage_2", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .stage(|x: u64| bb(cpu_work(x)))
                        .stage(|x: u64| bb(x.wrapping_add(1)))
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("with_fence", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .stage(|x: u64| bb(cpu_work(x)))
                        .fence(FenceMode::Chunked(NonZeroUsize::new(500).unwrap()))
                        .stage(|x: u64| bb(x.wrapping_add(1)))
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_tokio_spawn_blocking(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokio_spawn_blocking");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(BenchmarkId::new("spawn_blocking_cpu", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(size);
                    for i in 0..size {
                        handles.push(tokio::task::spawn_blocking(move || bb(cpu_work(i as u64))));
                    }
                    for h in handles {
                        bb(h.await.unwrap());
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_stream_pipeline, bench_tokio_spawn_blocking);
criterion_main!(benches);
