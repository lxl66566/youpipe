use std::hint::black_box as bb;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use youpipe::stream;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn bench_mixed_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_load");
    // 1K → 10K → 100K: aligned with `async_vs_tokio` so the streaming-CPU
    // story reads off one consistent size axis across both benches.
    for size in [1_000usize, 10_000, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_cpu", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r = stream(data.clone()).stage(|x: u64| bb(cpu_work(x))).run();
                    bb(r)
                });
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

criterion_group!(benches, bench_mixed_load);
criterion_main!(benches);
