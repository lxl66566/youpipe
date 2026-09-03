mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

// Cross-library groups compare `pipe_ref(&data)` against rayon's `par_iter`:
// both borrow the same warm slice, nothing is cloned or freed inside the
// timed region — like-for-like without any input-lifecycle alignment hacks.
// The `_cold` variant in `sync_lightweight` documents why the owned `pipe(v)`
// caliber needs a cache-warming setup (see its comment).

fn bench_par_map_vs_rayon(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_cpu_heavy");
    // 1K / 100K anchors: setup-dominated vs steady-state. The 10K midpoint
    // interpolates monotonically between them and was dropped to keep the
    // full suite fast (see benches/common/mod.rs).
    for size in [1_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> =
                        youpipe::pipe_ref(data).map(|&x| black_box(cpu_work(x))).collect();
                    black_box(r)
                });
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
                b.iter(|| {
                    black_box(
                        youpipe::pipe_ref(data)
                            .map(|&x| x + 1u64)
                            .map(|x| x * 3)
                            .map(|x| x - 2)
                            .collect(),
                    )
                });
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
    // 10K / 1M anchors: hot-cache vs memory-bandwidth-bound. The 100K
    // midpoint sits in the page-cache/malloc regime that is most sensitive
    // to whole-group sequence artifacts (see the "100K measurement trap"
    // section in docs/benchmarks.md) and is covered by the isolated A/B
    // scripts instead.
    for size in [10_000, 1_000_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map_borrowed", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(
                        youpipe::pipe_ref(data)
                            .map(|&x| black_box(x.wrapping_add(1)))
                            .collect(),
                    )
                });
            },
        );

        // Owned-input variant (fresh clone, no warming): documents the
        // one-shot cost of the owning `pipe(v)` API. glibc's large `memcpy`
        // uses non-temporal stores that bypass the cache, so the fresh clone
        // arrives cold-from-RAM and the measured time is dominated by
        // allocator/memory latency — a property of the input lifecycle, not
        // of the engine. Not comparable to the borrowed rows above.
        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map_owned_cold", size),
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
            BenchmarkId::new("youpipe_try_map", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(
                        youpipe::pipe_ref(data)
                            .try_map(|&x| -> Result<u64, &'static str> { Ok(x + 1) })
                            .map(|x| x * 3)
                            .try_collect()
                            .unwrap(),
                    )
                });
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
    // Mirrors `bench_par_map_vs_rayon` but ends in `.for_each(..)` instead of
    // `.collect()`, so the comparison isolates the dispatch machinery (no
    // output buffer allocation / writes) and documents the ramp-up win from
    // sharing `hybrid_dispatch` with the collect path.
    let mut group = c.benchmark_group("sync_for_each");
    // 1K / 100K anchors (10K midpoint dropped, same policy as
    // `bench_par_map_vs_rayon`).
    for size in [1_000, 100_000] {
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
                b.iter(|| {
                    let sink = sink.clone();
                    youpipe::pipe_ref(data)
                        .map(|&x| black_box(cpu_work(x)))
                        .for_each(move |r| {
                            sink.fetch_add(r, std::sync::atomic::Ordering::Relaxed);
                        });
                });
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

criterion_group! {
    name = benches;
    config = common::criterion();
    targets =
        bench_par_map_vs_rayon,
        bench_pipeline_fusion,
        bench_lightweight_work,
        bench_try_collect,
        bench_for_each_vs_rayon
}
criterion_main!(benches);
