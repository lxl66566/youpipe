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
                    let r: Vec<u64> = youpipe::pipe_ref(data)
                        .map(|&x| black_box(cpu_work(x)))
                        .collect();
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

/// Owned-input caliber (`pipe(data.clone())`), deliberately in its **own
/// group**: it is a one-shot-cost documentation of the owning API (fresh
/// clone, cold from RAM — glibc's large `memcpy` uses non-temporal stores),
/// *not* comparable to the borrowed rows in `sync_lightweight`. Keeping it in
/// the same group invited cross-row comparisons in charts that the input
/// lifecycle (not the engine) dominates.
fn bench_lightweight_owned_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_lightweight_owned_cold");
    for size in [10_000, 1_000_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
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

        // Owned + filter caliber: routes to the range-based filter tree
        // (`fused_try_filter_collect`) instead of the index fast path — the
        // A/B row for that tree. Owned input (like `youpipe_filter_map_owned`):
        // the clone is paid identically on both sides of any A/B.
        group.bench_with_input(
            BenchmarkId::new("youpipe_try_filter_owned", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(
                        youpipe::pipe(data.clone())
                            .try_map(|x: u64| -> Result<u64, &'static str> { Ok(x + 1) })
                            .filter(|&x: &u64| x % 3 == 0)
                            .map(|x| x * 2)
                            .try_collect()
                            .unwrap(),
                    )
                });
            },
        );

        // rayon equivalent: Result-carrying chain collected into a Result —
        // the short-circuiting counterpart of youpipe's try_map/try_collect
        // (a plain map chain would give rayon a cheaper closure).
        group.bench_with_input(BenchmarkId::new("rayon_try_map", size), &data, |b, data| {
            b.iter(|| {
                let r: Result<Vec<u64>, &'static str> = data
                    .par_iter()
                    .map(|&x| -> Result<u64, &'static str> { Ok(x + 1) })
                    .map(|r| r.map(|x| x * 3))
                    .collect();
                black_box(r.unwrap())
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
        // just consumes the value with `black_box` — enough to defeat dead-code
        // elimination. (An earlier version accumulated into a shared
        // `AtomicU64::fetch_add`, which under 32 workers bounces one cache
        // line per item: identical on both sides, but pure measurement noise
        // layered on top of the dispatch machinery under comparison.)
        group.bench_with_input(
            BenchmarkId::new("youpipe_cpu_heavy", size),
            &data,
            |b, data| {
                b.iter(|| {
                    youpipe::pipe_ref(data)
                        .map(|&x| black_box(cpu_work(x)))
                        .for_each(|r| {
                            black_box(r);
                        });
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_cpu_heavy", size),
            &data,
            |b, data| {
                b.iter(|| {
                    data.par_iter()
                        .map(|&x| black_box(cpu_work(x)))
                        .for_each(|r| {
                            black_box(r);
                        });
                });
            },
        );
    }
    group.finish();
}

fn bench_filter_chain(c: &mut Criterion) {
    // Filter chains cannot use the index-based core (output cardinality is
    // unknown), so they exercise the merge path — historically a single
    // fork/join tree even for off-pool callers. Watch this group when touching
    // the filter dispatch (hybrid ramp-up for filter chains).
    let mut group = c.benchmark_group("sync_filter");
    for size in [1_000, 10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));
        group.bench_with_input(
            BenchmarkId::new("youpipe_filter_map", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = youpipe::pipe_ref(data)
                        .map(|&x| x + 1)
                        .filter(|&x: &u64| x % 3 == 0)
                        .map(|x| x * 2)
                        .collect();
                    black_box(r)
                });
            },
        );

        // Owned caliber: same chain over `pipe(data.clone())`. The clone is
        // input-lifecycle cost paid identically on every side of an A/B; it
        // exercises the owned filter tree (`fused_filter_collect`), which is
        // a different implementation from the borrowed one.
        group.bench_with_input(
            BenchmarkId::new("youpipe_filter_map_owned", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = youpipe::pipe(data.clone())
                        .map(|x| x + 1)
                        .filter(|&x: &u64| x % 3 == 0)
                        .map(|x| x * 2)
                        .collect();
                    black_box(r)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_filter_map", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(
                        data.par_iter()
                            .map(|&x| x + 1)
                            .filter(|&x: &u64| x % 3 == 0)
                            .map(|x| x * 2)
                            .collect::<Vec<u64>>(),
                    )
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("sequential", size), &data, |b, data| {
            b.iter(|| {
                black_box(
                    data.iter()
                        .map(|&x| x + 1)
                        .filter(|&x: &u64| x % 3 == 0)
                        .map(|x| x * 2)
                        .collect::<Vec<u64>>(),
                )
            });
        });
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
        bench_for_each_vs_rayon,
        bench_filter_chain,
        bench_lightweight_owned_cold
}
criterion_main!(benches);
