mod common;

use std::{hint::black_box, num::NonZeroUsize};

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

/// Input-materialization caliber for the owning `pipe()` entry (todo perf
/// #9): a non-`Vec` input is materialized with a serial O(n) fill on the
/// driver thread before the parallel phase (`Vec` inputs ride std's
/// `vec::IntoIter` collect specialization for free). Rows, everything inside
/// the timed region:
///
/// - `range_input` — `pipe(0..n)`: serial iota fill + parallel map + input free;
/// - `vec_input` — `pipe(v.clone())`: warm memcpy rebuild + parallel map + input free;
/// - `borrowed_floor` — `pipe_ref(&v)`: engine only, the reference for how much of `range_input` is
///   input materialization rather than engine;
/// - `range_gen` — `pipe_range(0..n)`: the zero-materialization generation core (items generated in
///   the leaves, no input buffer).
///
/// 1M/4M (8/32 MB) avoid the 100K whole-group collapse regime; the sizes are
/// the anchors documented to stay clean at +-1% in-group.
fn bench_input_materialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_lightweight_input_materialize");
    for size in [1_000_000usize, 4_000_000] {
        let data: Vec<u64> = (0..size as u64).collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(BenchmarkId::new("range_input", size), |b| {
            b.iter(|| {
                let r: Vec<u64> = youpipe::pipe(0..size as u64)
                    .map(|x| black_box(x.wrapping_add(1)))
                    .collect();
                black_box(r)
            });
        });

        group.bench_with_input(BenchmarkId::new("vec_input", size), &data, |b, data| {
            b.iter(|| {
                let r: Vec<u64> = youpipe::pipe(data.clone())
                    .map(|x| black_box(x.wrapping_add(1)))
                    .collect();
                black_box(r)
            });
        });

        group.bench_with_input(
            BenchmarkId::new("borrowed_floor", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let r: Vec<u64> = youpipe::pipe_ref(data)
                        .map(|&x| black_box(x.wrapping_add(1)))
                        .collect();
                    black_box(r)
                });
            },
        );

        // The generation core (`pipe_range`): items generated in the leaves,
        // no input buffer. usize items (the generation core's input type);
        // same width as the u64 rows on this target.
        group.bench_function(BenchmarkId::new("range_gen", size), |b| {
            b.iter(|| {
                let r: Vec<u64> = youpipe::pipe_range(0..size)
                    .map(|x: usize| black_box((x as u64).wrapping_add(1)))
                    .collect();
                black_box(r)
            });
        });
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

fn bench_filter_selectivity(c: &mut Criterion) {
    // Survival-rate shapes for the borrowed filter collect (the merge tree's
    // output-side cost scales with survivor count; ~33% already exists as
    // `sync_filter`). Anchors the low/mid/high ends for selectivity-sensitive
    // experiments (e.g. the count-then-place knob). rayon rows double as
    // drift controls in same-binary knob A/Bs.
    type Pred = fn(&u64) -> bool;
    let mut group = c.benchmark_group("filter_selectivity");
    for size in [10_000, 100_000] {
        let data: Vec<u64> = (0..size).collect();
        // (name, keep-rate) — `keep90` inverts the predicate so the chain
        // shape (map / filter / map) stays identical across rates.
        let shapes: [(&str, Pred); 3] = [
            ("keep10", |&x: &u64| x % 10 == 0),
            ("keep50", |&x: &u64| x % 2 == 0),
            ("keep90", |&x: &u64| x % 10 != 0),
        ];

        group.throughput(Throughput::Elements(size));
        for (name, keep) in shapes {
            group.bench_with_input(
                BenchmarkId::new(format!("youpipe_{name}"), size),
                &data,
                |b, data| {
                    b.iter(|| {
                        let r: Vec<u64> = youpipe::pipe_ref(data)
                            .map(|&x| x + 1)
                            .filter(keep)
                            .map(|x| x * 2)
                            .collect();
                        black_box(r)
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("rayon_{name}"), size),
                &data,
                |b, data| {
                    b.iter(|| {
                        black_box(
                            data.par_iter()
                                .map(|&x| x + 1)
                                .filter(keep)
                                .map(|x| x * 2)
                                .collect::<Vec<u64>>(),
                        )
                    });
                },
            );
        }
    }
    group.finish();
}


/// Reduce-terminal family: the `.map(f).sum()` shape (todo perf #3) across
/// borrowed/owned calibers, against rayon's `.map().sum()` and the old
/// materialize-then-sum path (`collect()` + serial fold) the reduce core
/// replaces. The map is the lightweight `x + 1` shape — where the removed
/// output-buffer cost dominates the story (cpu-heavy maps amortize it).
fn bench_reduce_family(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_reduce");
    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let data: Vec<u64> = (0..size).collect();

        group.throughput(Throughput::Elements(size));

        // youpipe reduce core, borrowed input (warm slice — the
        // `sync_lightweight` caliber).
        group.bench_with_input(
            BenchmarkId::new("youpipe_sum_borrowed", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let s: u64 = youpipe::pipe_ref(data)
                        .map(|&x| black_box(x.wrapping_add(1)))
                        .sum();
                    black_box(s)
                });
            },
        );

        // youpipe reduce core, owned input (fresh clone — the owning-API
        // caliber; the clone is paid identically on every side of an A/B).
        group.bench_with_input(
            BenchmarkId::new("youpipe_sum_owned", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let s: u64 = youpipe::pipe(data.clone())
                        .map(|x| black_box(x.wrapping_add(1)))
                        .sum();
                    black_box(s)
                });
            },
        );

        // The old path: materialize the whole output `Vec`, then fold it
        // serially — what `.sum()` used to require.
        group.bench_with_input(
            BenchmarkId::new("youpipe_collect_sum_borrowed", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let v: Vec<u64> = youpipe::pipe_ref(data)
                        .map(|&x| black_box(x.wrapping_add(1)))
                        .collect();
                    let s: u64 = v.iter().sum();
                    black_box(s)
                });
            },
        );

        // rayon cross-library anchor (doubles as the drift control).
        group.bench_with_input(
            BenchmarkId::new("rayon_sum", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let s: u64 = data
                        .par_iter()
                        .map(|&x| black_box(x.wrapping_add(1)))
                        .sum();
                    black_box(s)
                });
            },
        );

        // sequential floor.
        group.bench_with_input(
            BenchmarkId::new("sequential_sum", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let s: u64 = data.iter().map(|&x| black_box(x.wrapping_add(1))).sum();
                    black_box(s)
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
        bench_reduce_family,
        bench_try_collect,
        bench_for_each_vs_rayon,
        bench_filter_chain,
        bench_filter_selectivity,
        bench_lightweight_owned_cold,
        bench_input_materialize,
        bench_nested_on_pool
}

/// Nested fused terminals inside pool workers — the on-pool hybrid dispatch
/// path (`Stealing` latch: the calling worker drives the batch and waits by
/// stealing). Two regimes:
///
/// - `nested_single`: one submitted job runs a nested `.collect()`; the
///   other P-1 workers are idle/awake, so this isolates the batch ramp-up
///   (how fast the pool distributes the injected chunks).
/// - `nested_saturated`: P concurrent submitted jobs each run a nested
///   `.collect()` — every worker is a hybrid driver waiting on its own
///   latch while stealing; the throughput regime for the stealing wait.
///
/// The rayon rows run the same shape on a same-sized rayon pool
/// (`ThreadPool::spawn` + nested `par_iter`), the direct analogue of an
/// on-pool nested terminal.
fn bench_nested_on_pool(c: &mut Criterion) {
    let threads = std::thread::available_parallelism().map_or(32, NonZeroUsize::get);
    let pool = youpipe::ComputePool::new(threads);
    let rayon_pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();

    let mut group = c.benchmark_group("sync_nested_on_pool");
    for size in [1_000, 100_000] {
        let data: std::sync::Arc<Vec<u64>> = std::sync::Arc::new((0..size).collect());

        group.throughput(Throughput::Elements(size));

        group.bench_with_input(
            BenchmarkId::new("youpipe_nested_single", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    let data = std::sync::Arc::clone(data);
                    let p = pool.clone();
                    pool.submit(move || {
                        let r: Vec<u64> = youpipe::pipe_ref(data.as_slice())
                            .map(|&x| black_box(cpu_work(x)))
                            .with_compute_pool(p)
                            .collect();
                        tx.send(r).unwrap();
                    });
                    black_box(rx.recv().unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_nested_single", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    let data = std::sync::Arc::clone(data);
                    rayon_pool.spawn(move || {
                        let r: Vec<u64> = data
                            .par_iter()
                            .map(|&x| black_box(cpu_work(x)))
                            .collect();
                        tx.send(r).unwrap();
                    });
                    black_box(rx.recv().unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("youpipe_nested_saturated", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    let jobs: Vec<_> = (0..threads)
                        .map(|_| {
                            let tx = tx.clone();
                            let data = std::sync::Arc::clone(data);
                            let p = pool.clone();
                            move || {
                                let r: Vec<u64> = youpipe::pipe_ref(data.as_slice())
                                    .map(|&x| black_box(cpu_work(x)))
                                    .with_compute_pool(p)
                                    .collect();
                                tx.send(r).unwrap();
                            }
                        })
                        .collect();
                    pool.submit_batch(jobs);
                    drop(tx);
                    let mut last = Vec::new();
                    for _ in 0..threads {
                        last = rx.recv().unwrap();
                    }
                    black_box(last)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_nested_saturated", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    for _ in 0..threads {
                        let tx = tx.clone();
                        let data = std::sync::Arc::clone(data);
                        rayon_pool.spawn(move || {
                            let r: Vec<u64> = data
                                .par_iter()
                                .map(|&x| black_box(cpu_work(x)))
                                .collect();
                            tx.send(r).unwrap();
                        });
                    }
                    drop(tx);
                    let mut last = Vec::new();
                    for _ in 0..threads {
                        last = rx.recv().unwrap();
                    }
                    black_box(last)
                });
            },
        );
    }
    group.finish();
}
criterion_main!(benches);
