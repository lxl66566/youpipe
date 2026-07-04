use std::hint::black_box as bb;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use youpipe::{ComputePool, Workload, pipe, stream};

/// Clone `src` and warm it into cache so the measured time reflects framework
/// overhead, not allocator/page-fault latency. See `sync_vs_rayon.rs`.
fn warm_clone_tasks(src: &[(u64, u32)]) -> Vec<(u64, u32)> {
    let v: Vec<(u64, u32)> = src.to_vec();
    let mut acc = 0u64;
    for &(x, _) in &v {
        acc = acc.wrapping_add(x);
    }
    bb(acc);
    v
}

fn warm_clone_io_tasks(src: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let v: Vec<(u64, u64)> = src.to_vec();
    let mut acc = 0u64;
    for &(x, _) in &v {
        acc = acc.wrapping_add(x);
    }
    bb(acc);
    v
}

// ── Workload generators ──

/// CPU work with variable cost. `iterations` controls how much CPU time is
/// used. Min ~5 iterations, Max ~5000 iterations => 1000x spread.
fn cpu_work_variable(x: u64, iterations: u32) -> u64 {
    let mut r = x;
    for _ in 0..iterations {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Simulated IO work: blocks for approximately `micros` microseconds.
///
/// Note: `thread::sleep` has a granularity of ~50 µs on Linux (CFS timer tick),
/// so `Duration::from_micros(1)` actually sleeps ~50 µs. This is **consistent**
/// across all frameworks (they all call the same `thread::sleep`), so relative
/// comparisons remain fair — but the absolute latency is higher than the
/// argument suggests.
fn io_work_variable(x: u64, micros: u64) -> u64 {
    if micros > 0 {
        std::thread::sleep(std::time::Duration::from_micros(micros));
    }
    x.wrapping_add(1)
}

/// Generate a skewed workload distribution: ~90% fast tasks, ~10% slow tasks.
/// Fast tasks: iterations/sleep at MIN level
/// Slow tasks: iterations/sleep at MAX level (100x+ the fast level)
/// This mimics real-world patterns where most items are quick but a few are
/// expensive.
fn generate_skewed_workload(size: usize) -> Vec<(u64, u32)> {
    let min_iters: u32 = 5;
    let max_iters: u32 = 5000; // 1000x spread
    let mut tasks = Vec::with_capacity(size);
    for i in 0..size {
        let is_slow = i % 10 == 0; // ~10% slow tasks
        let iters = if is_slow { max_iters } else { min_iters };
        tasks.push((i as u64, iters));
    }
    tasks
}

fn generate_skewed_io_workload(size: usize) -> Vec<(u64, u64)> {
    let min_micros: u64 = 1;
    let max_micros: u64 = 200; // 200x spread
    let mut tasks = Vec::with_capacity(size);
    for i in 0..size {
        let is_slow = i % 10 == 0;
        let micros = if is_slow { max_micros } else { min_micros };
        tasks.push((i as u64, micros));
    }
    tasks
}

/// Generate a log-uniform workload: task costs are uniformly distributed
/// on a log scale from min to max. This ensures fair, deterministic spread.
fn generate_log_uniform_workload(size: usize) -> Vec<(u64, u32)> {
    let min_iters: u32 = 5;
    let max_iters: u32 = 5000; // 1000x spread
    let mut tasks = Vec::with_capacity(size);
    for i in 0..size {
        // Deterministic: cycle through a range of iteration counts
        let t = i as f64 / size as f64;
        let log_min = (min_iters as f64).ln();
        let log_max = (max_iters as f64).ln();
        let iters = ((log_min + t * (log_max - log_min)).exp() as u32).max(min_iters);
        tasks.push((i as u64, iters));
    }
    tasks
}

// ── CPU unbalanced benchmarks ──

fn bench_cpu_unbalanced_skewed(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_unbalanced_skewed");
    for size in [200, 1000, 5000] {
        let tasks = generate_skewed_workload(size);

        group.throughput(Throughput::Elements(size as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_tasks(tasks),
                    |v| {
                        let r = pipe(v)
                            .with_workload(Workload::Unbalanced)
                            .map(|(x, iters)| bb(cpu_work_variable(x, iters)))
                            .collect();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_par_iter", size),
            &tasks,
            |b, tasks| {
                b.iter(|| {
                    let r: Vec<u64> = tasks
                        .par_iter()
                        .map(|&(x, iters)| bb(cpu_work_variable(x, iters)))
                        .collect();
                    bb(r)
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("sequential", size), &tasks, |b, tasks| {
            b.iter(|| {
                let r: Vec<u64> = tasks
                    .iter()
                    .map(|&(x, iters)| bb(cpu_work_variable(x, iters)))
                    .collect();
                bb(r)
            });
        });
    }
    group.finish();
}

fn bench_cpu_unbalanced_log_uniform(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_unbalanced_log_uniform");
    for size in [200, 1000, 5000] {
        let tasks = generate_log_uniform_workload(size);

        group.throughput(Throughput::Elements(size as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("youpipe_par_map", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_tasks(tasks),
                    |v| {
                        let r = pipe(v)
                            .with_workload(Workload::Unbalanced)
                            .map(|(x, iters)| bb(cpu_work_variable(x, iters)))
                            .collect();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rayon_par_iter", size),
            &tasks,
            |b, tasks| {
                b.iter(|| {
                    let r: Vec<u64> = tasks
                        .par_iter()
                        .map(|&(x, iters)| bb(cpu_work_variable(x, iters)))
                        .collect();
                    bb(r)
                });
            },
        );
    }
    group.finish();
}

fn bench_cpu_unbalanced_stream(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_unbalanced_stream");
    for size in [200, 1000, 5000] {
        let tasks = generate_skewed_workload(size);

        group.throughput(Throughput::Elements(size as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_unordered", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_tasks(tasks),
                    |v| {
                        let r = stream(v)
                            .stage(|(x, iters): (u64, u32)| bb(cpu_work_variable(x, iters)))
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_ordered", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_tasks(tasks),
                    |v| {
                        let r = stream(v)
                            .stage(|(x, iters): (u64, u32)| bb(cpu_work_variable(x, iters)))
                            .ordered()
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_spawn_blocking", size),
            &tasks,
            |b, tasks| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(tasks.len());
                        for &(x, iters) in tasks {
                            handles.push(tokio::task::spawn_blocking(move || {
                                bb(cpu_work_variable(x, iters))
                            }));
                        }
                        for h in handles {
                            bb(h.await.unwrap());
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

// ── IO unbalanced benchmarks ──

fn bench_io_unbalanced(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_unbalanced");
    for size in [100, 500, 1000] {
        let tasks = generate_skewed_io_workload(size);

        group.throughput(Throughput::Elements(size as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_unordered", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_io_tasks(tasks),
                    |v| {
                        let r = stream(v)
                            .stage(|(x, micros): (u64, u64)| bb(io_work_variable(x, micros)))
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("youpipe_stream_ordered", size),
            &tasks,
            |b, tasks| {
                b.iter_batched(
                    || warm_clone_io_tasks(tasks),
                    |v| {
                        let r = stream(v)
                            .stage(|(x, micros): (u64, u64)| bb(io_work_variable(x, micros)))
                            .ordered()
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_spawn_blocking", size),
            &tasks,
            |b, tasks| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(tasks.len());
                        for &(x, micros) in tasks {
                            handles.push(tokio::task::spawn_blocking(move || {
                                bb(io_work_variable(x, micros))
                            }));
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
            &tasks,
            |b, tasks| {
                b.iter(|| {
                    let r: Vec<u64> = tasks
                        .par_iter()
                        .map(|&(x, micros)| bb(io_work_variable(x, micros)))
                        .collect();
                    bb(r)
                });
            },
        );
    }
    group.finish();
}

// ── Mixed CPU+IO unbalanced benchmarks ──

fn bench_mixed_unbalanced(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_cpu_io_unbalanced");
    group.sample_size(10);
    for size in [200, 1000] {
        let half = size / 2;
        let cpu_tasks = generate_skewed_workload(half);
        let io_tasks = generate_skewed_io_workload(half);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(BenchmarkId::new("youpipe_stream", size), |b| {
            let cpu_tasks = cpu_tasks.clone();
            let io_tasks = io_tasks.clone();
            b.iter(|| {
                let cpu_results = stream(cpu_tasks.clone())
                    .stage(|(x, iters): (u64, u32)| bb(cpu_work_variable(x, iters)))
                    .run();
                let io_items: Vec<(u64, u64)> = cpu_results
                    .into_iter()
                    .zip(io_tasks.iter().map(|&(_, micros)| micros))
                    .collect();
                let r = stream(io_items)
                    .stage(|(x, micros): (u64, u64)| bb(io_work_variable(x, micros)))
                    .run();
                bb(r)
            });
        });

        group.bench_function(BenchmarkId::new("tokio_mixed", size), |b| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let cpu_tasks = cpu_tasks.clone();
            let io_tasks = io_tasks.clone();
            b.iter(|| {
                rt.block_on(async {
                    let mut cpu_handles = Vec::with_capacity(cpu_tasks.len());
                    for &(x, iters) in &cpu_tasks {
                        cpu_handles.push(tokio::task::spawn_blocking(move || {
                            bb(cpu_work_variable(x, iters))
                        }));
                    }
                    let mut cpu_results = Vec::with_capacity(cpu_handles.len());
                    for h in cpu_handles {
                        cpu_results.push(h.await.unwrap());
                    }

                    let mut io_handles = Vec::with_capacity(io_tasks.len());
                    for (x, &(_, micros)) in cpu_results.into_iter().zip(io_tasks.iter()) {
                        io_handles.push(tokio::task::spawn_blocking(move || {
                            bb(io_work_variable(x, micros))
                        }));
                    }
                    for h in io_handles {
                        bb(h.await.unwrap());
                    }
                });
            });
        });
    }
    group.finish();
}

// ── Fused oversubscription: blocking-IO `for_each` with custom pool ──
//
// Simulates a mixed CPU+IO workload (read-compute-write per item) where each
// leaf blocks on `thread::sleep`. The global pool (num_cpus threads) leaves
// cores idle during IO stalls; an oversubscribed pool fills those gaps.

fn bench_fused_oversubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("fused_oversubscribe");
    group.sample_size(10);

    let ncpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);

    for size in [200, 1000] {
        group.throughput(Throughput::Elements(size as u64));

        // Baseline: global pool (num_cpus threads).
        group.bench_function(BenchmarkId::new("fused_global", size), |b| {
            b.iter(|| {
                let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let s = sum.clone();
                pipe(0..size)
                    .with_workload(Workload::Unbalanced)
                    .for_each(move |i: i32| {
                        let micros = if i % 10 == 0 { 2000 } else { 100 };
                        std::thread::sleep(std::time::Duration::from_micros(micros as u64));
                        s.fetch_add(i as u64, std::sync::atomic::Ordering::Relaxed);
                    });
                std::hint::black_box(sum.load(std::sync::atomic::Ordering::Relaxed));
            });
        });

        // Oversubscribed: 2× num_cpus threads (pre-created pool — fastest).
        group.bench_function(BenchmarkId::new("fused_oversub_2x", size), |b| {
            let pool = ComputePool::new(ncpus * 2);
            b.iter(|| {
                let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let s = sum.clone();
                pipe(0..size)
                    .with_compute_pool(pool.clone())
                    .with_workload(Workload::Unbalanced)
                    .for_each(move |i: i32| {
                        let micros = if i % 10 == 0 { 2000 } else { 100 };
                        std::thread::sleep(std::time::Duration::from_micros(micros as u64));
                        s.fetch_add(i as u64, std::sync::atomic::Ordering::Relaxed);
                    });
                std::hint::black_box(sum.load(std::sync::atomic::Ordering::Relaxed));
            });
        });

        // Oversubscribed via the convenience method (transient pool per call).
        // Demonstrates the ~ms pool construction overhead vs pre-created pool.
        group.bench_function(BenchmarkId::new("fused_oversub_2x_transient", size), |b| {
            b.iter(|| {
                let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let s = sum.clone();
                pipe(0..size)
                    .with_oversubscribe(2)
                    .with_workload(Workload::Unbalanced)
                    .for_each(move |i: i32| {
                        let micros = if i % 10 == 0 { 2000 } else { 100 };
                        std::thread::sleep(std::time::Duration::from_micros(micros as u64));
                        s.fetch_add(i as u64, std::sync::atomic::Ordering::Relaxed);
                    });
                std::hint::black_box(sum.load(std::sync::atomic::Ordering::Relaxed));
            });
        });

        // Oversubscribed: 4× num_cpus threads.
        group.bench_function(BenchmarkId::new("fused_oversub_4x", size), |b| {
            let pool = ComputePool::new(ncpus * 4);
            b.iter(|| {
                let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let s = sum.clone();
                pipe(0..size)
                    .with_compute_pool(pool.clone())
                    .with_workload(Workload::Unbalanced)
                    .for_each(move |i: i32| {
                        let micros = if i % 10 == 0 { 2000 } else { 100 };
                        std::thread::sleep(std::time::Duration::from_micros(micros as u64));
                        s.fetch_add(i as u64, std::sync::atomic::Ordering::Relaxed);
                    });
                std::hint::black_box(sum.load(std::sync::atomic::Ordering::Relaxed));
            });
        });

        // rayon reference (global pool, num_cpus threads).
        group.bench_function(BenchmarkId::new("rayon_global", size), |b| {
            b.iter(|| {
                let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let s = sum.clone();
                (0..size).into_par_iter().for_each(move |i: i32| {
                    let micros = if i % 10 == 0 { 2000 } else { 100 };
                    std::thread::sleep(std::time::Duration::from_micros(micros as u64));
                    s.fetch_add(i as u64, std::sync::atomic::Ordering::Relaxed);
                });
                std::hint::black_box(sum.load(std::sync::atomic::Ordering::Relaxed));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_cpu_unbalanced_skewed,
    bench_cpu_unbalanced_log_uniform,
    bench_cpu_unbalanced_stream,
    bench_io_unbalanced,
    bench_mixed_unbalanced,
    bench_fused_oversubscribe,
);
criterion_main!(benches);
