//! IO / mixed(CPU+IO) / mixed(sync+async) streaming benchmarks.
//!
//! Unlike `unbalanced.rs` (which simulates IO with blocking
//! `std::thread::sleep`), this bench simulates **truly async** IO with
//! `tokio::time::sleep` — a wait that *yields* the OS thread back to the
//! runtime. That is the regime where M:N async concurrency beats the
//! blocking-thread-per-core model: `io_concurrency` concurrent waits can be
//! multiplexed over `async_workers` threads, whereas a blocking wait stalls
//! its thread and caps concurrency at the thread count.
//!
//! Groups:
//! * `io_async_pure` — single IO stage. Compares youpipe `run_async` (M:N) vs
//!   blocking approaches vs tokio-native async.
//! * `io_async_mixed` — CPU stage (sync) -> IO stage. Compares youpipe
//!   `run_mixed_async` (sync CPU + async IO) vs the all-blocking
//!   `run_multi_stage` vs tokio.

use std::{hint::black_box as bb, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::{AsyncPool, PipelineConfig, StreamPipeline};

/// CPU work, variable cost controlled by `iters`.
fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Blocking IO wait (stalls the OS thread).
fn blocking_io(x: u64, dur: Duration) -> u64 {
    if !dur.is_zero() {
        std::thread::sleep(dur);
    }
    x.wrapping_add(1)
}

/// Async IO wait (yields the OS thread back to the runtime).
async fn async_io(x: u64, dur: Duration) -> u64 {
    if !dur.is_zero() {
        tokio::time::sleep(dur).await;
    }
    x.wrapping_add(1)
}

/// Skewed IO latency: ~90% at `BASE`, ~10% at `BASE * 8` (realistic
/// network/disk tail latency). Returns `(value, latency)`.
///
/// `BASE` is deliberately ≥ the tokio coarse-timer granularity (~1 ms): sub-ms
/// `tokio::time::sleep` durations round up to ~1 ms, which would make the bench
/// measure timer overhead rather than real async concurrency. With ms-scale
/// latencies the comparison is honest and the M:N advantage is measurable.
fn skewed_io(size: usize) -> Vec<(u64, Duration)> {
    const BASE_MS: u64 = 1;
    let base = Duration::from_millis(BASE_MS);
    let tail = Duration::from_millis(BASE_MS * 8);
    (0..size)
        .map(|i| {
            let dur = if i % 10 == 0 { tail } else { base };
            (i as u64, dur)
        })
        .collect()
}

/// Skewed CPU cost: ~90% fast (5 iters), ~10% slow (5000 iters).
fn skewed_cpu(size: usize) -> Vec<(u64, u32)> {
    let min_iters: u32 = 5;
    let max_iters: u32 = 5000;
    (0..size)
        .map(|i| {
            let iters = if i % 10 == 0 { max_iters } else { min_iters };
            (i as u64, iters)
        })
        .collect()
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

// ── Pure IO: async (yielding) vs blocking ──

fn bench_pure_io_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_async_pure");
    group.sample_size(10);
    for size in [200_usize, 500] {
        let tasks = skewed_io(size);
        group.throughput(Throughput::Elements(size as u64));

        // youpipe run_async: M:N concurrency via the async runtime. The async
        // pool is built once and reused across iterations (runtime creation is
        // ~ms and would otherwise dominate smaller sizes).
        {
            let pool = AsyncPool::from_global(num_cpus()).expect("async runtime");
            let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(512))
                .with_async_pool(pool);
            group.bench_with_input(
                BenchmarkId::new("youpipe_async", size),
                &tasks,
                |b, tasks| {
                    b.iter(|| {
                        let r = sp.run_async(
                            tasks.clone(),
                            |(x, dur): (u64, Duration)| async move { async_io(x, dur).await },
                            false,
                        );
                        bb(r)
                    });
                },
            );
        }

        // youpipe sync stream: blocking sleep on the compute pool (N threads).
        group.bench_with_input(
            BenchmarkId::new("youpipe_blocking", size),
            &tasks,
            |b, tasks| {
                let sp = StreamPipeline::new(PipelineConfig::default());
                b.iter(|| {
                    let r = sp.run(
                        tasks.clone(),
                        |(x, dur): (u64, Duration)| bb(blocking_io(x, dur)),
                        false,
                    );
                    bb(r)
                });
            },
        );

        // tokio-native async: spawn one task per item (async sleep). This is
        // the async ceiling — unbounded concurrency with no channel handoff.
        let rt = tokio::runtime::Runtime::new().unwrap();
        group.bench_with_input(
            BenchmarkId::new("tokio_async_native", size),
            &tasks,
            |b, tasks| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(tasks.len());
                        for &(x, dur) in tasks {
                            handles.push(tokio::spawn(async move { bb(async_io(x, dur).await) }));
                        }
                        for h in handles {
                            bb(h.await.unwrap());
                        }
                    });
                });
            },
        );

        // tokio spawn_blocking: blocking sleep on tokio's blocking pool.
        group.bench_with_input(
            BenchmarkId::new("tokio_spawn_blocking", size),
            &tasks,
            |b, tasks| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(tasks.len());
                        for &(x, dur) in tasks {
                            handles
                                .push(tokio::task::spawn_blocking(move || bb(blocking_io(x, dur))));
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

// ── Mixed CPU (sync) + IO (async vs blocking) ──
//
// Each item is `((value, cpu_iters), io_micros)`: the CPU stage computes
// `cpu_work(value, cpu_iters)` and threads `io_micros` along; the IO stage then
// waits `io_micros`.

fn bench_mixed_cpu_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_async_mixed");
    group.sample_size(10);
    for size in [200_usize, 500] {
        let items: Vec<((u64, u32), Duration)> = skewed_cpu(size)
            .into_iter()
            .zip(skewed_io(size).into_iter().map(|(_, d)| d))
            .collect();
        group.throughput(Throughput::Elements(size as u64));

        // youpipe run_mixed_async: sync CPU on compute pool → async IO on the
        // async runtime (overlapping stages).
        {
            let pool = AsyncPool::from_global(num_cpus()).expect("async runtime");
            let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(512))
                .with_async_pool(pool);
            group.bench_with_input(
                BenchmarkId::new("youpipe_mixed_async", size),
                &items,
                |b, items| {
                    b.iter(|| {
                        let r = sp.run_mixed_async(
                            items.clone(),
                            |((x, iters), dur): ((u64, u32), Duration)| {
                                (bb(cpu_work(x, iters)), dur)
                            },
                            |(val, dur): (u64, Duration)| async move { async_io(val, dur).await },
                            false,
                        );
                        bb(r)
                    });
                },
            );
        }

        // youpipe run_multi_stage: both stages blocking on the compute pool
        // (the previous all-sync baseline).
        {
            let sp = StreamPipeline::new(PipelineConfig::default());
            group.bench_with_input(
                BenchmarkId::new("youpipe_mixed_blocking", size),
                &items,
                |b, items| {
                    b.iter(|| {
                        let r = sp.run_multi_stage(
                            items.clone(),
                            |((x, iters), dur): ((u64, u32), Duration)| {
                                (bb(cpu_work(x, iters)), dur)
                            },
                            |(val, dur): (u64, Duration)| bb(blocking_io(val, dur)),
                            false,
                        );
                        bb(r)
                    });
                },
            );
        }

        // tokio mixed: spawn_blocking for both CPU and blocking-IO stages.
        let rt = tokio::runtime::Runtime::new().unwrap();
        group.bench_with_input(
            BenchmarkId::new("tokio_mixed_blocking", size),
            &items,
            |b, items| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut cpu_handles = Vec::with_capacity(items.len());
                        for &((x, iters), dur) in items {
                            cpu_handles.push(tokio::task::spawn_blocking(move || {
                                (bb(cpu_work(x, iters)), dur)
                            }));
                        }
                        let mut cpu_results = Vec::with_capacity(cpu_handles.len());
                        for h in cpu_handles {
                            cpu_results.push(h.await.unwrap());
                        }
                        let mut io_handles = Vec::with_capacity(cpu_results.len());
                        for (val, dur) in cpu_results {
                            io_handles.push(tokio::task::spawn_blocking(move || {
                                bb(blocking_io(val, dur))
                            }));
                        }
                        for h in io_handles {
                            bb(h.await.unwrap());
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_pure_io_async, bench_mixed_cpu_io);
criterion_main!(benches);
