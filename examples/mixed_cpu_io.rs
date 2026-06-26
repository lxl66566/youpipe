//! Mixed CPU + async IO: youpipe `stream().stage(cpu).stage_async(io)` vs
//! tokio `spawn_blocking` for both stages.
//!
//! youpipe's mixed pipeline keeps the CPU stage on the work-stealing compute
//! pool (rayon-style, sized to cores) and the IO stage on the async runtime
//! (M:N tasks), connected by a sync→async bridge. The two stages overlap —
//! IO consumers start as soon as the first CPU result lands — so a CPU-bound
//! stage and an IO-bound stage progress in parallel rather than back-to-back.
//!
//! ```text
//! cargo run --example mixed_cpu_io
//! ```

use std::time::{Duration, Instant};

use youpipe::{AsyncPool, PipelineConfig, stream};

/// Variable-cost CPU work, skewed: ~90 % fast (5 iters), ~10 % slow (5000).
fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Async IO: yields via `tokio::time::sleep`.
async fn async_io(x: u64, dur: Duration) -> u64 {
    if !dur.is_zero() {
        tokio::time::sleep(dur).await;
    }
    x.wrapping_add(1)
}

fn skewed(size: usize) -> Vec<((u64, u32), Duration)> {
    let cpu: Vec<(u64, u32)> = (0..size)
        .map(|i| {
            let iters = if i % 10 == 0 { 5000 } else { 5 };
            (i as u64, iters)
        })
        .collect();
    let io: Vec<Duration> = (0..size)
        .map(|i| {
            if i % 10 == 0 {
                Duration::from_millis(8)
            } else {
                Duration::from_millis(1)
            }
        })
        .collect();
    cpu.into_iter().zip(io).collect()
}

const SIZE: usize = 200;

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(4)
}

fn main() {
    let items = skewed(SIZE);

    // ── youpipe: CPU stage on compute pool → async IO stage ──
    let pool = AsyncPool::from_global(num_cpus()).expect("async runtime");
    let yp_start = Instant::now();
    let yp_result = stream(items.clone())
        .with_config(PipelineConfig::default().with_io_concurrency(512))
        .with_async_pool(pool)
        .stage(|((x, iters), dur): ((u64, u32), Duration)| {
            // Sync CPU stage on the work-stealing compute pool.
            (cpu_work(x, iters), dur)
        })
        .stage_async(|(val, dur): (u64, Duration)| async move {
            // Async IO stage on the runtime (M:N).
            async_io(val, dur).await
        })
        .run();
    let yp_elapsed = yp_start.elapsed();

    // ── tokio: spawn_blocking for both stages (the all-blocking baseline) ──
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_start = Instant::now();
    let tokio_result: Vec<u64> = rt.block_on(async {
        let mut handles = Vec::with_capacity(items.len());
        for &((x, iters), dur) in &items {
            handles.push(tokio::task::spawn_blocking(move || {
                (cpu_work(x, iters), dur)
            }));
        }
        let mut mid = Vec::with_capacity(handles.len());
        for h in handles {
            mid.push(h.await.unwrap());
        }
        let mut handles = Vec::with_capacity(mid.len());
        for (val, dur) in mid {
            handles.push(tokio::task::spawn_blocking(move || {
                std::thread::sleep(dur);
                val.wrapping_add(1)
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    });
    let tokio_elapsed = tokio_start.elapsed();

    let mut yp_sorted = yp_result;
    yp_sorted.sort_unstable();
    let mut tokio_sorted = tokio_result;
    tokio_sorted.sort_unstable();
    assert_eq!(yp_sorted, tokio_sorted);

    println!(
        "Mixed CPU + async IO over {SIZE} skewed items ({} results agree)",
        yp_sorted.len()
    );
    println!(
        "  youpipe stage+stage_async: {:>10.3?}   (CPU+IO overlap, M:N)",
        yp_elapsed
    );
    println!(
        "  tokio  spawn_blocking x2:  {:>10.3?}   (all-blocking baseline)",
        tokio_elapsed
    );
    println!();
    println!("youpipe's mixed pipeline overlaps CPU and IO: the IO side starts");
    println!("consuming the moment the first CPU item is ready. The tokio");
    println!("baseline serialises the two stages — every CPU task must finish");
    println!("before any IO task is spawned — and holds an OS thread per item");
    println!("for the IO wait (no M:N multiplexing).");
}
