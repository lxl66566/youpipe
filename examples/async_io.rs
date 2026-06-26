//! Async IO with M:N concurrency: youpipe `stream().stage_async()` vs
//! `tokio::spawn` per-item, on workloads whose waits actually yield.
//!
//! The async stage runs `io_concurrency` tokio tasks on a runtime with
//! `async_workers` OS threads. Each task yields its thread back to the runtime
//! while awaiting — so thousands of concurrent IO waits can be multiplexed
//! over a handful of threads. This is the regime where async beats
//! `spawn_blocking` (which holds a thread per item).
//!
//! ```text
//! cargo run --example async_io
//! ```
//!
//! ## API shape
//!
//! The simple form needs **zero** configuration — sensible defaults pick a
//! worker count, an `io_concurrency`, and lazily build a tokio runtime the
//! first time `stage_async` runs in a `run()` call:
//!
//! ```text
//! stream(items)
//!     .stage_async(|x| async move { ... })
//!     .run();
//! ```
//!
//! Tune only when you outgrow the defaults:
//!
//! ```text
//! use youpipe::prelude::*;
//!
//! //! // 1. Raise the in-flight IO cap (default 128).
//! items.stream()
//!     .with_config(PipelineConfig::default().with_io_concurrency(512))
//!     .stage_async(|x| async move { ... })
//!     .run();
//!
//! // 2. Reuse one runtime across many `run()` calls (criterion benches,
//! //    long-lived services). Construction (~ms) is paid once.
//! let pool = AsyncPool::from_default().expect("async runtime");
//! items0.stream().with_async_pool(pool).stage_async(...).run();
//! // (the pool is moved into the run; rebuild per call or wrap a Handle
//! // yourself to truly share across runs.)
//! ```

use std::time::{Duration, Instant};

use youpipe::prelude::*;

/// Async IO work: yields the OS thread back to the runtime while sleeping.
/// Real network/disk IO behaves the same way (`.await` is the yield point).
async fn async_io(x: u64, dur: Duration) -> u64 {
    if !dur.is_zero() {
        tokio::time::sleep(dur).await;
    }
    x.wrapping_add(1)
}

/// Skewed latency: ~90 % at `BASE`, ~10 % at `BASE * 8` (realistic network /
/// disk tail latency). The point of M:N concurrency is to keep the runtime
/// saturated even when a few items stall for 8× longer than the rest.
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

const SIZE: usize = 200;

fn main() {
    let tasks = skewed_io(SIZE);

    // ── youpipe stream().stage_async(): defaults only, no tuning ──
    // `io_concurrency` defaults to 128; the tokio runtime is built lazily on
    // the first `acquire_async()` inside this `run()` and reused for the rest
    // of the call. Compare with the "tune io_concurrency + reuse pool" form
    // documented in the module header above.
    let yp_start = Instant::now();
    let yp_result = tasks
        .clone()
        .stream()
        .stage_async(|(x, dur): (u64, Duration)| async move { async_io(x, dur).await })
        .run();
    let yp_elapsed = yp_start.elapsed();

    // ── tokio-native async: one task per item, unbounded ──
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_start = Instant::now();
    let tokio_result: Vec<u64> = rt.block_on(async {
        let mut handles = Vec::with_capacity(tasks.len());
        for &(x, dur) in &tasks {
            handles.push(tokio::spawn(async move { async_io(x, dur).await }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    });
    let tokio_elapsed = tokio_start.elapsed();

    // Both must produce identical outputs (modulo order).
    let mut yp_sorted = yp_result;
    yp_sorted.sort_unstable();
    let mut tokio_sorted = tokio_result;
    tokio_sorted.sort_unstable();
    assert_eq!(yp_sorted, tokio_sorted);

    println!(
        "Async IO over {SIZE} items (skewed 1ms / 8ms tail, {} items agree)",
        yp_sorted.len()
    );
    println!(
        "  youpipe stage_async: {:>10.3?}   (M:N, default io_concurrency=128)",
        yp_elapsed
    );
    println!(
        "  tokio  spawn:        {:>10.3?}   (async ceiling, 1 task/item)",
        tokio_elapsed
    );
    println!();
    println!("Both run async tasks on a tokio runtime, so both yield the OS");
    println!("thread while waiting. youpipe adds a feeder→consumer channel");
    println!("topology with bounded `io_concurrency` (backpressure); tokio's");
    println!("native spawn is the asymptotic ceiling — youpipe should land");
    println!("within a small constant of it.");
}
