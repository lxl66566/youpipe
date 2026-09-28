//! One-shot profiling driver for the work-stealing pool (an opt-in target of
//! the `youpipe-bench` lab crate). Depends on youpipe's `hotpath` feature,
//! which turns every `#[hotpath::measure]` probe planted in `src/pool/`,
//! `src/builder/`, and `src/handoff/` into a real per-function recorder under
//! a `HotpathGuard`.
//!
//! ```text
//! # default: sweep small→large cpu_heavy batches
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath
//!
//! # focused scenario (for isolating one bottleneck):
//! #   hotpath-profile [size] [heavy|light] [iters]
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- 10000 heavy 200
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- 1000000 light 20
//!
//! # true streaming engine (defeat the fused pass-through — see run_stream_engine):
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- stream-engine 1000000 30
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- stream-engine-ordered 1000000 20
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- stream-engine-foreach 1000000 20
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- stream-engine-fence 1000000 5
//! cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- stream-engine-async 1000000 10
//! ```
//!
//! For machine-readable output (A/B comparisons), override without touching the
//! code via env vars:
//! ```text
//! HOTPATH_OUTPUT_FORMAT=json-pretty HOTPATH_OUTPUT_PATH=target/hotpath-report.json \
//!   cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- 1000000 light 20
//! ```
//!
//! The probes are permanent (feature-gated to no-ops in normal builds), so you
//! can re-run this whenever the scheduler changes to see — without `perf` and
//! without reading disassembly — exactly how many times each worker parked, how
//! long each `join`/`steal`/`inject` took, and where the per-call fixed
//! overhead is actually spent.
//!
//! Run ONE scenario per process invocation when comparing: the `HotpathGuard`
//! aggregates function stats over the whole process lifetime, so a mixed run
//! merges the per-scenario call counts and durations.

use std::{hint::black_box, num::NonZeroUsize};

use hotpath::{Format, HotpathGuardBuilder};
use youpipe::prelude::*;

fn cpu_heavy(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn cpu_light(x: u64) -> u64 {
    x.wrapping_add(1)
}

fn main() {
    let _guard = HotpathGuardBuilder::new("hotpath_profile")
        .percentiles(&[50.0, 90.0, 95.0, 99.0, 99.9])
        .format(Format::Table)
        .build();

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        None => run_sweep(), // backward-compatible default
        Some("stream") => {
            let (size, iters) = size_iters(&args, 1000, 100);
            run_stream(size, iters);
        },
        Some("stream-engine") => {
            let (size, iters) = size_iters(&args, 1000, 100);
            run_stream_engine(size, iters);
        },
        Some("stream-engine-ordered") => {
            let (size, iters) = size_iters(&args, 1_000_000, 20);
            run_stream_engine_ordered(size, iters);
        },
        Some("stream-engine-foreach") => {
            let (size, iters) = size_iters(&args, 1_000_000, 20);
            run_stream_engine_foreach(size, iters);
        },
        Some("stream-engine-fence") => {
            let (size, iters) = size_iters(&args, 1_000_000, 5);
            run_stream_engine_fence(size, iters);
        },
        Some("stream-engine-async") => {
            let (size, iters) = size_iters(&args, 1_000_000, 10);
            run_stream_engine_async(size, iters);
        },
        Some(size) => {
            let size: usize = size.parse().expect("size must be a usize");
            let light = args.get(2).map(String::as_str) == Some("light");
            let iters: usize = args
                .get(3)
                .map(String::as_str)
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            run_focused(size, light, iters);
        },
    }
}

/// Parse `[size, iters]` args for the named-scenario subcommands, falling back
/// to the scenario's defaults when omitted.
fn size_iters(args: &[String], default_size: usize, default_iters: usize) -> (usize, usize) {
    let size = args
        .get(2)
        .map(String::as_str)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_size);
    let iters = args
        .get(3)
        .map(String::as_str)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_iters);
    (size, iters)
}

fn run_sweep() {
    for &size in &[1_000usize, 10_000, 100_000, 1_000_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        for _ in 0..50 {
            let v = data.clone();
            let out: Vec<u64> = v.pipe().map(|x| black_box(cpu_heavy(x))).collect();
            black_box(out);
        }
        println!("ran size={size}");
    }
}

/// Streaming-engine scenario: one sync stage, unordered. Exercises the
/// feeder (inline or pool job), the crossfire channel handoff, the pool
/// stage workers, and the burst-drain collector.
fn run_stream(size: usize, iters: usize) {
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = v
            .stream()
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .run();
        black_box(out);
    }
    println!("ran stream size={size} iters={iters}");
}

/// True-streaming-engine scenario: identical shape to `run_stream` but with a
/// dormant `CancellationToken`. The pure-sync `stream()` chain would otherwise
/// ride the fused pass-through (`fuse_exec` eligibility excludes any cancel
/// token), so the feeder / crossfire channels / stage workers / collector
/// drain would never execute and the hotpath report would show only the fused
/// core. The token is never fired; its per-item atomic load is the price of
/// keeping the streaming path observable.
fn run_stream_engine(size: usize, iters: usize) {
    let token = CancellationToken::new();
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = v
            .stream()
            .with_cancel(token.clone())
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .run();
        black_box(out);
    }
    println!("ran stream-engine size={size} iters={iters}");
}

/// Ordered variant of [`run_stream_engine`]: same dormant-token single-stage
/// shape, `.ordered()` routes the collector through `drain_ordered` (seq-tagged
/// `ReorderBuffer` re-sequencing) instead of `drain_unordered`.
fn run_stream_engine_ordered(size: usize, iters: usize) {
    let token = CancellationToken::new();
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = v
            .stream()
            .ordered()
            .with_cancel(token.clone())
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .run();
        black_box(out);
    }
    println!("ran stream-engine-ordered size={size} iters={iters}");
}

/// `for_each` terminal variant of [`run_stream_engine`]: same dormant-token
/// single-stage shape, but the collector sinks into a closure
/// (`ForEachCollector`) instead of materializing a `Vec` — isolates the
/// terminal-channel drain from output-buffer effects.
fn run_stream_engine_foreach(size: usize, iters: usize) {
    let token = CancellationToken::new();
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let mut sum = 0u64;
        v.stream()
            .with_cancel(token.clone())
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .for_each(|x: u64| sum = sum.wrapping_add(x));
        black_box(sum);
    }
    println!("ran stream-engine-foreach size={size} iters={iters}");
}

/// Fence (Chunked) chain variant of [`run_stream_engine`]: sync stage →
/// chunked fence → sync stage. The fence forwarder batches items into
/// chunks of 500 before releasing them to the second stage — the shape the
/// convoy bistability (todo #4) was observed on. Fewer default iters: the
/// pathology can inflate per-run wall time by an order of magnitude.
fn run_stream_engine_fence(size: usize, iters: usize) {
    let token = CancellationToken::new();
    let chunk = NonZeroUsize::new(500).expect("nonzero");
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = v
            .stream()
            .with_cancel(token.clone())
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .fence(FenceMode::Chunked(chunk))
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .run();
        black_box(out);
    }
    println!("ran stream-engine-fence size={size} iters={iters}");
}

/// Mixed sync→async chain: one sync stage feeding `stage_async` consumers,
/// collected by the async burst-drain. No cancel token needed — an async
/// stage already opts the chain out of the fused pass-through. Exercises the
/// mixed-mode channels (`sync_async_channel` sender side on the stage
/// workers, async receiver side in the collector).
fn run_stream_engine_async(size: usize, iters: usize) {
    let data: Vec<u64> = (0..size as u64).collect();
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = v
            .stream()
            .stage(|x: u64| black_box(x.wrapping_add(1)))
            .stage_async(|x: u64| async move { black_box(x.wrapping_add(1)) })
            .run();
        black_box(out);
    }
    println!("ran stream-engine-async size={size} iters={iters}");
}

fn run_focused(size: usize, light: bool, iters: usize) {
    let data: Vec<u64> = (0..size as u64).collect();
    let work = if light {
        "light"
    } else {
        "heavy"
    };
    for _ in 0..iters {
        let v = data.clone();
        let out: Vec<u64> = if light {
            v.pipe().map(|x| black_box(cpu_light(x))).collect()
        } else {
            v.pipe().map(|x| black_box(cpu_heavy(x))).collect()
        };
        black_box(out);
    }
    println!("ran size={size} work={work} iters={iters}");
}
