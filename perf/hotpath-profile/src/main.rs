//! One-shot profiling driver for the work-stealing pool. Standalone package
//! under `perf/`; depends on youpipe's `hotpath` feature, which turns every
//! `#[hotpath::measure]` probe planted in `src/pool/` and `src/builder/` into a
//! real per-function recorder under a `HotpathGuard`.
//!
//! ```text
//! # default: sweep small→large cpu_heavy batches
//! cargo run --release -p hotpath-profile
//!
//! # focused scenario (for isolating one bottleneck):
//! #   hotpath-profile [size] [heavy|light] [iters]
//! cargo run --release -p hotpath-profile -- 10000 heavy 200
//! cargo run --release -p hotpath-profile -- 1000000 light 20
//! ```
//!
//! For machine-readable output (A/B comparisons), override without touching the
//! code via env vars:
//! ```text
//! HOTPATH_OUTPUT_FORMAT=json-pretty HOTPATH_OUTPUT_PATH=target/hotpath-report.json \
//!   cargo run --release -p hotpath-profile -- 1000000 light 20
//! ```
//!
//! The probes are permanent (feature-gated to no-ops in normal builds), so you
//! can re-run this whenever the scheduler changes to see — without `perf` and
//! without reading disassembly — exactly how many times each worker parked, how
//! long each `join`/`steal`/`inject` took, and where the per-call fixed
//! overhead is actually spent.

use std::hint::black_box;

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
        Some(size) => {
            let size: usize = size.parse().expect("size must be a usize");
            let light = args.get(2).map(String::as_str) == Some("light");
            let iters: usize = args
                .get(3)
                .map(String::as_str)
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            run_focused(size, light, iters);
        }
    }
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

fn run_focused(size: usize, light: bool, iters: usize) {
    let data: Vec<u64> = (0..size as u64).collect();
    let work = if light { "light" } else { "heavy" };
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
