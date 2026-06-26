//! One-shot profiling driver for the work-stealing pool. Builds only with
//! `--features hotpath` (see `Cargo.toml`'s `[[example]]` `required-features`).
//!
//! Runs the exact workload from `docs/PERF_NOTES.md` (#1) — a `cpu_heavy`
//! `pipe().map().collect()` across small and large batch sizes — under a
//! `HotpathGuard`, so every `#[hotpath::measure]` probe planted in
//! `src/pool/` and `src/builder/` records call-count / latency / percentile
//! data.
//!
//! ```text
//! cargo run --release --example hotpath_profile --features hotpath
//! ```
//!
//! Because the probes are permanent (feature-gated to no-ops in normal
//! builds), you can re-run this whenever the scheduler changes to see — without
//! `perf` and without reading disassembly — exactly how many times each worker
//! parked, how long each `join`/`steal`/`inject` took, and where the per-call
//! fixed overhead is actually spent.

use std::hint::black_box;

use hotpath::{Format, HotpathGuardBuilder};

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn main() {
    // One guard per process. Defaults to a human-readable table on stdout; for
    // machine-readable output (e.g. A/B comparisons) override without touching
    // the code via env vars:
    //     HOTPATH_OUTPUT_FORMAT=json-pretty \
    //     HOTPATH_OUTPUT_PATH=target/hotpath-report.json \
    //     cargo run --release --example hotpath_profile --features hotpath
    let _guard = HotpathGuardBuilder::new("hotpath_profile")
        .percentiles(&[50.0, 90.0, 95.0, 99.0, 99.9])
        .format(Format::Table)
        .build();

    // The #1 PERF_NOTES workload: small vs large batches on the same pool.
    // The 1 k batch exercises the serial short-circuit (it never enters
    // `par_index_collect`); the larger batches drive the full fork/join path.
    for &size in &[1_000usize, 10_000, 100_000, 1_000_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        for _ in 0..50 {
            let v = data.clone();
            let out: Vec<u64> = youpipe::pipe(v).map(|x| black_box(cpu_work(x))).collect();
            black_box(out);
        }
        println!("ran size={size}");
    }

    // Guard writes the report on drop.
}
