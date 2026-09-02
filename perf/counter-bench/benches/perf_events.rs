//! Perf-event counter benchmarks — criterion driven by Linux hardware
//! performance counters (instructions / cycles / cache-misses / …) instead of
//! wall-clock time, via the standalone `criterion-perf-counters` crate.
//!
//! Hardware counters answer a question wall time cannot: *how much work does
//! the code actually do* (instructions are frequency-invariant), and
//! therefore reproduce across runs far better than time on a machine with
//! frequency boost and background load. See the "perf-event counters"
//! section in docs/benchmarks.md for the measured drift comparison and the
//! verdict on which counters are worth watching.
//!
//! Usage:
//!
//! ```sh
//! cargo bench --manifest-path perf/counter-bench/Cargo.toml --bench perf_events
//! PERF_EVENT=cycles    cargo bench --manifest-path perf/counter-bench/Cargo.toml
//! PERF_EVENT=ref-cycles cargo bench --manifest-path perf/counter-bench/Cargo.toml
//! PERF_EVENT=walltime  cargo bench --manifest-path perf/counter-bench/Cargo.toml
//! # full statistical treatment on a single event:
//! BENCH_SAMPLE_SIZE=100 BENCH_WARMUP_MS=3000 BENCH_MEASUREMENT_MS=5000 \
//!     cargo bench --manifest-path perf/counter-bench/Cargo.toml
//! ```
//!
//! Design notes — why these benches look different from the main suite:
//!
//! * **Plain `b.iter` with borrowed input, no `iter_batched` setup.** Criterion opens *one
//!   measurement window per sample* for plain `iter`, but one window *per iteration* for
//!   `BatchSize::PerIteration`. Each window pays `4 × n_threads` syscalls
//!   (enable/disable/read/reset per per-thread counter), so PerIteration windows would inflate fast
//!   benches several-fold; plain `iter` makes the cost ~0.1%. Input is `data.iter().copied()`
//!   (borrowed, no per-iteration `warm_clone`): the point here is methodology comparison and
//!   counter stability, not the clone-vs-borrow semantics the main suite measures. `pipe()` buffers
//!   its input either way, so both measurement modes see identical work.
//! * **Only persistent-pool benches.** Counters are opened per task in `/proc/self/task` (all
//!   youpipe/rayon pool workers are covered — they spawn during unmeasured warm-up), but threads
//!   that spawn *and* exit inside one window (e.g. `channel_throughput`'s per-iteration producer/
//!   consumer threads) contribute nothing. Those benches are excluded.
//! * The `sequential` anchor has a data-independent instruction stream, so its instruction count is
//!   near-deterministic — use it as the sanity check that counters (and the window arithmetic) are
//!   exact.
//!
//! Counters are user-space-only (`exclude_kernel` + `exclude_hv`): required
//! for unprivileged use at `kernel.perf_event_paranoid >= 2`, and it keeps
//! interrupt/timer noise out of the counts. As a consequence syscall-heavy
//! (IO) work is *undercounted* — do not compare absolute counter values
//! across different event classes, and don't read "instructions" as total
//! CPU work for kernel-heavy paths.

mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, measurement::Measurement};
use criterion_perf_counters::Perf;
use perfcnt::linux::{HardwareEventType as HW, PerfCounterBuilderLinux as Builder};
use rayon::prelude::*;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn bench_cpu_heavy<M: Measurement>(c: &mut Criterion<M>) {
    let mut group = c.benchmark_group("perf_sync_cpu_heavy");
    // Same 100K anchor as the main suite's `sync_cpu_heavy` (steady-state;
    // the 1K setup-dominated anchor adds nothing to counter methodology).
    let size = 100_000;
    let data: Vec<u64> = (0..size).collect();

    group.throughput(Throughput::Elements(size));

    group.bench_function("youpipe_par_map", |b| {
        b.iter(|| {
            black_box(
                youpipe::pipe(data.iter().copied())
                    .map(|x| black_box(cpu_work(x)))
                    .collect(),
            )
        });
    });

    group.bench_function("rayon_par_iter", |b| {
        b.iter(|| {
            let r: Vec<u64> = data.par_iter().map(|&x| black_box(cpu_work(x))).collect();
            black_box(r)
        });
    });

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let r: Vec<u64> = data.iter().map(|&x| black_box(cpu_work(x))).collect();
            black_box(r)
        });
    });

    group.finish();
}

fn bench_lightweight<M: Measurement>(c: &mut Criterion<M>) {
    let mut group = c.benchmark_group("perf_sync_lightweight");
    // Same 10K hot-cache anchor as the main suite's `sync_lightweight` —
    // framework-overhead regime, where per-window overhead would hurt most.
    let size = 10_000;
    let data: Vec<u64> = (0..size).collect();

    group.throughput(Throughput::Elements(size));

    group.bench_function("youpipe_par_map_warm", |b| {
        b.iter(|| {
            black_box(
                youpipe::pipe(data.iter().copied())
                    .map(|x| black_box(x.wrapping_add(1)))
                    .collect(),
            )
        });
    });

    group.bench_function("rayon_par_iter", |b| {
        b.iter(|| {
            let r: Vec<u64> = data.par_iter().map(|&x| black_box(x + 1)).collect();
            black_box(r)
        });
    });

    group.finish();
}

/// Map a `PERF_EVENT` name to a hardware event; `walltime` (handled by
/// `main`) switches the whole bench back to criterion's clock so the same
/// code can be measured under both methodologies for a drift A/B.
fn hardware_event(name: &str) -> Result<(HW, &'static str), String> {
    // The second tuple element is the canonical report name ('static on
    // purpose: criterion's ValueFormatter stores it).
    let (hw, canonical) = match name {
        "instructions" => (HW::Instructions, "instructions"),
        "cycles" => (HW::CPUCycles, "cycles"),
        "ref-cycles" => (HW::RefCPUCycles, "ref-cycles"),
        "cache-misses" => (HW::CacheMisses, "cache-misses"),
        "branch-misses" => (HW::BranchMisses, "branch-misses"),
        "branches" => (HW::BranchInstructions, "branches"),
        other => {
            return Err(format!(
                "unknown PERF_EVENT `{other}` (expected one of \
                 instructions|cycles|ref-cycles|cache-misses|branch-misses|branches|walltime)"
            ))
        },
    };
    Ok((hw, canonical))
}

/// Build the per-thread counter template for `hw`.
///
/// User-space-only attribution (`exclude_kernel` + `exclude_hv`): required
/// for unprivileged counters at `kernel.perf_event_paranoid >= 2`, and it
/// keeps interrupt/timer noise out of the counts (see module docs). The
/// fork opens each counter disabled, so the warm-up phase never leaks into
/// the first measurement window.
fn make_builder(hw: HW) -> Builder {
    let mut builder = Builder::from_hardware_event(hw);
    builder.exclude_kernel().exclude_hv();
    builder
}

fn run<M: Measurement>(mut c: Criterion<M>) {
    bench_cpu_heavy(&mut c);
    bench_lightweight(&mut c);
}

fn main() {
    let event = std::env::var("PERF_EVENT").unwrap_or_else(|_| "instructions".into());
    match event.as_str() {
        "walltime" => run(common::criterion().configure_from_args()),
        ev => match hardware_event(ev) {
            Ok((hw, name)) => run(common::criterion()
                .with_measurement(Perf::with_event_name(move || make_builder(hw), name))
                .configure_from_args()),
            Err(msg) => panic!("{msg}"),
        },
    }
    // Mirrors `criterion_main!`: the generated harness prints the final
    // summary (and honors `--test`/`--list` CLI modes) after the groups run.
    Criterion::default().configure_from_args().final_summary();
}
