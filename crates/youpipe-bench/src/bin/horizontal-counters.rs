//! Attribution runner for the horizontal `cpu_balanced` gap (docs/todo.md
//! 性能#1, zstd_shape/fused residuals):
//! one process, both sides of the comparison interleaved rep-by-rep, the
//! exact `cpu_balanced` workload (borrowed `u64` input, 100-iteration
//! foldable `cpu_work`), per-iteration wall times on stdout as CSV for
//! `perf stat` / `perf record` sessions.
//!
//! Two modes:
//! * `time` (default) — the measurement loop; warmup then a duration-driven
//!   timed loop per (rep, side), interleaved `A-B-A-B` with the side order
//!   flipped every rep to cancel position bias (same logic as the horizontal
//!   harness).
//! * `addr` — one kept-alive output per side after warmup, printing the
//!   output `Vec` address / alignment / capacity and the containing VMA's
//!   `AnonHugePages` from `/proc/self/smaps` (allocator / THP comparison).
//!
//! Usage:
//!   cargo run --release -p youpipe-bench --bin horizontal-counters -- \
//!       --n 2000000,4000000 --reps 5 --duration-ms 700
//!   ... --mode addr --n 4000000

use std::fmt::Write as _;
use std::hint::black_box as bb;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use youpipe::pipe_ref;

/// CPU work with per-item cost controlled by `iters` — copied verbatim from
/// benches/horizontal/unix.rs so LLVM folds it to the same closed form
/// (~6 ns/item) on both sides. Any deviation here invalidates the caliber.
fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Youpipe,
    Rayon,
}

impl Side {
    fn name(self) -> &'static str {
        match self {
            Side::Youpipe => "youpipe",
            Side::Rayon => "rayon",
        }
    }

    fn from_name(s: &str) -> Side {
        match s {
            "youpipe" => Side::Youpipe,
            "rayon" => Side::Rayon,
            other => panic!("unknown side: {other}"),
        }
    }
}

/// One timed `cpu_balanced` iteration, work replicated from
/// benches/horizontal/unix.rs (borrowed input, black-boxed result, drop
/// outside the timed region).
fn run_iter(side: Side, data: &[u64]) -> Vec<u64> {
    match side {
        Side::Youpipe => pipe_ref(data).map(|&x| bb(cpu_work(x, 100))).collect(),
        Side::Rayon => data.par_iter().map(|&x| bb(cpu_work(x, 100))).collect(),
    }
}

/// `clone_input` mirrors the horizontal harness: each side's job closes over
/// its own clone of the data. Shared-input mode exists to probe whether the
/// buffer sharing itself moves the needle.
fn measure(
    side: Side,
    data: &[u64],
    warmup: usize,
    duration: Duration,
    rep: usize,
    clone_input: bool,
) {
    let owned;
    let data = if clone_input {
        owned = data.to_vec();
        owned.as_slice()
    } else {
        data
    };
    for _ in 0..warmup {
        drop(run_iter(side, data));
    }
    let t0 = Instant::now();
    let mut iters = 0usize;
    // Buffered: a per-iteration write syscall (~5-20 µs) lengthens the
    // inter-iteration gap enough to push idle workers past their spin/yield
    // window into a full park, inflating youpipe's per-iteration wake
    // cascade by 3-4x context switches and +30% wall — measured 2026-09-25.
    // Keep the gap at plain allocator cost, like the horizontal harness.
    let mut out = String::new();
    while t0.elapsed() < duration {
        let t = Instant::now();
        let r = run_iter(side, data);
        let r = bb(r);
        let ns = t.elapsed().as_nanos();
        drop(r);
        let _ = writeln!(out, "{},{},{},{},{}", side.name(), data.len(), rep, iters, ns);
        iters += 1;
    }
    print!("{out}");
}

/// Print the output `Vec`'s address/alignment/size and the containing VMA's
/// hugepage state: allocator + first-touch caliber comparison.
fn probe_addr(side: Side, data: &[u64], warmup: usize) {
    for _ in 0..warmup {
        drop(run_iter(side, data));
    }
    let out = run_iter(side, data);
    let ptr = out.as_ptr() as usize;
    println!(
        "{}: out_ptr={ptr:#x} len={} cap={} align64={} align4096={} align2M={}",
        side.name(),
        out.len(),
        out.capacity(),
        ptr % 64,
        ptr % 4096,
        ptr % (2 * 1024 * 1024),
    );
    println!(
        "{}: in_ptr={:#x} align64={} align4096={}",
        side.name(),
        data.as_ptr() as usize,
        data.as_ptr() as usize % 64,
        data.as_ptr() as usize % 4096,
    );
    if let Some(info) = vma_of(ptr) {
        println!(
            "{}: vma {}-{} size={} rss={} anon_hugepages={}",
            side.name(),
            info.start,
            info.end,
            info.size_kb,
            info.rss_kb,
            info.anon_hugepages_kb,
        );
    }
}

struct VmaInfo {
    start: usize,
    end: usize,
    size_kb: usize,
    rss_kb: usize,
    anon_hugepages_kb: usize,
}

/// Find the VMA containing `addr` and extract its Size/RSS/AnonHugePages.
fn vma_of(addr: usize) -> Option<VmaInfo> {
    let smaps = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut in_range = false;
    let mut info: Option<VmaInfo> = None;
    for line in smaps.lines() {
        if let Some((range, _perms)) = line.split_once(' ') {
            if let Some((s, e)) = range.split_once('-') {
                if let (Ok(s), Ok(e)) = (usize::from_str_radix(s, 16), usize::from_str_radix(e, 16))
                {
                    in_range = addr >= s && addr < e;
                    if in_range {
                        info = Some(VmaInfo {
                            start: s,
                            end: e,
                            size_kb: 0,
                            rss_kb: 0,
                            anon_hugepages_kb: 0,
                        });
                    }
                    continue;
                }
            }
        }
        if in_range {
            if let (Some(i), Some((k, kb))) = (
                info.as_mut(),
                line.trim().split_once(':').and_then(|(k, v)| {
                    v.trim()
                        .split(' ')
                        .next()
                        .and_then(|n| n.parse::<usize>().ok())
                        .map(|kb| (k, kb))
                }),
            ) {
                match k {
                    "Size" => i.size_kb = kb,
                    "Rss" => i.rss_kb = kb,
                    "AnonHugePages" => i.anon_hugepages_kb = kb,
                    _ => {}
                }
            }
        }
    }
    info
}

struct Config {
    ns: Vec<usize>,
    reps: usize,
    duration: Duration,
    warmup: usize,
    addr_mode: bool,
    /// Which sides to run: `both` (interleaved, default), or one side alone
    /// for separate-process A/B (pool cross-interference probe).
    sides: Vec<Side>,
    clone_input: bool,
}

fn parse_args() -> Config {
    let mut cfg = Config {
        ns: vec![2_000_000, 4_000_000],
        reps: 5,
        duration: Duration::from_millis(700),
        warmup: 3,
        addr_mode: false,
        sides: vec![Side::Youpipe, Side::Rayon],
        clone_input: true,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || {
            args.next()
                .unwrap_or_else(|| panic!("{a} requires a value"))
        };
        match a.as_str() {
            "--n" => cfg.ns = val().split(',').map(|s| s.parse().expect("n")).collect(),
            "--reps" => cfg.reps = val().parse().expect("reps"),
            "--duration-ms" => {
                cfg.duration = Duration::from_millis(val().parse().expect("duration-ms"));
            },
            "--warmup" => cfg.warmup = val().parse().expect("warmup"),
            "--mode" => cfg.addr_mode = val() == "addr",
            "--sides" => {
                cfg.sides = val().split(',').map(Side::from_name).collect();
            },
            "--input" => cfg.clone_input = val() == "clone",
            other => panic!("unknown arg: {other}"),
        }
    }
    cfg
}

fn main() {
    let cfg = parse_args();
    // Both sides use their process-global pools, warmed by the first rep's
    // warmup iterations — same shared-runtime shape as the horizontal bench.
    for &n in &cfg.ns {
        let data: Vec<u64> = (0..n as u64).collect();
        if cfg.addr_mode {
            probe_addr(Side::Youpipe, &data, cfg.warmup);
            probe_addr(Side::Rayon, &data, cfg.warmup);
            continue;
        }
        let mut times: Vec<(Side, Vec<f64>)> =
            cfg.sides.iter().map(|&s| (s, Vec::new())).collect();
        for rep in 0..cfg.reps {
            // Flip the side order every rep: ABCABC position-bias cancel.
            let order = if rep % 2 == 0 {
                cfg.sides.clone()
            } else {
                cfg.sides.iter().rev().copied().collect()
            };
            for &side in &order {
                measure(side, &data, cfg.warmup, cfg.duration, rep, cfg.clone_input);
                // Slot index is side-stable regardless of run order.
                let slot = times.iter_mut().find(|(s, _)| *s == side).unwrap();
                let t = Instant::now();
                let r = run_iter(side, &data);
                drop(bb(r));
                slot.1.push(t.elapsed().as_nanos() as f64);
            }
        }
        for (side, ts) in &times {
            let mut v = ts.clone();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            eprintln!(
                "n={:<8} {:<8} median={:>8.3} ms ({} samples)",
                n,
                side.name(),
                v[v.len() / 2] / 1e6,
                v.len(),
            );
        }
        if times.iter().all(|(s, _)| cfg.sides.contains(s)) && cfg.sides.len() == 2 {
            let med = |s: Side| {
                let mut v = times.iter().find(|(x, _)| *x == s).unwrap().1.clone();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v[v.len() / 2]
            };
            eprintln!(
                "n={:<8} rayon/youpipe = {:.3}",
                n,
                med(Side::Rayon) / med(Side::Youpipe)
            );
        }
    }
}
