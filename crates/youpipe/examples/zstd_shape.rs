//! Interactive investigation tool for the expensive-item unbalanced regime:
//! "compress many differently-sized files" (zstd-like, µs–ms per item).
//!
//! Same lognormal workload family as the criterion `zstd_shape` group in
//! `benches/unbalanced.rs` — this binary adds what a criterion group cannot
//! do: quick parameter sweeps (`sweep`), per-shape rounds (`shape`), and
//! bench-side per-thread participation instrumentation (`instr`: first/last
//! item timestamp per thread, late-arrival detection) for diagnosing
//! scheduler ramp/straggler behaviour. The 2026-09 flat-dispatch experiment
//! (docs/src/dev/scheduler.md) was diagnosed with exactly this tooling.
//!
//! ```sh
//! cargo run --release -p youpipe --example zstd_shape -- main 5
//! cargo run --release -p youpipe --example zstd_shape -- shape 5
//! cargo run --release -p youpipe --example zstd_shape -- threads 5 16
//! cargo run --release -p youpipe --example zstd_shape -- sweep 5 2000
//! cargo run --release -p youpipe --example zstd_shape -- instr 5 1.5
//! ```

// Timing/throughput math deliberately widens numeric types (ns → ms, byte
// counts → f64 shares); the values are bench-scale so precision/truncation
// lints are noise here.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::{
    cell::Cell,
    hint::black_box as bb,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use rayon::prelude::*;
use youpipe::{Workload, pipe_ref};

fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// Lognormal file-size shape (Box-Muller over a fixed-seed LCG), clamped to
/// [256 B, cap]; `iters = size × 32` ≈ 3 ns/byte — zstd magnitude. Runtime
/// iteration counts so LLVM cannot fold the kernel.
///
/// `ZSTD_SEED` overrides the seed: heavy-tail results are sensitive to which
/// chunk a monster item lands in, so cross-seed runs guard against chunk
/// boundary luck (see dev/scheduler.md "Latecomer slack").
fn gen_docs_shaped(n: usize, iters_per_byte: u32, sigma: f64, cap_b: f64) -> Vec<(u64, u32)> {
    let mut seed: u64 = std::env::var("ZSTD_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x2545_f491_4f6c_dd1d);
    let mut next_f64 = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };
    (0..n)
        .map(|i| {
            let (u1, u2) = (next_f64().max(1e-12), next_f64());
            let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
            let size = (9.0 + sigma * z).exp().clamp(256.0, cap_b) as usize;
            (i as u64, (size * iters_per_byte as usize) as u32)
        })
        .collect()
}

fn gen_docs(n: usize, iters_per_byte: u32) -> Vec<(u64, u32)> {
    gen_docs_shaped(n, iters_per_byte, 1.5, 2.0 * 1024.0 * 1024.0)
}

type Job = Box<dyn FnMut() -> f64>;

fn run_job(job: &mut dyn FnMut() -> f64, warmup: usize, measure_ms: u128) -> (f64, usize) {
    for _ in 0..warmup {
        job();
    }
    let mut iters = 0usize;
    let mut total = 0.0;
    let t0 = Instant::now();
    while t0.elapsed().as_millis() < measure_ms {
        total += job();
        iters += 1;
    }
    (total / iters as f64, iters)
}

fn finish<T>(r: T, t: Instant) -> f64 {
    let r = bb(r);
    let dt = t.elapsed().as_nanos() as f64;
    drop(r);
    dt
}

fn run(n: usize, rounds: usize, libs_fn: impl Fn() -> Vec<(&'static str, Job)>) {
    let docs = gen_docs(n, 32);
    let total_seq: f64 = docs.iter().map(|&(_, it)| f64::from(it)).sum::<f64>() * 0.095e-9;
    println!(
        "n={n} total_seq≈{:.1} ms max_item≈{:.2} ms fair_share≈{:.2} ms",
        total_seq * 1e3,
        f64::from(docs.iter().map(|&(_, it)| it).max().unwrap()) * 0.095e-3,
        total_seq * 1e3 / num_threads()
    );
    let mut results: Vec<(&'static str, Vec<f64>)> = Vec::new();
    for r in 0..rounds {
        let mut libs = libs_fn();
        if r % 2 == 0 {
            libs.reverse();
        }
        for (name, job) in &mut libs {
            let (ms, _) = run_job(
                job.as_mut(),
                if r == 0 {
                    3
                } else {
                    1
                },
                500,
            );
            match results.iter_mut().find(|(n2, _)| n2 == name) {
                Some((_, v)) => v.push(ms / 1e6),
                None => results.push((name, vec![ms / 1e6])),
            }
        }
    }
    for (name, v) in &results {
        let mut v = v.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "  {name:<20} median={:.3} ms  rounds={:?}",
            v[v.len() / 2],
            v
        );
    }
}

fn num_threads() -> f64 {
    std::thread::available_parallelism().map_or(31.0, |n| n.get() as f64)
}

// ── bench-side participation instrumentation: per-thread first/last item ──

const MAX_THREADS: usize = 512;
#[allow(clippy::declare_interior_mutable_const)]
const U64MAX: AtomicU64 = AtomicU64::new(u64::MAX);
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static FIRST: [AtomicU64; MAX_THREADS] = [U64MAX; MAX_THREADS];
static LAST: [AtomicU64; MAX_THREADS] = [U64MAX; MAX_THREADS];
static COUNT: [AtomicU64; MAX_THREADS] = [ZERO; MAX_THREADS];
static NEXT_ID: AtomicU64 = AtomicU64::new(0);
static EPOCH: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static MY_ID: Cell<usize> = const { Cell::new(usize::MAX) };
    static MY_EPOCH: Cell<u64> = const { Cell::new(0) };
}

fn now_us() -> u64 {
    use std::sync::OnceLock;
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_micros() as u64
}

fn record_item() {
    let id = MY_ID.with(|c| {
        let v = c.get();
        if v == usize::MAX {
            let v = NEXT_ID.fetch_add(1, Ordering::Relaxed) as usize;
            c.set(v);
            v
        } else {
            v
        }
    });
    let epoch = EPOCH.load(Ordering::Relaxed);
    MY_EPOCH.with(|e| {
        if e.get() != epoch {
            e.set(epoch);
            FIRST[id].store(now_us(), Ordering::Relaxed);
        }
    });
    LAST[id].store(now_us(), Ordering::Relaxed);
    COUNT[id].fetch_add(1, Ordering::Relaxed);
}

fn reset_instr() {
    EPOCH.fetch_add(1, Ordering::Relaxed);
}

fn summarize_instr(iter_start_us: u64, iter_end_us: u64, label: &str) {
    let mut rows: Vec<(u64, u64, usize, u64)> = Vec::new(); // (first_rel, last_rel, id, count)
    for (i, first) in FIRST.iter().enumerate() {
        let f = first.load(Ordering::Relaxed);
        if f != u64::MAX && f >= iter_start_us {
            rows.push((
                f - iter_start_us,
                LAST[i].load(Ordering::Relaxed) - iter_start_us,
                i,
                COUNT[i].load(Ordering::Relaxed),
            ));
        }
    }
    rows.sort_unstable();
    let dur = iter_end_us - iter_start_us;
    let n = rows.len();
    if n == 0 {
        println!("  [{label}] no data");
        return;
    }
    let ramp_p50 = rows[n / 2].0;
    let late: Vec<String> = rows
        .iter()
        .filter(|(f, ..)| *f > 1000)
        .map(|(f, _, id, c)| format!("    LATE id={id} first={f}µs count={c}"))
        .collect();
    let idle_early = rows.iter().filter(|(_, l, ..)| l + 1000 < dur).count();
    println!(
        "  [{label}] threads={n} dur={dur}µs ramp(p50)={ramp_p50}µs busy_at_end(>1ms margin)={}",
        n - idle_early
    );
    for l in late {
        println!("{l}");
    }
}

// One match arm per investigation mode keeps each experiment readable
// side-by-side; the aggregate `main` is long by design.
#[allow(clippy::too_many_lines)]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map_or("main", String::as_str);
    let rounds: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    match mode {
        // youpipe (Unbalanced + default) vs rayon at n=2000/8000
        "main" => {
            for &n in &[2000usize, 8000] {
                println!("── main n={n} ──");
                run(n, rounds, || {
                    vec![
                        (
                            "youpipe Unbalanced",
                            Box::new({
                                let docs = gen_docs(n, 32);
                                move || {
                                    let t = Instant::now();
                                    let r: Vec<u64> = pipe_ref(&docs)
                                        .with_workload(Workload::Unbalanced)
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(r, t)
                                }
                            }) as Job,
                        ),
                        (
                            "youpipe default",
                            Box::new({
                                let docs = gen_docs(n, 32);
                                move || {
                                    let t = Instant::now();
                                    let r: Vec<u64> = pipe_ref(&docs)
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(r, t)
                                }
                            }) as Job,
                        ),
                        (
                            "rayon",
                            Box::new({
                                let docs = gen_docs(n, 32);
                                move || {
                                    let t = Instant::now();
                                    let r: Vec<u64> = docs
                                        .par_iter()
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(r, t)
                                }
                            }) as Job,
                        ),
                    ]
                });
            }
        },
        // Workload::Custom oversplit sweep at fixed n
        "sweep" => {
            let n: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000);
            println!("── sweep n={n} ──");
            run(n, rounds, || {
                let mut libs: Vec<(&'static str, Job)> = vec![(
                    "rayon",
                    Box::new({
                        let docs = gen_docs(n, 32);
                        move || {
                            let t = Instant::now();
                            let r: Vec<u64> = docs
                                .par_iter()
                                .map(|&(x, it)| bb(cpu_work(x, it)))
                                .collect();
                            finish(r, t)
                        }
                    }) as Job,
                )];
                for f in [1usize, 2, 4, 8, 16, 32, 64, 128] {
                    let name: &'static str =
                        Box::leak(format!("youpipe Custom({f})").into_boxed_str());
                    libs.push((
                        name,
                        Box::new({
                            let docs = gen_docs(n, 32);
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = pipe_ref(&docs)
                                    .with_workload(Workload::Custom(
                                        std::num::NonZeroUsize::new(f).unwrap(),
                                    ))
                                    .map(|&(x, it)| bb(cpu_work(x, it)))
                                    .collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ));
                }
                libs
            });
        },
        // per-shape rounds: (label, sigma, cap_bytes)
        "shape" => {
            let shapes: &[(&str, f64, f64, usize)] = &[
                ("heavy-tail cap2MB n=2000", 1.5, 2.0 * 1024.0 * 1024.0, 2000),
                ("capped cap256KB n=2000", 1.5, 256.0 * 1024.0, 2000),
                ("balanced(sigma0) n=2000", 0.0, 2.0 * 1024.0 * 1024.0, 2000),
                ("heavy-tail cap2MB n=8000", 1.5, 2.0 * 1024.0 * 1024.0, 8000),
                ("capped cap256KB n=8000", 1.5, 256.0 * 1024.0, 8000),
                ("balanced(sigma0) n=8000", 0.0, 2.0 * 1024.0 * 1024.0, 8000),
            ];
            // `ZSTD_SHAPE_N` overrides every shape's item count: the slack
            // sweet spot depends on items per chunk, so the boundary region
            // (e.g. n=4000) needs its own pass at fixed shape.
            let n_override: Option<usize> = std::env::var("ZSTD_SHAPE_N")
                .ok()
                .and_then(|v| v.parse().ok());
            for &(label, sigma, cap, n0) in shapes {
                let n = n_override.unwrap_or(n0);
                let label: &'static str = if n == n0 {
                    label
                } else {
                    Box::leak(format!("{label} n={n}").into_boxed_str())
                };
                println!("── {label} ──");
                let mut results: Vec<(&'static str, Vec<f64>)> = Vec::new();
                for r in 0..rounds {
                    let mut libs: Vec<(&str, Job)> = vec![
                        (
                            "rayon",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = docs
                                        .par_iter()
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                        (
                            "youpipe Unb",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = pipe_ref(&docs)
                                        .with_workload(Workload::Unbalanced)
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                    ];
                    if r % 2 == 1 {
                        libs.reverse();
                    }
                    for (name, job) in &mut libs {
                        let (ns, _) = run_job(
                            job.as_mut(),
                            if r == 0 {
                                3
                            } else {
                                1
                            },
                            400,
                        );
                        match results.iter_mut().find(|(n2, _)| n2 == name) {
                            Some((_, v)) => v.push(ns / 1e6),
                            None => results.push((name, vec![ns / 1e6])),
                        }
                    }
                }
                for (name, v) in &results {
                    let mut v = v.clone();
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    println!(
                        "  {name:<14} median={:.3} ms  rounds={:?}",
                        v[v.len() / 2],
                        v
                    );
                }
            }
        },
        // thread-count A/B: logical (default global pools) vs physical cores,
        // both libraries on persistent pools — the SMT-oversubscription
        // hypothesis for the residual uniform gap (dev/scheduler.md)
        "threads" => {
            let phys: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);
            let logical = num_threads() as usize;
            println!("── threads: logical={logical} physical={phys} ──");
            let yp_phys = youpipe::ComputePool::new(phys);
            let ry_phys = std::sync::Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(phys)
                    .build()
                    .expect("rayon pool"),
            );
            let shapes: &[(&str, f64, f64, usize)] = &[
                ("uniform sigma0 n=2000", 0.0, 2.0 * 1024.0 * 1024.0, 2000),
                ("heavy-tail n=2000", 1.5, 2.0 * 1024.0 * 1024.0, 2000),
                ("uniform sigma0 n=8000", 0.0, 2.0 * 1024.0 * 1024.0, 8000),
                ("heavy-tail n=8000", 1.5, 2.0 * 1024.0 * 1024.0, 8000),
                ("capped n=8000", 1.5, 256.0 * 1024.0, 8000),
            ];
            for &(label, sigma, cap, n) in shapes {
                println!("── {label} ──");
                let mut results: Vec<(&'static str, Vec<f64>)> = Vec::new();
                for r in 0..rounds {
                    let mut libs: Vec<(&'static str, Job)> = vec![
                        (
                            "rayon@logical",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = docs
                                        .par_iter()
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                        (
                            "rayon@phys",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                let pool = std::sync::Arc::clone(&ry_phys);
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = pool.install(|| {
                                        docs.par_iter()
                                            .map(|&(x, it)| bb(cpu_work(x, it)))
                                            .collect()
                                    });
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                        (
                            "yp@logical",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = pipe_ref(&docs)
                                        .with_workload(Workload::Unbalanced)
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                        (
                            "yp@phys",
                            Box::new({
                                let docs = gen_docs_shaped(n, 32, sigma, cap);
                                let pool = yp_phys.clone();
                                move || {
                                    let t = Instant::now();
                                    let v: Vec<u64> = pipe_ref(&docs)
                                        .with_workload(Workload::Unbalanced)
                                        .with_compute_pool(pool.clone())
                                        .map(|&(x, it)| bb(cpu_work(x, it)))
                                        .collect();
                                    finish(v, t)
                                }
                            }) as Job,
                        ),
                    ];
                    if r % 2 == 1 {
                        libs.reverse();
                    }
                    for (name, job) in &mut libs {
                        let (ns, _) = run_job(
                            job.as_mut(),
                            if r == 0 {
                                3
                            } else {
                                1
                            },
                            400,
                        );
                        match results.iter_mut().find(|(n2, _)| n2 == name) {
                            Some((_, v)) => v.push(ns / 1e6),
                            None => results.push((name, vec![ns / 1e6])),
                        }
                    }
                }
                for (name, v) in &results {
                    let mut v = v.clone();
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    println!(
                        "  {name:<14} median={:.3} ms  rounds={:?}",
                        v[v.len() / 2],
                        v
                    );
                }
                let med = |name: &str| {
                    let v = &results.iter().find(|(n, _)| *n == name).unwrap().1;
                    let mut v = v.clone();
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    v[v.len() / 2]
                };
                println!(
                    "  vs rayon@logical: yp@logical {:+.1}%  yp@phys {:+.1}%  rayon@phys {:+.1}%",
                    (med("yp@logical") / med("rayon@logical") - 1.0) * 100.0,
                    (med("yp@phys") / med("rayon@logical") - 1.0) * 100.0,
                    (med("rayon@phys") / med("rayon@logical") - 1.0) * 100.0,
                );
            }
        },
        // participation instrumentation: ramp + late workers per batch
        "instr" => {
            let sigma: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1.5);
            let n = 2000usize;
            for r in 0..rounds {
                println!("── round {r} (sigma={sigma}) ──");
                for (label, use_rayon) in [("rayon", true), ("youpipe Unb", false)] {
                    let docs = gen_docs_shaped(n, 32, sigma, 2.0 * 1024.0 * 1024.0);
                    reset_instr();
                    let s = now_us();
                    let epoch_items = |&(x, it): &(u64, u32)| {
                        record_item();
                        bb(cpu_work(x, it))
                    };
                    if use_rayon {
                        let v: Vec<u64> = docs.par_iter().map(epoch_items).collect();
                        bb(v);
                    } else {
                        let v: Vec<u64> = pipe_ref(&docs)
                            .with_workload(Workload::Unbalanced)
                            .map(epoch_items)
                            .collect();
                        bb(v);
                    }
                    let e = now_us();
                    summarize_instr(s, e, label);
                }
            }
        },
        _ => {
            println!("unknown mode {mode:?}; see module docs for main|sweep|shape|threads|instr");
        },
    }
}
