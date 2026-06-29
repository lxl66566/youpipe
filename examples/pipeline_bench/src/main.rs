//! Heavy-tail document processing pipeline: comparing youpipe, rayon, tokio,
//! and sequential execution on a mixed CPU + simulated IO workload.
//!
//! Simulates a three-stage data processing pipeline (read → compute → write)
//! where document sizes follow a heavy-tailed log-normal distribution.
//! No actual disk IO — latencies are simulated with `thread::sleep` or
//! `tokio::time::sleep` to avoid SSD wear while keeping the comparison
//! reproducible.
//!
//! ```text
//! cd examples/pipeline-bench && cargo run --release
//! ```
//!
//! Environment:
//!   `N_DOCS=2000`   Number of documents (default: 2000)

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use rayon::prelude::*;
use youpipe::prelude::*;

// ── Constants ──────────────────────────────────────────────────────────

const DEFAULT_N_DOCS: usize = 2000;
const LN_MU: f64 = 9.0; // ln-scale mean (e^9 ≈ 8.1 KB)
const LN_SIGMA: f64 = 1.5; // ln-scale std dev (heavy tail)
const SIZE_MIN: usize = 256; // minimum doc size (bytes)
const SIZE_MAX: usize = 2_000_000; // maximum doc size (bytes)

// Per-byte cost factors — calibrated for ~5–8 s sequential.
const IO_READ_NS: u64 = 30; // nanoseconds per byte for simulated IO read
const IO_READ_MAX_NS: u64 = 5_000_000;
const CPU_ITERS_PER_BYTE: usize = 3; // SipHash rounds per byte
const IO_WRITE_NS: u64 = 15; // nanoseconds per byte for simulated IO write
const IO_WRITE_MAX_NS: u64 = 3_000_000;

// ── LCG PRNG (SplitMix64, deterministic) ───────────────────────────────

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Sample from a log-normal distribution via Box-Muller, clamped to [min, max].
fn log_normal(rng: &mut Lcg, mu: f64, sigma: f64, min: usize, max: usize) -> usize {
    let u1 = rng.next_f64().max(f64::EPSILON);
    let u2 = rng.next_f64().max(f64::EPSILON);
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    let size = (mu + sigma * z).exp().round() as isize;
    size.clamp(min as isize, max as isize) as usize
}

// ── Document ───────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Doc {
    id: usize,
    size: usize,
    cpu_iters: usize,
}

fn make_docs(n: usize) -> Vec<Doc> {
    let mut rng = Lcg::new(42);
    let mut docs = Vec::with_capacity(n);
    for id in 0..n {
        let size = log_normal(&mut rng, LN_MU, LN_SIGMA, SIZE_MIN, SIZE_MAX);
        let cpu_iters = std::cmp::max(1, size * CPU_ITERS_PER_BYTE);
        docs.push(Doc {
            id,
            size,
            cpu_iters,
        });
    }
    docs
}

fn io_read_dur(doc: &Doc) -> Duration {
    Duration::from_nanos((doc.size as u64 * IO_READ_NS).min(IO_READ_MAX_NS))
}

fn io_write_dur(doc: &Doc) -> Duration {
    Duration::from_nanos((doc.size as u64 * IO_WRITE_NS).min(IO_WRITE_MAX_NS))
}

// ── CPU work: heavy SipHash rounds (hard for compiler to optimise away) ─

fn cpu_analyze(doc: &Doc) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = doc.id as u64;
    for _ in 0..doc.cpu_iters {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        h.hash(&mut hasher);
        h = hasher.finish();
    }
    black_box(h) // prevent dead-code elimination of the loop
}

// ── Engine implementations ─────────────────────────────────────────────

/// Sequential baseline — one doc at a time, blocking IO, single thread.
fn run_sequential(docs: &[Doc]) -> Vec<u64> {
    docs.iter()
        .map(|doc| {
            std::thread::sleep(io_read_dur(doc));
            let result = cpu_analyze(doc);
            std::thread::sleep(io_write_dur(doc));
            result
        })
        .collect()
}

/// Rayon `par_iter` — all documents processed in batch parallel on the
/// global work-stealing pool. IO stages block rayon threads.
fn run_rayon(docs: &[Doc]) -> Vec<u64> {
    docs.par_iter()
        .map(|doc| {
            std::thread::sleep(io_read_dur(doc));
            let result = cpu_analyze(doc);
            std::thread::sleep(io_write_dur(doc));
            result
        })
        .collect()
}

/// Tokio native — async IO on the multi-thread runtime, CPU on
/// `spawn_blocking`. Uses one task per document. The spawn_blocking pool
/// grows up to 512 threads, leading to oversubscription on CPU-bound work.
#[allow(clippy::unnecessary_to_owned)]
fn run_tokio(docs: &[Doc]) -> Vec<u64> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut handles = Vec::with_capacity(docs.len());
        // tokio::spawn requires `'static`, so we must iterate owned Docs
        // (Copy → no borrow on the outer slice). clippy suggests removing
        // `.copied()` but that would introduce a non-'static borrow.
        for doc in docs.iter().copied() {
            handles.push(tokio::spawn(async move {
                tokio::time::sleep(io_read_dur(&doc)).await;
                let result = tokio::task::spawn_blocking(move || cpu_analyze(&doc))
                    .await
                    .unwrap();
                tokio::time::sleep(io_write_dur(&doc)).await;
                result
            }));
        }
        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            results.push(h.await.unwrap());
        }
        results
    })
}

/// Youpipe all-sync — all three stages run on the work-stealing compute
/// pool. Pipeline parallelism lets stages overlap, but blocking IO wastes
/// compute-pool threads.
fn run_youpipe_sync(docs: Vec<Doc>) -> Vec<u64> {
    stream(docs)
        .stage(|doc: Doc| {
            std::thread::sleep(io_read_dur(&doc));
            doc
        })
        .stage(|doc: Doc| {
            let result = cpu_analyze(&doc);
            (result, doc)
        })
        .stage(|(result, doc): (u64, Doc)| {
            std::thread::sleep(io_write_dur(&doc));
            result
        })
        .run()
}

/// Youpipe mixed — IO stages run asynchronously on the tokio runtime (M:N),
/// CPU stage runs on the work-stealing compute pool. This is the ideal
/// configuration for mixed CPU/IO workloads.
fn run_youpipe_mixed(docs: Vec<Doc>) -> Vec<u64> {
    stream(docs)
        .stage_async(|doc: Doc| async move {
            tokio::time::sleep(io_read_dur(&doc)).await;
            doc
        })
        .stage(|doc: Doc| {
            let result = cpu_analyze(&doc);
            (result, doc)
        })
        .stage_async(|(result, doc): (u64, Doc)| async move {
            tokio::time::sleep(io_write_dur(&doc)).await;
            result
        })
        .run()
}

// ── main ───────────────────────────────────────────────────────────────

fn main() {
    let n_docs = std::env::var("N_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_N_DOCS);

    let docs = make_docs(n_docs);

    // ── Workload summary ──
    let total_size: usize = docs.iter().map(|d| d.size).sum();
    let total_cpu_iters: usize = docs.iter().map(|d| d.cpu_iters).sum();

    let mut sizes: Vec<usize> = docs.iter().map(|d| d.size).collect();
    sizes.sort_unstable();
    let p50 = sizes[n_docs / 2];
    let p90 = sizes[(n_docs * 9) / 10];
    let p99 = sizes[(n_docs * 99) / 100];

    let ncpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

    println!("═══ Pipeline Benchmark ═══");
    println!();
    println!("CPU cores        : {ncpus}");
    println!("Documents        : {n_docs}");
    println!(
        "Size distribution: log-normal (μ={LN_MU}, σ={LN_SIGMA}, clipped [{SIZE_MIN}..{SIZE_MAX}] B)"
    );
    println!(
        "  median         : {p50} B  ({:.1} KiB)",
        p50 as f64 / 1024.0
    );
    println!(
        "  P90            : {p90} B  ({:.1} KiB)",
        p90 as f64 / 1024.0
    );
    println!(
        "  P99            : {p99} B  ({:.1} KiB)",
        p99 as f64 / 1024.0
    );
    println!(
        "  total          : {} B  ({:.1} MiB)",
        total_size,
        total_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Total CPU iters  : {total_cpu_iters}  ({:.1} M)",
        total_cpu_iters as f64 / 1_000_000.0
    );
    println!();
    println!("Pipeline stages:");
    println!(
        "  1. IO read  : size × {IO_READ_NS} ns, max {IO_READ_MAX_NS} ns ({:.1} ms)",
        IO_READ_MAX_NS as f64 / 1_000_000.0
    );
    println!("  2. CPU      : size × {CPU_ITERS_PER_BYTE} SipHash rounds");
    println!(
        "  3. IO write : size × {IO_WRITE_NS} ns, max {IO_WRITE_MAX_NS} ns ({:.1} ms)",
        IO_WRITE_MAX_NS as f64 / 1_000_000.0
    );
    println!();
    println!("The sync versions use `std::thread::sleep` for IO (blocks OS thread).");
    println!("The async versions use `tokio::time::sleep` (yields to runtime, M:N).");

    // ── Run each approach ──
    struct ApproachResult {
        name: &'static str,
        elapsed: Duration,
        checksum: u64,
        note: &'static str,
    }

    let mut results: Vec<ApproachResult> = Vec::new();

    macro_rules! run_one {
        ($name:expr, $expr:expr, $note:expr) => {
            let start = Instant::now();
            let vec: Vec<u64> = $expr;
            let elapsed = start.elapsed();
            let checksum = vec.iter().fold(0, |a, &b| a ^ b);
            results.push(ApproachResult {
                name: $name,
                elapsed,
                checksum,
                note: $note,
            });
            drop(vec);
        };
    }

    // ── Profile sequential run (IO read / CPU / IO write breakdown) ──
    let mut prof_io_read = Duration::ZERO;
    let mut prof_cpu = Duration::ZERO;
    let mut prof_io_write = Duration::ZERO;
    for doc in &docs {
        let t0 = Instant::now();
        std::thread::sleep(io_read_dur(doc));
        prof_io_read += t0.elapsed();

        let t0 = Instant::now();
        cpu_analyze(doc);
        prof_cpu += t0.elapsed();

        let t0 = Instant::now();
        std::thread::sleep(io_write_dur(doc));
        prof_io_write += t0.elapsed();
    }
    let prof_total = prof_io_read + prof_cpu + prof_io_write;

    println!("Profiled sequential time breakdown:");
    println!(
        "  IO read  : {:>7.3?}  ({:.0} % of total)",
        prof_io_read,
        prof_io_read.as_secs_f64() / prof_total.as_secs_f64() * 100.0
    );
    println!(
        "  CPU      : {:>7.3?}  ({:.0} % of total, ~{:.1} ns/iter)",
        prof_cpu,
        prof_cpu.as_secs_f64() / prof_total.as_secs_f64() * 100.0,
        prof_cpu.as_secs_f64() / total_cpu_iters as f64 * 1e9
    );
    println!(
        "  IO write : {:>7.3?}  ({:.0} % of total)",
        prof_io_write,
        prof_io_write.as_secs_f64() / prof_total.as_secs_f64() * 100.0
    );
    println!("  total    : {:>7.3?}", prof_total);
    println!();

    println!("Running each approach once (wall-clock, single shot)…");
    println!();

    run_one!(
        "Sequential",
        run_sequential(&docs),
        "single-threaded baseline"
    );
    run_one!(
        "Rayon par_iter",
        run_rayon(&docs),
        "batch parallel, IO blocks pool"
    );
    run_one!(
        "Tokio async+blocking",
        run_tokio(&docs),
        "async IO + spawn_blocking CPU"
    );
    run_one!(
        "Youpipe sync stages",
        run_youpipe_sync(docs.clone()),
        "all stages on compute pool"
    );
    run_one!(
        "Youpipe mixed async IO",
        run_youpipe_mixed(docs),
        "async IO + compute pool CPU"
    );

    // ── Verify correctness ──
    let ref_checksum = results[0].checksum;
    let all_ok = results.iter().all(|r| r.checksum == ref_checksum);
    assert!(all_ok, "Checksum mismatch!");
    println!("  all checksums match: {:#x}  ✓", ref_checksum);
    println!();

    // ── Final table (sorted by time) ──
    results.sort_by_key(|a| a.elapsed);

    println!("Results (fastest first):");
    println!();
    println!(
        "  {:<28} {:>9} {:>11} {:>8}  Note",
        "Strategy", "Time", "Docs/s", "Spdup",
    );
    println!("  {}", "─".repeat(80));

    let seq = results
        .iter()
        .find(|r| r.name == "Sequential")
        .map(|r| r.elapsed)
        .unwrap();
    let fastest = results[0].elapsed;

    for r in &results {
        let docs_s = n_docs as f64 / r.elapsed.as_secs_f64();
        let speedup_vs_seq = seq.as_secs_f64() / r.elapsed.as_secs_f64();
        let marker = if r.elapsed == fastest {
            "  ← fastest"
        } else {
            ""
        };
        println!(
            "  {:<28} {:>6.3?} {:>9.0} {:>6.2}×  {}{}",
            r.name, r.elapsed, docs_s, speedup_vs_seq, r.note, marker
        );
    }

    println!();
    println!("Speedup = sequential_time / approach_time.  Higher is better.");
    println!();

    // ── Analysis ──
    let yp_mixed = results
        .iter()
        .find(|r| r.name == "Youpipe mixed async IO")
        .unwrap()
        .elapsed;
    let tokio = results
        .iter()
        .find(|r| r.name == "Tokio async+blocking")
        .unwrap()
        .elapsed;
    let rayon = results
        .iter()
        .find(|r| r.name == "Rayon par_iter")
        .unwrap()
        .elapsed;
    let yp_sync = results
        .iter()
        .find(|r| r.name == "Youpipe sync stages")
        .unwrap()
        .elapsed;

    println!("Observations:");
    println!();
    println!(
        "  • Youpipe mixed async IO is {:.1}× faster than Tokio and {:.1}× faster than Rayon.",
        tokio.as_secs_f64() / yp_mixed.as_secs_f64(),
        rayon.as_secs_f64() / yp_mixed.as_secs_f64(),
    );
    println!("    Async IO (M:N) frees the compute pool for pure CPU work; pipeline overlap");
    println!("    hides the IO latency behind CPU computation.");
    println!();
    println!(
        "  • Rayon is {:.1}× faster than Youpipe sync stages.",
        yp_sync.as_secs_f64() / rayon.as_secs_f64(),
    );
    println!("    With all-sync stages on the same compute pool, pipeline splitting");
    println!("    divides threads across 3 stages, reducing effective CPU parallelism.");
    println!("    Rayon dedicates all threads to full docs (IO + CPU + IO).");
    println!();
    println!(
        "  • Tokio's spawn_blocking pool oversubscribes CPU cores (up to 512 threads vs {ncpus} cores),",
    );
    println!("    adding context-switch overhead vs a fixed-size work-stealing pool.");
    println!("    Its async IO is efficient, but CPU throughput suffers.");
    println!();
    println!(
        "  • IO accounts for {:.0}% of sequential time — without overlapping",
        (prof_io_read + prof_io_write).as_secs_f64() / prof_total.as_secs_f64() * 100.0
    );
    println!("    it is pure waste in the parallel versions. youpipe mixed hides it.");
    println!();
    println!("Try varying N_DOCS (e.g. `N_DOCS=500`) or the per-byte cost constants");
    println!("to explore how the ranking changes with IO/CPU ratio and scale.");
}
