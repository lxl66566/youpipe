//! Horizontal cross-library comparison benchmark (exported as JSON for
//! `perf/plot-horizontal.py`).
//!
//! Unlike the criterion benches (which compare youpipe variants against one
//! alternative in depth), this target answers "what should a user pick for a
//! given workload" across the whole ecosystem: youpipe, rayon, tokio,
//! `futures::stream`, and hand-written `std::thread` baselines. Seven
//! representative scenarios cover the CPU/IO × balanced/unbalanced ×
//! short/long-task matrix plus two realistic mixed sync+async pipelines.
//!
//! Methodology (mirrors `perf/bench-suite`, proven against this machine's
//! ±10-30 % inter-run drift):
//! * **Interleaved rounds, ABCABC not AABBCC** — every round runs every (scenario, batch, library)
//!   once, and odd rounds reverse the library order to cancel position bias. Verdicts use the
//!   median across rounds.
//! * **Per-iteration timing with setup excluded** — the closure times only the workload (`Instant`
//!   around the run, input rebuild / warm-clone outside the timed region), same rationale as
//!   `sync_vs_rayon`.
//! * **Shared runtimes** — one tokio runtime + `TokioPool` handle for the whole process;
//!   per-iteration runtime construction (~ms) would dominate small batches and is not what a real
//!   application does.
//! * Simulated IO uses sleeps (never touches SSD); the realistic web scenario talks HTTP/1.1 over a
//!   loopback Unix socket served by an in-process mock server with a controllable latency
//!   distribution.
//!
//! ```sh
//! cargo bench --bench horizontal -- --rounds 5 --out target/horizontal/results.json
//! python3 perf/plot-horizontal.py target/horizontal/results.json
//! ```

// Bench statistics accumulate counters into f64; the casts below lose at most
// mantissa bits, irrelevant at the 2-8 % cross-round drift these numbers live at.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::{
    fmt::Write as _,
    hint::black_box as bb,
    process::Command,
    sync::{Arc, OnceLock, atomic::Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::{future::join_all, prelude::*};
use rayon::prelude::*;
use youpipe::{ComputePool, PipelineConfig, TokioPool, Workload, pipe, stream};

// ── CLI / harness knobs ──

struct Config {
    rounds: usize,
    measure_ms: u128,
    warmup_first: usize,
    warmup_rest: usize,
    out: String,
    /// Only run scenarios whose name contains one of these substrings.
    scenarios: Vec<String>,
}

fn parse_args() -> Config {
    let mut cfg = Config {
        rounds: 5,
        measure_ms: 700,
        warmup_first: 5,
        warmup_rest: 2,
        out: "target/horizontal/results.json".to_owned(),
        scenarios: Vec::new(),
    };
    // Drop cargo's own `--bench <name>` passthrough pair, if present.
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    if let Some(pos) = raw.iter().position(|a| a == "--bench") {
        raw.drain(pos..=(pos + 1).min(raw.len() - 1));
    }
    let mut args = raw.into_iter();
    while let Some(a) = args.next() {
        let mut val = |name: &str| {
            args.next()
                .unwrap_or_else(|| panic!("{name} requires a value"))
        };
        match a.as_str() {
            "--rounds" => cfg.rounds = val("--rounds").parse().expect("rounds"),
            "--measure-ms" => cfg.measure_ms = val("--measure-ms").parse().expect("measure-ms"),
            "--warmup-first" => {
                cfg.warmup_first = val("--warmup-first").parse().expect("warmup-first");
            },
            "--warmup-rest" => cfg.warmup_rest = val("--warmup-rest").parse().expect("warmup-rest"),
            "--out" => cfg.out = val("--out"),
            "--scenarios" => {
                cfg.scenarios = val("--scenarios").split(',').map(str::to_owned).collect();
            },
            other => panic!("unknown arg: {other}"),
        }
    }
    cfg
}

// ── Shared runtimes (built once, reused across every iteration) ──

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

fn tokio_rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

/// Fresh `TokioPool` views over one shared underlying runtime, so per-run
/// state is reset without paying the ~ms runtime build per iteration.
fn tokio_pool() -> TokioPool {
    static POOL: OnceLock<TokioPool> = OnceLock::new();
    let pool = POOL.get_or_init(|| TokioPool::build(num_cpus()).expect("youpipe async pool"));
    TokioPool::new(pool.handle().clone(), num_cpus())
}

/// Oversubscribed compute pool for blocking-IO sync stages (512 threads,
/// matching tokio's blocking pool; silently clamped to MAX_COMPUTE_WORKERS).
fn big_compute_pool() -> ComputePool {
    static POOL: OnceLock<ComputePool> = OnceLock::new();
    POOL.get_or_init(|| ComputePool::new(512)).clone()
}

fn io_config() -> PipelineConfig {
    PipelineConfig::default().with_io_concurrency(512)
}

/// io_concurrency set above the largest batch, so async-stage fan-out is not
/// the limiter. Aligns youpipe with the tokio spawn-per-item baseline's
/// effectively unbounded in-flight count in the high-concurrency scenarios
/// (io_async is the deliberate finite-512 comparison instead).
fn unbounded_io_config() -> PipelineConfig {
    PipelineConfig::default().with_io_concurrency(4096)
}

// ── Workload primitives ──

/// CPU work with per-item cost controlled by `iters`.
fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// IO latency: ~90 % at 1 ms, ~10 % at 8 ms (network/disk tail). Deliberately
/// ≥ tokio's coarse-timer granularity (~1 ms) so the bench measures async
/// concurrency, not timer overhead (same distribution as `io_async.rs`).
fn io_latency(i: usize) -> Duration {
    if i % 10 == 0 {
        Duration::from_millis(8)
    } else {
        Duration::from_millis(1)
    }
}

fn blocking_io(x: u64, dur: Duration) -> u64 {
    std::thread::sleep(dur);
    x.wrapping_add(1)
}

async fn async_io(x: u64, dur: Duration) -> u64 {
    tokio::time::sleep(dur).await;
    x.wrapping_add(1)
}

/// Rebuild + cache-warm the input outside the timed region. youpipe's fused
/// `pipe()` takes ownership while rayon/std borrow a warm `&[T]`; without
/// warming, the fresh clone arrives cold-from-RAM and measures memcpy latency
/// instead of the framework (see docs/benchmarks.md).
fn warm_clone<T: Copy>(src: &[T]) -> Vec<T> {
    let v = src.to_vec();
    // Read the rebuilt buffer once to pull it into cache: the clone itself
    // reads `src` but writes a fresh allocation, which arrives cold.
    let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), size_of_val(&v[..])) };
    let mut acc = 0u64;
    for chunk in bytes.chunks_exact(8) {
        acc = acc.wrapping_add(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    bb(acc);
    v
}

/// ~90 % cheap items (5 iters), ~10 % heavy (5000 iters) — a 1000× spread.
fn skewed_cpu(size: usize) -> Vec<(u64, u32)> {
    (0..size)
        .map(|i| {
            let iters = if i % 10 == 0 {
                5000
            } else {
                5
            };
            (i as u64, iters)
        })
        .collect()
}

/// Log-normal document sizes (Box-Muller over a fixed-seed LCG), clamped to
/// [256 B, 2 MB]. Same shape as perf/pipeline-bench: heavy-tailed, P99 ≈
/// 259 KiB.
fn gen_doc_sizes(n: usize) -> Vec<usize> {
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next_f64 = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };
    (0..n)
        .map(|_| {
            let (u1, u2) = (next_f64().max(1e-12), next_f64());
            let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
            let size = (9.0 + 1.5 * z).exp();
            size.clamp(256.0, 2.0 * 1024.0 * 1024.0) as usize
        })
        .collect()
}

/// Parse stage of the realistic scenarios: 3 SipHash rounds per 64-byte
/// chunk — pure CPU, cost proportional to payload size.
fn parse_hash(sz: usize) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for chunk in 0..(sz / 64).max(1) {
        h.write(&chunk.to_le_bytes());
        h.write(&chunk.to_le_bytes());
        h.write(&chunk.to_le_bytes());
    }
    h.finish()
}

/// Parse stage of the web scenario: cheap fixed-cost CPU work per response.
fn web_parse(body: &[u8]) -> u64 {
    cpu_work(u64::from(body[0]) + body.len() as u64, 200)
}

/// Realistic-scenario simulated IO: fetch ≈ 30 ns/B (cap 5 ms), save ≈
/// 15 ns/B (cap 3 ms) — same shape as perf/pipeline-bench. All sleeps, no
/// disk touched.
fn fetch_dur(sz: usize) -> Duration {
    Duration::from_nanos((sz as u64 * 30).min(5_000_000))
}

fn save_dur(sz: usize) -> Duration {
    Duration::from_nanos((sz as u64 * 15).min(3_000_000))
}

/// Stop the clock after black-boxing the result: `bb` forces every output
/// element to be materialized (otherwise LLVM may DCE the whole map chain,
/// whose items have no side effects), while the deallocation of `r` happens
/// after `dt` is taken.
fn finish<T>(r: T, t: Instant) -> f64 {
    let r = bb(r);
    let dt = t.elapsed().as_nanos() as f64;
    drop(r);
    dt
}

// ── Mock HTTP server (Unix socket — no TCP TIME_WAIT buildup, no SSD) ──

const RESP_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n";
const BODY_LEN: usize = 1024;

/// One mock HTTP/1.1-over-UDS server. Each request `GET /N` sleeps for
/// `io_latency(N)` then returns a fixed 1 KiB body. Latency is controlled
/// server-side so every client framework sees the identical response-time
/// distribution.
fn start_mock_server() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        let path = format!("/tmp/youpipe-bench-mock-{}.sock", std::process::id());
        let _ = std::fs::remove_file(&path);
        // Tokio's bind registers the fd with the reactor — it must run
        // inside the runtime context even though the accept loop then runs
        // as a detached task.
        let listener = tokio_rt()
            .block_on(async { tokio::net::UnixListener::bind(&path).expect("bind mock server") });
        tokio_rt().spawn(async move {
            loop {
                let Ok((conn, _)) = listener.accept().await else {
                    break
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut conn = conn;
                    // Read until the end of the request head (\r\n\r\n).
                    let mut buf = Vec::with_capacity(64);
                    let mut chunk = [0u8; 64];
                    while !buf.ends_with(b"\r\n\r\n") {
                        let Ok(n) = conn.read(&mut chunk).await else {
                            return
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    // Parse the index out of "GET /N HTTP/1.1".
                    let idx: usize = buf
                        .split(|&b| b == b' ')
                        .nth(1)
                        .and_then(|p| p.strip_prefix(b"/"))
                        .and_then(|p| std::str::from_utf8(p).ok())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    tokio::time::sleep(io_latency(idx)).await;
                    let mut resp = RESP_HEAD.to_vec();
                    resp.resize(RESP_HEAD.len() + BODY_LEN, b'x');
                    let _ = conn.write_all(&resp).await;
                });
            }
        });
        path
    })
}

/// One request-response round trip: connect, GET, read the full response.
async fn http_get(path: &str, i: usize) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::UnixStream::connect(path)
        .await
        .expect("connect mock");
    conn.write_all(format!("GET /{i} HTTP/1.1\r\nHost: bench\r\n\r\n").as_bytes())
        .await
        .expect("write request");
    let mut head = vec![0u8; RESP_HEAD.len()];
    conn.read_exact(&mut head).await.expect("read head");
    let mut body = vec![0u8; BODY_LEN];
    conn.read_exact(&mut body).await.expect("read body");
    body
}

// ── Baseline implementations (the "what would I write without youpipe" set) ──

/// Hand-written parallel map: split into N equal chunks, one thread per chunk.
/// The classic `std::thread::scope` baseline a user writes without a library.
fn std_chunked_map<T: Copy + Sync, R: Send>(input: &[T], f: impl Fn(T) -> R + Sync) -> Vec<R> {
    let chunk_len = input.len().div_ceil(num_cpus());
    std::thread::scope(|s| {
        let handles: Vec<_> = input
            .chunks(chunk_len)
            .map(|chunk| s.spawn(|| chunk.iter().map(|&x| f(x)).collect::<Vec<R>>()))
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

// ── Scenario / harness model ──

/// One (library, batch) measurement cell: returns the per-iteration wall time
/// in nanoseconds, timing only the workload itself (setup excluded).
type Job = Box<dyn FnMut() -> f64>;

struct Batch {
    n: usize,
    libs: Vec<(&'static str, Job)>,
}

struct Scenario {
    name: &'static str,
    batches: Vec<Batch>,
}

struct ResultRec {
    scenario: &'static str,
    lib: &'static str,
    n: usize,
    /// Per-round per-iteration times, nanoseconds.
    rounds_ns: Vec<f64>,
}

/// Run `job` for `warmup` untimed iterations, then loop until `measure_ms`
/// has elapsed, averaging the per-iteration times (each timed internally so
/// setup stays out of the measurement).
fn measure(job: &mut Job, warmup: usize, measure_ms: u128) -> f64 {
    for _ in 0..warmup {
        job();
    }
    let mut total_ns = 0.0;
    let mut iters = 0usize;
    let t0 = Instant::now();
    while t0.elapsed().as_millis() < measure_ms {
        total_ns += job();
        iters += 1;
    }
    total_ns / iters as f64
}

fn run_all(cfg: &Config) -> Vec<ResultRec> {
    let mut scenarios = build_scenarios();
    let mut recs: Vec<ResultRec> = Vec::new();
    for round in 0..cfg.rounds {
        let order = if round % 2 == 0 {
            "forward"
        } else {
            "reversed"
        };
        eprintln!(
            "──── round {}/{}, order {order} ────",
            round + 1,
            cfg.rounds
        );
        let warmup = if round == 0 {
            cfg.warmup_first
        } else {
            cfg.warmup_rest
        };
        for sc in &mut scenarios {
            if !cfg.scenarios.is_empty()
                && !cfg.scenarios.iter().any(|s| sc.name.contains(s.as_str()))
            {
                continue;
            }
            for batch in &mut sc.batches {
                // ABCABC interleaving: reverse the library order on odd rounds
                // to cancel position bias.
                let iter: Box<dyn Iterator<Item = &mut (&'static str, Job)>> = if round % 2 == 0 {
                    Box::new(batch.libs.iter_mut())
                } else {
                    Box::new(batch.libs.iter_mut().rev())
                };
                for (lib, job) in iter {
                    let ns = measure(job, warmup, cfg.measure_ms);
                    eprintln!(
                        "  {:<16} n={:<7} {:<22} {:>10.2} ms",
                        sc.name,
                        batch.n,
                        lib,
                        ns / 1e6
                    );
                    recs.push(ResultRec {
                        scenario: sc.name,
                        lib,
                        n: batch.n,
                        rounds_ns: vec![ns],
                    });
                }
            }
        }
    }
    // Collapse into one record per (scenario, lib, n) holding all its rounds.
    let mut merged: Vec<ResultRec> = Vec::new();
    for r in recs {
        if let Some(m) = merged
            .iter_mut()
            .find(|m| m.scenario == r.scenario && m.lib == r.lib && m.n == r.n)
        {
            m.rounds_ns.extend(r.rounds_ns);
        } else {
            merged.push(r);
        }
    }
    merged
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn write_json(cfg: &Config, recs: &[ResultRec]) -> std::io::Result<()> {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{{\n\"meta\": {{\n  \"timestamp\": \"{}\",\n  \"cpus\": {},\n  \"hostname\": \"{}\",\n  \
         \"git_rev\": \"{}\",\n  \"rounds\": {},\n  \"measure_ms\": {},\n  \"statistic\": \
         \"median over rounds of per-iteration wall time\"\n}},\n\"results\": [\n",
        now_rfc3339(),
        num_cpus(),
        hostname(),
        git_rev(),
        cfg.rounds,
        cfg.measure_ms,
    );
    for (i, r) in recs.iter().enumerate() {
        let m = median(&mut r.rounds_ns.clone());
        let rounds_str = r
            .rounds_ns
            .iter()
            .map(|v| format!("{v:.1}"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(
            out,
            "{{\"scenario\": \"{}\", \"lib\": \"{}\", \"n\": {}, \"rounds_ns\": [{}], \
             \"median_ns\": {m:.1}}}{}",
            r.scenario,
            r.lib,
            r.n,
            rounds_str,
            if i + 1 == recs.len() {
                ""
            } else {
                ","
            },
        );
    }
    let _ = writeln!(out, "]}}");
    if let Some(parent) = std::path::Path::new(&cfg.out).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&cfg.out, out)
}

fn hostname() -> String {
    Command::new("hostname")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_owned(),
        )
}

fn git_rev() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_owned(),
        )
}

/// Minimal RFC3339 UTC timestamp (no chrono dependency).
fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d) = civil_from_days((secs / 86400) as i64);
    format!(
        "{y:04}-{mo:02}-{d:02}T{:<02}:{:<02}:{:<02}Z",
        secs / 3600 % 24,
        secs / 60 % 60,
        secs % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    (
        y + i64::from(mp < 2),
        (mp + i64::from(mp < 2) * 10 - i64::from(mp >= 2) * 2) as u32,
        (doy - (153 * mp + 2) / 5 + 1) as u32,
    )
}

// ── Scenarios ──

fn build_scenarios() -> Vec<Scenario> {
    vec![
        cpu_balanced(),
        cpu_unbalanced(),
        io_async(),
        io_blocking(),
        mixed_cpu_io(),
        real_doc(),
        real_web(),
    ]
}

/// S1: balanced CPU-heavy map (100 iters/item ≈ 100 ns). The canonical
/// parallel-map workload; the batch sweep shows how fixed scheduling overhead
/// amortizes with data volume.
fn cpu_balanced() -> Scenario {
    let batches = [1_000usize, 10_000, 100_000, 1_000_000]
        .into_iter()
        .map(|n| {
            let data: Vec<u64> = (0..n as u64).collect();
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe",
                        Box::new({
                            let data = data.clone();
                            move || {
                                let v = warm_clone(&data);
                                let t = Instant::now();
                                let r: Vec<u64> = pipe(v).map(|x| bb(cpu_work(x, 100))).collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "rayon",
                        Box::new({
                            let data = data.clone();
                            move || {
                                // Same input lifecycle as the youpipe job:
                                // fresh warm clone (untimed) whose buffer is
                                // consumed — and freed — inside the timed
                                // region. `par_iter` borrows the same `data`
                                // every iteration, so its timed region frees
                                // nothing and its cache state never sees the
                                // 8 MB clone thrash; `into_par_iter` matches
                                // ownership costs like for like.
                                let v = warm_clone(&data);
                                let t = Instant::now();
                                let r: Vec<u64> =
                                    v.into_par_iter().map(|x| bb(cpu_work(x, 100))).collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "rayon (borrowed)",
                        Box::new({
                            let data = data.clone();
                            move || {
                                // rayon's natural API: borrow the warm input,
                                // nothing freed inside the timed region. Kept
                                // as a separate row so the chart shows both
                                // readings: like-for-like memory lifecycle
                                // (the `rayon` row) vs each library's most
                                // idiomatic call (this row).
                                let t = Instant::now();
                                let r: Vec<u64> =
                                    data.par_iter().map(|&x| bb(cpu_work(x, 100))).collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "std threads",
                        Box::new(move || {
                            let t = Instant::now();
                            let r = std_chunked_map(&data, |x| bb(cpu_work(x, 100)));
                            finish(r, t)
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "cpu_balanced",
        batches,
    }
}

/// S2: skewed CPU cost — ~10 % of items are 1000× heavier. Equal-sized static
/// chunks strand slow items in a few threads; work stealing rebalances.
fn cpu_unbalanced() -> Scenario {
    let batches = [10_000usize, 100_000]
        .into_iter()
        .map(|n| {
            let data: Vec<(u64, u32)> = skewed_cpu(n);
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe (Unbalanced)",
                        Box::new({
                            let data = data.clone();
                            move || {
                                let v = warm_clone(&data);
                                let t = Instant::now();
                                let r: Vec<u64> = pipe(v)
                                    .with_workload(Workload::Unbalanced)
                                    .map(|(x, iters)| bb(cpu_work(x, iters)))
                                    .collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "youpipe (default)",
                        Box::new({
                            let data = data.clone();
                            move || {
                                let v = warm_clone(&data);
                                let t = Instant::now();
                                let r: Vec<u64> =
                                    pipe(v).map(|(x, iters)| bb(cpu_work(x, iters))).collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "rayon",
                        Box::new({
                            let data = data.clone();
                            move || {
                                // Same input lifecycle as the youpipe jobs
                                // (fresh warm clone, freed inside the timed
                                // region) — see cpu_balanced's rayon job.
                                let v = warm_clone(&data);
                                let t = Instant::now();
                                let r: Vec<u64> = v
                                    .into_par_iter()
                                    .map(|(x, iters)| bb(cpu_work(x, iters)))
                                    .collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "rayon (borrowed)",
                        Box::new({
                            let data = data.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = data
                                    .par_iter()
                                    .map(|&(x, iters)| bb(cpu_work(x, iters)))
                                    .collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "std threads",
                        Box::new({
                            let data = data.clone();
                            move || {
                                let t = Instant::now();
                                let r = std_chunked_map(&data, |(x, iters)| bb(cpu_work(x, iters)));
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "cpu_unbalanced",
        batches,
    }
}

/// S3: pure async IO (yielding waits, 1 ms / 8 ms tail, 512 concurrent).
/// The regime where M:N multiplexing beats one-thread-per-wait.
fn io_async() -> Scenario {
    let batches = [500usize, 2_000, 5_000]
        .into_iter()
        .map(|n| {
            let tasks: Vec<(u64, Duration)> = (0..n).map(|i| (i as u64, io_latency(i))).collect();
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r = stream(tasks.clone())
                                    .with_config(io_config())
                                    .with_async_pool(tokio_pool())
                                    .stage_async(|(x, dur): (u64, Duration)| async move {
                                        async_io(x, dur).await
                                    })
                                    .run();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "tokio",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    let sem = Arc::new(tokio::sync::Semaphore::new(512));
                                    let mut handles = Vec::with_capacity(tasks.len());
                                    for &(x, dur) in &tasks {
                                        let permit = sem.clone().acquire_owned().await.unwrap();
                                        handles.push(tokio::spawn(async move {
                                            let _permit = permit;
                                            async_io(x, dur).await
                                        }));
                                    }
                                    join_all(handles)
                                        .await
                                        .into_iter()
                                        .map(|h| h.unwrap())
                                        .collect()
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "futures",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    stream::iter(tasks.iter().copied())
                                        .map(|(x, dur)| async move { async_io(x, dur).await })
                                        .buffer_unordered(512)
                                        .collect()
                                        .await
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "io_async",
        batches,
    }
}

/// S4: blocking IO (thread-stalling waits). Concurrency is capped by thread
/// count; the comparison shows why oversubscription (or M:N async) matters.
fn io_blocking() -> Scenario {
    let batches = [500usize, 2_000]
        .into_iter()
        .map(|n| {
            let tasks: Vec<(u64, Duration)> = (0..n).map(|i| (i as u64, io_latency(i))).collect();
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe (32 thr)",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r = stream(tasks.clone())
                                    .stage(|(x, dur): (u64, Duration)| bb(blocking_io(x, dur)))
                                    .run();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "youpipe (512 thr)",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r = stream(tasks.clone())
                                    .with_compute_pool(big_compute_pool())
                                    .stage(|(x, dur): (u64, Duration)| bb(blocking_io(x, dur)))
                                    .run();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "tokio",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    let mut handles = Vec::with_capacity(tasks.len());
                                    for &(x, dur) in &tasks {
                                        handles.push(tokio::task::spawn_blocking(move || {
                                            blocking_io(x, dur)
                                        }));
                                    }
                                    join_all(handles)
                                        .await
                                        .into_iter()
                                        .map(|h| h.unwrap())
                                        .collect()
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "std threads",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = std::thread::scope(|s| {
                                    let handles: Vec<_> = tasks
                                        .iter()
                                        .map(|&(x, dur)| s.spawn(move || blocking_io(x, dur)))
                                        .collect();
                                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "io_blocking",
        batches,
    }
}

/// S5: mixed load — sync CPU stage (100 iters/item) then async IO stage
/// (1 ms / 8 ms tail). Overlapping CPU and IO on separate pools is the
/// mixed-regime selling point.
fn mixed_cpu_io() -> Scenario {
    let batches = [500usize, 2_000]
        .into_iter()
        .map(|n| {
            let tasks: Vec<(u64, u32, Duration)> =
                (0..n).map(|i| (i as u64, 100, io_latency(i))).collect();
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r = stream(tasks.clone())
                                    .with_config(unbounded_io_config())
                                    .with_async_pool(tokio_pool())
                                    .stage(|(x, iters, dur): (u64, u32, Duration)| {
                                        (bb(cpu_work(x, iters)), dur)
                                    })
                                    .stage_async(|(v, dur): (u64, Duration)| async move {
                                        async_io(v, dur).await
                                    })
                                    .run();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "tokio",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    // Stage 1: blocking CPU on the blocking pool,
                                    // streamed into a bounded channel.
                                    let (tx1, mut rx1) =
                                        tokio::sync::mpsc::channel::<(u64, Duration)>(256);
                                    for &(x, iters, dur) in &tasks {
                                        let tx1 = tx1.clone();
                                        tokio::task::spawn_blocking(move || {
                                            let _ = tx1.blocking_send((cpu_work(x, iters), dur));
                                        });
                                    }
                                    drop(tx1);
                                    // Relay task: stage 2 spawns one async IO per item.
                                    let relay = tokio::spawn(async move {
                                        let mut handles = Vec::new();
                                        while let Some((v, dur)) = rx1.recv().await {
                                            handles.push(tokio::spawn(async move {
                                                async_io(v, dur).await
                                            }));
                                        }
                                        handles
                                    });
                                    let handles = relay.await.unwrap();
                                    join_all(handles)
                                        .await
                                        .into_iter()
                                        .map(|h| h.unwrap())
                                        .collect()
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "futures",
                        Box::new({
                            let tasks = tasks.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    stream::iter(tasks.iter().copied())
                                        .map(|(x, iters, dur)| {
                                            let v = cpu_work(x, iters);
                                            async move { async_io(v, dur).await }
                                        })
                                        .buffer_unordered(4096)
                                        .collect()
                                        .await
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "mixed_cpu_io",
        batches,
    }
}

/// S6: realistic document pipeline (three stages, sync + async mixed):
/// fetch (async IO) → parse (CPU) → save (async IO), log-normal heavy-tailed
/// document sizes. Adapted from perf/pipeline-bench; all IO is sleeps.
fn real_doc() -> Scenario {
    let batches = [1_000usize, 4_000]
        .into_iter()
        .map(|n| {
            let docs: Vec<(usize, u64)> = gen_doc_sizes(n)
                .into_iter()
                .enumerate()
                .map(|(i, sz)| (sz, i as u64))
                .collect();
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe",
                        Box::new({
                            let docs = docs.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = stream(docs.clone())
                                    .with_config(unbounded_io_config())
                                    .with_async_pool(tokio_pool())
                                    .stage_async(move |(sz, i): (usize, u64)| async move {
                                        async_io(i, fetch_dur(sz)).await;
                                        sz
                                    })
                                    .stage(|sz: usize| (parse_hash(sz), sz))
                                    .stage_async(|(h, sz): (u64, usize)| async move {
                                        async_io(h, save_dur(sz)).await
                                    })
                                    .run();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "tokio",
                        Box::new({
                            let docs = docs.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = tokio_rt().block_on(async {
                                    // Stage 1: spawn one fetch task per document,
                                    // streamed into a bounded channel.
                                    let (tx1, mut rx1) = tokio::sync::mpsc::channel::<usize>(128);
                                    for &(sz, i) in &docs {
                                        tokio::spawn({
                                            let tx1 = tx1.clone();
                                            async move {
                                                async_io(i, fetch_dur(sz)).await;
                                                let _ = tx1.send(sz).await;
                                            }
                                        });
                                    }
                                    drop(tx1);
                                    // Stage 2: parse on the blocking pool, stream on.
                                    let (tx2, mut rx2) =
                                        tokio::sync::mpsc::channel::<(u64, usize)>(128);
                                    let relay_tx = tx2.clone();
                                    let relay = tokio::spawn(async move {
                                        while let Some(sz) = rx1.recv().await {
                                            tokio::task::spawn_blocking({
                                                let tx2 = relay_tx.clone();
                                                move || {
                                                    let h = parse_hash(sz);
                                                    let _ = tx2.blocking_send((h, sz));
                                                }
                                            });
                                        }
                                    });
                                    drop(tx2);
                                    // Stage 3: save, collect all in-flight tasks.
                                    let mut handles = Vec::new();
                                    while let Some((h, sz)) = rx2.recv().await {
                                        handles.push(tokio::spawn(async move {
                                            async_io(h, save_dur(sz)).await
                                        }));
                                    }
                                    relay.await.unwrap();
                                    join_all(handles)
                                        .await
                                        .into_iter()
                                        .map(|h| h.unwrap())
                                        .collect()
                                });
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                    (
                        "rayon",
                        Box::new({
                            let docs = docs.clone();
                            move || {
                                let t = Instant::now();
                                let r: Vec<u64> = docs
                                    .par_iter()
                                    .map(|&(sz, i)| {
                                        blocking_io(i, fetch_dur(sz));
                                        let h = parse_hash(sz);
                                        blocking_io(h, save_dur(sz));
                                        h
                                    })
                                    .collect();
                                finish(r, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "real_doc",
        batches,
    }
}

/// S7: realistic web pipeline over a loopback mock HTTP server:
/// fetch (async HTTP request) → parse (CPU) → aggregate (sync reduce).
fn real_web() -> Scenario {
    let path = start_mock_server();
    let batches = [500usize, 2_000]
        .into_iter()
        .map(|n| {
            Batch {
                n,
                libs: vec![
                    (
                        "youpipe",
                        Box::new({
                            let path = path.to_owned();
                            move || {
                                let t = Instant::now();
                                let total = Arc::new(std::sync::atomic::AtomicU64::new(0));
                                let tot = total.clone();
                                stream(0..n)
                                    .with_config(unbounded_io_config())
                                    .with_async_pool(tokio_pool())
                                    .stage_async({
                                        let path = path.clone();
                                        move |i: usize| {
                                            let path = path.clone();
                                            async move { http_get(&path, i).await }
                                        }
                                    })
                                    .stage(|body: Vec<u8>| bb(web_parse(&body)))
                                    .for_each(move |v: u64| {
                                        tot.fetch_add(v, Ordering::Relaxed);
                                    });
                                finish(total.load(Ordering::Relaxed), t)
                            }
                        }) as Job,
                    ),
                    (
                        "tokio",
                        Box::new({
                            let path = path.to_owned();
                            move || {
                                let t = Instant::now();
                                let total = tokio_rt().block_on(async {
                                    // Stage 1: fetch tasks streamed into a bounded channel.
                                    let (tx1, mut rx1) = tokio::sync::mpsc::channel::<Vec<u8>>(128);
                                    for i in 0..n {
                                        let tx1 = tx1.clone();
                                        let path = path.clone();
                                        tokio::spawn(async move {
                                            let _ = tx1.send(http_get(&path, i).await).await;
                                        });
                                    }
                                    drop(tx1);
                                    // Stage 2: parse tasks.
                                    let (tx2, mut rx2) = tokio::sync::mpsc::channel::<u64>(128);
                                    let relay_tx = tx2.clone();
                                    let relay = tokio::spawn(async move {
                                        while let Some(body) = rx1.recv().await {
                                            let tx2 = relay_tx.clone();
                                            tokio::spawn(async move {
                                                let _ = tx2.send(web_parse(&body)).await;
                                            });
                                        }
                                    });
                                    drop(tx2);
                                    // Stage 3: aggregate on the calling task.
                                    let mut total = 0u64;
                                    while let Some(v) = rx2.recv().await {
                                        total += v;
                                    }
                                    relay.await.unwrap();
                                    total
                                });
                                finish(total, t)
                            }
                        }) as Job,
                    ),
                    (
                        "futures",
                        Box::new({
                            let path = path.to_owned();
                            move || {
                                let t = Instant::now();
                                let total = tokio_rt().block_on(async {
                                    stream::iter(0..n)
                                        .map({
                                            let path = path.clone();
                                            move |i| {
                                                let path = path.clone();
                                                async move { http_get(&path, i).await }
                                            }
                                        })
                                        .buffer_unordered(4096)
                                        .map(|body| web_parse(&body))
                                        .fold(0u64, |acc, v| async move { acc + v })
                                        .await
                                });
                                finish(total, t)
                            }
                        }) as Job,
                    ),
                ],
            }
        })
        .collect();
    Scenario {
        name: "real_web",
        batches,
    }
}

fn main() {
    let cfg = parse_args();
    eprintln!(
        "horizontal bench: rounds={} measure_ms={} out={}",
        cfg.rounds, cfg.measure_ms, cfg.out
    );
    let recs = run_all(&cfg);
    write_json(&cfg, &recs).expect("write json");
    eprintln!("wrote {}", cfg.out);
}
