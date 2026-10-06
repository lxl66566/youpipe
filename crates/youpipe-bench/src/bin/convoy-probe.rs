//! Bistability forensics harness for the streaming convoy pathology
//! (todo P1 #4; see docs/src/dev/streaming.md "Worker recv loops" and
//! dead-ends.md "streaming 相邻 sync stage 编译期自动融合").
//!
//! Runs ONE shape R times in-process, recording per run the wall time (ms)
//! and the voluntary / nonvoluntary context-switch deltas of every thread
//! (`/proc/self/task`, grouped by thread name: `yp-pool-*` vs collector vs
//! tokio). Voluntary switches are the futex-park proxy: a run that parks per
//! item shows ~n voluntary switches, a burst-pipelined run ~hundreds.
//!
//! ```sh
//! # fence_infra canary shape, 20 runs
//! cargo run --release -p youpipe-bench --bin convoy-probe -- \
//!     --shape fence --n 100000 --runs 20
//!
//! # parameter sweep: repeat the binary per cell (fresh process each time —
//! # the mode is bistable across processes too), taskset like bench_ab.sh
//! # (keep core 0 for the OS)
//! for w in 1 2 4 8 15 16; do
//!   taskset -c 1-31 convoy-probe --shape fence --workers $w --runs 12
//! done
//! ```

use std::{
    hint::black_box as bb,
    thread::sleep,
    time::{Duration, Instant},
};

use youpipe::{
    AsyncStageOptions, CancellationToken, FenceMode, FenceOptions, PipelineConfig,
    SyncStageOptions, TokioPool, stream,
};

fn bump(x: u64) -> u64 {
    bb(x.wrapping_add(1))
}

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    bb(r)
}

// ── /proc context-switch accounting ──

/// Per-thread snapshot entry: (tid, group, voluntary, nonvoluntary). Per-TID
/// (not per-group sums) so a thread that dies mid-run is detectable instead
/// of silently shrinking its group total (which could clamp to 0 delta).
type ThreadSnap = Vec<(u32, &'static str, u64, u64)>;

/// Per-group within-run delta: (group, voluntary, nonvoluntary, exited),
/// `exited` counting threads alive at `before` but gone at `after`.
type GroupDelta = Vec<(String, u64, u64, usize)>;

/// Collapse a comm into its display group: `yp-pool-*` (stage workers /
/// feeder / fence forwarder in pool mode), `tokio-worker` (async runtime),
/// `main/dedicated` (collector thread plus dedicated-mode unnamed worker
/// threads).
fn group_of(comm: &str) -> &'static str {
    if comm.starts_with("yp-pool-") {
        "yp-pool"
    } else if comm.starts_with("tokio-runtime-w") {
        "tokio-worker"
    } else {
        "main/dedicated"
    }
}

/// Snapshot every thread's ctxt-switch counters keyed by TID.
fn snapshot() -> ThreadSnap {
    let mut out = Vec::new();
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
        return out;
    };
    for e in dir.flatten() {
        let tid_str = e.file_name().to_string_lossy().into_owned();
        let (Ok(tid), Some((comm, v, nv))) = (tid_str.parse::<u32>(), task_status(&tid_str)) else {
            continue;
        };
        out.push((tid, group_of(&comm), v, nv));
    }
    out
}

fn task_status(tid: &str) -> Option<(String, u64, u64)> {
    let s = std::fs::read_to_string(format!("/proc/self/task/{tid}/status")).ok()?;
    let mut name = None;
    let mut v = 0;
    let mut nv = 0;
    for line in s.lines() {
        if let Some(x) = line.strip_prefix("Name:") {
            name = Some(x.trim().to_owned());
        } else if let Some(x) = line.strip_prefix("voluntary_ctxt_switches:") {
            v = x.trim().parse().unwrap_or(0);
        } else if let Some(x) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
            nv = x.trim().parse().unwrap_or(0);
        }
    }
    Some((name?, v, nv))
}

fn group_slot<'a>(groups: &'a mut GroupDelta, g: &str) -> &'a mut (String, u64, u64, usize) {
    let idx = match groups.iter().position(|(bg, ..)| bg == g) {
        Some(i) => i,
        None => {
            groups.push((g.to_owned(), 0, 0, 0));
            groups.len() - 1
        },
    };
    &mut groups[idx]
}

/// Within-run per-group delta from two per-TID snapshots.
///
/// Threads born inside the run (no `before` entry) contribute their full
/// lifetime counters — correct, since birth was inside the run. Threads that
/// DIED inside the run (in `before`, gone from `after` — e.g. the transient
/// pool spawned under `--cpu-workers`) are counted as `exited`: Linux
/// discards a thread's ctxt-switch counters at exit, so their switches are
/// unobservable from snapshots and are NOT part of v/nv. In-run sampling
/// would perturb the very switches under measurement, so flagging is the
/// honest accounting (review BS-2). TIDs are assumed not reused within one
/// run (monotonic until pid_max wraparound).
fn delta(before: &ThreadSnap, after: &ThreadSnap) -> GroupDelta {
    let mut groups: GroupDelta = Vec::new();
    for &(tid, g, v, nv) in after {
        let (ov, onv) = before
            .iter()
            .find(|&&(btid, ..)| btid == tid)
            .map_or((0, 0), |&(_, _, bv, bnv)| (bv, bnv));
        let s = group_slot(&mut groups, g);
        s.1 += v.saturating_sub(ov);
        s.2 += nv.saturating_sub(onv);
    }
    for &(tid, g, ..) in before {
        if !after.iter().any(|&(atid, ..)| atid == tid) {
            group_slot(&mut groups, g).3 += 1;
        }
    }
    groups
}

// ── shapes ──

struct Cfg {
    shape: String,
    n: usize,
    runs: usize,
    workers: usize,     // per-stage SyncStageOptions::workers pin (0 = default split)
    w1: usize,          // stage-1-only workers pin (0 = fall back to `workers`)
    w2: usize,          // stage-2-only workers pin (0 = fall back to `workers`)
    pinbuf: usize,      // per-stage SyncStageOptions::buffer pin (0 = default floor)
    buffer: usize,      // config buffer_size
    chunk: usize,       // fence chunk (0 = Barrier)
    io: usize,          // async io_concurrency
    cpu_workers: usize, // with_compute_workers pin (0 = default)
    cost_cpu: bool,
    delay_ms: u64,
    threads: bool,
    csv: bool,
}

fn stage_opts(c: &Cfg) -> SyncStageOptions {
    stage_opts_n(c, c.workers)
}

/// SyncStageOptions with an explicit worker count (`n` = the resolved per-stage
/// pin: `--w1`/`--w2` fall back to `--workers`).
fn stage_opts_n(c: &Cfg, n: usize) -> SyncStageOptions {
    let mut o = SyncStageOptions::new();
    if n > 0 {
        o = o.workers(n);
    }
    if c.pinbuf > 0 {
        o = o.buffer(c.pinbuf);
    }
    o
}

impl Cfg {
    /// Stage-2 pin: `--w2` if set, else `--workers`.
    fn w2_or(&self) -> usize {
        if self.w2 > 0 {
            self.w2
        } else {
            self.workers
        }
    }
}

fn fence_mode(chunk: usize) -> FenceMode {
    match chunk {
        0 => FenceMode::Barrier,
        k => FenceMode::Chunked(k.try_into().expect("chunk > 0")),
    }
}

/// Build + run one pipeline of the requested shape; returns (len, wall ms).
/// The async shapes share one process-wide runtime (handle built once by the
/// caller) so per-run tokio construction noise cannot seed the mode.
fn run_shape(c: &Cfg, data: &[u64], tokio_handle: Option<&tokio::runtime::Handle>) -> (usize, f64) {
    // Untimed input prep: fresh Vec per run (the pipeline consumes it),
    // cache-warmed so the timed region never measures cold-from-RAM clones.
    let v: Vec<u64> = data.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);

    let f = if c.cost_cpu {
        cpu_work
    } else {
        bump
    };
    let opts = stage_opts(c);
    let fence_opts = if c.pinbuf > 0 {
        FenceOptions::new().buffer(c.pinbuf)
    } else {
        FenceOptions::new()
    };
    let never = CancellationToken::new();
    let config = PipelineConfig::default()
        .with_buffer_size(c.buffer)
        .with_io_concurrency(c.io);

    let t0 = Instant::now();
    // Each match arm consumes `pipe` exactly once (arms are exclusive).
    let pipe = stream(v).with_config(config);
    let pipe = if c.cpu_workers > 0 {
        pipe.with_compute_workers(c.cpu_workers)
    } else {
        pipe
    };
    let r: Vec<u64> = match c.shape.as_str() {
        // The strongest sharded-terminal shape (sharded_term/single_*): one
        // stage, streaming forced by the cancel token — the terminal fan-in
        // data plane dominates (todo #1 soak cell).
        "single" => pipe.with_cancel(never).stage_with(opts, f).run(),
        // Known-fast anchor: two sync populations, streaming forced by the
        // cancel token (fused pass-through declines).
        "sync2" => pipe
            .with_cancel(never)
            .stage_with(opts, f)
            .stage_with(opts, f)
            .run(),
        // fence_infra canary: bump . fence(Chunked) . bump, zero CPU work.
        // Stage 1 pulls `--w1` (default `--workers`), stage 2 `--w2` — the
        // asymmetry isolates which side of the fence the collapse lives on.
        "fence" => pipe
            .stage_with(stage_opts_n(c, c.w1), f)
            .fence_with(fence_opts, fence_mode(c.chunk))
            .stage_with(stage_opts_n(c, c.w2_or()), f)
            .run(),
        // 3-stage variant of `fence` (probe measured 36 ms @100K).
        "fence3" => pipe
            .stage_with(opts, f)
            .stage_with(opts, f)
            .fence_with(fence_opts, fence_mode(c.chunk))
            .stage_with(opts, f)
            .run(),
        // The 23↔244 ms bistable shape: two sync prefixes into stage_async.
        "async2" => {
            let h = tokio_handle.expect("async shapes need the runtime handle");
            pipe.with_async_pool(TokioPool::new(h.clone()))
                .stage_with(stage_opts_n(c, c.w1), f)
                .stage_with(stage_opts_n(c, c.w2_or()), f)
                .stage_async_with(
                    AsyncStageOptions::new().io_concurrency(c.io),
                    |x: u64| async move { bb(x.wrapping_add(1)) },
                )
                .run()
        },
        // One sync prefix: isolates the mixed-channel handoff from the
        // inter-sync MPMC hop.
        "async1" => {
            let h = tokio_handle.expect("async shapes need the runtime handle");
            pipe.with_async_pool(TokioPool::new(h.clone()))
                .stage_with(opts, f)
                .stage_async_with(
                    AsyncStageOptions::new().io_concurrency(c.io),
                    |x: u64| async move { bb(x.wrapping_add(1)) },
                )
                .run()
        },
        // Async-only: feeder pushes a mixed-mode channel directly (the
        // single-feeder-throughput-limit reference, probe: 83 ms).
        "async0" => {
            let h = tokio_handle.expect("async shapes need the runtime handle");
            pipe.with_async_pool(TokioPool::new(h.clone()))
                .stage_async_with(
                    AsyncStageOptions::new().io_concurrency(c.io),
                    |x: u64| async move { bb(x.wrapping_add(1)) },
                )
                .run()
        },
        other => panic!("unknown shape {other:?} (single|sync2|fence|fence3|async2|async1|async0)"),
    };
    let dt = t0.elapsed().as_secs_f64() * 1e3;
    (r.len(), dt)
}

/// Raw-file logger for the `crossfire-trace` feature: one line per
/// send/recv/wake episode. Enabled by building with
/// `--features crossfire-trace` AND setting `CONVOY_TRACE_PATH`; volume is
/// huge (line per channel op), so keep `--n` in the low thousands.
#[cfg(feature = "crossfire-trace")]
fn init_trace_logger() {
    struct Raw(std::sync::Mutex<std::fs::File>);
    impl log::Log for Raw {
        fn enabled(&self, _m: &log::Metadata) -> bool {
            true
        }

        fn log(&self, r: &log::Record) {
            use std::io::Write;
            if let Ok(mut f) = self.0.lock() {
                let _ = writeln!(f, "{}", r.args());
            }
        }

        fn flush(&self) {}
    }
    if let Ok(path) = std::env::var("CONVOY_TRACE_PATH") {
        let f = std::fs::File::create(&path).expect("create trace file");
        log::set_boxed_logger(Box::new(Raw(std::sync::Mutex::new(f)))).expect("set logger");
        log::set_max_level(log::LevelFilter::Debug);
    }
}

fn main() {
    #[cfg(feature = "crossfire-trace")]
    init_trace_logger();
    let mut c = Cfg {
        shape: "fence".into(),
        n: 100_000,
        runs: 12,
        workers: 0,
        w1: 0,
        w2: 0,
        pinbuf: 0,
        buffer: 256,
        chunk: 500,
        io: 128,
        cpu_workers: 0,
        cost_cpu: false,
        delay_ms: 0,
        threads: false,
        csv: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || {
            args.next()
                .unwrap_or_else(|| panic!("missing value for {a}"))
        };
        match a.as_str() {
            "--shape" => c.shape = val(),
            "--n" => c.n = val().parse().unwrap(),
            "--runs" => c.runs = val().parse().unwrap(),
            "--workers" => c.workers = val().parse().unwrap(),
            "--w1" => c.w1 = val().parse().unwrap(),
            "--w2" => c.w2 = val().parse().unwrap(),
            "--pinbuf" => c.pinbuf = val().parse().unwrap(),
            "--buffer" => c.buffer = val().parse().unwrap(),
            "--chunk" => c.chunk = val().parse().unwrap(),
            "--io" => c.io = val().parse().unwrap(),
            "--cpu-workers" => c.cpu_workers = val().parse().unwrap(),
            "--cost" => c.cost_cpu = val() == "cpu",
            "--delay-ms" => c.delay_ms = val().parse().unwrap(),
            "--threads" => c.threads = true,
            "--csv" => c.csv = true,
            other => panic!("unknown flag {other:?}"),
        }
    }

    let data: Vec<u64> = (0..c.n as u64).collect();
    let needs_tokio = c.shape.starts_with("async");
    // One shared runtime for the whole process: per-run construction would
    // add ms-scale noise to every iteration (same rationale as sync_fuse).
    let runtime = needs_tokio.then(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::thread::available_parallelism().map_or(4, std::num::NonZero::get))
            .build()
            .expect("async runtime")
    });
    let handle = runtime.as_ref().map(|r| r.handle().clone());

    let mut times: Vec<f64> = Vec::with_capacity(c.runs);
    let mut rows: Vec<(f64, GroupDelta)> = Vec::new();
    for i in 0..c.runs {
        if c.delay_ms > 0 && i > 0 {
            sleep(Duration::from_millis(c.delay_ms));
        }
        let before = if c.threads {
            snapshot()
        } else {
            Vec::new()
        };
        let (len, dt) = run_shape(&c, &data, handle.as_ref());
        let after = if c.threads {
            snapshot()
        } else {
            Vec::new()
        };
        assert_eq!(len, c.n, "pipeline lost items");
        times.push(dt);
        if c.threads {
            rows.push((dt, delta(&before, &after)));
        }
    }

    if c.csv {
        println!("shape,n,workers,w1,w2,pinbuf,buffer,chunk,io,cpu_workers,cost,run,ms");
        for (i, &t) in times.iter().enumerate() {
            println!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{:.3}",
                c.shape,
                c.n,
                c.workers,
                c.w1,
                c.w2,
                c.pinbuf,
                c.buffer,
                c.chunk,
                c.io,
                c.cpu_workers,
                if c.cost_cpu {
                    "cpu"
                } else {
                    "bump"
                },
                i + 1,
                t
            );
        }
    } else {
        let opt = |v: usize| {
            if v == 0 {
                "auto".to_owned()
            } else {
                v.to_string()
            }
        };
        println!(
            "# shape={} n={} workers={} w1={} w2={} pinbuf={} buffer={} chunk={} io={} \
             cpu_workers={} cost={} runs={}",
            c.shape,
            c.n,
            opt(c.workers),
            opt(c.w1),
            opt(c.w2),
            opt(c.pinbuf),
            c.buffer,
            if c.chunk == 0 {
                "Barrier".to_owned()
            } else {
                c.chunk.to_string()
            },
            c.io,
            opt(c.cpu_workers),
            if c.cost_cpu {
                "cpu"
            } else {
                "bump"
            },
            c.runs,
        );
        for (i, &t) in times.iter().enumerate() {
            println!("run {:2}  {:9.3} ms", i + 1, t);
        }
    }
    let mut sorted = times.clone();
    sorted.sort_by(f64::total_cmp);
    println!(
        "summary min={:.1} med={:.1} max={:.1} (ms)",
        sorted[0],
        sorted[sorted.len() / 2],
        sorted[sorted.len() - 1]
    );
    if c.threads {
        println!(
            "# per-run ctx-switch deltas by thread group (v=voluntary/futex-park,\n             # \
             nv=involuntary). exited=N marks threads that died inside the run\n             # \
             (e.g. the transient pool under --cpu-workers): Linux discards their\n             # \
             counters at exit, so their switches are NOT included in v/nv."
        );
        for (i, (dt, d)) in rows.iter().enumerate() {
            let parts: Vec<String> = d
                .iter()
                .map(|(g, v, nv, ex)| {
                    if *ex > 0 {
                        format!("{g}:v={v},nv={nv},exited={ex}")
                    } else {
                        format!("{g}:v={v},nv={nv}")
                    }
                })
                .collect();
            println!("run {:2}  {:9.3} ms  {}", i + 1, dt, parts.join("  "));
        }
    }
}
