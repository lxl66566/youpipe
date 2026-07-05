//! Real-world mixed CPU/IO benchmark: read skewed-size files, process them
//! (compress + AES-256-GCM encrypt, the backup-encryption pipeline), and write
//! back. Compares three engines, **run once each**, timed.
//!
//! # What it measures
//!
//! A directory of files whose sizes span ~3 orders of magnitude (log-uniform
//! 8 KiB .. 8 MiB), filled with compressible content. Each file is read,
//! sealed, and written to an output directory. Three implementations do the
//! same work:
//!
//! | Engine | Shape |
//! |---|---|
//! | **youpipe** | `stream().stage(read).stage(process).stage(write).run()` on an oversubscribed compute pool — read/process/write fully pipelined across files. |
//! | **rayon** | `par_iter().for_each(read; process; write)` on the default core-sized pool — idiomatic "just parallelise". |
//! | **tokio** | one `tokio::spawn` task per file: `tokio::fs::read` → `spawn_blocking(seal)` → `tokio::fs::write`. |
//!
//! To keep the read side comparably cold, input files are evicted from the OS
//! page cache (`POSIX_FADV_DONTNEED` on Linux) before each engine.
//!
//! # CPU load / task selection
//!
//! `FC_TASK` picks the per-file work:
//! - `compress` (default): zstd (`FC_ZSTD_LEVEL`, default 15) **then**
//!   AES-256-GCM. zstd at high level is genuinely CPU-heavy and scales with
//!   input size — the regime where read/CPU/write pipelining overlaps real
//!   compute with blocking `fsync`.
//! - `aes`: plain AES-256-GCM. Fast (AES-NI); CPU is marginal so the workload
//!   is IO/memory bound. Kept as the light-CPU reference point.
//!
//! # Run
//!
//! ```text
//! cargo run --release -p file-encrypt-bench
//! ```
//!
//! Optional env overrides (for sizing the workload / task up/down):
//!
//! ```text
//! FC_TASK=compress FC_ZSTD_LEVEL=19 FC_COUNT=800 FC_MIN_KIB=4 FC_MAX_MIB=64 \
//!   cargo run --release -p file-encrypt-bench
//! ```
//!
//! # Where the data lives
//!
//! The temp data root defaults to `FC_DATA_DIR` if set, otherwise the OS temp
//! dir. **This matters a lot**: if that dir is RAM-backed (tmpfs/ramfs —
//! common for `/tmp`), there is no real blocking disk IO to overlap with CPU,
//! and youpipe's pipelining advantage will not show; the benchmark prints a
//! warning when it detects this. For disk-bound numbers point `FC_DATA_DIR`
//! at real storage, e.g.:
//!
//! ```text
//! FC_DATA_DIR=/var/tmp/feb cargo run --release -p file-encrypt-bench
//! ```

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

mod cipher;
mod engine_rayon;
mod engine_tokio;
mod engine_youpipe;
mod workload;
use workload::FileEntry;

/// Default workload shape. Sizes span 3 orders of magnitude (1024×) for a
/// realistic skewed directory; the total (~200–250 MiB) is large enough that
/// data movement dominates per-engine setup cost while bounding peak memory
/// under the pipelined stages. Override via `FC_COUNT` / `FC_MIN_KIB` /
/// `FC_MAX_MIB`.
const DEFAULT_COUNT: usize = 200;
const DEFAULT_MIN_KIB: u64 = 8;
const DEFAULT_MAX_MIB: u64 = 8;

struct EngineResult {
    name: &'static str,
    config: String,
    elapsed: Duration,
    files: usize,
    verified_ok: usize,
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn main() {
    let count = env_or("FC_COUNT", DEFAULT_COUNT);
    let min_kib = env_or("FC_MIN_KIB", DEFAULT_MIN_KIB as usize) as u64;
    let max_mib = env_or("FC_MAX_MIB", DEFAULT_MAX_MIB as usize) as u64;
    // fsync on writes (durable "write back"). Default on — without it writes
    // are buffered and the workload is memory-bound, not disk-bound. Override
    // with FC_FSYNC=0 to see the buffered (RAM-speed) regime.
    let fsync = std::env::var("FC_FSYNC").map(|v| v != "0").unwrap_or(true);
    // Per-file task: heavy CPU (compress+encrypt, default) or light (AES only).
    let task = match std::env::var("FC_TASK").as_deref() {
        Ok("aes") => cipher::Task::Aes,
        _ => {
            let level = env_or("FC_ZSTD_LEVEL", 15) as i32;
            cipher::Task::CompressEncrypt(level)
        }
    };

    let root: PathBuf = std::env::var("FC_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("youpipe-file-encrypt-bench"));
    // Start from a clean slate.
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");

    let fs_type = workload::fs_type_name(&root);
    let ram_backed = workload::is_ram_backed(&root);

    let input_dir = root.join("input");

    println!("Generating {count} files (log-uniform {min_kib} KiB .. {max_mib} MiB)...");
    let (files, total_bytes) =
        workload::generate(&input_dir, count, min_kib * 1024, max_mib * 1024 * 1024)
            .expect("generate workload");
    let total_mib = total_bytes as f64 / (1024.0 * 1024.0);

    // One key shared by every engine (cipher cost is therefore identical
    // across them; only scheduling/IO differs).
    let cipher = Arc::new(cipher::Cipher::new(task));

    println!();
    println!("=== File Encrypt Benchmark ===");
    println!("CPU cores       : {}", num_cpus());
    println!(
        "Files           : {} ({} .. {} sizes scattered)",
        files.len(),
        workload::fmt_bytes(min_kib * 1024),
        workload::fmt_bytes(max_mib * 1024 * 1024)
    );
    println!("Total input     : {:.1} MiB", total_mib);
    println!("Task            : {}", task.label());
    println!("Data dir        : {} ({fs_type})", root.display());
    println!("Write fsync     : {fsync} (durable write-back)");
    if ram_backed {
        println!("Cache           : eviction is a no-op on RAM-backed fs");
    } else {
        println!("Cache           : evicted (POSIX_FADV_DONTNEED) before each engine");
    }
    if ram_backed {
        println!();
        println!("! WARNING: data dir is RAM-backed ({fs_type}). There is no real");
        println!("! blocking disk IO here, so youpipe's read/encrypt/write overlap has");
        println!("! nothing to hide behind — the workload is memory-bandwidth bound and");
        println!("! a fused approach (rayon) tends to win. Set FC_DATA_DIR to real");
        println!("! storage to measure the disk-IO regime where pipelining pays off.");
    }
    println!();

    let out_youpipe = root.join("out_youpipe");
    let out_rayon = root.join("out_rayon");
    let out_tokio = root.join("out_tokio");

    let mut results: Vec<EngineResult> = Vec::new();

    // ── youpipe ──
    {
        let pairs = engine_youpipe::make_pairs(&files, &out_youpipe);
        let (elapsed, n) = engine_youpipe::run(pairs, &out_youpipe, cipher.clone(), fsync);
        let ok = verify(&files, &out_youpipe, &cipher);
        results.push(EngineResult {
            name: engine_youpipe::NAME,
            config: engine_youpipe::config_str(),
            elapsed,
            files: n,
            verified_ok: ok,
        });
    }

    // ── rayon ──
    {
        let pairs = engine_rayon::make_pairs(&files, &out_rayon);
        let (elapsed, n) = engine_rayon::run(pairs, &out_rayon, cipher.clone(), fsync);
        let ok = verify(&files, &out_rayon, &cipher);
        results.push(EngineResult {
            name: engine_rayon::NAME,
            config: engine_rayon::config_str(),
            elapsed,
            files: n,
            verified_ok: ok,
        });
    }

    // ── tokio ──
    {
        let pairs = engine_tokio::make_pairs(&files, &out_tokio);
        let (elapsed, n) = engine_tokio::run(pairs, &out_tokio, cipher.clone(), fsync);
        let ok = verify(&files, &out_tokio, &cipher);
        results.push(EngineResult {
            name: engine_tokio::NAME,
            config: engine_tokio::config_str(),
            elapsed,
            files: n,
            verified_ok: ok,
        });
    }

    // ── Report ──
    let fastest = results
        .iter()
        .min_by_key(|r| r.elapsed)
        .map(|r| r.name)
        .unwrap_or("");
    for r in &results {
        let mib_s = total_mib / r.elapsed.as_secs_f64().max(1e-9);
        let files_s = r.files as f64 / r.elapsed.as_secs_f64().max(1e-9);
        let tag = if r.name == fastest {
            " <== fastest"
        } else {
            ""
        };
        println!("{}", r.name);
        println!("    cfg   : {}", r.config);
        println!(
            "    time  : {:.3?}   |  {:.1} MiB/s  |  {:.0} files/s  |  verified {}/{}{tag}",
            r.elapsed, mib_s, files_s, r.verified_ok, r.files
        );
        println!();
    }

    // Any output that failed to round-trip is a hard failure.
    let bad = results.iter().filter(|r| r.verified_ok != r.files).count();
    if bad != 0 {
        eprintln!("ERROR: {bad} engine(s) failed verification");
        std::process::exit(1);
    }

    // Cleanup.
    let _ = std::fs::remove_dir_all(&root);
}

/// Verify every output: exists and is large enough to hold nonce+tag, plus a
/// full decrypt-and-compare round-trip on a sample (the 12 largest + 4
/// smallest files). Returns the number of files present and well-formed.
///
/// Output size can't be predicted from the input size in `CompressEncrypt`
/// mode (it depends on compressibility), so we don't assert an exact length —
/// the sample round-trip is the real correctness check.
fn verify(files: &[FileEntry], out_dir: &Path, cipher: &cipher::Cipher) -> usize {
    let mut ok = 0usize;
    // Pick sample indices: sort by size desc, take largest 12 + smallest 4.
    let mut by_size: Vec<usize> = (0..files.len()).collect();
    by_size.sort_by_key(|&i| std::cmp::Reverse(files[i].size));
    let mut sample: Vec<usize> = by_size.iter().take(12).copied().collect();
    if files.len() > 12 {
        sample.extend(by_size.iter().rev().take(4).copied());
    }
    let sample_set: std::collections::HashSet<usize> = sample.into_iter().collect();

    for (i, e) in files.iter().enumerate() {
        let out = out_dir.join(e.path.file_name().expect("filename"));
        let meta = match std::fs::metadata(&out) {
            Ok(m) => m,
            Err(_) => continue,
        };
        // Must at least hold nonce + tag.
        if meta.len() as usize > cipher::OVERHEAD {
            ok += 1;
        }
        if sample_set.contains(&i) {
            // Full round-trip: read output, open (decrypt [+decompress]),
            // compare to input.
            let blob = std::fs::read(&out).expect("read output for verify");
            let pt = cipher.open(&blob);
            let inp = std::fs::read(&e.path).expect("read input for verify");
            assert_eq!(
                pt,
                inp,
                "verify failed: opened output != input for {:?}",
                e.path.file_name()
            );
        }
    }
    ok
}
