//! youpipe engine: a **3-stage tuned streaming pipeline**.
//!
//!   stage 1 READ    (blocking IO):  (inp, out) → (out, plaintext)
//!   stage 2 PROCESS (CPU-heavy):    (out, plaintext) → (out, blob)
//!   stage 3 WRITE   (blocking IO):  (out, blob) → ()
//!
//! The three stages are connected by lock-free channels, so reading file N+1,
//! processing file N and writing file N-1 all progress at once — full
//! read/process/write overlap. With a genuinely CPU-heavy task (zstd compress
//! at high level) and durable writes (`fsync`), this overlap is where the
//! advantage over the fused baselines comes from: while write workers block on
//! `fsync`, the process workers keep burning CPU on other files.
//!
//! # Tuning rationale
//!
//! - **Oversubscribed compute pool.** File IO via `std::fs` *blocks* the OS
//!   thread, so concurrent file operations (esp. durable `fsync`) are bounded
//!   by the worker count — not by cores. The default global pool (one thread
//!   per core) caps that at `num_cpus`, which serialises too many flushes. We
//!   size the pool at `3 × num_cpus` (capped at 128): with 3 sync stages that
//!   gives each stage `~num_cpus` workers — enough read/write concurrency to
//!   keep the device busy, while the CPU process stage gets exactly the cores
//!   it needs and is *not* oversubscribed. (Bigger pools were measured to be
//!   slower: the even stage division oversubscribes the CPU process stage,
//!   adding context-switch overhead without IO benefit. Sweep with `FC_POOL`.)
//!   This is the `with_compute_pool` use case from `youpipe`'s docs.
//!
//! - **Small `buffer_size` (4).** Items here are whole files — up to tens of
//!   MiB. The default 256-slot channel would let ~256 of these pile up between
//!   stages and blow up peak memory. A buffer of 4 keeps enough slack for the
//!   stages to stay pipelined (the processor never stalls on an empty channel
//!   once the first read completes) while bounding resident memory.
//!
//! - **`Workload::Unbalanced`.** File sizes span 3+ orders of magnitude and the
//!   CPU cost (zstd) scales with size, so the cost skew is even sharper than
//!   the size skew; the finer oversplit factor helps work-stealing rebalance a
//!   worker stuck on a large file against workers draining small ones.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use youpipe::{ComputePool, PipelineConfig, Workload, stream};

use crate::{
    cipher::Cipher,
    workload::{FileEntry, evict_dir_cache, write_output},
};

pub const NAME: &str = "youpipe stream (3 stages, tuned)";

/// Build the `(input, output)` pair list from the workload + an output dir.
pub fn make_pairs(files: &[FileEntry], out_dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    files
        .iter()
        .map(|e| {
            let out = out_dir.join(e.path.file_name().expect("filename"));
            (e.path.clone(), out)
        })
        .collect()
}

/// Run the youpipe pipeline. Pool construction is untimed (it pays the
/// thread-spawn cost once, before the clock starts).
pub fn run(
    pairs: Vec<(PathBuf, PathBuf)>,
    out_dir: &Path,
    cipher: Arc<Cipher>,
    fsync: bool,
) -> (Duration, usize) {
    std::fs::create_dir_all(out_dir).expect("create output dir");
    evict_dir_cache(
        pairs
            .first()
            .map(|(inp, _)| inp.parent().expect("input parent"))
            .expect("non-empty pairs"),
    );

    let n = pairs.len();
    let cpus = num_cpus();
    // See module docs: 3 × num_cpus (capped) is the measured sweet spot.
    let pool_threads = pool_size(cpus);

    let cfg = PipelineConfig::default()
        .with_buffer_size(4)
        .with_workload(Workload::Unbalanced);

    // Untimed setup: build the pool so the timer measures only the work.
    let pool = ComputePool::new(pool_threads);
    let cipher_stage = cipher;

    let t0 = Instant::now();
    let _outputs: Vec<()> = stream(pairs)
        .with_config(cfg)
        .with_compute_pool(pool)
        // Stage 1 — READ (blocking IO).
        .stage(|(inp, out): (PathBuf, PathBuf)| {
            let plaintext = std::fs::read(&inp).expect("read input");
            (out, plaintext)
        })
        // Stage 2 — PROCESS (CPU-heavy: zstd + AES-GCM on the work-stealing
        // pool). This is the stage the pipeline overlaps with blocking IO.
        .stage(move |(out, plaintext): (PathBuf, Vec<u8>)| {
            let blob = cipher_stage.seal(&plaintext);
            (out, blob)
        })
        // Stage 3 — WRITE (blocking IO, optionally durable via fsync).
        .stage(move |(out, blob): (PathBuf, Vec<u8>)| {
            write_output(&out, &blob, fsync).expect("write output");
        })
        .run();
    let elapsed = t0.elapsed();

    (elapsed, n)
}

pub fn config_str() -> String {
    let cpus = num_cpus();
    let pool_threads = pool_size(cpus);
    format!(
        "pool={pool_threads} threads (~3×{cpus}), buffer=4, Workload::Unbalanced, stages=read|process|write"
    )
}

fn pool_size(cpus: usize) -> usize {
    // Override to sweep. Default: 3 × num_cpus (capped) so each of the 3 sync
    // stages gets ~num_cpus workers — saturating CPU for encrypt without
    // oversubscribing it.
    if let Ok(v) = std::env::var("FC_POOL") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    (cpus.max(4) * 3).min(128)
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}
