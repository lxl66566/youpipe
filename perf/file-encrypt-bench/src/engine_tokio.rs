//! tokio baseline: one task per file, async IO + `spawn_blocking` for AES.
//!
//!   tokio::spawn(async {
//!       let pt = tokio::fs::read(inp).await;            // async IO
//!       let ct = spawn_blocking(move || seal(pt)).await; // CPU off-thread
//!       tokio::fs::write(out, ct).await;                 // async IO
//!   });
//!
//! This is the idiomatic async pattern for "blocking CPU inside an IO service".
//! Note `tokio::fs::{read,write}` are themselves `spawn_blocking` under the
//! hood (the tokio blocking-pool), so the runtime multiplexes both the IO and
//! the AES over a pool of blocking threads — distinct from rayon's core-sized
//! pool. The three steps per file still run sequentially within the task; the
//! concurrency is across files (one task per file).

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    cipher::Cipher,
    workload::{FileEntry, evict_dir_cache, write_output},
};

pub const NAME: &str = "tokio spawn + spawn_blocking (io async)";

pub fn make_pairs(files: &[FileEntry], out_dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    files
        .iter()
        .map(|e| {
            let out = out_dir.join(e.path.file_name().expect("filename"));
            (e.path.clone(), out)
        })
        .collect()
}

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

    // Untimed setup: build the multi-thread runtime once.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let t0 = Instant::now();
    rt.block_on(async move {
        let mut handles = Vec::with_capacity(pairs.len());
        for (inp, out) in &pairs {
            let inp = inp.clone();
            let out = out.clone();
            let cipher = cipher.clone();
            handles.push(tokio::spawn(async move {
                // Async IO: read (internally a spawn_blocking on tokio's
                // blocking pool).
                let plaintext = tokio::fs::read(inp).await.expect("read input");
                // AES on a blocking thread, off the async workers.
                let blob = tokio::task::spawn_blocking(move || cipher.seal(&plaintext))
                    .await
                    .expect("encrypt join");
                // Write + (optional) durable fsync as blocking IO.
                if fsync {
                    tokio::task::spawn_blocking(move || {
                        write_output(&out, &blob, true).expect("write output");
                    })
                    .await
                    .expect("write join");
                } else {
                    tokio::fs::write(out, blob).await.expect("write output");
                }
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }
    });
    let elapsed = t0.elapsed();

    drop(rt);
    (elapsed, n)
}

pub fn config_str() -> String {
    format!("{} worker threads, fs + spawn_blocking", num_cpus())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}
