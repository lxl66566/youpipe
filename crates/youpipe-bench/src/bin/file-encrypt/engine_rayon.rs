//! rayon baseline: the idiomatic "just parallelise over files".
//!
//! `par_iter().for_each(..)` over the file list on rayon's default global
//! pool (one thread per core). Each task does read → encrypt → write back to
//! back on the same thread — no pipelining between files' stages, and
//! concurrency is capped at `num_cpus` because the blocking file IO holds the
//! rayon worker thread for its whole duration.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use rayon::prelude::*;

use crate::{
    cipher::Cipher,
    workload::{FileEntry, evict_dir_cache, write_output},
};

pub const NAME: &str = "rayon par_iter (default pool)";

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
    // Warm the global pool so its lazy construction (first `par_iter` call)
    // isn't charged to rayon — mirrors how the other engines pre-build their
    // pools before the clock starts.
    (0..num_cpus()).into_par_iter().for_each(|_| {});

    let t0 = Instant::now();
    pairs.par_iter().for_each(move |(inp, out)| {
        let plaintext = std::fs::read(inp).expect("read input");
        let blob = cipher.seal(&plaintext);
        write_output(out, &blob, fsync).expect("write output");
    });
    let elapsed = t0.elapsed();

    (elapsed, n)
}

pub fn config_str() -> String {
    format!("default global pool ({} threads)", num_cpus())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}
