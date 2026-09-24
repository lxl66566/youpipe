//! Shared setup helpers for the gungraun benches.
//!
//! Input generation and pool spawns are produced here and handed to the
//! bench functions through the `#[bench]` argument expressions. Under the
//! count-all-threads caliber (see `benches/common/mod.rs`) setup is counted,
//! so every row of a comparison group must run the same setup work — hence
//! [`both_pools`].
//!
//! [`POOL_THREADS`] pins every pool to the same small size on both sides of a
//! comparison — that is what makes the counts machine-independent. The
//! default pools size themselves to `available_parallelism()`, so an 8-core
//! and a 32-core host would execute a different number of worker idle/backoff
//! rounds and the Ir counts would not be comparable. A fixed 4-worker pool
//! still exercises the full dispatch/steal/wake code surface (hybrid chunk
//! dispatch, latch spins, condvar parking) — it just does so with a
//! reproducible worker count.

use youpipe::ComputePool;

/// Worker count for every pinned pool (both youpipe and rayon rows).
///
/// 4: enough workers for cross-worker stealing and the wake cascade to be on
/// the measured path, few enough that their idle spin/yield budgets stay a
/// small fraction of real-work instructions under the ~50x-slower valgrind
/// execution.
pub const POOL_THREADS: usize = 4;

/// A freshly spawned youpipe pool with [`POOL_THREADS`] workers.
///
/// `ComputePool::new` blocks until every worker is primed, so the thread
/// spawns never land inside the measured region.
pub fn compute_pool() -> ComputePool {
    ComputePool::new(POOL_THREADS)
}
/// A youpipe pool AND a rayon pool, both with [`POOL_THREADS`] workers.
///
/// Symmetric setup for every row of a cross-library group (including rows
/// that use only one pool, or neither): under the count-all-threads caliber
/// the fixed setup cost must be identical across rows so it cancels in
/// pairwise deltas. Both pools are also torn down within the measured
/// window (at latest at bench-function exit) on every row, so teardown
/// cancels too.
pub fn both_pools() -> (ComputePool, rayon::ThreadPool) {
    (compute_pool(), rayon_pool())
}

/// A freshly spawned rayon pool with [`POOL_THREADS`] workers, for the
/// `pool.install(..)` anchor rows.
pub fn rayon_pool() -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(POOL_THREADS)
        .build()
        .expect("spawn rayon pool")
}

/// Deterministic input: `size` ascending u64s.
pub fn data(size: usize) -> Vec<u64> {
    (0..size as u64).collect()
}

/// The criterion suite's CPU kernel: first-order linear recurrence that LLVM
/// strength-reduces to ~8 instructions/item (see benchmarks.md "LLVM folds
/// constant-iteration CPU work"). Kept identical here so instruction counts
/// are relatable to the wall-clock suite's anchors.
pub fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}
