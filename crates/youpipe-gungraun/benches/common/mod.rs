//! Shared gungraun harness config for the youpipe benches.
//!
//! # Why the default toggle cannot see the pool
//!
//! gungraun's default `EntryPoint::Default` drives Callgrind's
//! `--toggle-collect=<bench fn>` + `--collect-atstart=no`: collection is a
//! **per-thread** state, flipped only on the thread(s) executing the bench
//! function. A thread-pool library's workers never enter that function, so
//! with the default setup a `pipe(..).collect()` benchmark counts only the
//! driver-side dispatch (~26 kIr) and none of the worker execution —
//! verified empirically (10 k channel handoffs reported 8.7 kIr).
//!
//! # The counting-everything caliber
//!
//! [`count_all_threads`] disables the toggle (`EntryPoint::None`) and starts
//! collection at process start, so every thread's instructions are summed.
//! The reported number is then a **process total**: gungraun scaffolding +
//! setup + measured op + teardown. That is still fully deterministic and
//! exactly what regression baselines need; to keep cross-row deltas pure,
//! every row of a group runs the *same* setup work (`both_pools()` spawns a
//! youpipe pool AND a rayon pool even where the row uses only one), so the
//! fixed offset cancels in every pairwise comparison.
//!
//! (`callgrind::zero_stats()` at bench-function entry would shave the setup
//! offset off instead, but the client-requests feature needs valgrind
//! headers + libclang at build time — not worth the environment dependency
//! for a constant that cancels anyway.)

use gungraun::{Callgrind, EntryPoint, LibraryBenchmarkConfig};

/// Count instructions on **all** threads (see the module docs for why the
/// default per-function toggle is unusable for pool benches).
pub fn count_all_threads() -> LibraryBenchmarkConfig {
    LibraryBenchmarkConfig::default()
        .tool(
            Callgrind::with_args(["--collect-atstart=yes"]).entry_point(EntryPoint::None),
        )
        .clone()
}
