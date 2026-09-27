//! **youpipe** — high-performance Rust concurrent pipeline batch processing
//! framework.
//!
//! # Quick start
//!
//! ```
//! use youpipe::pipe;
//!
//! // Data-first fused pipeline
//! let results: Vec<i32> = pipe(0..1000).map(|x| x * 2).collect();
//!
//! // Fallible chain (short-circuits on first Err)
//! let results: Result<Vec<i32>, &str> = pipe(0..100)
//!     .try_map(|x| {
//!         if x == 50 {
//!             Err("bad")
//!         } else {
//!             Ok(x * 2)
//!         }
//!     })
//!     .try_collect();
//! ```

#![warn(clippy::pedantic, clippy::cargo)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    // In-source `warn(clippy::cargo)` outranks the [lints] table `-A`; two syn
    // versions (2.0 via clap, 3.0 via newer derives) are unavoidable.
    clippy::multiple_crate_versions
)]

// ── Compile-time guard: `panic = "abort"` disables panic safety ──
//
// The pool/join machinery (LeafGuard / ForEachGuard cleanup of partial slot
// state, `halt_unwinding` / `resume_unwind` propagation, `AbortIfPanic` guards)
// relies on unwinding. Under `panic = "abort"` every `catch_unwind` is a no-op:
// a panic inside any pool worker aborts the whole process instead of
// propagating to the caller.
//
// This `cfg` is accurate inside the library compilation, unlike build-script
// env vars (`CARGO_CFG_PANIC` mirrors the build-script's own panic strategy,
// always `unwind`, not the target crate's — verified). The `deprecated`-const
// indirection is the standard stable-Rust trick for emitting a compile-time
// warning from a `cfg` gate without a proc-macro.
#[cfg(panic = "abort")]
const _: () = {
    #[deprecated(
        since = "0.4.0",
        note = "youpipe is compiled with `panic = \"abort\"`; any panic inside a pool worker will \
                abort the whole process instead of propagating to the caller. The LeafGuard / \
                ForEachGuard panic-safety paths never run under abort. To restore panic \
                propagation, force `panic = \"unwind\"` for youpipe via a `.cargo/config.toml` \
                override: `[build] rustflags = [\"-C\", \"panic=unwind\"]`. See youpipe's own \
                `.cargo/config.toml` for the worked example."
    )]
    const PANIC_ABORT_DISABLES_SAFETY: () = ();
    const _: () = PANIC_ABORT_DISABLES_SAFETY;
};

pub mod builder;
pub mod executor;
pub mod handoff;
pub(crate) mod pool;
pub mod prelude;
pub mod runtime;
pub mod scope;
pub mod state;
pub mod sync;

pub use builder::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, Pipe, PipeRef,
    PipelineConfig, RangePipe, StageMarker, StageOptions, StreamPipe, StreamStart, SyncMap, TryMap,
    TryPipe, TryPipeRef, Workload, pipe, pipe_range, pipe_ref, stream,
};
pub use executor::{ComputePool, compute::MAX_COMPUTE_WORKERS};
pub use handoff::{AsyncReceiver, AsyncSender, Receiver, Sender, async_channel, channel};
#[cfg(feature = "tokio-runtime")]
pub use runtime::TokioPool;
pub use runtime::{AsyncRuntime, DefaultRuntime, NoRuntime};
pub use scope::{PipelineScope, ScopedPipe, ScopedTryPipe, scope};
pub use state::{FenceBarrier, FenceMode, ReorderBuffer};
pub use sync::CancellationToken;

/// Process-wide cache of `std::thread::available_parallelism()`.
///
/// On Linux, std re-reads the cgroup CPU-quota files (`/sys/fs/cgroup/...`)
/// on *every* call — measured ~26 µs of openat/statx/read syscalls per call
/// on the 32-core Zen bench machine (2026-09, NixOS). `PipelineConfig::default()`
/// runs per `pipe()`/`pipe_ref()` construction and `resolve_exec_pool` per
/// fused terminal, so two uncached calls added ~50 µs of fixed cost to every
/// small fused batch (+60 % wall time at 1K items — regression found while
/// bisecting the horizontal suite). Affinity/quota changes after the first
/// call are intentionally not reflected; rayon sizes its global pool once for
/// the same reason.
pub(crate) fn num_cpus() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::thread::available_parallelism().map_or(4, std::num::NonZero::get))
}
