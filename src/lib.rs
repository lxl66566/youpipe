//! **youpipe** — high-performance Rust concurrent pipeline batch processing
//! framework.
//!
//! # Quick start
//!
//! ```
//! use youpipe::pipe;
//!
//! // Data-first fused pipeline
//! let results: Vec<i32> = pipe(0..1000)
//!     .map(|x| x * 2)
//!     .collect();
//!
//! // Fallible chain (short-circuits on first Err)
//! let results: Result<Vec<i32>, &str> = pipe(0..100)
//!     .try_map(|x| if x == 50 { Err("bad") } else { Ok(x * 2) })
//!     .try_collect();
//! ```

#![warn(clippy::pedantic, clippy::cargo)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

// ── Compile-time guard: `panic = "abort"` disables panic safety ──
//
// youpipe's pool/join machinery (LeafGuard / ForEachGuard cleanup of partial
// slot state, `halt_unwinding` / `resume_unwind` propagation, `AbortIfPanic`
// guards) relies on unwinding. Under `panic = "abort"` every `catch_unwind` is
// a no-op: a panic inside any pool worker aborts the whole process instead of
// propagating to the caller — a failing item kills the process rather than
// erroring the pipeline.
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
                abort the whole process instead of propagating to the caller. The \
                LeafGuard / ForEachGuard panic-safety paths never run under abort. To restore \
                panic propagation, force `panic = \"unwind\"` for youpipe via a \
                `.cargo/config.toml` override: `[build] rustflags = [\"-C\", \"panic=unwind\"]`. \
                See youpipe's own `.cargo/config.toml` for the worked example."
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
pub(crate) mod util;

pub use builder::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, Pipe, PipelineConfig,
    StageMarker, StreamPipe, StreamStart, SyncMap, TryMap, TryPipe, Workload, pipe, stream,
};
pub use executor::ComputePool;
pub use handoff::{
    AsyncReceiver, AsyncSender, Receiver, Sender, SharedWaitGroup, async_channel, channel,
};
#[cfg(feature = "compio-runtime")]
pub use runtime::CompioPool;
#[cfg(feature = "tokio-runtime")]
pub use runtime::TokioPool;
pub use runtime::{AsyncRuntime, DefaultRuntime, NoRuntime};
pub use scope::{PipelineScope, ScopedPipe, scope};
pub use state::{FenceBarrier, FenceMode, ReorderBuffer};
pub use sync::CancellationToken;
