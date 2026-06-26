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

#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

pub mod builder;
pub mod executor;
pub mod handoff;
pub(crate) mod pool;
pub mod runtime;
pub mod scope;
pub mod state;
pub mod sync;
pub(crate) mod util;

pub use builder::{Pipe, StreamPipe, TryPipe, pipe, stream};
pub use builder::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, PipelineConfig,
    StageMarker, SyncMap, TryMap, Workload,
};
#[cfg(feature = "tokio-runtime")]
pub use executor::AsyncPool;
pub use executor::ComputePool;
pub use handoff::{
    AsyncReceiver, AsyncSender, BatchConfig, Receiver, Sender, SharedBatcher, SharedRingBuffer,
    SharedWaitGroup, async_channel, channel,
};
pub use runtime::Runtime;
#[cfg(feature = "tokio-runtime")]
pub use runtime::TokioRuntime;
pub use scope::{PipelineScope, ScopedPipe, scope};
pub use state::{FenceBarrier, FenceMode, ReorderBuffer};
pub use sync::CancellationToken;
