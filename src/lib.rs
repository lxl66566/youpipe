//! **youpipe** — high-performance Rust concurrent pipeline batch processing
//! framework.
//!
//! # Quick start
//!
//! ```
//! use youpipe::{par_map, Pipeline, Workload};
//!
//! // One-shot parallel map
//! let results = par_map(0..1000, |x| x * 2);
//!
//! // Fused pipeline (compile-time stage fusion)
//! let results = Pipeline::from_vec(vec![])
//!     .map(|x: i32| x + 1)
//!     .filter(|x: &i32| x % 2 == 0)
//!     .collect(0..1000);
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

pub use builder::{
    Pipeline, PipelineConfig, StreamPipeline, Workload, par_chunks_map, par_map,
    par_map_with_workload, try_par_map,
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
pub use scope::{PipelineScope, ScopedPipeline, scope};
pub use state::{FenceBarrier, FenceMode, ReorderBuffer};
pub use sync::CancellationToken;
