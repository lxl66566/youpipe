//! Curated re-exports + an extension trait so common usage needs only one line.
//!
//! ```rust
//! use youpipe::prelude::*;
//!
//! // Extension methods on every `IntoIterator` — equivalent to the free
//! // `pipe(items)` / `stream(items)` functions:
//! let r: Vec<i32> = (0..100).pipe().map(|x| x + 1).collect();
//! // (100 items: under miri a longer stream would just run slow — the
//! // single emulated pool worker triggers the dedicated-thread fallback,
//! // and miri interprets every spawned thread.)
//! let s: Vec<i32> = (0..100).stream().stage(|x| x * 2).run();
//! ```
//!
//! The free functions `pipe(items)` / `stream(items)` remain available for
//! callers that prefer the function-call style or want to keep the iterator
//! type's method namespace clean.

#[cfg(feature = "tokio-runtime")]
pub use crate::runtime::TokioPool;
pub use crate::{
    Identity, Pipe, PipelineConfig, StageOptions, StreamPipe, StreamStart, Workload,
    executor::{ComputePool, compute::MAX_COMPUTE_WORKERS},
    handoff::{Receiver, Sender, async_channel, channel},
    pipe, pipe_ref,
    runtime::{AsyncRuntime, DefaultRuntime},
    scope::{PipelineScope, ScopedPipe, ScopedTryPipe, scope},
    state::{FenceBarrier, FenceMode, ReorderBuffer},
    stream,
    sync::CancellationToken,
};

/// Data-first entry points on any [`IntoIterator`].
///
/// Implemented for every `I: IntoIterator` so callers can write
/// `items.pipe().map(...).collect()` or `items.stream().stage(...).run()`
/// after a single `use youpipe::prelude::*;`. The methods are thin wrappers
/// over the free functions [`pipe`](crate::pipe) / [`stream`](crate::stream)
/// and produce identical types — pick whichever style reads better at the
/// call site.
///
/// Not user-implementable: the blanket impl below already covers every
/// `IntoIterator`; a hand-written impl could only duplicate it.
pub trait IterExt: IntoIterator + Sized {
    /// Build a fused CPU pipeline. Equivalent to [`pipe`](crate::pipe).
    ///
    /// ```rust
    /// use youpipe::prelude::*;
    /// let r: Vec<i32> = (0..10).pipe().map(|x| x + 1).collect();
    /// assert_eq!(r, (1..=10).collect::<Vec<_>>());
    /// ```
    fn pipe(self) -> Pipe<Identity, Self::Item, Self::Item>
    where
        Self::Item: Send + 'static,
    {
        pipe(self)
    }

    /// Build a streaming pipeline. Equivalent to [`stream`](crate::stream).
    ///
    /// ```rust
    /// use youpipe::prelude::*;
    /// let r: Vec<i32> = (0..10).stream().stage(|x: i32| x + 1).run();
    /// assert_eq!(r.len(), 10);
    /// ```
    fn stream(self) -> StreamPipe<StreamStart, Self::Item, Self::Item>
    where
        Self::Item: Send + Unpin + 'static,
    {
        stream(self)
    }
}

// Blanket impl: every `IntoIterator` is a youpipe source. Not sealed — there
// is nothing to gain by implementing `IterExt` outside this crate.
impl<I: IntoIterator> IterExt for I {}
