//! Curated re-exports + an extension trait so common usage needs only one line.
//!
//! ```rust
//! use youpipe::prelude::*;
//!
//! // Extension methods on every `IntoIterator` — equivalent to the free
//! // `pipe(items)` / `stream(items)` functions:
//! let r: Vec<i32> = (0..1000).pipe().map(|x| x + 1).collect();
//! let s: Vec<i32> = (0..1000).stream().stage(|x| x * 2).run();
//! ```
//!
//! The free functions `pipe(items)` / `stream(items)` remain available for
//! callers that prefer the function-call style or want to keep the iterator
//! type's method namespace clean.

#[cfg(feature = "tokio-runtime")]
pub use crate::runtime::TokioPool;
pub use crate::{
    Identity, Pipe, PipelineConfig, StreamPipe, StreamStart, Workload,
    executor::ComputePool,
    handoff::{Receiver, Sender, async_channel, channel},
    pipe,
    runtime::{AsyncRuntime, DefaultRuntime},
    scope::{PipelineScope, ScopedPipe, scope},
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
/// Not user-implementable: the blanket impl over `IntoIterator` already
/// covers every valid source, and a hand-written impl would either duplicate
/// that coverage or break the `IntoIterator` contract. There is no super-trait
/// gate only because there is nothing to gate — users gain nothing by
/// implementing this trait themselves.
pub trait IterExt: IntoIterator + Sized {
    /// Build a fused CPU pipeline. Equivalent to [`pipe`](crate::pipe).
    ///
    /// ```rust
    /// use youpipe::prelude::*;
    /// let r: Vec<i32> = (0..10).pipe().map(|x| x + 1).collect();
    /// assert_eq!(r, (1..=10).collect::<Vec<_>>());
    /// ```
    fn pipe(self) -> crate::Pipe<crate::Identity, Self::Item, Self::Item>
    where
        Self::Item: Send + 'static,
    {
        crate::pipe(self)
    }

    /// Build a streaming pipeline. Equivalent to [`stream`](crate::stream).
    ///
    /// ```rust
    /// use youpipe::prelude::*;
    /// let r: Vec<i32> = (0..10).stream().stage(|x: i32| x + 1).run();
    /// assert_eq!(r.len(), 10);
    /// ```
    fn stream(self) -> crate::StreamPipe<crate::StreamStart, Self::Item, Self::Item>
    where
        Self::Item: Send + Unpin + 'static,
    {
        crate::stream(self)
    }
}

// Blanket impl: every `IntoIterator` is a youpipe source. Deliberately not
// sealed behind a private super-trait — there is nothing the caller could
// gain by implementing `IterExt` for a non-`IntoIterator` type that the
// blanket impl doesn't already cover.
impl<I: IntoIterator> IterExt for I {}
