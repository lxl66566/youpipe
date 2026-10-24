//! Typed DAG pipeline engine — the third engine alongside `pipe`
//! (compile-time fusion) and `stream` (channel-chained linear topology).
//!
//! `graph()` is a runtime DAG orchestration layer: it reuses the streaming
//! data plane (`handoff` channels) and schedulers (`ComputePool`,
//! `AsyncRuntime`, parking leases) and replaces only the topology assembly.
//! Nodes and edges are runtime data, so diamonds, multiple sources and
//! many-to-many fan-in compose without nesting pipelines inside closures.
//! Keep linear chains on `stream`/`pipe`; use `graph` for complex topologies
//! (no compile-time fusion, no seq reordering, no fences by design).
//!
//! # Type safety at the edges
//!
//! Every [`NodeHandle<I, O>`] pins its endpoint item types and
//! [`GraphBuilder::edge`] interlocks the two handles — `up: NodeHandle<UpIn,
//! T>` against `down: NodeHandle<T, DownOut>` — so an item-type mismatch is
//! a compile error while the topology stays runtime data. The remaining
//! mistakes (cycles, dangling nodes, duplicate names or edges) are pure
//! runtime data and reported together by `run()`'s validation pass.

use std::{future::Future, marker::PhantomData, sync::Arc};

use crate::{
    builder::PipelineConfig,
    executor::compute::ComputePool,
    runtime::{AsyncRuntime, DefaultRuntime},
    sync::{CancellationToken, RunOutcome},
};

mod async_exec;
mod exec;
mod fanin;
mod plan;

#[cfg(test)]
// `Default::default()` for node options is the documented call shape (the
// graph README example); pedantic prefers the named constructor.
#[allow(clippy::default_trait_access)]
mod tests;

pub use crate::builder::{AsyncStageOptions, SyncStageOptions};

/// Per-node tuning for sync graph nodes ([`GraphBuilder::node`] /
/// [`GraphBuilder::node_expand`]): the streaming [`SyncStageOptions`] under
/// its graph name. `workers` pins the node's ComputePool worker count
/// (default: the run budget split evenly, every node at least one); `buffer`
/// pins the capacity of every outgoing edge of the node (default: the
/// configured `buffer_size` floored at the node's producer count × 4).
pub type NodeOptions = SyncStageOptions;

/// Per-node tuning for async graph nodes ([`GraphBuilder::node_async`]): the
/// streaming [`AsyncStageOptions`] under its graph name. `io_concurrency`
/// pins the node's concurrent task fan-out (default 128); `buffer` pins
/// every outgoing edge's capacity as on [`NodeOptions`].
pub type AsyncNodeOptions = AsyncStageOptions;

/// Handle to one node registered on a [`GraphBuilder`].
///
/// The topology is runtime data; the endpoint item types `I` (input) and `O`
/// (output) are carried as `PhantomData<fn(I) -> O>` (covariant) so that
/// [`GraphBuilder::edge`] can type-interlock the two endpoints at compile
/// time. `Copy` — pass it to `edge` by value as often as needed.
pub struct NodeHandle<I, O> {
    /// Index into `GraphPlan::nodes`; minted only by the owning builder.
    id: usize,
    marker: PhantomData<fn(I) -> O>,
}

impl<I, O> NodeHandle<I, O> {
    /// The node's registration index (debugging / error reporting aid).
    #[must_use]
    pub fn id(&self) -> usize {
        self.id
    }
}

impl<I, O> Clone for NodeHandle<I, O> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I, O> Copy for NodeHandle<I, O> {}

impl<I, O> std::fmt::Debug for NodeHandle<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle").field("id", &self.id).finish()
    }
}

/// Entry point for a typed DAG pipeline.
///
/// ```
/// use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
/// use youpipe::graph;
///
/// // Diamond: load -> hash -> (fast, slow) -> merge -> out
/// let mut g = graph();
/// let src = g.source("load", 0..1_000u64);
/// let hash = g.node("hash", Default::default(), |x: u64| x.wrapping_mul(2654435761));
/// let fast = g.node("fast", Default::default(), |x: u64| x / 2);
/// let slow = g.node("slow", Default::default(), |x: u64| x * 3);
/// let total = Arc::new(AtomicU64::new(0));
/// let acc = Arc::clone(&total);
/// let out = g.sink("out", move |x: u64| {
///     acc.fetch_add(x, Ordering::Relaxed);
/// });
/// g.edge(src, hash)
///     .edge(hash, fast)
///     .edge(hash, slow)
///     .edge(fast, out)
///     .edge(slow, out);
/// g.run();
/// let expected: u64 = (0..1_000u64)
///     .map(|x| {
///         let h = x.wrapping_mul(2654435761);
///         h / 2 + h * 3
///     })
///     .sum();
/// assert_eq!(total.load(Ordering::Relaxed), expected);
/// ```
#[must_use]
pub fn graph() -> GraphBuilder<DefaultRuntime> {
    GraphBuilder {
        plan: plan::GraphPlan::default(),
        config: PipelineConfig::default(),
        cancel: None,
        compute_pool: None,
        async_pool: None,
    }
}

/// Builder for a typed DAG pipeline. Register nodes with
/// [`source`](Self::source) / [`node`](Self::node) / [`sink`](Self::sink)
/// (and their variants), connect them with [`edge`](Self::edge), then run to
/// whole-graph EOF with [`run`](Self::run) / [`try_run`](Self::try_run).
pub struct GraphBuilder<R: AsyncRuntime = DefaultRuntime> {
    pub(crate) plan: plan::GraphPlan,
    pub(crate) config: PipelineConfig,
    pub(crate) cancel: Option<CancellationToken>,
    pub(crate) compute_pool: Option<ComputePool>,
    pub(crate) async_pool: Option<R>,
}

impl GraphBuilder<DefaultRuntime> {
    /// Switch the async runtime backend type (e.g. to a custom
    /// [`AsyncRuntime`](crate::AsyncRuntime) impl). Generic conversion in the
    /// spirit of `StreamPipe::with_async_pool`: the backend instance itself
    /// is built lazily per run, or attached with
    /// [`with_async_pool`](Self::with_async_pool) after switching.
    ///
    /// A previously attached backend instance of the old type is dropped.
    #[must_use]
    pub fn with_runtime<R: AsyncRuntime>(self) -> GraphBuilder<R> {
        GraphBuilder {
            plan: self.plan,
            config: self.config,
            cancel: self.cancel,
            compute_pool: self.compute_pool,
            async_pool: None,
        }
    }
}

/// Spawn-step placeholder until the execution engines land; replaced by the
/// per-kind spawn closures in `exec` / `async_exec`.
impl<R: AsyncRuntime> GraphBuilder<R> {
    /// Iterator source: a single feeder pulls `iter` and broadcasts each item
    /// to every outgoing edge (hence `T: Clone`; wrap bulky payloads in
    /// `Arc` for cheap fan-out).
    pub fn source<T, I>(&mut self, name: impl Into<Arc<str>>, iter: I) -> NodeHandle<(), T>
    where
        T: Send + Clone + Unpin + 'static,
        I: IntoIterator<Item = T> + Send + 'static,
        // The single feeder job pulls the iterator on another thread.
        I::IntoIter: Send + 'static,
    {
        let id = self.plan.push_node(
            name.into(),
            plan::NodeKind::Source,
            plan::NodeOpts::default(),
            exec::source_spawn::<T, I>(iter),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Pull-based async source: one runtime task calls `f` repeatedly;
    /// `None` ends the stream. Runs on the async runtime backend; the
    /// downstream edge carries an async sender.
    pub fn source_async<T, F, Fut>(&mut self, name: impl Into<Arc<str>>, f: F) -> NodeHandle<(), T>
    where
        T: Send + Clone + Unpin + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<T>> + Send,
    {
        let id = self.plan.push_node(
            name.into(),
            plan::NodeKind::SourceAsync,
            plan::NodeOpts::default(),
            async_exec::source_async_spawn::<T, F, Fut>(f),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Sync CPU node: `f: Fn(I) -> O` runs on the ComputePool. `O: Clone` is
    /// the broadcast fan-out requirement (paid even on single-output nodes,
    /// keeping the rule simple). Multiple workers (see [`NodeOptions`])
    /// share every input edge and every output edge.
    pub fn node<I, O, F>(
        &mut self,
        name: impl Into<Arc<str>>,
        opts: NodeOptions,
        f: F,
    ) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        let name: Arc<str> = name.into();
        let id = self.plan.push_node(
            Arc::clone(&name),
            plan::NodeKind::Sync,
            plan::NodeOpts {
                workers: opts.workers,
                buffer: opts.buffer,
                io_concurrency: None,
            },
            exec::node_spawn::<I, O, F>(name, f),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Sync one-to-many node: `f` expands one input into zero or more
    /// outputs by pushing into a per-worker reused scratch `Vec` (zero
    /// steady-state allocation). Each expanded item is broadcast to every
    /// outgoing edge like a [`node`](Self::node) output.
    pub fn node_expand<I, O, F>(
        &mut self,
        name: impl Into<Arc<str>>,
        opts: NodeOptions,
        f: F,
    ) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I, &mut Vec<O>) + Send + Sync + 'static,
    {
        let name: Arc<str> = name.into();
        let id = self.plan.push_node(
            Arc::clone(&name),
            plan::NodeKind::Expand,
            plan::NodeOpts {
                workers: opts.workers,
                buffer: opts.buffer,
                io_concurrency: None,
            },
            exec::expand_node_spawn::<I, O, F>(name, f),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Async IO node: `f: Fn(I) -> Future<Output = O>` runs as
    /// `io_concurrency` concurrent tasks on the async runtime backend (see
    /// [`AsyncNodeOptions`]). Gated behind the `tokio-runtime` feature like
    /// the stream engine's async stage API.
    #[cfg(feature = "tokio-runtime")]
    pub fn node_async<I, O, F, Fut>(
        &mut self,
        name: impl Into<Arc<str>>,
        opts: AsyncNodeOptions,
        f: F,
    ) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send,
    {
        let name: Arc<str> = name.into();
        let id = self.plan.push_node(
            Arc::clone(&name),
            plan::NodeKind::Async,
            plan::NodeOpts {
                workers: None,
                buffer: opts.buffer,
                io_concurrency: opts.io_concurrency,
            },
            async_exec::node_async_spawn::<I, O, F, Fut>(name, f),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Sink: side-effect consumer, fixed at one worker. For a multi-worker
    /// consumer use [`node`](Self::node) and ignore its output.
    pub fn sink<I, F>(&mut self, name: impl Into<Arc<str>>, f: F) -> NodeHandle<I, ()>
    where
        I: Send + Unpin + 'static,
        F: Fn(I) + Send + Sync + 'static,
    {
        let name: Arc<str> = name.into();
        let id = self.plan.push_node(
            Arc::clone(&name),
            plan::NodeKind::Sink,
            plan::NodeOpts::default(),
            exec::sink_spawn::<I, F>(name, f),
        );
        NodeHandle {
            id,
            marker: PhantomData,
        }
    }

    /// Connect `up`'s output to `down`'s input. The item type is pinned by
    /// the two handles (compile-time check); the `(up_id, down_id)` pair is
    /// recorded as runtime data. Duplicate node pairs are rejected at
    /// `run()`-time validation.
    pub fn edge<UpIn, T, DownOut>(
        &mut self,
        up: NodeHandle<UpIn, T>,
        down: NodeHandle<T, DownOut>,
    ) -> &mut Self
    where
        T: Send + Clone + Unpin + 'static,
    {
        let up_async = self.plan.nodes[up.id].kind.is_async();
        let down_async = self.plan.nodes[down.id].kind.is_async();
        self.plan.edges.push(plan::EdgePlan {
            up: up.id,
            down: down.id,
            connector: plan::make_connector::<T>(up_async, down_async),
        });
        self
    }

    /// Attach a custom [`ComputePool`] for sync nodes and feeders (default:
    /// the global pool). Same semantics as `StreamPipe::with_compute_pool`.
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Pin the compute-worker budget divided across sync nodes. Same
    /// semantics as `StreamPipe::with_compute_workers`.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see [`PipelineConfig::with_compute_workers`]).
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Set the per-edge buffer capacity between nodes (floored at the
    /// producing node's worker count × 4; a node's `buffer` pin overrides
    /// its own outgoing edges). Same semantics as
    /// `StreamPipe::with_buffer_size`.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` (see [`PipelineConfig::with_buffer_size`]).
    #[must_use]
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.config = self.config.with_buffer_size(n);
        self
    }

    /// Attach a [`CancellationToken`]: every worker / feeder / task checks it
    /// per batch and drains fast once fired; `run` then returns normally
    /// (use [`run_checked`](Self::run_checked) to observe the cancellation
    /// instead of checking the token yourself). A worker panic also cancels
    /// the token (user-supplied or internal) to accelerate the drain.
    #[must_use]
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Attach a managed async runtime backend so async nodes and sources
    /// reuse it across runs instead of building a transient one per `run()`
    /// (same rationale as `StreamPipe::with_async_pool`).
    #[must_use]
    pub fn with_async_pool(mut self, rt: R) -> Self {
        self.async_pool = Some(rt);
        self
    }

    /// Run to whole-graph EOF, returning the async-runtime construction
    /// error instead of panicking.
    ///
    /// # Panics
    ///
    /// Panics on topology validation failure (message names the offending
    /// nodes). A panic inside any node/feeder/task is re-raised on the
    /// calling thread after the graph drained (first panic wins).
    pub fn try_run(self) -> std::io::Result<()> {
        exec::run_graph(self)
    }

    /// [`try_run`](Self::try_run)'s unwrap-style wrapper.
    ///
    /// # Panics
    ///
    /// Same contract as [`try_run`](Self::try_run); an `Err` (async-runtime
    /// construction failure) panics here.
    pub fn run(self) {
        self.try_run()
            .expect("graph run: failed to build async runtime (see try_run)");
    }

    /// Run the graph and report whether it completed or was cancelled —
    /// [`run`](Self::run)'s opt-in counterpart that makes the cancelled
    /// short-circuit observable.
    ///
    /// Identical execution to [`run`](Self::run) (same validation panics,
    /// same panic re-raising — a node panic unwinds the caller before any
    /// outcome exists); the only addition is one token read after every sink
    /// drained. The graph engine has no collector (sinks are user nodes), so
    /// the outcome is `RunOutcome<()>`: a cancelled run reports
    /// [`Cancelled`](RunOutcome::Cancelled) with `emitted == 0` — count in
    /// the sink if the partial count matters. With no token attached the
    /// outcome is always [`Completed`](RunOutcome::Completed).
    ///
    /// # Panics
    ///
    /// Same contract as [`run`](Self::run).
    pub fn run_checked(self) -> RunOutcome<()> {
        self.try_run_checked()
            .expect("graph run: failed to build async runtime (see try_run)")
    }

    /// Fallible counterpart to [`run_checked`](Self::run_checked), as
    /// [`try_run`](Self::try_run) is to [`run`](Self::run).
    ///
    /// # Panics
    ///
    /// Same contract as [`try_run`](Self::try_run).
    pub fn try_run_checked(self) -> std::io::Result<RunOutcome<()>> {
        let token = self.cancel.clone();
        exec::run_graph(self)?;
        // `emitted == 0`: no collector exists — sinks are user nodes (see
        // `run_checked`). A worker panic cancels the token too, but
        // `run_graph` re-raises it above, so this read only sees user
        // cancellation.
        Ok(RunOutcome::classify((), 0, token.as_ref()))
    }
}
