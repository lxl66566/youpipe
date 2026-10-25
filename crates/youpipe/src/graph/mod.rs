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

use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

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
mod tests;

pub use crate::builder::{AsyncStageOptions, SyncStageOptions};

/// Per-node tuning for sync graph nodes ([`GraphBuilder::node_with`] /
/// [`GraphBuilder::node_expand_with`]): the streaming [`SyncStageOptions`]
/// under its graph name. `workers` pins the node's ComputePool worker count
/// (default: the run budget split evenly, every node at least one); `buffer`
/// pins the capacity of every outgoing edge of the node (default: the
/// configured `buffer_size` floored at the node's producer count × 4).
pub type NodeOptions = SyncStageOptions;

/// Per-node tuning for async graph nodes ([`GraphBuilder::node_async_with`]):
/// the streaming [`AsyncStageOptions`] under its graph name. `io_concurrency`
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

/// Output-position marker of a [`GraphBuilder::sink_collect`] node: the
/// handle is `NodeHandle<T, Collect>`, and only such a handle fits
/// [`GraphBuilder::run_collect`] — side-effect [`GraphBuilder::sink`]
/// handles (`NodeHandle<T, ()>`) are rejected at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collect;

/// Entry point for a typed DAG pipeline.
///
/// ```
/// use youpipe::graph;
///
/// // Diamond: load -> hash -> (fast, slow) -> merge -> collected output
/// let mut g = graph();
/// let src = g.source("load", 0..1_000u64);
/// let hash = g.node("hash", |x: u64| x.wrapping_mul(2_654_435_761));
/// let fast = g.node("fast", |x: u64| x / 2);
/// let slow = g.node("slow", |x: u64| x * 3);
/// let merge = g.sink_collect("merge");
/// g.edge(src, hash)
///     .edge(hash, fast)
///     .edge(hash, slow)
///     .edge(fast, merge)
///     .edge(slow, merge);
/// let out = g.run_collect(merge);
/// let expected: u64 = (0..1_000u64)
///     .map(|x| {
///         let h = x.wrapping_mul(2_654_435_761);
///         h / 2 + h * 3
///     })
///     .sum();
/// assert_eq!(out.iter().sum::<u64>(), expected);
/// ```
#[must_use]
pub fn graph() -> GraphBuilder<DefaultRuntime> {
    GraphBuilder {
        plan: plan::GraphPlan::default(),
        config: PipelineConfig::default(),
        cancel: None,
        compute_pool: None,
        async_pool: None,
        outputs: Vec::new(),
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
    /// Shared collect-sink buffers indexed by node id (`None` for
    /// non-collect nodes and for the one a `run_collect` terminal took).
    pub(crate) outputs: Vec<Option<Box<dyn Any + Send>>>,
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
            outputs: self.outputs,
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
    pub fn node<I, O, F>(&mut self, name: impl Into<Arc<str>>, f: F) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        self.node_with(name, NodeOptions::new(), f)
    }

    /// [`Self::node`] with per-node tuning — see [`NodeOptions`].
    pub fn node_with<I, O, F>(
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
    pub fn node_expand<I, O, F>(&mut self, name: impl Into<Arc<str>>, f: F) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I, &mut Vec<O>) + Send + Sync + 'static,
    {
        self.node_expand_with(name, NodeOptions::new(), f)
    }

    /// [`Self::node_expand`] with per-node tuning — see [`NodeOptions`].
    pub fn node_expand_with<I, O, F>(
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
    pub fn node_async<I, O, F, Fut>(&mut self, name: impl Into<Arc<str>>, f: F) -> NodeHandle<I, O>
    where
        I: Send + Unpin + 'static,
        O: Send + Clone + Unpin + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send,
    {
        self.node_async_with(name, AsyncNodeOptions::new(), f)
    }

    /// [`Self::node_async`] with per-node tuning — see
    /// [`AsyncNodeOptions`]. Gated behind the `tokio-runtime` feature.
    #[cfg(feature = "tokio-runtime")]
    pub fn node_async_with<I, O, F, Fut>(
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

    /// Collecting sink: accumulates every item that reaches the node into a
    /// `Vec` returned by [`run_collect`](Self::run_collect). Like
    /// [`sink`](Self::sink) fixed at one worker; the handle's output marker
    /// is [`Collect`] so only a collect-sink handle fits `run_collect`.
    ///
    /// Item order follows delivery order (multi-worker upstreams interleave);
    /// sort at the call site when order matters.
    ///
    /// # Panics
    ///
    /// A graph containing a collect sink must be run through
    /// [`run_collect`](Self::run_collect) or one of its `try_`/`_checked`
    /// variants; a plain [`run`](Self::run) panics instead of silently
    /// dropping the collected items.
    pub fn sink_collect<T>(&mut self, name: impl Into<Arc<str>>) -> NodeHandle<T, Collect>
    where
        T: Send + Unpin + 'static,
    {
        let name: Arc<str> = name.into();
        let buf: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
        let id = self.plan.push_node(
            Arc::clone(&name),
            plan::NodeKind::Sink,
            plan::NodeOpts::default(),
            exec::collect_spawn::<T>(Arc::clone(&buf)),
        );
        if self.outputs.len() <= id {
            self.outputs.resize_with(id + 1, || None);
        }
        self.outputs[id] = Some(Box::new(buf));
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
    /// between claim batches — the batched data plane once per claim batch
    /// (up to that many items can be accepted after `cancel()`), per-item
    /// paths per item — and drains fast once fired. A batch accepted before
    /// the fire is mapped and delivered in full: delivery never consults the
    /// token, so the emitted set is independent of ring pressure. `run` then
    /// returns normally (use [`run_checked`](Self::run_checked) to observe
    /// the cancellation instead of checking the token yourself). A worker
    /// panic also cancels the token (user-supplied or internal) to
    /// accelerate the drain.
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
    /// drained. Side-effect sinks keep no count, so the outcome is
    /// `RunOutcome<()>` with `emitted == 0` — for a counted partial use a
    /// collect sink with [`run_collect_checked`](Self::run_collect_checked).
    /// With no token attached the outcome is always
    /// [`Completed`](RunOutcome::Completed).
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
        // `emitted == 0`: side-effect sinks keep no count (see
        // `run_checked`). A worker panic cancels the token too, but
        // `run_graph` re-raises it above, so this read only sees user
        // cancellation.
        Ok(RunOutcome::classify((), 0, token.as_ref()))
    }

    /// Run to whole-graph EOF and return the collect sink's accumulated
    /// items. `try_run_collect` / `run_collect_checked` /
    /// `try_run_collect_checked` follow the [`run`](Self::run) family's
    /// naming.
    ///
    /// # Panics
    ///
    /// Same contract as [`run`](Self::run); additionally the graph must not
    /// contain another collect sink whose handle was not passed here (its
    /// items would drop silently).
    pub fn run_collect<T>(self, sink: NodeHandle<T, Collect>) -> Vec<T>
    where
        T: 'static,
    {
        self.try_run_collect(sink)
            .expect("graph run: failed to build async runtime (see try_run_collect)")
    }

    /// [`run_collect`](Self::run_collect) returning the async-runtime
    /// construction error instead of panicking.
    ///
    /// # Panics
    ///
    /// Same contract as [`try_run`](Self::try_run).
    pub fn try_run_collect<T>(mut self, sink: NodeHandle<T, Collect>) -> std::io::Result<Vec<T>>
    where
        T: 'static,
    {
        let buf = self.take_output::<T>(sink.id);
        exec::run_graph(self)?;
        Ok(take_items(&buf))
    }

    /// [`run_collect`](Self::run_collect) with the cancelled/completed
    /// outcome: `emitted` counts the items the collect sink received before
    /// the drain (`emitted == partial.len()`).
    ///
    /// # Panics
    ///
    /// Same contract as [`run_collect`](Self::run_collect).
    pub fn run_collect_checked<T>(self, sink: NodeHandle<T, Collect>) -> RunOutcome<Vec<T>>
    where
        T: 'static,
    {
        self.try_run_collect_checked(sink)
            .expect("graph run: failed to build async runtime (see try_run_collect)")
    }

    /// Fallible counterpart to [`run_collect_checked`](Self::run_collect_checked).
    ///
    /// # Panics
    ///
    /// Same contract as [`try_run`](Self::try_run).
    pub fn try_run_collect_checked<T>(
        mut self,
        sink: NodeHandle<T, Collect>,
    ) -> std::io::Result<RunOutcome<Vec<T>>>
    where
        T: 'static,
    {
        let token = self.cancel.clone();
        let buf = self.take_output::<T>(sink.id);
        exec::run_graph(self)?;
        let items = take_items(&buf);
        let emitted = items.len();
        Ok(RunOutcome::classify(items, emitted, token.as_ref()))
    }

    /// Take the shared buffer of the collect sink `id`, clearing its slot so
    /// the run's uncollected-collector check does not fire for it.
    fn take_output<T: 'static>(&mut self, id: usize) -> Arc<Mutex<Vec<T>>> {
        let slot = self
            .outputs
            .get_mut(id)
            .and_then(Option::take)
            .unwrap_or_else(|| panic!("graph: node {id} is not a collect-sink handle"));
        let buf: Arc<Mutex<Vec<T>>> = *slot
            .downcast()
            .expect("graph: internal type mismatch on a collect-sink buffer");
        buf
    }
}

/// Move the accumulated items out of a collect-sink buffer. `mem::take` under
/// the lock works even while a finished worker's `Arc` clone is still
/// dropping: the worker released the mutex before decrementing the run's
/// completion counter, so the lock observes every push.
fn take_items<T>(buf: &Arc<Mutex<Vec<T>>>) -> Vec<T> {
    std::mem::take(
        &mut *buf
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}
