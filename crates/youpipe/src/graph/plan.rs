//! Build-time structures of the typed DAG engine: the type-erased node and
//! edge tables, plus topology validation.
//!
//! The builder API is typed — every [`NodeHandle`](super::NodeHandle) pins
//! its endpoint item types and `edge()` interlocks them at compile time —
//! but the topology itself is runtime data. Nodes therefore store
//! type-erased spawn steps and edges store type-erased channel connectors,
//! both created at the `source`/`node`/`edge` call sites where the concrete
//! item types are still known; `run()` validates the topology, then walks it
//! in topological order connecting everything (see `exec`).

use std::{
    any::Any,
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};

use super::exec::GraphCore;

/// Type-erased channel endpoint handed between the run walk and the nodes'
/// spawn steps. The concrete type inside is fixed by construction — the
/// connector is created where the edge's item type is pinned by both handles
/// — so a downcast mismatch is an internal bug, never a user error.
pub(crate) type Erased = Box<dyn Any + Send>;

/// One node's type-erased spawn step: receives the run context, the node's
/// resolved worker count, its input endpoints (in in-edge registration
/// order) and its output endpoints (in out-edge registration order), and
/// spawns the node's workers or runtime tasks. Endpoint ownership transfers
/// wholly — the run walk never retains a clone that would hold a channel
/// open after the node took over.
pub(crate) type SpawnStep = Box<dyn FnOnce(&GraphCore, usize, Vec<Erased>, Vec<Erased>) + Send>;

/// One edge's type-erased channel factory: receives the run context and the
/// edge's resolved buffer capacity, creates the channel (and, for an async
/// upstream feeding a sync downstream, spawns the bridge thread), and
/// returns the (upstream sender, downstream receiver) pair.
pub(crate) type EdgeConnector = Box<dyn FnOnce(&GraphCore, usize) -> (Erased, Erased) + Send>;

/// Node classification. Drives validation, the edge channel flavour and the
/// worker-budget accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    /// Iterator source: one sync feeder worker pulling the iterator.
    Source,
    /// Pull-based async source: one runtime task calling `f` until `None`.
    SourceAsync,
    /// Sync 1-to-1 map node: ComputePool workers.
    Sync,
    /// Sync 1-to-N expansion node: ComputePool workers.
    Expand,
    /// Async IO node: `io_concurrency` runtime tasks. Constructed only by
    /// `node_async`, which — like the stream engine's async stages — exists
    /// only under the `tokio-runtime` feature.
    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    Async,
    /// Side-effect terminal: exactly one sync worker.
    Sink,
}

impl NodeKind {
    /// Whether this node is a source (sources must not have incoming edges).
    pub(crate) fn is_source(self) -> bool {
        matches!(self, NodeKind::Source | NodeKind::SourceAsync)
    }

    /// Whether this node's producers are async-runtime tasks — the sender
    /// side of every outgoing edge is an `AsyncSender`.
    pub(crate) fn is_async(self) -> bool {
        matches!(self, NodeKind::SourceAsync | NodeKind::Async)
    }
}

/// Per-node option pins captured at registration; unset fields fall back to
/// the run-level budget / buffer resolution (see `exec::run_graph`).
#[derive(Default)]
pub(crate) struct NodeOpts {
    // Read by the worker-budget / buffer resolution landing with the sync
    // execution engine.
    #[allow(dead_code)]
    pub(crate) workers: Option<NonZeroUsize>,
    #[allow(dead_code)]
    pub(crate) buffer: Option<NonZeroUsize>,
    #[allow(dead_code)]
    pub(crate) io_concurrency: Option<NonZeroUsize>,
}

/// One registered node: name, kind, option pins and the type-erased spawn
/// step built at registration.
pub(crate) struct NodePlan {
    pub(crate) name: Arc<str>,
    pub(crate) kind: NodeKind,
    pub(crate) opts: NodeOpts,
    pub(crate) spawn: SpawnStep,
}

/// One registered edge: endpoint node ids plus the type-erased connector
/// built at the `edge()` call site.
pub(crate) struct EdgePlan {
    pub(crate) up: usize,
    pub(crate) down: usize,
    pub(crate) connector: EdgeConnector,
}

/// The runtime topology under construction.
#[derive(Default)]
pub(crate) struct GraphPlan {
    pub(crate) nodes: Vec<NodePlan>,
    pub(crate) edges: Vec<EdgePlan>,
}

impl GraphPlan {
    /// Append one node, returning its id (the index into `nodes`).
    pub(crate) fn push_node(
        &mut self,
        name: Arc<str>,
        kind: NodeKind,
        opts: NodeOpts,
        spawn: SpawnStep,
    ) -> usize {
        let id = self.nodes.len();
        self.nodes.push(NodePlan {
            name,
            kind,
            opts,
            spawn,
        });
        id
    }

    /// Validate the topology and compute its topological order.
    ///
    /// Reports every violation at once so one `run()` surfaces the whole
    /// malformed graph instead of one error per iteration: cycles, non-source
    /// nodes without incoming edges, sources with incoming edges, sinks with
    /// outgoing edges, duplicate node names and duplicate edges. Item-type
    /// mistakes are impossible here — `edge()` rejects them at compile time —
    /// so a runtime error list is the correct layering.
    ///
    /// Kahn's algorithm doubles as the cycle detector: when fewer nodes than
    /// registered reach in-degree zero, the remainder sits on a cycle. The
    /// returned order is Kahn-stable (sources in registration order, seeded
    /// through a FIFO), which also guarantees every source feeder is spawned
    /// before any downstream worker job.
    pub(crate) fn validate_and_order(&self) -> Result<Vec<usize>, Vec<String>> {
        let mut errors = Vec::new();
        let n = self.nodes.len();

        let mut names = HashSet::with_capacity(n);
        for node in &self.nodes {
            if !names.insert(node.name.clone()) {
                errors.push(format!("duplicate node name `{}`", node.name));
            }
        }

        let mut edge_pairs = HashSet::with_capacity(self.edges.len());
        let mut in_deg = vec![0usize; n];
        let mut out_deg = vec![0usize; n];
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
        for edge in &self.edges {
            if !edge_pairs.insert((edge.up, edge.down)) {
                errors.push(format!(
                    "duplicate edge `{}->{}`",
                    self.nodes[edge.up].name, self.nodes[edge.down].name
                ));
            }
            in_deg[edge.down] += 1;
            out_deg[edge.up] += 1;
            adjacency[edge.up].push(edge.down);
        }

        for (i, node) in self.nodes.iter().enumerate() {
            if node.kind.is_source() {
                if in_deg[i] > 0 {
                    errors.push(format!(
                        "source `{}` must not have incoming edges",
                        node.name
                    ));
                }
            } else if in_deg[i] == 0 {
                errors.push(format!(
                    "node `{}` requires at least one incoming edge",
                    node.name
                ));
            }
            if node.kind == NodeKind::Sink && out_deg[i] > 0 {
                errors.push(format!(
                    "sink `{}` must not have outgoing edges",
                    node.name
                ));
            }
        }

        // Kahn topological sort (FIFO: registration-stable, sources first).
        let mut deg = in_deg;
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| deg[i] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(u) = queue.pop_front() {
            order.push(u);
            for &down in &adjacency[u] {
                deg[down] -= 1;
                if deg[down] == 0 {
                    queue.push_back(down);
                }
            }
        }
        if order.len() < n {
            let stuck: Vec<Arc<str>> = (0..n)
                .filter(|i| deg[*i] > 0)
                .map(|i| Arc::clone(&self.nodes[i].name))
                .collect();
            let names: Vec<&str> = stuck.iter().map(|s| &**s).collect();
            errors.push(format!("cycle detected involving: {}", names.join(", ")));
        }

        if errors.is_empty() {
            Ok(order)
        } else {
            Err(errors)
        }
    }
}

/// Build the type-erased connector for one edge. Created at the `edge()`
/// call site where the item type `T` is pinned by both handles; the
/// (upstream-kind, downstream-kind) pair selects the channel flavour so each
/// node's sender and receiver types stay single — zero dynamic dispatch on
/// the hot path:
///
/// | upstream | downstream | channel                                        |
/// |----------|------------|------------------------------------------------|
/// | sync     | sync       | `channel` (SyncSender, Receiver)               |
/// | sync     | async      | `sync_async_channel` (SyncSender, AsyncRecv)   |
/// | async    | async      | `async_channel` (AsyncSender, AsyncRecv)       |
/// | async    | sync       | `async_channel` + bridge thread (Receiver)     |
///
/// The mixed-mode channel lets sync workers push straight into an async
/// consumer; the bridge case runs the reverse direction on a dedicated OS
/// thread (a blocking `SyncSender::send` must never run inside a runtime
/// task — see `async_exec::bridge`).
pub(crate) fn make_connector<T>(up_async: bool, down_async: bool) -> EdgeConnector
where
    T: Send + Clone + Unpin + 'static,
{
    Box::new(move |core: &GraphCore, buffer: usize| {
        // Annotated so the arms' heterogeneous concrete tuples coerce to the
        // erased pair (inference would otherwise pick the first arm's types).
        let pair: (Erased, Erased) = match (up_async, down_async) {
            (false, false) => {
                let (tx, rx) = crate::handoff::channel::<T>(buffer);
                (Box::new(tx), Box::new(rx))
            },
            (false, true) => {
                let (tx, rx) = crate::handoff::sync_async_channel::<T>(buffer);
                (Box::new(tx), Box::new(rx))
            },
            (true, true) => {
                let (tx, rx) = crate::handoff::async_channel::<T>(buffer);
                (Box::new(tx), Box::new(rx))
            },
            (true, false) => {
                let (tx, a_rx) = crate::handoff::async_channel::<T>(buffer);
                let rx = super::async_exec::bridge(core, a_rx, buffer);
                (Box::new(tx), Box::new(rx))
            },
        };
        pair
    })
}
