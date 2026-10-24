# DAG pipelines

`graph()` builds runtime DAGs: fan-out broadcast, fan-in joins, multiple
sources and sinks, sync CPU nodes and async IO nodes mixed in one topology —
everything the linear [`stream()`](stream.md) chain cannot express.

## When to use which engine

- [`pipe()`](pipe.md) — linear CPU chains, fused at compile time. Fastest
  per item; no channels, no fan-out.
- [`stream()`](stream.md) — linear chains that need async IO, cancellation,
  fences or expansion. Stage topology is fixed by the call chain.
- `graph()` — **non-linear** topologies: the same data item feeding several
  consumers (broadcast), several producers feeding one node (join), more than
  one source or sink. Costs a runtime topology build + validation pass where
  `stream()` pays only the channel setup.

If the workload is expressible as a chain, keep it a chain. Reach for
`graph()` when the dataflow itself branches or merges.

## Quick start

```rust
use youpipe::{NodeHandle, graph};

let mut g = graph();
let src: NodeHandle<(), u32> = g.source("load", 0..10_000);        // iterator source
let cpu = g.node("hash", Default::default(), |x: u32| x.wrapping_mul(3)); // sync node
let exp = g.node_expand("split", Default::default(), |x: u32, out: &mut Vec<u32>| {
    out.push(x);
    out.push(x + 1);                                                 // 1-to-N node
});
let snk = g.sink("out", |x: u32| { /* side effect */ });             // sink, 1 worker
g.edge(src, cpu);
g.edge(cpu, exp).edge(exp, snk);
g.run();   // or g.try_run() -> io::Result<()>, g.run_checked() -> RunOutcome<()>
```

Nodes are values: `g.edge(from, to)` connects two `NodeHandle`s (they are
`Copy`) and type-checks at compile time — `from`'s output type must equal
`to`'s input type. A full diamond:

```rust
use youpipe::graph;

let mut g = graph();
let src = g.source("load", 0..1_000u64);
let a = g.node("double", Default::default(), |x: u64| x * 2);
let b = g.node("plus", Default::default(), |x: u64| x + 1_000);
let join = g.node("join", Default::default(), |x: u64| x); // fan-in
let snk = g.sink("out", |x: u64| { /* ... */ });
g.edge(src, a);
g.edge(src, b);            // fan-out: broadcast to both branches
g.edge(a, join);
g.edge(b, join);           // fan-in: fair round-robin over inbound edges
g.edge(join, snk);
g.run();
```

With the `tokio-runtime` feature, `g.node_async(..)` runs a node's future on
the async runtime (built lazily per `run()`, or shared via
`with_async_pool`), and `g.source_async("pull", || async move { .. })` is a
pull-based source: every call returns a future yielding `Some(item)` until
the stream ends with `None` (EOF). See [`examples/graph_diamond.rs`] for a
diamond mixing sync and async branches.

[`examples/graph_diamond.rs`]:
https://github.com/lxl66566/youpipe/blob/main/crates/youpipe/examples/graph_diamond.rs

## Semantics

**Fan-out is broadcast.** Every outbound edge of a node receives a **clone**
of each item — the output type must be `Clone`. For large payloads wrap them
in `Arc<T>` (one refcount bump per edge instead of a deep copy) or share
`Arc<[T]>`/bytes.

**Fan-in requires one element type.** A node with several inbound edges is
consumed fairly (round-robin) over those edges; all inbound edges must carry
the same item type. Flows with different payload types are joined by mapping
each branch into a common type first (e.g. an `enum`).

**No reordering.** There is no `ordered()` / fence / seq machinery: items
leave a node in completion order, and a join emits whatever its inbound edges
deliver next. Deterministic aggregation is the caller's job (counters,
`reduce`-style sinks, sort at the end).

**EOF.** A `source` ends when its iterator is exhausted; a `source_async`
ends the first time its future yields `None`. Nodes drain their inbound
edges and propagate closure downstream; `run()` returns once every sink has
seen every upstream close.

**Cancellation.** `with_cancel(token)` checks the
[`CancellationToken`](https://docs.rs/youpipe/latest/youpipe/struct.CancellationToken.html)
per item on every node. After `token.cancel()` no new items are accepted,
in-flight items finish, and `run()` returns promptly with the partial output
(your sinks keep whatever they already consumed). `run_checked()` reports the
difference instead: `RunOutcome::Completed(())` versus
`RunOutcome::Cancelled` — the graph engine has no collector, so its `emitted`
is always 0; count in the sink if the partial count matters.

**Panics.** The first node panic is re-raised in the `run()` caller's thread
with the original payload. `try_run()` returns `io::Error` only for runtime
construction failures — node panics still unwind.

**Validation.** `run()` checks the topology before starting and panics,
naming the offending node: cycles, a non-source node with no inbound edge, a
source with an inbound edge, a sink with an outbound edge, duplicate node
names, duplicate edges. Type mismatches are already compile errors at
`g.edge(..)`.

**Deadlock safety.** Node workers run on the compute pool by default, but a
pool too narrow to host every blocked worker automatically falls back to
dedicated OS threads — a 10-node graph on `with_compute_workers(2)`
completes. Nesting is safe too: running a `graph()` from inside a pool
worker (e.g. inside a `pipe().for_each` closure) does not deadlock the outer
pool.

## Tuning

Graph-level knobs (same semantics as the stream engine):

| Method | Effect |
| --- | --- |
| `with_compute_pool(pool)` | sync nodes run on this pool instead of the default |
| `with_compute_workers(n)` | size of the per-run compute pool |
| `with_buffer_size(n)` | capacity of every edge channel (backpressure) |
| `with_cancel(token)` | attach a cancellation token |
| `with_async_pool(pool)` | reuse an `AsyncRuntime` pool across runs |
| `with_runtime(..)` | switch the `AsyncRuntime` backend type |

Every node takes an options argument (the `Default::default()` above). It
carries the per-node knobs with the same semantics as the stream engine's
stage options — most importantly the node's worker count. The default is
**automatic**: the pool budget is split across the graph's nodes (at least
one worker each). Pin a node's `workers` when you know better — e.g. 1 for a
cheap ordering-sensitive stage, several for the hot node of the graph; async
nodes' concurrency follows the stream engine's `io_concurrency` budget.

For heavy items remember the broadcast clone: prefer `Arc` payloads on
fan-out (or `pipe`-style ownership when no fan-out is involved). Pool and
concurrency sizing guidance lives in [pools](../advanced/pools.md) and
[tuning](../advanced/tuning.md).
