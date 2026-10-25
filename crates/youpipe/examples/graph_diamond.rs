//! DAG pipeline: diamond fan-out/fan-in with mixed sync CPU and async IO
//! nodes in one `graph()`.
//!
//! The graph engine orchestrates runtime DAGs — topologies the linear
//! `stream()` chain cannot express: broadcast fan-out, fair fan-in joins,
//! multiple sources and sinks. Data flows over the same MPMC channels as the
//! streaming engine; sync nodes run on the compute pool, async nodes on the
//! tokio runtime (built lazily on `run()`).
//!
//! ```text
//! cargo run --release --example graph_diamond
//! ```
//!
//! Topology:
//!
//! ```text
//!                ┌─► hash (sync CPU) ─────────────┐
//! load (src) ────┤                                ├──► join ──► persist (async IO) ──► out (collect)
//!                └─► fetch_meta (async IO) ──────┘
//! ```
//!
//! `load` broadcasts every item to both branches (fan-out); `join` consumes
//! both inbound edges with fair round-robin (fan-in); the collect sink
//! returns the union — 2 items per input — straight out of `run_collect`.

use youpipe::graph;

const SIZE: u64 = 2_000;

/// Sync CPU work: iterated mixing, the same shape as the fused examples.
fn hash(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn main() {
    let mut g = graph();
    let src = g.source("load", 0..SIZE);

    // Sync branch: CPU hash on the work-stealing compute pool.
    let hashed = g.node("hash", |x: u64| hash(x));

    // Async branch: simulated IO wait on the tokio runtime (M:N tasks).
    let fetched = g.node_async("fetch_meta", |x: u64| async move {
        if x % 10 == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        x.wrapping_add(1)
    });

    // Fan-in: `join` round-robins over both inbound edges (same element type).
    let join = g.node("join", |x: u64| x);

    // Async sink-side node: simulated durable write.
    let persisted = g.node_async("persist", |x: u64| async move {
        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        x
    });

    let snk = g.sink_collect("out");

    g.edge(src, hashed);
    g.edge(src, fetched);
    g.edge(hashed, join);
    g.edge(fetched, join);
    g.edge(join, persisted);
    g.edge(persisted, snk);

    let start = std::time::Instant::now();
    let out = g.run_collect(snk);
    let elapsed = start.elapsed();

    // Broadcast conservation: each of the SIZE inputs reaches both branches,
    // so the collect sink returns exactly 2 × SIZE items. Sums wrap (the
    // full-range hash overflows u64 long before 2 × SIZE items).
    let wrapping_sum = |items: &[u64]| items.iter().fold(0u64, |acc, x| acc.wrapping_add(*x));
    let expect: u64 = (0..SIZE).fold(0u64, |acc, x| {
        acc.wrapping_add(hash(x)).wrapping_add(x.wrapping_add(1))
    });
    assert_eq!(
        out.len(),
        usize::try_from(2 * SIZE).unwrap(),
        "fan-out then fan-in must deliver every item to both branches"
    );
    assert_eq!(wrapping_sum(&out), expect);

    println!("Diamond graph over {SIZE} items (sync + async branches, joined)");
    println!("  collected {} items in {elapsed:.3?}", 2 * SIZE);
    println!();
    println!("Both branches run concurrently: the async side sleeps on the");
    println!("runtime while the sync side hashes on the compute pool, and the");
    println!("join consumes whichever branch delivers next.");
}
