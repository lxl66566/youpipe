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
//! load (src) ────┤                                ├──► join ──► persist (async IO) ──► out (sink)
//!                └─► fetch_meta (async IO) ──────┘
//! ```
//!
//! `load` broadcasts every item to both branches (fan-out); `join` consumes
//! both inbound edges with fair round-robin (fan-in); the sink observes the
//! union — 2 items per input.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
    let sink_count = Arc::new(AtomicU64::new(0));
    let sink_sum = Arc::new(AtomicU64::new(0));

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

    let (c, s) = (sink_count.clone(), sink_sum.clone());
    let snk = g.sink("out", move |x: u64| {
        c.fetch_add(1, Ordering::Relaxed);
        s.fetch_add(x, Ordering::Relaxed);
    });

    g.edge(src, hashed);
    g.edge(src, fetched);
    g.edge(hashed, join);
    g.edge(fetched, join);
    g.edge(join, persisted);
    g.edge(persisted, snk);

    let start = std::time::Instant::now();
    g.run();
    let elapsed = start.elapsed();

    // Broadcast conservation: each of the SIZE inputs reaches both branches,
    // so the sink observes exactly 2 × SIZE items.
    let expect_sum: u64 = (0..SIZE).map(|x| hash(x) + x.wrapping_add(1)).sum();
    assert_eq!(
        sink_count.load(Ordering::Relaxed),
        2 * SIZE,
        "fan-out then fan-in must deliver every item to both branches"
    );
    assert_eq!(sink_sum.load(Ordering::Relaxed), expect_sum);

    println!("Diamond graph over {SIZE} items (sync + async branches, joined)");
    println!("  sink received {} items in {elapsed:.3?}", 2 * SIZE);
    println!();
    println!("Both branches run concurrently: the async side sleeps on the");
    println!("runtime while the sync side hashes on the compute pool, and the");
    println!("join consumes whichever branch delivers next.");
}
