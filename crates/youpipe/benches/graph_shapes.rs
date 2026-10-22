//! Graph-engine shape benches: DAG assembly cost vs the streaming engine,
//! plus the DAG-only shapes (diamond, two-source fan-in) against a
//! workload-equivalent serial stream chain.
//!
//! * `chain_*` — the strict A/B: a source → cpu → cpu → sink linear chain
//!   driven through `graph()` vs the identical closure chain through
//!   `stream()` (a never-fired cancel token forces the streaming topology,
//!   i.e. the same channel-hop data plane the graph engine rides on). The
//!   only delta is the assembly layer (runtime plan/validation, edge
//!   connectors, budget split), so this isolates the graph engine's
//!   per-run and per-item overhead against its closest sibling.
//! * `diamond_cpu_*` — `src → hash(cpu) → (fast cpu, slow cpu) → merge(bump)
//!   → sink` on the graph engine vs the workload-equivalent serial
//!   `stream` chain (3 cpu stages + 1 bump): same closure work per source
//!   item, but the graph fans the two cpu branches out to separate node
//!   populations and fair-merges them back (sink sees 2n items). This is a
//!   shape comparison, not an engine A/B — it reports what the DAG
//!   parallelism actually buys.
//! * `diamond_cheap_graph` / `fanin_cheap_graph` — graph-only infrastructure
//!   anchors: an all-pass-through diamond isolates the broadcast-clone +
//!   fair fan-in poll cost; two sources into one node (2n items total)
//!   isolates the fan-in poll alone. Compare against `chain_cheap_graph`
//!   for the added per-item cost of each shape.
//!
//! Same-binary engine selection via `YOUPIPE_GRAPH_VARIANT=graph|stream`
//! (unset = both), mirroring `sync_fuse`'s knob so recompile layout noise
//! cannot leak into the A/B:
//!
//! ```sh
//! perf/bench-suite/bench_ab.sh -a stream=wt -b graph=wt \
//!     -E stream=YOUPIPE_GRAPH_VARIANT=stream \
//!     -E graph=YOUPIPE_GRAPH_VARIANT=graph \
//!     -B graph_shapes -r 5 'chain_cpu_.*/100000' 'chain_cheap_.*/100000'
//! ```

mod common;

use std::{
    hint::black_box as bb,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use youpipe::{CancellationToken, NodeOptions, graph, stream};

/// Per-item CPU cost of each cpu node, aligned with `mixed_load`'s
/// `youpipe_stream_cpu` anchor (50 mul-add rounds ≈ tens of ns).
const CPU_ITERS: u32 = 50;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..CPU_ITERS {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn bump(x: u64) -> u64 {
    bb(x.wrapping_add(1))
}

fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Graph,
    Stream,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Stream => "stream",
        }
    }
}

/// `YOUPIPE_GRAPH_VARIANT=graph|stream` registers only that side's ids
/// (unset = both); graph-only anchors below additionally check `Graph`.
fn want(side: Side) -> bool {
    match std::env::var("YOUPIPE_GRAPH_VARIANT").as_deref() {
        Ok(v) => v == side.label(),
        _ => true,
    }
}

/// Sink accumulator: fresh per run, summed after EOF (keeps the graph
/// output side-effect observable without returning anything from `run`).
fn run_chain_graph(v: Vec<u64>, cheap: bool) -> u64 {
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", v);
    let stage = move |x: u64| {
        if cheap {
            bump(x)
        } else {
            bb(cpu_work(x))
        }
    };
    let a = g.node("a", NodeOptions::default(), stage);
    let b = g.node("b", NodeOptions::default(), stage);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, a).edge(a, b).edge(b, out);
    g.run();
    total.load(Ordering::Relaxed)
}

fn run_chain_stream(v: Vec<u64>, cancel: &CancellationToken, cheap: bool) -> Vec<u64> {
    let stage = move |x: u64| {
        if cheap {
            bump(x)
        } else {
            bb(cpu_work(x))
        }
    };
    stream(v)
        .with_cancel(cancel.clone())
        .stage(stage)
        .stage(stage)
        .run()
}

/// `src → hash(cpu) → (fast cpu, slow cpu) → merge(bump) → sink`; the sink
/// sees 2n items. Per source item: 3× cpu + 1 bump + one clone per branch.
fn run_diamond_graph(v: Vec<u64>, cheap: bool) -> u64 {
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let stage = move |x: u64| {
        if cheap {
            bump(x)
        } else {
            bb(cpu_work(x))
        }
    };
    let mut g = graph();
    let src = g.source("src", v);
    let hash = g.node("hash", NodeOptions::default(), stage);
    let fast = g.node("fast", NodeOptions::default(), stage);
    let slow = g.node("slow", NodeOptions::default(), stage);
    let merge = g.node("merge", NodeOptions::default(), bump);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, hash)
        .edge(hash, fast)
        .edge(hash, slow)
        .edge(fast, merge)
        .edge(slow, merge)
        .edge(merge, out);
    g.run();
    total.load(Ordering::Relaxed)
}

/// Serial workload equivalent of the diamond: 3× cpu + 1 bump per item.
fn run_diamond_stream(v: Vec<u64>, cancel: &CancellationToken, cheap: bool) -> Vec<u64> {
    let stage = move |x: u64| {
        if cheap {
            bump(x)
        } else {
            bb(cpu_work(x))
        }
    };
    stream(v)
        .with_cancel(cancel.clone())
        .stage(stage)
        .stage(stage)
        .stage(stage)
        .stage(bump)
        .run()
}

/// Two sources (n items each) fanned into one pass-through node: isolates
/// the fair fan-in poll with zero fan-out clone and zero closure work.
fn run_fanin_graph(v1: Vec<u64>, v2: Vec<u64>) -> u64 {
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph();
    let s1 = g.source("s1", v1);
    let s2 = g.source("s2", v2);
    let m = g.node("m", NodeOptions::default(), bump);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(s1, m).edge(s2, m).edge(m, out);
    g.run();
    total.load(Ordering::Relaxed)
}

fn bench_graph_shapes(c: &mut Criterion) {
    // Never cancelled: `with_cancel` only forces the streaming topology on
    // the stream side, it must not change the work done.
    let cancel = CancellationToken::new();

    let mut group = c.benchmark_group("graph_shapes");
    for size in [1_000usize, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        group.throughput(Throughput::Elements(size as u64));

        for (tag, cheap) in [("cpu", false), ("cheap", true)] {
            if want(Side::Graph) {
                group.bench_with_input(
                    BenchmarkId::new(format!("chain_{tag}_graph"), size),
                    &data,
                    |b, data| {
                        b.iter_batched(
                            || warm_clone(data),
                            |v| bb(run_chain_graph(v, cheap)),
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
            if want(Side::Stream) {
                group.bench_with_input(
                    BenchmarkId::new(format!("chain_{tag}_stream"), size),
                    &data,
                    |b, data| {
                        b.iter_batched(
                            || warm_clone(data),
                            |v| bb(run_chain_stream(v, &cancel, cheap)),
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }

        if want(Side::Graph) {
            group.bench_with_input(
                BenchmarkId::new("diamond_cpu_graph", size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| bb(run_diamond_graph(v, false)),
                        BatchSize::PerIteration,
                    );
                },
            );
            group.bench_with_input(
                BenchmarkId::new("diamond_cheap_graph", size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| bb(run_diamond_graph(v, true)),
                        BatchSize::PerIteration,
                    );
                },
            );
            group.bench_with_input(
                BenchmarkId::new("fanin_cheap_graph", size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || (warm_clone(data), warm_clone(data)),
                        |(v1, v2)| bb(run_fanin_graph(v1, v2)),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
        if want(Side::Stream) {
            group.bench_with_input(
                BenchmarkId::new("diamond_cpu_stream", size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| bb(run_diamond_stream(v, &cancel, false)),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_graph_shapes
}
criterion_main!(benches);
