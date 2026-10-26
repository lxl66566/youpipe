//! Differential tests: the same topology and transforms run through the
//! stream engine and the graph engine, outputs compared as multisets.
//!
//! The engines share the data plane (handoff channels, batched claims,
//! cancellation-blind delivery) but assemble it differently — stream stages
//! anchor on one ring, graph fan-ins round over many — so an acceptance or
//! delivery divergence in either engine shows up here as a cross-engine
//! diff. Fixtures: linear chain, broadcast diamond (graph) against the
//! equivalent per-item dual emission (stream `expand`), variable expansion,
//! and cancel-at-k partial invariants.

use std::time::{Duration, Instant};

use youpipe::{CancellationToken, RunOutcome, graph, stream};

/// Miri interprets every spawned thread and channel atomic; scale item
/// counts down but keep the same dispatch paths per item (same convention as
/// `graph_dag.rs`).
fn n_items(native: u64) -> u64 {
    if cfg!(miri) {
        native / 20
    } else {
        native
    }
}

fn f1(x: u64) -> u64 {
    x.wrapping_mul(3).wrapping_add(1)
}

fn f2(x: u64) -> u64 {
    x.wrapping_mul(31).wrapping_add(7)
}

/// Variable 0/1/2-per-item expansion (the zero arm included: an expansion
/// that swallows items must not swallow their channel bookkeeping).
fn expand_rule(x: u64, out: &mut Vec<u64>) {
    for k in 0..x % 3 {
        out.push(x.wrapping_add(k));
    }
}

/// Compare as multisets: delivery order is engine- and schedule-dependent
/// by contract, the emitted set is not.
fn multiset(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v
}

/// Bounded failure for deadlock-class regressions (same contract as
/// `graph_dag.rs`'s `run_with_timeout`): a wedged engine fails the test in
/// bounded time instead of hanging the suite.
fn run_with_timeout<T: Send + 'static>(label: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{label}: run did not finish within 30s (deadlock?)"))
}

#[test]
fn linear_chain_matches() {
    let n = n_items(10_000);
    let via_stream = stream(0..n).stage(f1).stage(f2).run();

    let mut g = graph();
    let src = g.source("src", 0..n);
    let a = g.node("a", f1);
    let b = g.node("b", f2);
    let out = g.sink_collect("out");
    g.edge(src, a).edge(a, b).edge(b, out);
    let via_graph = g.run_collect(out);

    let mut expect: Vec<u64> = (0..n).map(|x| f2(f1(x))).collect();
    expect.sort_unstable();
    assert_eq!(via_stream.len(), usize::try_from(n).unwrap());
    assert_eq!(multiset(via_graph), expect, "graph must match");
    assert_eq!(multiset(via_stream), expect);
}

/// The graph diamond's joined multiset equals "both branch outputs per
/// item", which the linear-only stream engine spells as one `expand` —
/// the broadcast + fan-in machinery is what the comparison guards. A tiny
/// buffer forces backpressure through the broadcast clone path.
#[test]
fn broadcast_diamond_matches_stream_expand() {
    let n = n_items(5_000);
    let via_stream = stream(0..n)
        .expand(|x| vec![x.wrapping_mul(2), x.wrapping_add(1000)])
        .run();

    let mut g = graph();
    let src = g.source_broadcast("src", 0..n);
    let a = g.node("a", |x: u64| x.wrapping_mul(2));
    let b = g.node("b", |x: u64| x.wrapping_add(1000));
    let join = g.node("join", |x: u64| x);
    let out = g.sink_collect("out");
    g.edge(src, a).edge(src, b);
    g.edge(a, join).edge(b, join).edge(join, out);
    g = g.with_buffer_size(4);
    let via_graph = g.run_collect(out);

    let n_usize = usize::try_from(n).unwrap();
    assert_eq!(via_stream.len(), 2 * n_usize);
    assert_eq!(via_graph.len(), 2 * n_usize);
    assert_eq!(multiset(via_graph), multiset(via_stream));
}

#[test]
fn expansion_matches() {
    let n = n_items(5_000);
    // The expansion consumes the raw input: `expand_rule` varies over
    // x % 3, and any map ahead of it would fold that away (f1's 3x + 1 is
    // always 1 mod 3 — a fixed 1-per-item fan-out, not the variable shape
    // this fixture exists to exercise).
    let via_stream = stream(0..n).expand(|x| {
        let mut v = Vec::new();
        expand_rule(x, &mut v);
        v
    });

    // miri: the expand staging allocates per item; keep the interpreted
    // pass inside the watchdog budget.
    let via_stream = run_with_timeout("stream expand", || via_stream.run());

    let mut g = graph();
    let src = g.source("src", 0..n);
    let exp = g.node_expand("exp", expand_rule);
    let out = g.sink_collect("out");
    g.edge(src, exp).edge(exp, out);
    let via_graph = run_with_timeout("graph expand", move || g.run_collect(out));

    let mut expect: Vec<u64> = (0..n)
        .flat_map(|x| (0..x % 3).map(move |k| x + k))
        .collect();
    expect.sort_unstable();
    assert_eq!(multiset(via_graph), expect, "graph must match");
    assert_eq!(multiset(via_stream), expect);
}

/// Cancel fired mid-stream by the workload itself (deterministic — no
/// wall-clock timing): the two engines' partials may differ in size
/// (acceptance granularity is per claim batch on both, batch sizes differ)
/// but must satisfy the same invariants — `Cancelled` with
/// `emitted == partial.len()`, a non-empty partial strictly below `n`, and
/// every partial item a genuine full-run item (`f2` is injective mod 2^64,
/// so membership is unambiguous).
#[test]
fn cancel_at_k_partial_invariants_match() {
    let n = n_items(50_000);
    let k = n / 4;

    let token_s = CancellationToken::new();
    let fire = token_s.clone();
    let gate_s = move |x: u64| {
        if x == k {
            fire.cancel();
        }
        x
    };
    let via_stream = stream(0..n)
        .with_cancel(token_s)
        .stage(gate_s)
        .stage(f2)
        .run_checked();

    let token_g = CancellationToken::new();
    let fire = token_g.clone();
    let mut g = graph();
    let src = g.source("src", 0..n);
    let gate = g.node("gate", move |x: u64| {
        if x == k {
            fire.cancel();
        }
        x
    });
    let m = g.node("m", f2);
    let out = g.sink_collect("out");
    g.edge(src, gate).edge(gate, m).edge(m, out);
    let via_graph = run_with_timeout("graph cancel", move || {
        g.with_cancel(token_g).run_collect_checked(out)
    });

    let full: Vec<u64> = (0..n).map(f2).collect();
    let n_usize = usize::try_from(n).unwrap();
    let start = Instant::now();
    for (label, outcome) in [("stream", via_stream), ("graph", via_graph)] {
        let RunOutcome::Cancelled { partial, emitted } = outcome else {
            panic!("{label}: mid-stream cancel must report Cancelled")
        };
        assert_eq!(
            emitted,
            partial.len(),
            "{label}: emitted counts the partial"
        );
        assert!(!partial.is_empty(), "{label}: trigger items must arrive");
        assert!(partial.len() < n_usize, "{label}: must stop before EOF");
        for v in &partial {
            assert!(
                full.binary_search(v).is_ok(),
                "{label}: partial item {v} is not a full-run item"
            );
        }
    }
    // Guard the fixture itself: under a wedged engine the drains would hang
    // (the timeout above), under a silently-completing one this trips.
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "cancelled runs must drain promptly"
    );
}
