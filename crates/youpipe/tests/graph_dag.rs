//! Integration tests for the `graph()` DAG engine.
//!
//! Every test builds a small graph, runs it, and asserts **conservation**
//! (item counts and sums through fan-out/fan-in/expand) — the properties that
//! must hold under any worker count, buffer size, or edge scheduling. Liveness
//! risks (narrow pools, async bridges) are wrapped in `run_with_timeout` so a
//! deadlock fails in bounded time instead of hanging the suite.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use youpipe::{AsyncNodeOptions, CancellationToken, ComputePool, NodeHandle, NodeOptions, graph};

/// Miri interprets every spawned thread; scale item counts down but keep the
/// same dispatch paths per item (same convention as `pipeline_integration.rs`).
fn n_items(native: u64) -> u64 {
    if cfg!(miri) {
        native / 20
    } else {
        native
    }
}

/// Run `f` on a worker thread and panic with `label` if it does not finish
/// within 30 s — bounded failure for deadlock-class regressions. On timeout
/// the worker thread leaks; acceptable because the test has already failed.
fn run_with_timeout<T: Send + 'static>(label: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        // A panicking worker still delivers via the dropped sender (Err).
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{label}: graph run did not finish within 30s (deadlock?)"))
}

/// Silence the default panic hook around a closure that is expected to panic,
/// assert the message contains every `expected` fragment, and restore the hook.
fn catch_panic_asserting<F: FnOnce()>(f: F, expected: &[&str]) {
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .expect_err("closure was expected to panic but returned");
    std::panic::set_hook(prev_hook);
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| panic!("panic payload was not a string: {payload:?}"));
    for frag in expected {
        assert!(msg.contains(frag), "panic message {msg:?} lacks {frag:?}");
    }
}

/// A side-effect sink collecting every received item into a shared Vec.
fn collector_sink<T: Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl Fn(T)) {
    let items = Arc::new(Mutex::new(Vec::new()));
    let cell = items.clone();
    (items, move |x: T| cell.lock().unwrap().push(x))
}

// ── Linear correctness ──

#[test]
fn linear_graph_item_conservation() {
    let n = n_items(10_000);
    let mut g = graph();
    let src: NodeHandle<(), u64> = g.source("load", 0..n);
    let a = g.node(
        "triple",
        NodeOptions::default(),
        |x: u64| x.wrapping_mul(3).wrapping_add(1),
    );
    let b = g.node(
        "mix",
        NodeOptions::default(),
        |x: u64| x.wrapping_mul(31).wrapping_add(7),
    );
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, a).edge(a, b).edge(b, term);
    g.run();

    let items = seen.lock().unwrap();
    assert_eq!(items.len(), usize::try_from(n).unwrap(), "one output per input");
    let transform =
        |x: u64| x.wrapping_mul(3).wrapping_add(1).wrapping_mul(31).wrapping_add(7);
    let mut got = items.clone();
    got.sort_unstable();
    let mut expect: Vec<u64> = (0..n).map(transform).collect();
    expect.sort_unstable();
    assert_eq!(got, expect);
}

#[test]
fn empty_source_produces_nothing() {
    let mut g = graph();
    let src = g.source("load", 0..0u64);
    let n = g.node("proc", NodeOptions::default(), |x: u64| x + 1);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, n).edge(n, term);
    g.run();
    assert!(seen.lock().unwrap().is_empty(), "empty source must reach the sink 0 times");
}

#[test]
fn empty_graph_runs() {
    let g = graph();
    g.try_run().expect("a graph with no nodes must run trivially");
}

// ── Fan-out / fan-in ──

/// Diamond: src → (double, plus) → join → sink. Broadcast conservation: each
/// branch observes all `n` inputs exactly once, so the join receives `2n`
/// items whose multiset equals both branch outputs combined. A tiny buffer
/// forces backpressure through the broadcast clone path.
#[test]
fn diamond_broadcast_conservation() {
    let n = n_items(10_000);
    let (a_count, a_sum) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicU64::new(0)));
    let (b_count, b_sum) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicU64::new(0)));

    let mut g = graph();
    let src = g.source("load", 0..n);
    let a = g.node("double", NodeOptions::default(), {
        let (count, sum) = (a_count.clone(), a_sum.clone());
        move |x: u64| {
            count.fetch_add(1, Ordering::Relaxed);
            let y = x.wrapping_mul(2);
            sum.fetch_add(y, Ordering::Relaxed);
            y
        }
    });
    let b = g.node("plus", NodeOptions::default(), {
        let (count, sum) = (b_count.clone(), b_sum.clone());
        move |x: u64| {
            count.fetch_add(1, Ordering::Relaxed);
            let y = x.wrapping_add(1000);
            sum.fetch_add(y, Ordering::Relaxed);
            y
        }
    });
    let join = g.node("join", NodeOptions::default(), |x: u64| x);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, a);
    g.edge(src, b);
    g.edge(a, join).edge(b, join).edge(join, term);
    g = g.with_buffer_size(4);
    g.run();

    let n_usize = usize::try_from(n).unwrap();
    assert_eq!(a_count.load(Ordering::Relaxed), n_usize, "branch a sees every item");
    assert_eq!(b_count.load(Ordering::Relaxed), n_usize, "branch b sees every item");

    let items = seen.lock().unwrap();
    assert_eq!(items.len(), 2 * n_usize, "join receives both full branches");
    assert_eq!(
        items.iter().copied().sum::<u64>(),
        a_sum.load(Ordering::Relaxed) + b_sum.load(Ordering::Relaxed),
        "sink sum equals both branch sums"
    );
}

/// Fan-in of two disjoint sources into one node → one sink: the union of all
/// inputs, exactly once each.
#[test]
fn multi_source_fan_in_single_sink() {
    let n1 = n_items(700);
    let n2 = n_items(300);
    let mut g = graph();
    let left = g.source("left", 0..n1);
    let right = g.source("right", n1..n1 + n2);
    let m = g.node("shift", NodeOptions::default(), |x: u64| x + 1);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(left, m);
    g.edge(right, m);
    g.edge(m, term);
    g.run();

    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    let mut expect: Vec<u64> = (0..n1 + n2).map(|x| x + 1).collect();
    expect.sort_unstable();
    assert_eq!(got, expect);
}

/// Broadcast to two sinks: each sink receives the full stream independently.
#[test]
fn multi_sink_broadcast() {
    let n = n_items(5_000);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let m = g.node("scale", NodeOptions::default(), |x: u64| x.wrapping_mul(3));
    let (seen_a, sink_a) = collector_sink::<u64>();
    let (seen_b, sink_b) = collector_sink::<u64>();
    let sa = g.sink("out-a", sink_a);
    let sb = g.sink("out-b", sink_b);
    g.edge(src, m).edge(m, sa);
    g.edge(m, sb);
    g.run();

    let n_usize = usize::try_from(n).unwrap();
    let expect_sum: u64 = (0..n).map(|x| x.wrapping_mul(3)).sum();
    for (label, seen) in [("out-a", seen_a), ("out-b", seen_b)] {
        let items = seen.lock().unwrap();
        assert_eq!(items.len(), n_usize, "sink {label} receives every item");
        assert_eq!(items.iter().copied().sum::<u64>(), expect_sum, "sink {label} sum");
    }
}

// ── Expansion ──

#[test]
fn node_expand_count_conservation() {
    let n = n_items(5_000);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let exp = g.node_expand(
        "split",
        NodeOptions::default(),
        |x: u64, out: &mut Vec<u64>| {
            out.push(x);
            out.push(x.wrapping_add(1));
        },
    );
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, exp).edge(exp, term);
    g.run();

    let items = seen.lock().unwrap();
    assert_eq!(
        items.len(),
        2 * usize::try_from(n).unwrap(),
        "expand emits exactly two outputs per input"
    );
    // turbofish: with the serde feature on, u64: PartialEq<serde_json::Value>
    // makes the bare sum() ambiguous in assert_eq!
    assert_eq!(
        items.iter().copied().sum::<u64>(),
        (0..n).map(|x| 2 * x + 1).sum::<u64>()
    );
}

// ── Async nodes (tokio-runtime) ──

/// Async diamond: one sync branch, one async branch (sleep = IO wait), fan-in
/// back to a sync join. Broadcast conservation must hold across the bridge.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_diamond_broadcast() {
    let n = n_items(200);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let a = g.node("double", NodeOptions::default(), |x: u64| x.wrapping_mul(2));
    let b = g.node_async("fetch", AsyncNodeOptions::default(), |x: u64| async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        x.wrapping_add(1000)
    });
    let join = g.node("join", NodeOptions::default(), |x: u64| x);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, a);
    g.edge(src, b);
    g.edge(a, join).edge(b, join).edge(join, term);
    g = g.with_async_pool(youpipe::TokioPool::build(2).expect("build runtime"));
    run_with_timeout("async diamond", move || g.run());

    let items = seen.lock().unwrap();
    let n_usize = usize::try_from(n).unwrap();
    assert_eq!(items.len(), 2 * n_usize, "join receives both full branches");
    assert_eq!(
        items.iter().copied().sum::<u64>(),
        (0..n).map(|x| x.wrapping_mul(2) + x + 1000).sum::<u64>(),
        "both branch outputs account for every item"
    );
}

/// sync → async → sync: both bridges (compute-pool worker → runtime task →
/// compute-pool worker) in one graph, values transformed at each hop.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn sync_async_sync_bridge_chain() {
    let n = n_items(1_000);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let s1 = g.node("bump", NodeOptions::default(), |x: u64| x + 1);
    let io = g.node_async("persist", AsyncNodeOptions::default(), |x: u64| async move {
        tokio::time::sleep(Duration::from_micros(50)).await;
        x.wrapping_mul(10)
    });
    let s2 = g.node("tag", NodeOptions::default(), |x: u64| x.wrapping_add(7));
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, s1).edge(s1, io).edge(io, s2).edge(s2, term);
    g = g.with_async_pool(youpipe::TokioPool::build(2).expect("build runtime"));
    run_with_timeout("sync->async->sync bridges", move || g.run());

    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    let mut expect: Vec<u64> = (0..n).map(|x| (x + 1).wrapping_mul(10) + 7).collect();
    expect.sort_unstable();
    assert_eq!(got, expect);
}

/// Pull-based async source: each poll returns a future yielding `Some(item)`
/// until the stream ends with `None` (EOF) — downstream sees exactly the
/// emitted prefix, nothing more.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn source_async_eof_stops() {
    let n = n_items(500);
    let pulled = Arc::new(AtomicUsize::new(0));
    let counter = pulled.clone();

    let mut g = graph();
    let src = g.source_async("pull", move || {
        let counter = counter.clone();
        async move {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            if i < usize::try_from(n).unwrap() {
                Some(i as u64)
            } else {
                None
            }
        }
    });
    let m = g.node("shift", NodeOptions::default(), |x: u64| x + 1);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, m).edge(m, term);
    g = g.with_async_pool(youpipe::TokioPool::build(2).expect("build runtime"));
    run_with_timeout("async source EOF", move || g.run());

    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    let mut expect: Vec<u64> = (0..n).map(|x| x + 1).collect();
    expect.sort_unstable();
    assert_eq!(got, expect, "exactly the items before EOF, each once");
}

// ── Cancellation ──

/// A token cancelled mid-run truncates the stream: workers stop accepting new
/// items, the run drains in-flight work and returns well before `n`.
#[test]
#[cfg_attr(miri, ignore)] // wall-clock cancellation timing
fn cancel_mid_run_truncates() {
    let n: u64 = 20_000;
    let token = CancellationToken::new();
    let mut g = graph();
    let src = g.source("load", 0..n);
    let slow = g.node("slow", NodeOptions::default(), |x: u64| {
        thread::sleep(Duration::from_micros(50));
        x
    });
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, slow).edge(slow, term);
    g = g.with_cancel(token.clone());
    {
        let token = token.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            token.cancel();
        });
    }

    let start = Instant::now();
    g.run();
    let elapsed = start.elapsed();

    let got = seen.lock().unwrap().len();
    assert!(
        u64::try_from(got).unwrap() < n,
        "cancelled run stopped early: {got}/{n}"
    );
    // Full serial work is n × 50 µs = 1 s; the cancel lands at 10 ms, so even
    // a fully parallel drain must finish far below 1 s.
    assert!(
        elapsed < Duration::from_secs(1),
        "cancelled run must drain quickly, took {elapsed:?}"
    );
}

/// Pre-fired token: the run returns immediately with at most the in-flight
/// items — no sleeps needed, so this stays miri-friendly.
#[test]
fn prefired_cancel_returns_quickly() {
    let n = n_items(1_000);
    let token = CancellationToken::new();
    token.cancel();
    let mut g = graph();
    let src = g.source("load", 0..n);
    let m = g.node("proc", NodeOptions::default(), |x: u64| x + 1);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, m).edge(m, term);
    g = g.with_cancel(token);
    g.run();
    assert!(
        seen.lock().unwrap().len() < usize::try_from(n).unwrap(),
        "a cancelled-before-start run must not process the whole input"
    );
}

// ── Panic propagation ──

/// A node panic must be re-raised in the `run()` caller's thread with the
/// original payload (not swallowed with the worker).
#[test]
fn node_panic_propagates_to_caller() {
    let n = n_items(5_000);
    let boom_at = n / 2;
    catch_panic_asserting(
        move || {
            let mut g = graph();
            let src = g.source("load", 0..n);
            let bad = g.node("boom", NodeOptions::default(), move |x: u64| {
                assert!(x != boom_at, "graph node boom at {x}");
                x
            });
            let (seen, sink) = collector_sink::<u64>();
            let term = g.sink("out", sink);
            g.edge(src, bad).edge(bad, term);
            g.run();
            drop(seen);
        },
        &["graph node boom"],
    );
}

// ── Validation (run() rejects malformed graphs, naming the node) ──

#[test]
fn validation_rejects_cycle() {
    catch_panic_asserting(
        || {
            let mut g = graph();
            let src = g.source("load", 0..10u64);
            let a = g.node("hop-a", NodeOptions::default(), |x: u64| x);
            let b = g.node("hop-b", NodeOptions::default(), |x: u64| x);
            let term = g.sink("out", |_x: u64| {});
            g.edge(src, a);
            g.edge(a, b).edge(b, a); // cycle a → b → a
            g.edge(b, term);
            g.run();
        },
        &["hop-a"],
    );
}

#[test]
fn validation_rejects_missing_inbound() {
    catch_panic_asserting(
        || {
            let mut g = graph();
            let src = g.source("load", 0..10u64);
            let m = g.node("proc", NodeOptions::default(), |x: u64| x + 1);
            let term = g.sink("out", |_x: u64| {});
            g.edge(src, m).edge(m, term);
            // `orphan` is a non-source with no inbound edge (its handle is
            // unused by design).
            g.node("orphan", NodeOptions::default(), |x: u64| x + 2);
            g.run();
        },
        &["orphan"],
    );
}

#[test]
fn validation_rejects_sink_with_outbound() {
    catch_panic_asserting(
        || {
            let mut g = graph();
            let src = g.source("load", 0..10u64);
            let m = g.node("work", NodeOptions::default(), |x: u64| x + 1);
            let term = g.sink("term", |_x: u64| {});
            // The sink's outbound edge type-checks only against a `()`-input
            // node (a sink's Out is `()`), so the rejection below is the
            // runtime validation, not the type system.
            let tail = g.node("tail", NodeOptions::default(), |(): ()| 0u64);
            g.edge(src, m).edge(m, term);
            g.edge(term, tail); // sinks must be leaves
            g.run();
        },
        &["term"],
    );
}

#[test]
fn validation_rejects_duplicate_name() {
    catch_panic_asserting(
        || {
            let mut g = graph();
            let src = g.source("load", 0..10u64);
            let a = g.node("dup", NodeOptions::default(), |x: u64| x + 1);
            let b = g.node("dup", NodeOptions::default(), |x: u64| x + 2);
            let term = g.sink("out", |_x: u64| {});
            g.edge(src, a).edge(a, b).edge(b, term);
            g.run();
        },
        &["dup"],
    );
}

#[test]
fn validation_rejects_duplicate_edge() {
    catch_panic_asserting(
        || {
            let mut g = graph();
            let src = g.source("dedup-src", 0..10u64);
            let m = g.node("dedup-node", NodeOptions::default(), |x: u64| x + 1);
            let term = g.sink("out", |_x: u64| {});
            g.edge(src, m);
            g.edge(src, m); // same edge twice
            g.edge(m, term);
            g.run();
        },
        &["dedup-src"],
    );
}

// ── Scheduling: narrow pools and custom pools ──

/// A 6-node graph (source, fan-out, expand, fan-in, sink) on a 2-worker
/// compute pool: fewer pool workers than nodes must not deadlock — the
/// dedicated-thread fallback keeps blocked feeders off the pool.
#[test]
#[cfg_attr(miri, ignore)] // liveness under a narrow pool: miri interprets every fallback thread
fn narrow_compute_pool_no_deadlock() {
    let n: u64 = 5_000;
    let mut g = graph();
    let src = g.source("load", 0..n);
    let scale = g.node("scale", NodeOptions::default(), |x: u64| x.wrapping_mul(3));
    let shift = g.node("shift", NodeOptions::default(), |x: u64| x.wrapping_add(1));
    let split = g.node_expand("split", NodeOptions::default(), |x: u64, out: &mut Vec<u64>| {
        out.push(x);
        out.push(x.wrapping_add(1));
    });
    let join = g.node("join", NodeOptions::default(), |x: u64| x);
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, scale);
    g.edge(src, shift);
    g.edge(scale, split);
    g.edge(split, join).edge(shift, join).edge(join, term);
    g = g.with_compute_workers(2);
    run_with_timeout("narrow 2-worker pool, 6-node graph", move || g.run());

    // Conservation through the split path (2 outputs per scaled input) plus
    // the direct branch: 3n items, sum = Σ(3x) + Σ(3x + 1) + Σ(x + 1).
    let items = seen.lock().unwrap();
    assert_eq!(items.len(), 3 * usize::try_from(n).unwrap());
    let expect_sum: u64 = (0..n).map(|x| 7 * x + 2).sum();
    assert_eq!(items.iter().copied().sum::<u64>(), expect_sum);
}

/// An externally owned pool drives the graph's sync nodes.
#[test]
fn custom_compute_pool_runs() {
    let n = n_items(3_000);
    let pool = ComputePool::new(2);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let m = g.node("mix", NodeOptions::default(), |x: u64| x.wrapping_mul(5).wrapping_add(3));
    let (seen, sink) = collector_sink::<u64>();
    let term = g.sink("out", sink);
    g.edge(src, m).edge(m, term);
    g = g.with_compute_pool(pool);
    g.run();

    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    let mut expect: Vec<u64> = (0..n).map(|x| x.wrapping_mul(5).wrapping_add(3)).collect();
    expect.sort_unstable();
    assert_eq!(got, expect);
}
