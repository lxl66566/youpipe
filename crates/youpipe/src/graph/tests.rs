//! Graph engine tests: topology validation, then end-to-end runs over
//! diamonds, fan-in/fan-out, async mixes, cancellation, panic propagation
//! and deadlock regressions.

use super::*;

/// Miri interprets every spawned thread and channel atomic; scale item
/// counts down but keep the same dispatch paths per item (same convention
/// as the integration suites).
fn n_items(native: u64) -> u64 {
    if cfg!(miri) {
        native / 20
    } else {
        native
    }
}

/// A deliberately non-`Clone` payload: flowing through the move
/// registrations is a compile-time property, so the tests below prove the
/// `Clone` bound is broadcast-only.
struct Opaque(u64);

/// Two nodes in a cycle never reach in-degree zero; the panic names them.
#[test]
#[should_panic(expected = "cycle detected")]
fn rejects_cycle() {
    let mut g = graph();
    let src = g.source("s", 0..8u32);
    let a = g.node("a", |x: u32| x + 1);
    let b = g.node("b", |x: u32| x + 2);
    g.edge(src, a).edge(a, b).edge(b, a);
    g.run();
}

/// A non-source node with no incoming edge has nothing to consume.
#[test]
#[should_panic(expected = "node `orphan` requires at least one incoming edge")]
fn rejects_orphan_node() {
    let mut g = graph();
    let _src = g.source("s", 0..8u32);
    let orphan = g.node("orphan", |x: u32| x);
    let snk = g.sink("out", |_: u32| {});
    g.edge(orphan, snk);
    g.run();
}

/// A non-sink node with no outgoing edge: its worker would exit after the
/// first claim batch (zero open output edges already read as "every edge
/// closed") and silently truncate the upstream run.
#[test]
#[should_panic(expected = "node `dead` requires at least one outgoing edge")]
fn rejects_dangling_node() {
    let mut g = graph();
    let src = g.source_broadcast("s", 0..8u32);
    let dead = g.node("dead", |x: u32| x);
    let snk = g.sink("out", |_: u32| {});
    g.edge(src, dead).edge(src, snk);
    g.run();
}

/// A move-registration with several outgoing edges would have to clone —
/// which the registration promised not to require — so validation rejects
/// it and names the `_broadcast` escape hatch.
#[test]
#[should_panic(expected = "move-only node `split` must not have 2 outgoing edges")]
fn move_node_rejects_fan_out() {
    let mut g = graph();
    let src = g.source("s", 0..8u32);
    let split = g.node("split", |x: u32| x);
    let a = g.sink("a", |_: u32| {});
    let b = g.sink("b", |_: u32| {});
    g.edge(src, split).edge(split, a).edge(split, b);
    g.run();
}

/// A source with an incoming edge is a topology contradiction. Built with
/// unit-typed items so the handle types interlock.
#[test]
#[should_panic(expected = "source `s2` must not have incoming edges")]
fn rejects_source_with_input() {
    let mut g = graph();
    let s1 = g.source("s1", std::iter::empty::<()>());
    let s2 = g.source("s2", std::iter::empty::<()>());
    g.edge(s1, s2);
    g.run();
}

/// A sink consumes; an outgoing edge from it is rejected.
#[test]
#[should_panic(expected = "sink `out` must not have outgoing edges")]
fn rejects_sink_with_output() {
    let mut g = graph();
    let src = g.source("s", std::iter::empty::<()>());
    let snk = g.sink("out", |()| {});
    g.edge(src, snk);
    g.edge(snk, src);
    g.run();
}

#[test]
#[should_panic(expected = "duplicate node name `dup`")]
fn rejects_duplicate_names() {
    let mut g = graph();
    let _a = g.source("dup", 0..8u32);
    let _b = g.node("dup", |x: u32| x);
    g.run();
}

#[test]
#[should_panic(expected = "duplicate edge `a->out`")]
fn rejects_duplicate_edge() {
    let mut g = graph();
    let src = g.source("s", 0..8u32);
    let a = g.node("a", |x: u32| x);
    let out = g.sink("out", |_: u32| {});
    g.edge(src, a).edge(a, out).edge(a, out);
    g.run();
}

/// Every violation is reported in one pass (not one per run).
#[test]
fn reports_all_violations_at_once() {
    let mut g = graph();
    let _a = g.source("x", 0..8u32);
    let _b = g.node("x", |v: u32| v);
    let orphan = g.node("orphan", |v: u32| v);
    g.edge(orphan, orphan);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.run();
    }))
    .expect_err("must panic");
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default();
    assert!(msg.contains("duplicate node name"), "got: {msg}");
    assert!(
        msg.contains("requires at least one incoming edge"),
        "got: {msg}"
    );
    assert!(msg.contains("cycle detected"), "got: {msg}");
}

// ── Sync end-to-end ──

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Linear chain through every sync node kind: conservation over the whole
/// path.
#[test]
fn linear_chain_conserves_items() {
    let n = n_items(10_000u64);
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let double = g.node("double", |x: u64| x * 2);
    let inc = g.node("inc", |x: u64| x + 1);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, double).edge(double, inc).edge(inc, out);
    g.run();
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).map(|x| x * 2 + 1).sum::<u64>()
    );
}

/// Diamond topology: one upstream broadcasting into two branches that fan
/// back into one sink. The sink must see both branches' items.
#[test]
fn diamond_broadcast_then_fan_in() {
    let n = n_items(5_000u64);
    let count = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let acc_n = Arc::clone(&count);
    let acc_sum = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let split = g.node_broadcast("split", |x: u64| x);
    let a = g.node("a", |x: u64| x * 10);
    let b = g.node("b", |x: u64| x + 1);
    let out = g.sink("out", move |x: u64| {
        acc_n.fetch_add(1, Ordering::Relaxed);
        acc_sum.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, split)
        .edge(split, a)
        .edge(split, b)
        .edge(a, out)
        .edge(b, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), 2 * n);
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).map(|x| x * 10).sum::<u64>() + (0..n).map(|x| x + 1).sum::<u64>()
    );
}

/// Multiple sources feeding one merge node: the fan-in sees every source's
/// items exactly once.
#[test]
fn multi_source_fan_in() {
    let n = n_items(4_000u64);
    let count = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let acc_n = Arc::clone(&count);
    let acc_sum = Arc::clone(&total);
    let mut g = graph();
    let s1 = g.source("s1", 0..n);
    let s2 = g.source("s2", (0..n).map(|x| x + 1_000_000));
    let merge = g.node("merge", |x: u64| x);
    let out = g.sink("out", move |x: u64| {
        acc_n.fetch_add(1, Ordering::Relaxed);
        acc_sum.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(s1, merge).edge(s2, merge).edge(merge, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), 2 * n);
    assert_eq!(
        total.load(Ordering::Relaxed),
        2 * (0..n).sum::<u64>() + n * 1_000_000
    );
}

/// Broadcast conservation: both sinks receive the identical stream.
#[test]
fn broadcast_conservation_two_sinks() {
    let n = n_items(3_000u64);
    let t1 = Arc::new(AtomicU64::new(0));
    let t2 = Arc::new(AtomicU64::new(0));
    let a1 = Arc::clone(&t1);
    let a2 = Arc::clone(&t2);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let m = g.node_broadcast("m", |x: u64| x + 7);
    let out1 = g.sink("out1", move |x: u64| {
        a1.fetch_add(x, Ordering::Relaxed);
    });
    let out2 = g.sink("out2", move |x: u64| {
        a2.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, m).edge(m, out1).edge(m, out2);
    g.run();
    let expected: u64 = (0..n).map(|x| x + 7).sum();
    assert_eq!(t1.load(Ordering::Relaxed), expected);
    assert_eq!(t2.load(Ordering::Relaxed), expected);
}

/// Expansion node: variable 1-to-N expansion (odd inputs vanish).
#[test]
fn expand_node_multiplies_and_filters() {
    let n = n_items(4_000u64);
    let count = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let acc_n = Arc::clone(&count);
    let acc_sum = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let expand = g.node_expand("expand", |x: u64, out: &mut Vec<u64>| {
        if x % 2 == 0 {
            out.push(x);
            out.push(x * 2);
        }
    });
    let out = g.sink("out", move |x: u64| {
        acc_n.fetch_add(1, Ordering::Relaxed);
        acc_sum.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, expand).edge(expand, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), n); // n/2 inputs x 2 outputs
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).filter(|x| x % 2 == 0).map(|x| x * 3).sum::<u64>()
    );
}

/// A non-`Clone` payload through every move registration (`source`, `node`,
/// `node_expand`, `sink_collect`): the graph compiles — the `Clone` bound is
/// broadcast-only — and conserves items end to end.
#[test]
fn non_clone_items_flow_through_move_registrations() {
    let n = n_items(4_000u64);
    let mut g = graph();
    let src = g.source("src", (0..n).map(Opaque));
    let tag = g.node("tag", |x: Opaque| Opaque(x.0.wrapping_add(1)));
    let expand = g.node_expand("expand", |x: Opaque, out: &mut Vec<Opaque>| {
        let doubled = x.0.wrapping_mul(2);
        out.push(x);
        out.push(Opaque(doubled));
    });
    let out = g.sink_collect("out");
    g.edge(src, tag).edge(tag, expand).edge(expand, out);
    let items = g.run_collect(out);
    let expect: u64 = (0..n).map(|x| 3 * (x + 1)).sum();
    assert_eq!(items.len(), 2 * usize::try_from(n).unwrap());
    assert_eq!(items.iter().map(|x| x.0).sum::<u64>(), expect);
}

/// Multi-worker nodes over a shared MPMC input with per-worker batch-claim
/// shares: conservation at 4 and 3 workers.
#[test]
fn multi_worker_nodes_conserve() {
    let n = n_items(20_000u64);
    let count = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let acc_n = Arc::clone(&count);
    let acc_sum = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let stage1 = g.node_with("s1", NodeOptions::new().workers(4), |x: u64| {
        x.wrapping_mul(3)
    });
    let stage2 = g.node_with("s2", NodeOptions::new().workers(3), |x: u64| x ^ 0xdead);
    let out = g.sink("out", move |x: u64| {
        acc_n.fetch_add(1, Ordering::Relaxed);
        acc_sum.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, stage1).edge(stage1, stage2).edge(stage2, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), n);
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).map(|x| (x * 3) ^ 0xdead).sum::<u64>()
    );
}

/// Narrow pool + pinned worker counts beyond the pool: the lease does not
/// fit, the run takes the dedicated-thread fallback and still completes.
/// Watchdog-protected (a deadlock hangs otherwise).
// miri: dedicated-fallback thread stress the interpreter cannot pace (same
// convention as the integration suites' narrow-pool tests).
#[cfg_attr(miri, ignore)]
#[test]
fn narrow_pool_dedicated_fallback_completes() {
    let n = n_items(20_000u64);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = {
        let pool = ComputePool::new(2);
        std::thread::spawn(move || {
            let pool_for_graph = pool.clone();
            let total = Arc::new(AtomicU64::new(0));
            let acc = Arc::clone(&total);
            let mut g = graph();
            let src = g.source("src", 0..n);
            let a = g.node_with("a", NodeOptions::new().workers(4), |x: u64| {
                x.wrapping_mul(7)
            });
            let b = g.node_with("b", NodeOptions::new().workers(4).buffer(2), |x: u64| {
                x.wrapping_add(11)
            });
            let out = g.sink("out", move |x: u64| {
                acc.fetch_add(x, Ordering::Relaxed);
            });
            let mut g = g.with_compute_pool(pool_for_graph).with_buffer_size(2);
            g.edge(src, a).edge(a, b).edge(b, out);
            g.run();
            tx.send(total.load(Ordering::Relaxed)).unwrap();
        })
    };
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("narrow-pool graph deadlocked");
    assert_eq!(got, (0..n).map(|x| x * 7 + 11).sum::<u64>());
    handle.join().unwrap();
}

/// Cooperative cancellation: the sink cancels mid-stream; `run` returns
/// normally with a partial (at least the trigger count, far below the guard
/// bound) consumption.
#[test]
fn cancel_returns_normally() {
    let token = CancellationToken::new();
    let sink_token = token.clone();
    let count = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&count);
    let mut g = graph();
    let src = g.source("src", (0..).take(10_000_000));
    let m = g.node("m", |x: u64| x + 1);
    let out = g.sink("out", move |_: u64| {
        let n = acc.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 2_000 {
            sink_token.cancel();
        }
    });
    g.edge(src, m).edge(m, out);
    g.with_cancel(token).run();
    let seen = count.load(Ordering::Relaxed);
    assert!(seen >= 2_000, "sink must see the trigger items: {seen}");
    assert!(seen < 10_000_000, "cancel must stop the stream: {seen}");
}

/// `run_checked`: the same mid-stream cancellation observed through the
/// outcome instead of a post-hoc token read. Side-effect sinks keep no
/// count, so `emitted` stays 0 by contract (collect sinks report it — see
/// `run_collect_checked_reports_cancelled_partial`).
#[test]
fn run_checked_reports_cancelled() {
    let token = CancellationToken::new();
    let sink_token = token.clone();
    let count = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&count);
    let mut g = graph();
    let src = g.source("src", (0..).take(10_000_000));
    let out = g.sink("out", move |_: u64| {
        let n = acc.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 2_000 {
            sink_token.cancel();
        }
    });
    g.edge(src, out);
    let outcome = g.with_cancel(token).run_checked();
    assert_eq!(outcome, RunOutcome::Cancelled {
        partial: (),
        emitted: 0
    });
    let seen = count.load(Ordering::Relaxed);
    assert!(
        (2_000..10_000_000).contains(&seen),
        "sink must see the trigger items then stop: {seen}"
    );
}

/// `run_checked` on an uncalled graph: `Completed`.
#[test]
fn run_checked_completes() {
    let mut g = graph();
    let src = g.source("src", 0..n_items(10_000));
    let out = g.sink("out", |_: u64| {});
    g.edge(src, out);
    assert_eq!(g.run_checked(), RunOutcome::Completed(()));
}

// ── Collect sinks ──

/// `sink_collect` + `run_collect` over a linear chain: the returned Vec
/// carries every item (order follows delivery — compare as a multiset).
#[test]
fn collect_sink_returns_all_items() {
    let n = n_items(8_000u64);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let double = g.node("double", |x: u64| x * 2);
    let out = g.sink_collect("out");
    g.edge(src, double).edge(double, out);
    let mut items = g.run_collect(out);
    items.sort_unstable();
    assert_eq!(items, (0..n).map(|x| x * 2).collect::<Vec<_>>());
}

/// A collect sink at a fan-in point: every branch's items land in the Vec.
#[test]
fn collect_sink_accumulates_fan_in() {
    let n = n_items(4_000u64);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let split = g.node_broadcast("split", |x: u64| x);
    let a = g.node("a", |x: u64| x * 10);
    let b = g.node("b", |x: u64| x + 1);
    let out = g.sink_collect("out");
    g.edge(src, split)
        .edge(split, a)
        .edge(split, b)
        .edge(a, out)
        .edge(b, out);
    let items = g.run_collect(out);
    assert_eq!(items.len(), 2 * usize::try_from(n).unwrap());
    assert_eq!(
        items.iter().sum::<u64>(),
        (0..n).map(|x| x * 10).sum::<u64>() + (0..n).map(|x| x + 1).sum::<u64>()
    );
}

/// `run_collect_checked` on a mid-stream cancellation: the partial Vec comes
/// back and `emitted` counts it (the side-effect `run_checked` cannot).
#[test]
fn run_collect_checked_reports_cancelled_partial() {
    let token = CancellationToken::new();
    let fire = token.clone();
    let n = 10_000_000u64;
    let mut g = graph();
    let src = g.source("src", 0..n);
    let gate = g.node("gate", move |x: u64| {
        if x == 2_000 {
            fire.cancel();
        }
        x
    });
    let out = g.sink_collect("out");
    g.edge(src, gate).edge(gate, out);
    let outcome = g.with_cancel(token).run_collect_checked(out);
    let RunOutcome::Cancelled { partial, emitted } = outcome else {
        panic!("mid-stream cancel must report Cancelled: {outcome:?}")
    };
    assert_eq!(emitted, partial.len());
    assert!(!partial.is_empty(), "trigger items must reach the sink");
    assert!(usize::try_from(n).unwrap() > partial.len());
}

/// Plain `run` on a graph with a collect sink panics instead of dropping
/// the collected items silently.
#[test]
#[should_panic(expected = "collect sink(s) `out` dropped their items")]
fn plain_run_rejects_collect_sink() {
    let mut g = graph();
    let src = g.source("src", 0..8u32);
    let out = g.sink_collect("out");
    g.edge(src, out);
    g.run();
}

/// A second collect sink whose handle was not passed to `run_collect`
/// panics (its items would otherwise drop silently).
#[test]
#[should_panic(expected = "collect sink(s) `kept` dropped their items")]
fn uncollected_collect_sink_panics() {
    let mut g = graph();
    let src = g.source_broadcast("src", 0..8u32);
    let dropped = g.sink_collect("taken");
    let kept = g.sink_collect("kept");
    g.edge(src, dropped).edge(src, kept);
    g.run_collect(dropped);
}

/// A panicking node re-raises on the run caller after the drain (first panic
/// wins; the process must not abort).
#[test]
fn node_panic_propagates_to_run_caller() {
    let n = n_items(10_000u64);
    // Relative to `n` (miri scales it down): the boom point must sit inside
    // the stream or the panic never fires.
    let boom_at = n / 20;
    let seen = Arc::new(AtomicU64::new(0));
    let seen_acc = Arc::clone(&seen);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut g = graph();
        let src = g.source("src", 0..n);
        let boom = g.node("boom", move |x: u64| {
            assert!(x < boom_at, "boom at {x}");
            x
        });
        let out = g.sink("out", move |_: u64| {
            seen_acc.fetch_add(1, Ordering::Relaxed);
        });
        g.edge(src, boom).edge(boom, out);
        g.run();
    }));
    let payload = result.expect_err("panic must propagate out of run()");
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default();
    assert!(msg.contains("boom at"), "unexpected panic payload: {msg}");
}

/// Nested run: a graph `run()` inside a ComputePool worker of the same pool
/// takes the dedicated-thread fallback and completes; a graph node closure
/// running an inner graph composes the same way.
// miri: nested dedicated-fallback thread stress the interpreter cannot pace.
#[cfg_attr(miri, ignore)]
#[test]
fn nested_run_inside_pool_worker_and_inside_node() {
    let pool = ComputePool::new(4);
    let inner_pool = pool.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let outer_pool = pool.clone();
    pool.submit(move || {
        // Graph-inside-graph: the outer node closure runs an inner graph.
        let inner_total = Arc::new(AtomicU64::new(0));
        let acc = Arc::clone(&inner_total);
        let mut g = graph();
        let src = g.source("isrc", 0..2_000u64);
        let m = g.node("im", |x: u64| x + 1);
        let out = g.sink("iout", move |x: u64| {
            acc.fetch_add(x, Ordering::Relaxed);
        });
        g.edge(src, m).edge(m, out);
        let g = g.with_compute_pool(inner_pool);
        g.run();
        tx.send(inner_total.load(Ordering::Relaxed)).unwrap();

        // Plain nested run on the pool worker itself.
        let total = Arc::new(AtomicU64::new(0));
        let acc2 = Arc::clone(&total);
        let mut g2 = graph();
        let src2 = g2.source("nsrc", 0..3_000u64);
        let m2 = g2.node("nm", |x: u64| x * 2);
        let out2 = g2.sink("nout", move |x: u64| {
            acc2.fetch_add(x, Ordering::Relaxed);
        });
        g2.edge(src2, m2).edge(m2, out2);
        let g2 = g2.with_compute_pool(outer_pool);
        g2.run();
        tx.send(total.load(Ordering::Relaxed)).unwrap();
    });
    let inner = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("graph-inside-graph deadlocked");
    assert_eq!(inner, (1..=2_000u64).sum::<u64>());
    let nested = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("nested run inside pool worker deadlocked");
    assert_eq!(nested, (0..3_000u64).map(|x| x * 2).sum::<u64>());
}

// ── Async / mixed end-to-end ──

/// Mixed CPU/IO chain (the canonical graph shape): sync source -> sync CPU
/// node -> async IO node -> sync node -> sink, with a yielded await on the
/// IO leg.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_mixed_chain_conserves() {
    let n = n_items(3_000u64);
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("load", 0..n);
    let hash = g.node("hash", |x: u64| x.wrapping_mul(2_654_435_761));
    let io = g.node_async_with(
        "persist",
        AsyncNodeOptions::new().io_concurrency(16),
        |x: u64| async move {
            tokio::task::yield_now().await;
            x
        },
    );
    let agg = g.node("agg", |x: u64| x / 2);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, hash)
        .edge(hash, io)
        .edge(io, agg)
        .edge(agg, out);
    g.run();
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n)
            .map(|x| x.wrapping_mul(2_654_435_761) / 2)
            .sum::<u64>()
    );
}

/// The async move path drops the `Clone` bound too: a non-`Clone` payload
/// through `source` -> `node_async` -> `sink_collect` compiles and conserves.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn non_clone_items_flow_through_async_move_node() {
    let n = n_items(1_500u64);
    let mut g = graph();
    let src = g.source("src", (0..n).map(Opaque));
    let io = g.node_async_with(
        "io",
        AsyncNodeOptions::new().io_concurrency(4),
        |x: Opaque| async move {
            tokio::task::yield_now().await;
            Opaque(x.0.wrapping_add(5))
        },
    );
    let out = g.sink_collect("out");
    g.edge(src, io).edge(io, out);
    let items = g.run_collect(out);
    assert_eq!(items.len(), usize::try_from(n).unwrap());
    assert_eq!(
        items.iter().map(|x| x.0).sum::<u64>(),
        (0..n).map(|x| x + 5).sum::<u64>()
    );
}

/// Pull-based async source: `f` drives the stream until `None`; conservation
/// at the sink.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_source_streams_until_none() {
    let n = n_items(2_000u64);
    let produced = Arc::new(AtomicU64::new(0));
    let probe = Arc::clone(&produced);
    let count = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&count);
    let mut g = graph();
    let src = g.source_async("gen", move || {
        let next = probe.fetch_add(1, Ordering::Relaxed);
        async move { (next < n).then_some(next) }
    });
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(1, Ordering::Relaxed);
        assert!(x < n);
    });
    g.edge(src, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), n);
    assert_eq!(produced.load(Ordering::Relaxed), n + 1); // one None poll past the end
}

/// Async upstream into a SYNC downstream: the dedicated bridge thread
/// forwards the async channel into the sync one.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_to_sync_bridge_feeds_sync_node() {
    let n = n_items(2_500u64);
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let io = g.node_async_with(
        "io",
        AsyncNodeOptions::new().io_concurrency(8),
        |x: u64| async move {
            tokio::task::yield_now().await;
            x + 1
        },
    );
    let sync_stage = g.node("after", |x: u64| x * 2);
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, io).edge(io, sync_stage).edge(sync_stage, out);
    g.run();
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).map(|x| (x + 1) * 2).sum::<u64>()
    );
}

/// Mixed diamond: one sync split feeding an async branch and a sync branch,
/// both fanning into one sink (a sync->async mixed channel plus a plain
/// sync edge).
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn mixed_diamond_async_and_sync_branches() {
    let n = n_items(2_000u64);
    let count = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let acc_n = Arc::clone(&count);
    let acc_sum = Arc::clone(&total);
    let mut g = graph();
    let src = g.source("src", 0..n);
    let split = g.node_broadcast("split", |x: u64| x);
    let io = g.node_async_with(
        "io",
        AsyncNodeOptions::new().io_concurrency(8),
        |x: u64| async move {
            tokio::task::yield_now().await;
            x * 10
        },
    );
    let cpu = g.node("cpu", |x: u64| x + 1);
    let out = g.sink("out", move |x: u64| {
        acc_n.fetch_add(1, Ordering::Relaxed);
        acc_sum.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, split)
        .edge(split, io)
        .edge(split, cpu)
        .edge(io, out)
        .edge(cpu, out);
    g.run();
    assert_eq!(count.load(Ordering::Relaxed), 2 * n);
    assert_eq!(
        total.load(Ordering::Relaxed),
        (0..n).map(|x| x * 10).sum::<u64>() + (0..n).map(|x| x + 1).sum::<u64>()
    );
}

/// A panicking async node re-raises on the run caller (the runtime would
/// otherwise discard the payload with the unobserved JoinHandle).
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_node_panic_propagates_to_run_caller() {
    let n = n_items(5_000u64);
    // Relative trigger: miri's n_items scaling shrinks n below any fixed
    // threshold, so the panic would never fire and the test would pass vacuously.
    let boom_at = n / 10;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut g = graph();
        let src = g.source("src", 0..n);
        let boom = g.node_async_with(
            "boom",
            AsyncNodeOptions::new().io_concurrency(4),
            move |x: u64| async move {
                assert!(x < boom_at, "async boom at {x}");
                x
            },
        );
        let out = g.sink("out", |_: u64| {});
        g.edge(src, boom).edge(boom, out);
        g.run();
    }));
    let payload = result.expect_err("async panic must propagate out of run()");
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default();
    assert!(msg.contains("async boom at"), "unexpected payload: {msg}");
}

/// Cooperative cancellation of an async stream: the pull source stops at the
/// token; `run` returns normally.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn async_cancel_returns_normally() {
    let token = CancellationToken::new();
    let sink_token = token.clone();
    let count = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&count);
    let mut g = graph();
    let src = g.source_async("gen", || async move { Some(0u64) }); // bounded by the token
    let out = g.sink("out", move |_: u64| {
        let n = acc.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1_000 {
            sink_token.cancel();
        }
    });
    g.edge(src, out);
    g.with_cancel(token).run();
    let seen = count.load(Ordering::Relaxed);
    assert!(seen >= 1_000, "sink must see the trigger items: {seen}");
    assert!(seen < 100_000, "cancel must stop the async source: {seen}");
}

/// Attached runtime backends: `with_async_pool` reuses one instance, and
/// `with_runtime` re-types the builder to the same backend before nodes are
/// registered.
#[cfg(feature = "tokio-runtime")]
// miri (windows): building the tokio runtime registers an IOCP handle,
// which the interpreter cannot execute; Linux miri (epoll) stays covered.
#[cfg_attr(all(miri, target_os = "windows"), ignore)]
#[test]
fn with_async_pool_and_with_runtime_backends() {
    use crate::TokioPool;

    let shared = TokioPool::build(2).expect("test runtime");
    let total = Arc::new(AtomicU64::new(0));
    let acc = Arc::clone(&total);
    let mut g = graph()
        .with_runtime::<TokioPool>()
        .with_async_pool(shared.clone());
    let src = g.source("src", 0..500u64);
    let io = g.node_async_with(
        "io",
        AsyncNodeOptions::new().io_concurrency(4),
        |x: u64| async move {
            tokio::task::yield_now().await;
            x + 1
        },
    );
    let out = g.sink("out", move |x: u64| {
        acc.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, io).edge(io, out);
    g.run();
    assert_eq!(total.load(Ordering::Relaxed), (1..=500u64).sum::<u64>());

    // A second run on the same attached backend (no transient rebuild).
    let total2 = Arc::new(AtomicU64::new(0));
    let acc2 = Arc::clone(&total2);
    let mut g = graph().with_async_pool(shared);
    let src = g.source("src2", 0..500u64);
    let io = g.node_async_with(
        "io2",
        AsyncNodeOptions::new().io_concurrency(4),
        |x: u64| async move { x * 2 },
    );
    let out = g.sink("out2", move |x: u64| {
        acc2.fetch_add(x, Ordering::Relaxed);
    });
    g.edge(src, io).edge(io, out);
    g.run();
    assert_eq!(
        total2.load(Ordering::Relaxed),
        (0..500u64).map(|x| x * 2).sum::<u64>()
    );
}
