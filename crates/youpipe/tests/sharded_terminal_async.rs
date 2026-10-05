//! End-to-end semantics of the async flavour of `YOUPIPE_SHARDED_TERM=1`:
//! the async terminal's consumer tasks fan out into one async shard ring per
//! task group instead of one shared MPSC async ring, and the collector
//! aggregates the shards (todo #1 residual (c)).
//!
//! Own test binary (not a block inside `sharded_terminal.rs`): the knob is
//! read through a process-wide `OnceLock`, so the env var must be set before
//! the first `run()` in this process — one `#[test]` fn with sequential
//! scenario blocks, same discipline as the sync suite.
#![cfg(feature = "tokio-runtime")]

use std::sync::atomic::{AtomicUsize, Ordering};

use youpipe::{AsyncStageOptions, PipelineConfig, stream, sync::CancellationToken};

/// Miri runs ~100x slower than native: shrink the item counts (the topology
/// under test — shard count, EOF aggregation, ordering — is size-independent).
fn n_main() -> usize {
    if cfg!(miri) {
        500
    } else {
        20_000
    }
}

fn n_aux() -> usize {
    if cfg!(miri) {
        100
    } else {
        5_000
    }
}

/// An un-fired token: disables the fused pass-through for the sync prefix,
/// keeps cancellation inert.
fn inert_cancel() -> CancellationToken {
    CancellationToken::new()
}

#[test]
fn sharded_async_terminal_semantics() {
    // SAFETY: this fn is the only test in this binary and no worker threads
    // exist yet — the first `run()` (which initializes the knob's OnceLock)
    // happens after this line.
    unsafe { std::env::set_var("YOUPIPE_SHARDED_TERM", "1") };

    // ── Unordered mixed chain (sync feeder path → `spawn_single`): the
    //    terminal shards must deliver the exact multiset ──
    let items: Vec<u64> = (0..n_main() as u64).collect();
    let mut got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x.wrapping_mul(3).wrapping_add(1) })
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = items.iter().map(|x| (x + 1) * 3 + 1).collect();
    assert_eq!(got, expected);

    // ── Ordered mixed chain: exact input order across shards (the
    //    ReorderBuffer never depended on arrival order) ──
    let got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x.wrapping_mul(3).wrapping_add(1) })
        .ordered()
        .run();
    assert_eq!(got, expected);

    // ── Async-only chain (async-feeder path → `spawn_async_feeder_single`):
    //    the feeder pushes the mixed-mode channel directly into the sharded
    //    terminal ──
    let mut got = stream(0..n_aux() as u64)
        .stage_async(|x: u64| async move { x.wrapping_add(1_000_000) })
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = (0..n_aux() as u64).map(|x| x + 1_000_000).collect();
    assert_eq!(got, expected);

    // ── Ordered async-only chain ──
    let got = stream(0..n_aux() as u64)
        .stage_async(|x: u64| async move { x.wrapping_add(1_000_000) })
        .ordered()
        .run();
    assert_eq!(got, expected);

    // ── Expand upstream of the async terminal ──
    let mut got = stream(0..n_aux() as u64)
        .expand_emit(|x: u64, out: &mut Vec<u64>| {
            out.push(x);
            out.push(x.wrapping_add(100_000));
        })
        .stage_async(|x: u64| async move { x + 1 })
        .run();
    got.sort_unstable();
    let mut expected: Vec<u64> = (0..n_aux() as u64)
        .flat_map(|x| [x + 1, x + 100_001])
        .collect();
    expected.sort_unstable();
    assert_eq!(got, expected);

    // ── for_each terminal: exactly-n invocations, nothing duplicated ──
    let seen = AtomicUsize::new(0);
    let checksum = AtomicUsize::new(0);
    stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .stage(|x: u64| x * 2)
        .stage_async(|x: u64| async move { x + 1 })
        .for_each(|x: u64| {
            seen.fetch_add(1, Ordering::Relaxed);
            checksum.fetch_add(usize::try_from(x).unwrap(), Ordering::Relaxed);
        });
    assert_eq!(seen.into_inner(), n_aux());
    let expected_sum: usize = (0..n_aux()).map(|x| x * 2 + 1).sum();
    assert_eq!(checksum.into_inner(), expected_sum);

    // ── Small fan-out (io_concurrency 2): drains to EOF without hang — the
    //    guard against per-shard fixed cost breaking tiny terminals ──
    let got = stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .stage_async_with(
            AsyncStageOptions::new().io_concurrency(2),
            |x: u64| async move { x + 3 },
        )
        .ordered()
        .run();
    assert_eq!(got, (0..n_aux() as u64).map(|x| x + 3).collect::<Vec<_>>());

    // ── io_concurrency 1: never shards (single task IS one SPSC ring) ──
    let mut got = stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .stage_async_with(
            AsyncStageOptions::new().io_concurrency(1),
            |x: u64| async move { x + 4 },
        )
        .run();
    got.sort_unstable();
    assert_eq!(got, (0..n_aux() as u64).map(|x| x + 4).collect::<Vec<_>>());

    // ── io_concurrency above the runtime's worker threads: tasks share
    //    shards (the round-robin sender sharing path) ──
    let mut got = stream(0..n_main() as u64)
        .with_config(PipelineConfig::default().with_io_concurrency(64))
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x * 2 })
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = (0..n_main() as u64).map(|x| (x + 1) * 2).collect();
    assert_eq!(got, expected);

    // ── Producer task dies mid-run (panic → runtime drops its sender): the
    //    shard closes, the run must drain the remaining shards and return
    //    (fewer items) instead of hanging. Same contract as the shared
    //    ring's one-sender-of-many drop. ──
    let n_panic: u64 = if cfg!(miri) {
        200
    } else {
        10_000
    };
    let got = stream(0..n_panic)
        .with_cancel(inert_cancel())
        .stage_async(move |x: u64| async move {
            assert!(x != n_panic / 2, "async consumer task exits mid-run");
            x
        })
        .run();
    // Exactly one item is lost: the panicking task dies holding the item it
    // was processing; the remaining tasks drain the input channel and their
    // shards, and the dead task"s shard closes on its dropped sender.
    assert_eq!(got.len(), usize::try_from(n_panic - 1).unwrap());

    // ── Cancellation: a fired token mid-run must return (fewer items), not
    //    hang on the shard aggregation ──
    let token = CancellationToken::new();
    let fired = token.clone();
    let n_cancel: u64 = if cfg!(miri) {
        2_000
    } else {
        100_000
    };
    let cancelled = stream(0..n_cancel)
        .with_cancel(token)
        .stage(move |x: u64| {
            if x == n_cancel / 2 {
                fired.cancel();
            }
            x
        })
        .stage_async(|x: u64| async move { x })
        .run();
    assert!(
        cancelled.len() <= usize::try_from(n_cancel).unwrap(),
        "cancelled run returned {} items",
        cancelled.len()
    );

    // ── Zero-shape: single-item input through a sharded-eligible terminal ──
    let got = stream(vec![42u64])
        .stage_async(|x: u64| async move { x + 1 })
        .run();
    assert_eq!(got, vec![43]);
}
