//! End-to-end semantics of the `YOUPIPE_SHARDED_TERM=1` terminal topology:
//! one SPSC output ring per terminal worker instead of one shared MPSC ring.
//!
//! One `#[test]` function on purpose: the knob is read through a
//! process-wide `OnceLock`, so the env var must be set before the first
//! `run()` in this process — sibling test fns running in parallel would race
//! the init. Scenarios are sequential blocks inside the single fn.
//!
//! `with_cancel` (an un-fired token) forces the streaming topology on pure
//! sync chains — otherwise they take the fused pass-through and never build
//! a terminal channel at all.

use std::sync::atomic::{AtomicUsize, Ordering};

use youpipe::{FenceMode, SyncStageOptions, stream, sync::CancellationToken};

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

/// An un-fired token: disables the fused pass-through, keeps cancellation
/// inert.
fn inert_cancel() -> CancellationToken {
    CancellationToken::new()
}

#[test]
fn sharded_terminal_semantics() {
    // SAFETY: this fn is the only test in this binary and no worker threads
    // exist yet — the first `run()` (which initializes the knob's OnceLock)
    // happens after this line.
    unsafe { std::env::set_var("YOUPIPE_SHARDED_TERM", "1") };

    // ── Unordered single stage: same multiset as sequential ──
    let items: Vec<u64> = (0..n_main() as u64).collect();
    let mut got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = items.iter().map(|x| x * 3 + 1).collect();
    assert_eq!(got, expected);

    // ── Ordered single stage: exact input order across shards ──
    let got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
        .ordered()
        .run();
    assert_eq!(got, expected);

    // ── Ordered multi-stage (3 sync stages): seq monotonicity across the
    //    mid channels AND the sharded terminal ──
    let got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x + 1)
        .stage(|x: u64| x * 2)
        .stage(|x: u64| x ^ 0x55)
        .ordered()
        .run();
    let expected: Vec<u64> = items.iter().map(|x| ((x + 1) * 2) ^ 0x55).collect();
    assert_eq!(got, expected);

    // ── Expand terminal: fan-out shards must deliver every expansion ──
    let mut got = stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .expand_emit(|x: u64, out: &mut Vec<u64>| {
            out.push(x);
            out.push(x + 100_000);
        })
        .run();
    got.sort_unstable();
    let mut expected: Vec<u64> = (0..n_aux() as u64).flat_map(|x| [x, x + 100_000]).collect();
    expected.sort_unstable();
    assert_eq!(got, expected);

    // ── Fence mid-chain + sharded terminal after it ──
    let mut got = stream(items.clone())
        .with_cancel(inert_cancel())
        .stage(|x: u64| x + 7)
        .fence(FenceMode::Chunked(
            std::num::NonZeroUsize::new(500).unwrap(),
        ))
        .stage(|x: u64| x * 11)
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = items.iter().map(|x| (x + 7) * 11).collect();
    assert_eq!(got, expected);

    // ── for_each terminal: exactly-n invocations, nothing duplicated ──
    let seen = AtomicUsize::new(0);
    let checksum = AtomicUsize::new(0);
    stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .stage(|x: u64| x * 2)
        .for_each(|x: u64| {
            seen.fetch_add(1, Ordering::Relaxed);
            checksum.fetch_add(usize::try_from(x).unwrap(), Ordering::Relaxed);
        });
    assert_eq!(seen.into_inner(), n_aux());
    let expected_sum: usize = (0..n_aux()).map(|x| x * 2).sum();
    assert_eq!(checksum.into_inner(), expected_sum);

    // ── Small-worker shape (2 workers): drains to EOF without hang ──
    let got = stream(0..n_aux() as u64)
        .with_cancel(inert_cancel())
        .stage_with(SyncStageOptions::new().workers(2), |x: u64| x + 3)
        .ordered()
        .run();
    assert_eq!(got, (0..n_aux() as u64).map(|x| x + 3).collect::<Vec<_>>());

    // ── Cancellation: a fired token mid-run must return (fewer items),
    //    not hang on the shard aggregation ──
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
        .run();
    assert!(
        cancelled.len() <= usize::try_from(n_cancel).unwrap(),
        "cancelled run returned {} items",
        cancelled.len()
    );

    // ── Zero-shape: single-item input through a sharded-eligible stage ──
    let got = stream(vec![42u64])
        .with_cancel(inert_cancel())
        .stage(|x: u64| x + 1)
        .run();
    assert_eq!(got, vec![43]);
}
