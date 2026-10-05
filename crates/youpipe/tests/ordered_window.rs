//! Ordered-drain reorder-window regressions (review B6 + P-1, 2026-10).
//!
//! B6: the ordered drain historically clamped its reorder window to
//! `next_pow2(n).clamp(1 Ki, 1 Mi)`. A straggler (slow item early in the
//! stream) lets successors overtake it without bound — the live span at the
//! collector is bounded only by `n`, not by the pipeline's channel
//! capacities — so any window below `n` silently dropped data once the span
//! crossed it. The window now pre-sizes to the pipeline occupancy and grows
//! up to `next_pow2(n)` (see `state::stream::OrderedWindow`).
//!
//! P-1: with the growth fix, a large-`n` ordered run pre-sizes to the
//! occupancy estimate instead of a fixed 1 Mi slots (16 MiB zero-init per
//! run); see the `OrderedWindow::auto` unit tests in `state::stream`.

use std::{thread::sleep, time::Duration};

use youpipe::{SyncStageOptions, stream};

/// The review's repro shape: `buffer ≥ n` (everything fits in the feeder
/// channel), `n > 1 Mi`, seq 0 delayed. Pre-fix the 1 Mi window clamp
/// dropped exactly `n - 1 Mi` items (`got 1048576 of 1148576`).
#[test]
fn ordered_straggler_large_buffer_returns_everything() {
    let n: u64 = (1 << 20) + 100_000;
    let count = usize::try_from(n).unwrap();
    let out: Vec<u64> = stream(0..n)
        .with_buffer_size(count + 16)
        .stage_with(SyncStageOptions::new().workers(32), |x: u64| {
            if x == 0 {
                sleep(Duration::from_millis(300)); // park seq 0 behind its successors
            }
            x
        })
        .ordered()
        .run();
    assert_eq!(out.len(), count);
    assert_eq!(
        out,
        (0..n).collect::<Vec<_>>(),
        "output must be input order"
    );
}

/// Same straggler with *default* buffers: the live span far exceeds both
/// the 1 Ki initial window and the pipeline's in-flight occupancy (Σ
/// buffers + Σ workers ≈ a few hundred slots), so the drain must have
/// grown the reorder buffer repeatedly. Pre-fix this dropped ~half the
/// stream (`got 1048576 of 2000000` with n = 2 M).
#[test]
fn ordered_straggler_default_buffers_grow_window() {
    let n: u64 = 300_000; // span ≫ initial 1 Ki window, ≫ pipeline occupancy
    let count = usize::try_from(n).unwrap();
    let out: Vec<u64> = stream(0..n)
        .stage_with(SyncStageOptions::new().workers(16), |x: u64| {
            if x == 0 {
                sleep(Duration::from_millis(200));
            }
            x
        })
        .ordered()
        .run();
    assert_eq!(out.len(), count);
    assert_eq!(out, (0..n).collect::<Vec<_>>());
}

/// An explicit small buffer must behave identically: occupancy sizing can
/// never trade correctness (the pre-fix clamp had the same failure with
/// `with_buffer_size(64)`).
#[test]
fn ordered_straggler_tiny_buffer_returns_everything() {
    let n: u64 = 150_000;
    let count = usize::try_from(n).unwrap();
    let out: Vec<u64> = stream(0..n)
        .with_buffer_size(64)
        .stage_with(SyncStageOptions::new().workers(16), |x: u64| {
            if x == 0 {
                sleep(Duration::from_millis(200));
            }
            x
        })
        .ordered()
        .run();
    assert_eq!(out.len(), count);
}

/// `for_each` shares the ordered drain: the same straggler must deliver
/// every item to the closure, in input order.
#[test]
fn ordered_straggler_for_each_full_delivery() {
    let n: u64 = 100_000;
    let mut count = 0u64;
    let mut last = None;
    stream(0..n)
        .stage_with(SyncStageOptions::new().workers(8), |x: u64| {
            if x == 0 {
                sleep(Duration::from_millis(150));
            }
            x
        })
        .ordered()
        .for_each(|x: u64| {
            if let Some(l) = last {
                assert_eq!(x, l + 1, "ordered for_each must deliver in input order");
            }
            last = Some(x);
            count += 1;
        });
    assert_eq!(count, n);
}
