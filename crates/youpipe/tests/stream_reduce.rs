//! Aggregation terminals on the streaming topology (the non-fused fallback
//! taken with cancellation or async stages): outputs fold into the drain
//! accumulator; results must match the sequential fold of the same chain.

use youpipe::{CancellationToken, stream};

/// Never cancelled — only forces the chain off the fused pass-through.
fn inert() -> CancellationToken {
    CancellationToken::new()
}

fn bump(x: u64) -> u64 {
    x * 2 + 1
}

#[test]
#[allow(clippy::cast_possible_truncation)] // test sizes fit usize everywhere
fn streaming_aggregations_match_sequential() {
    let n = 1000_u64;
    let expected: Vec<u64> = (0..n).map(bump).collect();

    let chained = || {
        stream((0..n).collect::<Vec<_>>())
            .with_cancel(inert())
            .stage(bump)
    };

    let s: u64 = chained().sum();
    assert_eq!(s, expected.iter().copied().sum::<u64>());
    assert_eq!(chained().count(), n as usize);
    assert_eq!(chained().min(), expected.iter().copied().min());
    assert_eq!(chained().max(), expected.iter().copied().max());
    assert_eq!(
        chained().reduce(u64::wrapping_add),
        expected.iter().copied().reduce(u64::wrapping_add)
    );
    assert_eq!(
        chained().fold(0_u64, |a, o| a + o, |a, b| a + b),
        expected.iter().copied().sum::<u64>()
    );
}

/// `ordered()` + a non-commutative `reduce`: the fold must see input order,
/// exactly like the materialise-then-fold path did.
#[test]
fn streaming_reduce_ordered_non_commutative() {
    let items: Vec<String> = (0..50_u64).map(|i| format!("{i:02}-")).collect();
    let expected: String = items.iter().cloned().collect();

    let got = stream(items)
        .with_cancel(inert())
        .stage(|s: String| s)
        .ordered()
        .reduce(|a, b| a + &b);
    assert_eq!(got, Some(expected));
}

#[test]
fn streaming_aggregations_empty_and_single() {
    let empty: Vec<u64> = Vec::new();
    assert_eq!(stream(empty).with_cancel(inert()).stage(bump).sum(), 0);
    assert_eq!(
        stream(Vec::<u64>::new())
            .with_cancel(inert())
            .stage(bump)
            .count(),
        0
    );
    assert_eq!(
        stream(Vec::<u64>::new())
            .with_cancel(inert())
            .stage(bump)
            .min(),
        None
    );
    assert_eq!(
        stream(Vec::<u64>::new())
            .with_cancel(inert())
            .stage(bump)
            .max(),
        None
    );
    assert_eq!(
        stream(Vec::<u64>::new())
            .with_cancel(inert())
            .stage(bump)
            .reduce(u64::wrapping_add),
        None
    );
    assert_eq!(
        stream(Vec::<u64>::new())
            .with_cancel(inert())
            .stage(bump)
            .fold(7_u64, |a, o| a + o, u64::wrapping_add),
        7
    );

    assert_eq!(
        stream(vec![41_u64])
            .with_cancel(inert())
            .stage(bump)
            .count(),
        1
    );
    assert_eq!(
        stream(vec![41_u64]).with_cancel(inert()).stage(bump).sum(),
        bump(41)
    );
    assert_eq!(
        stream(vec![41_u64]).with_cancel(inert()).stage(bump).min(),
        Some(bump(41))
    );
}

/// Async-stage chains drain through the async collector variants.
#[cfg(feature = "tokio-runtime")]
#[test]
#[allow(clippy::cast_possible_truncation)] // test sizes fit usize everywhere
fn streaming_aggregations_async_stage() {
    let n = 500_u64;
    let expected: u64 = (0..n).map(bump).sum::<u64>();

    let s = stream((0..n).collect::<Vec<_>>())
        .stage_async(|x: u64| async move { bump(x) })
        .sum();
    assert_eq!(s, expected);

    let c = stream((0..n).collect::<Vec<_>>())
        .stage_async(|x: u64| async move { bump(x) })
        .count();
    assert_eq!(c, n as usize);
}
