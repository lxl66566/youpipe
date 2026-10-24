//! `run_checked` / `RunOutcome` semantics for the stream engine: a cancelled
//! run reports `Cancelled { emitted }` with an exact count instead of the
//! silently truncated `Vec` that `run()` returns.

use youpipe::{CancellationToken, RunOutcome, stream};

/// Miri interprets every spawned thread and channel atomic; scale item
/// counts down but keep the same dispatch paths (same convention as the
/// other integration suites).
fn n_items(native: u64) -> u64 {
    if cfg!(miri) { native / 20 } else { native }
}

/// Mid-run cancellation: the worker that observes the trigger item fires the
/// token. `emitted` must count exactly the collected items, the trigger
/// item itself must be among them (a worker finishes its current item), and
/// the run must return short — the default 256-slot channels bound what can
/// still cross after the trigger (n/2), so `emitted < n` for these sizes.
#[test]
fn run_checked_reports_cancelled_mid_run() {
    let n = n_items(100_000);
    let token = CancellationToken::new();
    let fired = token.clone();
    let trigger = n / 2;
    let outcome = stream(0..n)
        .with_cancel(token)
        .stage(move |x: u64| {
            if x == trigger {
                fired.cancel();
            }
            x + 1
        })
        .run_checked();
    let partial = match outcome {
        RunOutcome::Cancelled { partial, emitted } => {
            assert_eq!(
                emitted,
                partial.len(),
                "emitted must count exactly the collected items"
            );
            partial
        },
        RunOutcome::Completed(_) => panic!("mid-run cancellation must report Cancelled"),
    };
    assert!(
        partial.len() < usize::try_from(n).unwrap(),
        "cancelled run must return short: {} of {n}",
        partial.len()
    );
    assert!(
        partial.contains(&(trigger + 1)),
        "the trigger item completes and must be emitted"
    );
}

/// Unfired token: `Completed`, full output (unordered drain — compare as a
/// multiset).
#[test]
fn run_checked_unfired_token_completes() {
    let n = n_items(10_000);
    let token = CancellationToken::new();
    let mut got = stream(0..n)
        .with_cancel(token)
        .stage(|x: u64| x + 1)
        .run_checked()
        .into_inner();
    got.sort_unstable();
    assert_eq!(got, (0..n).map(|x| x + 1).collect::<Vec<_>>());
}

/// No token attached: `run_checked` delegates to `run`'s topology (the
/// fused pass-through stays eligible) and always reports `Completed` —
/// the input order the pass-through produces makes exact equality valid.
#[test]
fn run_checked_without_token_completes() {
    let outcome = stream(0..1_000u64).stage(|x| x * 2).run_checked();
    assert_eq!(
        outcome,
        RunOutcome::Completed((0..1_000u64).map(|x| x * 2).collect::<Vec<_>>())
    );
}

/// Ordered + cancelled: the partial output stays strictly ascending (input
/// order, gaps allowed — the feeder stopped mid-stream) with the same exact
/// `emitted == partial.len()` accounting.
#[test]
fn run_checked_ordered_cancel_stays_in_input_order() {
    let n = n_items(100_000);
    let token = CancellationToken::new();
    let fired = token.clone();
    let trigger = n / 2;
    let outcome = stream(0..n)
        .with_cancel(token)
        .stage(move |x: u64| {
            if x == trigger {
                fired.cancel();
            }
            x + 1
        })
        .ordered()
        .run_checked();
    let partial = match outcome {
        RunOutcome::Cancelled { partial, emitted } => {
            assert_eq!(emitted, partial.len());
            partial
        },
        RunOutcome::Completed(_) => panic!("must report Cancelled"),
    };
    assert!(partial.len() < usize::try_from(n).unwrap());
    assert!(
        partial.windows(2).all(|w| w[0] < w[1]),
        "ordered output must stay ascending"
    );
}

/// Pre-fired token (the `try_run_checked` fallible twin): the feeder aborts
/// on its first check; the outcome is still `Cancelled` with a consistent
/// count.
#[test]
fn run_checked_prefired_token() {
    let token = CancellationToken::new();
    token.cancel();
    let outcome = stream(0..10_000u64)
        .with_cancel(token)
        .stage(|x| x + 1)
        .try_run_checked()
        .expect("sync chain never fails runtime construction");
    let (partial, emitted) = match outcome {
        RunOutcome::Cancelled { partial, emitted } => (partial, emitted),
        RunOutcome::Completed(_) => panic!("pre-fired token must report Cancelled"),
    };
    assert_eq!(emitted, partial.len());
    assert!(partial.len() < 10_000);
}
