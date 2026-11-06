//! Regression for the sharded async terminal
//! (`YOUPIPE_SHARDED_TERM_ASYNC=1`) on a one-worker runtime
//! (`with_async_pool(TokioPool::build(1))`). The terminal selector used to
//! decide on `io_concurrency` and `config.async_workers` alone, but the
//! factory re-caps shards at the resolved driver count — one worker drives
//! a single consumer task, so the factory computed zero shards and the
//! channel constructor panicked (`shards must be nonzero`; debug builds
//! tripped the `AsyncSharded::make` debug_assert first). Miri reaches the
//! same shape with its default runtime (`available_parallelism() == 1`).
//!
//! Own test binary (same discipline as `sharded_terminal_async`): the knob
//! is read through a process-wide `OnceLock`, so the env var must be set
//! before the first `run()` in this process.
#![cfg(feature = "tokio-runtime")]

use youpipe::{TokioPool, stream};

#[test]
fn sharded_async_terminal_one_worker_runtime_keeps_single_ring() {
    // SAFETY: this fn is the only test in this binary and no worker
    // threads exist yet — the first `run()` (which initializes the knob's
    // OnceLock) happens after this line.
    unsafe { std::env::set_var("YOUPIPE_SHARDED_TERM_ASYNC", "1") };

    let n: u64 = if cfg!(miri) {
        200
    } else {
        10_000
    };
    let pool = TokioPool::build(1).expect("build one-worker runtime");
    let mut got = stream(0..n)
        .with_async_pool(pool)
        .stage_async(|x: u64| async move { x.wrapping_mul(3).wrapping_add(1) })
        .run();
    got.sort_unstable();
    let expected: Vec<u64> = (0..n).map(|x| x * 3 + 1).collect();
    assert_eq!(got, expected);
}
