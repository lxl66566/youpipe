//! Streaming `.stage_async(..)` coverage on the **compio** backend.
//!
//! compio's runtime is single-threaded and its native IO/timer primitives are
//! `!Send`, so these tests use `Send`-bounded async closures (pure
//! computation + crossfire channels). They validate that the streaming
//! machinery — feeder, consumer fan-out, sync→async handoff, async collector
//! — drives correctly on compio's thread-local runtime, mirroring the tokio
//! tests in `pipeline_integration.rs`.

#![cfg(feature = "compio-runtime")]

use youpipe::{CompioPool, PipelineConfig, stream};

#[test]
fn test_compio_stage_async_without_explicit_pool() {
    // Default transient runtime (built lazily in `acquire_async`). No
    // `with_async_pool` attached.
    let items: Vec<u64> = (0..100).collect();
    let mut result = stream(items)
        .stage_async(|x: u64| async move { x.wrapping_mul(3) })
        .run();
    result.sort_unstable();
    let expected: Vec<u64> = (0..100).map(|x| x * 3).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_compio_mixed_sync_async() {
    // sync CPU stage → async stage, exercising the sync→async mixed-mode
    // channel handoff (ComputePool workers write the `SyncSender`, compio
    // consumer tasks `recv().await` on the shared `AsyncReceiver`).
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x * 2 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_compio_two_async_stages() {
    // Two consecutive async stages — both consumers and the async→async
    // bridge share the lazily-built runtime on the calling thread.
    let items: Vec<u64> = (0..50).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage_async(|x: u64| async move { x * 10 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..50).map(|x| (x + 1) * 10).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_compio_async_then_sync_via_bridge() {
    // async-first → sync stage. Exercises `spawn_async_feeder`: the feeder
    // uses a mixed-mode channel, the AsyncStage consumes it directly, and the
    // SyncStage's default impl bridges async→sync on a dedicated OS thread
    // (which builds its own thread-local compio runtime to poll the crossfire
    // recv).
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage(|x: u64| x * 2)
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_compio_explicit_pool_reuse() {
    // An explicitly-attached `CompioPool` is reused across runs (the
    // thread-local runtime persists on the calling thread).
    let pool = CompioPool::build_default().unwrap();
    for shift in 0..5 {
        let items: Vec<u64> = (0..50).collect();
        let result: Vec<u64> = stream(items)
            .with_async_pool(pool.clone())
            .stage_async(move |x: u64| async move { x + shift })
            .ordered()
            .run();
        let expected: Vec<u64> = (0..50).map(|x| x + shift).collect();
        assert_eq!(result, expected);
    }
}

#[test]
fn test_compio_io_concurrency_bounded() {
    // A higher `io_concurrency` fans out more consumer tasks; validate the
    // output is still complete and correct.
    let items: Vec<u64> = (0..200).collect();
    let result: Vec<u64> = stream(items)
        .with_config(PipelineConfig::default().with_io_concurrency(64))
        .stage_async(|x: u64| async move { x | 1 })
        .run();
    let mut sorted = result;
    sorted.sort_unstable();
    let expected: Vec<u64> = (0..200).map(|x| x | 1).collect();
    assert_eq!(sorted, expected);
}
