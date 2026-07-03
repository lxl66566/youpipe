use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use youpipe::{FenceMode, pipe, stream};

fn cpu_heavy(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..200 {
        r = r.wrapping_mul(31).wrapping_add(17);
    }
    r
}

#[test]
fn test_par_map_correctness() {
    let items: Vec<u64> = (0..1000).collect();
    let result = pipe(items.clone()).map(|x: u64| cpu_heavy(x)).collect();
    let expected: Vec<u64> = items.iter().map(|&x| cpu_heavy(x)).collect();
    assert_eq!(result.len(), expected.len());
    let mut r = result;
    r.sort_unstable();
    let mut e = expected;
    e.sort_unstable();
    assert_eq!(r, e);
}

#[test]
fn test_par_map_empty() {
    let result = pipe(Vec::<u64>::new()).map(|x: u64| x + 1).collect();
    assert!(result.is_empty());
}

#[test]
fn test_par_map_single() {
    let result = pipe(vec![42u64]).map(|x: u64| x + 1).collect();
    assert_eq!(result, vec![43]);
}

#[test]
fn test_pipeline_fusion_3_stages() {
    let items: Vec<i32> = (0..500).collect();
    let result = pipe(items)
        .map(|x: i32| x + 1)
        .map(|x: i32| x * 3)
        .map(|x: i32| x - 7)
        .collect();
    let expected: Vec<i32> = (0..500).map(|x| (x + 1) * 3 - 7).collect();
    let mut r = result;
    r.sort_unstable();
    let mut e = expected;
    e.sort_unstable();
    assert_eq!(r, e);
}

#[test]
fn test_pipeline_filter_map() {
    let items: Vec<i32> = (0..100).collect();
    let result = pipe(items)
        .filter(|x: &i32| x % 3 == 0)
        .map(|x: i32| x * 10)
        .collect();
    let expected: Vec<i32> = (0..100).filter(|x| x % 3 == 0).map(|x| x * 10).collect();
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r, expected);
}

#[test]
fn test_try_map_ok() {
    let result = pipe(0..100)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x * 3) })
        .try_collect()
        .unwrap();
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r, (0..100).map(|x| x * 3).collect::<Vec<_>>());
}

#[test]
fn test_try_map_err_short_circuits() {
    let result = pipe(0..100)
        .try_map(|x: i32| -> Result<i32, String> {
            if x == 50 {
                Err(format!("bad: {x}"))
            } else {
                Ok(x * 2)
            }
        })
        .try_collect();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "bad: 50");
}

#[test]
fn test_try_map_then_map() {
    // Chain an inffallible map after a try_map: error type stays the same.
    let r: Result<Vec<String>, &str> = pipe(0..5)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x * 2) })
        .map(|x: i32| x.to_string())
        .try_collect();
    assert_eq!(r.unwrap(), vec!["0", "2", "4", "6", "8"]);
}

#[test]
fn test_try_map_parallel_large() {
    // Large enough to exceed the serial threshold (num_threads * 64) and
    // exercise the index-based parallel fast path (MAY_FILTER == false).
    let n = 50_000;
    let result = pipe(0..n)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x.wrapping_mul(3)) })
        .map(|x: i32| x + 1)
        .try_collect()
        .unwrap();
    assert_eq!(result.len(), n as usize);
    assert_eq!(result[0], 1);
    assert_eq!(result[n as usize - 1], (n - 1) * 3 + 1);
}

#[test]
fn test_try_map_parallel_error_short_circuits() {
    // Error in the parallel path (index-based fast path) must propagate.
    let n = 50_000;
    let result = pipe(0..n)
        .try_map(|x: i32| -> Result<i32, String> {
            if x == 30_000 {
                Err("mid-batch error".into())
            } else {
                Ok(x * 2)
            }
        })
        .try_collect();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "mid-batch error");
}

#[test]
fn test_stream_single_ordered() {
    let items: Vec<i32> = (0..100).collect();
    let result = stream(items).stage(|x: i32| x * 2 + 1).ordered().run();
    let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_single_unordered() {
    let items: Vec<i32> = (0..100).collect();
    let mut result = stream(items).stage(|x: i32| x * 2 + 1).run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_multi_stage() {
    let items: Vec<i32> = (0..200).collect();
    let mut result = stream(items)
        .stage(|x: i32| x + 10)
        .stage(|x: i32| x * 2)
        .run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..200).map(|x| (x + 10) * 2).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_with_fence_chunked() {
    let items: Vec<i32> = (0..100).collect();
    let mut result = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Chunked(NonZeroUsize::new(25).unwrap()))
        .stage(|x: i32| x * 5)
        .run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..100).map(|x| (x + 1) * 5).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_with_fence_full_barrier() {
    let items: Vec<i32> = (0..50).collect();
    let result = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Barrier)
        .stage(|x: i32| x * 2)
        .ordered()
        .run();
    let expected: Vec<i32> = (0..50).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

/// Regression: `fence` previously deadlocked whenever the input size exceeded
/// the inter-stage channel buffer (256 by default). These run with a large
/// input far above that buffer to lock in the eager-drain fix.
///
/// Skipped under Miri: the regression requires the stage-1 and stage-2 pool
/// jobs to run *concurrently* (the design keeps total blocking jobs ≤ pool
/// size), i.e. a global pool of at least 2 workers. Miri reports
/// `available_parallelism() == 1`, so the global pool has a single worker and
/// the two stage jobs can't both be scheduled — the pipeline then deadlocks
/// once input exceeds the buffer. This is a test-environment constraint, not
/// a code defect. The fence code paths are still exercised here by the
/// smaller-input `test_stream_with_fence*` tests.
#[test]
#[cfg_attr(miri, ignore)]
fn test_stream_fence_large_input_no_deadlock() {
    let n: usize = 5_000; // well above the default 256-slot channel buffer

    // Chunked, unordered.
    let items: Vec<i32> = (0..n as i32).collect();
    let mut r = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
        .stage(|x: i32| x * 3)
        .run();
    r.sort_unstable();
    let expected: Vec<i32> = (0..n as i32).map(|x| (x + 1) * 3).collect();
    assert_eq!(r, expected);

    // Barrier, ordered — the exact shape that hung the bench.
    let items: Vec<i32> = (0..n as i32).collect();
    let r = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Barrier)
        .stage(|x: i32| x * 3)
        .ordered()
        .run();
    assert_eq!(r, expected);
}

#[test]
fn test_stream_expand() {
    let items: Vec<i32> = (0..10).collect();
    let mut result = stream(items)
        .expand(|x: i32| vec![x, x * 10])
        .stage(|x: i32| x + 1)
        .run();
    result.sort_unstable();
    let mut expected: Vec<i32> = (0..10).flat_map(|x| vec![x + 1, x * 10 + 1]).collect();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
#[should_panic(expected = "incompatible with `.expand()`")]
fn test_stream_expand_ordered_rejected() {
    // expand + ordered() is rejected: expand fan-out shares the parent seq,
    // which the single-item-per-seq ReorderBuffer cannot handle. See the
    // `StageSpawn::has_expand` doc and the panic message in `run`.
    let items: Vec<i32> = (0..10).collect();
    let _ = stream(items)
        .expand(|x: i32| vec![x, x * 10])
        .stage(|x: i32| x + 1)
        .ordered()
        .run();
}

// ── Async stage regression coverage (gated by tokio-runtime) ──
//
// These exercise the lazy-pool path: no `with_async_pool` is attached, so
// `StreamCtx::acquire_async` must build one runtime per `run()` and reuse it
// across every bridge / async consumer in that call. A regression that
// builds a runtime per `acquire_async` call would still pass these (the
// output is unchanged) but would silently wreck small-workload latency; the
// real correctness guard is that none of these hang or panic when the
// lazily-built runtime is dropped at the end of `run()`.

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_stage_async_without_explicit_pool() {
    // The simplest async path: no config, no `with_async_pool`. Should "just
    // work" with sensible defaults.
    let items: Vec<u64> = (0..100).collect();
    let mut result = stream(items)
        .stage_async(|x: u64| async move { x.wrapping_mul(3) })
        .run();
    result.sort_unstable();
    let expected: Vec<u64> = (0..100).map(|x| x * 3).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_mixed_sync_async_without_explicit_pool() {
    // sync CPU stage → async IO stage, no explicit pool. Exercises the
    // sync→async bridge plus the async consumer, both of which call
    // `acquire_async` — they must share the lazily-built runtime.
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x * 2 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_two_async_stages_share_lazy_pool() {
    // Two consecutive async stages — the hardest case for the lazy pool.
    // Both stages' consumers and the async→async bridge all call
    // `acquire_async`; they must observe the same lazily-built runtime, and
    // the runtime must outlive every detached bridge task.
    let items: Vec<u64> = (0..50).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage_async(|x: u64| async move { x * 10 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..50).map(|x| (x + 1) * 10).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_async_then_sync_via_bridge() {
    // async-first → sync stage. Exercises the default `spawn_async_feeder`
    // path: the feeder uses a mixed-mode channel, the AsyncStage's
    // `spawn_async_feeder` recurses into StreamStart (identity), and the
    // SyncStage's default impl bridges async→sync on a dedicated OS thread.
    // This is the topology that previously did a blocking `send` inside a
    // `tokio::spawn` task (parking a tokio worker on backpressure).
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage(|x: u64| x * 2)
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_sync_to_async_does_not_stall_tokio_driver() {
    // Regression guard for the "async driver + blocking worker" anti-pattern.
    //
    // The sync→async handoff parks producers on `SyncSender::send` when the
    // mixed-mode channel fills under backpressure. That blocking call MUST
    // live on a ComputePool OS thread — never on a tokio worker. If it ran
    // inside a `tokio::spawn` task, it would park the tokio worker thread and
    // stall *every* other task on it (or, with one worker, deadlock).
    //
    // This test amplifies the effect with a single-worker runtime: any
    // blocking op on that one worker freezes the whole async side. We run a
    // sync→async pipeline under deliberate backpressure (fast sync producer,
    // slow async consumer) while a heartbeat task measures its own scheduling
    // gaps on the same runtime. A healthy gap stays near the sleep duration;
    // a stalled driver (blocking `send` on the tokio worker) spikes it.
    use std::time::{Duration, Instant};

    use youpipe::{AsyncPool, PipelineConfig};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build single-worker runtime");

    // Heartbeat: 30 sleeps of 5 ms, tracking the worst observed gap between
    // successive wake-ups. Spawned onto the same single-worker runtime as the
    // pipeline's async consumers, so it shares the fate of the tokio worker.
    let (hb_tx, hb_rx) = std::sync::mpsc::channel::<Duration>();
    {
        let _enter = rt.handle().enter();
        tokio::spawn(async move {
            let mut max_gap = Duration::ZERO;
            let mut prev = Instant::now();
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let now = Instant::now();
                max_gap = max_gap.max(now - prev);
                prev = now;
            }
            let _ = hb_tx.send(max_gap);
        });
    }

    // Pipeline runs on a dedicated OS thread so a stalled runtime can't hang
    // the test thread — we observe completion via a channel with a timeout.
    // Fast sync stage floods the channel; slow async stage (1 ms sleep each,
    // only 4 consumers) drains it slowly, so the mixed-mode channel fills and
    // sync workers park on `send`. This is precisely the regime where a
    // tokio-hosted producer would freeze the runtime.
    let n: u64 = 3000;
    let (res_tx, res_rx) = std::sync::mpsc::channel::<Vec<u64>>();
    let pipe = stream(0..n)
        .with_config(PipelineConfig::default().with_io_concurrency(4))
        .with_async_pool(AsyncPool::new(rt.handle().clone(), 1))
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            x * 2
        });
    std::thread::spawn(move || {
        let _ = res_tx.send(pipe.run());
    });

    // Strong guarantee: the single tokio worker was never parked by a blocking
    // send. Heartbeat gaps stay near 5 ms even while sync workers are parked
    // on `send` under backpressure. 40 ms leaves headroom for single-worker
    // scheduling jitter; a stalled driver either never finishes the heartbeat
    // (timeout below) or spikes past this.
    let max_gap = hb_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("heartbeat never finished — tokio worker stalled by a blocking op");
    assert!(
        max_gap < Duration::from_millis(40),
        "tokio driver stalled under sync→async backpressure: max heartbeat gap \
         {max_gap:?} (expected ~5 ms) — a blocking send is likely running on \
         the tokio worker"
    );

    // Weak guarantee: the pipeline completed at all — no deadlock from a
    // stalled runtime starving its own consumers.
    let result = res_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("pipeline deadlocked — tokio worker stalled by a blocking op");
    assert_eq!(result.len(), n as usize);

    // Keep the runtime alive until both observations land.
    drop(rt);
}

#[test]
fn test_fence_cancellation_aborts_early() {
    // The fence forwarder checks the cancellation token. In Barrier mode it
    // buffers all upstream items before forwarding any — without the cancel
    // check it would ignore the token and keep draining until upstream
    // finished, defeating the purpose of cancellation.
    use std::{thread, time::Duration};

    use youpipe::CancellationToken;

    let token = CancellationToken::new();
    let items: Vec<u32> = (0..10_000).collect();
    let slow = |x: u32| -> u32 {
        thread::sleep(Duration::from_micros(20));
        x + 1
    };

    let cancel_handle = {
        let token = token.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            token.cancel();
        })
    };

    let start = std::time::Instant::now();
    let result = stream(items)
        .with_cancel(token)
        .stage(slow)
        .fence(FenceMode::Barrier)
        .stage(|x: u32| x * 2)
        .run();
    let elapsed = start.elapsed();

    cancel_handle.join().unwrap();

    // Cancellation should abort well before processing all 10 000 items.
    assert!(
        result.len() < 10_000,
        "expected early abort, got {}",
        result.len()
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "fence cancellation should shortcut the run, took {elapsed:?}"
    );
}

#[test]
fn test_scope_non_static() {
    let factor = 7i32;
    let result = youpipe::scope(|s| s.pipe(0..20).map(|x: i32| x * factor).collect());
    let expected: Vec<i32> = (0..20).map(|x| x * 7).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_par_map() {
    let offset = 100i32;
    let result = youpipe::scope(|s| s.pipe(0..50).map(|x: i32| x + offset).collect());
    assert_eq!(result, (100..150).collect::<Vec<_>>());
}

#[test]
fn test_par_map_counts_items() {
    let counter = Arc::new(AtomicUsize::new(0));
    let items: Vec<u64> = (0..1000).collect();
    let c = counter.clone();
    let result = pipe(items)
        .map(move |x: u64| {
            c.fetch_add(1, Ordering::Relaxed);
            cpu_heavy(x)
        })
        .collect();
    assert_eq!(result.len(), 1000);
    assert_eq!(counter.load(Ordering::Relaxed), 1000);
}

#[test]
fn test_large_dataset() {
    let items: Vec<u64> = (0..100_000).collect();
    let result = pipe(items).map(|x: u64| x.wrapping_add(1)).collect();
    assert_eq!(result.len(), 100_000);
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r[0], 1);
    assert_eq!(r[99999], 100_000);
}

/// `pipe` accepts any `IntoIterator`, not just `Vec`. Verifies the entry point
/// honours ranges and array references.
#[test]
fn test_pipe_accepts_arbitrary_iterator() {
    let r1: Vec<i32> = pipe(0..10).map(|x: i32| x * 2).collect();
    assert_eq!(r1, (0..10).map(|x| x * 2).collect::<Vec<_>>());

    let r2: Vec<i32> = pipe(&[1, 2, 3]).map(|&x: &i32| x + 1).collect();
    assert_eq!(r2, vec![2, 3, 4]);
}

/// `with_workload(Unbalanced)` produces the same result as `Balanced` but with
/// finer-grained task splitting — guards against the oversplit factor silently
/// changing output cardinality.
#[test]
fn test_pipe_with_workload_unbalanced() {
    use youpipe::Workload;
    let r = pipe(0..1000)
        .map(|x: i32| x.wrapping_mul(3))
        .with_workload(Workload::Unbalanced)
        .collect();
    let mut sorted = r;
    sorted.sort_unstable();
    assert_eq!(sorted, (0..1000).map(|x| x * 3).collect::<Vec<_>>());
}

// ── Prelude: extension-trait style must match the free-function style ──

#[test]
fn test_prelude_pipe_matches_free_function() {
    use youpipe::prelude::IterExt;

    let free: Vec<i32> = pipe(0..100).map(|x: i32| x * 2).collect();
    let method: Vec<i32> = (0..100).pipe().map(|x: i32| x * 2).collect();
    assert_eq!(free, method);
    assert_eq!(free, (0..100).map(|x| x * 2).collect::<Vec<_>>());
}

#[test]
fn test_prelude_stream_matches_free_function() {
    use youpipe::prelude::IterExt;

    let mut free = youpipe::stream(0..50).stage(|x: i32| x + 1).run();
    free.sort_unstable();
    let mut method = (0..50).stream().stage(|x: i32| x + 1).run();
    method.sort_unstable();
    assert_eq!(free, method);
}

/// The prelude trait is a blanket impl over `IntoIterator` — exercises a few
/// iterator sources beyond plain ranges to confirm there's no hidden bound.
#[test]
fn test_prelude_iterext_on_various_sources() {
    use youpipe::prelude::IterExt;

    // Vec
    let v: Vec<i32> = vec![1, 2, 3].pipe().map(|x| x + 1).collect();
    assert_eq!(v, vec![2, 3, 4]);

    // Slice reference
    let s: Vec<i32> = [1, 2, 3].iter().copied().pipe().map(|x| x * 10).collect();
    assert_eq!(s, vec![10, 20, 30]);

    // Stream from a Vec
    let r: Vec<i32> = vec![1, 2, 3].stream().stage(|x: i32| x - 1).ordered().run();
    assert_eq!(r, vec![0, 1, 2]);
}

// ── Panic propagation ──

#[test]
fn test_pipe_panic_propagates_parallel() {
    // Large enough to hit the parallel index-based path (n > serial threshold).
    // A panicking closure must propagate through the join tree and LeafGuard
    // cleanup, surfacing as a real panic on the collecting thread.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Vec<i32> = pipe(0..50_000i32)
            .map(|x| {
                if x == 25_000 {
                    panic!("boom at {x}");
                }
                x + 1
            })
            .collect();
    }));
    assert!(result.is_err());
    let payload = result.unwrap_err();
    let msg = payload
        .downcast_ref::<String>()
        .expect("panic payload is String");
    assert_eq!(msg, "boom at 25000");
}

#[test]
fn test_pipe_panic_propagates_serial() {
    // Small batch: hits the serial fallback loop inside collect().
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Vec<i32> = pipe(0..100i32)
            .map(|x| {
                if x == 50 {
                    panic!("serial boom");
                }
                x + 1
            })
            .collect();
    }));
    assert!(result.is_err());
}

#[test]
fn test_try_collect_panic_propagates() {
    // Panic inside try_collect's fast path (index-based): the TryLeafGuard
    // must clean up partial output slots before the panic propagates.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<Vec<i32>, &'static str> = pipe(0..50_000i32)
            .try_map(|x| -> Result<i32, &'static str> {
                if x == 25_000 {
                    panic!("try boom");
                }
                Ok(x + 1)
            })
            .try_collect();
    }));
    assert!(result.is_err());
    let payload = result.unwrap_err();
    assert_eq!(*payload.downcast_ref::<&'static str>().unwrap(), "try boom");
}
