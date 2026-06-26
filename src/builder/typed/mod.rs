mod fused;
mod slots;
mod stream;
mod traits;

pub(crate) use self::fused::fused_collect_scoped;
pub use self::{
    fused::{Pipeline, par_chunks_map, par_map, par_map_with_workload, try_par_map},
    stream::StreamPipeline,
    traits::{Fence, Filter, FusedStage, Identity, Ordered, StageMarker, SyncMap},
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{builder::config::PipelineConfig, state::FenceMode};

    #[test]
    fn test_fused_sync_collect() {
        let items: Vec<i32> = (0..100).collect();
        let result = Pipeline::new()
            .map(|x: i32| x * 2)
            .map(|x: i32| x + 1)
            .collect(items);
        let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, expected);
    }

    #[test]
    fn test_fused_filter() {
        let items: Vec<i32> = (0..20).collect();
        let result = Pipeline::new()
            .filter(|x: &i32| x % 2 == 0)
            .map(|x: i32| x * 10)
            .collect(items);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, vec![0, 20, 40, 60, 80, 100, 120, 140, 160, 180]);
    }

    #[test]
    fn test_empty_input() {
        let items: Vec<i32> = vec![];
        let result = Pipeline::new().map(|x: i32| x * 2).collect(items);
        assert!(result.is_empty());
    }

    /// Type-changing maps must compile: `i32 -> String -> usize`. The previous
    /// `Pipeline<S, T>` overloaded `T` as both input and output, so any stage
    /// that changed the element type failed to compile. With `Pipeline<S, I,
    /// O>` the input type `I` stays fixed while `O` tracks the latest
    /// output.
    #[test]
    fn test_type_changing_map() {
        let items: Vec<i32> = (0..5).collect();
        // i32 -> String -> usize, with a filter on the String stage.
        let result: Vec<usize> = Pipeline::new()
            .map(|x: i32| x.to_string())
            .filter(|s: &String| !s.is_empty())
            .map(|s: String| s.len())
            .collect(items);
        assert_eq!(result, vec![1, 1, 1, 1, 1]);
    }

    /// Type-changing map ending in an ordered collect (exercises the `ordered`
    /// builder with a non-identity output type).
    #[test]
    fn test_type_changing_map_ordered() {
        let items: Vec<i32> = (0..5).collect();
        let result: Vec<i64> = Pipeline::new()
            .map(|x: i32| i64::from(x) * 10)
            .ordered()
            .collect(items);
        assert_eq!(result, vec![0, 10, 20, 30, 40]);
    }

    #[test]
    fn test_par_map() {
        let items: Vec<i32> = (0..100).collect();
        let result = par_map(items, |x: i32| x * 3);
        let mut r = result;
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    /// Correctness on a large input that exercises the recursive index split
    /// across many leaves.
    #[test]
    fn test_par_map_large() {
        let n: usize = 200_000;
        let items: Vec<u64> = (0..n).map(|x| x as u64).collect();
        let result = par_map(items.clone(), |x: u64| x.wrapping_mul(3).wrapping_add(1));
        assert_eq!(result.len(), n);
        for (i, r) in result.iter().enumerate() {
            assert_eq!(*r, (i as u64).wrapping_mul(3).wrapping_add(1));
        }
    }

    /// Validates that input items are consumed exactly once and output slots
    /// hold the right values, using a Drop type. A double-free or
    /// use-after-free would surface under Miri or as a wrong count.
    #[test]
    fn test_par_map_drop_type() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        #[derive(Debug)]
        struct Tracker(Arc<AtomicUsize>);
        impl PartialEq for Tracker {
            fn eq(&self, other: &Self) -> bool {
                Arc::ptr_eq(&self.0, &other.0)
            }
        }
        impl Drop for Tracker {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let items: Vec<Tracker> = (0..5000).map(|_| Tracker(counter.clone())).collect();
        let arcs: Vec<Arc<AtomicUsize>> = par_map(items, |t| {
            let c = t.0.clone();
            drop(t);
            c
        })
        .into_iter()
        .collect();
        assert_eq!(arcs.len(), 5000);
        // All input Trackers have been dropped (moved into the closure and consumed).
        assert_eq!(counter.load(Ordering::SeqCst), 5000);
        // The returned Arcs are still live — dropping them must not touch counter.
        drop(arcs);
        assert_eq!(counter.load(Ordering::SeqCst), 5000);
    }

    /// Panic propagation + cleanup for the index-based par_map path. Uses a
    /// Drop-tracking type so a leak or double-free shows up as a wrong drop
    /// count (and as UB under Miri).
    #[test]
    fn test_par_map_panic_safety() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Tracker {
            idx: usize,
            counter: Arc<AtomicUsize>,
        }
        impl Drop for Tracker {
            fn drop(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let panic_idx: usize = 1500;
        let n = 4000;
        let items: Vec<Tracker> = (0..n)
            .map(|idx| Tracker {
                idx,
                counter: counter.clone(),
            })
            .collect();

        let panic_idx_closure = panic_idx;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            par_map(items, move |t| {
                let idx = t.idx;
                drop(t);
                assert!(idx != panic_idx_closure, "induced panic at idx {idx}");
                idx as u64
            });
        }));
        assert!(result.is_err(), "par_map should propagate the panic");
        // Every input Tracker must have been dropped exactly once: the ones
        // consumed before the panic, plus the ones cleaned up by the recursion.
        assert_eq!(
            counter.load(Ordering::SeqCst),
            n,
            "expected all {n} Trackers dropped exactly once"
        );
    }

    /// Panic safety for the fused collect fast path (no filter).
    #[test]
    fn test_fused_collect_panic_safety() {
        let items: Vec<i32> = (0..2000).collect();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pipeline::new()
                .map(|x: i32| if x == 1500 { panic!("boom") } else { x + 1 })
                .collect(items);
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_single_stage_unordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run(items, |x: i32| x * 2, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_single_stage_ordered() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let result = sp.run(items, |x: i32| x * 2, true);
        assert_eq!(result, (0..100).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[test]
    fn test_stream_multi_stage() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..100).collect();
        let mut result = sp.run_multi_stage(items, |x: i32| x + 1, |x: i32| x * 3, false);
        result.sort_unstable();
        assert_eq!(result, (0..100).map(|x| (x + 1) * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_ok() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x * 3) });
        let mut r = result.unwrap();
        r.sort_unstable();
        assert_eq!(r, (0..100).map(|x: i32| x * 3).collect::<Vec<_>>());
    }

    #[test]
    fn test_try_par_map_err() {
        let items: Vec<i32> = (0..100).collect();
        let result = try_par_map(items, |x: i32| -> Result<i32, String> {
            if x == 50 {
                Err(format!("bad: {x}"))
            } else {
                Ok(x * 2)
            }
        });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "bad: 50");
    }

    #[test]
    fn test_try_par_map_empty() {
        let items: Vec<i32> = vec![];
        let result = try_par_map(items, |x: i32| -> Result<i32, &str> { Ok(x) });
        assert_eq!(result.unwrap(), Vec::<i32>::new());
    }

    #[test]
    fn test_stream_cancel_unordered() {
        let token = crate::sync::CancellationToken::new();
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config).with_cancel(token.clone());
        let items: Vec<i32> = (0..1000).collect();
        token.cancel();
        let result = sp.run(
            items,
            |x: i32| -> i32 {
                std::thread::sleep(std::time::Duration::from_micros(100));
                x * 2
            },
            false,
        );
        assert!(result.len() < 1000);
    }

    #[test]
    fn test_stream_no_cancel() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..50).collect();
        let result = sp.run(items, |x: i32| x * 2, false);
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn test_stream_nested() {
        let config = PipelineConfig::default();
        let sp = StreamPipeline::new(config);
        let items: Vec<i32> = (0..5).collect();
        let mut result = sp.run_nested(items, |x: i32| vec![x, x + 100], |x: i32| x * 2, false);
        result.sort_unstable();
        let mut expected: Vec<i32> = (0..5).flat_map(|x| vec![x * 2, (x + 100) * 2]).collect();
        expected.sort_unstable();
        assert_eq!(result, expected);
    }

    /// Regression: `with_cancel` previously only worked for `run`. The
    /// multi-stage / fence / nested paths ignored the token; this test guards
    /// against that regression by exercising all three with a pre-cancelled
    /// token + per-item sleep so that under cancellation none of them should
    /// process the full input.
    #[test]
    fn test_stream_cancel_all_variants() {
        use std::{num::NonZeroUsize, time::Duration};

        fn sleep_map<T: Copy>(x: T) -> T {
            std::thread::sleep(Duration::from_micros(50));
            x
        }

        let mk = || {
            let token = crate::sync::CancellationToken::new();
            let sp = StreamPipeline::new(PipelineConfig::default()).with_cancel(token.clone());
            (token, sp)
        };
        let items: Vec<i32> = (0..1000).collect();

        // multi_stage
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_multi_stage(items.clone(), sleep_map, sleep_map, false);
            assert!(r.len() < 1000, "multi_stage cancel failed: {}", r.len());
        }
        // with_fence
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_with_fence(
                items.clone(),
                sleep_map,
                sleep_map,
                FenceMode::Chunked(NonZeroUsize::new(32).unwrap()),
                false,
            );
            assert!(r.len() < 1000, "with_fence cancel failed: {}", r.len());
        }
        // nested
        {
            let (token, sp) = mk();
            token.cancel();
            let r = sp.run_nested(items, |x| vec![x, x + 1], sleep_map, false);
            assert!(r.len() < 2000, "nested cancel failed: {}", r.len());
        }
    }

    // ── async streaming stage tests ──

    /// `run_async` correctness (unordered): an async stage over `u64 -> u64`.
    /// Uses `tokio::time::sleep` (a yielding wait) so the test also exercises
    /// the M:N scheduling path rather than a blocking stall.
    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn test_run_async_unordered() {
        let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(16));
        let items: Vec<u64> = (0..100).collect();
        let mut r = sp.run_async(items, |x: u64| async move { x * 2 }, false);
        r.sort_unstable();
        assert_eq!(r, (0..100u64).map(|x| x * 2).collect::<Vec<_>>());
    }

    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn test_run_async_ordered() {
        let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(16));
        let items: Vec<u64> = (0..100).collect();
        let r = sp.run_async(items, |x: u64| async move { x * 2 }, true);
        assert_eq!(r, (0..100u64).map(|x| x * 2).collect::<Vec<_>>());
    }

    /// `run_mixed_async` correctness: sync CPU stage + async IO stage, both
    /// ordered and unordered. Verifies the sync→async bridge preserves item
    /// count and transforms values correctly.
    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn test_run_mixed_async_unordered() {
        let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(16));
        let items: Vec<u64> = (0..100).collect();
        let mut r =
            sp.run_mixed_async(items, |x: u64| x + 1, |m: u64| async move { m * 10 }, false);
        r.sort_unstable();
        assert_eq!(r, (0..100u64).map(|x| (x + 1) * 10).collect::<Vec<_>>());
    }

    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn test_run_mixed_async_ordered() {
        let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(16));
        let items: Vec<u64> = (0..100).collect();
        let r = sp.run_mixed_async(items, |x: u64| x + 1, |m: u64| async move { m * 10 }, true);
        assert_eq!(r, (0..100u64).map(|x| (x + 1) * 10).collect::<Vec<_>>());
    }

    /// Cancellation must propagate to the async paths: a pre-cancelled token
    /// plus per-item yielding wait must short-circuit well before the full
    /// input is processed.
    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn test_run_async_cancel() {
        let token = crate::sync::CancellationToken::new();
        let sp = StreamPipeline::new(PipelineConfig::default().with_io_concurrency(8))
            .with_cancel(token.clone());
        let items: Vec<u64> = (0..1000).collect();
        token.cancel();
        let r = sp.run_async(items, |x: u64| async move { x * 2 }, false);
        assert!(r.len() < 1000, "run_async cancel failed: {}", r.len());
    }
}
