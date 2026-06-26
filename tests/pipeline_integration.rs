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
    // Chain an infallible map after a try_map: error type stays the same.
    let r: Result<Vec<String>, &str> = pipe(0..5)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x * 2) })
        .map(|x: i32| x.to_string())
        .try_collect();
    assert_eq!(r.unwrap(), vec!["0", "2", "4", "6", "8"]);
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
