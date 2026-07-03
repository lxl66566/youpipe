use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[test]
fn test_scope_basic() {
    let multiplier = 3i32;
    let result = youpipe::scope(|s| {
        s.pipe([1, 2, 3, 4, 5])
            .map(|x: i32| x * multiplier)
            .collect()
    });
    assert_eq!(result, vec![3, 6, 9, 12, 15]);
}

#[test]
fn test_scope_par_via_collect() {
    // The new ScopedPipe uses the recursive work-stealing core, so
    // large inputs parallelise automatically without an explicit `par_map`.
    let offset = 100i32;
    let result = youpipe::scope(|s| s.pipe(0..50i32).map(|x: i32| x + offset).collect());
    let expected: Vec<i32> = (100..150).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_chained_map() {
    let a = 10i32;
    let b = 5i32;
    let result = youpipe::scope(|s| {
        s.pipe(0..10i32)
            .map(|x: i32| x * a)
            .map(|x: i32| x + b)
            .collect()
    });
    let expected: Vec<i32> = (0..10).map(|x| x * 10 + 5).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_empty() {
    let result = youpipe::scope(|s| s.pipe(Vec::<i32>::new()).map(|x: i32| x * 2).collect());
    assert!(result.is_empty());
}

#[test]
fn test_scope_mutate_external() {
    let data: Vec<i32> = vec![10, 20, 30];
    let doubled: Vec<i32> =
        youpipe::scope(|s| s.pipe(0..3i32).map(|i: i32| data[i as usize] * 2).collect());
    assert_eq!(doubled, vec![20, 40, 60]);
}

#[test]
fn test_scope_large() {
    let factor = 2u64;
    let result = youpipe::scope(|s| s.pipe(0..1000u64).map(|x: u64| x * factor).collect());
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r.len(), 1000);
    assert_eq!(r[0], 0);
    assert_eq!(r[999], 1998);
}

/// The headline scope feature: borrow a *non-Copy, non-`'static`* value from
/// outside the scope and use it inside the parallel closures. The previous
/// `T: 'static` bound made this impossible.
#[test]
fn test_scope_truly_non_static_borrow() {
    let cached: Vec<String> = (0..5).map(|i| format!("val{i}^2={}", i * i)).collect();
    let expected: Vec<usize> = cached.iter().map(String::len).collect();
    let result: Vec<usize> = youpipe::scope(|s| {
        s.pipe(0..cached.len())
            .map(|i: usize| cached[i].len())
            .collect()
    });
    assert_eq!(result, expected);
}

/// Multiple pipelines in the same `scope` sharing a stack-local lookup table —
/// the practical pattern `scope` unlocks. Without scope you'd need to `Arc`
/// the table or `clone()` it per pipeline.
#[test]
fn test_scope_shared_lookup_across_pipelines() {
    let table: Vec<u64> = (0..1000)
        .map(|i| (i as u64).wrapping_mul(2654435761))
        .collect();
    let (hits, sum) = youpipe::scope(|s| {
        let hits: usize = s
            .pipe(0..table.len())
            .map(|i: usize| if table[i] % 2 == 0 { 1usize } else { 0 })
            .collect()
            .into_iter()
            .sum();
        let sum: u64 = s
            .pipe(0..table.len())
            .map(|i: usize| table[i])
            .collect()
            .into_iter()
            .sum();
        (hits, sum)
    });
    let expected_sum: u64 = (0..1000).map(|i| (i as u64).wrapping_mul(2654435761)).sum();
    let expected_hits: usize = (0..1000)
        .map(|i| (i as u64).wrapping_mul(2654435761))
        .filter(|v| v % 2 == 0)
        .count();
    assert_eq!(hits, expected_hits);
    assert_eq!(sum, expected_sum);
}

// ── ScopedPipe::for_each — side-effect terminal without an output buffer ──

#[test]
fn test_scope_for_each_borrows_local() {
    // The headline pattern: for_each borrows stack-local state for its side
    // effect, with no output Vec allocated.
    let table: Vec<i32> = (0..50).collect();
    let sum = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let s = sum.clone();
    youpipe::scope(|scope| {
        scope.pipe(0..table.len()).for_each(move |i: usize| {
            s.fetch_add(table[i], Ordering::Relaxed);
        });
    });
    assert_eq!(sum.load(Ordering::Relaxed), (0..50).sum::<i32>());
}

#[test]
fn test_scope_for_each_chained() {
    let factor = 3i32;
    let offset = 100i32;
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
    let c = collected.clone();
    youpipe::scope(|scope| {
        scope
            .pipe(0..20i32)
            .map(|x: i32| x * factor)
            .map(|x: i32| x + offset)
            .for_each(move |x: i32| c.lock().unwrap().push(x));
    });
    let mut got = Arc::try_unwrap(collected).unwrap().into_inner().unwrap();
    got.sort_unstable();
    assert_eq!(got, (0..20).map(|x| x * 3 + 100).collect::<Vec<_>>());
}

#[test]
fn test_scope_for_each_filter() {
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    youpipe::scope(|scope| {
        scope
            .pipe(0..1000i32)
            .filter(|x: &i32| x % 5 == 0)
            .for_each(move |_| {
                c.fetch_add(1, Ordering::Relaxed);
            });
    });
    assert_eq!(
        counter.load(Ordering::Relaxed),
        (0..1000).filter(|x| x % 5 == 0).count()
    );
}

#[test]
fn test_scope_for_each_empty() {
    let called = AtomicBool::new(false);
    youpipe::scope(|scope| {
        scope
            .pipe(Vec::<i32>::new())
            .for_each(|_: i32| called.store(true, Ordering::Relaxed));
    });
    assert!(!called.load(Ordering::Relaxed));
}

#[test]
#[cfg_attr(miri, ignore)] // 80k items too slow under miri; the parallel
// for_each path is miri-validated by smaller tests.
fn test_scope_for_each_parallel_large() {
    // Large enough to force the parallel par_for_each path.
    let n: u64 = 80_000;
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    youpipe::scope(|scope| {
        scope.pipe(0..n).for_each(move |_: u64| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    });
    assert_eq!(counter.load(Ordering::Relaxed), n as usize);
}

// ── ScopedPipe::pipe(&[T]) — borrow a slice without cloning T ──
// `s.pipe(&slice)` yields `ScopedPipe<_, &T, &T>` via `&[T]: IntoIterator`,
// the youpipe counterpart of rayon's `slice.par_iter()`.

#[test]
fn test_scope_pipe_borrows_slice() {
    // The rayon-par_iter counterpart: iterate a borrowed slice, no clone of T.
    let files: Vec<String> = (0..50).map(|i| format!("row-{i}")).collect();
    let total_len = Arc::new(AtomicUsize::new(0));
    let t = total_len.clone();
    youpipe::scope(|scope| {
        scope.pipe(&files).for_each(move |s: &String| {
            t.fetch_add(s.len(), Ordering::Relaxed);
        });
    });
    assert_eq!(
        total_len.load(Ordering::Relaxed),
        files.iter().map(String::len).sum::<usize>()
    );
}

#[test]
fn test_scope_pipe_borrow_chained_map_filter() {
    let data: Vec<i32> = (0..100).collect();
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
    let c = collected.clone();
    youpipe::scope(|scope| {
        scope
            .pipe(&data)
            .filter(|x: &&i32| **x % 3 == 0)
            .map(|x: &i32| *x * 10)
            .for_each(move |x: i32| c.lock().unwrap().push(x));
    });
    let mut got = Arc::try_unwrap(collected).unwrap().into_inner().unwrap();
    got.sort_unstable();
    assert_eq!(
        got,
        (0..100)
            .filter(|x| x % 3 == 0)
            .map(|x| x * 10)
            .collect::<Vec<_>>()
    );
}

#[test]
#[cfg_attr(miri, ignore)] // 100k items too slow under miri; borrow path is
// identical to the owned for_each path (already
// miri-validated), only `T = &U`.
fn test_scope_pipe_borrow_parallel_large() {
    // Large slice → parallel par_for_each path with borrowed items.
    let data: Vec<u64> = (0..100_000).collect();
    let sum = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let s = sum.clone();
    youpipe::scope(|scope| {
        scope.pipe(&data).for_each(move |v: &u64| {
            s.fetch_add(*v, Ordering::Relaxed);
        });
    });
    assert_eq!(sum.load(Ordering::Relaxed), (0..100_000u64).sum::<u64>());
}

#[test]
fn test_scope_pipe_borrow_empty() {
    let data: Vec<i32> = vec![];
    let called = AtomicBool::new(false);
    youpipe::scope(|scope| {
        scope
            .pipe(&data)
            .for_each(|_: &i32| called.store(true, Ordering::Relaxed));
    });
    assert!(!called.load(Ordering::Relaxed));
}

#[test]
fn test_scope_pipe_borrow_no_clone_of_non_copy() {
    // Prove no clone happens: the borrowed refs point at the original slice
    // memory. We compare addresses (as `usize`, since raw pointers are
    // `!Send + !Sync` and cannot live inside a `Sync` closure). If the slice
    // were cloned, the observed addresses would not match the originals.
    let data: Vec<String> = (0..10).map(|i| format!("v{i}")).collect();
    let data_ptrs: Vec<usize> = data.iter().map(|s| s.as_ptr() as usize).collect();

    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let o = observed.clone();
    youpipe::scope(|scope| {
        scope.pipe(&data).for_each(move |s: &String| {
            o.lock().unwrap().push(s.as_ptr() as usize);
        });
    });

    let mut got = Arc::try_unwrap(observed).unwrap().into_inner().unwrap();
    got.sort_unstable();
    let mut want = data_ptrs.clone();
    want.sort_unstable();
    assert_eq!(got, want, "pipe(&slice) must borrow — addresses must match");
}

#[test]
#[cfg_attr(miri, ignore)] // 50k-item panic test too slow under miri; the
// owned for_each panic path is miri-validated by
// the smaller pipeline_integration equivalent.
fn test_scope_for_each_panic_propagates_parallel() {
    struct DropCounter {
        counter: Arc<AtomicUsize>,
        val: i32,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let items: Vec<DropCounter> = (0..50_000)
        .map(|i| DropCounter {
            counter: c.clone(),
            val: i,
        })
        .collect();
    let total = items.len();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        youpipe::scope(|scope| {
            scope.pipe(items).for_each(|d: DropCounter| {
                if d.val == 25_000 {
                    panic!("scoped for_each boom");
                }
            });
        });
    }));
    assert!(result.is_err());
    assert_eq!(
        counter.load(Ordering::Relaxed),
        total,
        "every scoped input item must be dropped exactly once on panic"
    );
}
