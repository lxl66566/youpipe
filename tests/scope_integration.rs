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
