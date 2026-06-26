#[test]
fn test_scope_basic() {
    let multiplier = 3i32;
    let result = youpipe::scope(|s| {
        let items = vec![1, 2, 3, 4, 5];
        s.pipeline().map(|x: i32| x * multiplier).collect(items)
    });
    assert_eq!(result, vec![3, 6, 9, 12, 15]);
}

#[test]
fn test_scope_par_via_collect() {
    // The new ScopedPipeline uses the recursive work-stealing core, so
    // large inputs parallelise automatically without an explicit `par_map`.
    let offset = 100i32;
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = (0..50).collect();
        s.pipeline().map(|x: i32| x + offset).collect(items)
    });
    let expected: Vec<i32> = (100..150).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_chained_map() {
    let a = 10i32;
    let b = 5i32;
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = (0..10).collect();
        s.pipeline()
            .map(|x: i32| x * a)
            .map(|x: i32| x + b)
            .collect(items)
    });
    let expected: Vec<i32> = (0..10).map(|x| x * 10 + 5).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_empty() {
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = vec![];
        s.pipeline().map(|x: i32| x * 2).collect(items)
    });
    assert!(result.is_empty());
}

#[test]
fn test_scope_mutate_external() {
    let data: Vec<i32> = vec![10, 20, 30];
    let doubled: Vec<i32> = youpipe::scope(|s| {
        let items: Vec<i32> = vec![0, 1, 2];
        s.pipeline()
            .map(|i: i32| data[i as usize] * 2)
            .collect(items)
    });
    assert_eq!(doubled, vec![20, 40, 60]);
}

#[test]
fn test_scope_large() {
    let factor = 2u64;
    let result = youpipe::scope(|s| {
        let items: Vec<u64> = (0..1000).collect();
        s.pipeline().map(|x: u64| x * factor).collect(items)
    });
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
        let items: Vec<usize> = (0..cached.len()).collect();
        s.pipeline().map(|i: usize| cached[i].len()).collect(items)
    });
    assert_eq!(result, expected);
}
