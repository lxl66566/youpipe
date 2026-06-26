#[test]
fn test_scope_basic() {
    let multiplier = 3i32;
    let result = youpipe::scope(|s| {
        let items = vec![1, 2, 3, 4, 5];
        s.pipeline(items).map(|x: i32| x * multiplier).collect()
    });
    assert_eq!(result, vec![3, 6, 9, 12, 15]);
}

#[test]
fn test_scope_par_map_non_static() {
    let offset = 100i32;
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = (0..50).collect();
        s.pipeline(items).par_map(|x: i32| x + offset, 4).collect()
    });
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r, (100..150).collect::<Vec<_>>());
}

#[test]
fn test_scope_chained_map() {
    let a = 10i32;
    let b = 5i32;
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = (0..10).collect();
        s.pipeline(items)
            .map(|x: i32| x * a)
            .map(|x: i32| x + b)
            .collect()
    });
    let expected: Vec<i32> = (0..10).map(|x| x * 10 + 5).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_empty() {
    let result = youpipe::scope(|s| {
        let items: Vec<i32> = vec![];
        s.pipeline(items).map(|x: i32| x * 2).collect()
    });
    assert!(result.is_empty());
}

#[test]
fn test_scope_mutate_external() {
    let data: Vec<i32> = vec![10, 20, 30];
    let doubled: Vec<i32> = youpipe::scope(|s| {
        let indices: Vec<usize> = (0..3).collect();
        s.pipeline(indices).map(|i: usize| data[i] * 2).collect()
    });
    assert_eq!(doubled, vec![20, 40, 60]);
}

#[test]
fn test_scope_large() {
    let factor = 2u64;
    let result = youpipe::scope(|s| {
        let items: Vec<u64> = (0..1000).collect();
        s.pipeline(items).par_map(|x: u64| x * factor, 4).collect()
    });
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r.len(), 1000);
    assert_eq!(r[0], 0);
    assert_eq!(r[999], 1998);
}
