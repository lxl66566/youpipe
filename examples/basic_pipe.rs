//! Basic data-first parallel map — the simplest youpipe entry point.
//!
//! ```text
//! cargo run --example basic_pipe
//! ```

use youpipe::prelude::*;

fn main() {
    // `pipe` consumes any `IntoIterator` and returns a fused, chainable
    // pipeline. `.collect()` executes the chain on the work-stealing compute
    // pool and returns a `Vec`.
    let result: Vec<i32> = (0..10_000).pipe().map(|x| x * 2).collect();

    assert_eq!(result.len(), 10_000);
    assert_eq!(result[0], 0);
    assert_eq!(result[9999], 19_998);
    println!("pipe: processed {} items", result.len());

    // Chained stages fuse at compile time — no intermediate Vec between
    // map/filter steps, the way rayon's `par_iter().map().filter().collect()`
    // does.
    let filtered: Vec<i32> = (0..100)
        .pipe()
        .map(|x| x + 1)
        .filter(|x: &i32| x % 3 == 0)
        .map(|x| x * 10)
        .collect();

    let expected: Vec<i32> = (1..=100).filter(|x| x % 3 == 0).map(|x| x * 10).collect();
    assert_eq!(filtered, expected);
    println!("fused map+filter+map: {} items", filtered.len());
}
