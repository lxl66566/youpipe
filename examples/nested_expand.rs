//! 1-to-N expansion (flatMap-style): youpipe `stream().expand().stage()` vs
//! rayon's `flat_map` and std's `flat_map`.
//!
//! `.expand(f)` lets a stage produce a `Vec<N>` per input item — useful for
//! tokenising, log expansion, generating candidates, etc. Each expanded item
//! inherits the parent's sequence tag so `.ordered()` restores per-input
//! grouping in the output.
//!
//! ```text
//! cargo run --example nested_expand
//! ```

use std::time::Instant;

use rayon::prelude::*;

use youpipe::stream;

const N_INPUTS: usize = 1000;

/// Expand each `i` into a vec of 5 integers: `[i*10, i*10+1, ..., i*10+4]`.
fn expand(i: i32) -> Vec<i32> {
    let base = i * 10;
    vec![base, base + 1, base + 2, base + 3, base + 4]
}

/// Light per-item work to keep the focus on the expansion + collection cost.
fn postprocess(x: i32) -> i32 {
    x.wrapping_mul(3)
}

fn main() {
    let inputs: Vec<i32> = (0..N_INPUTS as i32).collect();

    // ── youpipe: stream().expand().stage().run() ──
    // Unordered — items arrive in completion order. (Ordered mode assumes a
    // 1-to-1 input↔output mapping for its sequence-tag reorder buffer; expand
    // produces 1-to-N which doesn't fit that model.)
    let yp_start = Instant::now();
    let mut yp_result = stream(inputs.clone())
        .expand(|i: i32| expand(i))
        .stage(|x: i32| postprocess(x))
        .run();
    yp_result.sort_unstable();
    let yp_elapsed = yp_start.elapsed();

    // ── rayon: par_iter().flat_map().map().collect() ──
    let rn_start = Instant::now();
    let rn_result: Vec<i32> = inputs
        .par_iter()
        .flat_map(|&i| expand(i))
        .map(postprocess)
        .collect();
    let rn_elapsed = rn_start.elapsed();

    // ── std sequential ──
    let seq_start = Instant::now();
    let seq_result: Vec<i32> = inputs.iter().flat_map(|&i| expand(i)).map(postprocess).collect();
    let seq_elapsed = seq_start.elapsed();

    // Correctness: rayon and std produce the same ordered output. youpipe's
    // unordered output, sorted, must equal the sequential output.
    yp_result.sort_unstable();
    assert_eq!(yp_result, seq_result);
    assert_eq!(rn_result, seq_result);

    println!(
        "1-to-5 expand + postprocess, {N_INPUTS} inputs → {} items",
        yp_result.len()
    );
    println!(
        "  youpipe stream().expand().stage():  {:>10.3?}",
        yp_elapsed
    );
    println!(
        "  rayon   par_iter().flat_map().map(): {:>10.3?}",
        rn_elapsed
    );
    println!(
        "  std     iter().flat_map().map():    {:>10.3?}",
        seq_elapsed
    );
    println!();
    println!("First few outputs: {:?}", &yp_result[..10]);
}
