//! Compile-time fusion: youpipe `pipe` vs rayon `par_iter`, side-by-side.
//!
//! youpipe's `.map().filter().map()` chain compiles to a single monomorphized
//! closure per worker — there is no intermediate `Vec` between stages, no
//! virtual dispatch, no per-stage allocator traffic. rayon's `par_iter()` has
//! the same property for its iterator-adapter chain; this example shows the
//! two are equivalent in both API shape and per-item throughput.
//!
//! ```text
//! cargo run --example fused_vs_rayon
//! ```

use rayon::prelude::*;
use youpipe::prelude::*;

const SIZE: usize = 1_000_000;

/// Three-stage CPU chain — light enough that the framework overhead
/// (split / steal / collect) dominates, which is exactly where fusion
/// matters. Heavier per-item work hides framework differences.
fn cpu_chain(x: u64) -> u64 {
    let a = x.wrapping_add(1);
    let b = a.wrapping_mul(3);
    b.wrapping_sub(2)
}

fn main() {
    let data: Vec<u64> = (0..SIZE as u64).collect();

    // ── youpipe: fused pipe().map().map().map().collect() ──
    let yp_start = std::time::Instant::now();
    let yp_result: Vec<u64> = data
        .clone()
        .pipe()
        .map(|x| x.wrapping_add(1))
        .map(|x| x.wrapping_mul(3))
        .map(|x| x.wrapping_sub(2))
        .collect();
    let yp_elapsed = yp_start.elapsed();

    // ── rayon: equivalent par_iter().map().map().map().collect() ──
    let rn_start = std::time::Instant::now();
    let rn_result: Vec<u64> = data
        .par_iter()
        .map(|&x| x.wrapping_add(1))
        .map(|x| x.wrapping_mul(3))
        .map(|x| x.wrapping_sub(2))
        .collect();
    let rn_elapsed = rn_start.elapsed();

    // ── sequential iterator: the lower bound (no parallelism overhead) ──
    let seq_start = std::time::Instant::now();
    let seq_result: Vec<u64> = data.iter().map(|&x| cpu_chain(x)).collect();
    let seq_elapsed = seq_start.elapsed();

    // Correctness: all three must agree.
    assert_eq!(yp_result, rn_result);
    assert_eq!(yp_result, seq_result);

    println!(
        "3-stage CPU chain over {SIZE} items (all results agree, first={} last={})",
        yp_result[0],
        yp_result[SIZE - 1]
    );
    println!(
        "  youpipe  pipe():  {:>10.3?}   (fused, single closure per worker)",
        yp_elapsed
    );
    println!(
        "  rayon    par_iter: {:>10.3?}   (fused, single closure per worker)",
        rn_elapsed
    );
    println!(
        "  std      iter():   {:>10.3?}   (single-threaded baseline)",
        seq_elapsed
    );
    println!();
    println!("Both youpipe and rayon fuse the chain at compile time — the");
    println!("difference vs the sequential iterator is the parallel speedup.");
    println!("youpipe uses the same recursive join + work-stealing strategy");
    println!("as rayon, so per-item throughput should be in the same league.");
}
