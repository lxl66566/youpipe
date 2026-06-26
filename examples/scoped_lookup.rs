//! The headline `scope` feature: borrow a *non-`'static`* stack-local value
//! from every parallel worker without cloning or `Arc`-ing it.
//!
//! The pattern is a parallel lookup against a table that lives on the calling
//! thread's stack. Without `scope`, each closure would need an owned copy of
//! the table (clone or `Arc<Vec<_>>`) because `Send + 'static` requires no
//! borrowed data. With `scope`, the closures borrow `&table` by shared
//! reference for the duration of the scope — verified by the borrow checker,
//! enforced by `ComputePool::join` blocking the calling thread until every
//! sub-task finishes.
//!
//! ```text
//! cargo run --example scoped_lookup
//! ```

use std::time::Instant;

use rayon::prelude::*;
use youpipe::prelude::*;

const TABLE_SIZE: usize = 200_000;
const LOOKUPS: usize = 1_000_000;

/// Build a non-`Copy`, non-`'static` lookup table. `String` forces a heap
/// allocation per entry — without `scope`, cloning this table into every
/// worker would dominate the runtime.
fn build_table() -> Vec<String> {
    (0..TABLE_SIZE)
        .map(|i| format!("row-{i}-payload-{i:#x}"))
        .collect()
}

/// The work each item performs: a hashed lookup into the table, returning the
/// length of the matched row.
fn lookup(table: &[String], i: usize) -> usize {
    // Hash the index to spread load around the table.
    let h = (i.wrapping_mul(2654435761)) % table.len();
    table[h].len()
}

fn main() {
    let table = build_table();
    let lookup_indices: Vec<usize> = (0..LOOKUPS).collect();

    // ── youpipe scope: borrow `&table` in every parallel closure ──
    let yp_start = Instant::now();
    let yp_lengths: Vec<usize> = scope(|s| {
        s.pipe(lookup_indices.iter().copied())
            .map(|i: usize| lookup(&table, i))
            .collect()
    });
    let yp_elapsed = yp_start.elapsed();
    let yp_total: usize = yp_lengths.iter().sum();

    // ── rayon baseline: par_iter also supports non-'static borrowing via
    // its own scoped threads. This is a fair head-to-head. ──
    let rn_start = Instant::now();
    let rn_lengths: Vec<usize> = lookup_indices
        .par_iter()
        .map(|&i| lookup(&table, i))
        .collect();
    let rn_elapsed = rn_start.elapsed();
    let rn_total: usize = rn_lengths.iter().sum();

    // ── sequential baseline: same work, single thread, same borrow ──
    let seq_start = Instant::now();
    let seq_lengths: Vec<usize> = lookup_indices.iter().map(|&i| lookup(&table, i)).collect();
    let seq_elapsed = seq_start.elapsed();
    let seq_total: usize = seq_lengths.iter().sum();

    assert_eq!(yp_total, rn_total);
    assert_eq!(yp_total, seq_total);
    let checksum = yp_total;

    println!("Parallel lookup against {TABLE_SIZE}-row stack-local table, {LOOKUPS} lookups");
    println!("  checksum (all agree): {checksum}");
    println!(
        "  youpipe scope():  {:>10.3?}   (borrows &table, no clone)",
        yp_elapsed
    );
    println!(
        "  rayon   par_iter: {:>10.3?}   (borrows &table, no clone)",
        rn_elapsed
    );
    println!(
        "  std     iter():   {:>10.3?}   (single-threaded baseline)",
        seq_elapsed
    );
    println!();
    println!("Both youpipe and rayon let the parallel workers borrow a");
    println!("stack-local table. Without `scope`, you would have to either");
    println!("`Arc<Vec<_>>` the table (atomic refcount overhead) or `clone()`");
    println!("it per worker (200k string clones — milliseconds of pure waste).");
}
