//! Unbalanced workloads: youpipe's work-stealing pool vs naive parallelism.
//!
//! ~10 % of items do 1000× more work than the others. A naive "split into N
//! equal chunks, give one to each thread" strategy starves the threads that
//! drew the slow items. Work-stealing (via recursive `join` splitting + the
//! `st3` LIFO deque scheduler) lets idle workers steal sub-tasks from busy
//! ones, balancing the load dynamically.
//!
//! `.with_workload(Workload::Unbalanced)` raises the oversplit factor — more
//! smaller leaves so a slow item only blocks a small leaf, leaving the rest
//! stealable.
//!
//! ```text
//! cargo run --example unbalanced
//! ```

use std::time::Instant;

use rayon::prelude::*;
use youpipe::{Workload, pipe};

const SIZE: usize = 5_000;

/// Variable-cost CPU work. `iters` controls duration.
fn cpu_work(x: u64, iters: u32) -> u64 {
    let mut r = x;
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// ~10 % slow (5000 iters), ~90 % fast (5 iters) — a 1000× cost spread.
fn skewed(size: usize) -> Vec<(u64, u32)> {
    (0..size)
        .map(|i| {
            let iters = if i % 10 == 0 { 5000 } else { 5 };
            (i as u64, iters)
        })
        .collect()
}

fn main() {
    let items = skewed(SIZE);

    // ── youpipe: default Balanced (4× oversplit) ──
    let yp_balanced_start = Instant::now();
    let yp_balanced: Vec<u64> = pipe(items.clone())
        .map(|(x, iters)| cpu_work(x, iters))
        .collect();
    let yp_balanced_elapsed = yp_balanced_start.elapsed();

    // ── youpipe: Unbalanced (8× oversplit, finer task granularity) ──
    let yp_unbal_start = Instant::now();
    let yp_unbal: Vec<u64> = pipe(items.clone())
        .with_workload(Workload::Unbalanced)
        .map(|(x, iters)| cpu_work(x, iters))
        .collect();
    let yp_unbal_elapsed = yp_unbal_start.elapsed();

    // ── rayon: par_iter (work-stealing, ~comparable strategy) ──
    let rn_start = Instant::now();
    let rn: Vec<u64> = items
        .par_iter()
        .map(|&(x, iters)| cpu_work(x, iters))
        .collect();
    let rn_elapsed = rn_start.elapsed();

    // ── sequential iterator: the baseline ──
    let seq_start = Instant::now();
    let seq: Vec<u64> = items.iter().map(|&(x, iters)| cpu_work(x, iters)).collect();
    let seq_elapsed = seq_start.elapsed();

    // Correctness.
    let mut yp_b = yp_balanced;
    yp_b.sort_unstable();
    let mut yp_u = yp_unbal;
    yp_u.sort_unstable();
    let mut rn_s = rn;
    rn_s.sort_unstable();
    let mut seq_s = seq;
    seq_s.sort_unstable();
    assert_eq!(yp_b, yp_u);
    assert_eq!(yp_b, rn_s);
    assert_eq!(yp_b, seq_s);

    println!("Unbalanced CPU workload, {SIZE} items (~10% slow, 1000× cost spread)");
    println!(
        "  all sorted outputs agree, first={}, last={}",
        yp_b[0],
        yp_b[SIZE - 1]
    );
    println!(
        "  youpipe pipe() Balanced:    {:>10.3?}   (4× oversplit)",
        yp_balanced_elapsed
    );
    println!(
        "  youpipe pipe() Unbalanced:  {:>10.3?}   (8× oversplit, finer stealing)",
        yp_unbal_elapsed
    );
    println!(
        "  rayon   par_iter:           {:>10.3?}   (work-stealing baseline)",
        rn_elapsed
    );
    println!(
        "  std     iter():             {:>10.3?}   (single-threaded baseline)",
        seq_elapsed
    );
    println!();
    println!("Both youpipe and rayon use recursive `join`-based splitting that");
    println!("produces more leaves than threads. Idle workers steal unstarted");
    println!("leaves from busy workers — including the leaves that contain slow");
    println!("items. Without work-stealing, the threads unlucky enough to draw");
    println!("slow items would block while the rest sit idle.");
}
