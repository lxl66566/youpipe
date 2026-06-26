//! Fence boundaries: `.fence(mode)` controls exactly one adjacent stage
//! transition, never the whole stream. Each call is an independent boundary.
//!
//! This example demonstrates three things:
//!
//! 1. **Two independent fences in one chain.** A fence between stage 1↔2 and
//!    another between stage 2↔3 — they don't interfere.
//! 2. **`FenceMode::Chunked(k)` overlaps stages.** Stage 2 starts consuming the
//!    moment the first batch of `k` items clears the fence, long before stage 1
//!    finishes. Visible via per-stage "first item seen" timestamps.
//! 3. **`FenceMode::Barrier` is a hard cut.** Stage 2 sees nothing until stage
//!    1 is fully drained.
//!
//! ```text
//! cargo run --example fence
//! ```

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use youpipe::prelude::*;

/// Per-stage observation: when did this stage first/last see an item?
#[derive(Default, Clone)]
struct StageTrace {
    /// `None` until the first item arrives, then the elapsed since `run()`
    /// start.
    first_seen: Arc<Mutex<Option<Duration>>>,
    /// Updated on every item; the final value is when the stage went idle.
    last_seen: Arc<Mutex<Option<Duration>>>,
}

impl StageTrace {
    fn new() -> Self {
        Self {
            first_seen: Arc::new(Mutex::new(None)),
            last_seen: Arc::new(Mutex::new(None)),
        }
    }

    /// Record `elapsed`. First call sets `first_seen`; every call refreshes
    /// `last_seen`.
    fn mark(&self, elapsed: Duration) {
        let mut first = self.first_seen.lock().unwrap();
        if first.is_none() {
            *first = Some(elapsed);
        }
        *self.last_seen.lock().unwrap() = Some(elapsed);
    }

    fn first_seen(&self) -> Option<Duration> {
        *self.first_seen.lock().unwrap()
    }

    fn last_seen(&self) -> Option<Duration> {
        *self.last_seen.lock().unwrap()
    }
}

/// Stage 1: variable-cost CPU work, ~10 % slow. Heavy enough that the full
/// 1000-item pass takes tens of ms — comfortably above the fence batch time
/// so the Chunked vs Barrier contrast shows up clearly in the timestamps.
fn stage1(x: i32) -> i32 {
    let mut r = x as u64;
    let iters = if x % 10 == 0 { 2_000_000 } else { 2_000 };
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r as i32
}

/// Stage 2: light transform.
fn stage2(x: i32) -> i32 {
    x.wrapping_mul(3)
}

/// Stage 3: light transform.
fn stage3(x: i32) -> i32 {
    x.wrapping_add(1)
}

const SIZE: i32 = 1_000;

fn run_with(mode: FenceMode) -> (Vec<i32>, [StageTrace; 3]) {
    let items: Vec<i32> = (0..SIZE).collect();
    let traces = [StageTrace::new(), StageTrace::new(), StageTrace::new()];

    // Wrap each stage so it records the first-arrival time. The original
    // computation runs unchanged inside the wrapper. Each stage closure owns
    // its own clone of the trace handles (`Arc`-backed, cheap).
    let t0 = Instant::now();
    let trace0 = traces[0].clone();
    let trace1 = traces[1].clone();
    let trace2 = traces[2].clone();

    // Two fences: between stage 1↔2 and stage 2↔3 — independent boundaries.
    let result = items
        .stream()
        .stage(move |x: i32| {
            trace0.mark(t0.elapsed());
            stage1(x)
        })
        .fence(mode)
        .stage(move |x: i32| {
            trace1.mark(t0.elapsed());
            stage2(x)
        })
        .fence(mode)
        .stage(move |x: i32| {
            trace2.mark(t0.elapsed());
            stage3(x)
        })
        .ordered()
        .run();

    (result, traces)
}

fn main() {
    let chunk_k = NonZeroUsize::new(64).unwrap();

    // ── Chunked: stages should overlap ──
    let (mut chunked_result, chunked_traces) = run_with(FenceMode::Chunked(chunk_k));
    chunked_result.sort_unstable();
    let mut expected: Vec<i32> = (0..SIZE).map(stage1).map(stage2).map(stage3).collect();
    expected.sort_unstable();
    assert_eq!(chunked_result, expected);

    // ── Barrier: hard cut between every adjacent pair ──
    let (mut barrier_result, barrier_traces) = run_with(FenceMode::Barrier);
    barrier_result.sort_unstable();
    assert_eq!(barrier_result, expected);

    let stage_labels = ["stage 1", "stage 2", "stage 3"];
    println!("3-stage chain with two independent fences, {SIZE} items (sorted outputs agree)");
    println!();

    let fmt = |d: Option<Duration>| d.map_or("?".to_string(), |d| format!("{d:>9.3?}"));

    println!("  FenceMode::Chunked(k={chunk_k}):");
    for (i, label) in stage_labels.iter().enumerate() {
        let t = &chunked_traces[i];
        println!(
            "    {label}: first={:>10}  last={:>10}  after run() start",
            fmt(t.first_seen()),
            fmt(t.last_seen())
        );
    }
    // The headline contrast: in chunked mode stage 2 starts consuming before
    // stage 1 finishes — the two overlap.
    let s1_last = chunked_traces[0].last_seen();
    let s2_first = chunked_traces[1].first_seen();
    if let (Some(s1_last), Some(s2_first)) = (s1_last, s2_first) {
        let overlap = s1_last.saturating_sub(s2_first);
        println!("    → stage 2 started {overlap:>9.3?} before stage 1 finished (overlap).");
    }

    println!();
    println!("  FenceMode::Barrier:");
    for (i, label) in stage_labels.iter().enumerate() {
        let t = &barrier_traces[i];
        println!(
            "    {label}: first={:>10}  last={:>10}  after run() start",
            fmt(t.first_seen()),
            fmt(t.last_seen())
        );
    }
    let s1_last = barrier_traces[0].last_seen();
    let s2_first = barrier_traces[1].first_seen();
    if let (Some(s1_last), Some(s2_first)) = (s1_last, s2_first) {
        let gap = s2_first.saturating_sub(s1_last);
        println!("    → stage 2 waited {gap:>9.3?} after stage 1 finished (hard barrier).");
    }
    println!();
    println!("Each `.fence()` gates exactly one adjacent stage boundary. The");
    println!("chain above has two independent fences (1↔2 and 2↔3); neither");
    println!("spans the whole stream. Add as many as the topology needs.");
}
