//! Multi-stage streaming pipeline: youpipe `stream().stage().stage()` vs
//! tokio's blocking-task pool, on a chain of CPU stages.
//!
//! The streaming API is data-first: items live inside the `StreamPipe` builder
//! and `.run()` triggers execution. Stages are chained via `.stage(f)` and
//! connected by lock-free channels — output of stage 1 streams into stage 2
//! as soon as items are ready, letting both stages progress in parallel.
//!
//! ```text
//! cargo run --example stream_chain
//! ```

use std::hint::black_box as bb;

use youpipe::stream;

/// Two-stage CPU chain — compute, then post-process. Both stages are CPU-bound.
fn stage1(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn stage2(x: u64) -> u64 {
    x.wrapping_mul(31).wrapping_add(7)
}

const SIZE: usize = 10_000;

fn main() {
    let data: Vec<u64> = (0..SIZE as u64).collect();

    // ── youpipe: stream().stage(s1).stage(s2).run() ──
    let yp_start = std::time::Instant::now();
    let yp_result = stream(data.clone())
        .stage(|x: u64| bb(stage1(x)))
        .stage(|x: u64| bb(stage2(x)))
        .run();
    let yp_elapsed = yp_start.elapsed();

    // ── tokio: spawn_blocking for each stage, joined at the end ──
    let tokio_start = std::time::Instant::now();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_result: Vec<u64> = rt.block_on(async {
        // Stage 1.
        let mut handles = Vec::with_capacity(data.len());
        for x in &data {
            let x = *x;
            handles.push(tokio::task::spawn_blocking(move || bb(stage1(x))));
        }
        let mut mid = Vec::with_capacity(handles.len());
        for h in handles {
            mid.push(h.await.unwrap());
        }
        // Stage 2.
        let mut handles = Vec::with_capacity(mid.len());
        for x in mid {
            handles.push(tokio::task::spawn_blocking(move || bb(stage2(x))));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    });
    let tokio_elapsed = tokio_start.elapsed();

    // Correctness: both must produce identical output (order is irrelevant
    // for these pure functions — we sort to compare).
    let mut yp_sorted = yp_result;
    yp_sorted.sort_unstable();
    let mut tokio_sorted = tokio_result;
    tokio_sorted.sort_unstable();
    assert_eq!(yp_sorted, tokio_sorted);

    println!(
        "2-stage CPU pipeline over {SIZE} items (sorted outputs agree, {} items)",
        yp_sorted.len()
    );
    println!(
        "  youpipe  stream(): {:>10.3?}   (channels between stages, overlapping)",
        yp_elapsed
    );
    println!(
        "  tokio   spawn_blocking: {:>10.3?}   (one task per item, per stage)",
        tokio_elapsed
    );
    println!();
    println!("youpipe's streaming pipeline keeps both stages' worker pools");
    println!("running concurrently — stage 2 starts consuming the moment stage");
    println!("1 emits its first item. tokio's spawn_blocking serialises the two");
    println!("stages: every stage-1 task must finish before any stage-2 task is");
    println!("spawned.");
}
