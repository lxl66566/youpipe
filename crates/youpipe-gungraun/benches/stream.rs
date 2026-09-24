//! Deterministic streaming-engine benches: instruction counts for the
//! `stream()` data plane on a pinned 4-worker pool.
//!
//! * `single_stage` — one plain `.stage()`: exercises the fused pass-through
//!   (pure-sync chains execute on the fused core, no channels).
//! * `two_stage` — two `.stage()`s: adds one bounded inter-stage channel
//!   handoff per item on top.
//!
//! Counting caliber: all threads, process total (see `common/mod.rs`); the
//! sequential anchor runs the same `both_pools()` setup so the fixed offset
//! cancels in the `compare_by_id` ratio. Async stages (`stage_async`) are
//! deliberately absent: the tokio runtime's timers make instruction counts
//! time- and scheduling-dependent (see Readme.md).
//!
//! ```sh
//! cargo bench -p youpipe-gungraun --bench stream
//! ```

// gungraun's macros emit `::`-rooted paths into the generated harness;
// the allow keeps that harmless if this crate ever inherits a workspace
// `unused_qualifications` lint.
#![allow(unused_qualifications)]

mod common;

use std::hint::black_box;

use gungraun::prelude::*;
use youpipe::prelude::*;
use youpipe_gungraun::{cpu_work, data};

type Pools = (youpipe::ComputePool, rayon::ThreadPool);

fn yp_single(pools: Pools, data: Vec<u64>) -> usize {
    let (pool, _rayon) = pools;
    let r: Vec<u64> = data
        .stream()
        .with_compute_pool(pool)
        .stage(|x: u64| black_box(cpu_work(x)))
        .run();
    black_box(r.len())
}

fn yp_two(pools: Pools, data: Vec<u64>) -> usize {
    let (pool, _rayon) = pools;
    let r: Vec<u64> = data
        .stream()
        .with_compute_pool(pool)
        .stage(|x: u64| black_box(cpu_work(x)))
        .stage(|x: u64| black_box(x.wrapping_add(1)))
        .run();
    black_box(r.len())
}

fn sq(pools: Pools, data: Vec<u64>, two: bool) -> usize {
    drop(pools);
    let r: Vec<u64> = if two {
        data.iter()
            .map(|&x| black_box(black_box(cpu_work(x)).wrapping_add(1)))
            .collect()
    } else {
        data.iter().map(|&x| black_box(cpu_work(x))).collect()
    };
    black_box(r.len())
}

#[library_benchmark]
#[bench::size_10k(youpipe_gungraun::both_pools(), data(10_000))]
fn yp_single_stage(pools: Pools, data: Vec<u64>) -> usize {
    yp_single(pools, data)
}

#[library_benchmark]
#[bench::size_10k(youpipe_gungraun::both_pools(), data(10_000))]
fn sq_single_stage(pools: Pools, data: Vec<u64>) -> usize {
    sq(pools, data, false)
}

library_benchmark_group!(
    name = stream_single_stage,
    compare_by_id = true,
    benchmarks = [yp_single_stage, sq_single_stage]
);

#[library_benchmark]
#[bench::size_10k(youpipe_gungraun::both_pools(), data(10_000))]
fn yp_two_stage(pools: Pools, data: Vec<u64>) -> usize {
    yp_two(pools, data)
}

#[library_benchmark]
#[bench::size_10k(youpipe_gungraun::both_pools(), data(10_000))]
fn sq_two_stage(pools: Pools, data: Vec<u64>) -> usize {
    sq(pools, data, true)
}

library_benchmark_group!(
    name = stream_two_stage,
    compare_by_id = true,
    benchmarks = [yp_two_stage, sq_two_stage]
);

// Invoked at item position: the macro expands to the `fn main` itself.
main!(
    config = common::count_all_threads(),
    library_benchmark_groups = [stream_single_stage, stream_two_stage]
);
