//! Deterministic fused-engine benches: youpipe `pipe` vs rayon `par_iter` vs
//! sequential, counted as executed instructions under Callgrind.
//!
//! Counting caliber: all threads, process total (see `common/mod.rs` — the
//! default per-function toggle cannot see pool workers). Every row's setup
//! runs the same `both_pools()` work, so the fixed offset cancels in the
//! pairwise `compare_by_id` deltas.
//!
//! ```sh
//! cargo bench -p youpipe-gungraun --bench fused
//! cargo bench -p youpipe-gungraun --bench fused -- '*cpu_heavy*'
//! ```

// gungraun's macros emit `::`-rooted paths into the generated harness;
// the allow keeps that harmless if this crate ever inherits a workspace
// `unused_qualifications` lint.
#![allow(unused_qualifications)]

mod common;

use std::hint::black_box;

use gungraun::prelude::*;
use rayon::prelude::*;
use youpipe_gungraun::{cpu_work, data};

/// Pools handed to every row: youpipe pool + rayon pool, both pinned to 4
/// workers. Rows use one (or neither); the other is dropped at function
/// exit — identical work on every row, so setup+teardown cancels in deltas.
type Pools = (youpipe::ComputePool, rayon::ThreadPool);

fn yp_cpu_heavy(pools: Pools, data: Vec<u64>) -> usize {
    let (pool, _rayon) = pools;
    let r: Vec<u64> = youpipe::pipe(data)
        .with_compute_pool(pool)
        .map(|x| black_box(cpu_work(x)))
        .collect();
    black_box(r.len())
}

fn ry_cpu_heavy(pools: Pools, data: Vec<u64>) -> usize {
    let (_youpipe, pool) = pools;
    let r: Vec<u64> = pool.install(|| data.par_iter().map(|&x| black_box(cpu_work(x))).collect());
    black_box(r.len())
}

fn sq_cpu_heavy(pools: Pools, data: Vec<u64>) -> usize {
    drop(pools);
    let r: Vec<u64> = data.iter().map(|&x| black_box(cpu_work(x))).collect();
    black_box(r.len())
}

fn yp_light(pools: Pools, data: Vec<u64>) -> usize {
    let (pool, _rayon) = pools;
    let r: Vec<u64> = youpipe::pipe(data)
        .with_compute_pool(pool)
        .map(|x| black_box(x.wrapping_add(1)))
        .collect();
    black_box(r.len())
}

fn ry_light(pools: Pools, data: Vec<u64>) -> usize {
    let (_youpipe, pool) = pools;
    let r: Vec<u64> = pool.install(|| data.par_iter().map(|&x| black_box(x + 1)).collect());
    black_box(r.len())
}

fn sq_light(pools: Pools, data: Vec<u64>) -> usize {
    drop(pools);
    let r: Vec<u64> = data.iter().map(|&x| black_box(x + 1)).collect();
    black_box(r.len())
}

/// Emit one `(yp, ry, seq)` triple sharing the bench id `size_<n>`, so
/// `compare_by_id` pairs them. Function names must be unique items, hence the
/// explicit idents (same pattern as the zstdx-gungraun crates).
macro_rules! fused_rows {
    (
        $group:ident,
        $id:ident,
        $size:expr,
        $yp:ident,
        $ry:ident,
        $sq:ident,
        $f_yp:expr,
        $f_ry:expr,
        $f_sq:expr
    ) => {
        #[library_benchmark]
        #[bench::$id(youpipe_gungraun::both_pools(), data($size))]
        fn $yp(pools: Pools, data: Vec<u64>) -> usize {
            $f_yp(pools, data)
        }

        #[library_benchmark]
        #[bench::$id(youpipe_gungraun::both_pools(), data($size))]
        fn $ry(pools: Pools, data: Vec<u64>) -> usize {
            $f_ry(pools, data)
        }

        #[library_benchmark]
        #[bench::$id(youpipe_gungraun::both_pools(), data($size))]
        fn $sq(pools: Pools, data: Vec<u64>) -> usize {
            $f_sq(pools, data)
        }

        library_benchmark_group!(
            name = $group,
            compare_by_id = true,
            benchmarks = [$yp, $ry, $sq]
        );
    };
}

// Sizes mirror the criterion suite's anchors (1K setup-dominated / 100K
// steady-state for cpu_heavy; 10K hot-cache framework-overhead regime for
// light). 100K cpu_heavy ≈ 90M Ir under valgrind (~2 s); the light rows are
// dominated by dispatch instructions on purpose.
fused_rows!(
    cpu_heavy_1k,
    size_1k,
    1_000,
    yp_cpu_heavy_1k,
    ry_cpu_heavy_1k,
    sq_cpu_heavy_1k,
    yp_cpu_heavy,
    ry_cpu_heavy,
    sq_cpu_heavy
);
fused_rows!(
    cpu_heavy_100k,
    size_100k,
    100_000,
    yp_cpu_heavy_100k,
    ry_cpu_heavy_100k,
    sq_cpu_heavy_100k,
    yp_cpu_heavy,
    ry_cpu_heavy,
    sq_cpu_heavy
);
fused_rows!(
    light_10k,
    size_10k,
    10_000,
    yp_light_10k,
    ry_light_10k,
    sq_light_10k,
    yp_light,
    ry_light,
    sq_light
);
fused_rows!(
    light_100k,
    size_100k,
    100_000,
    yp_light_100k,
    ry_light_100k,
    sq_light_100k,
    yp_light,
    ry_light,
    sq_light
);

// Invoked at item position: the macro expands to the `fn main` itself. A
// `fn main() { main!(...) }` wrapper compiles but silently does nothing.
main!(
    config = common::count_all_threads(),
    library_benchmark_groups = [cpu_heavy_1k, cpu_heavy_100k, light_10k, light_100k]
);
