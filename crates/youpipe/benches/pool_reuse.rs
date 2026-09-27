//! Per-run cost of the transient pools behind `with_compute_workers(n)` /
//! `with_oversubscribe(f)` versus a pre-created `with_compute_pool` handle.
//!
//! Every fused terminal with a non-default worker budget builds and tears
//! down an `ExecPool::Owned` pool inside the timed region; this group makes
//! that ~ms cost visible against the same run on a pre-created pool, across
//! small/medium/large inputs (pool share shrinks as the batch grows).
//!
//! Variants per size (all on the same cheap per-item op so the framework +
//! pool lifecycle share dominates at small n):
//!   * `transient_workers8`  — `.with_compute_workers(8)` per call
//!   * `transient_oversub2`  — `.with_oversubscribe(2)` per call (64 threads on a 32-core host:
//!     worst case)
//!   * `prebuilt_pool8`      — one `ComputePool::new(8)`, reused via clone
//!   * `global_default`      — no worker override: the process-global pool
//!
//! Criterion's warm-up covers the first-build JIT/page-fault effects; the
//! pool-recycling cache (see `ComputePool::new` docs) makes the transient
//! variants converge onto `prebuilt_pool8` when enabled.
//!
//! NOTE: this bench's own `prebuilt_pool8` setup pool participates in the
//! recycling cache (same worker count as `transient_workers8`) — identical
//! registries, so the two variants differ only in the builder call.

mod common;

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::ComputePool;

/// One LCG step: ~1 ns per item. The regime where per-run pool construction
/// dominates the terminal call — the case `with_compute_workers` users in
/// tight loops actually hit.
fn cheap(x: u64) -> u64 {
    x.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

fn bench_pool_reuse(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_reuse");
    for size in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let pool8 = ComputePool::new(8);

        group.bench_with_input(
            BenchmarkId::new("transient_workers8", size),
            &size,
            |b, &n| {
                b.iter(|| {
                    let r: Vec<u64> = youpipe::pipe(0..n as u64)
                        .map(|x| black_box(cheap(x)))
                        .with_compute_workers(8)
                        .collect();
                    black_box(r);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("transient_oversub2", size),
            &size,
            |b, &n| {
                b.iter(|| {
                    let r: Vec<u64> = youpipe::pipe(0..n as u64)
                        .map(|x| black_box(cheap(x)))
                        .with_oversubscribe(2)
                        .collect();
                    black_box(r);
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("prebuilt_pool8", size), &size, |b, &n| {
            b.iter(|| {
                let r: Vec<u64> = youpipe::pipe(0..n as u64)
                    .map(|x| black_box(cheap(x)))
                    .with_compute_pool(pool8.clone())
                    .collect();
                black_box(r);
            });
        });

        group.bench_with_input(BenchmarkId::new("global_default", size), &size, |b, &n| {
            b.iter(|| {
                let r: Vec<u64> = youpipe::pipe(0..n as u64)
                    .map(|x| black_box(cheap(x)))
                    .collect();
                black_box(r);
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_pool_reuse
}
criterion_main!(benches);
