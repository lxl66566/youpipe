//! Expand-heavy microbenchmarks: fan-out × per-item cost × API shape.
//!
//! Purpose (docs/todo #4, closed): quantify the per-item `Vec` allocation
//! cost of the owned-buffer `expand` API against the push-style
//! `expand_emit` (per-worker reused scratch buffer). rayon's `flat_map`
//! (owned `Vec`) and `flat_map_iter` (lazy iterator) rows give the external
//! anchors — youpipe's push-style API is the structural counterpart of
//! `flat_map_iter`, the owned API of `flat_map`.
//!
//! Matrix: fan-out ∈ {4, 64} × cost ∈ {cheap, cpu} at one size anchor
//! (10 K inputs). Throughput counts **output** elements so the fan-out axis
//! compares equal work per row.

mod common;

use std::hint::black_box as bb;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use youpipe::stream;

const SIZE: usize = 10_000;

fn cheap(x: u64) -> u64 {
    x.wrapping_mul(3).wrapping_add(1)
}

fn cpu(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..100 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

/// The streaming engine takes ownership (no borrowed entry), so each
/// iteration rebuilds the input in the (untimed) setup and pulls it into
/// cache — a cold clone would measure allocator/memcpy latency instead of
/// the framework (same rationale as `async_vs_tokio.rs`).
fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

fn bench_expand_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("expand_heavy");
    let data: Vec<u64> = (0..SIZE as u64).collect::<Vec<u64>>();

    for fanout in [4u64, 64] {
        for (cost_label, f) in [("cheap", cheap as fn(u64) -> u64), ("cpu", cpu)] {
            group.throughput(Throughput::Elements(SIZE as u64 * fanout));

            group.bench_with_input(
                BenchmarkId::new(format!("owned_vec/fanout={fanout}/cost={cost_label}"), SIZE),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r: Vec<u64> = stream(v)
                                .expand(move |x| {
                                    (0..fanout)
                                        .map(|i| f(x.wrapping_add(i)))
                                        .collect::<Vec<_>>()
                                })
                                .run();
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("push_emit/fanout={fanout}/cost={cost_label}"), SIZE),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r: Vec<u64> = stream(v)
                                .expand_emit(move |x, out: &mut Vec<u64>| {
                                    for i in 0..fanout {
                                        out.push(f(x.wrapping_add(i)));
                                    }
                                })
                                .run();
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new(
                    format!("rayon_flat_map/fanout={fanout}/cost={cost_label}"),
                    SIZE,
                ),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r: Vec<u64> = v
                                .par_iter()
                                .flat_map(move |x| {
                                    (0..fanout)
                                        .map(|i| f(x.wrapping_add(i)))
                                        .collect::<Vec<_>>()
                                })
                                .collect();
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new(
                    format!("rayon_flat_map_iter/fanout={fanout}/cost={cost_label}"),
                    SIZE,
                ),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r: Vec<u64> = v
                                .par_iter()
                                .flat_map_iter(|&x| (0..fanout).map(move |i| f(x.wrapping_add(i))))
                                .collect();
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_expand_heavy
}
criterion_main!(benches);
