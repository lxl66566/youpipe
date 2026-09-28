//! Sharded terminal fan-in A/B (evidence bench for todo #1).
//!
//! Terminal topology under test: N terminal-stage workers → one output
//! channel → the sole collector. `YOUPIPE_SHARDED_TERM=0/1` selects the
//! channel shape (one shared MPSC ring vs one SPSC ring per worker) inside
//! the SAME binary — drive verdicts with the runtime-knob A/B so recompile
//! layout noise cannot leak in:
//!
//! ```sh
//! perf/bench-suite/bench_ab.sh -a off=wt -b on=wt \
//!     -E off=YOUPIPE_SHARDED_TERM=0 -E on=YOUPIPE_SHARDED_TERM=1 \
//!     -B sharded_term -r 5 --per-id 'sharded_term/.*'
//! ```
//!
//! Every shape is forced onto the streaming topology by an inert
//! `with_cancel` token — pure sync chains otherwise take the fused
//! pass-through and never build a terminal channel at all.
//!
//! * `single_*` — one stage, the direct todo #1 shape (hotpath: engine paced at ~365 ns/item by the
//!   collector-side data plane);
//! * `multi2_*` — two stages (terminal fan-in behind one mid channel);
//! * `workers2_*` — `StageOptions::workers(2)` small-terminal shape, the guard against per-shard
//!   fixed cost regressing low-worker pipelines;
//! * `expand_*` — expand terminal (per-shard multi-output).
//!
//! `cheap` (x+1) is the primary read: with near-zero per-item CPU the
//! channel handoff dominates and the fan-in shape is directly visible;
//! `cpu` (50 mul-adds) shows whether the effect survives realistic stages.

mod common;

use std::hint::black_box as bb;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::{CancellationToken, StageOptions, stream};

/// Per-item CPU cost aligned with `mixed_load`'s `youpipe_stream_cpu` anchor
/// (50 mul-add rounds ≈ tens of ns).
fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..50 {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn bump(x: u64) -> u64 {
    bb(x.wrapping_add(1))
}

/// See `mixed_load::warm_clone` — the streaming engine takes ownership, so
/// each iteration rebuilds the input in the untimed setup and pulls it into
/// cache (a cold clone measures allocator/NT-store latency instead).
fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

fn bench_sharded_term(c: &mut Criterion) {
    // Never cancelled: `with_cancel` only forces the streaming topology
    // (fused pass-through declines), it must not change the work done.
    let cancel = CancellationToken::new();
    let mut group = c.benchmark_group("sharded_term");
    for size in [1_000usize, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("single_unordered_cheap", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v).with_cancel(cancel.clone()).stage(bump).run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("single_unordered_cpu", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v)
                            .with_cancel(cancel.clone())
                            .stage(|x: u64| bb(cpu_work(x)))
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("single_ordered_cpu", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v)
                            .with_cancel(cancel.clone())
                            .stage(|x: u64| bb(cpu_work(x)))
                            .ordered()
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("multi2_cpu", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .with_cancel(cancel.clone())
                        .stage(|x: u64| bb(cpu_work(x)))
                        .stage(bump)
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("workers2_cpu", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .with_cancel(cancel.clone())
                        .stage_with(StageOptions::new().workers(2), |x: u64| bb(cpu_work(x)))
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("expand_cheap", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .with_cancel(cancel.clone())
                        .expand_emit(|x: u64, out: &mut Vec<u64>| {
                            out.push(x);
                            out.push(x.wrapping_add(1));
                        })
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion();
    targets = bench_sharded_term
}
criterion_main!(benches);
