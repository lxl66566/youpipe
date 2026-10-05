//! Async sharded terminal fan-in A/B (evidence bench for todo #1 residual
//! (c)): the async terminal's `io_concurrency` consumer tasks → one output
//! channel → the sole async collector. `YOUPIPE_SHARDED_TERM=0/1` selects
//! the channel shape (one shared MPSC async ring vs one async shard ring
//! per task group, shards capped at the runtime's worker threads) inside
//! the SAME binary — drive verdicts with the runtime-knob A/B so recompile
//! layout noise cannot leak in:
//!
//! ```sh
//! perf/bench-suite/bench_ab.sh -a off=wt -b on=wt \
//!     -E off=YOUPIPE_SHARDED_TERM=0 -E on=YOUPIPE_SHARDED_TERM=1 \
//!     -B sharded_term_async -r 5 --per-id \
//!     sharded_term_async/async0_cheap sharded_term_async/async1_cheap \
//!     sharded_term_async/async1_cpu sharded_term_async/async1_ordered_cpu
//! ```
//!
//! Ids must be listed explicitly: `--per-id` iterates the filter LIST, so a
//! single `'sharded_term_async/.*'` fragment silently degrades to ONE
//! combined criterion pass per round (found when re-running this bench —
//! the combined and true per-id halves agreed, so the verdict held; the
//! command above is the strict form).
//! Shape selection avoids the known bistable convoy cells (todo #4,
//! streaming.md "Convoy collapse forensics"): two sync prefixes feeding
//! `stage_async` flip between fast and collapsed modes and would drown the
//! terminal signal. The immune references are used instead:
//!
//! * `async1_*` — ONE sync prefix (cheap or cpu) into `stage_async` with an instant-return future:
//!   the terminal is the async stage's output ring with 128 hot producer tasks — the direct todo #1
//!   (c) shape;
//! * `async1_ordered_*` — ordered variant (ReorderBuffer downstream of the shards);
//! * `async0_*` — async-only chain (mixed-mode feeder channel consumed directly):
//!   feeder-throughput-limited reference, expected knob-insensitive (control for leakage into
//!   non-terminal channels).
//!
//! `cheap` (x+1) is the channel-dominated read; `cpu` (50 mul-adds inside
//! the future) shows whether the effect survives realistic stages.

mod common;

use std::hint::black_box as bb;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::{CancellationToken, stream};

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

fn bench_sharded_term_async(c: &mut Criterion) {
    // Never cancelled: `with_cancel` only forces the sync prefix off the
    // fused pass-through, it must not change the work done.
    let cancel = CancellationToken::new();
    let mut group = c.benchmark_group("sharded_term_async");
    for size in [1_000usize, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("async1_cheap", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .with_cancel(cancel.clone())
                        .stage(bump)
                        .stage_async(|x: u64| async move { bb(x.wrapping_add(1)) })
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("async1_cpu", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .with_cancel(cancel.clone())
                        .stage(|x: u64| bb(cpu_work(x)))
                        .stage_async(|x: u64| async move { bb(cpu_work(x)) })
                        .run();
                    bb(r)
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("async1_ordered_cpu", size),
            &data,
            |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v)
                            .with_cancel(cancel.clone())
                            .stage(|x: u64| bb(cpu_work(x)))
                            .stage_async(|x: u64| async move { bb(cpu_work(x)) })
                            .ordered()
                            .run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("async0_cheap", size), &data, |b, data| {
            b.iter_batched(
                || warm_clone(data),
                |v| {
                    let r = stream(v)
                        .stage_async(|x: u64| async move { bb(x.wrapping_add(1)) })
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
    targets = bench_sharded_term_async
}
criterion_main!(benches);
