//! Adjacent-sync-stage fusion A/B (evidence bench for todo "streaming 相邻
//! sync stage 融合").
//!
//! Any non-sync link (expand / fence / async / cancel) forces the whole chain
//! onto the streaming topology, where adjacent `SyncStage`s each get their own
//! worker population joined by a channel hop. This bench measures what
//! manually composing adjacent sync closures into one stage would save:
//!
//! * `async_*` — `stage(f1).stage(f2).stage_async(g)` vs `stage(f2 ∘ f1).stage_async(g)` (3
//!   populations / 2 hops vs 2 / 1);
//! * `fence_*` — same but the non-sync link is `fence(Chunked(500))` + a trailing sync stage, so
//!   the *sync prefix is the workload* (the async tail of `async_*` is per-item ~1 µs and would
//!   mask the hop cost);
//! * `cancel_*` — `stage(f1).stage(f2).with_cancel(tok)` vs composed: pure sync chain forced onto
//!   streaming by the cancel token;
//! * `quad_*` — four sync stages (cancel shape) to show run-length scaling;
//! * `*_cheap` — pass-through closures: the infrastructure-dominated regime.
//!
//! Both sides are ONE binary: `YOUPIPE_SYNC_FUSE_VARIANT=split|merged`
//! selects which ids register (unset = both, for plain inspection runs).
//! Drive verdicts with the same-binary knob A/B so recompile layout noise
//! cannot leak in (see docs/src/dev/benchmarks.md). Per-id isolation needs
//! one filter per shape×size (a single broad filter degrades `-1` to one
//! combined pass):
//!
//! ```sh
//! perf/bench-suite/bench_ab.sh -a split=wt -b merged=wt \
//!     -E split=YOUPIPE_SYNC_FUSE_VARIANT=split \
//!     -E merged=YOUPIPE_SYNC_FUSE_VARIANT=merged \
//!     -B sync_fuse -r 5 -1 'cancel_pair_cpu_.*/100000' \
//!     'cancel_pair_cpu_.*/1000' 'fence_pair_cpu_.*/100000' ...
//! ```

mod common;

use std::{hint::black_box as bb, num::NonZeroUsize};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use youpipe::{CancellationToken, FenceMode, TokioPool, stream};

/// Per-item CPU cost of each sync stage, aligned with `mixed_load`'s
/// `youpipe_stream_cpu` anchor (50 mul-add rounds ≈ tens of ns).
const CPU_ITERS: u32 = 50;

fn cpu_work(x: u64) -> u64 {
    let mut r = x;
    for _ in 0..CPU_ITERS {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

fn bump(x: u64) -> u64 {
    bb(x.wrapping_add(1))
}

fn warm_clone(src: &[u64]) -> Vec<u64> {
    let v: Vec<u64> = src.to_vec();
    let mut acc = 0u64;
    for x in &v {
        acc = acc.wrapping_add(*x);
    }
    bb(acc);
    v
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Split,
    Merged,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Merged => "merged",
        }
    }
}

fn registered_sides() -> Vec<Side> {
    match std::env::var("YOUPIPE_SYNC_FUSE_VARIANT").as_deref() {
        Ok("split") => vec![Side::Split],
        Ok("merged") => vec![Side::Merged],
        _ => vec![Side::Split, Side::Merged],
    }
}

fn bench_sync_fuse(c: &mut Criterion) {
    // One async runtime for the whole bench process (rebuilt per `run()` it
    // would add ms-scale tokio construction noise to every iteration).
    let pool = TokioPool::build(num_cpus()).expect("async runtime");
    let pool_handle = pool.handle().clone();
    // Never cancelled: `with_cancel` only forces the streaming topology
    // (fused pass-through declines), it must not change the work done.
    let cancel = CancellationToken::new();
    let chunked = FenceMode::Chunked(NonZeroUsize::new(500).unwrap());

    let mut group = c.benchmark_group("sync_fuse");
    for size in [1_000usize, 100_000] {
        let data: Vec<u64> = (0..size as u64).collect();
        group.throughput(Throughput::Elements(size as u64));

        for side in registered_sides() {
            // async_pair_cpu: sync-sync-async (3 populations, 2 hops) vs the
            // manually composed closure (2 populations, 1 hop).
            group.bench_with_input(
                BenchmarkId::new(format!("async_pair_cpu_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .with_async_pool(TokioPool::new(pool_handle.clone()))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage_async(|x: u64| async move { x.wrapping_add(1) })
                                    .run(),
                                Side::Merged => stream(v)
                                    .with_async_pool(TokioPool::new(pool_handle.clone()))
                                    .stage(|x: u64| bb(cpu_work(cpu_work(x))))
                                    .stage_async(|x: u64| async move { x.wrapping_add(1) })
                                    .run(),
                            };
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            // fence_pair_cpu: the sync prefix is the workload — the fence
            // (forwarder thread) and the trailing stage are identical on both
            // sides, so the delta isolates the inter-sync hop + population.
            group.bench_with_input(
                BenchmarkId::new(format!("fence_pair_cpu_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .fence(chunked)
                                    .stage(bump)
                                    .run(),
                                Side::Merged => stream(v)
                                    .stage(|x: u64| bb(cpu_work(cpu_work(x))))
                                    .fence(chunked)
                                    .stage(bump)
                                    .run(),
                            };
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            // cancel_pair_cpu: pure sync chain forced onto the streaming
            // topology by the cancel token (fused pass-through declines).
            group.bench_with_input(
                BenchmarkId::new(format!("cancel_pair_cpu_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .run(),
                                Side::Merged => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(|x: u64| bb(cpu_work(cpu_work(x))))
                                    .run(),
                            };
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            // cancel_pair_cheap / fence_pair_cheap: pass-through closures —
            // the infrastructure-dominated regime where the inter-sync hop is
            // the largest cost fraction.
            group.bench_with_input(
                BenchmarkId::new(format!("cancel_pair_cheap_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(bump)
                                    .stage(bump)
                                    .run(),
                                Side::Merged => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(|x: u64| bump(bump(x)))
                                    .run(),
                            };
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("fence_pair_cheap_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .stage(bump)
                                    .stage(bump)
                                    .fence(chunked)
                                    .stage(bump)
                                    .run(),
                                Side::Merged => stream(v)
                                    .stage(|x: u64| bump(bump(x)))
                                    .fence(chunked)
                                    .stage(bump)
                                    .run(),
                            };
                            bb(r)
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            // fence_infra: pass-through stages around a Chunked(500) fence,
            // no CPU work at all — a control anchor (identical on both
            // sides, like mixed_load's rayon_par_iter). It exists because
            // the step-1 A/B uncovered a bistable convoy pathology in this
            // shape (~2.3 µs/item, 226 ms @100K in the probe, vs ~0.36 µs
            // for the 3-stage fence chain): the canary keeps that mode
            // observable for whoever fixes it (see dev/dead-ends.md and
            // todo).
            group.bench_with_input(BenchmarkId::new("fence_infra", size), &data, |b, data| {
                b.iter_batched(
                    || warm_clone(data),
                    |v| {
                        let r = stream(v).stage(bump).fence(chunked).stage(bump).run();
                        bb(r)
                    },
                    BatchSize::PerIteration,
                );
            });

            // cancel_quad_cpu: four sync stages — how the saving scales with
            // the length of the adjacent sync run (5 populations / 4 hops vs
            // 2 / 1 in split vs merged... with the feeder: 5 vs 2 worker
            // populations total).
            group.bench_with_input(
                BenchmarkId::new(format!("cancel_quad_cpu_{}", side.label()), size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || warm_clone(data),
                        |v| {
                            let r = match side {
                                Side::Split => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .stage(|x: u64| bb(cpu_work(x)))
                                    .run(),
                                Side::Merged => stream(v)
                                    .with_cancel(cancel.clone())
                                    .stage(|x: u64| bb(cpu_work(cpu_work(cpu_work(cpu_work(x))))))
                                    .run(),
                            };
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
    targets = bench_sync_fuse
}
criterion_main!(benches);
