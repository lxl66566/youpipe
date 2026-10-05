use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use youpipe::{FenceMode, Workload, pipe, pipe_range, stream};

fn cpu_heavy(x: u64) -> u64 {
    let mut r = x;
    // Miri: the arithmetic depth is irrelevant to memory safety — scale the
    // inner loop down ~50× so the ~30 tests using this helper stay fast
    // under the interpreter (same closure/dispatch paths per item).
    let iters: u32 = if cfg!(miri) {
        4
    } else {
        200
    };
    for _ in 0..iters {
        r = r.wrapping_mul(31).wrapping_add(17);
    }
    r
}

#[test]
fn test_par_map_correctness() {
    let items: Vec<u64> = (0..1000).collect();
    let result = pipe(items.clone()).map(|x: u64| cpu_heavy(x)).collect();
    let expected: Vec<u64> = items.iter().map(|&x| cpu_heavy(x)).collect();
    assert_eq!(result.len(), expected.len());
    let mut r = result;
    r.sort_unstable();
    let mut e = expected;
    e.sort_unstable();
    assert_eq!(r, e);
}

#[test]
fn test_par_map_empty() {
    let result = pipe(Vec::<u64>::new()).map(|x: u64| x + 1).collect();
    assert_eq!(result, [] as [u64; 0]);
}

#[test]
fn test_par_map_single() {
    let result = pipe(vec![42u64]).map(|x: u64| x + 1).collect();
    assert_eq!(result, vec![43]);
}

#[test]
fn test_pipeline_fusion_3_stages() {
    let items: Vec<i32> = (0..500).collect();
    let result = pipe(items)
        .map(|x: i32| x + 1)
        .map(|x: i32| x * 3)
        .map(|x: i32| x - 7)
        .collect();
    let expected: Vec<i32> = (0..500).map(|x| (x + 1) * 3 - 7).collect();
    let mut r = result;
    r.sort_unstable();
    let mut e = expected;
    e.sort_unstable();
    assert_eq!(r, e);
}

#[test]
fn test_pipeline_filter_map() {
    let items: Vec<i32> = (0..100).collect();
    let result = pipe(items)
        .filter(|x: &i32| x % 3 == 0)
        .map(|x: i32| x * 10)
        .collect();
    let expected: Vec<i32> = (0..100).filter(|x| x % 3 == 0).map(|x| x * 10).collect();
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r, expected);
}

/// A panicking owned filter chain must drop every input item exactly once:
/// the leaf's `FilterGuard` drops the unread tail on unwind, items already
/// moved through the chain drop with their (discarded) output `Vec`s, and the
/// panicking item is gone with the panic. The output is unordered, so the
/// range tree's per-chunk/leaf boundaries do not matter — only the accounting.
#[test]
fn test_owned_filter_panic_drop_accounting() {
    struct DropCounter {
        counter: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        500
    } else {
        20_000
    };
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let items: Vec<DropCounter> = (0..n)
        .map(|i| DropCounter {
            counter: c.clone(),
            val: i,
        })
        .collect();
    let panic_at = n / 3;

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let out: Vec<u64> = pipe(items)
            .filter(|d: &DropCounter| d.val % 2 == 0)
            .map(move |d: DropCounter| {
                assert!(d.val != panic_at, "boom");
                d.val
            })
            .collect();
        let _ = out;
    }));
    assert!(r.is_err(), "panic must propagate through the filter tree");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        usize::try_from(n).expect("item count fits usize"),
        "every input item must be dropped exactly once"
    );
}

/// `Err` short-circuit through the owned try+filter range tree
/// (`fused_try_filter_collect`): the failing leaf drops its unread input tail,
/// every other range is consumed, so all `n` inputs drop exactly once; the
/// outputs written before the failure (a scheduling-independent set when the
/// failing item is last) drop with the discarded partial `Vec`s.
#[allow(clippy::cast_possible_truncation)]
#[test]
fn test_try_filter_collect_err_drop_accounting() {
    struct InCounter {
        drops: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for InCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    struct OutCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OutCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        65_536
    };
    let in_drops = Arc::new(AtomicUsize::new(0));
    let out_drops = Arc::new(AtomicUsize::new(0));
    let (ic, oc) = (in_drops.clone(), out_drops.clone());
    let items: Vec<InCounter> = (0..n)
        .map(|i| InCounter {
            drops: ic.clone(),
            val: i,
        })
        .collect();

    let r: Result<Vec<OutCounter>, &str> = pipe(items)
        .try_map(move |d: InCounter| {
            // Fail on the LAST item: every earlier output is written
            // deterministically, regardless of leaf execution order.
            if d.val == n - 1 {
                Err("terminal failure")
            } else {
                Ok(d.val)
            }
        })
        .filter(|&v: &u64| v % 3 == 0)
        .map(move |_v: u64| OutCounter { drops: oc.clone() })
        .try_collect();
    assert!(r.is_err(), "the Err item must short-circuit the batch");

    assert_eq!(
        in_drops.load(Ordering::Relaxed),
        n as usize,
        "every input must drop exactly once on Err"
    );
    // Survivors of the filter among 0..n-1 (the failing item never outputs).
    let expected_out = (0..n - 1).filter(|v| v % 3 == 0).count();
    assert_eq!(
        out_drops.load(Ordering::Relaxed),
        expected_out,
        "every written output must drop exactly once on Err"
    );
}

/// Panic path through the same tree: the unwinding leaf's `FilterGuard`
/// drops its unread tail and `join` waits out the siblings (their ranges
/// consume fully), so all inputs drop exactly once. The output count is
/// scheduling-dependent — waited-out sibling subtrees may complete and drop
/// whole `Vec`s — so only the no-double-drop bound is asserted.
#[allow(clippy::cast_possible_truncation)]
#[test]
fn test_try_filter_collect_panic_drop_accounting() {
    struct InCounter {
        drops: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for InCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    struct OutCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OutCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        65_536
    };
    let in_drops = Arc::new(AtomicUsize::new(0));
    let out_drops = Arc::new(AtomicUsize::new(0));
    let (ic, oc) = (in_drops.clone(), out_drops.clone());
    let panic_at = n / 3;
    let items: Vec<InCounter> = (0..n)
        .map(|i| InCounter {
            drops: ic.clone(),
            val: i,
        })
        .collect();

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = pipe(items)
            .try_map(move |d: InCounter| {
                assert!(d.val != panic_at, "boom");
                Ok::<u64, &str>(d.val)
            })
            .filter(|&v: &u64| v % 3 == 0)
            .map(move |_v: u64| OutCounter { drops: oc.clone() })
            .try_collect();
    }));
    assert!(
        r.is_err(),
        "panic must propagate through the try filter tree"
    );

    assert_eq!(
        in_drops.load(Ordering::Relaxed),
        n as usize,
        "every input must drop exactly once on panic"
    );
    // Every written output drops exactly once: at most one output per
    // surviving item other than the panicking one (which never outputs).
    let max_out = (0..n).filter(|&v| v != panic_at && v % 3 == 0).count();
    let dropped = out_drops.load(Ordering::Relaxed);
    assert!(
        dropped <= max_out,
        "outputs must never double-drop (dropped {dropped} > {max_out})"
    );
}

// ── pipe_range: zero-materialization generation core ──

#[test]
fn test_pipe_range_collect_correctness() {
    // Non-zero start exercises the `base + i` item arithmetic; the size
    // forces the parallel generation path (not `prefers_serial`).
    let r: Vec<usize> = pipe_range(1_000_000..1_020_000)
        .map(|i: usize| i.wrapping_mul(3).wrapping_add(7))
        .collect();
    let expected: Vec<usize> = (1_000_000..1_020_000)
        .map(|i: usize| i.wrapping_mul(3).wrapping_add(7))
        .collect();
    assert_eq!(r, expected);
}

#[test]
fn test_pipe_range_type_changing_maps() {
    let r: Vec<u64> = pipe_range(0..4_000)
        .map(|i: usize| i as u64 + 1)
        .map(|x: u64| x * 10)
        .collect();
    assert_eq!(r.len(), 4_000);
    assert_eq!(r[0], 10);
    assert_eq!(r[3_999], 40_000);
}

#[test]
fn test_pipe_range_empty_and_singleton() {
    let r: Vec<usize> = pipe_range(0..0).map(|i: usize| i + 1).collect();
    assert_eq!(r, Vec::<usize>::new());
    let r: Vec<usize> = pipe_range(42..43).map(|i: usize| i + 1).collect();
    assert_eq!(r, vec![43]);
}

#[test]
fn test_pipe_range_serial_pool_arm() {
    // Single-worker pool takes `prefers_serial` — the inline serial arm.
    let r: Vec<usize> = pipe_range(0..10_000)
        .with_compute_workers(1)
        .map(|i: usize| i * 2)
        .collect();
    assert_eq!(r.len(), 10_000);
    assert_eq!(r[123], 246);
}

#[test]
fn test_pipe_range_filter_chain() {
    // Filter chains materialize the indices (documented fallback) —
    // correctness must match the plain `pipe(range)` result exactly.
    let r: Vec<usize> = pipe_range(0..10_000)
        .map(|i: usize| i + 1)
        .filter(|x: &usize| x % 3 == 0)
        .collect();
    let expected: Vec<usize> = (0..10_000).map(|i| i + 1).filter(|x| x % 3 == 0).collect();
    assert_eq!(r, expected);
}

#[test]
fn test_pipe_range_try_map_try_collect() {
    let r: Result<Vec<u64>, &str> = pipe_range(0..10_000)
        .map(|i: usize| i as u64)
        .try_map(|x: u64| {
            if x == 9_999 {
                Err("boom")
            } else {
                Ok(x + 1)
            }
        })
        .try_collect();
    assert_eq!(r.unwrap_err(), "boom");

    let ok: Vec<u64> = pipe_range(0..1_000)
        .map(|i: usize| i as u64)
        .try_map(|x: u64| Ok::<_, &str>(x + 1))
        .try_collect()
        .unwrap();
    assert_eq!(ok.len(), 1_000);
    assert_eq!(ok[999], 1_000);
}

#[test]
fn test_pipe_range_for_each() {
    use std::sync::{Arc, Mutex};
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    pipe_range(0..8_000)
        .map(|i: usize| i * 2)
        .for_each(move |x: usize| s.lock().unwrap().push(x));
    let mut seen = seen.lock().unwrap();
    seen.sort_unstable();
    let expected: Vec<usize> = (0..8_000).map(|i| i * 2).collect();
    assert_eq!(*seen, expected);
}

/// Panic through the generation core: every *constructed* output must be
/// dropped exactly once (leaf guard + sibling-cleanup accounting), even
/// though no input buffer exists to account for.
#[test]
fn test_pipe_range_panic_drop_accounting() {
    struct DropCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: usize = if cfg!(miri) {
        500
    } else {
        20_000
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let constructed = Arc::new(AtomicUsize::new(0));
    let (d, c) = (drops.clone(), constructed.clone());
    let panic_at = n / 3;

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let out: Vec<DropCounter> = pipe_range(0..n)
            .map(move |i: usize| {
                assert!(i != panic_at, "boom");
                c.fetch_add(1, Ordering::Relaxed);
                DropCounter { drops: d.clone() }
            })
            .collect();
        // Unreachable in practice (the map above panics at `panic_at`); if
        // scheduling somehow avoided it, `out`'s drop still keeps the
        // accounting below valid.
        drop(out);
    }));
    assert!(
        r.is_err(),
        "panic must propagate through the generation core"
    );
    assert_eq!(
        drops.load(Ordering::Relaxed),
        constructed.load(Ordering::Relaxed),
        "every constructed output must be dropped exactly once (no leak, no double drop)"
    );
}

#[test]
fn test_try_map_ok() {
    let result = pipe(0..100)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x * 3) })
        .try_collect()
        .unwrap();
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r, (0..100).map(|x| x * 3).collect::<Vec<_>>());
}

#[test]
fn test_try_map_err_short_circuits() {
    let result = pipe(0..100)
        .try_map(|x: i32| -> Result<i32, String> {
            if x == 50 {
                Err(format!("bad: {x}"))
            } else {
                Ok(x * 2)
            }
        })
        .try_collect();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "bad: 50");
}

#[test]
fn test_try_map_then_map() {
    // Chain an inffallible map after a try_map: error type stays the same.
    let r: Result<Vec<String>, &str> = pipe(0..5)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x * 2) })
        .map(|x: i32| x.to_string())
        .try_collect();
    assert_eq!(r.unwrap(), vec!["0", "2", "4", "6", "8"]);
}

#[test]
fn test_try_map_parallel_large() {
    // Large enough to exceed the serial threshold (num_threads * 64) and
    // exercise the index-based parallel fast path (MAY_FILTER == false).
    // Miri: threshold is num_threads*64 and miri's pool is 1 worker, so 2K
    // items still exceed it at ~40x.
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let result = pipe(0..n)
        .try_map(|x: i32| -> Result<i32, &str> { Ok(x.wrapping_mul(3)) })
        .map(|x: i32| x + 1)
        .try_collect()
        .unwrap();
    assert_eq!(result.len(), usize::try_from(n).unwrap());
    assert_eq!(result[0], 1);
    assert_eq!(result[usize::try_from(n).unwrap() - 1], (n - 1) * 3 + 1);
}

#[test]
fn test_try_map_parallel_error_short_circuits() {
    // Error in the parallel path (index-based fast path) must propagate.
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let err_at = n * 3 / 5;
    let result = pipe(0..n)
        .try_map(move |x: i32| -> Result<i32, String> {
            if x == err_at {
                Err("mid-batch error".into())
            } else {
                Ok(x * 2)
            }
        })
        .try_collect();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "mid-batch error");
}

#[test]
fn test_stream_single_ordered() {
    let items: Vec<i32> = (0..100).collect();
    let result = stream(items).stage(|x: i32| x * 2 + 1).ordered().run();
    let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_single_unordered() {
    let items: Vec<i32> = (0..100).collect();
    let mut result = stream(items).stage(|x: i32| x * 2 + 1).run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..100).map(|x| x * 2 + 1).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_multi_stage() {
    let items: Vec<i32> = (0..200).collect();
    let mut result = stream(items)
        .stage(|x: i32| x + 10)
        .stage(|x: i32| x * 2)
        .run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..200).map(|x| (x + 10) * 2).collect();
    assert_eq!(result, expected);
}

/// `.ordered()` must re-sequence to exact input order under every racy
/// combination the streaming topology allows: several stage workers, tiny
/// buffers (backpressure), fence links (barrier and chunked), both
/// terminals. Regression guard for the former "ordered output order
/// violation" fuzz finding — that finding turned out to be a harness
/// template-selection bug (an exact-order assertion on an unordered
/// pipeline), so this test pins the contract the library actually promises.
#[test]
fn test_stream_ordered_exact_order_stress() {
    fn xorshift(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }
    // Miri: same dispatch/drain paths per item, fewer rounds (see
    // `cpu_heavy` for the ~1000× interpreter rationale).
    let rounds: u64 = if cfg!(miri) {
        20
    } else {
        2000
    };
    let mut rng = 0x243f_6a88_85a3_08d3_u64;
    for round in 0..rounds {
        let n = 1 + xorshift(&mut rng) % 64;
        let items: Vec<u64> = (0..n).map(|_| xorshift(&mut rng)).collect();
        let (workers, buffer) = (
            usize::try_from(1 + xorshift(&mut rng) % 4).unwrap(),
            usize::try_from(1 + xorshift(&mut rng) % 8).unwrap(),
        );
        let (mul, add) = (xorshift(&mut rng) | 1, xorshift(&mut rng));
        let expect: Vec<u64> = items
            .iter()
            .map(|&x| x.wrapping_mul(mul).wrapping_add(add))
            .collect();
        let ctx = format!("round {round}: n={n} workers={workers} buffer={buffer}");
        let stage = move |x: u64| x.wrapping_mul(mul).wrapping_add(add);
        match round % 3 {
            // stage × 2 on the real topology (worker pin opts out of the
            // fused pass-through) + Vec terminal
            0 => {
                let got = stream(items)
                    .with_compute_workers(workers)
                    .with_buffer_size(buffer)
                    .stage(stage)
                    .stage(|x: u64| x)
                    .ordered()
                    .run();
                assert_eq!(got, expect, "{ctx}");
            },
            // same shape, for_each terminal (same ordered drain, sink
            // instead of Vec)
            1 => {
                let mut got = Vec::with_capacity(expect.len());
                stream(items)
                    .with_compute_workers(workers)
                    .with_buffer_size(buffer)
                    .stage(stage)
                    .stage(|x: u64| x)
                    .ordered()
                    .for_each(|x| got.push(x));
                assert_eq!(got, expect, "{ctx}");
            },
            // fence mid-chain: both fence modes release order differently,
            // the collector must not care
            _ => {
                let mode = if xorshift(&mut rng) & 1 == 0 {
                    FenceMode::Barrier
                } else {
                    FenceMode::Chunked(
                        NonZeroUsize::new(usize::try_from(1 + xorshift(&mut rng) % 4).unwrap())
                            .unwrap(),
                    )
                };
                let got = stream(items)
                    .with_compute_workers(workers)
                    .with_buffer_size(buffer)
                    .stage(stage)
                    .fence(mode)
                    .stage(|x: u64| x)
                    .ordered()
                    .run();
                assert_eq!(got, expect, "{ctx}");
            },
        }
    }
}

// ── Fused pass-through: pure SyncStage chains run on the fused core ──

/// A pure sync chain's pass-through output must equal both the sequential
/// composition and the equivalent fused `pipe().map().collect()` — across
/// the serial shortcut (n = 1), the hybrid-dispatch range (n = 10_000), and
/// `.ordered()` (whose input-order contract the pass-through satisfies
/// trivially).
#[test]
fn test_stream_fused_pass_through_equivalence() {
    let f = |x: u64| cpu_heavy(x).wrapping_add(1);
    let g = |x: u64| x.wrapping_mul(3);

    for n in [1, 2, 10_000] {
        let items: Vec<u64> = (0..n).collect();
        let expected: Vec<u64> = items.iter().map(|&x| g(f(x))).collect();

        let fused = pipe(items.clone()).map(f).map(g).collect();
        assert_eq!(fused, expected, "fused baseline (n = {n})");

        let pass = stream(items.clone()).stage(f).stage(g).run();
        assert_eq!(pass, expected, "pass-through output (n = {n})");

        let ordered = stream(items).stage(f).stage(g).ordered().run();
        assert_eq!(ordered, expected, "ordered pass-through (n = {n})");
    }
}

/// A zero-stage chain (`StreamStart`) also takes the pass-through; the
/// result is the input verbatim.
#[test]
fn test_stream_no_stage_pass_through_identity() {
    let items: Vec<u32> = (0..100).collect();
    assert_eq!(stream(items.clone()).run(), items);
}

/// Stage panics propagate to the `run()` caller on the pass-through (the
/// fused core cleans up partial state and resumes the unwind) instead of
/// the streaming path's process abort. Catching the unwind here doubles as
/// proof the pass-through is active for this chain shape — a streaming-path
/// stage panic would abort the whole test process.
#[test]
fn test_stream_pass_through_panic_propagates() {
    let result = std::panic::catch_unwind(|| {
        stream(0..1000u64)
            .stage(|x| x + 1)
            .stage(|x: u64| {
                assert!(x != 500, "stage boom");
                x * 2
            })
            .run()
    });
    assert!(result.is_err(), "stage panic must propagate to the caller");
}

/// `with_cancel` disables the pass-through: a pre-cancelled token must
/// truncate the output. The fused core has no cancel checks, so honouring
/// the token proves the streaming path ran.
#[test]
fn test_stream_pass_through_cancel_excluded() {
    use youpipe::CancellationToken;

    let token = CancellationToken::new();
    token.cancel();
    let r = stream(0..10_000u32)
        .with_cancel(token)
        .stage(|x| x + 1)
        .run();
    assert!(r.len() < 10_000, "cancelled run must abort early");
}

/// A `SyncStageOptions::workers` pin disables the pass-through: the pinned
/// budget must cap stage concurrency (the fused core would use the whole
/// pool). Mirrors `test_compute_workers_pin_survives_compute_pool`.
#[test]
#[cfg_attr(miri, ignore)] // wall-clock-based concurrency probing
fn test_stream_pass_through_stage_pin_excluded() {
    use youpipe::SyncStageOptions;

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, p) = (active.clone(), peak.clone());
    let r = stream(0..200u64)
        .stage_with(SyncStageOptions::new().workers(2), move |x| {
            let now = a.fetch_add(1, Ordering::SeqCst) + 1;
            p.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            a.fetch_sub(1, Ordering::SeqCst);
            x + 1
        })
        .run();
    assert_eq!(r.len(), 200);
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "pinned stage workers must cap concurrency, observed peak {}",
        peak.load(Ordering::SeqCst)
    );
}

#[test]
fn test_stream_with_fence_chunked() {
    let items: Vec<i32> = (0..100).collect();
    let mut result = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Chunked(NonZeroUsize::new(25).unwrap()))
        .stage(|x: i32| x * 5)
        .run();
    result.sort_unstable();
    let expected: Vec<i32> = (0..100).map(|x| (x + 1) * 5).collect();
    assert_eq!(result, expected);
}

/// `fence_with` reads the fence's output-channel capacity from
/// `FenceOptions` (previously hardcoded to `ctx.buffer_size(parallelism)`);
/// a tight buffer must still deliver every item in both modes.
#[test]
fn test_stream_fence_with_options_buffer() {
    use youpipe::FenceOptions;

    for mode in [
        FenceMode::Barrier,
        FenceMode::Chunked(NonZeroUsize::new(7).unwrap()),
    ] {
        let r: Vec<u64> = stream(0..500u64)
            .stage(|x| x + 1)
            .fence_with(FenceOptions::new().buffer(2), mode)
            .stage(|x| x * 2)
            .run();
        let mut expected: Vec<u64> = (0..500u64).map(|x| (x + 1) * 2).collect();
        expected.sort_unstable();
        let mut got = r;
        got.sort_unstable();
        assert_eq!(got, expected);
    }
}

#[test]
fn test_stream_with_fence_full_barrier() {
    let items: Vec<i32> = (0..50).collect();
    let result = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Barrier)
        .stage(|x: i32| x * 2)
        .ordered()
        .run();
    let expected: Vec<i32> = (0..50).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

/// Regression: `fence` previously deadlocked whenever the input size exceeded
/// the inter-stage channel buffer (256 by default). These run with a large
/// input far above that buffer to lock in the eager-drain fix.
///
/// Skipped under Miri: not a correctness constraint — with the liveness
/// fallback a 1-worker pool would switch the stage workers and feeder to
/// dedicated OS threads and complete — but miri interprets every spawned
/// thread, making a 5000-item, 3-thread pipeline prohibitively slow. The
/// fence code paths are still exercised here by the smaller-input
/// `test_stream_with_fence*` tests.
#[test]
#[cfg_attr(miri, ignore)]
fn test_stream_fence_large_input_no_deadlock() {
    let n: i32 = 5_000; // well above the default 256-slot channel buffer

    // Chunked, unordered.
    let items: Vec<i32> = (0..n).collect();
    let mut r = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
        .stage(|x: i32| x * 3)
        .run();
    r.sort_unstable();
    let expected: Vec<i32> = (0..n).map(|x| (x + 1) * 3).collect();
    assert_eq!(r, expected);

    // Barrier, ordered — the exact shape that hung the bench.
    let items: Vec<i32> = (0..n).collect();
    let r = stream(items)
        .stage(|x: i32| x + 1)
        .fence(FenceMode::Barrier)
        .stage(|x: i32| x * 3)
        .ordered()
        .run();
    assert_eq!(r, expected);
}

#[test]
fn test_stream_expand() {
    let items: Vec<i32> = (0..10).collect();
    let mut result = stream(items)
        .expand(|x: i32| vec![x, x * 10])
        .stage(|x: i32| x + 1)
        .run();
    result.sort_unstable();
    let mut expected: Vec<i32> = (0..10).flat_map(|x| vec![x + 1, x * 10 + 1]).collect();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
#[should_panic(expected = "incompatible with `.expand()`")]
fn test_stream_expand_ordered_rejected() {
    // expand + ordered() is rejected: expand fan-out shares the parent seq,
    // which the single-item-per-seq ReorderBuffer cannot handle. See the
    // `StageSpawn::has_expand` doc and the panic message in `run`.
    let items: Vec<i32> = (0..10).collect();
    let _ = stream(items)
        .expand(|x: i32| vec![x, x * 10])
        .stage(|x: i32| x + 1)
        .ordered()
        .run();
}

// ── Async stage regression coverage (gated by tokio-runtime) ──
//
// These exercise the lazy-pool path: no `with_async_pool` is attached, so
// `StreamCtx::acquire_async` must build one runtime per `run()` and reuse it
// across every bridge / async consumer in that call. A regression that
// builds a runtime per `acquire_async` call would still pass these (the
// output is unchanged) but would silently wreck small-workload latency; the
// real correctness guard is that none of these hang or panic when the
// lazily-built runtime is dropped at the end of `run()`.

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_stage_async_without_explicit_pool() {
    // The simplest async path: no config, no `with_async_pool`. Should "just
    // work" with sensible defaults.
    let items: Vec<u64> = (0..100).collect();
    let mut result = stream(items)
        .stage_async(|x: u64| async move { x.wrapping_mul(3) })
        .run();
    result.sort_unstable();
    let expected: Vec<u64> = (0..100).map(|x| x * 3).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_mixed_sync_async_without_explicit_pool() {
    // sync CPU stage → async IO stage, no explicit pool. Exercises the
    // sync→async bridge plus the async consumer, both of which call
    // `acquire_async` — they must share the lazily-built runtime.
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move { x * 2 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_two_async_stages_share_lazy_pool() {
    // Two consecutive async stages — the hardest case for the lazy pool.
    // Both stages' consumers and the async→async bridge all call
    // `acquire_async`; they must observe the same lazily-built runtime, and
    // the runtime must outlive every detached bridge task.
    let items: Vec<u64> = (0..50).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage_async(|x: u64| async move { x * 10 })
        .ordered()
        .run();
    let expected: Vec<u64> = (0..50).map(|x| (x + 1) * 10).collect();
    assert_eq!(result, expected);
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn test_async_then_sync_via_bridge() {
    // async-first → sync stage. The feeder uses a mixed-mode channel, the
    // AsyncStage's `spawn_async_feeder` recurses into StreamStart (identity),
    // and the AsyncStage's async output is bridged async→sync exactly once by
    // the SyncStage's `spawn_async_feeder` override (one dedicated OS thread
    // running `block_on` + a blocking forward). Historically this shape paid
    // 3 bridge threads (feeder bridge + per-level default bridge + final
    // bridge); the overrides keep exactly the one bridge that sync consumers
    // genuinely need.
    let items: Vec<u64> = (0..100).collect();
    let result: Vec<u64> = stream(items)
        .stage_async(|x: u64| async move { x + 1 })
        .stage(|x: u64| x * 2)
        .ordered()
        .run();
    let expected: Vec<u64> = (0..100).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

/// Async-first chains with every downstream kind — multiple sync stages,
/// an expansion, a fence, and a second async stage — all correct through the
/// `spawn_async_feeder` overrides. Guards the recursion added when the
/// per-level default bridges were eliminated (a wrong override sends the
/// async channel to a stage expecting a sync one, or vice versa, and
/// typically deadlocks or drops items).
///
/// Runs on a private 2-thread pool: each of these chains would otherwise
/// grant up to `n_cpus` workers per sync stage as pool jobs on the *global*
/// pool, and the per-run liveness budget only holds while a single `run()`
/// uses the pool — several full-budget chains running concurrently (as the
/// parallel test harness does) can starve each other's feeder jobs, hanging
/// the suite.
#[cfg(feature = "tokio-runtime")]
#[test]
fn test_async_first_all_downstream_kinds() {
    let pool = youpipe::ComputePool::new(2);
    // .stage_async → .stage → .stage (async→sync bridge, then pure sync).
    let r: Vec<u64> = stream(0..100u64)
        .with_compute_pool(pool.clone())
        .stage_async(|x| async move { x + 1 })
        .stage(|x| x * 2)
        .stage(|x| x + 10)
        .ordered()
        .run();
    assert_eq!(r, (0..100u64).map(|x| (x + 1) * 2 + 10).collect::<Vec<_>>());

    // .stage_async → .expand (async feeder into an expansion stage).
    // usize elements keep the fan-out count cast-free.
    let r: Vec<usize> = stream(0..20usize)
        .with_compute_pool(pool.clone())
        .stage_async(|x| async move { x + 1 })
        .expand(|x| vec![x; x])
        .run();
    assert_eq!(
        r.len(),
        (1..=20usize).sum::<usize>(),
        "expand after async lost items"
    );

    // .stage_async → .fence(Barrier) → .stage (async feeder through a fence).
    let r: Vec<u64> = stream(0..100u64)
        .with_compute_pool(pool.clone())
        .stage_async(|x| async move { x + 1 })
        .fence(FenceMode::Barrier)
        .stage(|x| x * 3)
        .ordered()
        .run();
    assert_eq!(r, (0..100u64).map(|x| (x + 1) * 3).collect::<Vec<_>>());

    // .stage_async → .stage → .stage_async (async→sync→async round trip).
    // Input starts at 3: the last closure subtracts 3 and the expected-value
    // expression mirrors it, so a 0 input would overflow-check-panic now
    // that overflow checks actually run under `cargo test`.
    let r: Vec<u64> = stream(3..103u64)
        .with_compute_pool(pool)
        .stage_async(|x| async move { x + 1 })
        .stage(|x| x * 2)
        .stage_async(|x| async move { x - 3 })
        .ordered()
        .run();
    assert_eq!(r, (3..103u64).map(|x| (x + 1) * 2 - 3).collect::<Vec<_>>());
}

// ── Streaming terminal inside an async context ──

/// Silence the default panic hook around a closure that is expected to panic,
/// assert the message contains every `expected` fragment, and restore the
/// hook. Keeps expected-panic tests from printing scary backtraces.
#[cfg(feature = "tokio-runtime")]
fn catch_panic_asserting<F: FnOnce()>(f: F, expected: &[&str]) {
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .expect_err("closure was expected to panic but returned");
    std::panic::set_hook(prev_hook);
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| panic!("panic payload was not a string: {payload:?}"));
    for frag in expected {
        assert!(msg.contains(frag), "panic message {msg:?} lacks {frag:?}");
    }
}

/// `run()` on an async-stage chain inside a tokio runtime context must fail
/// with a youpipe-specific panic pointing at `spawn_blocking` — not tokio's
/// opaque "Cannot start a runtime from within a runtime" (the pre-fix
/// behaviour, which never mentioned the caller's library).
#[cfg(feature = "tokio-runtime")]
#[tokio::test]
async fn test_run_in_async_context_panics_with_youpipe_hint() {
    catch_panic_asserting(
        || {
            stream(0..8u64).stage_async(|x| async move { x + 1 }).run();
        },
        &["youpipe", "spawn_blocking"],
    );
}

/// The recommended workaround actually works: the same chain inside
/// `spawn_blocking` (a blocking thread has no runtime TLS context) completes.
#[cfg(feature = "tokio-runtime")]
#[tokio::test]
async fn test_run_in_spawn_blocking_works() {
    let result =
        tokio::task::spawn_blocking(|| stream(0..8u64).stage_async(|x| async move { x + 1 }).run())
            .await
            .expect("spawn_blocking join");
    let mut sorted = result;
    sorted.sort_unstable();
    assert_eq!(sorted, (1..=8u64).collect::<Vec<_>>());
}

// ── Async stage panic propagation ──

/// A panicking async stage closure must propagate to the `run()` caller —
/// the runtime isolates panics at the task boundary and drops the payload
/// with the unobserved JoinHandle, so the run used to close its channel
/// normally and silently return truncated output (review B2).
#[cfg(feature = "tokio-runtime")]
#[test]
fn test_stage_async_panic_propagates_to_caller() {
    catch_panic_asserting(
        || {
            let _ = stream(0..1000u64)
                .stage_async(|x: u64| async move {
                    assert!(x != 500, "async boom at {x}");
                    x + 1
                })
                .run();
        },
        &["async boom at 500"],
    );
}

/// Ordered flavour: the recorded payload must win over the ordered drain's
/// accounting assertion (a panicked task drops its in-flight item, which
/// would otherwise trip `emitted + dropped == n` with a misleading
/// invariant message before the real panic could be re-raised).
#[cfg(feature = "tokio-runtime")]
#[test]
fn test_stage_async_panic_propagates_ordered() {
    catch_panic_asserting(
        || {
            let _ = stream(0..1000u64)
                .stage_async(|x: u64| async move {
                    assert!(x != 500, "async boom at {x}");
                    x + 1
                })
                .ordered()
                .run();
        },
        &["async boom at 500"],
    );
}

/// Nested `run()` inside an async stage closure: the inner run's async
/// terminal calls `block_on` from a runtime task, which re-raises with the
/// actionable `spawn_blocking` hint. That panic used to be swallowed with
/// the task (every consumer died, the run silently returned 0 items);
/// it must now reach the outer `run()` caller. The inner runs share one
/// pre-built pool so the failure path does not construct a runtime per
/// task.
#[cfg(feature = "tokio-runtime")]
#[test]
fn test_nested_run_inside_stage_async_propagates() {
    use youpipe::TokioPool;

    let shared = TokioPool::build(2).expect("build shared runtime");
    let inner_pool = shared.clone();
    catch_panic_asserting(
        move || {
            let _ = stream(0..8u64)
                .stage_async(move |x: u64| {
                    let pool = inner_pool.clone();
                    async move {
                        let _inner: Vec<u64> = stream(0..4u64)
                            .with_async_pool(pool)
                            .stage_async(|y: u64| async move { y + 1 })
                            .run();
                        x
                    }
                })
                .run();
        },
        &["youpipe", "spawn_blocking"],
    );
}

#[cfg(feature = "tokio-runtime")]
#[test]
// Wall-clock assertion (heartbeat gap < 40 ms) — meaningless under miri's
// ~1000x interpreted slowdown; the no-stall property is checked by the
// non-miri run.
#[cfg_attr(miri, ignore)]
fn test_sync_to_async_does_not_stall_tokio_driver() {
    // Regression guard for the "async driver + blocking worker" anti-pattern.
    //
    // The sync→async handoff parks producers on `SyncSender::send` when the
    // mixed-mode channel fills under backpressure. That blocking call MUST
    // live on a ComputePool OS thread — never on a tokio worker. If it ran
    // inside a `tokio::spawn` task, it would park the tokio worker thread and
    // stall *every* other task on it (or, with one worker, deadlock).
    //
    // This test amplifies the effect with a single-worker runtime: any
    // blocking op on that one worker freezes the whole async side. We run a
    // sync→async pipeline under deliberate backpressure (fast sync producer,
    // slow async consumer) while a heartbeat task measures its own scheduling
    // gaps on the same runtime. A healthy gap stays near the sleep duration;
    // a stalled driver (blocking `send` on the tokio worker) spikes it.
    use std::time::{Duration, Instant};

    use youpipe::{PipelineConfig, TokioPool};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build single-worker runtime");

    // Heartbeat: 30 sleeps of 5 ms, tracking the worst observed gap between
    // successive wake-ups. Spawned onto the same single-worker runtime as the
    // pipeline's async consumers, so it shares the fate of the tokio worker.
    let (hb_tx, hb_rx) = std::sync::mpsc::channel::<Duration>();
    {
        let _enter = rt.handle().enter();
        tokio::spawn(async move {
            let mut max_gap = Duration::ZERO;
            let mut prev = Instant::now();
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let now = Instant::now();
                max_gap = max_gap.max(now - prev);
                prev = now;
            }
            let _ = hb_tx.send(max_gap);
        });
    }

    // Pipeline runs on a dedicated OS thread so a stalled runtime can't hang
    // the test thread — we observe completion via a channel with a timeout.
    // Fast sync stage floods the channel; slow async stage (1 ms sleep each,
    // only 4 consumers) drains it slowly, so the mixed-mode channel fills and
    // sync workers park on `send`. This is precisely the regime where a
    // tokio-hosted producer would freeze the runtime.
    let n: u64 = 3000;
    let (res_tx, res_rx) = std::sync::mpsc::channel::<Vec<u64>>();
    let pipe = stream(0..n)
        .with_config(PipelineConfig::default().with_io_concurrency(4))
        .with_async_pool(TokioPool::new(rt.handle().clone()))
        .stage(|x: u64| x + 1)
        .stage_async(|x: u64| async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            x * 2
        });
    std::thread::spawn(move || {
        let _ = res_tx.send(pipe.run());
    });

    // Strong guarantee: the single tokio worker was never parked by a blocking
    // send. Heartbeat gaps stay near 5 ms even while sync workers are parked
    // on `send` under backpressure. A *stalled* driver parks the heartbeat for
    // the whole backpressure drain — (3000 − 256 buffer) items at 4 × 1 ms
    // consumers ≈ 700 ms — so the bound just needs to separate that regime
    // from scheduler noise. 250 ms does with ~3× margin each way: tight
    // enough to catch a real stall, loose enough to survive this test running
    // inside the parallel test suite, where dozens of other tests' pools and
    // runtimes routinely spike a 5 ms sleep's wake-up by 100 ms+ (observed
    // flakes at the old 40 ms bound on an otherwise idle machine).
    let max_gap = hb_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("heartbeat never finished — tokio worker stalled by a blocking op");
    assert!(
        max_gap < Duration::from_millis(250),
        "tokio driver stalled under sync→async backpressure: max heartbeat gap {max_gap:?} \
         (expected ~5 ms) — a blocking send is likely running on the tokio worker"
    );

    // Weak guarantee: the pipeline completed at all — no deadlock from a
    // stalled runtime starving its own consumers.
    let result = res_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("pipeline deadlocked — tokio worker stalled by a blocking op");
    assert_eq!(result.len(), usize::try_from(n).unwrap());

    // Keep the runtime alive until both observations land.
    drop(rt);
}

#[test]
// Wall-clock assertions (elapsed < 500 ms, heartbeat gap < 40 ms) cannot
// hold under miri's ~1000x interpreted slowdown; the logic assertions of
// both tests are exercised by the non-miri run.
#[cfg_attr(miri, ignore)]
fn test_fence_cancellation_aborts_early() {
    // The fence forwarder checks the cancellation token. In Barrier mode it
    // buffers all upstream items before forwarding any — without the cancel
    // check it would ignore the token and keep draining until upstream
    // finished, defeating the purpose of cancellation.
    use std::{thread, time::Duration};

    use youpipe::CancellationToken;

    let token = CancellationToken::new();
    let items: Vec<u32> = (0..10_000).collect();
    let slow = |x: u32| -> u32 {
        thread::sleep(Duration::from_micros(20));
        x + 1
    };

    let cancel_handle = {
        let token = token.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            token.cancel();
        })
    };

    let start = std::time::Instant::now();
    let result = stream(items)
        .with_cancel(token)
        .stage(slow)
        .fence(FenceMode::Barrier)
        .stage(|x: u32| x * 2)
        .run();
    let elapsed = start.elapsed();

    cancel_handle.join().unwrap();

    // Cancellation should abort well before processing all 10 000 items.
    assert!(
        result.len() < 10_000,
        "expected early abort, got {}",
        result.len()
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "fence cancellation should shortcut the run, took {elapsed:?}"
    );
}

#[test]
fn test_scope_non_static() {
    let factor = 7i32;
    let result = youpipe::scope(|s| s.pipe(0..20).map(|x: i32| x * factor).collect());
    let expected: Vec<i32> = (0..20).map(|x| x * 7).collect();
    assert_eq!(result, expected);
}

#[test]
fn test_scope_par_map() {
    let offset = 100i32;
    let result = youpipe::scope(|s| s.pipe(0..50).map(|x: i32| x + offset).collect());
    assert_eq!(result, (100..150).collect::<Vec<_>>());
}

#[test]
fn test_par_map_counts_items() {
    let counter = Arc::new(AtomicUsize::new(0));
    let items: Vec<u64> = (0..1000).collect();
    let c = counter.clone();
    let result = pipe(items)
        .map(move |x: u64| {
            c.fetch_add(1, Ordering::Relaxed);
            cpu_heavy(x)
        })
        .collect();
    assert_eq!(result.len(), 1000);
    assert_eq!(counter.load(Ordering::Relaxed), 1000);
}

#[test]
fn test_large_dataset() {
    // Miri: scaled down 50× — the point is crossing the multi-chunk split
    // thresholds, not the absolute size, and 2K items already spans several
    // chunks across the (single) emulated worker.
    let n: usize = if cfg!(miri) {
        2_000
    } else {
        100_000
    };
    let items: Vec<u64> = (0..n as u64).collect();
    let result = pipe(items).map(|x: u64| x.wrapping_add(1)).collect();
    assert_eq!(result.len(), n);
    let mut r = result;
    r.sort_unstable();
    assert_eq!(r[0], 1);
    assert_eq!(r[n - 1], n as u64);
}

/// `pipe` accepts any `IntoIterator`, not just `Vec`. Verifies the entry point
/// honours ranges and array references.
#[test]
fn test_pipe_accepts_arbitrary_iterator() {
    let r1: Vec<i32> = pipe(0..10).map(|x: i32| x * 2).collect();
    assert_eq!(r1, (0..10).map(|x| x * 2).collect::<Vec<_>>());

    let r2: Vec<i32> = pipe(&[1, 2, 3]).map(|&x: &i32| x + 1).collect();
    assert_eq!(r2, vec![2, 3, 4]);
}

/// `with_workload(Unbalanced)` produces the same result as `Balanced` but with
/// finer-grained task splitting — guards against the oversplit factor silently
/// changing output cardinality.
#[test]
fn test_pipe_with_workload_unbalanced() {
    use youpipe::Workload;
    let r = pipe(0..1000)
        .map(|x: i32| x.wrapping_mul(3))
        .with_workload(Workload::Unbalanced)
        .collect();
    let mut sorted = r;
    sorted.sort_unstable();
    assert_eq!(sorted, (0..1000).map(|x| x * 3).collect::<Vec<_>>());
}

// ── Prelude: extension-trait style must match the free-function style ──

#[test]
fn test_prelude_pipe_matches_free_function() {
    use youpipe::prelude::IterExt;

    let free: Vec<i32> = pipe(0..100).map(|x: i32| x * 2).collect();
    let method: Vec<i32> = (0..100).pipe().map(|x: i32| x * 2).collect();
    assert_eq!(free, method);
    assert_eq!(free, (0..100).map(|x| x * 2).collect::<Vec<_>>());
}

#[test]
fn test_prelude_stream_matches_free_function() {
    use youpipe::prelude::IterExt;

    let mut free = stream(0..50).stage(|x: i32| x + 1).run();
    free.sort_unstable();
    let mut method = (0..50).stream().stage(|x: i32| x + 1).run();
    method.sort_unstable();
    assert_eq!(free, method);
}

/// The prelude trait is a blanket impl over `IntoIterator` — exercises a few
/// iterator sources beyond plain ranges to confirm there's no hidden bound.
#[test]
fn test_prelude_iterext_on_various_sources() {
    use youpipe::prelude::IterExt;

    // Vec
    let v: Vec<i32> = vec![1, 2, 3].pipe().map(|x| x + 1).collect();
    assert_eq!(v, vec![2, 3, 4]);

    // Slice reference
    let s: Vec<i32> = [1, 2, 3].iter().copied().pipe().map(|x| x * 10).collect();
    assert_eq!(s, vec![10, 20, 30]);

    // Stream from a Vec
    let r: Vec<i32> = vec![1, 2, 3].stream().stage(|x: i32| x - 1).ordered().run();
    assert_eq!(r, vec![0, 1, 2]);
}

// ── Panic propagation ──

#[test]
fn test_pipe_panic_propagates_parallel() {
    // Large enough to hit the parallel index-based path (n > serial threshold).
    // A panicking closure must propagate through the join tree and LeafGuard
    // cleanup, surfacing as a real panic on the collecting thread.
    // Miri: scaled 25x (still >  the 1-thread serial threshold of 64).
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let boom_at = n / 2;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _: Vec<i32> = pipe(0..n)
            .map(move |x| {
                assert!(x != boom_at, "boom at {x}");
                x + 1
            })
            .collect();
    }));
    assert!(result.is_err());
    let payload = result.unwrap_err();
    let msg = payload
        .downcast_ref::<String>()
        .expect("panic payload is String");
    assert_eq!(msg, format!("boom at {}", n / 2).as_str());
}

#[test]
fn test_pipe_panic_propagates_serial() {
    // Small batch: hits the serial fallback loop inside collect().
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Vec<i32> = pipe(0..100i32)
            .map(|x| {
                assert!(x != 50, "serial boom");
                x + 1
            })
            .collect();
    }));
    assert!(result.is_err());
}

#[test]
fn test_try_collect_panic_propagates() {
    // Panic inside try_collect's fast path (index-based): the TryLeafGuard
    // must clean up partial output slots before the panic propagates.
    // Miri: scaled 25x (still > the 1-thread serial threshold of 64).
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let panic_at = n / 2;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<Vec<i32>, &'static str> = pipe(0..n)
            .try_map(move |x| -> Result<i32, &'static str> {
                assert!(x != panic_at, "try boom");
                Ok(x + 1)
            })
            .try_collect();
    }));
    assert!(result.is_err());
    let payload = result.unwrap_err();
    assert_eq!(*payload.downcast_ref::<&'static str>().unwrap(), "try boom");
}

// ── for_each terminal: side-effect-only pipelines (no output buffer) ──

#[test]
fn test_for_each_basic() {
    // Single-stage for_each: equivalent to rayon's par_iter().for_each().
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    pipe(0..1000usize).for_each(move |x: usize| {
        c.fetch_add(x, Ordering::Relaxed);
    });
    let expected: usize = (0..1000).map(|x: usize| x).sum();
    assert_eq!(counter.load(Ordering::Relaxed), expected);
}

#[test]
fn test_for_each_chained_map() {
    // Multi-stage chain ending in for_each: fuses map+map into one closure.
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    pipe(0..500usize)
        .map(|x: usize| x + 1)
        .map(|x: usize| x * 2)
        .for_each(move |x: usize| {
            c.fetch_xor(x, Ordering::Relaxed);
        });
    let expected: usize = (0..500).map(|x: usize| (x + 1) * 2).fold(0, |a, b| a ^ b);
    assert_eq!(counter.load(Ordering::Relaxed), expected);
}

#[test]
fn test_for_each_filter_skips_dropped() {
    // Filter is honoured: only even-valued outputs reach f.
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    pipe(0..1000i32)
        .filter(|x: &i32| x % 2 == 0)
        .for_each(move |_x: i32| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    assert_eq!(counter.load(Ordering::Relaxed), 500);
}

#[test]
fn test_for_each_empty() {
    let called = Arc::new(AtomicBool::new(false));
    let c = called.clone();
    pipe(Vec::<u64>::new()).for_each(move |_: u64| c.store(true, Ordering::Relaxed));
    assert!(
        !called.load(Ordering::Relaxed),
        "for_each on empty input must not call f"
    );
}

#[test]
fn test_for_each_single() {
    let sink = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = sink.clone();
    pipe(vec![42u64]).for_each(move |x: u64| s.lock().unwrap().push(x));
    assert_eq!(*sink.lock().unwrap(), vec![42]);
}

#[test]
fn test_for_each_parallel_large() {
    // Large enough to exceed the serial threshold and exercise the parallel
    // par_for_each path (MAY_FILTER == false → pure leaf).
    let n: usize = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    pipe(0..n).for_each(move |_x: usize| {
        c.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(counter.load(Ordering::Relaxed), n);
}

#[test]
fn test_for_each_parallel_filter_large() {
    // Large parallel path with MAY_FILTER == true (filter branch).
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    pipe(0..n).filter(|x: &i32| *x % 3 == 0).for_each(move |_| {
        c.fetch_add(1, Ordering::Relaxed);
    });
    let expected = (0..n).filter(|x| x % 3 == 0).count();
    assert_eq!(counter.load(Ordering::Relaxed), expected);
}

#[test]
fn test_for_each_panic_propagates_parallel() {
    // Large batch → parallel par_for_each path. Panic in f must surface.
    // Miri: scaled 25x (still > the 1-thread serial threshold of 64).
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let boom_at = n / 2;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        pipe(0..n).for_each(move |x| {
            assert!(x != boom_at, "for_each boom at {x}");
        });
    }));
    assert!(result.is_err());
    let payload = result.unwrap_err();
    let msg = payload.downcast_ref::<String>().expect("String payload");
    assert_eq!(msg, format!("for_each boom at {}", n / 2).as_str());
}

#[test]
fn test_for_each_panic_propagates_serial() {
    // Small batch → serial fallback inside for_each.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pipe(0..100i32).for_each(|x| {
            assert!(x != 50, "serial for_each boom");
        });
    }));
    assert!(result.is_err());
}

#[test]
fn test_for_each_panic_drops_unread_items() {
    // A non-Copy input with observable Drop. If ForEachGuard failed to drop
    // the unread tail, this would leak (miri would flag the UB; under normal
    // runs the count check confirms the tail was not double-dropped).
    struct DropCounter {
        counter: Arc<AtomicUsize>,
        val: i32,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    // Miri: scaled 25x — drop accounting is per-item and exact, so the
    // smaller batch proves the same LeakGuard property far cheaper.
    let n: i32 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let items: Vec<DropCounter> = (0..n)
        .map(|i| DropCounter {
            counter: c.clone(),
            val: i,
        })
        .collect();
    let total = items.len();

    let panic_at = n / 2;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Parallel path (large batch). Panic mid-batch.
        pipe(items).for_each(move |d: DropCounter| {
            assert!(d.val != panic_at, "mid-batch drop boom");
        });
    }));
    assert!(result.is_err());

    // Every DropCounter must be dropped exactly once: the consumed one (gone
    // with the panic) and the tail (dropped by ForEachGuard) + all prior
    // (consumed by successful iterations).
    assert_eq!(
        counter.load(Ordering::Relaxed),
        total,
        "every input item must be dropped exactly once on panic"
    );
}

#[test]
fn test_for_each_unbalanced_workload() {
    // Sanity: the Unbalanced oversplit path produces correct results in
    // for_each (no slot-validity assumptions violated).
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let n: usize = if cfg!(miri) {
        1_000
    } else {
        10_000
    };
    pipe(0..n)
        .with_workload(Workload::Unbalanced)
        .for_each(move |_| {
            c.fetch_add(1, Ordering::Relaxed);
        });
    assert_eq!(counter.load(Ordering::Relaxed), n);
}

#[test]
// Multi-driver Unbalanced dispatch: external threads hammering interleaved
// Unbalanced collect/for_each batches on the shared pool. Complements
// `test_hybrid_dispatch_spin_wait_stress` (Balanced) on the Unbalanced
// oversplit path: concurrent drivers contend for the injector and each
// other's deques while the CountLatch accounting must stay exact.
fn test_unbalanced_multi_driver_stress() {
    const ITERS: usize = if cfg!(miri) {
        50
    } else {
        2_000
    };
    const N: usize = if cfg!(miri) {
        32
    } else {
        1_000
    };
    let data: Vec<u64> = (0..N as u64).collect();
    std::thread::scope(|s| {
        for _ in 0..4 {
            let data = data.clone();
            s.spawn(move || {
                for i in 0..ITERS {
                    if i % 2 == 0 {
                        let r: Vec<u64> = pipe(data.clone())
                            .with_workload(Workload::Unbalanced)
                            .map(|x: u64| x.wrapping_mul(3).wrapping_add(1))
                            .collect();
                        for (j, v) in r.iter().enumerate() {
                            assert_eq!(*v, j as u64 * 3 + 1);
                        }
                    } else {
                        let sum = Arc::new(std::sync::atomic::AtomicU64::new(0));
                        let s2 = sum.clone();
                        pipe(data.clone())
                            .with_workload(Workload::Unbalanced)
                            .for_each(move |x: u64| {
                                s2.fetch_add(x, Ordering::Relaxed);
                            });
                        assert_eq!(sum.load(Ordering::Relaxed), (N as u64) * (N as u64 - 1) / 2);
                    }
                }
            });
        }
    });
}

#[test]
// Native: 4 threads x 20k iterations x 1k cpu_heavy items. Under miri the
// iteration count shrinks 200x (cpu_heavy itself is scaled down too, see
// its comment) so the interpreter covers the same spin/latch interleavings
// in minutes; the exhaustive interleaving analysis lives in the loom models
// (`latch.rs` loom_tests), this adds the interpreted-memory-model view.
fn test_hybrid_dispatch_spin_wait_stress() {
    // Regression guard for `CountLatch::wait_spin` (off-pool hybrid driver).
    //
    // An earlier version returned directly from the spin on observing
    // `counter == 0`, racing the latch's stack-frame free against the last
    // chunk's still-in-flight `LockLatch::set` — surfacing as a SIGSEGV under
    // the `sync_cpu_heavy` benchmark. The fix mandates that the spin end in
    // the mutex acquire (`LockLatch::wait`); this test hammers the path from
    // multiple external threads to catch any re-introduction of that
    // use-after-free.
    //
    // `cpu_heavy` per item keeps each batch's parallel work inside the spin
    // envelope (~tens of µs), so this exercises the spin fast path, not the
    // condvar fallback.
    const ITERS: usize = if cfg!(miri) {
        100
    } else {
        20_000
    };
    const N: usize = if cfg!(miri) {
        200
    } else {
        1_000
    };
    let data: Vec<u64> = (0..N as u64).collect();
    std::thread::scope(|s| {
        for _ in 0..4 {
            let data = data.clone();
            s.spawn(move || {
                for _ in 0..ITERS {
                    let v = data.clone();
                    let r: Vec<u64> = pipe(v).map(|x: u64| cpu_heavy(x)).collect();
                    assert_eq!(r.len(), N);
                    // Spot-check first/last to ensure indices map correctly.
                    assert_eq!(r[0], cpu_heavy(0));
                    assert_eq!(r[N - 1], cpu_heavy((N - 1) as u64));
                }
            });
        }
    });
}

// ── Workload::Custom ──

#[test]
fn test_workload_custom_correctness() {
    // Custom oversplit produces identical results across factors 1..=32 —
    // the split granularity must never affect the output.
    let expected: Vec<u64> = (0..10_000).map(|x| x * 3 + 1).collect();
    for factor in [1usize, 2, 7, 16, 32] {
        let r: Vec<u64> = pipe(0..10_000)
            .with_workload(Workload::Custom(NonZeroUsize::new(factor).unwrap()))
            .map(|x: u64| x * 3 + 1)
            .collect();
        assert_eq!(r, expected, "factor {factor} diverged");
    }
}

#[test]
fn test_workload_custom_try_collect() {
    let r: Result<Vec<u64>, &str> = pipe(0..1_000)
        .with_workload(Workload::Custom(NonZeroUsize::new(4).unwrap()))
        .try_map(|x: u64| Ok::<_, &str>(x + 1))
        .try_collect();
    assert_eq!(r.unwrap(), (1..=1_000).collect::<Vec<_>>());
}

// ── per-stage stage options (streaming) ──

#[test]
// Wall-clock concurrency observation relies on sleep overlap; miri's
// interpreted slowdown makes it meaningless (but minutes long).
// NOTE: keep the total pool occupancy short (~100 ms) — this test pins 2
// global-pool workers with sleeps, and longer runs delay other tests'
// worker spawn-ups enough to trip their wall-clock assertions (observed with
// test_fence_cancellation_aborts_early when this ran 500 ms).
#[cfg_attr(miri, ignore)]
fn test_stage_options_workers_pins_parallelism() {
    use std::time::Duration;

    use youpipe::SyncStageOptions;

    // The pinned stage must never exceed 2 concurrent executions, whatever
    // the default equal-division would have granted it.
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let a = active.clone();
    let m = max_active.clone();
    let n = 40usize;
    stream(0..n)
        .stage_with(SyncStageOptions::new().workers(2), move |x: usize| {
            let cur = a.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(cur, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
            a.fetch_sub(1, Ordering::SeqCst);
            x + 1
        })
        .stage(|x: usize| x * 2)
        .run();
    let max = max_active.load(Ordering::SeqCst);
    assert!(
        max <= 2 + 1, // +1: a worker may increment before the previous
        // decrement is visible; >3 would mean the pin was ignored
        "workers(2) pin not honoured: max concurrency {max}"
    );
    assert!(max >= 2, "pinned stage never reached 2 workers: {max}");
}

#[test]
// Async-task concurrency observation via timer sleeps; meaningless under
// miri's clock.
#[cfg_attr(miri, ignore)]
#[cfg(feature = "tokio-runtime")]
fn test_stage_options_io_concurrency_pins_fanout() {
    use std::time::Duration;

    use youpipe::AsyncStageOptions;

    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let a = active.clone();
    let m = max_active.clone();
    let n = 64usize;
    let r: Vec<usize> = stream(0..n)
        // Global default is 128; the per-stage pin must win.
        .with_io_concurrency(128)
        .stage_async_with(AsyncStageOptions::new().io_concurrency(3), move |x: usize| {
            let a = a.clone();
            let m = m.clone();
            async move {
                let cur = a.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(cur, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                a.fetch_sub(1, Ordering::SeqCst);
                x + 1
            }
        })
        .run();
    assert_eq!(r.len(), n);
    let max = max_active.load(Ordering::SeqCst);
    assert!(
        max <= 3 + 1, // +1: increment visible before the paired decrement
        "io_concurrency(3) pin not honoured: max fanout {max}"
    );
    assert!(max >= 3, "async stage never reached 3 tasks: {max}");
}

#[test]
fn test_stage_options_buffer_override_runs() {
    // A tiny explicit buffer tightens backpressure but must not affect
    // correctness (items flow through, none dropped).
    use youpipe::SyncStageOptions;

    // Miri: single emulated pool worker. An `n > buffer` run no longer
    // deadlocks there (the runner falls back to dedicated OS threads when the
    // pool cannot host every stage), but miri interprets each spawned thread
    // — keep the pins on the inline-feeder path so the test stays fast. The
    // tight-backpressure shaping itself is covered by the native run.
    let (n, b1, b2) = if cfg!(miri) {
        (100, 256, 256)
    } else {
        (500, 2, 4)
    };
    let r: Vec<usize> = stream(0..n)
        .stage_with(SyncStageOptions::new().buffer(b1), |x: usize| x + 1)
        .stage_with(SyncStageOptions::new().buffer(b2).workers(3), |x: usize| {
            x * 2
        })
        .run();
    let expected: Vec<usize> = (0..n).map(|x| (x + 1) * 2).collect();
    let mut sorted = r;
    sorted.sort_unstable();
    assert_eq!(sorted, expected);
}

#[test]
fn test_stream_expand_emit() {
    // Push-style expansion must produce exactly the same outputs as the
    // owned-Vec API, including empty groups and a downstream stage.
    let items: Vec<i32> = (0..10).collect();
    let mut result = stream(items.clone())
        .expand_emit(|x: i32, out: &mut Vec<i32>| {
            if x % 3 != 0 {
                out.push(x);
                out.push(x * 10);
            } // multiples of 3 expand to nothing
        })
        .stage(|x: i32| x + 1)
        .run();
    result.sort_unstable();
    let mut expected: Vec<i32> = items
        .iter()
        .filter(|x| **x % 3 != 0)
        .flat_map(|x| [x + 1, x * 10 + 1])
        .collect();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn test_stream_expand_emit_with_workers() {
    use youpipe::SyncStageOptions;

    let r: Vec<u32> = stream(0..50u32)
        .expand_emit_with(
            SyncStageOptions::new().workers(2),
            |x, out: &mut Vec<u32>| {
                for i in 0..=x {
                    out.push(i);
                }
            },
        )
        .run();
    assert_eq!(
        r.len(),
        (0..50u32).map(|x| x as usize + 1).sum::<usize>(),
        "expand_emit_with dropped items"
    );
}

#[test]
fn test_stage_options_expand_with_workers() {
    use youpipe::SyncStageOptions;

    let r: Vec<u32> = stream(0..50u32)
        .expand_with(SyncStageOptions::new().workers(2), |x| {
            vec![x; x as usize + 1]
        })
        .run();
    assert_eq!(
        r.len(),
        (0..50u32).map(|x| x as usize + 1).sum::<usize>(),
        "expand_with dropped items"
    );
}

#[test]
fn test_stage_budget_explicit_deduction() {
    // 2 explicit stages (4+4 workers) on an 8-worker budget: the unspecified
    // third stage must still run (≥1 worker), not deadlock — the budget
    // deduction must not zero it out.
    use youpipe::SyncStageOptions;

    let r: Vec<usize> = stream(0..100)
        .with_compute_workers(8)
        .stage_with(SyncStageOptions::new().workers(4), |x: usize| x + 1)
        .stage_with(SyncStageOptions::new().workers(4), |x: usize| x + 1)
        .stage(|x: usize| x * 10)
        .run();
    assert_eq!(r.len(), 100);
    assert_eq!(
        r.iter().sum::<usize>(),
        (0..100usize).map(|x| (x + 2) * 10).sum::<usize>()
    );
}

/// A `with_compute_workers` pin must survive a subsequent `with_compute_pool`
/// on an 8-thread pool (previously the pool setter silently overwrote the
/// budget with its own thread count, so only call order decided which knob
/// took effect). Observable through peak stage concurrency: a pinned budget
/// of 2 never lets 3 stage workers overlap, while the clobber behaviour
/// granted 8.
#[test]
#[cfg_attr(miri, ignore)] // wall-clock-based concurrency probing
fn test_compute_workers_pin_survives_compute_pool() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, p) = (active.clone(), peak.clone());
    let r = stream(0..200u64)
        .with_compute_workers(2)
        .with_compute_pool(youpipe::ComputePool::new(8))
        .stage(move |x| {
            let now = a.fetch_add(1, Ordering::SeqCst) + 1;
            p.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            a.fetch_sub(1, Ordering::SeqCst);
            x + 1
        })
        .run();
    assert_eq!(r.len(), 200);
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "pinned budget of 2 must cap stage concurrency, observed peak {}",
        peak.load(Ordering::SeqCst)
    );
}

// ── Streaming liveness: worker budget vs pool size ──

/// Regression (the intermittent `pipeline_integration` hang, todo P1 #14):
/// the streaming run's liveness budget was computed per run against the
/// **full** pool, so concurrent full-budget runs on a shared pool jointly
/// oversubscribed it — every worker parked on a full/empty inter-stage
/// channel while the still-queued stage-worker jobs (and anything behind
/// them in the injector FIFO, e.g. fused chunks) could never be popped.
/// Barrier-aligned full-budget runs reproduced the hang deterministically;
/// the pool-wide parking lease now admits a run only if its whole upper
/// bound of channel-parking jobs fits the remaining pool capacity, else it
/// falls back to dedicated threads. This shape deadlocked 5/5 pre-fix.
#[test]
#[cfg_attr(miri, ignore)] // thread-count stress the interpreter cannot pace
fn test_concurrent_full_budget_runs_share_pool_no_deadlock() {
    let r: Vec<usize> = run_with_deadlock_watchdog(|| {
        const RUNS: usize = 8;
        let barrier = Arc::new(std::sync::Barrier::new(RUNS));
        let pool = youpipe::ComputePool::new(8);
        let lens: Vec<usize> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..RUNS)
                .map(|_| {
                    let barrier = barrier.clone();
                    let pool = pool.clone();
                    s.spawn(move || {
                        // Align the runs so their [feeder, stage-1 workers,
                        // stage-2 workers] job batches interleave in the
                        // injector — the shape that parked every worker on a
                        // stage-1 channel with stage-2 jobs still queued.
                        barrier.wait();
                        let r: Vec<u64> = stream(0..2000u64)
                            .with_compute_pool(pool)
                            .stage(|x| x.wrapping_mul(3).wrapping_add(1))
                            .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
                            .stage(|x| x ^ 0x5a5a)
                            .run();
                        r.len()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        lens
    });
    assert_eq!(r, vec![2000; 8]);
}

/// Mixed admission under the parking lease: runs small enough to co-lease
/// the pool share it (pool mode), the overflow falls back to dedicated
/// threads, and the lease accounting must fully drain afterwards — pinned
/// stages lease their exact pin, unpinned ones the divided default.
#[test]
#[cfg_attr(miri, ignore)] // thread-count stress the interpreter cannot pace
fn test_concurrent_pinned_runs_mixed_admission_no_deadlock() {
    use youpipe::SyncStageOptions;

    let r: Vec<usize> = run_with_deadlock_watchdog(|| {
        const RUNS: usize = 12;
        let barrier = Arc::new(std::sync::Barrier::new(RUNS));
        // 8-thread pool; each run needs 1 feeder + 2 pinned stages × 2
        // workers = 5 slots, so ~1 run co-leases at a time and the rest take
        // dedicated threads — both admission paths run concurrently.
        let pool = youpipe::ComputePool::new(8);
        let lens: Vec<usize> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..RUNS)
                .map(|_| {
                    let barrier = barrier.clone();
                    let pool = pool.clone();
                    s.spawn(move || {
                        barrier.wait();
                        let r: Vec<u64> = stream(0..1000u64)
                            .with_compute_pool(pool)
                            .stage_with(SyncStageOptions::new().workers(2), |x| x + 1)
                            .stage_with(SyncStageOptions::new().workers(2), |x| x * 2)
                            .run();
                        r.len()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        lens
    });
    assert_eq!(r, vec![1000; 12]);
}

/// Regression (fence forwarders as leased pool jobs): each fence is one
/// channel-parking pool job in pool mode and MUST enter the run's lease
/// reservation. The shape below deadlocks deterministically without the
/// `fences` term: the reservation (feeder + 6 + 1 pinned workers = 8) admits
/// the run on the 8-thread pool, but the forwarder is a 9th parking job —
/// the injector FIFO pops [feeder, stage-1 ×6, forwarder] onto the 8
/// workers, the stage-2 job stays queued, and the run wedges with every
/// worker parked (feeder on a full stage-1 channel, stage 1 on a full mid
/// channel, forwarder on a full fenced channel that only the queued stage-2
/// job would drain). With the term, the reservation is 9 > 8 and the run
/// takes the dedicated-thread fallback instead.
#[test]
#[cfg_attr(miri, ignore)] // thread-count stress the interpreter cannot pace
fn test_fence_forwarder_counts_in_parking_lease_no_deadlock() {
    use youpipe::SyncStageOptions;

    let r: Vec<u64> = run_with_deadlock_watchdog(|| {
        stream(0..2000u64)
            .with_compute_pool(youpipe::ComputePool::new(8))
            .stage_with(SyncStageOptions::new().workers(6), |x| {
                x.wrapping_mul(3).wrapping_add(1)
            })
            .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
            .stage_with(SyncStageOptions::new().workers(1), |x| x ^ 0x5a5a)
            .run()
    });
    assert_eq!(r.len(), 2000);
    let mut sorted = r;
    sorted.sort_unstable();
    let mut expected: Vec<u64> = (0..2000u64)
        .map(|x| (x.wrapping_mul(3).wrapping_add(1)) ^ 0x5a5a)
        .collect();
    expected.sort_unstable();
    assert_eq!(sorted, expected);
}

/// Helper: run `f` on a helper thread and require completion within 30 s, so
/// a liveness regression fails the test instead of hanging the harness.
/// Mirrors the dedicated-pool rationale of
/// `test_nested_stream_inside_pool_worker_no_deadlock` (self-contained
/// scheduling, immune to parallel-suite noise).
// NOT cfg(not(miri)): the callers are `#[cfg_attr(miri, ignore)]`, and
// `ignore` skips execution but still compiles the call — gating the
// definition out made `cargo miri test` fail to build this target.
fn run_with_deadlock_watchdog<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (done_tx, done_rx) = std::sync::mpsc::channel::<T>();
    std::thread::spawn(move || {
        let _ = done_tx.send(f());
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("streaming pipeline deadlocked")
}

/// Regression: the worker budget previously ignored the feeder job and floored
/// every stage at 1 worker, so `k = P` default stages plus `n > k * buffer`
/// parked every pool thread inside channel send/recv while the last stage's
/// job sat unscheduled in the injector — a permanent hang. Now the feeder
/// reserves a slot, and chains that still don't fit dispatch stage workers as
/// dedicated threads. Both shapes complete here on a 4-thread pool.
#[test]
#[cfg_attr(miri, ignore)]
fn test_stream_stages_at_pool_size_no_deadlock() {
    // k == P: every stage gets a dedicated-thread worker; feeder is a thread.
    let r: Vec<u64> = run_with_deadlock_watchdog(|| {
        stream(0..2000u64)
            .with_compute_pool(youpipe::ComputePool::new(4))
            .stage(|x| x + 1)
            .stage(|x| x * 2)
            .stage(|x| x ^ 0x5a5a)
            .stage(|x| x.wrapping_mul(3))
            .run()
    });
    assert_eq!(r.len(), 2000);
    let mut sorted = r;
    sorted.sort_unstable();
    // `^ 0x5A5A` is not monotonic, so the expected side must be sorted too.
    let mut expected: Vec<u64> = (0..2000u64)
        .map(|x| (((x + 1) * 2) ^ 0x5a5a).wrapping_mul(3))
        .collect();
    expected.sort_unstable();
    assert_eq!(sorted, expected);

    // k > P: same fallback, more stages than threads.
    let r: Vec<u64> = run_with_deadlock_watchdog(|| {
        stream(0..2000u64)
            .with_compute_pool(youpipe::ComputePool::new(4))
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .stage(|x| x + 1)
            .run()
    });
    assert_eq!(r.len(), 2000);
    assert!(r.iter().all(|&x| x >= 8));
}

/// Regression: explicit `SyncStageOptions::workers` pins whose sum (plus the
/// feeder) exceeds the pool must be clamped to the liveness budget, not
/// oversubscribe the pool into a deadlock. `workers(3) + workers(3)` on a
/// 4-thread pool previously hung for n ≫ buffer.
#[test]
#[cfg_attr(miri, ignore)]
fn test_stream_explicit_pins_exceeding_pool_no_deadlock() {
    use youpipe::SyncStageOptions;

    let r: Vec<u64> = run_with_deadlock_watchdog(|| {
        stream(0..2000u64)
            .with_compute_pool(youpipe::ComputePool::new(4))
            .stage_with(SyncStageOptions::new().workers(3), |x| x + 1)
            .stage_with(SyncStageOptions::new().workers(3), |x| x * 2)
            .stage(|x| x + 10)
            .run()
    });
    assert_eq!(r.len(), 2000);
    let mut sorted = r;
    sorted.sort_unstable();
    let expected: Vec<u64> = (0..2000u64).map(|x| (x + 1) * 2 + 10).collect();
    assert_eq!(sorted, expected);
}

/// Nested same-pool pipelines with large inner batches: the outer stage
/// worker parks in the inner collector for the whole inner run, so the inner
/// chain can never rely on pool admission and must take the dedicated-thread
/// path. Small pool + `n > buffer` keeps the inner feeder non-inline.
#[test]
#[cfg_attr(miri, ignore)]
fn test_nested_stream_many_stages_no_deadlock() {
    let outer: Vec<u64> = run_with_deadlock_watchdog(|| {
        let pool = youpipe::ComputePool::new(4);
        stream(0..4u64)
            .with_compute_pool(pool.clone())
            .stage(move |_x| {
                let inner: Vec<u64> = stream(0..2000u64)
                    .with_compute_pool(pool.clone())
                    .stage(|v| v + 1)
                    .stage(|v| v * 2)
                    .stage(|v| v ^ 7)
                    .stage(|v| v.wrapping_mul(5))
                    .run();
                inner.len() as u64
            })
            .run()
    });
    assert_eq!(outer, vec![2000; 4]);
}

// ── StreamPipe::for_each ──

#[test]
fn test_stream_for_each_unordered_sees_all_items() {
    // miri: keep n within the default feeder buffer (256) so the feeder
    // stays on the inline path — larger n would take the pool/dedicated
    // feeder path, which miri only pays for in interpretation time, not
    // correctness (see prelude doc).
    let n: u64 = if cfg!(miri) {
        100
    } else {
        1_000
    };
    let mut total = 0u64;
    stream(0..n).stage(|x| x * 2).for_each(|x| total += x);
    assert_eq!(total, (0..n).map(|x| x * 2).sum::<u64>());
}

#[test]
fn test_stream_for_each_ordered_sees_input_order() {
    let mut seen: Vec<u32> = Vec::new();
    stream(0..150u32)
        .stage(|x| {
            // Skew completion order: even items take the slow path.
            if x % 2 == 0 {
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            x
        })
        .ordered()
        .for_each(|x| seen.push(x));
    assert_eq!(seen, (0..150).collect::<Vec<u32>>());
}

/// Regression: a `stream()` started *inside* a closure that already runs on
/// the same compute pool must not deadlock. The feeder job used to go
/// through `submit`, which pushes onto the calling worker's local LIFO deque
/// when the caller is a same-pool worker — breaking the injector-FIFO
/// ordering ("feeder before its stage workers") that the stream concurrency
/// argument depends on: every worker then parks on an empty channel recv
/// while the feeder sits unreachable behind the blocked caller's deque.
///
/// Both nesting shapes are covered: fused outer (closure runs in a pool
/// worker via hybrid dispatch) and streaming outer (closure runs in a stage
/// worker). Inner batches exceed the default 256-slot feeder buffer so the
/// pool-feeder path (not the inline one) is exercised.
///
/// The whole scenario runs on a small **dedicated** pool so the test is
/// self-contained: under the parallel test suite the *global* pool is
/// arbitrarily occupied by other tests' long-running jobs, which starves this
/// pipeline's feeder/worker jobs for seconds and turns a liveness regression
/// test into a flaky timing test. On the dedicated pool the healthy path
/// completes in milliseconds; the 30 s bound only ever trips on a real
/// deadlock (which never completes). Runs on a helper thread so a regression
/// fails the test instead of hanging the harness; miri is skipped
/// (thread-count stress the interpreter cannot make progress on).
#[test]
#[cfg_attr(miri, ignore)]
fn test_nested_stream_inside_pool_worker_no_deadlock() {
    let pool = youpipe::ComputePool::new(8);
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        // Shape 1: fused outer — for_each closures run on pool workers.
        let p1 = pool.clone();
        pipe(0..4u64)
            .with_compute_pool(pool.clone())
            .for_each(move |_x: u64| {
                let inner: Vec<u64> = stream(0..2000u64)
                    .with_compute_pool(p1.clone())
                    .stage(|v: u64| v + 1)
                    .run();
                assert_eq!(inner.len(), 2000);
            });

        // Shape 2: streaming outer — stage closures run on pool workers.
        let p2 = pool.clone();
        let outer: Vec<u64> = stream(0..4u64)
            .with_compute_pool(pool.clone())
            .stage(move |_x: u64| {
                let inner: Vec<u64> = stream(0..2000u64)
                    .with_compute_pool(p2.clone())
                    .stage(|v: u64| v + 1)
                    .run();
                inner.len() as u64
            })
            .run();
        assert_eq!(outer, vec![2000; 4]);
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("nested stream inside a pool worker deadlocked");
}

#[test]
#[cfg(feature = "tokio-runtime")]
fn test_stream_for_each_after_async_stage() {
    let mut count = 0usize;
    stream(0..64usize)
        .stage_async(|x| async move { x + 1 })
        .for_each(|_| count += 1);
    assert_eq!(count, 64);
}

#[test]
fn test_stream_for_each_empty_input() {
    let mut calls = 0;
    let empty: Vec<u64> = Vec::new();
    stream(empty).stage(|x: u64| x).for_each(|_| calls += 1);
    assert_eq!(calls, 0);
}

// ── quick setters ──

#[test]
fn test_compute_workers_clamped_above_max() {
    // 10_000 ≫ MAX_COMPUTE_WORKERS (511): the clamp keeps the config usable
    // instead of tripping the scheduler's THREADS_MAX assert. Uses the
    // streaming path: since the fused path now *honours* the budget (a
    // non-default value creates a transient pool), a fused variant of this
    // test would spawn 511 threads on every run.
    let n = 100u64;
    let mut r: Vec<u64> = stream(0..n)
        .with_compute_workers(10_000)
        .stage(|x| x + 1)
        .run();
    r.sort_unstable();
    assert_eq!(r, (1..=n).collect::<Vec<_>>());
}

#[test]
fn test_max_compute_workers_constant() {
    // The public constant must mirror what ComputePool::new actually clamps
    // to (511 on 64-bit) — README examples rely on it.
    assert_eq!(youpipe::MAX_COMPUTE_WORKERS, 511);
}

// ── Unbalanced failure-path drop accounting ──
//
// The Unbalanced workload fans the batch out as per-chunk trees
// (`hybrid_dispatch`). On batch failure the driver must clean each
// successful chunk's executed range exactly once while failed chunks clean
// themselves inside the recursion's guards. These tests pin that accounting
// with per-item Drop counters on both input and output.
//
// Determinism: the failing item is the LAST item of the batch. Everything
// before it completed its composed closure, so exactly `n − 1` outputs were
// ever written; any double-drop or mis-scoped cleanup moves the counters off
// these exact totals.

/// Burn ~200 ns/item so the batch is big enough to exercise deep chunk
/// trees (n = 64k over 32 chunks ≈ 2k items per chunk).
fn unbalanced_burn(x: u64) -> u64 {
    let mut r = x;
    let iters: u32 = if cfg!(miri) {
        4
    } else {
        2_000
    };
    for _ in 0..iters {
        r = r.wrapping_mul(7).wrapping_add(13);
    }
    r
}

// Test-side counters/durations are bench-scale (≪ 2^32), so the u64→usize
// narrowing below cannot lose bits on any realistic target.
#[allow(clippy::cast_possible_truncation)]
#[test]
fn test_unbalanced_panic_drop_accounting() {
    struct InCounter {
        drops: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for InCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    struct OutCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OutCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        65_536
    };
    let in_drops = Arc::new(AtomicUsize::new(0));
    let out_drops = Arc::new(AtomicUsize::new(0));
    let (ic, oc) = (in_drops.clone(), out_drops.clone());
    let items: Vec<InCounter> = (0..n)
        .map(|i| InCounter {
            drops: ic.clone(),
            val: i,
        })
        .collect();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        pipe(items)
            .with_workload(Workload::Unbalanced)
            .map(move |d: InCounter| {
                // The failing item is the LAST of the whole batch: any leaf
                // still holding earlier items will finish them (they do not
                // panic), so exactly `n-1` outputs are ever written,
                // deterministically, regardless of chunk execution order.
                assert!(d.val != n - 1, "unbalanced panic boom");
                let v = unbalanced_burn(d.val);
                // `d` (the input) drops here, inside the composed closure,
                // after the check — so on panic the guard drops it instead.
                drop(d);
                v
            })
            .map(move |_v: u64| OutCounter { drops: oc.clone() })
            .collect()
    }));
    assert!(result.is_err(), "panic must propagate");

    // Inputs: all n consumed by their closures (the panicking one included).
    assert_eq!(
        in_drops.load(Ordering::Relaxed),
        n as usize,
        "every input must drop exactly once"
    );
    // Outputs: exactly n-1 were ever written (item n-1 never produced one).
    // Panic-path cleanup is best-effort for *stolen siblings* (documented
    // single-tree behaviour, see `TryStrategy`): subtrees stolen from the
    // panicking chunk's tree complete successfully but are skipped by the
    // unwinding owner, so up to one chunk's worth of outputs may leak. The
    // hard guarantees under test: everything the cleanup *does* touch drops
    // exactly once (no double-drop → `dropped ≤ n-1`), and the leak stays
    // confined to the panicking chunk (→ `dropped ≥ n-1-chunk_span`, where
    // chunk_span is one top-level chunk's max range).
    let dropped = out_drops.load(Ordering::Relaxed);
    let chunk_span = n.div_ceil(32) as usize;
    assert!(
        dropped <= (n - 1) as usize,
        "outputs must never double-drop (dropped {dropped} > {})",
        n - 1
    );
    assert!(
        dropped >= (n as usize - 1).saturating_sub(chunk_span),
        "leak must stay inside the panicking chunk (dropped {dropped},          lower bound {})",
        (n as usize - 1) - chunk_span
    );
}

#[allow(clippy::cast_possible_truncation)]
#[test]
fn test_unbalanced_try_collect_err_drop_accounting() {
    // Same accounting through the try_collect failure path (op `Err`, not a
    // panic): successful chunks/halves are cleaned by the driver, the failing
    // chunk's already-written prefix by the recursion's internal cleanup.
    struct OutCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OutCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        65_536
    };
    let out_drops = Arc::new(AtomicUsize::new(0));
    let oc = out_drops.clone();

    let r: Result<Vec<OutCounter>, &str> = pipe(0..n)
        .with_workload(Workload::Unbalanced)
        .try_map(move |v: u64| {
            if v == n - 1 {
                Err("terminal failure")
            } else {
                Ok(v)
            }
        })
        .map(move |v: u64| {
            std::hint::black_box(unbalanced_burn(v));
            OutCounter { drops: oc.clone() }
        })
        .try_collect();
    assert!(r.is_err(), "the Err item must short-circuit the batch");

    // Identical totals to the panic variant: n-1 outputs ever written, each
    // dropped exactly once.
    assert_eq!(
        out_drops.load(Ordering::Relaxed),
        (n - 1) as usize,
        "every written output must drop exactly once on Err"
    );
}

// ── reduce terminals (todo perf #3) ──

/// All reduce terminals vs the sequential equivalent: owned + borrowed,
/// filter chains, the fold/count conveniences, and the stream fuse path.
#[test]
fn test_reduce_terminals_match_sequential() {
    use youpipe::pipe_ref;
    let data: Vec<u64> = (0..20_057u64)
        .map(|i| i.wrapping_mul(2_654_435_761))
        .collect();
    let f = |&x: &u64| x.wrapping_mul(3).wrapping_add(7);
    let g = |x: u64| x.wrapping_mul(3).wrapping_add(7);
    let mapped: Vec<u64> = data.iter().map(|&x| g(x)).collect();

    let expected_sum: u64 = mapped.iter().copied().sum();
    let expected_min = mapped.iter().copied().min();
    let expected_max = mapped.iter().copied().max();
    let expected_digit_sum: u64 = mapped.iter().map(|x| x % 10).sum();

    // Borrowed core.
    let s: u64 = pipe_ref(&data).map(f).sum();
    assert_eq!(s, expected_sum);
    assert_eq!(pipe_ref(&data).map(f).min(), expected_min);
    assert_eq!(pipe_ref(&data).map(f).max(), expected_max);
    assert_eq!(
        pipe_ref(&data).map(f).count(),
        mapped.len(),
        "count is post-filter cardinality"
    );
    assert_eq!(
        pipe_ref(&data)
            .map(f)
            .fold(0u64, |a, x: u64| a + x % 10, |a, b| a + b),
        expected_digit_sum,
        "fold with a different accumulator type"
    );
    assert_eq!(
        pipe_ref(&data).map(f).reduce(u64::wrapping_add),
        Some(expected_sum)
    );

    // Owned core.
    let s: u64 = pipe(data.clone()).map(g).sum();
    assert_eq!(s, expected_sum);

    // Filter chain: only survivors fold.
    let kept: u64 = mapped.iter().copied().filter(|x| x % 3 == 0).sum();
    let s: u64 = pipe_ref(&data).map(f).filter(|&x: &u64| x % 3 == 0).sum();
    assert_eq!(s, kept);
    assert_eq!(
        pipe_ref(&data).map(f).filter(|&x: &u64| x % 3 == 0).count(),
        mapped.iter().filter(|x| *x % 3 == 0).count()
    );

    // try variants (no-filter fast path + filter path).
    let r = pipe_ref(&data)
        .try_map(|&x| -> Result<u64, &str> { Ok(f(&x)) })
        .try_reduce(u64::wrapping_add);
    assert_eq!(r, Ok(Some(expected_sum)));
    let r = pipe_ref(&data)
        .try_map(|&x| -> Result<u64, &str> { Ok(f(&x)) })
        .filter(|&x: &u64| x % 3 == 0)
        .try_fold(0u64, |a, x: u64| a + x % 10, |a, b| a + b);
    let kept_digits: u64 = mapped
        .iter()
        .copied()
        .filter(|x| x % 3 == 0)
        .map(|x| x % 10)
        .sum();
    assert_eq!(r, Ok(kept_digits));

    // Stream fuse path: pure sync chain reduces on the fused core.
    let s: u64 = stream(data.iter().copied()).stage(g).sum();
    assert_eq!(s, expected_sum);
    assert_eq!(
        stream(data.iter().copied())
            .stage(g)
            .reduce(u64::wrapping_add),
        Some(expected_sum)
    );
    assert_eq!(stream(0..1000u64).stage(|x| x * 2).count(), 1000);
}

/// Empty and fully-filtered inputs: `None` / identity returns everywhere.
#[test]
fn test_reduce_empty_inputs() {
    use youpipe::{pipe_ref, stream};
    let empty: Vec<u64> = Vec::new();
    assert_eq!(
        pipe(empty.clone()).map(|x: u64| x + 1).reduce(|a, b| a + b),
        None
    );
    assert_eq!(pipe_ref(&empty).map(|&x| x + 1).reduce(|a, b| a + b), None);
    assert_eq!(pipe_ref(&empty).map(|&x| x + 1).min(), None);
    assert_eq!(pipe_ref(&empty).map(|&x| x + 1).max(), None);
    let s: u64 = pipe_ref(&empty).map(|&x| x + 1).sum();
    assert_eq!(s, 0);
    assert_eq!(pipe_ref(&empty).map(|&x| x + 1).count(), 0);
    assert_eq!(
        pipe_ref(&empty)
            .map(|&x| x + 1)
            .fold(42u64, |a, x: u64| a + x, |a, b| a + b),
        42
    );
    // Everything filtered out reduces to None.
    assert_eq!(
        pipe_ref(&(0..100u64).collect::<Vec<_>>())
            .map(|&x| x)
            .filter(|_: &u64| false)
            .reduce(|a, b| a + b),
        None
    );
    // try + stream empties.
    let r: Result<Option<u64>, &str> = pipe(empty)
        .try_map(|x| -> Result<u64, &str> { Ok(x + 1) })
        .try_reduce(|a, b| a + b);
    assert_eq!(r, Ok(None));
    let s: u64 = stream(Vec::<u64>::new()).stage(|x: u64| x + 1).sum();
    assert_eq!(s, 0);
    assert_eq!(stream(Vec::<u64>::new()).stage(|x: u64| x + 1).count(), 0);
}

/// A panicking stage inside a reduce leaf must drop every owned input item
/// exactly once (the reduce leaf guard's unread-tail cleanup + the tree's
/// propagation), matching the collect terminals' contract.
#[allow(clippy::cast_possible_truncation)] // test sizes fit usize everywhere
#[test]
fn test_reduce_panic_drop_accounting() {
    struct DropCounter {
        drops: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        50_000
    };
    let boom_at = n / 2;
    let drops = Arc::new(AtomicUsize::new(0));
    let d = drops.clone();
    let items: Vec<DropCounter> = (0..n)
        .map(|i| DropCounter {
            drops: d.clone(),
            val: i,
        })
        .collect();

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        pipe(items)
            .map(move |c: DropCounter| {
                assert!(c.val != boom_at, "boom at {boom_at}");
                c.val
            })
            .reduce(u64::wrapping_add)
    }));
    assert!(r.is_err(), "panic must propagate through the reduce tree");
    assert_eq!(
        drops.load(Ordering::Relaxed),
        n as usize,
        "every input must drop exactly once on panic"
    );
}

/// try_reduce short-circuits on the first Err: the error propagates and
/// every owned input item drops exactly once.
#[allow(clippy::cast_possible_truncation)] // test sizes fit usize everywhere
#[test]
fn test_try_reduce_short_circuits() {
    use youpipe::pipe_ref;
    struct InCounter {
        drops: Arc<AtomicUsize>,
        val: u64,
    }
    impl Drop for InCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let n: u64 = if cfg!(miri) {
        2_000
    } else {
        65_536
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let d = drops.clone();
    let items: Vec<InCounter> = (0..n)
        .map(|i| InCounter {
            drops: d.clone(),
            val: i,
        })
        .collect();

    let r: Result<Option<u64>, &str> = pipe(items)
        .try_map(move |c: InCounter| {
            if c.val % 3 == 1 {
                Err("odd man out")
            } else {
                Ok(c.val)
            }
        })
        .try_reduce(u64::wrapping_add);
    assert_eq!(r, Err("odd man out"));
    assert_eq!(
        drops.load(Ordering::Relaxed),
        n as usize,
        "every input must drop exactly once on Err"
    );

    // Borrowed variant: error at a fixed index, deterministic first error.
    let data: Vec<u64> = (0..10_000u64).collect();
    let r: Result<Option<u64>, &str> = pipe_ref(&data)
        .try_map(|&x| -> Result<u64, &str> {
            if x == 5_000 {
                Err("stop")
            } else {
                Ok(x)
            }
        })
        .try_reduce(|a, b| a + b);
    assert_eq!(r, Err("stop"));
}
