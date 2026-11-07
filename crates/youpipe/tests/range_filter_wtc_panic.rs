//! Correctness + panic-drop accounting for `pipe_range`'s generation filter
//! collect forced onto the write-then-compact core via `YOUPIPE_FILTER_COLLECT
//! =wtc` (todo #46): the generated wtc leaf writes survivors into the n-slot
//! buffer behind an output-half guard, and the driver's meta walk must drop
//! exactly the survivors of leaves that completed before a panic.
//!
//! Own test binary on purpose: the variant knob is a process-wide `OnceLock`
//! seeded from the environment, so the `set_var` below must not race another
//! test's env read (same discipline as `filter_collect_panic.rs`).

use std::{
    panic,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use youpipe::pipe_range;

#[test]
fn test_range_filter_gen_wtc_matches_and_panic_accounting() {
    struct DropCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    // SAFETY: sole env touch in this binary, before any pool thread spawns.
    unsafe { std::env::set_var("YOUPIPE_FILTER_COLLECT", "wtc") };

    let n: usize = if cfg!(miri) {
        500
    } else {
        20_000
    };
    let keep = |i: &usize| i % 3 != 0;

    // Correctness: the compacted output matches the sequential filter.
    let out: Vec<usize> = pipe_range(1_000..1_000 + n)
        .map(|i: usize| i.wrapping_mul(3))
        .filter(keep)
        .collect();
    let expected: Vec<usize> = (1_000..1_000 + n)
        .map(|i| i.wrapping_mul(3))
        .filter(keep)
        .collect();
    assert_eq!(out, expected);

    // Panic: keyed on the CALL index (construction order across leaves is
    // scheduling-dependent, but every kept item constructs exactly once, so
    // any index below the survivor count is always reached).
    for panic_call in [0, n / 5, n / 2] {
        let drops = Arc::new(AtomicUsize::new(0));
        let constructed = Arc::new(AtomicUsize::new(0));
        let (d, c) = (drops.clone(), constructed.clone());
        let nc = Arc::new(AtomicUsize::new(0));

        let r = panic::catch_unwind(panic::AssertUnwindSafe({
            #[cfg(miri)]
            let pipe = || {
                // miri: one available CPU would take the serial fallback and
                // never run the wtc tree.
                pipe_range(0..n).with_compute_workers(2)
            };
            #[cfg(not(miri))]
            let pipe = || pipe_range(0..n);
            move || {
                let out: Vec<DropCounter> = pipe()
                    .filter(keep)
                    .map(move |_: usize| {
                        let nth = nc.fetch_add(1, Ordering::Relaxed);
                        assert!(nth != panic_call, "boom");
                        c.fetch_add(1, Ordering::Relaxed);
                        DropCounter { drops: d.clone() }
                    })
                    .collect();
                // Unreachable when the panic fired; otherwise `out`'s drop
                // keeps the accounting below valid.
                drop(out);
            }
        }));
        assert!(
            r.is_err(),
            "a construction index below the survivor count is always reached"
        );
        assert_eq!(
            drops.load(Ordering::Relaxed),
            constructed.load(Ordering::Relaxed),
            "every constructed output must be dropped exactly once (no leak, no double drop) \
             [panic at call {panic_call}]"
        );
    }
}
