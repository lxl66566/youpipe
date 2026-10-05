//! Panic-drop accounting for the count-then-place by-ref filter collect,
//! forced via `YOUPIPE_FILTER_COLLECT=ctp`.
//!
//! Own test binary on purpose: the variant knob is a process-wide `OnceLock`
//! seeded from the environment, so the `set_var` below must not race another
//! test's env read (nextest runs one test per process anyway; this keeps plain
//! `cargo test` sound too).

use std::{
    panic,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use youpipe::pipe_ref;

/// Same shape as `test_pipe_range_panic_drop_accounting`, but through the
/// two-pass filter collect: a pass-2 panic must not leak the outputs of
/// subtrees that already finished placing into the shared buffer — every
/// constructed output drops exactly once.
///
/// The panic is keyed on the CALL INDEX, not the item: the count pass runs the
/// chain once over all `n` items, then the place pass runs it again. A low
/// index (`< n`) fires in any execution shape; a high index (`>= n`) only
/// fires when the parallel two-pass path actually ran — miri reports one
/// available CPU and would otherwise take the serial single-pass fallback, so
/// it pins a 2-worker pool to keep the place pass exercised.
#[test]
fn test_ctp_filter_panic_drop_accounting() {
    struct In;
    struct OutCounter {
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OutCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    // SAFETY: sole env touch in this binary, before any pool thread spawns.
    unsafe { std::env::set_var("YOUPIPE_FILTER_COLLECT", "ctp") };

    let n: usize = if cfg!(miri) {
        500
    } else {
        20_000
    };
    let items: Vec<In> = (0..n).map(|_| In).collect();

    for (panic_call, must_panic) in [
        (n / 3, true),
        (n / 2, true),
        (n + n / 4, false),
        (n + n / 2, false),
        (n + 3 * n / 4, false),
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let constructed = Arc::new(AtomicUsize::new(0));
        let (d, c) = (drops.clone(), constructed.clone());
        let nc = Arc::new(AtomicUsize::new(0));

        let r = panic::catch_unwind(panic::AssertUnwindSafe({
            let items = &items;
            move || {
                let filtered = pipe_ref(items).filter(|_: &&In| true);
                // miri: one available CPU would take the serial fallback and
                // never run the two-pass place tree.
                #[cfg(miri)]
                let filtered = filtered.with_compute_workers(2);
                let out: Vec<OutCounter> = filtered
                    .map(move |_: &In| {
                        let nth = nc.fetch_add(1, Ordering::Relaxed);
                        assert!(nth != panic_call, "boom");
                        c.fetch_add(1, Ordering::Relaxed);
                        OutCounter { drops: d.clone() }
                    })
                    .collect();
                // Unreachable when the panic fired; if scheduling somehow
                // avoided it, `out`'s drop still keeps the accounting below
                // valid.
                drop(out);
            }
        }));
        if must_panic {
            assert!(
                r.is_err(),
                "a sub-n call index is always reached, so this must panic"
            );
        }
        assert_eq!(
            drops.load(Ordering::Relaxed),
            constructed.load(Ordering::Relaxed),
            "every constructed output must be dropped exactly once (no leak, no double drop) \
             [panic at call {panic_call}]"
        );
    }
}
