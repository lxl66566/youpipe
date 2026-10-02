//! Fused-pipeline end-to-end fuzz: `pipe`/`pipe_ref` chains (map / filter /
//! try_map / map_err), all knobs (`with_compute_workers`, `with_oversubscribe`,
//! `with_workload`) and all infallible/fallible terminals, asserted against a
//! serial reference model.
//!
//! The fused builder is type-state, so the stage *kinds* of a chain cannot be
//! spliced at runtime; each input instead selects one of a fixed set of chain
//! templates whose parameters are data-driven. The fused core composes a
//! chain into one closure per worker anyway — what coverage needs is every
//! builder method and terminal exercised, not every chain shape.

#![no_main]

mod common;

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use common::{
    Reader, decode_op, fail_closure, filter_closure, map_closure, map_ref_closure, run_serial,
};
use libfuzzer_sys::fuzz_target;
use youpipe::{Workload, pipe, pipe_ref};

/// Shared knob application — works on every builder state that exposes the
/// fused-path `with_*` methods (Pipe / PipeRef / TryPipe / TryPipeRef).
macro_rules! knobs {
    ($p:expr, $workload:expr, $workers:expr, $pin:expr, $factor:expr, $oversub:expr) => {{
        let p = $p.with_workload($workload);
        let p = if $pin { p.with_compute_workers($workers) } else { p };
        if $oversub { p.with_oversubscribe($factor) } else { p }
    }};
}

/// Infallible terminal — `collect` asserted in exact order, `for_each`
/// asserted as a multiset: it applies `f` in unspecified parallel order
/// (rayon `par_iter().for_each` semantics), unlike the order-preserving
/// `collect`.
macro_rules! finish {
    ($p:expr, $for_each:expr, $expect:expr) => {
        if $for_each {
            let sink = Arc::new(Mutex::new(Vec::new()));
            let s = Arc::clone(&sink);
            $p.for_each(move |x: u64| s.lock().unwrap().push(x));
            let mut got = Arc::try_unwrap(sink).unwrap().into_inner().unwrap();
            let mut want = $expect;
            got.sort_unstable();
            want.sort_unstable();
            assert_eq!(
                got, want,
                "fused for_each multiset diverged from serial model"
            );
        } else {
            let got: Vec<u64> = $p.collect();
            assert_eq!(got, $expect, "fused collect diverged from serial model");
        }
    };
}

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    let flags = r.u8();
    let borrowed = flags & 0b0001 != 0;
    let for_each = flags & 0b0010 != 0;
    let oversub = flags & 0b0100 != 0;
    // A pinned budget builds a transient pool per iteration (~ms under ASan),
    // so pin at 1/4 probability — the pinned path stays covered without
    // dominating run time.
    let pin = r.pick(4) == 0;
    let workload = match (flags >> 4) % 4 {
        0 => Workload::Balanced,
        1 => Workload::Unbalanced,
        2 => Workload::Custom(NonZeroUsize::new(1).unwrap()),
        _ => Workload::Custom(NonZeroUsize::new(16).unwrap()),
    };
    let workers = 1 + r.pick(4); // small transient pools keep ASan iterations fast
    let factor = 1 + r.pick(3);

    let n = r.pick(65);
    let items: Vec<u64> = (0..n).map(|_| r.u64()).collect();

    match r.pick(5) {
        // map × 3 (borrowed variant: exercises the pipe_ref input path)
        0 => {
            let ops = vec![
                decode_op(&mut r, 0),
                decode_op(&mut r, 0),
                decode_op(&mut r, 0),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            if borrowed {
                let p = pipe_ref(&items)
                    .map(map_ref_closure(ops[0]))
                    .map(map_closure(ops[1]))
                    .map(map_closure(ops[2]));
                finish!(
                    knobs!(p, workload, workers, pin, factor, oversub),
                    for_each,
                    expect
                );
            } else {
                let p = pipe(items.clone())
                    .map(map_closure(ops[0]))
                    .map(map_closure(ops[1]))
                    .map(map_closure(ops[2]));
                finish!(
                    knobs!(p, workload, workers, pin, factor, oversub),
                    for_each,
                    expect
                );
            }
        },
        // map → filter → map: switches the core onto the filter-aware path
        1 => {
            let ops = vec![
                decode_op(&mut r, 0),
                decode_op(&mut r, 1),
                decode_op(&mut r, 0),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            if borrowed {
                let p = pipe_ref(&items)
                    .map(map_ref_closure(ops[0]))
                    .filter(filter_closure(ops[1]))
                    .map(map_closure(ops[2]));
                finish!(
                    knobs!(p, workload, workers, pin, factor, oversub),
                    for_each,
                    expect
                );
            } else {
                let p = pipe(items.clone())
                    .map(map_closure(ops[0]))
                    .filter(filter_closure(ops[1]))
                    .map(map_closure(ops[2]));
                finish!(
                    knobs!(p, workload, workers, pin, factor, oversub),
                    for_each,
                    expect
                );
            }
        },
        // filter × 2 → map: back-to-back cardinality changes
        2 => {
            let ops = vec![
                decode_op(&mut r, 1),
                decode_op(&mut r, 1),
                decode_op(&mut r, 0),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = pipe(items.clone())
                .filter(filter_closure(ops[0]))
                .filter(filter_closure(ops[1]))
                .map(map_closure(ops[2]));
            finish!(
                knobs!(p, workload, workers, pin, factor, oversub),
                for_each,
                expect
            );
        },
        // map → try_map → filter → map: fallible chain with a post-error filter
        3 => {
            let ops = vec![
                decode_op(&mut r, 0),
                decode_op(&mut r, 2),
                decode_op(&mut r, 1),
                decode_op(&mut r, 0),
            ];
            let expect = run_serial(&ops, &items);
            let p = pipe(items.clone())
                .map(map_closure(ops[0]))
                .try_map(fail_closure(ops[1]))
                .filter(filter_closure(ops[2]))
                .map(map_closure(ops[3]));
            let got = knobs!(p, workload, workers, pin, factor, oversub).try_collect();
            assert_eq!(got, expect, "fused try_collect diverged from serial model");
        },
        // try_map → map_err → map: error mapping before the terminal
        _ => {
            let ops = vec![decode_op(&mut r, 2), decode_op(&mut r, 0)];
            let expect = run_serial(&ops, &items).map_err(|e| e.wrapping_add(1));
            let p = pipe(items.clone())
                .try_map(fail_closure(ops[0]))
                .map_err(|e: u64| e.wrapping_add(1))
                .map(map_closure(ops[1]));
            let got = knobs!(p, workload, workers, pin, factor, oversub).try_collect();
            assert_eq!(
                got, expect,
                "fused try_collect(map_err) diverged from serial model"
            );
        },
    }
});
