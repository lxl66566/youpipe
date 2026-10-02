//! Streaming-pipeline end-to-end fuzz: `stream` chains (stage / expand_emit /
//! fence), ordered vs unordered collection, worker/buffer pins, `run` and
//! `for_each` terminals, asserted against a serial reference model.
//!
//! Two execution engines hide behind one API and both must hold:
//! - unpinned pure-`.stage()` chains take the fused pass-through;
//! - a `with_compute_workers` pin opts the chain out and runs the real streaming topology (feeder,
//!   per-stage channels, workers). The harness fuzzes both branches via the pin flag.
//!
//! The async backend is a process-global 2-worker `TokioPool` shared across
//! iterations: `run()` would otherwise construct (and immediately tear down) a
//! multi-thread runtime per iteration, which dominates fuzzing throughput.
//!
//! `.ordered()` + `.expand()` is a documented panic (single-item-per-seq
//! contract) — the harness never generates that combination.

#![no_main]

mod common;

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};

use common::{Op, Reader, decode_op, expand_closure, map_closure, run_serial};
use libfuzzer_sys::fuzz_target;
use youpipe::{FenceMode, TokioPool, stream};

fn pool() -> TokioPool {
    static POOL: OnceLock<TokioPool> = OnceLock::new();
    POOL.get_or_init(|| TokioPool::build(2).expect("global fuzz tokio runtime"))
        .clone()
}

fn fence_mode(r: &mut Reader<'_>) -> FenceMode {
    match r.pick(3) {
        0 => FenceMode::Barrier,
        k => FenceMode::Chunked(NonZeroUsize::new(k).unwrap()),
    }
}

fn decode_stage(r: &mut Reader<'_>) -> Op {
    // Streaming stages stay 1:1 in this harness (0-fanout `expand_emit` below
    // already covers cardinality reduction).
    decode_op(r, 0)
}

fn decode_expand(r: &mut Reader<'_>) -> Op {
    let mut op = decode_op(r, 3);
    if let Op::Expand { fanout } = &mut op {
        *fanout %= 6; // widen past decode_op's cap of 3; 0 = full drop
    }
    op
}

/// Terminal + order-sensitive assertion shared by all templates: exact match
/// when ordered, multiset match otherwise (completion order is unspecified).
fn finish(got: Vec<u64>, expect: &[u64], ordered: bool) {
    if ordered {
        assert_eq!(got, expect, "ordered stream diverged from serial model");
    } else {
        let mut g = got;
        let mut e = expect.to_vec();
        g.sort_unstable();
        e.sort_unstable();
        assert_eq!(g, e, "unordered stream multiset diverged from serial model");
    }
}

/// Shared pin/buffer application + terminal (`run` or a `for_each` recording
/// into a sink), asserting against the serial model. `pin`/`workers`/`buffer`
/// are passed in explicitly — macro hygiene hides closure locals from the
/// macro body.
macro_rules! emit {
    (
        $p:expr,
        $expect:expr,
        $ordered:expr,
        $for_each:expr,
        $pin:expr,
        $workers:expr,
        $buffer:expr
    ) => {{
        let p = if $pin { $p.with_compute_workers($workers) } else { $p };
        let p = p.with_buffer_size($buffer);
        let got = if $for_each {
            let sink = Arc::new(Mutex::new(Vec::new()));
            let s = Arc::clone(&sink);
            p.for_each(move |x: u64| s.lock().unwrap().push(x));
            Arc::try_unwrap(sink).unwrap().into_inner().unwrap()
        } else {
            p.run()
        };
        finish(got, &$expect, $ordered);
    }};
}

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    let flags = r.u8();
    let ordered = flags & 0b0001 != 0;
    let pin = flags & 0b0010 != 0;
    let for_each = flags & 0b0100 != 0;
    let workers = 1 + r.pick(4);
    let buffer = 1 + r.pick(8); // tiny buffers force backpressure paths

    let n = r.pick(33);
    let items: Vec<u64> = (0..n).map(|_| r.u64()).collect();

    // Templates with expand are ineligible for ordered collection (documented
    // panic) — fold them onto the ordered-compatible set.
    let tmpl = if ordered {
        r.pick(5) % 2
    } else {
        r.pick(5)
    };

    match tmpl {
        // stage × 2: fused pass-through when unpinned, real topology when pinned
        0 => {
            let ops = vec![decode_stage(&mut r), decode_stage(&mut r)];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = stream(items.clone())
                .with_async_pool(pool())
                .stage(map_closure(ops[0]))
                .stage(map_closure(ops[1]));
            let p = if ordered {
                p.ordered()
            } else {
                p
            };
            emit!(p, expect, ordered, for_each, pin, workers, buffer);
        },
        // stage → expand(0..=5, 0 = drop) → stage: cardinality changes
        1 => {
            let ops = vec![
                decode_stage(&mut r),
                decode_expand(&mut r),
                decode_stage(&mut r),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = stream(items.clone())
                .with_async_pool(pool())
                .stage(map_closure(ops[0]))
                .expand_emit(expand_closure(ops[1]))
                .stage(map_closure(ops[2]));
            emit!(p, expect, ordered, for_each, pin, workers, buffer);
        },
        // stage → fence(barrier|chunked) → stage
        2 => {
            let ops = vec![decode_stage(&mut r), decode_stage(&mut r)];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = stream(items.clone())
                .with_async_pool(pool())
                .stage(map_closure(ops[0]))
                .fence(fence_mode(&mut r))
                .stage(map_closure(ops[1]));
            let p = if ordered {
                p.ordered()
            } else {
                p
            };
            emit!(p, expect, ordered, for_each, pin, workers, buffer);
        },
        // expand → stage → expand: head and tail cardinality changes
        3 => {
            let ops = vec![
                decode_expand(&mut r),
                decode_stage(&mut r),
                decode_expand(&mut r),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = stream(items.clone())
                .with_async_pool(pool())
                .expand_emit(expand_closure(ops[0]))
                .stage(map_closure(ops[1]))
                .expand_emit(expand_closure(ops[2]));
            emit!(p, expect, ordered, for_each, pin, workers, buffer);
        },
        // stage → fence → stage → expand: fence feeding a fan-out tail
        _ => {
            let ops = vec![
                decode_stage(&mut r),
                decode_stage(&mut r),
                decode_expand(&mut r),
            ];
            let expect = run_serial(&ops, &items).expect("infallible template");
            let p = stream(items.clone())
                .with_async_pool(pool())
                .stage(map_closure(ops[0]))
                .fence(fence_mode(&mut r))
                .stage(map_closure(ops[1]))
                .expand_emit(expand_closure(ops[2]));
            emit!(p, expect, ordered, for_each, pin, workers, buffer);
        },
    }
});
