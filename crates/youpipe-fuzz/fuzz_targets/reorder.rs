//! `ReorderBuffer` fuzz: random out-of-order arrival patterns (plus
//! `flush_remaining` / `reset` interleavings) against a `BTreeMap` reference
//! model.
//!
//! The buffer's documented precondition — simultaneously-outstanding seqs
//! must stay `< capacity` apart, or slots alias and the older item is
//! silently dropped — is a caller contract, so the harness generates seqs
//! that always stay inside the window (contract violations are not bugs).

#![no_main]

mod common;

use std::collections::BTreeMap;

use common::Reader;
use libfuzzer_sys::fuzz_target;
use youpipe::ReorderBuffer;

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    // Capacity 2..=32: the interesting behaviour is small windows relative
    // to the arrival spread, not large buffers.
    let cap = 1usize << (1 + r.pick(5));
    let mut buf = ReorderBuffer::<u64>::new(cap);
    let mut model: BTreeMap<u64, u64> = BTreeMap::new();

    for b in r.rest().iter().take(256).copied() {
        match b % 8 {
            0..=5 => {
                // Pick an un-inserted seq inside a window that keeps every
                // pair of outstanding seqs < cap apart (see module docs).
                let next_expected = buf.next_expected();
                let lo = model.keys().next().copied().unwrap_or(next_expected);
                let span = u64::try_from(cap - 1).expect("cap <= 32");
                let seq = lo + u64::from(b) % span;
                if model.contains_key(&seq) {
                    continue; // duplicate seq is a contract violation, skip
                }
                let value = seq.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let mut got = Vec::new();
                buf.insert_into(seq, value, &mut |v| got.push(v));
                model.insert(seq, value);

                // Model: the contiguous run starting at the old next_expected.
                let mut ne = next_expected;
                let mut want = Vec::new();
                while let Some(v) = model.remove(&ne) {
                    want.push(v);
                    ne += 1;
                }
                assert_eq!(got, want, "flushed run diverged at seq {seq}");
                assert_eq!(buf.next_expected(), ne, "next_expected diverged");
            },
            6 => {
                let got = buf.flush_remaining();
                let want: Vec<u64> = model.values().copied().collect();
                model.clear();
                assert_eq!(got, want, "flush_remaining diverged");
            },
            _ => {
                buf.reset();
                model.clear();
            },
        }
    }
    // Dropping with items still outstanding exercises the Drop path
    // (double-drop / leak would trip the sanitizer or leak check).
});
