//! Shared plumbing for the fuzz targets: a byte-cursor decoder that maps any
//! input onto a valid program, plus the serial reference models the
//! concurrent implementations are asserted against.
//!
//! Each fuzz target links this as `mod common;` and uses a different subset,
//! so unused-item warnings here are expected.
#![allow(dead_code)]

/// Cursor over the fuzzer-provided bytes.
///
/// Every read is infallible: past-the-end reads return `0`, so any byte
/// string decodes to some (possibly trivial) program and the harness never
/// bails out on "malformed" input.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    pub fn new(data: &[u8]) -> Reader<'_> {
        Reader { data, pos: 0 }
    }

    pub fn u8(&mut self) -> u8 {
        let b = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        b
    }

    pub fn u64(&mut self) -> u64 {
        let mut buf = [0u8; 8];
        for b in &mut buf {
            *b = self.u8();
        }
        u64::from_le_bytes(buf)
    }

    /// Value in `0..max` (`max > 0`), consuming one byte.
    pub fn pick(&mut self, max: usize) -> usize {
        debug_assert!(max > 0);
        usize::from(self.u8()) % max
    }

    /// Remaining, unconsumed bytes — drives per-item / per-op streams.
    pub fn rest(&self) -> &[u8] {
        &self.data[self.pos.min(self.data.len())..]
    }
}

/// A deterministic, panic-free item operation. The harness compiles these
/// into stage closures; [`run_serial`] interprets the same list as the serial
/// reference the concurrent output must match exactly.
///
/// All arithmetic is wrapping: stage closures must not panic under debug
/// assertions (an overflow panic in a harness closure would be a false
/// positive, not a youpipe bug).
#[derive(Clone, Copy)]
pub enum Op {
    /// `x -> x.wrapping_mul(mul).wrapping_add(add)`; `mul` is forced odd so
    /// the map stays a bijection and filter predicates keep biting.
    Map { mul: u64, add: u64 },
    /// Keep `x` iff `(x & mask) == expect`.
    Filter { mask: u64, expect: u64 },
    /// Fail with `Err(x)` iff `x == magic` — fallible chains only.
    Fail { magic: u64 },
    /// Emit `fanout` copies `x.wrapping_add(i)`, `i` in `0..fanout`.
    Expand { fanout: u8 },
}

impl Op {
    pub fn apply(&self, x: u64, out: &mut Vec<u64>) -> Result<(), u64> {
        match *self {
            Op::Map { mul, add } => out.push(x.wrapping_mul(mul).wrapping_add(add)),
            Op::Filter { mask, expect } => {
                if (x & mask) == expect {
                    out.push(x);
                }
            },
            Op::Fail { magic } => {
                if x == magic {
                    return Err(x);
                }
                out.push(x);
            },
            Op::Expand { fanout } => {
                for i in 0..u64::from(fanout) {
                    out.push(x.wrapping_add(i));
                }
            },
        }
        Ok(())
    }
}

/// Serial reference: fold `items` through `ops`, short-circuiting on the
/// first `Err` (mirrors the fused `try_map` first-error contract).
pub fn run_serial(ops: &[Op], items: &[u64]) -> Result<Vec<u64>, u64> {
    let mut cur = items.to_vec();
    for op in ops {
        let mut next = Vec::with_capacity(cur.len());
        for &x in &cur {
            op.apply(x, &mut next)?;
        }
        cur = next;
    }
    Ok(cur)
}

/// Decode an `Op` of the requested kind (`0` map, `1` filter, `2` fail,
/// `3` expand) with data-driven parameters.
pub fn decode_op(r: &mut Reader<'_>, kind: u8) -> Op {
    match kind % 4 {
        0 => Op::Map {
            mul: r.u64() | 1, // force odd: keep the map a bijection
            add: r.u64(),
        },
        1 => Op::Filter {
            mask: r.u64(),
            expect: r.u64(),
        },
        2 => Op::Fail { magic: r.u64() },
        _ => Op::Expand { fanout: r.u8() % 4 },
    }
}

// ── Op → stage-closure compilers, shared by the pipeline/stream targets ──

pub fn map_closure(op: Op) -> impl Fn(u64) -> u64 {
    let Op::Map { mul, add } = op else {
        unreachable!("template mismatch")
    };
    move |x: u64| x.wrapping_mul(mul).wrapping_add(add)
}

/// First-stage variant for `pipe_ref`, whose items arrive as `&u64`.
pub fn map_ref_closure(op: Op) -> impl Fn(&u64) -> u64 {
    let Op::Map { mul, add } = op else {
        unreachable!("template mismatch")
    };
    move |x: &u64| x.wrapping_mul(mul).wrapping_add(add)
}

pub fn filter_closure(op: Op) -> impl Fn(&u64) -> bool {
    let Op::Filter { mask, expect } = op else {
        unreachable!("template mismatch")
    };
    move |x: &u64| (x & mask) == expect
}

pub fn fail_closure(op: Op) -> impl Fn(u64) -> Result<u64, u64> {
    let Op::Fail { magic } = op else {
        unreachable!("template mismatch")
    };
    move |x: u64| {
        if x == magic {
            Err(x)
        } else {
            Ok(x)
        }
    }
}

pub fn expand_closure(op: Op) -> impl Fn(u64, &mut Vec<u64>) {
    let Op::Expand { fanout } = op else {
        unreachable!("template mismatch")
    };
    move |x: u64, out: &mut Vec<u64>| {
        for i in 0..u64::from(fanout) {
            out.push(x.wrapping_add(i));
        }
    }
}
