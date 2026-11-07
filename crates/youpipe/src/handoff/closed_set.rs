//! Closed-input bookkeeping shared by the fan-in receiver rounds — the
//! sharded terminal's `ShardSet` and the graph engine's `FanIn`: an input
//! whose channel reported `Closed` is never polled again, because every
//! `try_recv`/`recv` on a closed crossfire channel re-reads its disconnect
//! state, burning a scan slot per pass after a producer exits early.
//!
//! n <= 64 is a `u64` bitmask (one `vec![false; n]` per set was a measured
//! share of the graph engine's fixed per-run assembly cost at µs-scale
//! workloads); wider sets keep the `Vec<bool>` (the historical
//! representation — both consumers' input counts are platform/user-bounded,
//! not capped at 64).

/// Closed-input marks of one fan-in receiver round; see the module doc.
pub(crate) enum ClosedSet {
    Bits(u64),
    Wide(Vec<bool>),
}

impl ClosedSet {
    pub(crate) fn new(n: usize) -> Self {
        if n <= 64 {
            Self::Bits(0)
        } else {
            Self::Wide(vec![false; n])
        }
    }

    #[inline]
    pub(crate) fn is_closed(&self, i: usize) -> bool {
        match self {
            Self::Bits(mask) => *mask & (1 << i) != 0,
            Self::Wide(v) => v[i],
        }
    }

    #[inline]
    pub(crate) fn close(&mut self, i: usize) {
        match self {
            Self::Bits(mask) => *mask |= 1 << i,
            Self::Wide(v) => v[i] = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mark/query semantics must agree across the 64-boundary representation
    /// split (the Bits arm carries every realistic fan-in; the Wide arm is
    /// only reachable past 64 inputs/shards). Indices are `< n` by
    /// precondition — the Bits arm shifts, the Wide arm indexes.
    #[test]
    fn marks_agree_across_the_64_boundary() {
        for n in [64, 65] {
            let mut set = ClosedSet::new(n);
            for i in 0..n {
                assert!(!set.is_closed(i), "n {n}: fresh set marks {i} closed");
            }
            set.close(0);
            set.close(63);
            assert!(set.is_closed(0) && set.is_closed(63));
            assert!(!set.is_closed(1));
            if n > 64 {
                set.close(64);
                assert!(set.is_closed(64));
            }
        }
    }
}
