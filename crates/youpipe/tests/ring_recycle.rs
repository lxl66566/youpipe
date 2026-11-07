//! End-to-end semantics of `YOUPIPE_RING_CACHE=1` (todo #45): every
//! handoff factory parks its fresh ring's shared handle, and the next
//! construction of the same (channel type, capacity) rebuilds endpoints
//! over the parked allocation via the crossfire fork's
//! `ChannelShared::try_reset`. Multi-run pipelines must stay correct
//! when every ring from run N is recycled into run N+1, and a cancelled
//! run's leftover items must still be released (deferred to the next
//! same-key construction — the documented trade).
//!
//! One `#[test]` function on purpose: the knob is read through a
//! process-wide `OnceLock`, so the env var must be set before the first
//! `run()`/channel construction in this process (same discipline as
//! `sharded_terminal.rs`).

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use youpipe::{FenceMode, graph, handoff::channel, stream, sync::CancellationToken};

/// Miri runs ~100x slower; the recycling topology is size-independent.
fn n() -> usize {
    if cfg!(miri) {
        300
    } else {
        5_000
    }
}

static DROPS: AtomicUsize = AtomicUsize::new(0);

struct DropCounted(u64);

impl Drop for DropCounted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Unique capacity: isolates the leftover-release scenario's cache key.
const CAP: usize = 33;

#[test]
fn ring_cache_semantics() {
    // SAFETY: this fn is the only test in this binary and no channel
    // construction has happened yet — the knob's OnceLock initializes
    // after this line.
    unsafe { std::env::set_var("YOUPIPE_RING_CACHE", "1") };

    let items: Vec<u64> = (0..n() as u64).collect();

    // ── Stream chains: run-to-run identity over recycled rings ──
    for _round in 0..3 {
        let mut got = stream(items.clone())
            .with_cancel(CancellationToken::new())
            .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
            .run();
        got.sort_unstable();
        let expected: Vec<u64> = items.iter().map(|x| x * 3 + 1).collect();
        assert_eq!(got, expected);
    }

    // ── Fenced chain: feeder + mid MPMC rings + sharded terminal, all
    //    recycled between rounds ──
    for _round in 0..3 {
        // The fence batches; it does not order (`.ordered()` is that
        // primitive) — assert the multiset, the same caliber as the
        // sharded-terminal suites.
        let mut got = stream(items.clone())
            .stage(|x: u64| x.wrapping_mul(2))
            .fence(FenceMode::Chunked(NonZeroUsize::new(50).unwrap()))
            .stage(|x: u64| x.wrapping_add(1))
            .run();
        got.sort_unstable();
        let expected: Vec<u64> = items.iter().map(|x| x * 2 + 1).collect();
        assert_eq!(got, expected);
    }

    // ── Graph engine: per-run edge rings recycled across runs ──
    for _round in 0..3 {
        let total = Arc::new(AtomicU64::new(0));
        let acc = Arc::clone(&total);
        let mut g = graph();
        let src = g.source("src", items.clone());
        let a = g.node("a", |x: u64| x.wrapping_mul(5).wrapping_add(7));
        let out = g.sink("out", move |x: u64| {
            acc.fetch_add(x, Ordering::Relaxed);
        });
        g.edge(src, a).edge(a, out);
        g.run();
        let expected: u64 = items.iter().map(|x| x * 5 + 7).sum();
        assert_eq!(total.load(Ordering::Relaxed), expected);
    }

    // ── Leftover-release contract: a channel abandoned with items inside
    //    parks its handle; the items' drops may defer to the next
    //    same-key construction, and MUST happen then ──
    {
        let (tx, rx) = channel::<DropCounted>(CAP);
        for i in 0..8 {
            tx.send(DropCounted(i)).expect("rx alive");
        }
        drop(rx);
        drop(tx); // 8 items abandoned in the parked ring
    }
    // The parked handle keeps the ring alive; items may or may not have
    // dropped yet. The next same-key construction takes the entry, the
    // reset refuses the non-empty ring, the handle drops, the ring's Drop
    // releases the leftovers — and the fresh channel works.
    let (tx, rx) = channel::<DropCounted>(CAP);
    let after = DROPS.load(Ordering::Relaxed);
    assert!(
        after >= 8,
        "leftover items must be released by the next same-key construction"
    );
    tx.send(DropCounted(99))
        .expect("fresh channel over the recycled key");
    assert_eq!(rx.recv().map(|d| d.0), Ok(99));
    drop(rx);
    drop(tx);
}
