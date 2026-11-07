//! Process-wide recycling cache for drained-and-closed channel rings
//! (todo #45): the per-run assembly residual is ring construction —
//! graph edges pay ~1.4 µs per ring (`benchmarks/graph.md` "Per-run
//! assembly attribution"), the stream spawn walk's rings ~27 µs of
//! `with_fence/1K`'s 164 µs (`benchmarks/streaming.md`) — and a run's
//! rings die drained-and-closed, i.e. exactly the state the fork's
//! `ChannelShared::try_reset` can rebuild endpoints over. The pool
//! recycling cache's channel twin: park the shared handle at
//! construction (an `Arc` clone that keeps NO endpoint refcount, so the
//! channel lives and dies exactly as before), and on the next
//! construction of the same key take it back, `try_reset` it, and
//! rebuild the endpoint pair over the same allocation.
//!
//! Measured verdict (todo #45 A/B, 2026-11-08): FALSIFIED — the
//! allocator's tcache already recycles the freed chunks, so the reset
//! pays nearly the same per-ring initialization the constructor pays
//! while this cache adds a mutex round trip + key scan per construction
//! (~35 per with_fence run): +23.6 % on with_fence/1K, +6…+10 % on the
//! sharded_term @1K rows, graph rows noise. Default off; kept as the
//! A/B vehicle over the fork's miri-clean reset surface.
//!
//! The parked handle changes one observable: a cancelled run's leftover
//! items stay inside the parked ring until the entry is taken (miss:
//! `try_reset` refuses a non-empty ring, the handle drops, the ring's
//! Drop releases the items) or evicted — item drops can be deferred
//! past run end. Normal (EOF) runs drain every ring, so nothing defers.
//!
//! Key: (channel type, capacity). The concrete `ChannelShared<F>`
//! carries both the item type and the flavor family, so a `TypeId` of it
//! plus the capacity is a strict key — same-size-different-type can
//! never cross-recycle (the downcast itself would fail). Entries: LRU
//! with slot 0 the most recent, byte-budgeted (the sharded terminal
//! parks ~32 shard rings per run; the chain-floor's lifted rings are
//! excluded by the per-ring byte cap — their construction amortizes
//! over soak-length runs anyway).

use std::{
    any::{Any, TypeId},
    sync::{Arc, Mutex, OnceLock, PoisonError},
};

use crossfire::{ChannelShared, ReceiverType, SenderType, flavor::Flavor};

/// Parked-ring budget: covers the bench shapes' working set (32 sharded
/// terminal rings + the graph/stream mid rings ≈ 40) while bounding what
/// a construction-heavy workload can pin.
const MAX_ENTRIES: usize = 48;

/// Per-ring byte cap: the default 256-slot/16-byte rings are ~4 KiB; the
/// chain floor's 64K-slot rings (0.5–1 MiB) are the soak shapes where
/// construction is amortized — parking those trades real memory for a
/// rounding error.
const MAX_RING_BYTES: usize = 64 << 10;

/// Total parked bytes across keys.
const MAX_TOTAL_BYTES: usize = 2 << 20;

struct Entry {
    key: (TypeId, usize),
    bytes: usize,
    shared: Arc<dyn Any + Send + Sync>,
}

#[derive(Default)]
struct RingCache {
    slots: Vec<Entry>,
    total_bytes: usize,
}

static CACHE: OnceLock<Mutex<RingCache>> = OnceLock::new();

fn lock() -> std::sync::MutexGuard<'static, RingCache> {
    CACHE
        .get_or_init(|| Mutex::new(RingCache::default()))
        .lock()
        // Nothing panics under this lock (reset/drop of a quiescent ring
        // takes no user code and no other lock), so poisoning can only
        // come from a panicking test elsewhere in the process;
        // `into_inner` keeps one such panic from cascading.
        .unwrap_or_else(PoisonError::into_inner)
}

/// Take the newest parked shared of `(ChannelShared<F>, capacity)` and,
/// when its ring is resettable (`try_reset` claims both endpoint
/// refcounts — the previous run must have finished draining), rebuild the
/// endpoint pair over it. `None` = miss: build fresh (the caller parks
/// the fresh handle via [`park_shared`]). The env-knob gate lives at the
/// factory call sites, so this cache stays env-free and unit-testable.
pub(crate) fn recycled<F, S, R>(capacity: usize) -> Option<(S, R)>
where
    F: Flavor,
    S: SenderType<Flavor = F>,
    R: ReceiverType<Flavor = F>,
{
    let key = (TypeId::of::<ChannelShared<F>>(), capacity);
    let shared = {
        let mut cache = lock();
        let idx = cache.slots.iter().position(|e| e.key == key)?;
        let entry = cache.slots.remove(idx);
        cache.total_bytes -= entry.bytes;
        // Erased -> concrete; the key pins the type, this cannot miss.
        entry.shared.downcast::<ChannelShared<F>>().ok()?
    };
    if !shared.try_reset() {
        return None; // endpoints still alive, or leftovers: drop, miss
    }
    // The same construction `build()` performs, in the same order.
    Some((S::new(shared.clone()), R::new(shared)))
}

/// Park a freshly built channel's shared handle for the next
/// construction of the same key.
pub(crate) fn park_shared<F: Flavor>(
    capacity: usize,
    slot_size: usize,
    shared: Arc<ChannelShared<F>>,
) {
    let bytes = capacity.max(1) * slot_size;
    if bytes > MAX_RING_BYTES {
        return;
    }
    let key = (TypeId::of::<ChannelShared<F>>(), capacity);
    let mut cache = lock();
    cache.total_bytes += bytes;
    cache.slots.insert(0, Entry {
        key,
        bytes,
        shared: shared as Arc<dyn Any + Send + Sync>,
    });
    // Evict oldest-first past either budget.
    while cache.slots.len() > MAX_ENTRIES || cache.total_bytes > MAX_TOTAL_BYTES {
        let Some(evicted) = cache.slots.pop() else {
            break
        };
        cache.total_bytes -= evicted.bytes;
    }
}

#[cfg(test)]
mod tests {
    use crossfire::{MRx, MTx, mpmc};

    use super::*;

    /// The cache is keyed and bounded; these tests bypass the env knob by
    /// driving `recycled`/`park_shared` directly (the knob tier has its
    /// own end-to-end test binary, `tests/ring_recycle.rs`).
    #[test]
    fn miss_then_hit_reuses_the_same_allocation() {
        // Unique capacity so the shared global cache state of other tests
        // cannot collide (LRU content is process-global).
        let cap = 97;
        let key = (TypeId::of::<ChannelShared<mpmc::Array<u32>>>(), cap);
        {
            let mut cache = lock();
            while let Some(i) = cache.slots.iter().position(|e| e.key == key) {
                let e = cache.slots.remove(i);
                cache.total_bytes -= e.bytes;
            }
        }
        assert!(
            recycled::<mpmc::Array<u32>, MTx<_>, MRx<_>>(cap).is_none(),
            "empty cache misses"
        );
        let (tx, rx) = mpmc::bounded_blocking::<u32>(cap);
        let handle = tx.shared_handle();
        park_shared(cap, size_of::<u32>(), handle.clone());
        drop(rx);
        drop(tx);
        let reused =
            recycled::<mpmc::Array<u32>, MTx<_>, MRx<_>>(cap).expect("parked entry must hit");
        assert!(
            Arc::ptr_eq(&handle, &reused.0.shared_handle()),
            "recycled must rebuild over the parked allocation"
        );
        drop(reused);
    }

    #[test]
    fn per_ring_byte_cap_skips_large_rings() {
        let cap = 8192; // 8192 * 16B = 128 KiB, over the cap for u128
        let key = (TypeId::of::<ChannelShared<mpmc::Array<u128>>>(), cap);
        let (tx, _rx) = mpmc::bounded_blocking::<u128>(cap);
        park_shared(cap, size_of::<u128>(), tx.shared_handle());
        let cache = lock();
        assert!(
            cache.slots.iter().all(|e| e.key != key),
            "over-budget ring must not be parked"
        );
    }
}
