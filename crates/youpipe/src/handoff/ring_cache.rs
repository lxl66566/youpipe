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
//!
//! The mutex comes from `youpipe_sys` so production runs on `parking_lot` (no
//! poisoning to recover from) and `--cfg loom` swaps in the model-checked one
//! the `loom_tests` gate drives.

use std::{
    any::{Any, TypeId},
    sync::{Arc, OnceLock},
};

use crossfire::{ChannelShared, ReceiverType, SenderType, flavor::Flavor};
use youpipe_sys::Mutex;

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

/// `(ChannelShared<F>, capacity)` — see the module doc.
type Key = (TypeId, usize);

/// Type-erased parked handle; `Key` pins the concrete type it downcasts to.
type ErasedShared = Arc<dyn Any + Send + Sync>;

struct Entry {
    key: Key,
    bytes: usize,
    shared: ErasedShared,
}

#[derive(Default)]
struct RingCache {
    slots: Vec<Entry>,
    total_bytes: usize,
}

impl RingCache {
    /// Remove the newest entry of `key` and hand its handle back. Several
    /// entries of one key can exist (two threads parking the same shape), and
    /// each is a distinct allocation, so one removal per call is exact.
    fn take(&mut self, key: Key) -> Option<ErasedShared> {
        let idx = self.slots.iter().position(|e| e.key == key)?;
        let entry = self.slots.remove(idx);
        self.total_bytes -= entry.bytes;
        Some(entry.shared)
    }

    /// Park `shared`, evicting oldest-first past either budget. The per-ring
    /// cap is enforced here so the bound holds for every insertion path.
    fn park(&mut self, key: Key, bytes: usize, shared: ErasedShared) {
        if bytes > MAX_RING_BYTES {
            return;
        }
        self.total_bytes += bytes;
        self.slots.insert(0, Entry { key, bytes, shared });
        // Evict oldest-first past either budget.
        while self.slots.len() > MAX_ENTRIES || self.total_bytes > MAX_TOTAL_BYTES {
            let Some(evicted) = self.slots.pop() else {
                break
            };
            self.total_bytes -= evicted.bytes;
        }
    }
}

static CACHE: OnceLock<Mutex<RingCache>> = OnceLock::new();

/// The process-wide cache. `OnceLock` stays `std`: its initializer cannot
/// panic, so no other thread can observe a half-initialized slot, and loom
/// does not model one-shot initialization (each model builds its own cache).
fn cache() -> &'static Mutex<RingCache> {
    CACHE.get_or_init(|| Mutex::new(RingCache::default()))
}

/// Take the newest parked shared of `(ChannelShared<F>, capacity)` and, when
/// its ring is resettable (`try_reset` claims both endpoint refcounts — the
/// previous run must have finished draining), rebuild the endpoint pair over
/// it. `None` = miss: build fresh (the caller parks the fresh handle via
/// [`park_shared`]). The env-knob gate lives at the factory call sites, so
/// this cache stays env-free and unit-testable.
pub(crate) fn recycled<F, S, R>(capacity: usize) -> Option<(S, R)>
where
    F: Flavor,
    S: SenderType<Flavor = F>,
    R: ReceiverType<Flavor = F>,
{
    take_and_rebuild(
        cache(),
        (TypeId::of::<ChannelShared<F>>(), capacity),
        |shared: Arc<ChannelShared<F>>| {
            if !shared.try_reset() {
                return None; // endpoints still alive, or leftovers: drop, miss
            }
            // The same construction `build()` performs, in the same order.
            Some((S::new(shared.clone()), R::new(shared)))
        },
    )
}

/// Lift a parked handle out of the cache and hand it to `rebuild` **after** the
/// mutex is released: `rebuild` claims the ring's endpoints, and a rejected
/// ring's `Drop` releases its leftover items — user drop code that may itself
/// build a channel. Generic over the handle only so `loom_tests` can drive it
/// with a stand-in (a real ring cannot be built inside a loom execution).
fn take_and_rebuild<H, T>(
    cache: &Mutex<RingCache>,
    key: Key,
    rebuild: impl FnOnce(Arc<H>) -> Option<T>,
) -> Option<T>
where
    H: Any + Send + Sync,
{
    // The guard lives for this statement only.
    let shared = cache.lock().take(key)?;
    // Erased -> concrete; the key pins the type, this cannot miss.
    let shared = shared.downcast::<H>().ok()?;
    rebuild(shared)
}

/// Park a freshly built channel's shared handle for the next
/// construction of the same key.
pub(crate) fn park_shared<F: Flavor>(
    capacity: usize,
    slot_size: usize,
    shared: Arc<ChannelShared<F>>,
) {
    let key = (TypeId::of::<ChannelShared<F>>(), capacity);
    let bytes = capacity.max(1) * slot_size;
    cache()
        .lock()
        .park(key, bytes, shared as Arc<dyn Any + Send + Sync>);
}

#[cfg(test)]
mod tests {
    use crossfire::{MRx, MTx, mpmc};

    use super::*;

    /// Drop every parked entry of `key`: the cache is process-global and
    /// sibling tests may have parked the same shape.
    fn drain(key: Key) {
        while let Some(shared) = cache().lock().take(key) {
            drop(shared);
        }
    }

    /// The cache is keyed and bounded; these tests bypass the env knob by
    /// driving `recycled`/`park_shared` directly (the knob tier has its
    /// own end-to-end test binary, `tests/ring_recycle.rs`).
    #[test]
    fn miss_then_hit_reuses_the_same_allocation() {
        // Unique capacity so the shared global cache state of other tests
        // cannot collide (LRU content is process-global).
        let cap = 97;
        let key = (TypeId::of::<ChannelShared<mpmc::Array<u32>>>(), cap);
        drain(key);
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
        assert!(
            cache().lock().take(key).is_none(),
            "over-budget ring must not be parked"
        );
    }
}

/// The ring cache's bookkeeping and the take path's lock discipline: a
/// byte/count budget that stays exact under concurrent parks, one handle handed
/// out per take, and the taken handle only rebuilt after the mutex is released.
///
/// Layout: two or three threads at `LOOM_MAX_PREEMPTIONS=2`, each model
/// constructs its own `Mutex<RingCache>` — `youpipe_sys` registers a primitive
/// against the loom execution that created it, so the process-global `CACHE`
/// cannot be shared between executions. Parked payloads are stand-ins keyed by
/// `TypeId::of::<Stub>()`: nothing in the take/park path inspects the payload,
/// and a real ring would only add allocation noise. Evictions drop their payload
/// *inside* `RingCache::park`, which is safe (a quiescent ring's drop takes no
/// lock), so only the handles that leave through `take` carry the lock probe.
///
/// Falsification runs (temporary edits): calling `rebuild` inside the lock scope
/// of `take_and_rebuild` deadlocks `take_hands_out_each_parked_handle_once` —
/// the probe re-acquires the mutex, standing in for a rejected ring's
/// leftover-item drop code that builds another channel; skipping the
/// `total_bytes -= entry.bytes` in `take` fails the same model on its accounting
/// assertion; skipping the eviction loop's subtraction fails
/// `budget_bounds_and_accounting_hold` on `total_bytes <= MAX_TOTAL_BYTES`.
#[cfg(all(test, loom))]
mod loom_tests {
    use std::sync::{Arc, Weak};

    use super::*;

    /// Payload stand-in; `id` identifies the allocation.
    struct Stub {
        id: usize,
        probe: Option<Weak<Mutex<RingCache>>>,
    }

    impl Stub {
        /// Dropped wherever the cache drops it.
        fn plain(id: usize) -> Arc<Self> {
            Arc::new(Self { id, probe: None })
        }

        /// Must be dropped with the cache mutex released: re-acquires it like a
        /// rejected ring's leftover-item drop code that builds another channel.
        fn probed(id: usize, cache: &Arc<Mutex<RingCache>>) -> Arc<Self> {
            Arc::new(Self {
                id,
                probe: Some(Arc::downgrade(cache)),
            })
        }
    }

    impl Drop for Stub {
        fn drop(&mut self) {
            // The upgrade fails only when the whole cache is being torn down
            // after the model finished.
            if let Some(probe) = &self.probe
                && let Some(cache) = probe.upgrade()
            {
                let _guard = cache.lock();
            }
        }
    }

    fn park(cache: &Arc<Mutex<RingCache>>, id: usize, bytes: usize, stub: Arc<Stub>) {
        cache.lock().park((TypeId::of::<Stub>(), id), bytes, stub);
    }

    fn key(id: usize) -> Key {
        (TypeId::of::<Stub>(), id)
    }

    /// Both budgets hold after concurrent parks, the byte accounting equals the
    /// sum of the parked entries, and the per-ring cap keeps the oversized ring
    /// out. Anything that evicts (or skips) without adjusting `total_bytes`
    /// drifts one of these.
    ///
    /// Cost: 773 executions, 0.10 s.
    #[test]
    fn budget_bounds_and_accounting_hold() {
        loom::model(|| {
            let cache = Arc::new(Mutex::new(RingCache::default()));

            let big = Arc::clone(&cache);
            let big_parker = loom::thread::spawn(move || {
                // 33 rings at the cap already exceed the total byte budget.
                for id in 0..33 {
                    park(&big, id, MAX_RING_BYTES, Stub::plain(id));
                }
                park(&big, 1000, MAX_RING_BYTES + 1, Stub::plain(1000));
            });
            let small = Arc::clone(&cache);
            let small_parker = loom::thread::spawn(move || {
                for id in 0..20 {
                    park(&small, 100 + id, 1, Stub::plain(100 + id));
                }
            });

            big_parker.join().unwrap();
            small_parker.join().unwrap();

            let guard = cache.lock();
            assert!(guard.slots.len() <= MAX_ENTRIES, "entry budget holds");
            assert!(guard.total_bytes <= MAX_TOTAL_BYTES, "byte budget holds");
            assert_eq!(
                guard.total_bytes,
                guard.slots.iter().map(|e| e.bytes).sum::<usize>(),
                "accounting equals the parked entries"
            );
            assert!(
                guard.slots.iter().all(|e| e.bytes <= MAX_RING_BYTES),
                "oversized ring must not be parked"
            );
            drop(guard);
        });
    }

    /// One take hands out one parked allocation, and the rebuild runs with the
    /// mutex released: the second taker's nested take inside its own rebuild
    /// must not deadlock.
    ///
    /// Cost: 266 executions, 0.04 s.
    #[test]
    fn take_hands_out_each_parked_handle_once() {
        loom::model(|| {
            let cache = Arc::new(Mutex::new(RingCache::default()));
            park(&cache, 0, 1, Stub::probed(0, &cache));
            park(&cache, 1, 2, Stub::plain(1));

            let taker = Arc::clone(&cache);
            let first = loom::thread::spawn(move || {
                take_and_rebuild(&*taker, key(0), |stub: Arc<Stub>| Some(stub.id))
            });
            let other = Arc::clone(&cache);
            let second = loom::thread::spawn(move || {
                let taken = take_and_rebuild(&*other, key(0), |stub: Arc<Stub>| Some(stub.id));
                let nested = take_and_rebuild(&*other, key(0), |stub: Arc<Stub>| Some(stub.id));
                (taken, nested)
            });
            let third_cache = Arc::clone(&cache);
            let third = loom::thread::spawn(move || {
                take_and_rebuild(&*third_cache, key(1), |stub: Arc<Stub>| Some(stub.id))
            });

            let first_taken = first.join().unwrap();
            let (second_taken, nested) = second.join().unwrap();
            assert_eq!(
                [first_taken, second_taken].into_iter().flatten().count(),
                1,
                "exactly one of the two takers gets the single parked entry"
            );
            assert_eq!(
                first_taken.or(second_taken),
                Some(0),
                "the parked allocation is handed out"
            );
            assert_eq!(nested, None, "no second entry for that key");
            assert_eq!(third.join().unwrap(), Some(1), "the other key is intact");
            let guard = cache.lock();
            assert!(guard.slots.is_empty(), "both entries were taken");
            assert_eq!(guard.total_bytes, 0, "accounting follows every removal");
            drop(guard);
        });
    }
}
