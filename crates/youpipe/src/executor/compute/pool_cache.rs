//! Process-wide recycling cache for pools created by
//! [`ComputePool::new`](super::ComputePool::new).
//!
//! Every fused terminal with a non-default worker budget
//! (`with_compute_workers(n ≠ ncpus)` / `with_oversubscribe(f)`) resolves to
//! a transient pool built inside the terminal call. Building a pool costs
//! thread spawn + priming, joining it another round of futex waits — ~15 µs
//! per worker per run (measured, `pool_reuse` bench / dev/scheduler.md
//! "Transient pool recycling"), which in tight loops dwarfs the actual work.
//! This cache parks dropped pools keyed by worker count and hands them back
//! on the next `new` of the same size, turning the second and later runs
//! into an `Arc` clone.
//!
//! Trade-off (deliberate: simple-and-predictable over idle reaping): a
//! dropped pool's threads stay alive (parked) until eviction or
//! [`ComputePool::clear_cached_pools`](super::ComputePool::clear_cached_pools).
//! Idle-timeout reaping was rejected — a timer per pool makes thread counts
//! nondeterministic and the failure mode (user believes the pool is gone)
//! *less* predictable, not more. The auditable invariant instead: the cache
//! holds at most [`CACHE_CAPACITY`] pools; everything evicted, cleared, or
//! built through `new_pinned` joins for real.
//!
//! Bypassed by `new_pinned` (pinned workers hold scarce CPU placement —
//! silently keeping them alive after drop would be a footgun) and by the
//! global pool (already process-lifetime).
//!
//! The mutex comes from `youpipe_sys` so production runs on `parking_lot` (no
//! poisoning to recover from) and `--cfg loom` swaps in the model-checked one
//! the `loom_tests` gate drives.

use std::sync::OnceLock;

use youpipe_sys::Mutex;

use super::ComputePool;

/// Cached worker-count combinations. Small on purpose: the dominant shapes
/// are one or two alternating configs; each extra slot trades resident
/// parked threads for fewer spawn/join storms when more configs alternate.
const CACHE_CAPACITY: usize = 4;

/// LRU of `(worker count, pool)` pairs; slot 0 is most recent.
/// Invariants: `slots.len() <= CACHE_CAPACITY`, keys unique.
///
/// A plain `Vec` beats a map here: at most 4 entries, and a linear `usize`
/// compare is cheaper than any hash.
///
/// Generic over the parked handle only so `loom_tests` can drive the protocol
/// with a stand-in: a real `ComputePool` cannot be built inside a loom
/// execution (its workers are OS threads). Production instantiates
/// `PoolCache<ComputePool>` and pays nothing for the parameter.
struct PoolCache<H> {
    slots: Vec<(usize, H)>,
}

impl<H: Clone> PoolCache<H> {
    fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Hit: move the slot to the LRU front and clone the handle out.
    fn get(&mut self, workers: usize) -> Option<H> {
        let idx = self.slots.iter().position(|&(k, _)| k == workers)?;
        let entry = self.slots.remove(idx);
        self.slots.insert(0, entry);
        Some(self.slots[0].1.clone())
    }

    /// Insert a freshly built handle. Returns the handles that fell out of the
    /// LRU — the caller must drop them **outside** the cache mutex (see
    /// [`acquire_from`]).
    fn insert(&mut self, workers: usize, handle: H) -> Vec<H> {
        debug_assert!(self.slots.iter().all(|&(k, _)| k != workers));
        self.slots.insert(0, (workers, handle));
        // `split_off(at)` requires at <= len, so clamp: below capacity
        // nothing falls out.
        self.slots
            .split_off(CACHE_CAPACITY.min(self.slots.len()))
            .into_iter()
            .map(|(_, handle)| handle)
            .collect()
    }

    /// Empty the cache. Same outside-the-lock drop contract as [`insert`](Self::insert).
    fn clear(&mut self) -> Vec<H> {
        self.slots.drain(..).map(|(_, handle)| handle).collect()
    }
}

static CACHE: OnceLock<Mutex<PoolCache<ComputePool>>> = OnceLock::new();

/// The process-wide cache. `OnceLock` stays `std`: its initializer cannot
/// panic, so no other thread can observe a half-initialized slot, and loom
/// does not model one-shot initialization (each model builds its own cache).
fn cache() -> &'static Mutex<PoolCache<ComputePool>> {
    CACHE.get_or_init(|| Mutex::new(PoolCache::new()))
}

/// Return a pool with exactly `workers` worker threads, reusing a parked one
/// when the (clamped) size matches. Worker count is the whole key: two
/// configs that resolve to the same size legitimately share a pool — a pool
/// is a generic job executor, exactly like a pre-created pool shared via
/// `with_compute_pool`.
pub(super) fn acquire(workers: usize) -> ComputePool {
    let key = workers.clamp(1, crate::MAX_COMPUTE_WORKERS);
    acquire_from(cache(), key, ComputePool::build)
}

/// The acquire protocol, generic over the handle so `loom_tests` can check it:
/// double-checked under the mutex with `build` running outside it.
fn acquire_from<H: Clone>(
    cache: &Mutex<PoolCache<H>>,
    key: usize,
    build: impl FnOnce(usize) -> H,
) -> H {
    if let Some(handle) = cache.lock().get(key) {
        return handle;
    }
    // Miss: build outside the lock. `ComputePool::build` spawns threads
    // (~ms); holding the mutex across it would serialize concurrent misses
    // on unrelated sizes behind one spawn batch.
    let fresh = build(key);
    let mut stale = Vec::new();
    let out = {
        let mut cache = cache.lock();
        if let Some(handle) = cache.get(key) {
            // Lost a build race: another thread inserted this size while we
            // were spawning. Reuse theirs; `fresh` joins below.
            stale.push(fresh);
            handle
        } else {
            let out = fresh.clone();
            stale = cache.insert(key, fresh);
            out
        }
    }; // lock released
    // Drop stale handles strictly outside the mutex: dropping the last handle
    // joins worker threads, and a worker can be blocked on this very mutex —
    // a detached job (`pool.submit(|| ..nested pipeline..)` followed by
    // `drop(pool)`) that itself calls `ComputePool::new` would deadlock if
    // the join happened under the lock (joiner waits for the job to finish,
    // job waits for the lock).
    drop(stale);
    out
}

/// Drop every parked pool, joining its worker threads. Returns how many
/// pools were dropped. `ComputePool::clear_cached_pools` is the public
/// wrapper.
pub(super) fn clear() -> usize {
    clear_from(cache())
}

/// The clear protocol: hand the handles back, then drop them outside the
/// mutex (same join contract as [`acquire_from`]).
fn clear_from<H: Clone>(cache: &Mutex<PoolCache<H>>) -> usize {
    let stale = cache.lock().clear();
    let n = stale.len();
    drop(stale); // joins — outside the lock (see `acquire_from`)
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The LRU mechanics are datastructure-only, so the tests run on clones
    /// of the *global* pool's handle: real identity comparison via registry
    /// id, zero thread spawning, and dropping clones never tears the global
    /// pool down.
    fn probe() -> ComputePool {
        ComputePool::global().clone()
    }

    #[test]
    fn hit_clones_and_exact_key_miss() {
        let mut cache = PoolCache::new();
        let g = probe();
        assert!(cache.get(8).is_none(), "empty cache misses");
        assert!(cache.insert(8, g.clone()).is_empty());
        let hit = cache.get(8).expect("exact key must hit");
        assert_eq!(
            hit.registry().id(),
            g.registry().id(),
            "hit must be a clone of the inserted pool"
        );
        assert!(cache.get(9).is_none(), "different size must not hit");
    }

    #[test]
    fn capacity_evicts_least_recently_used() {
        let mut cache = PoolCache::new();
        for k in [1, 2, 3, 4] {
            cache.insert(k, probe());
        }
        // Touch key 1: it must survive the next insert; key 2 becomes the
        // eviction victim.
        let _ = cache.get(1);
        let evicted = cache.insert(5, probe());
        assert_eq!(evicted.len(), 1);
        assert!(cache.get(2).is_none(), "LRU key was evicted");
        assert!(cache.get(1).is_some(), "touched key survived");
        assert!(cache.get(5).is_some(), "fresh insert present");
    }

    #[test]
    fn clear_empties_all_slots() {
        let mut cache = PoolCache::new();
        cache.insert(1, probe());
        cache.insert(2, probe());
        assert_eq!(cache.clear().len(), 2);
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
    }
}

/// The cache protocol: one entry per key, capacity-bounded LRU, and every
/// evicted / cleared / raced-out handle dropped only after the mutex is
/// released.
///
/// Layout: two or three threads at `LOOM_MAX_PREEMPTIONS=2`, each model
/// constructs its own `Mutex<PoolCache<Probe>>` — `youpipe_sys` registers a
/// primitive against the loom execution that created it, so the process-global
/// `CACHE` cannot be shared between executions.
///
/// Falsification runs (temporary edits): dropping `stale` inside the lock scope
/// of `acquire_from` deadlocks `concurrent_misses_share_one_pool` and
/// `eviction_drops_outside_the_lock`, and the same move in `clear_from`
/// deadlocks `clear_drops_outside_the_lock` — the probe's `Drop` re-acquires the
/// mutex, which is what a real pool drop does through a worker parked in
/// `ComputePool::new`. Dropping the re-check under the lock (single-checked
/// acquire) fails `concurrent_misses_share_one_pool` on `PoolCache::insert`'s
/// unique-key assertion.
#[cfg(all(test, loom))]
mod loom_tests {
    use std::sync::{Arc, Weak};

    use youpipe_sys::{AtomicUsize, Ordering};

    use super::*;

    /// Stand-in for a `ComputePool` handle; `id` plays the role of
    /// `registry().id()` (two handles of one pool compare equal by it).
    ///
    /// Every probe re-acquires the cache mutex on `Drop`: dropping the last
    /// real handle joins worker threads, and a worker running a detached job
    /// that builds a pool would be waiting on this very mutex. A handle
    /// dropped while the guard is still held therefore fails the model.
    #[derive(Clone)]
    struct Probe {
        id: usize,
        cache: Weak<Mutex<PoolCache<Probe>>>,
    }

    impl Probe {
        fn new(id: usize, cache: &Arc<Mutex<PoolCache<Probe>>>) -> Self {
            Self {
                id,
                cache: Arc::downgrade(cache),
            }
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            // The upgrade fails only when the whole cache is being torn down
            // after the model finished.
            if let Some(cache) = self.cache.upgrade() {
                let _guard = cache.lock();
            }
        }
    }

    /// Invariants every model ends on: the capacity bound and one entry per
    /// key. Callers must not hold the guard themselves.
    fn assert_cache_invariants(cache: &Mutex<PoolCache<Probe>>) {
        let guard = cache.lock();
        assert!(guard.slots.len() <= CACHE_CAPACITY, "capacity holds");
        for (i, &(key, _)) in guard.slots.iter().enumerate() {
            assert!(
                guard.slots[i + 1..].iter().all(|&(other, _)| other != key),
                "keys are unique"
            );
        }
    }

    /// Concurrent misses of one key must converge on a single parked pool: the
    /// double check is what keeps the loser's freshly built probe out of the
    /// cache, and every caller ends up with the same instance.
    ///
    /// Cost: 138 executions, 0.02 s.
    #[test]
    fn concurrent_misses_share_one_pool() {
        loom::model(|| {
            const THREADS: usize = 3;
            let cache = Arc::new(Mutex::new(PoolCache::<Probe>::new()));
            let ids = Arc::new(AtomicUsize::new(0));

            let mut threads = Vec::with_capacity(THREADS);
            for _ in 0..THREADS {
                let thread_cache = Arc::clone(&cache);
                let thread_ids = Arc::clone(&ids);
                threads.push(loom::thread::spawn(move || {
                    acquire_from(&*thread_cache, 3, |_| {
                        Probe::new(thread_ids.fetch_add(1, Ordering::Relaxed), &thread_cache)
                    })
                }));
            }
            let handles: Vec<Probe> = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect();

            let id = handles[0].id;
            assert!(
                handles.iter().all(|handle| handle.id == id),
                "a racing miss must not hand out two instances"
            );
            // One build per thread at most; fewer when a thread hits an entry
            // an earlier one already parked.
            assert!(
                ids.load(Ordering::Relaxed) <= THREADS,
                "one build per thread at most"
            );
            let guard = cache.lock();
            assert_eq!(guard.slots.len(), 1, "one entry per key");
            assert_eq!(guard.slots[0].0, 3, "the key is the entry's key");
            drop(guard);
            assert_cache_invariants(&cache);
        });
    }

    /// A full cache evicts its least recently used slot on the next insert, and
    /// the evicted handle leaves the guard before it is dropped.
    ///
    /// Cost: 1114 executions, 0.12 s.
    #[test]
    fn eviction_drops_outside_the_lock() {
        loom::model(|| {
            let cache = Arc::new(Mutex::new(PoolCache::<Probe>::new()));
            for k in 0..CACHE_CAPACITY {
                let mut guard = cache.lock();
                assert!(guard.insert(k, Probe::new(k, &cache)).is_empty());
            }

            let mut threads = Vec::with_capacity(3);
            for k in [10, 11, 12] {
                let thread_cache = Arc::clone(&cache);
                threads.push(loom::thread::spawn(move || {
                    acquire_from(&*thread_cache, k, |key| Probe::new(key, &thread_cache))
                }));
            }
            for thread in threads {
                assert!(thread.join().unwrap().id >= 10, "fresh key must build");
            }

            let guard = cache.lock();
            assert_eq!(guard.slots.len(), CACHE_CAPACITY, "capacity holds");
            assert!(
                guard.slots.iter().all(|(k, _)| *k >= 2),
                "the two least recently used keys were evicted"
            );
            drop(guard);
            assert_cache_invariants(&cache);
        });
    }

    /// `clear` hands every parked handle back, so the joins happen with no
    /// guard held — even while other threads keep acquiring.
    ///
    /// Cost: 462 executions, 0.07 s.
    #[test]
    fn clear_drops_outside_the_lock() {
        loom::model(|| {
            let cache = Arc::new(Mutex::new(PoolCache::<Probe>::new()));
            let mut guard = cache.lock();
            for k in 0..CACHE_CAPACITY {
                guard.insert(k, Probe::new(k, &cache));
            }
            drop(guard);

            let clearer_cache = Arc::clone(&cache);
            let clearer = loom::thread::spawn(move || clear_from(&*clearer_cache));
            let mut accquirers = Vec::with_capacity(2);
            for k in [1, 2] {
                let accquirer_cache = Arc::clone(&cache);
                accquirers.push(loom::thread::spawn(move || {
                    acquire_from(&*accquirer_cache, k, |key| {
                        Probe::new(key, &accquirer_cache)
                    })
                }));
            }

            let cleared = clearer.join().unwrap();
            assert_eq!(cleared, CACHE_CAPACITY, "every parked pool is cleared");
            let ids: Vec<usize> = accquirers
                .into_iter()
                .map(|thread| thread.join().unwrap().id)
                .collect();
            assert_eq!(ids, [1, 2], "each key resolves to its own pool");
            // Leftover is either empty (the clear ran last) or exactly the pools
            // the accquirers re-inserted after it.
            let guard = cache.lock();
            assert!(guard.slots.len() <= 2, "nothing else is parked");
            drop(guard);
            assert_cache_invariants(&cache);
        });
    }
}
