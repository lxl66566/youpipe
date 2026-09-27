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

use std::sync::{Mutex, OnceLock, PoisonError};

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
struct PoolCache {
    slots: Vec<(usize, ComputePool)>,
}

impl PoolCache {
    fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Hit: move the slot to the LRU front and clone the handle out.
    fn get(&mut self, workers: usize) -> Option<ComputePool> {
        let idx = self.slots.iter().position(|&(k, _)| k == workers)?;
        let entry = self.slots.remove(idx);
        self.slots.insert(0, entry);
        Some(self.slots[0].1.clone())
    }

    /// Insert a freshly built pool. Returns the pools that fell out of the
    /// LRU — the caller must drop them **outside** the cache mutex (see
    /// [`acquire`]).
    fn insert(&mut self, workers: usize, pool: ComputePool) -> Vec<ComputePool> {
        debug_assert!(self.slots.iter().all(|&(k, _)| k != workers));
        self.slots.insert(0, (workers, pool));
        // `split_off(at)` requires at <= len, so clamp: below capacity
        // nothing falls out.
        self.slots
            .split_off(CACHE_CAPACITY.min(self.slots.len()))
            .into_iter()
            .map(|(_, pool)| pool)
            .collect()
    }

    /// Empty the cache. Same outside-the-lock drop contract as [`insert`].
    fn clear(&mut self) -> Vec<ComputePool> {
        self.slots.drain(..).map(|(_, pool)| pool).collect()
    }
}

static CACHE: OnceLock<Mutex<PoolCache>> = OnceLock::new();

fn lock() -> std::sync::MutexGuard<'static, PoolCache> {
    CACHE
        .get_or_init(|| Mutex::new(PoolCache::new()))
        .lock()
        // No code panics while holding the lock (pool construction and every
        // pool drop happen outside it), so poisoning cannot happen through
        // this module; `into_inner` merely stops a panicking *test* elsewhere
        // in the process from cascading into every later pool creation.
        .unwrap_or_else(PoisonError::into_inner)
}

/// Return a pool with exactly `workers` worker threads, reusing a parked one
/// when the (clamped) size matches. Worker count is the whole key: two
/// configs that resolve to the same size legitimately share a pool — a pool
/// is a generic job executor, exactly like a pre-created pool shared via
/// `with_compute_pool`.
pub(super) fn acquire(workers: usize) -> ComputePool {
    let key = workers.clamp(1, crate::MAX_COMPUTE_WORKERS);
    if let Some(pool) = lock().get(key) {
        return pool;
    }
    // Miss: build outside the lock. `ComputePool::build` spawns threads
    // (~ms); holding the mutex across it would serialize concurrent misses
    // on unrelated sizes behind one spawn batch.
    let fresh = ComputePool::build(key);
    let mut stale = Vec::new();
    let out = {
        let mut cache = lock();
        if let Some(pool) = cache.get(key) {
            // Lost a build race: another thread inserted this size while we
            // were spawning. Reuse theirs; `fresh` joins below.
            stale.push(fresh);
            pool
        } else {
            let out = fresh.clone();
            stale = cache.insert(key, fresh);
            out
        }
    }; // lock released
    // Drop stale pools strictly outside the mutex: dropping the last handle
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
    let stale = lock().clear();
    let n = stale.len();
    drop(stale); // joins — outside the lock (see `acquire`)
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
