use std::sync::{Arc, OnceLock};

use crate::pool::{self, Registry, join::Captured};

/// Global work-stealing compute pool backed by a rayon-style scheduler.
///
/// Workers pull jobs from a shared injector queue and steal from each other's
/// local deques. The fast path for posting work is pure atomics — no
/// Mutex/Condvar unless threads are actually sleeping.
///
/// `ComputePool` is cheap to clone (one `Arc` clone + one atomic increment).
/// Each clone holds a reference to the underlying [`Registry`]; the pool's
/// worker threads stay alive until the last clone is dropped.
pub struct ComputePool {
    registry: Arc<Registry>,
}

impl Clone for ComputePool {
    fn clone(&self) -> Self {
        // Keep the registry alive until the last clone drops. Without this
        // increment, dropping any clone would decrement terminate_count to 0
        // and tear down worker threads while other clones still reference them.
        self.registry.increment_terminate_count();
        Self {
            registry: Arc::clone(&self.registry),
        }
    }
}

impl ComputePool {
    /// Returns the lazily-initialized global pool (one per process), sized to
    /// available parallelism.
    #[must_use]
    pub fn global() -> &'static Self {
        static POOL: OnceLock<ComputePool> = OnceLock::new();
        POOL.get_or_init(|| Self {
            // global_registry() already primes the workers.
            registry: pool::global_registry().clone(),
        })
    }

    /// Create a pool with `num_workers` threads.
    ///
    /// `num_workers` is clamped to `[1, MAX_COMPUTE_WORKERS]` (511): the
    /// scheduler's sleep bitmask addresses at most 511 workers. The clamp is
    /// silent — use [`MAX_COMPUTE_WORKERS`](crate::MAX_COMPUTE_WORKERS) to
    /// query the cap programmatically.
    ///
    /// # Pool recycling
    ///
    /// Pools are recycled through a small process-wide cache keyed by worker
    /// count: dropping the last external handle **parks** the pool (its
    /// threads stay alive, idle) instead of joining it, and the next `new`
    /// with the same size reuses it for the cost of an `Arc` clone.
    /// Construction is ~15 µs per worker (spawn + prime + later join), which
    /// dominates fused terminals in tight loops — `.with_compute_workers(n)`
    /// and `.with_oversubscribe(f)` both resolve through this constructor.
    /// At most a few recent sizes stay parked; evicted ones join for real.
    ///
    /// Consequences:
    ///
    /// * after dropping a pool its threads may still be alive (parked). If they must be gone —
    ///   thread-count budgeting, teardown, tests — call [`ComputePool::clear_cached_pools`];
    /// * [`Self::new_pinned`] is never cached: pinned workers hold scarce CPU placement and always
    ///   join on drop;
    /// * [`Self::global`] bypasses the cache (process-lifetime already);
    /// * two calls with sizes that clamp to the same value share one pool.
    #[must_use]
    pub fn new(num_workers: usize) -> Self {
        super::pool_cache::acquire(num_workers)
    }

    /// Build a pool eagerly — spawn, prime, no cache. Internal construction
    /// path for `pool_cache::acquire` (miss path).
    pub(super) fn build(num_workers: usize) -> Self {
        let registry = Registry::new(num_workers, false);
        registry.wait_until_primed();
        Self { registry }
    }

    /// Drop every pool parked in the process-wide recycling cache (see
    /// [`Self::new`]), joining their worker threads. Returns the number of
    /// pools dropped.
    ///
    /// Pools still referenced by a live handle are unaffected; this only
    /// releases the cache's own references.
    // The side effect is the contract; the count is diagnostic, so callers
    // discarding it are not bugs.
    #[allow(clippy::must_use_candidate)]
    pub fn clear_cached_pools() -> usize {
        super::pool_cache::clear()
    }

    /// Create a pool whose workers are pinned to the CPUs the process is
    /// allowed to run on: worker `i` → the i-th allowed CPU (round-robin
    /// when `num_workers` exceeds the CPU count).
    ///
    /// Regime (same-binary A/B, 16C/32T SMT, taskset to 31 CPUs, 5
    /// interleaved rounds): back-to-back **saturated batch loops** win,
    /// because a worker that parks between batches always wakes on its own
    /// — idle, cache-warm — core instead of being placed on a busy or cold
    /// one by CFS: zstd-shaped unbalanced −4…−7 %, fused collect 1–4 M
    /// −5…−7 %. The same pinning **regresses** shapes that rely on the
    /// scheduler steering woken threads to idle CPUs: small/medium batches
    /// with a participating driver (+5…+22 %), streaming stage workers
    /// parked on inter-stage channels (+17…+48 %), nested saturated 1 K
    /// (+37 %). Pin only a single, ≤-CPU-sized pool that serves fused
    /// batch terminals; never share it with `stream` pipelines, oversize
    /// it, or run several pinned pools on overlapping CPUs. Details:
    /// dev/scheduler.md "worker affinity".
    ///
    /// On platforms without affinity support (non-Linux, miri) this
    /// degrades to an unpinned pool.
    ///
    /// Never recycled through the pool cache (see [`Self::new`]): pinned
    /// workers hold scarce CPU placement, so they join for real when the
    /// last handle drops.
    #[must_use]
    pub fn new_pinned(num_workers: usize) -> Self {
        let registry = Registry::new(num_workers, true);
        registry.wait_until_primed();
        Self { registry }
    }

    /// Submit a single `'static` job to the pool.
    pub fn submit<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let heap_job = pool::job::HeapJob::new(job);
        let job_ref = heap_job.into_static_job_ref();
        self.registry.inject_or_push(job_ref);
    }

    /// Submit a `'static` job directly to the global injector, bypassing the
    /// on-pool local-deque fast path of [`Self::submit`].
    ///
    /// The injector's FIFO order is load-bearing here: a job injected before
    /// its dependent jobs is guaranteed to be popped first, whereas `submit`
    /// from a same-pool worker pushes onto that worker's local LIFO deque —
    /// where a job whose runners are all blocked can sit unreachable forever.
    pub(crate) fn submit_injected<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let heap_job = pool::job::HeapJob::new(job);
        let job_ref = heap_job.into_static_job_ref();
        self.registry.inject(job_ref);
    }

    /// Submit multiple jobs at once (reduces per-job notification overhead:
    /// one sleep notification for the whole batch instead of one per job).
    ///
    /// The batch always goes to the **global injector** in FIFO order —
    /// unlike [`Self::submit`], which routes through the on-pool local-deque
    /// fast path (`inject_or_push`) when called from a worker of this pool.
    /// If the injector's cross-batch FIFO order is part of your protocol (a
    /// job injected before its dependents is popped first, like
    /// [`Self::submit_injected`]), rely on `submit_batch`, not `submit`.
    ///
    /// `I::IntoIter` must be [`ExactSizeIterator`] so the `JobRef`s stream
    /// straight into the injector's segment-reserving `push_n` — no
    /// intermediate `Vec<JobRef>` allocation (the same trick the fused
    /// hybrid dispatcher's injection side uses).
    pub fn submit_batch<F, I>(&self, jobs: I)
    where
        F: FnOnce() + Send + 'static,
        I: IntoIterator<Item = F>,
        I::IntoIter: ExactSizeIterator,
    {
        self.registry.inject_batch(
            jobs.into_iter()
                .map(|f| pool::job::HeapJob::new(f).into_static_job_ref()),
        );
    }

    /// Number of worker threads in this pool.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.registry.num_threads()
    }

    /// Fork-join: runs `a` on the current thread and `b` on a pool worker,
    /// returns both results. When called from a pool worker, `b` is pushed to
    /// the local deque for stealing; the current thread steals other work while
    /// waiting if `b` is stolen.
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        pool::join::join(&self.registry, a, b)
    }

    /// Value-returning [`join`](Self::join): a panicking closure comes back as
    /// `Err(payload)` instead of the panic resuming through the caller's frame.
    /// For callers that clean up sibling state between the `join` call and
    /// their own unwinder (the fused tree recursion).
    pub fn join_captured<A, B, RA, RB>(&self, a: A, b: B) -> (Captured<RA>, Captured<RB>)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        pool::join::join_captured(&self.registry, a, b)
    }

    /// Returns a reference to the underlying registry.
    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Returns `true` if the current thread is a worker on *this* pool.
    ///
    /// When a user supplies a custom `ComputePool` via `with_compute_pool`, a
    /// worker of the *global* pool is "off-pool" relative to the custom one
    /// and may safely take the blocking wait; only a worker of the *same*
    /// pool must wait by stealing (see [`Self::on_this_pool_owner`]).
    pub(crate) fn is_on_this_pool(&self) -> bool {
        self.on_this_pool_owner().is_some()
    }

    /// The current thread's `(registry, index)` if it is a worker of *this*
    /// pool — the owner context for a work-stealing (`Stealing`) `CountLatch`.
    ///
    /// The fused hybrid dispatcher hands this to `CountLatch::with_count` so an
    /// on-pool caller waits through the work-stealing `wait_until` loop
    /// (parking, if at all, via the sleep module's latch protocol:
    /// `CoreLatch::set` → `notify_worker_latch_is_set`) instead of a condvar
    /// the caller's own pool would have to service. This is the same protocol
    /// `join`/`SpinLatch` uses, so an on-pool nested terminal can never
    /// deadlock its pool.
    pub(crate) fn on_this_pool_owner(&self) -> Option<(&Arc<Registry>, usize)> {
        let wt = pool::registry::WorkerThread::current();
        if wt.is_null() {
            return None;
        }
        // SAFETY: `wt` is non-null — set by a pool worker's `main_loop`.
        let wt = unsafe { &*wt };
        (wt.registry_id() == self.registry.id()).then(|| (wt.registry(), wt.index()))
    }
}

impl Drop for ComputePool {
    fn drop(&mut self) {
        // Decrement the ref-count started by Clone (or Registry::new for the
        // original). Only the last Drop (counter 1→0) signals workers to stop;
        // earlier Drops are no-ops. This mirrors the Arc ref-counting pattern
        // but uses the registry's own terminate_count so worker shutdown is
        // coordinated with the sleep/latch machinery.
        self.registry.terminate();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    #[test]
    fn test_pool_basic() {
        let pool = ComputePool::new(2);
        let (tx, rx) = mpsc::channel();
        pool.submit(move || {
            tx.send(42i32).unwrap();
        });
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
            42
        );
    }

    #[test]
    fn test_pool_multiple() {
        let pool = Arc::new(ComputePool::new(4));
        let (tx, rx) = mpsc::channel();
        for i in 0..10 {
            let tx = tx.clone();
            let p = pool.clone();
            p.submit(move || {
                tx.send(i).unwrap();
            });
        }
        drop(tx);
        let results: Vec<_> = rx.iter().collect();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_pool_work_stealing() {
        let pool = Arc::new(ComputePool::new(4));
        let (tx, rx) = mpsc::channel();
        let total = 1000;
        for i in 0..total {
            let tx = tx.clone();
            let p = pool.clone();
            p.submit(move || {
                let mut sum = 0u64;
                for j in 0..1000 {
                    sum = sum.wrapping_add(j);
                }
                tx.send((i, sum)).unwrap();
            });
        }
        drop(tx);
        let results: Vec<_> = rx.iter().collect();
        assert_eq!(results.len(), total);
    }

    #[test]
    fn test_join_basic() {
        let pool = Arc::new(ComputePool::new(4));
        let (tx, rx) = mpsc::channel::<(i32, i32)>();
        let pool_ref = pool.clone();
        pool.submit(move || {
            let (a, b) = pool_ref.join(|| 1 + 1, || 2 + 2);
            tx.send((a, b)).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        assert_eq!(result, (2, 4));
    }

    #[test]
    fn test_join_recursive() {
        let pool = Arc::new(ComputePool::new(4));
        let (tx, rx) = mpsc::channel::<i32>();
        let pool_ref = pool.clone();
        pool.submit(move || {
            let sum = recursive_sum(&pool_ref, 0, 64);
            tx.send(sum).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        let expected: i32 = (0..64).sum();
        assert_eq!(result, expected);
    }

    fn recursive_sum(pool: &Arc<ComputePool>, start: i32, end: i32) -> i32 {
        if end - start <= 8 {
            return (start..end).sum();
        }
        let mid = start + (end - start) / 2;
        let (left, right) = pool.join(
            move || recursive_sum(pool, start, mid),
            move || recursive_sum(pool, mid, end),
        );
        left + right
    }

    #[test]
    fn test_join_external_thread() {
        let pool = ComputePool::new(4);
        let (a, b) = pool.join(|| 10 + 20, || 30 + 40);
        assert_eq!(a, 30);
        assert_eq!(b, 70);
    }

    /// `new_pinned` must leave its workers affine to exactly one distinct
    /// CPU of the process's allowed set (read back from /proc, no mocks).
    /// Other tests in this binary create unpinned pools whose threads share
    /// the `yp-pool-*` name, so the invariant is "≥ n distinct single-CPU
    /// workers inside the allowed set".
    /// `new_pinned` must bypass the recycling cache: two constructions are
    /// two distinct registries (the "clears nothing" half of the contract
    /// lives in tests/pool_cache.rs — the lib test binary parks unrelated
    /// pools into the global cache from parallel tests, so counting here
    /// would be racy).
    #[test]
    fn test_new_pinned_not_cached() {
        let a = ComputePool::new_pinned(2);
        let b = ComputePool::new_pinned(2);
        assert_ne!(a.registry().id(), b.registry().id());
    }

    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn test_new_pinned_workers_have_distinct_single_cpu_affinity() {
        let allowed = youpipe_sys::allowed_cpus();
        if allowed.len() < 2 {
            // Restrictive sandbox; nothing meaningful to pin.
            return;
        }
        let n = Ord::min(4, allowed.len());
        let pool = ComputePool::new_pinned(n);
        // Force the workers to exist before scanning /proc.
        pool.registry().wait_until_primed();

        let mut seen = Vec::new();
        for status in std::fs::read_dir("/proc/self/task").unwrap() {
            let status = status.unwrap();
            let path = status.path().join("status");
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            // Worker threads are named `yp-pool-<i>` (≤ 15 chars in /proc).
            if !text
                .lines()
                .any(|l| l.starts_with("Name:") && l.contains("yp-pool-"))
            {
                continue;
            }
            let Some(list) = text
                .lines()
                .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            else {
                continue;
            };
            let cpus: Vec<u32> = list
                .split(',')
                .flat_map(|part| {
                    let (a, b) = part.split_once('-').unwrap_or((part, part));
                    let a: u32 = a.trim().parse().unwrap();
                    let b: u32 = b.trim().parse().unwrap();
                    a..=b
                })
                .collect();
            if cpus.len() == 1 && allowed.contains(&cpus[0]) {
                seen.push(cpus[0]);
            }
        }
        assert!(
            seen.len() >= n,
            "expected ≥{n} single-CPU workers, saw {seen:?}"
        );
        seen.sort_unstable();
        seen.dedup();
        assert!(
            seen.len() >= n,
            "workers must sit on distinct CPUs, got {seen:?}"
        );
    }
}
