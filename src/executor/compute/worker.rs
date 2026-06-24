use std::sync::{Arc, OnceLock};

use crate::pool::{self, Registry};

/// Global work-stealing compute pool backed by a rayon-style scheduler.
///
/// Workers pull jobs from a shared injector queue and steal from each other's
/// local deques. The fast path for posting work is pure atomics — no
/// Mutex/Condvar unless threads are actually sleeping.
pub struct ComputePool {
    registry: Arc<Registry>,
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

    /// Create a new pool with `num_workers` threads.
    #[must_use]
    pub fn new(num_workers: usize) -> Self {
        let registry = Registry::new(num_workers);
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

    /// Submit multiple jobs at once (reduces per-job notification overhead).
    pub fn submit_batch<F, I>(&self, jobs: I)
    where
        F: FnOnce() + Send + 'static,
        I: IntoIterator<Item = F>,
    {
        let job_refs: Vec<_> = jobs
            .into_iter()
            .map(|f| pool::job::HeapJob::new(f).into_static_job_ref())
            .collect();
        self.registry
            .inject_batch(job_refs.into_iter());
    }

    /// Number of worker threads in this pool.
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

    /// Returns a reference to the underlying registry.
    #[allow(dead_code)]
    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

impl Drop for ComputePool {
    fn drop(&mut self) {
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
            let sum = recursive_sum(pool_ref, 0, 64);
            tx.send(sum).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        let expected: i32 = (0..64).sum();
        assert_eq!(result, expected);
    }

    fn recursive_sum(pool: Arc<ComputePool>, start: i32, end: i32) -> i32 {
        if end - start <= 8 {
            return (start..end).sum();
        }
        let mid = start + (end - start) / 2;
        let p1 = pool.clone();
        let p2 = pool.clone();
        let (left, right) = pool.join(
            move || recursive_sum(p1, start, mid),
            move || recursive_sum(p2, mid, end),
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
}
