//! Registry of worker threads + work-stealing main loop. Adapted from
//! rayon-core's `registry.rs`, simplified (no broadcast, no FIFO, no
//! cross-registry, no custom spawn).

use std::{
    cell::Cell,
    hash::{DefaultHasher, Hasher},
    mem, ptr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use st3::{
    StealError,
    lifo::{Stealer, Worker},
};

use super::{
    job::{HeapJob, JobRef, StackJob},
    latch::{AsCoreLatch, CoreLatch, Latch, LatchRef, LockLatch, OnceLatch},
    sleep::Sleep,
    unwind,
};

/// Capacity of each worker's local (LIFO) deque. Rounded up to a power of two
/// by `st3`. When saturated, overflow spills into the global injector (tokio's
/// strategy): the hot local queue stays bounded and cache-friendly, no work is
/// dropped.
const LOCAL_DEQUE_CAPACITY: usize = 256;

// ── Registry ──

pub(crate) struct Registry {
    thread_infos: Vec<ThreadInfo>,
    /// Number of workers actually spawned, `<= thread_infos.len()`.
    ///
    /// `thread_infos` is pre-created for all workers, but on a mid-way spawn
    /// failure only `0..spawned` entries have a live thread that will ever
    /// set its `primed`/`stopped` latches — `terminate` and `Drop` must not
    /// touch the ghost tail (waiting on a ghost's `stopped` latch parks
    /// forever).
    ///
    /// Monotonic; written only by the constructing thread before it releases
    /// its Arc (or panics), read by whoever runs `Drop`.
    spawned: AtomicUsize,
    sleep: Sleep,
    /// Global injector queue for jobs coming from outside the pool or
    /// overflowing a worker's local deque. Unbounded, so overflow never drops
    /// work.
    ///
    /// `concurrent_queue` (not crossbeam): same block-based MPMC algorithm but
    /// without `crossbeam-epoch` (the source of the Miri UB that prompted the
    /// st3 migration). Its empty `pop` is 2 Acquire loads + a SeqCst fence, no
    /// CAS — cheap enough on this 99%-empty hot path that a hand-maintained
    /// `AtomicUsize` length counter is a measured regression (its per-op
    /// `fetch_add`/`fetch_sub` bounces a cache line).
    injected_jobs: concurrent_queue::ConcurrentQueue<JobRef>,

    // When this reaches 0, all work on this registry must be complete. The
    // global pool has a ref that never gets released; a user-created pool
    // holds one ref via the ComputePool.
    terminate_count: AtomicUsize,
}

struct ThreadInfo {
    /// Set once the worker has started and entered the main loop.
    primed: LockLatch,
    /// Set once the worker has fully exited (for tests).
    stopped: LockLatch,
    /// Set to request termination.
    terminate: OnceLatch,
    /// Stealer half of this worker's local deque.
    stealer: Stealer<JobRef>,
}

impl ThreadInfo {
    fn new(stealer: Stealer<JobRef>) -> ThreadInfo {
        ThreadInfo {
            primed: LockLatch::new(),
            stopped: LockLatch::new(),
            terminate: OnceLatch::new(),
            stealer,
        }
    }
}

impl Registry {
    pub(crate) fn new(num_threads: usize) -> Arc<Self> {
        let num_threads = Ord::min(num_threads.max(1), super::sleep::THREADS_MAX);

        let (workers, stealers): (Vec<_>, Vec<_>) = (0..num_threads)
            .map(|_| {
                let worker = Worker::<JobRef>::new(LOCAL_DEQUE_CAPACITY);
                let stealer = worker.stealer();
                (worker, stealer)
            })
            .unzip();

        let registry = Arc::new(Registry {
            thread_infos: stealers.into_iter().map(ThreadInfo::new).collect(),
            spawned: AtomicUsize::new(0),
            sleep: Sleep::new(num_threads),
            injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
            terminate_count: AtomicUsize::new(1),
        });

        for (index, worker) in workers.into_iter().enumerate() {
            let thread_registry = Arc::clone(&registry);
            match thread::Builder::new()
                .name(format!("yp-pool-{index}"))
                .spawn(move || {
                    unsafe { main_loop(worker, thread_registry, index) };
                }) {
                Ok(_) => {
                    registry.spawned.store(index + 1, Ordering::Release);
                },
                Err(e) => {
                    // The already-spawned workers hold Arc references, so the
                    // registry's Drop (whose force-terminate path would
                    // otherwise stop them) never runs while they are alive:
                    // without this explicit terminate() they park forever on
                    // their terminate latches — a thread + memory leak on top
                    // of the panic. With the latches set they exit, and Drop
                    // waiting only on the spawned prefix of `thread_infos`
                    // (see `spawned`) lets the last one free the registry.
                    registry.terminate();
                    panic!("failed to spawn pool worker {index}: {e}");
                },
            }
        }

        registry
    }

    /// Opaque identity for this registry.
    pub(crate) fn id(&self) -> usize {
        ptr::from_ref::<Self>(self) as usize
    }

    pub(crate) fn num_threads(&self) -> usize {
        self.thread_infos.len()
    }

    // ── Job injection ──

    /// Push from a worker thread's local deque, or inject from outside. Checks
    /// TLS to determine whether the caller is a pool worker.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn inject_or_push(&self, job_ref: JobRef) {
        let wt = WorkerThread::current();
        if !wt.is_null() && unsafe { (*wt).registry_id() } == self.id() {
            // SAFETY: wt is the current thread's WorkerThread.
            unsafe { (*wt).push(job_ref) };
        } else {
            self.inject(job_ref);
        }
    }

    /// Push a batch of jobs onto the calling worker's local deque with ONE
    /// sleep notification for the whole batch — the batched counterpart of
    /// [`Self::inject_or_push`] (which notifies per job). Falls back to
    /// [`Self::inject_batch`] when the caller is not a worker of this
    /// registry, mirroring `inject_or_push`'s TLS check.
    ///
    /// Batched notification matters under saturation: P concurrent on-pool
    /// drivers each dispatching a `num_threads`-sized batch through per-job
    /// notifications would hammer the shared JEC counter (one SeqCst RMW per
    /// job) — the convergence the local-deque dispatch exists to remove.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn push_local_batch(&self, jobs: impl ExactSizeIterator<Item = JobRef>) {
        let wt = WorkerThread::current();
        if !wt.is_null() && unsafe { (*wt).registry_id() } == self.id() {
            // SAFETY: wt is the current thread's WorkerThread of this registry.
            unsafe { (*wt).push_batch(jobs) };
        } else {
            self.inject_batch(jobs);
        }
    }

    /// Inject a job from outside the pool.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn inject(&self, job_ref: JobRef) {
        // `was_empty` drives the wake heuristic; read before the push. A
        // concurrent consumer draining the queue makes it racy, but it is only
        // an optimization hint — correctness rests on `new_injected_jobs`'
        // SeqCst fence + condvar-notify protocol.
        let queue_was_empty = self.injected_jobs.is_empty();
        // Unbounded queue, never closed → push cannot fail.
        let _ = self.injected_jobs.push(job_ref);
        self.sleep.new_injected_jobs(1, queue_was_empty);
    }

    /// Inject multiple jobs from outside the pool, notifying sleepers once.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn inject_batch(&self, job_refs: impl ExactSizeIterator<Item = JobRef>) {
        let queue_was_empty = self.injected_jobs.is_empty();
        // `push_n` reserves the whole batch with one tail CAS per block
        // segment (≤ 31 slots) instead of one contended CAS per job — the
        // batch is `num_threads-1 ≤ BLOCK_CAP` jobs, so this is a single CAS
        // in the common case.
        let count = self.injected_jobs.push_n(job_refs);
        if count > 0 {
            // The batch is bounded by the chunk-job count (num_threads), so
            // truncation cannot occur on realistic pool sizes.
            #[allow(clippy::cast_possible_truncation)]
            self.sleep.new_injected_jobs(count as u32, queue_was_empty);
        }
    }

    fn has_injected_job(&self) -> bool {
        !self.injected_jobs.is_empty()
    }

    fn pop_injected_job(&self) -> Option<JobRef> {
        // No separate length fast-path needed: see `injected_jobs` doc.
        self.injected_jobs.pop().ok()
    }

    // ── Worker coordination ──

    /// Notify a specific worker that its latch was set.
    pub(crate) fn notify_worker_latch_is_set(&self, target_worker_index: usize) {
        self.sleep.notify_worker_latch_is_set(target_worker_index);
    }

    /// Make the current worker thread wait on `latch`, stealing work in the
    /// meantime. Only valid when the current thread is a pool worker.
    pub(crate) fn wait_until_worker(latch: &CoreLatch) {
        let wt = WorkerThread::current();
        debug_assert!(!wt.is_null());
        unsafe { (*wt).wait_until(latch) };
    }

    /// If already on a worker thread of this registry, call `op` directly.
    /// Otherwise inject `op` as a job and block until it completes.
    pub(crate) fn in_worker<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce(&WorkerThread, bool) -> R + Send,
        R: Send,
    {
        let wt = WorkerThread::current();
        if !wt.is_null() && unsafe { (*wt).registry_id() == self.id() } {
            op(unsafe { &*wt }, false)
        } else {
            self.in_worker_cold(op)
        }
    }

    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn in_worker_cold<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce(&WorkerThread, bool) -> R + Send,
        R: Send,
    {
        thread_local!(static LOCK_LATCH: LockLatch = LockLatch::new());

        LOCK_LATCH.with(|l| {
            let job = StackJob::new(
                |injected| {
                    let wt = WorkerThread::current();
                    assert!(!wt.is_null());
                    op(unsafe { &*wt }, injected)
                },
                LatchRef::new(l),
            );
            // SAFETY: job lives on this stack frame until wait_and_reset returns.
            self.inject(unsafe { job.as_job_ref() });
            job.latch.wait_and_reset();
            unsafe { job.into_result() }
        })
    }

    // ── Termination ──

    pub(crate) fn increment_terminate_count(&self) {
        let prev = self.terminate_count.fetch_add(1, Ordering::AcqRel);
        debug_assert!(prev != 0);
        assert!(prev != usize::MAX, "overflow in terminate_count");
    }

    pub(crate) fn terminate(&self) {
        if self.terminate_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Only signal workers that exist (see `spawned`).
            let spawned = self.spawned.load(Ordering::Acquire);
            for (i, info) in self.thread_infos[..spawned].iter().enumerate() {
                unsafe {
                    OnceLatch::set_and_tickle_one(&raw const info.terminate, self, i);
                }
            }
        }
    }

    /// Wait for all workers to become ready (benchmark warm-up).
    pub(crate) fn wait_until_primed(&self) {
        for info in &self.thread_infos {
            info.primed.wait();
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        // Safety: we only drop the registry when all workers should stop.
        // If terminate_count hasn't reached 0, force terminate.
        if self.terminate_count.load(Ordering::Acquire) > 0 {
            self.terminate();
        }
        // Only the spawned prefix has a worker that will set `stopped`; on a
        // failed construction (spawn error at index k) the k..N-1 latches
        // belong to threads that never existed.
        let spawned = self.spawned.load(Ordering::Acquire);
        for info in &self.thread_infos[..spawned] {
            info.stopped.wait();
        }
    }
}

// ── Global registry ──

static GLOBAL_REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();

pub(crate) fn global_registry() -> &'static Arc<Registry> {
    GLOBAL_REGISTRY.get_or_init(|| {
        let cpus = crate::num_cpus();
        let registry = Registry::new(cpus);
        registry.wait_until_primed();
        registry
    })
}

/// Returns the registry for the current thread's pool, or the global pool.
pub(crate) fn current_registry() -> Arc<Registry> {
    let wt = WorkerThread::current();
    if wt.is_null() {
        Arc::clone(global_registry())
    } else {
        unsafe { Arc::clone((*wt).registry()) }
    }
}

// ── WorkerThread ──

pub(crate) struct WorkerThread {
    worker: Worker<JobRef>,
    index: usize,
    rng: XorShift64Star,
    registry: Arc<Registry>,
}

thread_local! {
    static WORKER_THREAD_STATE: Cell<*const WorkerThread> = const { Cell::new(ptr::null()) };
}

impl WorkerThread {
    #[inline]
    pub(crate) fn current() -> *const WorkerThread {
        WORKER_THREAD_STATE.get()
    }

    /// # Safety
    ///
    /// Must be called at most once per thread, before any other TLS access,
    /// with a pointer that stays valid until [`Self::current`] returns null
    /// again (the worker's `Drop` nulls it).
    unsafe fn set_current(thread: *const WorkerThread) {
        WORKER_THREAD_STATE.with(|t| {
            debug_assert!(t.get().is_null());
            t.set(thread);
        });
    }

    #[inline]
    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    #[inline]
    pub(crate) fn registry_id(&self) -> usize {
        self.registry.id()
    }

    #[inline]
    pub(crate) fn index(&self) -> usize {
        self.index
    }

    /// Push a job onto the local deque (overflow spills to the injector).
    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// # Safety
    ///
    /// Must only be called by the thread that owns this `WorkerThread` (TLS
    /// contract): `self.worker` is a single-producer deque.
    pub(crate) unsafe fn push(&self, job: JobRef) {
        let queue_was_empty = self.worker.is_empty();
        match self.worker.push(job) {
            Ok(()) => {
                self.registry.sleep.new_internal_jobs(1, queue_was_empty);
            },
            // Local deque full → spill to the injector (see LOCAL_DEQUE_CAPACITY).
            Err(overflow) => self.registry.inject(overflow),
        }
    }

    /// Push a batch of jobs onto the local deque with ONE sleep notification
    /// for the whole batch (see [`Registry::push_local_batch`]); individual
    /// overflow jobs spill to the injector.
    ///
    /// # Safety
    ///
    /// Must only be called by the thread that owns this `WorkerThread` (TLS
    /// contract): `self.worker` is a single-producer deque.
    pub(crate) unsafe fn push_batch(&self, jobs: impl Iterator<Item = JobRef>) {
        // Read before the first push: the empty→nonempty transition sizes the
        // wake cascade exactly like `inject_batch` does.
        let queue_was_empty = self.worker.is_empty();
        let mut pushed = 0usize;
        for job in jobs {
            if let Err(overflow) = self.worker.push(job) {
                // Local deque full → spill this job to the injector (see
                // LOCAL_DEQUE_CAPACITY). Stealers may drain concurrently, so
                // each later job retries the local deque instead of the whole
                // tail spilling. Cold path: capacity is 256 while the hybrid
                // batch is ≤ num_threads + chunk_slack.
                self.registry.inject(overflow);
            } else {
                pushed += 1;
            }
        }
        if pushed > 0 {
            // One JEC increment after all deque writes — a sleepy worker that
            // observes it aborts parking and re-searches via `find_work`,
            // whose steal scan covers every peer deque (the same protocol
            // the per-job `push` relies on).
            #[allow(clippy::cast_possible_truncation)]
            self.registry
                .sleep
                .new_internal_jobs(pushed as u32, queue_was_empty);
        }
    }

    /// Pop from the local deque.
    #[inline]
    pub(crate) fn try_pop_local(&self) -> Option<JobRef> {
        self.worker.pop()
    }

    fn has_injected_job(&self) -> bool {
        self.registry.has_injected_job()
    }

    /// Wait until `latch` is set, executing stolen work in the meantime.
    ///
    /// # Safety
    ///
    /// The caller must be a pool worker owning this `WorkerThread` (the
    /// wait loop dereferences the TLS pointer and runs arbitrary jobs), and
    /// `latch` must outlive the wait.
    #[inline]
    pub(crate) unsafe fn wait_until(&self, latch: &CoreLatch) {
        if !latch.probe() {
            unsafe { self.wait_until_cold(latch) };
        }
    }

    /// # Safety
    ///
    /// Same contract as [`Self::wait_until`]; additionally `latch` must not be
    /// deallocated while this thread runs stolen jobs.
    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    unsafe fn wait_until_cold(&self, latch: &CoreLatch) {
        let abort_guard = unwind::AbortIfPanic;

        'outer: while !latch.probe() {
            // Check for local work before going idle.
            if let Some(job) = self.try_pop_local() {
                unsafe { Self::execute(job) };
                continue;
            }

            let mut idle = self.registry.sleep.start_looking(self.index);
            while !latch.probe() {
                if let Some(job) = self.find_work() {
                    self.registry.sleep.work_found();
                    unsafe { Self::execute(job) };
                    continue 'outer;
                }
                self.registry
                    .sleep
                    .no_work_found(&mut idle, latch, || self.has_injected_job());
            }

            self.registry.sleep.work_found();
            break;
        }

        mem::forget(abort_guard);
    }

    /// # Safety
    ///
    /// Same contract as [`Self::wait_until`]: current thread must own this
    /// `WorkerThread`. The terminate latch is registry-owned and lives as long
    /// as the registry.
    unsafe fn wait_until_out_of_work(&self) {
        let index = self.index;
        let registry = &self.registry;
        unsafe {
            self.wait_until(registry.thread_infos[index].terminate.as_core_latch());
        }
        // Drain remaining local work.
        while let Some(job) = self.try_pop_local() {
            unsafe { Self::execute(job) };
        }
        // Let registry know we are done.
        unsafe { Latch::set(&raw const registry.thread_infos[index].stopped) };
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn find_work(&self) -> Option<JobRef> {
        // Preference: local deque → injected jobs → steal from peers.
        //
        // Checking the global injector *before* peer-stealing matches rayon's
        // order and matters for external-submit workloads (e.g. StreamPipeline,
        // where every task arrives via `pool.submit` → `inject`): the injector
        // pop is a single CAS-free dequeue, whereas `steal()` does a full
        // randomized peer-scan whose coherence traffic is wasted when the work
        // is actually sitting in the injector.
        self.try_pop_local()
            .or_else(|| self.registry.pop_injected_job())
            .or_else(|| self.steal())
    }

    /// # Safety
    ///
    /// Same contract as [`JobRef::execute`](super::job::JobRef::execute): the
    /// job must be valid, alive, and executed exactly once.
    #[inline]
    pub(crate) unsafe fn execute(job: JobRef) {
        unsafe { job.execute() };
    }

    /// Steal a single job from another worker. Only called when the local
    /// deque is empty.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn steal(&self) -> Option<JobRef> {
        let thread_infos = self.registry.thread_infos.as_slice();
        let num_threads = thread_infos.len();
        if num_threads <= 1 {
            return None;
        }

        // Full randomized scan of all victims (rayon-style). Our work arrives
        // via divide-and-conquer `join`, so at any instant only a few victims
        // hold (large) sub-trees; bounding the probe (e.g. to 4) measurably
        // *slows* work discovery — missing the victim-with-work across rounds
        // costs more than the empty-steal coherence traffic, which is anyway
        // parallelized across cores.
        loop {
            let mut retry = false;
            let start = self.rng.next_usize(num_threads);
            let job = (start..num_threads)
                .chain(0..start)
                .filter(|&i| i != self.index)
                .find_map(|victim_index| {
                    let victim = &thread_infos[victim_index];
                    // `steal_and_pop` with a budget of 1 returns the stolen job
                    // directly without pushing anything into our own deque.
                    match victim.stealer.steal_and_pop(&self.worker, |_| 1) {
                        Ok((job, _)) => Some(job),
                        Err(StealError::Empty) => None,
                        Err(StealError::Busy) => {
                            retry = true;
                            None
                        },
                    }
                });
            if job.is_some() || !retry {
                return job;
            }
            std::hint::spin_loop();
        }
    }
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        WORKER_THREAD_STATE.with(|t| {
            t.set(ptr::null());
        });
    }
}

/// Main loop for a worker thread. Allocated on the worker's stack.
///
/// # Safety
///
/// Must be called exactly once, at the bottom of a freshly spawned worker
/// thread, with `worker`/`registry` matching the thread's lifetime: the
/// function pins `WorkerThread` in TLS by pointer and only clears it on exit.
unsafe fn main_loop(worker: Worker<JobRef>, registry: Arc<Registry>, index: usize) {
    let worker_thread = WorkerThread {
        worker,
        index,
        rng: XorShift64Star::new(),
        registry,
    };
    // Pin the WorkerThread on the stack; its address is stable for the
    // lifetime of this function.
    let worker_thread_ref: &WorkerThread = &worker_thread;
    // SAFETY: `worker_thread_ref` outlives main_loop (it IS the stack frame).
    // The raw pointer in TLS is valid until we null it on drop.
    unsafe { WorkerThread::set_current(ptr::from_ref(worker_thread_ref)) };

    let registry = worker_thread_ref.registry();
    // Signal that we're ready.
    unsafe { Latch::set(&raw const registry.thread_infos[index].primed) };

    let abort_guard = unwind::AbortIfPanic;
    unsafe { worker_thread_ref.wait_until_out_of_work() };
    mem::forget(abort_guard);
}

/// Submit a `'static` closure as a heap job and inject it into the pool.
pub(crate) fn spawn_static<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    let job = HeapJob::new(f);
    let job_ref = job.into_static_job_ref();
    current_registry().inject(job_ref);
}

// ── RNG ──

/// xorshift* PRNG — fast, tolerates weak seeds (only zero is forbidden).
///
/// State is a `Cell<u64>` (not `AtomicU64`): `WorkerThread` is stored in TLS
/// and only ever touched by its owning worker thread, so single-threaded
/// interior mutability is both sound and cheaper than an atomic.
struct XorShift64Star {
    state: Cell<u64>,
}

impl XorShift64Star {
    fn new() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let mut seed = 0u64;
        while seed == 0 {
            let mut hasher = DefaultHasher::new();
            hasher.write_usize(COUNTER.fetch_add(1, Ordering::Relaxed));
            seed = hasher.finish();
        }
        XorShift64Star {
            state: Cell::new(seed),
        }
    }

    fn next(&self) -> u64 {
        let mut x = self.state.get();
        debug_assert_ne!(x, 0);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn next_usize(&self, n: usize) -> usize {
        // Result bounded by `n` (usize), safe to truncate on 32-bit
        (self.next() % n as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use super::*;

    /// Regression test for the spawn-failure hang: `thread_infos` is
    /// pre-created for all workers, so when construction fails at thread k
    /// the k..N-1 `stopped` latches have no thread to ever set them. `Drop`
    /// must wait only on the spawned prefix — pre-fix the last exiting worker
    /// parked forever inside `Drop` (or the panic thread itself, when k == 0).
    ///
    /// Partial construction is simulated directly (a real spawn failure needs
    /// RLIMIT_NPROC games unsuitable for CI): the `spawned` workers'
    /// `stopped` latches are pre-set as if they had exited, the ghost ones
    /// are left unset. A regression hangs the drop thread, caught by the
    /// timeout instead of wedging the test binary.
    #[test]
    fn drop_waits_only_for_spawned_workers() {
        for (num_threads, spawned) in [(4, 0), (4, 1), (4, 3), (8, 7)] {
            let thread_infos = (0..num_threads)
                .map(|_| {
                    let worker = Worker::<JobRef>::new(LOCAL_DEQUE_CAPACITY);
                    ThreadInfo::new(worker.stealer())
                })
                .collect();
            let registry = Arc::new(Registry {
                thread_infos,
                spawned: AtomicUsize::new(spawned),
                sleep: Sleep::new(num_threads),
                injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
                terminate_count: AtomicUsize::new(1),
            });
            for info in &registry.thread_infos[..spawned] {
                // SAFETY: `info` outlives the drop below.
                unsafe { Latch::set(ptr::from_ref(&info.stopped)) };
            }

            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                drop(registry);
                tx.send(()).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap_or_else(|_| {
                    panic!("Registry::drop hung with {spawned}/{num_threads} workers spawned")
                });
        }
    }
}
