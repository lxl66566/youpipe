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
/// by `st3`. When the local queue saturates, overflow spills into the global
/// injector — the same design used by the tokio scheduler, which keeps the hot
/// local queue bounded and cache-friendly.
const LOCAL_DEQUE_CAPACITY: usize = 256;

// ── Registry ──

pub(crate) struct Registry {
    thread_infos: Vec<ThreadInfo>,
    sleep: Sleep,
    /// Global injector queue for jobs coming from outside the pool or
    /// overflowing a worker's local deque.
    ///
    /// A lock-free, epoch-free MPMC queue (`concurrent_queue`) — the same
    /// block-based algorithm crossbeam's `Injector` used (WRITE/READ/DESTROY
    /// slot flags + direct `Box::from_raw` reclamation), but in a crate that
    /// does **not** pull in `crossbeam-epoch` (the source of the Miri UB that
    /// prompted the st3 migration). Its empty `pop` is 2 Acquire loads + a
    /// SeqCst fence with no CAS, which is cheaper on this 99%-empty hot
    /// path than a `Mutex<VecDeque>` *plus* a hand-maintained `AtomicUsize`
    /// length counter (measured: the extra `fetch_add`/`fetch_sub` on every
    /// push/pop costs more than it saves). Unbounded, so local-queue
    /// overflow never drops work.
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
            sleep: Sleep::new(num_threads),
            injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
            terminate_count: AtomicUsize::new(1),
        });

        for (index, worker) in workers.into_iter().enumerate() {
            let registry = Arc::clone(&registry);
            thread::Builder::new()
                .name(format!("yp-pool-{index}"))
                .spawn(move || {
                    unsafe { main_loop(worker, registry, index) };
                })
                .expect("failed to spawn pool worker");
        }

        registry
    }

    /// Opaque identity for this registry.
    fn id(&self) -> usize {
        std::ptr::from_ref::<Self>(self) as usize
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
        let mut count = 0u32;
        for job_ref in job_refs {
            let _ = self.injected_jobs.push(job_ref);
            count += 1;
        }
        if count > 0 {
            self.sleep.new_injected_jobs(count, queue_was_empty);
        }
    }

    fn has_injected_job(&self) -> bool {
        !self.injected_jobs.is_empty()
    }

    fn pop_injected_job(&self) -> Option<JobRef> {
        // `ConcurrentQueue::pop`'s empty path is 2 Acquire loads + a SeqCst
        // fence with no CAS — cheap enough that no separate length fast-path is
        // warranted (verified: a hand-maintained `AtomicUsize` length was
        // measurably slower, since its per-push/per-pop `fetch_add`/`fetch_sub`
        // bounce a cache line on every operation).
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
            for (i, info) in self.thread_infos.iter().enumerate() {
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
        for info in &self.thread_infos {
            info.stopped.wait();
        }
    }
}

// ── Global registry ──

static GLOBAL_REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();

pub(crate) fn global_registry() -> &'static Arc<Registry> {
    GLOBAL_REGISTRY.get_or_init(|| {
        let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
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
    fn registry_id(&self) -> usize {
        self.registry.id()
    }

    #[inline]
    pub(crate) fn index(&self) -> usize {
        self.index
    }

    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) unsafe fn push(&self, job: JobRef) {
        let queue_was_empty = self.worker.is_empty();
        match self.worker.push(job) {
            Ok(()) => {
                self.registry.sleep.new_internal_jobs(1, queue_was_empty);
            }
            // Local deque is full (256 slots): spill into the global injector.
            // This is the tokio overflow strategy — keeps the local queue
            // bounded and cache-friendly without dropping work.
            Err(overflow) => self.registry.inject(overflow),
        }
    }

    #[inline]
    fn local_deque_is_empty(&self) -> bool {
        self.worker.is_empty()
    }

    /// Pop from the local deque.
    #[inline]
    fn take_local_job(&self) -> Option<JobRef> {
        self.worker.pop()
    }

    /// Pop from the local deque (pub(crate) for join's use).
    #[inline]
    pub(crate) fn try_pop_local(&self) -> Option<JobRef> {
        self.worker.pop()
    }

    fn has_injected_job(&self) -> bool {
        self.registry.has_injected_job()
    }

    /// Wait until `latch` is set, executing stolen work in the meantime.
    #[inline]
    pub(crate) unsafe fn wait_until(&self, latch: &CoreLatch) {
        if !latch.probe() {
            unsafe { self.wait_until_cold(latch) };
        }
    }

    #[cold]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    unsafe fn wait_until_cold(&self, latch: &CoreLatch) {
        let abort_guard = unwind::AbortIfPanic;

        'outer: while !latch.probe() {
            // Check for local work before going idle.
            if let Some(job) = self.take_local_job() {
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

    unsafe fn wait_until_out_of_work(&self) {
        let index = self.index;
        let registry = &self.registry;
        unsafe {
            self.wait_until(registry.thread_infos[index].terminate.as_core_latch());
        }
        // Drain remaining local work.
        while let Some(job) = self.take_local_job() {
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
        self.take_local_job()
            .or_else(|| self.registry.pop_injected_job())
            .or_else(|| self.steal())
    }

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

        // Scan all victims each call. Unlike uniform async task pools, our
        // work arrives via divide-and-conquer `join`, so at any instant only a
        // few victims hold (large) sub-trees. Bounding the probe (e.g. to 4)
        // measurably *slows* work discovery — the latency of missing the
        // victim-with-work across rounds outweighs the empty-steal coherence
        // traffic, which is anyway parallelized across cores. So we keep the
        // classic rayon-style full randomized scan.
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
                        }
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
    unsafe { WorkerThread::set_current(std::ptr::from_ref(worker_thread_ref)) };

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
