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
use youpipe_sys::CachePadded;

use super::{
    job::{JobRef, StackJob},
    latch::{AsCoreLatch, CoreLatch, Latch, LatchRef, LockLatch, OnceLatch},
    sleep::Sleep,
    unwind,
};
use crate::handoff::{SpinBudget, SpinPoll, StealScanPoll};

/// Capacity of each worker's local (LIFO) deque. Rounded up to a power of two
/// by `st3`. When saturated, overflow spills into the global injector (tokio's
/// strategy): the hot local queue stays bounded and cache-friendly, no work is
/// dropped.
const LOCAL_DEQUE_CAPACITY: usize = 256;

/// Atomic backing `keep_warm`. Only this counter switches layer under
/// `--cfg loom`: the pool spawns real threads that loom cannot simulate, so the
/// models construct a `Registry` by literal and drive the three declaration
/// methods directly — every other atomic here stays `std` to keep the
/// real-thread tests on real primitives. `youpipe_sys::Ordering` is
/// `std::sync::atomic::Ordering` on both paths, so call sites need no cfg.
#[cfg(loom)]
type KeepWarmCount = youpipe_sys::AtomicUsize;
#[cfg(not(loom))]
type KeepWarmCount = AtomicUsize;

/// Atomic backing `parking_slots`, `cfg`-switched for the same reason as
/// [`KeepWarmCount`]: the lease is the crate's one all-or-nothing CAS
/// allocation and the models drive it without spawning workers.
#[cfg(loom)]
type ParkingCount = youpipe_sys::AtomicUsize;
#[cfg(not(loom))]
type ParkingCount = AtomicUsize;

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
    /// st3 migration). An empty `pop` is 2 Acquire loads + a SeqCst fence, no
    /// CAS — cheap enough that no `is_empty()` consult is worth a branch in
    /// front of it (measured, todo #44). A hand-maintained `AtomicUsize`
    /// length counter is a measured regression (its per-op
    /// `fetch_add`/`fetch_sub` bounces a cache line).
    injected_jobs: concurrent_queue::ConcurrentQueue<JobRef>,

    // When this reaches 0, all work on this registry must be complete. The
    // global pool has a ref that never gets released; a user-created pool
    // holds one ref via the ComputePool.
    terminate_count: AtomicUsize,
    /// Capacity leased by streaming runs for jobs that may **park on an
    /// inter-stage channel** (feeders, sync/expand stage workers). Such a job
    /// never returns to `find_work` until its run's channels drain, so a
    /// queued job of the same kind can be starved forever once every worker
    /// hosts a parked one. Invariant: `parking_slots ≤ num_threads` — if any
    /// parking job is still queued, at least one worker is free of parking
    /// jobs and will eventually pop it (regular jobs are finite). Leased
    /// atomically per run (`try_reserve_parking`) before the run's first job
    /// is submitted; released per job on completion (see `stream.rs`).
    parking_slots: ParkingCount,
    /// Live `ComputePool::keep_warm` guard count (todo #40); >0 = declared.
    /// `CachePadded`: idle workers load it once per idle episode (see
    /// `wait_until_cold`) — keep that load off the other registry lines.
    /// Refcounted like `terminate_count`, so a leaked guard only leaves the
    /// declared state on (the window decays to pure parking via
    /// `SpinBudget::note_park`).
    keep_warm: CachePadded<KeepWarmCount>,
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
    pub(crate) fn new(num_threads: usize, pin_workers: bool) -> Arc<Self> {
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
            parking_slots: ParkingCount::new(0),
            keep_warm: CachePadded(KeepWarmCount::new(0)),
        });

        // Worker→core pinning (`ComputePool::new_pinned`, or the
        // `YOUPIPE_PIN_WORKERS` A/B knob for every pool): worker `i` is
        // pinned to the i-th CPU of the process's allowed set (round-robin
        // when the pool is larger than the set). Rationale, regime limits,
        // and A/B numbers: dev/scheduler.md "worker affinity".
        let pin_targets = Arc::new(if pin_workers || workers_pinned_by_env() {
            youpipe_sys::allowed_cpus()
        } else {
            Vec::new()
        });

        for (index, worker) in workers.into_iter().enumerate() {
            let thread_registry = Arc::clone(&registry);
            let pin_targets = Arc::clone(&pin_targets);
            match thread::Builder::new()
                .name(format!("yp-pool-{index}"))
                .spawn(move || {
                    if !pin_targets.is_empty() {
                        // Round-robin past the end of the allowed set so every
                        // worker keeps the "parks wake on their own idle core"
                        // guarantee. Best-effort: on failure the worker simply
                        // stays unpinned (see `pin_current_thread_to`).
                        let cpu = pin_targets[index % pin_targets.len()];
                        let _ = youpipe_sys::pin_current_thread_to(cpu);
                    }
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
    /// The calling thread's [`WorkerThread`] iff it is a worker of *this*
    /// registry — the shared "am I on this pool?" check behind
    /// [`Self::inject_or_push`], [`Self::push_local_batch`],
    /// [`Self::in_worker`], and `ComputePool::on_this_pool_owner`.
    ///
    /// # SAFETY argument (concentrated here)
    ///
    /// `WorkerThread::current()` returns the TLS pointer that the worker's
    /// own `main_loop` set to its stack-allocated `WorkerThread` (and that
    /// `WorkerThread::drop` nulls before that frame goes away), so on the
    /// calling thread a non-null pointer is dereferenceable for the duration
    /// of the borrow. The `registry_id` re-check rejects a *foreign*
    /// registry's worker: pool threads regularly call into another pool's
    /// methods (a streaming run driving a nested pool), and only a
    /// same-registry worker may take the local-deque fast paths. The
    /// returned reference is `!Send` (`WorkerThread` holds a `Cell` RNG) and
    /// borrows `self`, so callers cannot smuggle it off the calling thread
    /// or past the registry's lifetime.
    #[inline]
    pub(crate) fn current_worker_of(&self) -> Option<&WorkerThread> {
        let wt = WorkerThread::current();
        if !wt.is_null() && unsafe { (*wt).registry_id() } == self.id() {
            // SAFETY: see the doc comment above.
            Some(unsafe { &*wt })
        } else {
            None
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn inject_or_push(&self, job_ref: JobRef) {
        match self.current_worker_of() {
            // SAFETY: `wt` is the calling thread's own WorkerThread (the
            // SPSC local-deque contract of `push`).
            Some(wt) => unsafe { wt.push(job_ref) },
            None => self.inject(job_ref),
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
        match self.current_worker_of() {
            // SAFETY: `wt` is the calling thread's own WorkerThread (the
            // SPSC local-deque contract of `push_batch`).
            Some(wt) => unsafe { wt.push_batch(jobs) },
            None => self.inject_batch(jobs),
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
        // segment (≤ BLOCK_CAP = 31 slots) instead of one contended CAS per
        // job. Single CAS only while the batch fits one segment: the
        // unbalanced wide tier injects `num_threads + 16` chunks minus the
        // driver's (47 on a 32-thread pool), pools above 32 threads exceed
        // BLOCK_CAP even without slack, and the public `submit_batch` takes
        // any size — larger batches just pay one CAS per extra segment.
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
        // No advisory `is_empty()` consult in front of the pop: on an empty
        // queue `pop()` itself is 2 Acquire loads + a SeqCst fence (no CAS, no
        // block deref) and returns `Err(Empty)` — the same `None`. Its only
        // wait loops are transient block-install races that a concurrent
        // pusher (or the popper completing a block transition) resolves in
        // bounded time; a quiescent drained queue returns immediately
        // (youpipe-concurrent-queue unbounded.rs). A consult was kept for a
        // time on a recompile A/B's ~+2.3% mixed/1K reading, but the
        // same-binary knob A/B (todo #44, 2026-11-08) measured both sides
        // noise-equal on every cell — that +2.3% was a layout draw — and
        // removed it; readings in benchmarks/attribution.md. A
        // hand-maintained length counter remains rejected (see the
        // `injected_jobs` field doc).
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
        match self.current_worker_of() {
            Some(wt) => op(wt, false),
            None => self.in_worker_cold(op),
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

    /// Atomically lease `n` parking slots (see `parking_slots`). Fails —
    /// without side effects — when the remaining capacity cannot host the
    /// whole request; a partial lease would leave a run half-admitted, which
    /// is exactly the deadlock this reservation prevents.
    pub(crate) fn try_reserve_parking(&self, n: usize) -> bool {
        let cap = self.num_threads();
        let mut used = self.parking_slots.load(Ordering::Acquire);
        loop {
            if used + n > cap {
                return false;
            }
            match self.parking_slots.compare_exchange_weak(
                used,
                used + n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => used = current,
            }
        }
    }

    /// Current parking-slot usage (leased minus released). A snapshot only —
    /// admission decisions must go through [`Self::try_reserve_parking`],
    /// which revalidates against the live counter.
    pub(crate) fn parking_used(&self) -> usize {
        self.parking_slots.load(Ordering::Acquire)
    }

    /// Return `n` leased parking slots. Called per job on completion and for
    /// the never-spawned remainder of a lease when the run finishes.
    pub(crate) fn release_parking(&self, n: usize) {
        if n > 0 {
            self.parking_slots.fetch_sub(n, Ordering::AcqRel);
        }
    }

    pub(crate) fn increment_terminate_count(&self) {
        let prev = self.terminate_count.fetch_add(1, Ordering::AcqRel);
        debug_assert!(prev != 0);
        assert!(prev != usize::MAX, "overflow in terminate_count");
    }

    // ── Caller-declared keep-warm (todo #40) ──

    /// One declaration level up (`KeepWarm` construction). `Release`: paired
    /// with the episode-snapshot load below, an idle worker that observes
    /// the declaration also observes everything sequenced before the
    /// declaring call site.
    #[inline]
    pub(crate) fn declare_keep_warm(&self) {
        self.keep_warm.fetch_add(1, Ordering::Release);
    }

    /// Whether a keep-warm declaration is live. One padded `Acquire` load
    /// per idle episode — the sole cost the keep-warm machinery adds to the
    /// default (undeclared, knob-off) path; the precedence resolver runs
    /// only when this returns `true` (see `wait_until_cold`).
    #[inline]
    pub(crate) fn keep_warm_declared(&self) -> bool {
        self.keep_warm.load(Ordering::Acquire) > 0
    }

    /// One declaration level down (`KeepWarm::drop`). `AcqRel`: the RMW both
    /// releases the declaring thread's prior writes and acquires the
    /// counter's latest value, so a racing `declare` on the same level is
    /// never lost.
    #[inline]
    pub(crate) fn release_keep_warm(&self) {
        self.keep_warm.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn terminate(&self) {
        if self.terminate_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            note_terminate_broadcast();
            // Only signal workers that exist (see `spawned`).
            let spawned = self.spawned.load(Ordering::Acquire);
            for (i, info) in self.thread_infos[..spawned].iter().enumerate() {
                unsafe {
                    OnceLatch::set_and_tickle_one(&raw const info.terminate, self, i);
                }
            }
        }
    }

    /// Test-only: number of workers currently in a real condvar park. Used
    /// by keep-warm leak tests outside the `pool` module to pin the decay
    /// safety net (a live declaration must not prevent parking).
    #[cfg(test)]
    pub(crate) fn sleeping_threads(&self) -> usize {
        self.sleep.sleeping_threads()
    }

    /// Wait for all workers to become ready (benchmark warm-up).
    ///
    /// The full vector is correct here, unlike [`Drop for Registry`]: its only
    /// caller, [`global_registry`], runs it on the value [`Registry::new`]
    /// returned, and `new` panics instead of returning when a spawn fails — the
    /// ghost tail only exists on that panic path.
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

/// Same-binary A/B escape hatch for worker pinning (default off): pins every
/// pool in the process, including the global one. The supported opt-in is
/// [`crate::ComputePool::new_pinned`]; this knob exists so scheduler-side
/// experiments and the documented A/B numbers stay re-runnable without
/// recompiles (the established methodology — see `YOUPIPE_OVERSPLIT`).
fn workers_pinned_by_env() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED
        .get_or_init(|| std::env::var("YOUPIPE_PIN_WORKERS").is_ok_and(|v| v == "1" || v == "true"))
}

// ── Global registry ──

static GLOBAL_REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();

pub(crate) fn global_registry() -> &'static Arc<Registry> {
    GLOBAL_REGISTRY.get_or_init(|| {
        let cpus = crate::num_cpus();
        // Unpinned by default; `YOUPIPE_PIN_WORKERS` still covers the global
        // pool as the same-binary A/B escape hatch.
        let registry = Registry::new(cpus, false);
        registry.wait_until_primed();
        registry
    })
}

// ── WorkerThread ──

pub(crate) struct WorkerThread {
    worker: Worker<JobRef>,
    index: usize,
    rng: XorShift64Star,
    /// Adaptive pre-park spin window for the idle loop (the pool-side port
    /// of the data plane's `SpinBudget`): plain thread-private state, held
    /// for the worker's whole lifetime so park-latency feedback adapts the
    /// window across idle episodes. `Cell` because every accessor runs
    /// under the TLS `&self` contract (only the owning thread touches it).
    idle_spin: Cell<SpinBudget<StealScanPoll>>,
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
    ///
    /// # Local-deque invariant
    ///
    /// Unlike rayon (whose local deques hold only self-capturing `StackJob`s
    /// — `spawn` always injects to the global queue), our on-pool fast paths
    /// also admit heap jobs here: `submit` via [`Registry::inject_or_push`],
    /// hybrid chunks via [`Self::push_batch`]. The deques' consumers execute
    /// popped jobs bare — [`join_on_captured`](super::join)'s wait loop has
    /// no panic guard — so every job type admitted to a local deque must
    /// uphold the `Job::execute` no-unwind contract: `StackJob` captures
    /// into its result slot, hybrid `ChunkJob` into its fail slot, `HeapJob`
    /// aborts. A job that could unwind past `execute` would destroy the
    /// waiter's frame while its stack-allocated jobs' refs still sit in the
    /// deque → UB.
    #[inline]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// # Safety
    ///
    /// Must only be called by the thread that owns this `WorkerThread` (TLS
    /// contract): `self.worker` is a single-producer deque.
    pub(crate) unsafe fn push(&self, job: JobRef) {
        // Measured 2026-10-25 (cargo-show-asm on `Registry::inject_or_push`):
        // this is_empty+push pair compiles to 5 atomic reads of the deque
        // (push_count x2, pop_count_and_head x2, head x1) — LLVM does not CSE
        // the duplicate Relaxed loads even after inlining. A fused st3 API
        // would reach 3 but needs fork edits; rejected (<1% expected gain,
        // record in docs/src/dev/scheduler.md "Vendored scheduler
        // dependencies").
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
        let mut spill: Vec<JobRef> = Vec::new();
        for job in jobs {
            if let Err(overflow) = self.worker.push(job) {
                // Local deque full → spill (see LOCAL_DEQUE_CAPACITY). The
                // deque only fills when the batch exceeds its 256 slots:
                // the hybrid batch is num_threads + chunk_slack chunks, so
                // this path is cold up to ~240 workers and then fires on
                // EVERY dispatch — collect the overflow tail and inject it
                // as ONE batch (single SeqCst fence + JEC increment) instead
                // of per-job injects serializing on the driver. Stealers may
                // drain concurrently, so every job still retries the local
                // deque first; only the actual overflows reach the tail.
                spill.push(overflow);
            } else {
                pushed += 1;
            }
        }
        if !spill.is_empty() {
            note_spill_tail_batch();
            self.registry.inject_batch(spill.into_iter());
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
            // Episode-local copy of the adaptive window state (thread-private
            // plain data; folded back into `idle_spin` at every exit below).
            let mut spin = self.idle_spin.get();
            // Caller-declared keep-warm (todo #40): a live declaration lends
            // this episode the default window when neither the env knob (same
            // machinery, already armed) nor an explicit `YOUPIPE_SPIN_ROUNDS`
            // pin (precedence: the operator pinned the ladder) covers it.
            // `base_max` is the strip target for every fold-back below: the
            // learned budget persists across episodes, the declared ceiling
            // never does, so a declaration cannot mutate the env-armed state.
            // When the resolver yields 0 the strip is an identity and the
            // ladder runs unchanged.
            let base_max = spin.max_us();
            // The counter load is the declaration signal and the ONLY cost on
            // the default (undeclared) path; the OnceLock read and the
            // resolver's precedence run only under a live declaration.
            let declared_max = if self.registry.keep_warm_declared() {
                super::sleep::resolve_keep_warm_window(
                    base_max,
                    super::sleep::spin_rounds_pinned(),
                    true,
                )
            } else {
                0
            };
            if declared_max != 0 {
                spin = spin.with_max_us(declared_max);
            }
            while !latch.probe() {
                // Adaptive window phase (knob armed): replaces the busy-spin
                // arm — poll with geometric pause backoff for up to the
                // current budget, then land on the yield arm. Knob off: the
                // guard fails and the ladder below runs unchanged.
                if spin.armed() && idle.in_spin_phase() {
                    let mut window_job: Option<JobRef> = None;
                    match spin.spin_poll(|| {
                        // A set latch ends the wait: stop mid-window, so a
                        // completed join never lags a full window.
                        if latch.probe() {
                            return SpinPoll::Closed;
                        }
                        // Stage-1 poll closure: local deque + injector only,
                        // no steal scan. The gap-idle signal (a new batch
                        // arriving via `inject_batch`) is fully visible;
                        // stealable ramp-down subtrees stay with the
                        // yield-round `find_work` below, delayed by at most
                        // the window.
                        match self
                            .try_pop_local()
                            .or_else(|| self.registry.pop_injected_job())
                        {
                            Some(job) => {
                                window_job = Some(job);
                                SpinPoll::Ready(1)
                            },
                            None => SpinPoll::Empty,
                        }
                    }) {
                        SpinPoll::Ready(_) => {
                            self.idle_spin.set(spin.with_max_us(base_max));
                            self.registry.sleep.work_found();
                            unsafe {
                                Self::execute(window_job.expect("window poll returned Ready"));
                            };
                            continue 'outer;
                        },
                        // Latch set mid-window: fall through to the episode
                        // exit; the probe re-check ends the wait.
                        SpinPoll::Closed => break,
                        SpinPoll::Empty => idle.window_exhausted(),
                    }
                }
                if let Some(job) = self.find_work() {
                    self.idle_spin.set(spin.with_max_us(base_max));
                    self.registry.sleep.work_found();
                    unsafe { Self::execute(job) };
                    continue 'outer;
                }
                // `Some(d)` only from rounds that attempted the park protocol
                // inside `Sleep::sleep`; fold it back into the window.
                if let Some(parked) = self
                    .registry
                    .sleep
                    .no_work_found(&mut idle, latch, || self.has_injected_job())
                {
                    if spin.armed() {
                        spin.note_park(parked);
                    }
                }
            }

            self.idle_spin.set(spin.with_max_us(base_max));
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
        // Drain remaining local work, then whatever is still queued in the
        // injector. A `JobRef` owns its heap box, so a job left unexecuted
        // here would leak — and were the pool ever revived (pool cache), the
        // stale task would run at a random later point. Every worker runs
        // this drain, and each pass re-checks both queues: a job executed
        // during the drain may itself submit, landing in its executor's own
        // local deque or the injector, so we loop until a full pass finds
        // nothing new. The last worker to stop therefore leaves both queues
        // empty (any push is followed by its pusher's own drain pass).
        loop {
            let mut ran = false;
            while let Some(job) = self.try_pop_local() {
                ran = true;
                unsafe { Self::execute(job) };
            }
            while let Some(job) = registry.pop_injected_job() {
                ran = true;
                unsafe { Self::execute(job) };
            }
            if !ran {
                break;
            }
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
        idle_spin: Cell::new(SpinBudget::new(super::sleep::pool_spin_budget_us())),
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

/// Test instrumentation for `push_batch`'s overflow-tail path (see there):
/// counts one per batched tail injection. Compiled out of non-test builds.
///
/// The `--cfg loom` builds keep the count thread-local: loom runs a model on
/// the test's own OS thread, and several models share one process, so a
/// process-wide static cannot be asserted from inside a model.
#[cfg(all(test, not(loom)))]
static SPILL_TAIL_BATCHES: AtomicUsize = AtomicUsize::new(0);

#[cfg(all(test, loom))]
thread_local! {
    static SPILL_TAIL_BATCHES: AtomicUsize = const { AtomicUsize::new(0) };
}

#[cfg(not(test))]
#[inline]
fn note_spill_tail_batch() {}

#[cfg(all(test, not(loom)))]
fn note_spill_tail_batch() {
    SPILL_TAIL_BATCHES.fetch_add(1, Ordering::Relaxed);
}

#[cfg(all(test, loom))]
fn note_spill_tail_batch() {
    SPILL_TAIL_BATCHES.with(|count| count.fetch_add(1, Ordering::Relaxed));
}

/// Batched spill count this thread observed (see [`SPILL_TAIL_BATCHES`]). Gated
/// like its only reader, the `st3_loom` batch models.
#[cfg(all(test, loom, st3_loom))]
fn spill_tail_batches() -> usize {
    SPILL_TAIL_BATCHES.with(|count| count.load(Ordering::Relaxed))
}

#[cfg(all(test, loom))]
thread_local! {
    /// Test instrumentation for `terminate`: counts one per entry into its
    /// broadcast branch, i.e. per caller that finds `terminate_count` at 1. A
    /// `OnceLatch` can only be set once, so a second broadcast over the same
    /// prefix would leave no trace on the latches; this count is what makes
    /// "exactly one broadcast" assertable. Thread-local for the same reason as
    /// [`SPILL_TAIL_BATCHES`]'s loom variant: several models share one process,
    /// and each asserts the delta across its own execution.
    static TERMINATE_BROADCASTS: AtomicUsize = const { AtomicUsize::new(0) };
}

#[cfg(not(all(test, loom)))]
#[inline]
fn note_terminate_broadcast() {}

#[cfg(all(test, loom))]
fn note_terminate_broadcast() {
    TERMINATE_BROADCASTS.with(|count| count.fetch_add(1, Ordering::Relaxed));
}

/// Broadcast-branch entries this thread observed (see
/// [`TERMINATE_BROADCASTS`]). Gated like its only readers, the `terminate`
/// models.
#[cfg(all(test, loom))]
fn terminate_broadcasts() -> usize {
    TERMINATE_BROADCASTS.with(|count| count.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::{
        super::sleep::{DEFAULT_POOL_SPIN_BUDGET_US, set_pool_spin_budget_for_tests},
        *,
    };

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
                parking_slots: ParkingCount::new(0),
                keep_warm: CachePadded(KeepWarmCount::new(0)),
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

    /// Knob-on smoke test: a real small pool with the adaptive window armed
    /// through the test-only knob override. Burst submissions with µs-spaced
    /// gaps must stay live (each burst runs the full wake protocol against
    /// windowing-or-parked workers), termination must join cleanly, and once
    /// input stops every worker must reach a real park — quiet parks decay
    /// the budget, so armed workers park rather than spin forever.
    #[test]
    fn pool_spin_budget_knob_on_smoke() {
        set_pool_spin_budget_for_tests(DEFAULT_POOL_SPIN_BUDGET_US);

        let registry = Registry::new(2, false);
        let ran = AtomicUsize::new(0);
        for burst in 0..30 {
            for _ in 0..4 {
                registry.in_worker(|_, _| {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            }
            // Alternate a window-sized gap (grows the budget) with a quiet
            // gap (decays it), exercising both feedback arms.
            let gap = if burst % 2 == 0 {
                50
            } else {
                2_000
            };
            thread::sleep(Duration::from_micros(gap));
        }
        assert_eq!(ran.load(Ordering::Relaxed), 120);

        // Input stopped: every worker must reach a real park.
        let deadline = Instant::now() + Duration::from_secs(10);
        while registry.sleep.sleeping_threads() < 2 {
            assert!(
                Instant::now() < deadline,
                "workers never parked after input stopped"
            );
            thread::sleep(Duration::from_millis(2));
        }

        // Termination joins cleanly.
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            drop(registry);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(10))
            .expect("registry drop (terminate) hung");

        set_pool_spin_budget_for_tests(0);
    }

    /// Cross-registry TLS identity: a worker of pool A calling pool B's
    /// worker-scoped entry points must be rejected by `current_worker_of`
    /// and take the cold path (`in_worker_cold` → inject into B + block on
    /// B's latch). This check is the single guard keeping a foreign worker
    /// off B's fast paths (pool threads routinely call into another pool's
    /// methods — see `current_worker_of`'s SAFETY doc); a regression routes
    /// B's `Stealing` `CountLatch` owner to A's `worker_index` inside B's
    /// sleep state (wrong-worker wake at best, out-of-bounds when A is the
    /// larger pool). Pins all three faces: identity, the injected flag, and
    /// an end-to-end `join` on B from A's worker.
    #[test]
    fn cross_registry_worker_identity_is_rejected() {
        let registry_a = Arc::new(Registry::new(2, false));
        let registry_b = Arc::new(Registry::new(2, false));
        let b_id = registry_b.id();

        let (tx, rx) = mpsc::channel();
        // `in_worker` from a plain thread runs the op ON a worker of A, which
        // is the context under test; the spawned thread + timeout keep a
        // blocking regression from wedging the test binary.
        let a = Arc::clone(&registry_a);
        let b = Arc::clone(&registry_b);
        thread::spawn(move || {
            let sum = a.in_worker(|_, _| {
                // Identity faces: own registry recognized, foreign rejected.
                assert!(a.current_worker_of().is_some());
                assert!(b.current_worker_of().is_none());
                // `in_worker` on B must run the op as an INJECTED job on one
                // of B's own workers, never inline on A's worker.
                b.in_worker(|wt_b, injected| {
                    assert!(injected, "op must take B's cold path");
                    assert_eq!(wt_b.registry_id(), b_id);
                });
                // End-to-end: `join` on B from A's worker routes the same way.
                let (x, y) = crate::pool::join::join(&b, || 1u32, || 2u32);
                x + y
            });
            tx.send(sum).unwrap();
        });
        let sum = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("cross-registry in_worker/join hung");
        assert_eq!(sum, 3);
    }

    /// `push_batch` overflow: a batch larger than `LOCAL_DEQUE_CAPACITY`
    /// (256) must fill the local deque, then spill the whole remaining tail
    /// to the injector as ONE batch (`SPILL_TAIL_BATCHES` pins the single
    /// fence+JEC round-trip) — the path every hybrid dispatch takes on
    /// pools above ~240 workers. Deterministic on a ONE-worker pool: no
    /// stealers exist, so the push loop fills all 256 slots before the
    /// 44-job tail spills (on multi-worker pools peers woken by the
    /// dispatch itself can drain the deque mid-loop, which is exactly why
    /// the tail batches rather than assuming a fixed split). The op is the
    /// only executor, so it drains its own deque and the spilled tail
    /// itself instead of waiting.
    ///
    /// Real-thread test only: under `--cfg loom` the registry's sleep and
    /// parking atomics are loom's, which panic outside a model, and the spill
    /// probe there is thread-local (`st3_loom_tests` owns the modeled split).
    #[cfg(not(loom))]
    #[test]
    fn push_batch_overflow_spills_tail_as_one_batch() {
        SPILL_TAIL_BATCHES.store(0, Ordering::Relaxed);
        let registry = Arc::new(Registry::new(1, false));
        let ran = Arc::new(AtomicUsize::new(0));

        let r2 = Arc::clone(&registry);
        let ran2 = Arc::clone(&ran);
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            r2.in_worker(|wt, _| {
                let jobs = (0..300).map(|_| {
                    let ran = Arc::clone(&ran2);
                    crate::pool::job::HeapJob::new(move || {
                        ran.fetch_add(1, Ordering::Relaxed);
                    })
                    .into_static_job_ref()
                });
                // Contract: the caller must be a worker of this registry
                // (we are inside `in_worker`).
                // `push_local_batch` is a safe fn (it does its own
                // `current_worker_of` dispatch) — the `unsafe` wrapper here
                // was stale from before todo #39 made it so.
                r2.push_local_batch(jobs);
                let deadline = Instant::now() + Duration::from_secs(30);
                while ran2.load(Ordering::Relaxed) < 300 {
                    assert!(Instant::now() < deadline, "overflow-tail jobs stalled");
                    if let Some(job) = wt.try_pop_local().or_else(|| r2.pop_injected_job()) {
                        // SAFETY: popped by the owning worker / from our own
                        // registry's injector, executed exactly once.
                        unsafe { WorkerThread::execute(job) };
                    }
                }
            });
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(30))
            .expect("push_batch overflow test hung");
        assert_eq!(ran.load(Ordering::Relaxed), 300);
        assert_eq!(
            SPILL_TAIL_BATCHES.load(Ordering::Relaxed),
            1,
            "the whole overflow tail must reach the injector as one batch"
        );
    }

    /// The `StealError::Busy` retry contract `WorkerThread::steal` loops on
    /// (todo #41): while a victim's head booking is held open by a concurrent
    /// stealer — modelled deterministically by an open `Worker::drain`, the
    /// same `book_items` CAS a competing `steal_and_pop` takes — every steal
    /// attempt must return `Busy`, and releasing the booking (the drain
    /// dropped mid-iteration) must let the retry succeed and hand over the
    /// first unbooked item. No loom model races this in youpipe: under the
    /// `--cfg loom` block st3 keeps real atomics (its simulations need the
    /// fork's own `st3_loom` cfg), so a model here would pass vacuously;
    /// booking contention is model-checked by the fork's `multi_stealer` /
    /// `drain_overflow` models instead.
    #[test]
    fn steal_and_pop_busy_until_drain_booking_released() {
        let victim = Worker::<u32>::new(LOCAL_DEQUE_CAPACITY);
        let stealer = victim.stealer();
        // The thief's own deque, as `steal`'s budget-1 destination.
        let thief = Worker::<u32>::new(LOCAL_DEQUE_CAPACITY);

        victim.push(1).unwrap();
        victim.push(2).unwrap();

        // Book only the head item (budget 1, the pool's steal idiom), leaving
        // item 2 beyond the booking for the post-release retry to steal.
        let booking = victim.drain(|_| 1).unwrap();
        assert_eq!(
            stealer.steal_and_pop(&thief, |_| 1),
            Err(StealError::Busy),
            "an open booking must block concurrent stealers"
        );

        // Dropping mid-booking consumes the booked item and advances the
        // head, releasing the booking.
        drop(booking);
        assert_eq!(
            stealer.steal_and_pop(&thief, |_| 1),
            Ok((2, 0)),
            "retry after release must steal the first unbooked item"
        );
        // Full accounting: of the two pushed items one was consumed by the
        // dropped booking, one was stolen — the victim is empty and nothing
        // leaked into the thief's deque (budget 1 returns the job directly).
        assert_eq!(victim.pop(), None);
        assert_eq!(thief.pop(), None);
    }
}

/// Pool-side protocol models (`keep_warm` publication, `parking_slots` lease,
/// `terminate` broadcast). `Registry::new` spawns real OS threads, so every
/// model builds the registry by literal (the shape
/// `tests::drop_waits_only_for_spawned_workers` uses) and drives one protocol
/// directly. Exploration is small but real: 22 / 379 / 51 executions for the
/// three `keep_warm` models at `LOOM_MAX_PREEMPTIONS=2` (two threads). The four
/// `terminate` models each finish below the harness's 10 ms timer resolution at
/// the same budget and at most three threads.
///
/// `terminate_count` and `spawned` stay `std` atomics like the rest of the
/// registry, so the models assert the protocol's outcome over the reachable
/// thread orders; their scheduling points are the thread boundaries and the
/// `terminate` latches, whose sets are modeled.
#[cfg(all(test, loom))]
mod loom_tests {
    use loom::{cell::UnsafeCell, sync::Arc};

    use super::*;
    use crate::builder::ParkingLease;

    /// Registry with no workers: `spawned == 0` and empty `thread_infos` keep
    /// every spawn/terminate path out of the model.
    fn workerless_registry() -> Arc<Registry> {
        Arc::new(Registry {
            thread_infos: Vec::new(),
            spawned: AtomicUsize::new(0),
            sleep: Sleep::new(1),
            injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
            terminate_count: AtomicUsize::new(1),
            parking_slots: ParkingCount::new(0),
            keep_warm: CachePadded(KeepWarmCount::new(0)),
        })
    }

    /// The claim `declare_keep_warm`'s `Release` buys: an idle episode that
    /// observes the declaration also observes everything the declaring call
    /// site sequenced before itself.
    ///
    /// The detector is loom's causality tracker, not the value assertion: the
    /// model runs one thread at a time, so `*ptr` can never tear. `with()`
    /// panics with "Causality violation: Concurrent read and write accesses"
    /// when the read does not happen-after the write. Verified by flipping the
    /// declaration to `Relaxed` (`declare_keep_warm`) and, separately, the
    /// episode snapshot to `Relaxed` (`keep_warm_declared`): both make this
    /// model fail on that panic, so it would catch a missing fence.
    #[test]
    fn declaration_publishes_the_declaring_threads_writes() {
        /// Value the declaring thread publishes before its declaration.
        const PUBLISHED: u8 = 0x2a;

        loom::model(|| {
            let registry = workerless_registry();
            let published = Arc::new(UnsafeCell::new(0u8));

            let declarer_registry = Arc::clone(&registry);
            let declarer_published = Arc::clone(&published);
            let declarer = loom::thread::spawn(move || {
                // SAFETY: the closure holds the only mutable reference for the
                // duration of the write, and the reader takes the cell only
                // after it observed the declaration.
                declarer_published.with_mut(|ptr| unsafe { *ptr = PUBLISHED });
                declarer_registry.declare_keep_warm();
            });

            // One idle worker's `wait_until_cold` loop: the episode snapshot is
            // re-taken every round, and only a live declaration licenses
            // reading the declaring thread's data.
            for _ in 0..3 {
                if registry.keep_warm_declared() {
                    // SAFETY: licensed by the declaration observed above.
                    let seen = published.with(|ptr| unsafe { *ptr });
                    assert_eq!(seen, PUBLISHED, "declaration did not publish the write");
                    break;
                }
            }

            declarer.join().unwrap();
            assert!(
                registry.keep_warm_declared(),
                "a live level must read as declared"
            );
        });
    }

    /// Two nested levels, answered by two releases (the `AcqRel` pair). What
    /// this pins: a live level always reads as declared, a thread's surviving
    /// level keeps the declaration visible across its first release, and every
    /// declare is answered by exactly one release. A counter's RMW order is
    /// coherent, so no increment can be *lost*: the failure mode the bounds
    /// catch is an unbalanced release, which wraps the refcount to
    /// `usize::MAX` and leaves `keep_warm_declared` true forever.
    #[test]
    fn concurrent_levels_balance_and_never_underflow() {
        /// Two levels per declaring thread, both threads live at once.
        const MAX_LEVELS: usize = 4;

        // One declaring thread's episode: two nested levels released at exit.
        fn hold_two_levels(registry: &Registry) {
            registry.declare_keep_warm();
            assert!(
                registry.keep_warm_declared(),
                "a live level must read as declared"
            );
            assert!(registry.keep_warm.load(Ordering::Acquire) <= MAX_LEVELS);
            registry.declare_keep_warm();
            assert!(
                registry.keep_warm_declared(),
                "a live level must read as declared"
            );
            assert!(registry.keep_warm.load(Ordering::Acquire) <= MAX_LEVELS);
            registry.release_keep_warm();
            assert!(
                registry.keep_warm_declared(),
                "the nested level must survive the inner release"
            );
            assert!(registry.keep_warm.load(Ordering::Acquire) <= MAX_LEVELS);
            registry.release_keep_warm();
        }

        loom::model(|| {
            let registry = workerless_registry();

            let declarer_registry = Arc::clone(&registry);
            let declarer = loom::thread::spawn(move || hold_two_levels(&declarer_registry));

            hold_two_levels(&registry);
            declarer.join().unwrap();

            assert_eq!(
                registry.keep_warm.load(Ordering::Acquire),
                0,
                "balanced levels must land on zero"
            );
            assert!(
                !registry.keep_warm_declared(),
                "no level is live at quiescence"
            );
        });
    }

    /// The fold-back contract behind `wait_until_cold`: a declaration only
    /// lends the episode window and must leave the registry exactly as it found
    /// it. The window arithmetic itself is
    /// `sleep::resolve_keep_warm_window`'s pure-function contract (unit-tested
    /// in `sleep::tests::keep_warm_window_precedence`); what a model adds is
    /// that the episode touches no other counter, sampled by an idle worker
    /// racing it.
    #[test]
    fn declaration_episode_leaves_registry_state_untouched() {
        loom::model(|| {
            let registry = workerless_registry();
            // Non-zero so a stray store/swap on the wrong field shows up.
            registry.parking_slots.store(1, Ordering::SeqCst);

            let declarer_registry = Arc::clone(&registry);
            let declarer = loom::thread::spawn(move || {
                declarer_registry.declare_keep_warm();
                declarer_registry.release_keep_warm();
            });

            // Idle-worker snapshots taken mid-episode.
            for _ in 0..3 {
                assert_eq!(
                    registry.parking_used(),
                    1,
                    "a declaration must not lease or release parking slots"
                );
                assert_eq!(registry.terminate_count.load(Ordering::Relaxed), 1);
                assert_eq!(registry.spawned.load(Ordering::Relaxed), 0);
                assert!(
                    registry.keep_warm.load(Ordering::Acquire) <= 1,
                    "one level cannot wrap the counter"
                );
            }

            declarer.join().unwrap();
            assert_eq!(
                registry.keep_warm.load(Ordering::Acquire),
                0,
                "a balanced episode lands on zero"
            );
            assert!(!registry.keep_warm_declared());
            assert_eq!(registry.parking_used(), 1);
        });
    }

    /// Registry with `num_threads` pre-created but never spawned workers:
    /// `num_threads()` (the lease cap) is `thread_infos.len()`, while
    /// `spawned == 0` keeps every spawn/terminate path out of the model.
    fn unspawned_registry(num_threads: usize) -> std::sync::Arc<Registry> {
        partially_spawned_registry(num_threads, 0, 1)
    }

    /// Registry whose first `spawned` pre-created workers have a live thread and
    /// whose tail (`spawned..num_threads`) is the ghost slice `terminate` and
    /// `Drop` must skip. No thread is spawned at all: the models play the live
    /// workers with `loom::thread`. `terminate_count` is the logical reference
    /// count, one per live `terminate` caller.
    fn partially_spawned_registry(
        num_threads: usize,
        spawned: usize,
        terminate_count: usize,
    ) -> std::sync::Arc<Registry> {
        assert!(spawned <= num_threads);
        let thread_infos = (0..num_threads)
            .map(|_| {
                let worker = Worker::<JobRef>::new(LOCAL_DEQUE_CAPACITY);
                ThreadInfo::new(worker.stealer())
            })
            .collect();
        std::sync::Arc::new(Registry {
            thread_infos,
            spawned: AtomicUsize::new(spawned),
            sleep: Sleep::new(num_threads),
            injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
            terminate_count: AtomicUsize::new(terminate_count),
            parking_slots: ParkingCount::new(0),
            keep_warm: CachePadded(KeepWarmCount::new(0)),
        })
    }

    /// The pool-scope liveness invariant behind `parking_slots`: `used` never
    /// exceeds the thread count, and every granted slot is returned. Three
    /// leasers on a two-slot pool means one request per round is refused, so
    /// both the CAS and the refusal arm run under contention.
    ///
    /// The bound is also the underflow detector: an unbalanced release wraps
    /// `release_parking`'s `fetch_sub` to `usize::MAX`, which the next sample
    /// sees as `used > CAP`. Verified by making the CAS grant `n` instead of
    /// `used + n` (a leaser whose CAS lost the race is still told it holds a
    /// slot): the model fails on the bound once that unrecorded slot is
    /// released. Returning `Ok` on the CAS-failure branch also fails it, by
    /// admitting past the cap.
    #[test]
    fn concurrent_leases_stay_within_the_thread_count() {
        /// Thread count, i.e. the lease cap.
        const CAP: usize = 2;
        /// Lease/release rounds per thread.
        const ROUNDS: usize = 2;

        fn lease_and_return(registry: &Registry) {
            for _ in 0..ROUNDS {
                if registry.try_reserve_parking(1) {
                    registry.release_parking(1);
                }
                assert!(
                    registry.parking_used() <= CAP,
                    "parking slots exceeded the pool's thread count"
                );
            }
        }

        loom::model(|| {
            let registry = unspawned_registry(CAP);

            let leasers: Vec<_> = (0..2)
                .map(|_| {
                    let leaser_registry = std::sync::Arc::clone(&registry);
                    loom::thread::spawn(move || lease_and_return(&leaser_registry))
                })
                .collect();
            lease_and_return(&registry);
            for leaser in leasers {
                leaser.join().unwrap();
            }

            assert_eq!(
                registry.parking_used(),
                0,
                "every granted slot must be returned"
            );
        });
    }

    /// All-or-nothing admission under contention. One slot stays leased for the
    /// whole model, so two racers that each request `CAP - 1` slots contend for
    /// exactly one grant: a grant must reserve the whole request (a partial grant
    /// leaves the counter short of the cap), and a refusal — including the retry
    /// after a lost CAS — must leave the counter untouched.
    #[test]
    fn rejected_lease_grants_nothing() {
        /// Thread count, i.e. the lease cap.
        const CAP: usize = 3;
        /// Slots each racer requests.
        const REQUEST: usize = CAP - 1;

        loom::model(|| {
            let registry = unspawned_registry(CAP);
            assert!(registry.try_reserve_parking(1), "one slot must fit");

            let racers: Vec<_> = (0..2)
                .map(|_| {
                    let racer_registry = std::sync::Arc::clone(&registry);
                    loom::thread::spawn(move || {
                        if racer_registry.try_reserve_parking(REQUEST) {
                            // Only this thread can release between its own CAS
                            // and this sample, so the read is exact.
                            assert_eq!(
                                racer_registry.parking_used(),
                                CAP,
                                "a granted lease must reserve the whole request"
                            );
                            racer_registry.release_parking(REQUEST);
                        }
                        assert!(
                            racer_registry.parking_used() <= CAP,
                            "parking slots exceeded the pool's thread count"
                        );
                    })
                })
                .collect();
            for racer in racers {
                racer.join().unwrap();
            }

            assert_eq!(
                registry.parking_used(),
                1,
                "only the sentinel slot is left leased"
            );
            registry.release_parking(1);
            // Larger than the pool outright, on an empty counter.
            assert!(!registry.try_reserve_parking(CAP + 1));
            assert_eq!(
                registry.parking_used(),
                0,
                "a rejected lease must not touch the counter"
            );
        });
    }

    /// A one-slot lease is returned exactly once: the wrapped job's drop guard
    /// releases its slot on completion, and `ParkingLease::drop` then finds its
    /// never-spawned remainder empty.
    ///
    /// A bystander leases and returns the one slot the lease leaves free while
    /// the job runs, so the job's release races a live CAS loop. A second
    /// release would wrap the counter to `usize::MAX`, caught by the bound and
    /// the final zero assertion.
    #[test]
    fn single_slot_lease_returns_its_slot() {
        /// Thread count, i.e. the lease cap.
        const CAP: usize = 2;
        /// Lease/release rounds for the bystander.
        const ROUNDS: usize = 2;

        loom::model(|| {
            let registry = unspawned_registry(CAP);
            assert!(registry.try_reserve_parking(1));
            let lease = ParkingLease::new(std::sync::Arc::clone(&registry), 1);
            assert_eq!(registry.parking_used(), 1);

            let job = lease.wrap_job(|| {});
            let job_registry = std::sync::Arc::clone(&registry);
            let worker = loom::thread::spawn(move || {
                job();
                assert!(
                    job_registry.parking_used() <= CAP,
                    "the job's release must not over-release"
                );
            });

            for _ in 0..ROUNDS {
                if registry.try_reserve_parking(1) {
                    registry.release_parking(1);
                }
                assert!(
                    registry.parking_used() <= CAP,
                    "parking slots exceeded the pool's thread count"
                );
            }

            worker.join().unwrap();
            assert_eq!(
                registry.parking_used(),
                0,
                "the wrapped job returns its slot"
            );

            drop(lease);
            assert_eq!(
                registry.parking_used(),
                0,
                "a spawned job's slot is not returned twice"
            );
        });
    }

    /// A multi-slot lease returns every slot: each of the `SPAWNED` wrapped jobs
    /// hands back one on completion and `ParkingLease::drop` hands back the
    /// never-spawned `RESERVED - SPAWNED` remainder — while a bystander leases
    /// and returns the slot the lease left free, so the remainder's release
    /// races a live CAS loop instead of running on a quiet counter.
    #[test]
    fn multi_slot_lease_returns_every_slot() {
        /// Thread count, i.e. the lease cap.
        const CAP: usize = 3;
        /// Slots the lease claims; `SPAWNED < RESERVED` exercises the
        /// never-spawned remainder.
        const RESERVED: usize = 2;
        const SPAWNED: usize = 1;
        /// Lease/release rounds for the bystander.
        const ROUNDS: usize = 2;

        loom::model(|| {
            let registry = unspawned_registry(CAP);
            assert!(registry.try_reserve_parking(RESERVED));
            let lease = ParkingLease::new(std::sync::Arc::clone(&registry), RESERVED);

            let workers: Vec<_> = (0..SPAWNED)
                .map(|_| {
                    let job = lease.wrap_job(|| {});
                    loom::thread::spawn(job)
                })
                .collect();

            let bystander_registry = std::sync::Arc::clone(&registry);
            let bystander = loom::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    if bystander_registry.try_reserve_parking(1) {
                        bystander_registry.release_parking(1);
                    }
                    assert!(
                        bystander_registry.parking_used() <= CAP,
                        "parking slots exceeded the pool's thread count"
                    );
                }
            });

            for worker in workers {
                worker.join().unwrap();
            }
            bystander.join().unwrap();
            assert_eq!(
                registry.parking_used(),
                RESERVED - SPAWNED,
                "joined jobs return their slots, the remainder stays leased"
            );

            drop(lease);
            assert_eq!(
                registry.parking_used(),
                0,
                "every reserved slot must be returned exactly once"
            );
        });
    }

    /// `terminate_count` reaching zero broadcasts exactly once while the other
    /// callers' decrements race it. The gate is the `fetch_sub(..) == 1` return,
    /// so exactly one caller can find the count at 1; all `spawned` latches end
    /// set (the one broadcast covered the whole prefix) and the branch-entry
    /// count grew by exactly one — the double-broadcast check the latches cannot
    /// provide, since a `OnceLatch` sets once and a second broadcast would be
    /// silent.
    ///
    /// Three references, three callers (the model thread plus two `loom::thread`
    /// ones), two fully spawned workers.
    #[test]
    fn concurrent_terminates_broadcast_once() {
        /// Pre-created workers, all spawned, so each broadcast iteration sets a
        /// modeled latch.
        const NUM_THREADS: usize = 2;

        loom::model(|| {
            // One logical reference per caller below.
            let registry = partially_spawned_registry(NUM_THREADS, NUM_THREADS, 3);
            let broadcasts_before = terminate_broadcasts();

            let callers: Vec<_> = (0..2)
                .map(|_| {
                    let caller_registry = std::sync::Arc::clone(&registry);
                    loom::thread::spawn(move || caller_registry.terminate())
                })
                .collect();
            registry.terminate();
            for caller in callers {
                caller.join().unwrap();
            }

            assert_eq!(
                terminate_broadcasts() - broadcasts_before,
                1,
                "reaching zero must enter the broadcast branch exactly once"
            );
            assert_eq!(
                registry.terminate_count.load(Ordering::Acquire),
                0,
                "every reference must be terminated"
            );
            for (index, info) in registry.thread_infos.iter().enumerate() {
                assert!(
                    info.terminate.as_core_latch().probe(),
                    "worker {index} was never signalled"
                );
                // SAFETY: `info` outlives the drop at the closure's end.
                unsafe { Latch::set(ptr::from_ref(&info.stopped)) };
            }
        });
    }

    /// The broadcast covers the `[..spawned]` prefix only. Shapes are a mid-way
    /// spawn failure — `spawned` at 0, 1 and `NUM_THREADS - 1` against
    /// `NUM_THREADS` pre-created workers — and the ghost tail's `terminate`
    /// latch must stay unset while every spawned one ends set. The branch is
    /// still entered once at `spawned == 0`: the count reaching zero is the
    /// trigger, the signal set is empty.
    ///
    /// One caller, one schedule: the shapes are what this model enumerates, and
    /// they carry the claim.
    #[test]
    fn terminate_signals_only_the_spawned_prefix() {
        const NUM_THREADS: usize = 4;

        for spawned in [0, 1, NUM_THREADS - 1] {
            loom::model(move || {
                let registry = partially_spawned_registry(NUM_THREADS, spawned, 1);
                let broadcasts_before = terminate_broadcasts();

                registry.terminate();

                assert_eq!(
                    terminate_broadcasts() - broadcasts_before,
                    1,
                    "the count reaching zero broadcasts once"
                );
                assert_eq!(
                    registry.terminate_count.load(Ordering::Acquire),
                    0,
                    "every reference must be terminated"
                );
                for (index, info) in registry.thread_infos.iter().enumerate() {
                    assert_eq!(
                        info.terminate.as_core_latch().probe(),
                        index < spawned,
                        "worker {index} of a {spawned}/{NUM_THREADS} spawn"
                    );
                }

                // Only the prefix has a worker that will ever mark it stopped.
                for info in &registry.thread_infos[..spawned] {
                    // SAFETY: `info` outlives the drop at the closure's end.
                    unsafe { Latch::set(ptr::from_ref(&info.stopped)) };
                }
            });
        }
    }

    /// `Registry::drop` waits only on the `[..spawned]` `stopped` latches. Both
    /// halves are asserted by the model finishing: with a ghost tail whose
    /// latches are never set, waiting on the full vector parks forever and loom
    /// reports the deadlock instead. The fully-spawned shape is the control,
    /// where the same wait covers every latch.
    ///
    /// A worker exits (and so releases its `Arc`) only after it was signalled,
    /// so the latches are pre-set here: the subject is the wait set, not the
    /// order between the exit and the drop. One schedule, and loom's deadlock
    /// detection is the assertion.
    #[test]
    fn drop_waits_only_for_spawned_stopped_latches() {
        const NUM_THREADS: usize = 4;

        for spawned in [0, NUM_THREADS - 1, NUM_THREADS] {
            loom::model(move || {
                let registry = partially_spawned_registry(NUM_THREADS, spawned, 1);
                for info in &registry.thread_infos[..spawned] {
                    // SAFETY: `info` outlives the drop below.
                    unsafe { Latch::set(ptr::from_ref(&info.stopped)) };
                }
                let broadcasts_before = terminate_broadcasts();

                drop(registry);

                assert_eq!(
                    terminate_broadcasts() - broadcasts_before,
                    1,
                    "Drop's terminate backstop must broadcast exactly once"
                );
            });
        }
    }

    /// The `terminate_count > 0` backstop: `Drop` decrements the second of the
    /// two outstanding references and a concurrent `terminate()` the first, so
    /// only the backstop can find the count at 1. Whichever side releases the
    /// `Arc` refcount last runs `Drop`, the branch is entered once either way,
    /// and the two decrements never race each other: the backstop is reachable
    /// only while a reference is still alive, so it runs downstream of the loom
    /// thread's `terminate` in program order.
    #[test]
    fn drop_backstop_does_not_double_broadcast() {
        const NUM_THREADS: usize = 2;

        loom::model(|| {
            // One reference terminated by the loom thread, one by the backstop.
            let registry = partially_spawned_registry(NUM_THREADS, NUM_THREADS, 2);
            for info in &registry.thread_infos {
                // SAFETY: `info` outlives the drop below.
                unsafe { Latch::set(ptr::from_ref(&info.stopped)) };
            }
            let broadcasts_before = terminate_broadcasts();

            let owner_registry = std::sync::Arc::clone(&registry);
            let owner = loom::thread::spawn(move || {
                owner_registry.terminate();
                // Only the backstop's decrement can follow this one.
                assert_eq!(
                    owner_registry.terminate_count.load(Ordering::Acquire),
                    1,
                    "terminate must stop at the backstop's reference count"
                );
                // The clone drops here; the last reference runs `Drop`, whose
                // backstop decrements and broadcasts.
            });

            drop(registry);
            owner.join().unwrap();
            assert_eq!(
                terminate_broadcasts() - broadcasts_before,
                1,
                "terminate and the Drop backstop must broadcast exactly once"
            );
        });
    }
}

#[cfg(all(test, loom, st3_loom))]
mod st3_loom_tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::pool::job::Job;

    const JOBS: usize = 4;
    /// Two threads, at most two preemptions: the interleaving space is bounded
    /// in-code rather than via `LOOM_MAX_PREEMPTIONS`, so the model cannot run
    /// away under an unbounded environment (it does not terminate without a
    /// bound).
    const MAX_THREADS: usize = 2;
    const PREEMPTION_BOUND: usize = 2;
    /// Vacuity guard: 1.1 s of exploration on a 32-core host against the
    /// ~0.3 ms of a single real-atomic schedule. This is what catches the
    /// regression the cfg gate cannot: the vendored st3 must keep loom as a
    /// regular target dependency (upstream's dev-dependency form links a
    /// real-atomic rlib while `--cfg st3_loom` is set on the test crate).
    const EXPLORATION_FLOOR: Duration = Duration::from_millis(200);

    /// Local-deque capacity of the batch models' owner. Small on purpose: a
    /// `BATCH_JOBS`-sized batch then leaves a two-job overflow tail, while a
    /// 256-slot deque would need a 258-job batch for the same split.
    const BATCH_CAPACITY: usize = 4;
    /// Batch size of the batch models: the first `BATCH_CAPACITY` jobs fit the
    /// owner's deque, the rest is the overflow tail.
    const BATCH_JOBS: usize = 6;
    /// Steal attempts the thief makes in the race model. Bounded so an empty
    /// deque cannot spin: every attempt is a preemption point.
    const STEAL_ATTEMPTS: usize = 3;

    /// Minimal job: records its executions in its own counter instead of
    /// owning data, so a duplicate consumption is observable rather than a
    /// double free.
    struct CountJob<'a> {
        ran: &'a AtomicUsize,
    }

    impl Job for CountJob<'_> {
        unsafe fn execute(this: *const ()) {
            // SAFETY: the `JobRef` is built from a `CountJob` owned by the
            // model closure, which outlives every execution (the thief is
            // joined before the closure returns).
            let job = unsafe { &*this.cast::<CountJob<'_>>() };
            job.ran.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Every job must have run exactly once: a job left at 0 was lost, a job at
    /// 2 landed in two places (or was popped twice).
    fn assert_each_executed_once(counters: &[AtomicUsize]) {
        for (id, ran) in counters.iter().enumerate() {
            assert_eq!(
                ran.load(Ordering::Relaxed),
                1,
                "job {id} was not executed exactly once"
            );
        }
    }

    /// Vacuity guard for the exploring models: a build where st3 keeps real
    /// atomics collapses every schedule into one and finishes in well under the
    /// floor (measured ~0.3 ms against 1.1 s for the deque model).
    fn assert_explored(start: Instant) {
        assert!(
            start.elapsed() >= EXPLORATION_FLOOR,
            "the st3 model did not explore interleavings (elapsed {:?}): st3 is not loom-backed \
             in this build",
            start.elapsed()
        );
    }

    /// Registry the batch models push into: built by literal with no workers, so
    /// no spawn or terminate path is part of the schedule.
    fn model_registry() -> Arc<Registry> {
        Arc::new(Registry {
            thread_infos: Vec::new(),
            spawned: AtomicUsize::new(0),
            sleep: Sleep::new(1),
            injected_jobs: concurrent_queue::ConcurrentQueue::unbounded(),
            terminate_count: AtomicUsize::new(1),
            parking_slots: ParkingCount::new(0),
            keep_warm: CachePadded(KeepWarmCount::new(0)),
        })
    }

    /// The batch's owner. `push_batch` never consults TLS
    /// (`Registry::push_local_batch` does that dispatch for its callers), so the
    /// model owns the `WorkerThread` directly instead of running a worker.
    fn model_owner(registry: &Arc<Registry>) -> WorkerThread {
        WorkerThread {
            worker: Worker::<JobRef>::new(BATCH_CAPACITY),
            index: 0,
            rng: XorShift64Star::new(),
            idle_spin: Cell::new(SpinBudget::new(0)),
            registry: Arc::clone(registry),
        }
    }

    /// Builds `BATCH_JOBS` jobs that count their executions and hands the
    /// counters (index = batch position) plus one `JobRef` per job, in batch
    /// order, to `f`.
    fn with_batch_jobs(f: impl FnOnce(&[AtomicUsize], Vec<JobRef>)) {
        let counters: Vec<AtomicUsize> = (0..BATCH_JOBS).map(|_| AtomicUsize::new(0)).collect();
        let jobs: Vec<CountJob<'_>> = counters.iter().map(|ran| CountJob { ran }).collect();
        let refs: Vec<JobRef> = jobs.iter().map(|job| unsafe { JobRef::new(job) }).collect();
        f(&counters, refs);
    }

    /// A `Worker<JobRef>` owner and one `Stealer` race over the same deque
    /// while it is pushed to, popped from its LIFO end and stolen from its
    /// FIFO end: every pushed job is consumed exactly once — no loss, no
    /// duplicate — whichever side drains it.
    ///
    /// The st3 deque is the subject, and `--cfg st3_loom` is what switches
    /// st3's atomics and registers its per-slot buffer cells (see
    /// `crates/youpipe-st3/src/loom_exports.rs`). Without it st3 keeps real
    /// atomics, loom sees a single schedule and the model passes vacuously
    /// (measured 1.1 s with the cfg against 0.3 ms without), so the module is
    /// gated on it instead of leaving a no-op in `--cfg loom`-only builds.
    #[test]
    fn st3_push_pop_steal_consume_every_job_once() {
        let start = Instant::now();
        let mut model = loom::model::Builder::new();
        model.max_threads = MAX_THREADS;
        model.preemption_bound = Some(PREEMPTION_BOUND);
        model.check(|| {
            let counters: Vec<AtomicUsize> = (0..JOBS).map(|_| AtomicUsize::new(0)).collect();
            let jobs: Vec<CountJob<'_>> = counters.iter().map(|ran| CountJob { ran }).collect();
            let mut refs: Vec<JobRef> =
                jobs.iter().map(|job| unsafe { JobRef::new(job) }).collect();

            let worker = Worker::<JobRef>::new(4);
            let stealer = worker.stealer();

            // Seed one job so the thief's first steal can actually transfer.
            assert!(worker.push(refs.pop().unwrap()).is_ok());

            let thief = loom::thread::spawn(move || {
                let dest = Worker::<JobRef>::new(4);
                let mut consumed = 0;
                for _ in 0..JOBS {
                    if let Ok((job, _)) = stealer.steal_and_pop(&dest, |_| 1) {
                        // SAFETY: each `JobRef` is popped once.
                        unsafe { job.execute() };
                        consumed += 1;
                    }
                }
                consumed
            });

            let mut consumed = 0;
            for job in refs {
                assert!(
                    worker.push(job).is_ok(),
                    "capacity 4 must accept {JOBS} jobs"
                );
                if let Some(job) = worker.pop() {
                    // SAFETY: each `JobRef` is popped once.
                    unsafe { job.execute() };
                    consumed += 1;
                }
            }
            while let Some(job) = worker.pop() {
                // SAFETY: each `JobRef` is popped once.
                unsafe { job.execute() };
                consumed += 1;
            }

            let stolen = thief.join().unwrap();
            assert_eq!(consumed + stolen, JOBS, "a job was lost");
            assert_each_executed_once(&counters);
        });
        assert_explored(start);
    }

    /// The overflow split of [`WorkerThread::push_batch`]: a batch larger than
    /// the local deque leaves the first `BATCH_CAPACITY` jobs there and sends
    /// the whole `BATCH_JOBS - BATCH_CAPACITY` tail to the injector in ONE
    /// `inject_batch`, in batch order. Draining both sides is also the
    /// no-hole/no-duplicate assertion: the two id sequences partition the batch.
    ///
    /// Only the owner runs here — the split reads owner-private deque state and
    /// concurrent stealing would move it (that is
    /// `push_batch_steal_race_consumes_every_job_once`) — so this model has one
    /// schedule (~0 ms). It still runs inside a model because both sides are
    /// modeled primitives in this build: the assertions are checked against the
    /// same st3 deque and injector the race model interleaves.
    ///
    /// Jobs are matched by `JobRef::id` (the data pointer), not by execution
    /// order: the deque drains LIFO, so its ids come out as the reversed batch
    /// prefix, while the injector drains FIFO in batch order. Both drains run to
    /// empty, which is the no-hole part of the claim.
    ///
    /// The single-batch claim rests on the spill probe alone: injecting the tail
    /// one job per `inject` leaves both id sequences intact and fails only the
    /// probe (falsified, count 0), and dropping the overflow instead of spilling
    /// it empties the injector sequence (falsified).
    #[test]
    fn push_batch_overflow_tail_takes_the_injector() {
        loom::model(|| {
            let registry = model_registry();
            let owner = model_owner(&registry);
            with_batch_jobs(|counters, refs| {
                let batch_ids: Vec<usize> = refs.iter().map(|job| job.id().0).collect();
                let spills_before = spill_tail_batches();
                // SAFETY: this thread is the owner, so the local deque has a
                // single producer.
                unsafe { owner.push_batch(refs.into_iter()) };

                let mut local_ids = Vec::new();
                while let Some(job) = owner.try_pop_local() {
                    local_ids.push(job.id().0);
                    // SAFETY: each `JobRef` is popped exactly once.
                    unsafe { job.execute() };
                }
                let mut tail_ids = Vec::new();
                while let Some(job) = registry.pop_injected_job() {
                    tail_ids.push(job.id().0);
                    // SAFETY: each `JobRef` is popped exactly once.
                    unsafe { job.execute() };
                }

                let mut expected_local = batch_ids[..BATCH_CAPACITY].to_vec();
                expected_local.reverse();
                assert_eq!(
                    local_ids, expected_local,
                    "the local deque must hold the batch prefix and nothing else"
                );
                assert_eq!(
                    tail_ids,
                    batch_ids[BATCH_CAPACITY..],
                    "the overflow tail must reach the injector whole and in batch order"
                );
                assert_eq!(
                    spill_tail_batches() - spills_before,
                    1,
                    "the overflow tail must reach the injector as ONE batch"
                );
                assert_each_executed_once(counters);
            });
        });
    }

    /// The same oversized batch pushed while a thief steals the deque's FIFO
    /// end: stealing drains the deque mid-loop, so only the jobs that actually
    /// overflowed reach the injector and a steal that lands between two pushes
    /// must leave no hole. Every job executes exactly once whichever side drains
    /// it, and both queues are empty at quiescence — a phantom claim (a
    /// booked-but-unpublished slot) would either strand a job at 0 or leave it
    /// behind in a queue.
    ///
    /// Two threads, `STEAL_ATTEMPTS` steals: 4.3 s of exploration on a 32-core
    /// host. Shortening the attempt loop is what buys the budget back (3 -> 2
    /// attempts measured 4.3 s -> 1.0 s); the module's `st3_loom` gate is what
    /// keeps a real-atomic build out, and `assert_explored` catches it if the
    /// gate is ever defeated.
    #[test]
    fn push_batch_steal_race_consumes_every_job_once() {
        let start = Instant::now();
        let mut model = loom::model::Builder::new();
        model.max_threads = MAX_THREADS;
        model.preemption_bound = Some(PREEMPTION_BOUND);
        model.check(|| {
            let registry = model_registry();
            let owner = model_owner(&registry);
            let stealer = owner.worker.stealer();
            with_batch_jobs(|counters, refs| {
                let thief = loom::thread::spawn(move || {
                    let dest = Worker::<JobRef>::new(BATCH_CAPACITY);
                    for _ in 0..STEAL_ATTEMPTS {
                        if let Ok((job, _)) = stealer.steal_and_pop(&dest, |_| 1) {
                            // SAFETY: each `JobRef` is popped exactly once.
                            unsafe { job.execute() };
                        }
                    }
                });

                // SAFETY: this thread is the owner, so the local deque has a
                // single producer; the thief only books its FIFO end.
                unsafe { owner.push_batch(refs.into_iter()) };
                while let Some(job) = owner.try_pop_local() {
                    // SAFETY: each `JobRef` is popped exactly once.
                    unsafe { job.execute() };
                }
                while let Some(job) = registry.pop_injected_job() {
                    // SAFETY: each `JobRef` is popped exactly once.
                    unsafe { job.execute() };
                }

                thief.join().unwrap();
                assert_each_executed_once(counters);
                assert!(
                    owner.try_pop_local().is_none(),
                    "a job was left in the local deque"
                );
                assert!(
                    registry.pop_injected_job().is_none(),
                    "a job was left in the injector"
                );
            });
        });
        assert_explored(start);
    }
}
