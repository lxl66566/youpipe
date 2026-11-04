//! Pool parking-lease admission shared by the stream (`resolve_admission`)
//! and graph (`resolve_budget`) engines: the [`ParkingLease`] primitive and
//! the one-shot [`admit`] that sizes a run against the pool's live lease
//! load, CASes the whole grant, and picks the dispatch mode.

use std::sync::Arc;

use super::config::PipelineConfig;
use crate::{executor::compute::ComputePool, pool::Registry};

/// Pool-capacity lease covering one run's channel-parking jobs (feeders,
/// sync/expand stage workers, fence forwarders, sink workers).
///
/// A job that parks inside a crossfire send/recv holds its worker until the
/// run's channels drain, so per-run budgeting alone ("my jobs ≤ my pool") is
/// only sound while a single run uses the pool: two concurrent full-budget
/// runs oversubscribe it, every worker parks on a full/empty channel, and
/// the still-queued jobs — stage workers of either run, fused chunks behind
/// them in the injector FIFO — can never be popped (reproduced with 4
/// concurrent 2-stage runs on the global pool; every worker parked on
/// `send`, all stage-2 jobs queued). The lease closes that hole at *pool*
/// scope: a run either atomically reserves capacity for its whole upper
/// bound of parking jobs (≤ remaining worker count) or falls back to
/// dedicated threads, restoring the invariant "some worker is free of
/// parking jobs whenever a parking job is still queued".
///
/// Release is per job (each wrapped job returns its slot on completion, via
/// drop so unwinds are covered) plus the never-spawned remainder here.
pub(crate) struct ParkingLease {
    registry: Arc<Registry>,
    /// Reserved upper bound: `fixed jobs + every divided worker grant`
    /// (see [`admit`]).
    reserved: usize,
    /// Jobs wrapped so far; never exceeds `reserved`.
    spawned: std::cell::Cell<usize>,
}

impl ParkingLease {
    /// Lease `reserved` parking slots on `registry`.
    pub(crate) fn new(registry: Arc<Registry>, reserved: usize) -> Self {
        Self {
            registry,
            reserved,
            spawned: std::cell::Cell::new(0),
        }
    }

    /// Wrap `job` so its completion — return or unwind — returns one leased
    /// slot. The release must not precede the job's last channel access,
    /// hence drop-guard (a plain tail call would be skipped on unwind).
    pub(crate) fn wrap_job<F>(&self, job: F) -> impl FnOnce() + Send + 'static
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawned.set(self.spawned.get() + 1);
        let registry = Arc::clone(&self.registry);
        move || {
            struct ReleaseSlot(Arc<Registry>);
            impl Drop for ReleaseSlot {
                fn drop(&mut self) {
                    self.0.release_parking(1);
                }
            }
            let _release = ReleaseSlot(registry);
            job();
        }
    }
}

impl Drop for ParkingLease {
    fn drop(&mut self) {
        // Return only the never-spawned remainder: spawned jobs return their
        // own slot, possibly still in flight while this drop runs (the
        // collector's return only orders the channel close, not each
        // worker's exit from the job).
        self.registry
            .release_parking(self.reserved - self.spawned.get());
    }
}

/// One run's admission decision, produced by [`admit`].
pub(crate) struct LeaseAdmission {
    /// Pool-capacity lease over the run's whole upper bound of
    /// channel-parking jobs; `None` in dedicated mode.
    pub(crate) lease: Option<ParkingLease>,
    /// Dedicated-OS-thread dispatch for every job of the run (nested runs
    /// on the same pool, or the lease did not fit).
    pub(crate) dedicated: bool,
    /// The division's per-unit share at the accepted budget (lease arm) or
    /// the unclamped budget (dedicated arm).
    pub(crate) share: usize,
    /// Worker portion of the accepted reservation (lease arm; 0 otherwise).
    /// Derived from the reservation, not the pre-CAS snapshot.
    pub(crate) worker_need: usize,
}

/// Shared parking-lease admission for the stream and graph engines.
///
/// Liveness invariant (pool mode): every blocking job the run parks on a
/// pool thread must be simultaneously schedulable — `fixed_jobs + worker
/// grants ≤ pool threads`. A worker parked inside a crossfire send/recv
/// never returns to the pool's find_work loop, so a job left queued while
/// every thread is parked deadlocks the run (stream pre-fix repro: 4 stages
/// on a 4-thread pool with n > k × buffer hung forever — the feeder job was
/// uncounted and the per-stage floor of 1 pushed the total past the pool).
/// The budget holds at *pool* scope via [`ParkingLease`]: atomically reserve
/// the whole upper bound or fall back to dedicated threads.
///
/// `fixed_jobs` counts the run's non-divided parking jobs (stream: the
/// feeder slot and fence forwarders; graph: source feeders and sinks — each
/// is one channel-parking job for the whole run), taken off the pool before
/// the division. `resolve` maps a live worker budget to `(share,
/// worker_need)` — the per-unit division and the upper bound of divided
/// worker grants at that budget (pins are a fixed term, unpinned units
/// multiply the share).
///
/// Snapshot discipline: the share is sized against a pre-CAS snapshot of
/// the pool's lease load, but the grant is CASed as one piece —
/// `try_reserve_parking` revalidates against the live counter, so a stale
/// snapshot can only under-promise (share too small), never over-admit.
/// Grants derived from the reservation's own arithmetic (the returned
/// `worker_need`) are therefore always covered; grants derived from the
/// stale snapshot are not — the stream engine's per-stage budget init once
/// underflowed on exactly that (2026-10-20 miri flake: `live_slots` read 0
/// while the CAS admitted a lease, `stage_workers`' `>= 1` grant floor then
/// wrapped `0 - 1`; fixed by initializing from `worker_need`). A nested run
/// (`run()` on a worker of the same pool) parks that worker for the whole
/// run, so pool admission can never be guaranteed — every job takes the
/// dedicated-thread fallback and the share resolves at the unclamped budget.
pub(crate) fn admit(
    pool: &ComputePool,
    config: &PipelineConfig,
    fixed_jobs: usize,
    resolve: impl Fn(usize) -> (usize, usize),
) -> LeaseAdmission {
    let pool_threads = pool.num_workers();
    // Budget resolution: an explicitly pinned `with_compute_workers` wins
    // (clamped to the pool by the caller's pool choice); unpinned follows
    // the pool's thread count so a big blocking-IO pool actually gets its
    // workers.
    let workers_budget = if config.compute_workers_pinned {
        config.compute_workers
    } else {
        config.compute_workers.max(pool_threads)
    };
    if pool.is_on_this_pool() {
        return LeaseAdmission {
            lease: None,
            dedicated: true,
            share: resolve(workers_budget).0,
            worker_need: 0,
        };
    }
    let used = pool.registry().parking_used();
    let live = pool_threads.saturating_sub(used + fixed_jobs);
    let (share, worker_need) = resolve(workers_budget.min(live));
    let need = fixed_jobs + worker_need;
    if need <= pool_threads && pool.registry().try_reserve_parking(need) {
        LeaseAdmission {
            lease: Some(ParkingLease::new(Arc::clone(pool.registry()), need)),
            dedicated: false,
            share,
            worker_need,
        }
    } else {
        LeaseAdmission {
            lease: None,
            dedicated: true,
            share: resolve(workers_budget).0,
            worker_need: 0,
        }
    }
}
