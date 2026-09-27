//! Semantics of the process-wide pool recycling cache behind
//! `ComputePool::new` (see its doc): reuse across calls, key isolation,
//! capacity eviction, `new_pinned` bypass, and `clear_cached_pools` really
//! joining worker threads.
//!
//! All tests serialize on one mutex: they observe the *process-global* cache
//! (clear counts, thread counts), which parallel sibling tests would pollute.

use std::sync::{Mutex, MutexGuard, OnceLock};

use youpipe::ComputePool;

/// Sizes used here are deliberately unique to this file — other test binaries
/// in the crate hammer sizes 2/4/8, and sharing a size with them would make
/// cache-slot bookkeeping order-dependent.
const SIZE_A: usize = 5;
const SIZE_B: usize = 6;

fn serialize() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Settle the thread population before handing out the guard: pool
    // teardown is asynchronous (dropping the last handle only signals the
    // workers), so the previous test's joins may still be in flight and
    // would poison every thread-count snapshot below.
    #[cfg(all(target_os = "linux", not(miri)))]
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut prev = thread_count();
        while std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
            let now = thread_count();
            if now == prev {
                break;
            }
            prev = now;
        }
    }
    guard
}

/// Worker threads visible to this process (`/proc/self/task`), Linux only.
/// Every caller is non-miri (`/proc` introspection), so the helper is too.
#[cfg(all(target_os = "linux", not(miri)))]
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").map_or(0, Iterator::count)
}

/// Thread teardown is asynchronous by design: dropping the last handle only
/// *signals* workers (each holds its own registry `Arc`); they exit — and the
/// last one runs the registry's Drop — shortly after. Poll briefly instead of
/// asserting an exact instant.
#[cfg(all(target_os = "linux", not(miri)))]
fn wait_for_thread_count(upper_bound: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let n = thread_count();
        if n <= upper_bound || std::time::Instant::now() > deadline {
            return n;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn new_recycles_by_worker_count() {
    let _guard = serialize();
    ComputePool::clear_cached_pools();

    let a = ComputePool::new(SIZE_A);
    drop(a); // parks in the cache instead of joining
    assert_eq!(
        ComputePool::clear_cached_pools(),
        1,
        "dropped pool must be parked, not joined"
    );
    // Different size: a second slot, no cross-talk.
    drop(ComputePool::new(SIZE_B));
    drop(ComputePool::new(SIZE_A));
    assert_eq!(ComputePool::clear_cached_pools(), 2);
}

/// Two sizes that clamp to the same worker count share one cached pool (the
/// cache keys on the clamped size). Only way to observe it through the public
/// API is the 511 clamp, so this spawns one max-size pool — too heavy for
/// miri's interpreted threads.
#[cfg(not(miri))]
#[test]
fn sizes_clamping_equal_share_one_slot() {
    let _guard = serialize();
    ComputePool::clear_cached_pools();

    let cap = youpipe::MAX_COMPUTE_WORKERS;
    drop(ComputePool::new(cap + 100));
    drop(ComputePool::new(10 * cap));
    assert_eq!(ComputePool::clear_cached_pools(), 1);
}

#[test]
fn cache_capacity_is_bounded() {
    let _guard = serialize();
    ComputePool::clear_cached_pools();

    // One more distinct size than the capacity: the oldest falls out (and
    // joins for real). The sizes only need to be distinct; miri shrinks
    // them because every miss spawns emulated threads (5 native pools of
    // ~50 workers stalled the miri run for minutes).
    // Mirrors CACHE_CAPACITY (executor/compute/pool_cache.rs).
    let base = if cfg!(miri) {
        10
    } else {
        50
    };
    for k in 0..5 {
        drop(ComputePool::new(base + k));
    }
    assert_eq!(
        ComputePool::clear_cached_pools(),
        4,
        "fifth distinct size must have evicted the first"
    );
}

#[test]
fn pinned_pools_are_not_cached() {
    let _guard = serialize();
    ComputePool::clear_cached_pools();

    drop(ComputePool::new_pinned(2));
    assert_eq!(
        ComputePool::clear_cached_pools(),
        0,
        "new_pinned must join on drop, never park"
    );
}

/// Cached pools must not leak worker threads: a TID observed running jobs on
/// a cached pool must vanish once `clear_cached_pools` joins it.
#[cfg(all(target_os = "linux", not(miri)))]
#[test]
fn clear_cached_pools_joins_worker_threads() {
    let _guard = serialize();
    ComputePool::clear_cached_pools();

    let before = thread_count();
    let pool = ComputePool::new(3);
    let (tx, rx) = std::sync::mpsc::channel();
    pool.submit(move || {
        // /proc/thread-self → /proc/<pid>/task/<tid>: the worker's own TID.
        let tid = std::fs::read_link("/proc/thread-self")
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        tx.send(tid).unwrap();
    });
    let tid = rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("job must run");
    drop(pool); // parks: 3 worker threads stay alive
    assert_eq!(
        thread_count(),
        before + 3,
        "parked pool must hold exactly its worker threads alive"
    );
    assert!(std::path::Path::new(&format!("/proc/self/task/{tid}")).exists());

    ComputePool::clear_cached_pools();
    assert_eq!(
        wait_for_thread_count(before),
        before,
        "clear must join the parked workers"
    );
    assert!(
        !std::path::Path::new(&format!("/proc/self/task/{tid}")).exists(),
        "worker TID must be gone after clear"
    );
}

/// The end-user win: a tight loop of fused terminals with a non-default
/// worker budget reuses one pool instead of spawning threads per run.
#[cfg(all(target_os = "linux", not(miri)))]
#[test]
fn tight_loop_with_compute_workers_keeps_thread_count_stable() {
    use youpipe::pipe;

    let _guard = serialize();
    ComputePool::clear_cached_pools();

    let before = thread_count();
    for _ in 0..8 {
        let r: Vec<u64> = pipe(0..1000u64)
            .map(|x| x.wrapping_mul(3))
            .with_compute_workers(SIZE_A)
            .collect();
        assert_eq!(r.len(), 1000);
    }
    assert_eq!(
        thread_count(),
        before + SIZE_A,
        "8 runs must share one cached pool, not spawn 8"
    );
    ComputePool::clear_cached_pools();
    assert_eq!(wait_for_thread_count(before), before);
}
