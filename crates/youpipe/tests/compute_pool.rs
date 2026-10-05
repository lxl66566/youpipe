use std::sync::Arc;

#[test]
fn test_compute_pool_basic() {
    let pool = youpipe::ComputePool::new(4);
    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..10 {
        let tx = tx.clone();
        pool.submit(move || {
            tx.send(i).unwrap();
        });
    }
    drop(tx);
    let results: Vec<_> = rx.iter().collect();
    assert_eq!(results.len(), 10);
}

#[test]
fn test_compute_pool_shared() {
    let pool = Arc::new(youpipe::ComputePool::new(4));
    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..100 {
        let tx = tx.clone();
        let p = pool.clone();
        p.submit(move || {
            let mut sum = 0u64;
            // Miri: the inner arithmetic depth is irrelevant to the pool
            // machinery under test — shrink it 50x.
            let inner: u32 = if cfg!(miri) {
                20
            } else {
                1_000
            };
            for j in 0..inner {
                sum = sum.wrapping_add(u64::from(j));
            }
            tx.send((i, sum)).unwrap();
        });
    }
    drop(tx);
    let results: Vec<_> = rx.iter().collect();
    assert_eq!(results.len(), 100);
}

#[test]
fn test_compute_pool_many_small_tasks() {
    let pool = Arc::new(youpipe::ComputePool::new(4));
    // Miri: 10k submit/wait cycles over the injector + steal path take tens
    // of interpreted minutes; 500 exercises every queue/steal/latch path
    // (the pool has 4 emulated workers) in seconds.
    let total = if cfg!(miri) {
        500
    } else {
        10_000
    };
    // Completion signal via a plain mpsc channel: one send per task, collect
    // `total` of them. (Replaces the removed SharedWaitGroup.)
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    for _ in 0..total {
        let done_tx = done_tx.clone();
        pool.submit(move || {
            done_tx.send(()).unwrap();
        });
    }
    drop(done_tx);
    assert_eq!(done_rx.iter().count(), total);
}

/// Driver work-assist on the hybrid dispatch path (reserve chunks): with every
/// pool worker busy on a blocking submitted job, the driver still runs its own
/// inline chunk 0 plus the reserve chunk(s), and the injected chunks complete
/// once the workers come back — the batch finishes correctly with each chunk
/// executed exactly once. The blockers auto-release after a delay so the
/// parked wait terminates without test-side coordination.
#[test]
fn test_hybrid_driver_assists_when_workers_busy() {
    use std::sync::mpsc;

    let pool = youpipe::ComputePool::new(4);
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    for _ in 0..4 {
        let rx = Arc::clone(&release_rx);
        let tx = started_tx.clone();
        pool.submit(move || {
            tx.send(()).unwrap();
            let _ = rx.lock().unwrap().recv();
        });
    }
    drop(started_tx);
    // Wait until all 4 workers are provably inside their blockers — from here
    // on no worker can pick up hybrid chunks until released.
    for _ in 0..4 {
        started_rx.recv().unwrap();
    }
    // Auto-release on a helper thread: the driver covers chunk 0 + reserve,
    // then parks on the latch until the workers drain the injected chunks.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(if cfg!(miri) {
            200
        } else {
            300
        }));
        for _ in 0..4 {
            release_tx.send(()).unwrap();
        }
    });

    let n: u32 = if cfg!(miri) {
        256
    } else {
        4096
    };
    let input: Vec<u64> = (0..u64::from(n)).collect();
    let expected: Vec<u64> = input.iter().map(|x| x * 2 + 1).collect();
    let got: Vec<u64> = youpipe::pipe(input)
        .with_compute_pool(pool.clone())
        .map(|x| x * 2 + 1)
        .collect();
    assert_eq!(got, expected);

    // The pool must still accept and run new work after the drain.
    let (tx, rx) = mpsc::channel();
    pool.submit(move || {
        tx.send(42).unwrap();
    });
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap(),
        42
    );
}

/// Panic semantics of a job `submit`ted from inside `join`'s A closure.
///
/// The on-pool submit fast path parks the HeapJob on the calling worker's
/// local deque — LIFO, on top of `job_b`. The join wait loop then pops and
/// executes it bare. Pre-fix, its panic unwound live through the join frame,
/// leaving `job_b`'s stack-allocated StackJob dangling in the deque → SIGSEGV
/// on the next pop/steal. The fix gives `HeapJob::execute` an `AbortIfPanic`,
/// so the panic aborts — the same outcome a submit-job panic already had in
/// the main loop / `wait_until_cold`, and rayon `spawn`'s semantics.
///
/// Asserted in a subprocess (an abort kills the process by design). Pre-fix
/// the child segfaults (SIGSEGV) or survives with corrupted stacks — either
/// way the parent's signal assert fails.
#[test]
#[cfg(all(unix, not(miri)))]
fn submit_panic_inside_join_aborts_not_unwinds() {
    const CHILD_MODE: &str = "YOUIPE_TEST_JOIN_SUBMIT_PANIC_CHILD";
    if std::env::var_os(CHILD_MODE).is_some() {
        submit_panic_inside_join_child();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("submit_panic_inside_join_aborts_not_unwinds")
        .env(CHILD_MODE, "1")
        .status()
        .expect("spawn child test binary");
    // SIGABRT = 6 on every Unix this crate targets (no libc dev-dep; the
    // constant is stable in POSIX).
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(6),
        "submit-job panic in join's wait loop must abort, got {status:?}"
    );
}

/// Child half of [`submit_panic_inside_join_aborts_not_unwinds`]: the review's
/// SIGSEGV repro. The iterations keep the pre-fix failure observable (the
/// dangling `job_b` ref is only executed on a LATER pop/steal); post-fix the
/// first iteration aborts inside `HeapJob::execute` and never returns here.
#[cfg(all(unix, not(miri)))]
fn submit_panic_inside_join_child() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    for iter in 0..5 {
        // new_pinned: bypasses the recycling cache, so each drop really
        // terminates (and drains) the pool.
        let pool = youpipe::ComputePool::new_pinned(2);
        let (p2, p3) = (pool.clone(), pool.clone());
        let h = std::thread::spawn(move || {
            let r = catch_unwind(AssertUnwindSafe(move || {
                p2.join(
                    move || {
                        // On-pool submit: HeapJob lands on this worker's
                        // local deque, LIFO on top of job_b.
                        p3.submit(move || {
                            std::thread::sleep(std::time::Duration::from_micros(200));
                            panic!("boom {iter}");
                        });
                    },
                    move || {
                        std::thread::sleep(std::time::Duration::from_micros(500));
                    },
                );
            }));
            r.is_err()
        });
        assert!(h.join().unwrap(), "submitted job's panic must surface");
        std::thread::sleep(std::time::Duration::from_millis(5));
        drop(pool);
    }
}
