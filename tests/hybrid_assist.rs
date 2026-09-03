// Hybrid work-assist liveness regressions (2026-09). Two historical failure
// modes are pinned here:
//
// 1. Stream starvation via injector reordering: an earlier assist variant popped foreign jobs from
//    the pool-global injector and re-queued them at the tail, breaking the FIFO that stream
//    pipelines rely on (feeder job submitted BEFORE its resident stage-worker jobs). With every
//    worker parked on an empty channel recv behind a displaced feeder, the whole pool deadlocked.
//    The current design never touches the injector — the driver executes withheld reserve chunks
//    only — so streams must stay live under concurrent assist drivers.
// 2. Panic/drop accounting: assist-executed chunks share the exact worker `execute` path (panic
//    capture → failure slot → latch decrement), so mid-batch panics must still drop every input
//    item exactly once.
// Native-only: 24 threads × thousands of items is a timing/latency stress —
// miri's single-threaded interpreter cannot make progress in reasonable time
// (the memory-safety angle of assist is covered by the deterministic
// `test_hybrid_driver_assists_when_workers_busy` under miri).
#[cfg_attr(miri, ignore)]
#[test]
fn stress_streams_plus_assist_drivers() {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    struct DropCounter {
        counter: Arc<AtomicUsize>,
        val: i32,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    let stop = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        // 8 stream pipelines (feeder + resident sync stage workers on the
        // global pool) — the foreign injector load of the parallel suite.
        for k in 0..8 {
            let stop2 = Arc::clone(&stop);
            s.spawn(move || {
                let mut rounds = 0usize;
                while stop2.load(Ordering::Relaxed) == 0 {
                    let r = youpipe::stream(0..500u64).stage(|x| x + 1).run();
                    assert_eq!(r.len(), 500);
                    rounds += 1;
                }
                eprintln!("stream {k}: {rounds} rounds");
            });
        }
        // 16 fused assist drivers in tight loops + panic/drop accounting.
        for t in 0..16 {
            let stop3 = Arc::clone(&stop);
            s.spawn(move || {
                let data: Vec<u64> = (0..1000).collect();
                let mut rounds = 0usize;
                while stop3.load(Ordering::Relaxed) == 0 {
                    let v = data.clone();
                    let r: Vec<u64> = youpipe::pipe(v).map(|x| x.wrapping_mul(3)).collect();
                    assert_eq!(r.len(), 1000);
                    if rounds % 16 == 7 {
                        // Periodic mid-batch panic: every item must be dropped
                        // exactly once even when the driver assists.
                        let counter = Arc::new(AtomicUsize::new(0));
                        let c = counter.clone();
                        let items: Vec<DropCounter> = (0..20_000)
                            .map(|i| DropCounter {
                                counter: c.clone(),
                                val: i,
                            })
                            .collect();
                        let panic_at = 7_777;
                        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            youpipe::pipe(items).for_each(move |d: DropCounter| {
                                assert!(d.val != panic_at, "boom");
                            });
                        }));
                        assert!(res.is_err());
                        assert_eq!(
                            counter.load(Ordering::Relaxed),
                            20_000,
                            "thread {t} round {rounds}: drop accounting broke under assist"
                        );
                    }
                    rounds += 1;
                }
                eprintln!("driver {t}: {rounds} rounds");
            });
        }
        // Watchdog: bounded run; also proves the suite is not deadlocked.
        s.spawn(move || {
            std::thread::sleep(Duration::from_secs(6));
            stop.store(1, Ordering::Relaxed);
        });
    });
}
