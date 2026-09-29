//! Batched data-plane ops (`YOUPIPE_BATCH_RECV`, todo #1 residual (d)):
//! `try_recv_batch` / `try_send_batch` on the channel wrappers plus the
//! end-to-end streaming topology with the knob on (collector batch drain,
//! worker batch burst, feeder batch push).
//!
//! The e2e knob block lives in ONE `#[test]` function: the knob is read
//! through a process-wide `OnceLock`, so the env var must be set before the
//! first `run()` in this process — sibling test fns must never race it
//! (the wrapper-level tests below never call `run()`). Miri runs ~100x
//! slower: counts shrink, the topology does not.
//!
//! Every potentially-unbounded wait here carries an explicit budget
//! (wall-clock deadline on the spin loops, `with_deadline` watchdog on each
//! e2e `run()`). A batch-claim liveness bug must fail the test with a
//! pointing message within seconds — the pre-budget version of the mpmc
//! spin test once burned 9 min at 200% CPU while holding the shared cargo
//! lock. Watchdog timeouts leak the stalled worker thread; the process
//! exit at the end of the binary reaps it, and the file's remaining tests
//! are sub-second, so the file stays under a minute even on failure.

use std::{mem::MaybeUninit, sync::Arc, time::Duration};

use youpipe::{
    FenceMode, StageOptions,
    handoff::{RecvItem, TryRecvError, channel, mpsc_channel},
    stream,
    sync::CancellationToken,
};

/// Wall-clock budget for one potentially-unbounded wait. Miri gets a
/// generous one: the budget is a liveness safety net, not a perf gate.
fn budget_secs() -> u64 {
    if cfg!(miri) {
        300
    } else {
        15
    }
}

/// Run `f` to completion or panic after `budget_secs()`. On timeout the
/// worker thread running `f` is deliberately leaked (unjoinable by design:
/// joining would re-block forever); process exit at the end of the test
/// binary reaps it and the file's remaining tests are sub-second.
fn with_deadline<T: Send + 'static>(label: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<T>();
    std::thread::Builder::new()
        .name(format!("watchdog-{label}"))
        .spawn(move || {
            let _ = done_tx.send(f());
        })
        .expect("spawn watchdog worker");
    match done_rx.recv_timeout(Duration::from_secs(budget_secs())) {
        Ok(v) => v,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
            "{label}: no completion within {}s — data-plane liveness bug (runaway this budget \
             exists to catch)",
            budget_secs()
        ),
        // The worker dropped `done_tx` without sending: it panicked (the
        // harness printed its message above).
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{label}: worker thread panicked before completion")
        },
    }
}

fn n_main() -> usize {
    if cfg!(miri) {
        400
    } else {
        20_000
    }
}

fn n_aux() -> usize {
    if cfg!(miri) {
        100
    } else {
        5_000
    }
}

/// Drain a receiver fully via batches, then verify the channel reports
/// Closed (EOF interacts with batching: a 0-batch is inconclusive, the
/// per-item `try_recv` must still resolve Disconnected after draining).
fn drain_batching<T: Send + 'static, R: RecvItem<T>>(rx: &R, max: usize) -> Vec<T> {
    let mut got = Vec::new();
    let mut buf: Vec<T> = Vec::with_capacity(max);
    loop {
        let n = rx.try_recv_batch(buf.spare_capacity_mut());
        if n == 0 {
            match rx.try_recv() {
                Ok(item) => {
                    got.push(item);
                    continue;
                },
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Closed) => return got,
            }
        } else {
            // SAFETY: try_recv_batch initialized exactly buf[..n].
            unsafe { buf.set_len(n) };
            got.append(&mut buf);
            continue;
        }
        match rx.recv() {
            Ok(item) => got.push(item),
            Err(_) => return got,
        }
    }
}

#[test]
fn mpsc_recv_batch_multi_producer_exactly_once() {
    // Whole-body watchdog: the liveness bug this family guards against
    // lives INSIDE the fork's `_read` stamp spin, which no outer loop can
    // interrupt — only fail-fast works (see the module doc).
    const P: usize = 4;
    const N: u32 = if cfg!(miri) {
        100
    } else {
        5_000
    };
    let mut got = with_deadline("mpsc exactly-once", move || {
        let (tx, rx) = mpsc_channel::<u32>(8);
        let mut hs = Vec::new();
        for p in 0..P {
            let tx = tx.clone();
            let base = u32::try_from(p).unwrap() * N;
            hs.push(std::thread::spawn(move || {
                for i in 0..N {
                    tx.send(base + i).unwrap();
                }
            }));
        }
        drop(tx);
        let got = drain_batching(&rx, 5);
        for h in hs {
            h.join().unwrap();
        }
        got
    });
    got.sort_unstable();
    assert_eq!(got, (0..u32::try_from(P).unwrap() * N).collect::<Vec<_>>());
}

#[test]
fn mpmc_recv_batch_multi_producer_multi_consumer_exactly_once() {
    // Whole-body watchdog (see `mpsc_...` above): bounds a lost-wakeup bug
    // even when it hides inside an uninterruptible channel-internal spin.
    const P: usize = 3;
    const C: usize = 3;
    const N: u32 = if cfg!(miri) {
        100
    } else {
        5_000
    };
    let mut all = with_deadline("mpmc exactly-once", move || {
        let (tx, rx) = channel::<u32>(8);
        let mut hs = Vec::new();
        for p in 0..P {
            let tx = tx.clone();
            let base = u32::try_from(p).unwrap() * N;
            hs.push(std::thread::spawn(move || {
                for i in 0..N {
                    tx.send(base + i).unwrap();
                }
            }));
        }
        drop(tx);
        let total = P * N as usize;
        let collected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut consumers = Vec::new();
        for _ in 0..C {
            let rx = rx.clone();
            let collected = Arc::clone(&collected);
            consumers.push(std::thread::spawn(move || {
                let mut got = Vec::new();
                let mut buf: Vec<u32> = Vec::with_capacity(6);
                while collected.load(std::sync::atomic::Ordering::Relaxed) < total {
                    let n = rx.try_recv_batch(buf.spare_capacity_mut());
                    if n == 0 {
                        std::hint::spin_loop();
                        continue;
                    }
                    // SAFETY: try_recv_batch initialized exactly buf[..n].
                    unsafe { buf.set_len(n) };
                    got.append(&mut buf);
                    collected.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                }
                got
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let mut all = Vec::new();
        for c in consumers {
            all.extend(c.join().unwrap());
        }
        all
    });
    all.sort_unstable();
    let total = P * N as usize;
    assert_eq!(all.len(), total);
    assert_eq!(all, (0..u32::try_from(P).unwrap() * N).collect::<Vec<_>>());
}

#[test]
fn send_batch_claims_free_prefix_then_blocking_tail() {
    // Whole-body watchdog (the send/recv interplay parks inside the
    // channel — an outer loop cannot budget that).
    let (drained_early, drained_tail) = with_deadline("send-batch prefix+tail", || {
        let (tx, rx) = channel::<u32>(4);
        // partially fill, then hand a batch larger than the free run
        for i in 0..3u32 {
            tx.send(i).unwrap();
        }
        let mut batch: Vec<MaybeUninit<u32>> = (10..14).map(MaybeUninit::new).collect();
        assert_eq!(tx.try_send_batch(&mut batch), 1);
        // full: nothing claimable, the unsent tail stays owned by the caller
        assert_eq!(tx.try_send_batch(&mut batch[1..]), 0);
        // SAFETY: initialized above, still owned (not sent).
        let tail: Vec<u32> = batch[1..]
            .iter()
            .map(|v| unsafe { v.assume_init() })
            .collect();
        // Free three slots from another thread, then push the 3-item tail
        // with the per-item blocking path (same semantics as the feeder
        // fallback: park per item while the ring is full). The drainer
        // MUST be a separate thread — the tail is one item longer than
        // the space left, so same-thread draining self-deadlocks.
        let drainer = {
            let rx = rx.clone();
            std::thread::spawn(move || {
                let mut v = Vec::with_capacity(3);
                for _ in 0..3 {
                    v.push(rx.recv().unwrap());
                }
                v
            })
        };
        for v in tail {
            tx.send(v).unwrap();
        }
        drop(tx);
        let early = drainer.join().unwrap();
        let late = drain_batching(&rx, 8);
        (early, late)
    });
    assert_eq!(drained_early, vec![0, 1, 2]);
    assert_eq!(drained_tail, vec![10, 11, 12, 13]);
}

#[test]
fn recv_batch_eof_drains_remaining_before_closed() {
    // MPSC
    let (tx, rx) = mpsc_channel::<u32>(16);
    for i in 0..10u32 {
        tx.send(i).unwrap();
    }
    drop(tx);
    let mut buf: Vec<u32> = Vec::with_capacity(3);
    let mut got = Vec::new();
    loop {
        let n = rx.try_recv_batch(buf.spare_capacity_mut());
        if n == 0 {
            break;
        }
        // SAFETY: initialized by the batch claim.
        unsafe { buf.set_len(n) };
        got.append(&mut buf);
    }
    assert_eq!(got, (0..10).collect::<Vec<_>>(), "every queued item first");
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Closed)));

    // MPMC
    let (tx, rx) = channel::<u32>(16);
    for i in 0..10u32 {
        tx.send(i).unwrap();
    }
    drop(tx);
    assert_eq!(drain_batching(&rx, 4), (0..10).collect::<Vec<_>>());
}

#[test]
fn knob_e2e_streaming_topology_with_batch() {
    // ONE test fn on purpose: the OnceLock-knob must initialize before any
    // `run()` in this process (see the module doc).
    unsafe { std::env::set_var("YOUPIPE_BATCH_RECV", "16") };
    let cancel = CancellationToken::new();
    let n = n_main();

    // unordered single stage (collector batch drain + worker batch burst +
    // feeder batch push)
    let out = with_deadline("unordered stage", {
        let cancel = cancel.clone();
        move || {
            stream(0..n as u64)
                .with_cancel(cancel)
                .stage(|x: u64| x.wrapping_add(1))
                .run()
        }
    });
    assert_eq!(out.len(), n);
    assert_eq!(out.iter().sum::<u64>(), (0..n as u64).map(|x| x + 1).sum());

    // ordered (ReorderBuffer re-sequencing over batched arrival runs)
    let out = with_deadline("ordered stage", {
        let cancel = cancel.clone();
        move || {
            stream(0..n as u64)
                .with_cancel(cancel)
                .stage(|x: u64| x.wrapping_mul(3))
                .ordered()
                .run()
        }
    });
    assert_eq!(out, (0..n as u64).map(|x| x * 3).collect::<Vec<_>>());

    // expand terminal (expand worker batch burst)
    let out = with_deadline("expand", {
        let cancel = cancel.clone();
        move || {
            stream(0..n_aux() as u64)
                .with_cancel(cancel)
                .expand(|x: u64| vec![x, x.wrapping_add(1)])
                .run()
        }
    });
    assert_eq!(out.len(), n_aux() * 2);

    // fence mid-chain (fence forwarder batch burst)
    let out = with_deadline("fence", {
        let cancel = cancel.clone();
        move || {
            stream(0..n_aux() as u64)
                .with_cancel(cancel)
                .stage(|x: u64| x.wrapping_add(2))
                .fence(FenceMode::Chunked(std::num::NonZeroUsize::new(64).unwrap()))
                .stage(|x: u64| x.wrapping_add(3))
                .run()
        }
    });
    assert_eq!(out.len(), n_aux());

    // small-worker terminal (workers(2)): the per-worker scratch must not
    // stall on a 2-shape topology
    let out = with_deadline("workers(2) ordered", {
        let cancel = cancel.clone();
        move || {
            stream(0..n_aux() as u64)
                .with_cancel(cancel)
                .stage_with(StageOptions::new().workers(2), |x: u64| x.wrapping_add(5))
                .ordered()
                .run()
        }
    });
    assert_eq!(out, (0..n_aux() as u64).map(|x| x + 5).collect::<Vec<_>>());
}
