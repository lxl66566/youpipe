//! End-to-end coverage of the blocking handoff paths that exercise the
//! vendored crossfire per-thread blocking waker (design C) through youpipe's
//! public channel API: park + wake on both sides, disconnect-while-parked,
//! the close-vs-rearm stale-entry hazard, multi-threaded contention, and the
//! `park_timeout` variants (driven via crossfire directly — the handoff
//! wrapper does not re-export timeouts).
//!
//! These are the real-thread counterparts of the waker protocol's loom
//! models in `youpipe-crossfire` (`cargo test -p youpipe-crossfire
//! --features loom`): the models enumerate the registry interleavings, these
//! tests prove the same protocol serves actual blocking traffic — and they
//! run under miri/tree-borrows, where the thread-local node's write-once
//! handle and the Arc/Weak upgrades are checked for memory safety.

use std::{
    thread,
    time::{Duration, Instant},
};

use crossfire::{RecvTimeoutError, SendTimeoutError, mpmc};
use youpipe::handoff::channel::{ChannelError, channel, mpsc_channel};

/// Miri interprets ~1000x slower than native (see docs/src/dev/testing.md);
/// keep every workload at "seconds under miri" scale.
fn contention_iters() -> usize {
    if cfg!(miri) {
        8
    } else {
        4_000
    }
}

/// Give a spawned blocker a chance to actually park (register its waker,
/// commit Waiting, sleep) before the main thread triggers the event that
/// must wake it. Not a synchronization mechanism — every assertion below
/// holds for any interleaving; this only steers the run into the parked
/// path so the wake/disconnect logic is really exercised.
fn let_them_park() {
    let yields = if cfg!(miri) {
        4
    } else {
        200
    };
    for _ in 0..yields {
        thread::yield_now();
    }
}

#[test]
fn blocking_send_parks_until_drain() {
    let (tx, rx) = channel::<u32>(1);
    tx.send(1).unwrap(); // full
    let blocker = {
        let tx = tx.clone();
        thread::spawn(move || tx.send(2).unwrap())
    };
    let_them_park();
    // Draining one item fires the receiver's registry: the parked sender
    // must wake and complete.
    assert_eq!(rx.recv().unwrap(), 1);
    blocker.join().unwrap();
    assert_eq!(rx.recv().unwrap(), 2);
}

#[test]
fn blocking_recv_parks_until_send() {
    let (tx, rx) = channel::<u32>(1);
    let blocker = thread::spawn(move || rx.recv().unwrap());
    let_them_park();
    tx.send(42).unwrap();
    assert_eq!(blocker.join().unwrap(), 42);
}

#[test]
fn parked_recv_disconnects_when_senders_drop() {
    let (tx, rx) = channel::<u32>(1);
    let blocker = thread::spawn(move || rx.recv());
    let_them_park();
    drop(tx); // close_wake: the parked receiver must wake with Closed
    assert_eq!(blocker.join().unwrap(), Err(ChannelError::Closed));
}

#[test]
fn parked_send_disconnects_when_receivers_drop() {
    let (tx, rx) = channel::<u32>(1);
    tx.send(1).unwrap(); // full
    let blocker = thread::spawn(move || tx.send(2));
    let_them_park();
    drop(rx);
    assert_eq!(blocker.join().unwrap(), Err(ChannelError::Closed));
}

#[test]
fn mpsc_blocking_send_parks_until_drain() {
    // MPSC send blocks on the same Multi registry as MPMC; only the recv
    // side differs (lock-free single-slot registry, no seq).
    let (tx, rx) = mpsc_channel::<u32>(1);
    tx.send(1).unwrap(); // full
    let blocker = {
        let tx = tx.clone();
        thread::spawn(move || tx.send(2).unwrap())
    };
    let_them_park();
    assert_eq!(rx.recv().unwrap(), 1);
    blocker.join().unwrap();
    assert_eq!(rx.recv().unwrap(), 2);
}

#[test]
fn stale_entry_close_must_not_disconnect_live_waiter_elsewhere() {
    // Design C's headline hazard, end-to-end: a thread leaves a (by design,
    // un-removed) waker entry on ch1 when its contended send episode ends,
    // then re-arms the same immortal node while blocking on ch2. Closing
    // ch1 pops that stale entry — it must be skipped by its seq tag, not
    // stamped Closed, or the live ch2 waiter would see a spurious
    // disconnect. With the pre-C per-episode node the entry was dead
    // weight (Weak upgrade fails); here the node is alive by construction.
    let (tx1, rx1) = channel::<u32>(1);
    let (tx2, rx2) = channel::<u32>(1);
    tx1.send(1).unwrap(); // ch1 full

    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let worker = {
        let tx1 = tx1;
        thread::spawn(move || {
            // Episode 1: contended send on ch1 (parks, wakes on the drain
            // below), ends with the node alive and the entry left behind.
            tx1.send(2).unwrap();
            ready_tx.send(()).unwrap();
            // Episode 2: the same node, re-armed, now parked on ch2's recv.
            rx2.recv()
        })
    };
    let_them_park();
    assert_eq!(rx1.recv().unwrap(), 1); // wake the parked sender
    ready_rx.recv().unwrap(); // ch1 episode fully over
    let_them_park(); // worker parks on ch2
    drop(rx1); // ch1 closes: pops the stale entry
    let_them_park();
    tx2.send(99).unwrap(); // ch2's own event wakes it
    assert_eq!(worker.join().unwrap(), Ok(99));
}

#[test]
fn contended_mpmc_traffic_under_real_threads() {
    const PRODUCERS: usize = 3;
    const CONSUMERS: usize = 2;
    let per_producer = contention_iters();
    let (tx, rx) = channel::<u64>(4);

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per_producer as u64 {
                    tx.send(p as u64 * per_producer as u64 + i).unwrap();
                }
            })
        })
        .collect();
    drop(tx); // so recv() sees Disconnected once the producers finish
    let consumers: Vec<_> = (0..CONSUMERS)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || {
                let (mut count, mut sum, mut seen) = (0u64, 0u64, Vec::new());
                while let Ok(v) = rx.recv() {
                    count += 1;
                    sum += v;
                    seen.push(v);
                }
                (count, sum, seen)
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    let (mut total, mut sum, mut all) = (0u64, 0u64, Vec::new());
    for c in consumers {
        let (count, s, mut seen) = c.join().unwrap();
        total += count;
        sum += s;
        all.append(&mut seen);
    }
    assert_eq!(total as usize, PRODUCERS * per_producer);
    // Duplicate- or loss-free under contention.
    all.sort_unstable();
    let dedup: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(dedup.len(), all.len());
    // Each producer p emits the disjoint range p*per..p*per+per-1.
    let per = per_producer as u64;
    let expected: u64 = (0..PRODUCERS as u64)
        .map(|p| p * per * per + per * (per - 1) / 2)
        .sum();
    assert_eq!(sum, expected);
}

#[test]
fn timeout_park_paths_expire_and_wake_early() {
    // The handoff wrapper does not expose timeouts; drive crossfire's
    // park_timeout variants directly — the same per-thread node and
    // registry protocol as the plain blocking calls, plus the deadline
    // check inside the park loop.
    let (tx, rx) = mpmc::bounded_blocking::<u32>(1);
    tx.send(1).unwrap(); // full
    let t0 = Instant::now();
    assert_eq!(
        tx.send_timeout(2, Duration::from_millis(30)),
        Err(SendTimeoutError::Timeout(2))
    );
    assert!(t0.elapsed() >= Duration::from_millis(30));

    // Woken before the deadline: the parked sender completes immediately
    // once a slot frees up.
    let blocker = {
        let tx = tx.clone();
        thread::spawn(move || tx.send_timeout(3, Duration::from_secs(60)).unwrap())
    };
    let_them_park();
    assert_eq!(rx.recv().unwrap(), 1);
    blocker.join().unwrap();
    assert_eq!(rx.recv().unwrap(), 3);

    // recv side: expiry on empty, then an early wake.
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(30)),
        Err(RecvTimeoutError::Timeout)
    );
    let blocker = {
        let rx = rx.clone();
        thread::spawn(move || rx.recv_timeout(Duration::from_secs(60)).unwrap())
    };
    let_them_park();
    tx.send(4).unwrap();
    assert_eq!(blocker.join().unwrap(), 4);
}
