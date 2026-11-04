//! Bounded cancellation-storm stress for the async waker faces (todo #14
//! pending audit, 2026-11-05). The loom arbiters in `waker_registry.rs`
//! model the registry-level async re-arm shapes (retire/Reuse, abandon,
//! cross-registry re-arm); these tests drive the SAME shapes through the
//! real poll paths with real executor wakers, which loom cannot express:
//!
//! * recv_cached futures dropped while registered (select cancellation
//!   mid-park) — the `abandon` Waiting→Closed path plus the Woken
//!   token-pass in `abandon_recv_waker`;
//! * the P-4 lucky-retry retire (cancel_waker / cancel_reuse_waker) when a
//!   parked episode completes or a send double-check succeeds;
//! * ONE slot moved across two channels' registries (cross-registry node
//!   reuse — the async analogue of the §12.5 stale-entry class, driven at
//!   the real-poll level);
//! * the MPSC async face (RegistrySingle: Init-park, retire leaves a stale
//!   WeakCell entry, fire's retry-on-Skip).
//!
//! Lost-wake failures here are hangs, so every test is bounded by
//! `tokio::time::timeout` and asserts exactly-once item accounting.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use crossfire::{mpmc, mpsc, AsyncWakerSlot};

const ITEMS: u64 = 512;
const CONSUMERS: usize = 4;
const CANCEL_BUDGET: usize = 64;
/// Long enough that the recv parks before the timer cancels it, short
/// enough to land inside a dry window.
const CANCEL_AFTER: Duration = Duration::from_millis(1);
const DRY_EVERY: u64 = 16;
const DRY_FOR: Duration = Duration::from_millis(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mpmc_recv_cancel_storm_exactly_once() {
    let (tx, rx) = mpmc::bounded_async::<u64>(1);
    let seen: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let cancels = Arc::new(AtomicUsize::new(0));

    let producer = tokio::spawn(async move {
        for i in 0..ITEMS {
            if i % DRY_EVERY == 0 {
                tokio::time::sleep(DRY_FOR).await;
            }
            if tx.send(i).await.is_err() {
                return;
            }
        }
    });

    let mut consumers = Vec::new();
    for c in 0..CONSUMERS {
        let rx = rx.clone();
        let seen = seen.clone();
        let cancels = cancels.clone();
        consumers.push(tokio::spawn(async move {
            let mut slot = AsyncWakerSlot::new();
            let mut budget = CANCEL_BUDGET;
            loop {
                tokio::select! {
                    // Cancel mid-park: the timer branch resolves only after
                    // the recv has registered (channel cap 1, producer
                    // yielding), so the dropped future abandons a
                    // registered node.
                    _ = tokio::time::sleep(CANCEL_AFTER), if c % 2 == 0 && budget > 0 => {
                        budget -= 1;
                        cancels.fetch_add(1, Ordering::Relaxed);
                    }
                    res = rx.recv_cached(&mut slot) => match res {
                        Ok(v) => {
                            assert!(seen.lock().unwrap().insert(v), "item {v} delivered twice");
                        }
                        Err(_) => return,
                    },
                }
            }
        }));
    }
    drop(rx);

    tokio::time::timeout(Duration::from_secs(30), async move {
        producer.await.unwrap();
        for c in consumers {
            c.await.unwrap();
        }
    })
    .await
    .expect("storm must not hang (lost wake)");
    assert_eq!(seen.lock().unwrap().len() as u64, ITEMS, "exactly-once delivery");
    assert!(cancels.load(Ordering::Relaxed) > 0, "storm must actually cancel parked recvs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mpmc_send_cancel_storm_exactly_once() {
    let (tx, rx) = mpmc::bounded_async::<u64>(1);
    let cancels = Arc::new(AtomicUsize::new(0));
    let cancels_p = cancels.clone();
    let producer = tokio::spawn(async move {
        let mut slot = AsyncWakerSlot::new();
        let mut budget = CANCEL_BUDGET;
        for i in 0..ITEMS {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(CANCEL_AFTER), if budget > 0 => {
                        budget -= 1;
                        cancels_p.fetch_add(1, Ordering::Relaxed);
                    }
                    res = tx.send_cached(i, &mut slot) => {
                        assert!(res.is_ok(), "consumer alive");
                        break;
                    }
                }
            }
        }
    });
    let seen: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut consumers = Vec::new();
    // One consumer: its stalls are the only drain pauses, so the producer's
    // send_cached actually parks (with several consumers someone is always
    // ready and the ring never stays full).
    let seen_c = seen.clone();
    consumers.push(tokio::spawn(async move {
        let mut slot = AsyncWakerSlot::new();
        let mut n = 0u64;
        while let Ok(v) = rx.recv_cached(&mut slot).await {
            assert!(seen_c.lock().unwrap().insert(v), "item {v} delivered twice");
            n += 1;
            if n % DRY_EVERY == 0 {
                // Consumer stall: the producer's parked send_cached gets
                // cancelled by its timer mid-park.
                tokio::time::sleep(DRY_FOR).await;
            }
        }
    }));

    tokio::time::timeout(Duration::from_secs(30), async move {
        producer.await.unwrap();
        for c in consumers {
            c.await.unwrap();
        }
    })
    .await
    .expect("send storm must not hang");
    assert_eq!(seen.lock().unwrap().len() as u64, ITEMS);
    assert!(cancels.load(Ordering::Relaxed) > 0, "send storm must cancel parked sends");
}

/// One slot, two channels, one task: the node is re-armed across the two
/// recv registries (will_wake holds — same task), the cross-registry reuse
/// shape behind the §12.5 stale-entry class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slot_reused_across_two_registries_exactly_once() {
    let (tx_a, rx_a) = mpmc::bounded_async::<u64>(1);
    let (tx_b, rx_b) = mpmc::bounded_async::<u64>(1);

    let pa = tokio::spawn(async move {
        for i in 0..ITEMS {
            if i % DRY_EVERY == 0 {
                tokio::time::sleep(DRY_FOR).await;
            }
            if tx_a.send(i).await.is_err() {
                return;
            }
        }
    });
    let pb = tokio::spawn(async move {
        for i in 0..ITEMS {
            if i % DRY_EVERY == 0 {
                tokio::time::sleep(DRY_FOR).await;
            }
            if tx_b.send(i).await.is_err() {
                return;
            }
        }
    });

    let consumer = tokio::spawn(async move {
        let mut slot = AsyncWakerSlot::new();
        let mut seen_a = HashSet::new();
        let mut seen_b = HashSet::new();
        let mut budget = CANCEL_BUDGET;
        let mut closed_a = false;
        let mut closed_b = false;
        // Alternate the SAME slot between the two channels; the cancel timer
        // drops registered episodes mid-park on either side. A closed side
        // is skipped — the other producer may still be backpressure-blocked.
        while !(closed_a && closed_b) {
            if !closed_a {
                tokio::select! {
                    _ = tokio::time::sleep(CANCEL_AFTER), if budget > 0 => {
                        budget -= 1;
                    }
                    res = rx_a.recv_cached(&mut slot) => match res {
                        Ok(v) => assert!(seen_a.insert(v), "A/{v} twice"),
                        Err(_) => closed_a = true,
                    },
                }
            }
            if !closed_b {
                tokio::select! {
                    _ = tokio::time::sleep(CANCEL_AFTER), if budget > 0 => {
                        budget -= 1;
                    }
                    res = rx_b.recv_cached(&mut slot) => match res {
                        Ok(v) => assert!(seen_b.insert(v), "B/{v} twice"),
                        Err(_) => closed_b = true,
                    },
                }
            }
        }
        (seen_a, seen_b)
    });

    let (seen_a, seen_b) = tokio::time::timeout(Duration::from_secs(30), async move {
        let _ = tokio::join!(pa, pb);
        consumer.await.unwrap()
    })
    .await
    .expect("cross-registry reuse must not hang");
    assert_eq!(seen_a.len() as u64, ITEMS, "channel A exactly-once");
    assert_eq!(seen_b.len() as u64, ITEMS, "channel B exactly-once");
}

/// The MPSC async face: RegistrySingle recv (Init-park, stale WeakCell
/// entries after retire) under multi-producer load and cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn mpsc_single_registry_cancel_storm_exactly_once() {
    let (tx, rx) = mpsc::bounded_async::<u64>(1);
    let per_producer = ITEMS / 2;
    let pa = {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..per_producer {
                if i % DRY_EVERY == 0 {
                    tokio::time::sleep(DRY_FOR).await;
                }
                if tx.send(i).await.is_err() {
                    return;
                }
            }
        })
    };
    let pb = tokio::spawn(async move {
        for i in per_producer..ITEMS {
            if i % DRY_EVERY == 0 {
                tokio::time::sleep(DRY_FOR).await;
            }
            if tx.send(i).await.is_err() {
                return;
            }
        }
    });

    let consumer = tokio::spawn(async move {
        let mut slot = AsyncWakerSlot::new();
        let mut seen = HashSet::new();
        let mut budget = CANCEL_BUDGET;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(CANCEL_AFTER), if budget > 0 => {
                    budget -= 1;
                }
                res = rx.recv_cached(&mut slot) => match res {
                    Ok(v) => assert!(seen.insert(v), "item {v} twice"),
                    Err(_) => break,
                },
            }
        }
        seen
    });

    let seen = tokio::time::timeout(Duration::from_secs(30), async move {
        let _ = tokio::join!(pa, pb);
        consumer.await.unwrap()
    })
    .await
    .expect("mpsc storm must not hang");
    assert_eq!(seen.len() as u64, ITEMS, "exactly-once delivery");
}
