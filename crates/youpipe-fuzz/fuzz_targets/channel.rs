//! Handoff-channel fuzz (the vendored crossfire data plane behind the
//! `handoff` wrapper): FIFO order, capacity semantics, close propagation,
//! and MPMC conservation under real thread interleaving.

#![no_main]

mod common;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use common::Reader;
use libfuzzer_sys::fuzz_target;
use youpipe::{
    channel,
    handoff::{ChannelError, TryRecvError, TrySendError},
};

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    match r.pick(3) {
        0 => spsc_interleaved(&mut r),
        1 => closed_semantics(&mut r),
        _ => mpmc_threaded(&mut r),
    }
});

/// Single-threaded interleave of `try_send`/`try_recv` against a `VecDeque`
/// model: strict FIFO, exact capacity accounting, no spurious Empty/Closed.
fn spsc_interleaved(r: &mut Reader<'_>) {
    let cap = 1 + r.pick(8);
    let (tx, rx) = channel::<u64>(cap);
    let mut model: VecDeque<u64> = VecDeque::new();
    let mut next = 0u64;
    for b in r.rest().iter().take(512).copied() {
        if b % 2 == 0 {
            if model.len() < cap {
                let v = next;
                next += 1;
                tx.try_send(v)
                    .expect("try_send must succeed while below capacity");
                model.push_back(v);
            } else {
                match tx.try_send(u64::from(b)) {
                    Err(TrySendError::Full(_)) => {},
                    Err(TrySendError::Closed(_)) => panic!("expected Full, got Closed"),
                    Ok(()) => panic!("try_send succeeded at capacity ({cap})"),
                }
            }
        } else {
            match rx.try_recv() {
                Ok(v) => assert_eq!(Some(v), model.pop_front(), "FIFO violation"),
                Err(TryRecvError::Empty) => assert!(
                    model.is_empty(),
                    "Empty reported with {} items buffered",
                    model.len()
                ),
                Err(TryRecvError::Closed) => panic!("Closed reported while sender is alive"),
            }
        }
    }
}

/// Close propagation: FIFO drain, Empty while any sender clone lives, Closed
/// once the last sender drops, and receiver clones keeping the channel open.
fn closed_semantics(r: &mut Reader<'_>) {
    let cap = 1 + r.pick(4);
    let n = r.pick(cap + 1); // 0..=cap: below capacity, `send` never blocks
    let (tx, rx) = channel::<u64>(cap);
    for i in 0..n as u64 {
        tx.send(i).expect("send below capacity");
    }

    let tx2 = tx.clone();
    drop(tx); // one clone alive: the channel must stay open
    let rx2 = rx.clone();
    drop(rx); // receiver clones behave like the original

    for i in 0..n as u64 {
        assert_eq!(rx2.recv(), Ok(i), "drain after clone/drop broke FIFO");
    }
    assert!(
        matches!(rx2.try_recv(), Err(TryRecvError::Empty)),
        "Empty expected while a sender clone is alive"
    );

    drop(tx2);
    assert!(
        matches!(rx2.try_recv(), Err(TryRecvError::Closed)),
        "Closed expected after the last sender dropped"
    );
    assert!(
        matches!(rx2.recv(), Err(ChannelError::Closed)),
        "blocking recv must report Closed after the last sender dropped"
    );
}

/// Real-thread MPMC stress: P producers × C consumers over a tiny buffer.
/// Asserts count conservation, multiset equality, and per-consumer
/// per-producer FIFO (a consumer's successive receives observe each
/// producer's send order — the ring is FIFO).
fn mpmc_threaded(r: &mut Reader<'_>) {
    let cap = 1 + r.pick(4);
    let producers = 1 + r.pick(3);
    let consumers = 1 + r.pick(2);
    let per = 1 + r.pick(16);

    let (tx, rx) = channel::<u64>(cap);
    let prod_handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            let pid = p as u64;
            std::thread::spawn(move || {
                for seq in 0..per as u64 {
                    tx.send((pid << 32) | seq)
                        .expect("send failed while consumers are live");
                }
            })
        })
        .collect();
    drop(tx); // the producer threads own the remaining sender handles

    let logs: Vec<Arc<Mutex<Vec<u64>>>> = (0..consumers)
        .map(|_| Arc::new(Mutex::new(Vec::new())))
        .collect();
    let cons_handles: Vec<_> = logs
        .iter()
        .map(|log| {
            let rx = rx.clone();
            let log = Arc::clone(log);
            std::thread::spawn(move || {
                // Closed (the only error variant) ends the drain: all
                // senders dropped and the ring is empty.
                while let Ok(v) = rx.recv() {
                    log.lock().unwrap().push(v);
                }
            })
        })
        .collect();

    for h in prod_handles {
        h.join().expect("producer thread panicked");
    }
    for h in cons_handles {
        h.join().expect("consumer thread panicked");
    }

    let mut all: Vec<u64> = Vec::new();
    for log in &logs {
        all.extend(log.lock().unwrap().iter().copied());
    }
    let mut want: Vec<u64> = (0..producers as u64)
        .flat_map(|pid| (0..per as u64).map(move |seq| (pid << 32) | seq))
        .collect();
    assert_eq!(all.len(), want.len(), "item count not conserved");
    all.sort_unstable();
    want.sort_unstable();
    assert_eq!(all, want, "item multiset not conserved");

    for log in &logs {
        let mut last: BTreeMap<u64, u64> = BTreeMap::new();
        for &v in log.lock().unwrap().iter() {
            let (pid, seq) = (v >> 32, v & 0xffff_ffff);
            // Strictly increasing only — NOT consecutive: rival consumers
            // take the intermediate items, so one consumer's subsequence of
            // a producer's stream has gaps by design. A decrease here would
            // be a real ring-FIFO violation.
            if let Some(&s) = last.get(&pid) {
                assert!(seq > s, "per-producer FIFO violated on one consumer");
            }
            last.insert(pid, seq);
        }
    }
}
