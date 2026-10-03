//! Handoff-channel fuzz (the vendored crossfire data plane behind the
//! `handoff` wrapper): FIFO order, capacity semantics, close propagation,
//! batch send/claim (`try_send_batch`/`try_recv_batch`), and MPMC/MPSC
//! conservation under real thread interleaving.

#![no_main]

mod common;

use std::{
    collections::{BTreeMap, VecDeque},
    mem::MaybeUninit,
    sync::{Arc, Mutex},
};

use common::Reader;
use libfuzzer_sys::fuzz_target;
use youpipe::{
    channel,
    handoff::{ChannelError, TryRecvError, TrySendError, mpsc_channel},
};

fuzz_target!(|data: &[u8]| {
    let mut r = Reader::new(data);
    match r.pick(5) {
        0 => spsc_interleaved(&mut r),
        1 => closed_semantics(&mut r),
        2 => batch_interleaved(&mut r),
        3 => mpmc_threaded(&mut r),
        _ => mpsc_threaded(&mut r),
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

/// Single-threaded batch-op interleave against a `VecDeque` model, at mixed
/// single/batch granularity: exact claimed counts (`try_send_batch` =
/// min(free, k), `try_recv_batch` = min(ready, buf)), strict FIFO across the
/// mix, and the EOF transition the engine relies on — after the last sender
/// drops, batch claims keep draining the buffered items and only then report
/// 0 while `try_recv` says Closed.
///
/// Mirrors the production call pattern: a typed Vec's `spare_capacity_mut()`
/// as the claim buffer (`claim_poll`) and fresh values written into a
/// `MaybeUninit` staging slice per send — the sent prefix is moved out and
/// must never be re-read.
fn batch_interleaved(r: &mut Reader<'_>) {
    let cap = 1 + r.pick(8);
    let (tx, rx) = channel::<u64>(cap);
    let mut model: VecDeque<u64> = VecDeque::new();
    let mut next = 0u64;

    for b in r.rest().iter().take(512).copied() {
        match b % 4 {
            // batch send of k fresh values
            0 => {
                let k = 1 + usize::from(b) % 4;
                let mut staging = [MaybeUninit::uninit(); 4];
                for (i, slot) in staging[..k].iter_mut().enumerate() {
                    slot.write(next + i as u64);
                }
                next += k as u64; // unsent values are skipped, never reused
                let sent = tx.try_send_batch(&mut staging[..k]);
                assert_eq!(
                    sent,
                    (cap - model.len()).min(k),
                    "try_send_batch must claim the whole free prefix"
                );
                for i in 0..sent {
                    model.push_back(next - k as u64 + i as u64);
                }
            },
            // batch claim into a fresh scratch Vec
            1 => {
                let c = 1 + usize::from(b) % 4;
                let mut scratch: Vec<u64> = Vec::with_capacity(c);
                let n = rx.try_recv_batch(scratch.spare_capacity_mut());
                assert_eq!(
                    n,
                    model.len().min(c),
                    "try_recv_batch must claim the whole ready run"
                );
                // SAFETY: try_recv_batch initialized exactly scratch[..n].
                unsafe { scratch.set_len(n) };
                for v in scratch {
                    assert_eq!(Some(v), model.pop_front(), "FIFO violated across batch");
                }
            },
            // single try_send below capacity
            2 => {
                if model.len() < cap {
                    let v = next;
                    next += 1;
                    tx.try_send(v)
                        .expect("try_send must succeed while below capacity");
                    model.push_back(v);
                }
            },
            // single try_recv
            _ => match rx.try_recv() {
                Ok(v) => assert_eq!(Some(v), model.pop_front(), "FIFO violation"),
                Err(TryRecvError::Empty) => assert!(
                    model.is_empty(),
                    "Empty reported with {} items buffered",
                    model.len()
                ),
                Err(TryRecvError::Closed) => panic!("Closed reported while sender is alive"),
            },
        }
    }

    drop(tx);
    let mut drained = Vec::new();
    loop {
        let mut scratch: Vec<u64> = Vec::with_capacity(3);
        let n = rx.try_recv_batch(scratch.spare_capacity_mut());
        if n == 0 {
            assert!(
                matches!(rx.try_recv(), Err(TryRecvError::Closed)),
                "0-claim after last sender dropped must mean Closed"
            );
            break;
        }
        // SAFETY: try_recv_batch initialized exactly scratch[..n].
        unsafe { scratch.set_len(n) };
        drained.extend(scratch);
    }
    for v in drained {
        assert_eq!(Some(v), model.pop_front(), "EOF drain broke FIFO");
    }
    assert!(model.is_empty(), "items lost at EOF");
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

/// Real-thread MPSC stress with the production drain rhythm (`claim_poll` /
/// the sharded terminal's `RecvItem::try_recv_batch`): batch-claim first,
/// fall back to blocking `recv` on a 0-claim — which also resolves the
/// Empty/Closed distinction a 0-batch cannot. One consumer (MPSC contract),
/// so the assertions strengthen to per-producer FIFO on the *single*
/// combined log: every producer's items appear in send order in one
/// sequence, plus count and multiset conservation.
fn mpsc_threaded(r: &mut Reader<'_>) {
    let cap = 1 + r.pick(4);
    let producers = 1 + r.pick(3);
    let per = 1 + r.pick(16);

    let (tx, rx) = mpsc_channel::<u64>(cap);
    let handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            let pid = p as u64;
            std::thread::spawn(move || {
                for seq in 0..per as u64 {
                    tx.send((pid << 32) | seq)
                        .expect("send failed while consumer is live");
                }
            })
        })
        .collect();
    drop(tx); // producer threads own the remaining sender handles

    let mut got: Vec<u64> = Vec::new();
    loop {
        let mut scratch: Vec<u64> = Vec::with_capacity(4);
        let n = rx.try_recv_batch(scratch.spare_capacity_mut());
        if n > 0 {
            // SAFETY: try_recv_batch initialized exactly scratch[..n].
            unsafe { scratch.set_len(n) };
            got.extend(scratch);
            continue;
        }
        // 0-claim: block for the next item or Closed.
        match rx.recv() {
            Ok(v) => got.push(v),
            Err(ChannelError::Closed) => break,
        }
    }

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    let mut want: Vec<u64> = (0..producers as u64)
        .flat_map(|pid| (0..per as u64).map(move |seq| (pid << 32) | seq))
        .collect();
    assert_eq!(got.len(), want.len(), "item count not conserved");

    // FIFO on the arrival-order sequence (before the sort destroys it): the
    // single consumer's log must show every producer's items in send order.
    let mut last: BTreeMap<u64, u64> = BTreeMap::new();
    for &v in &got {
        let (pid, seq) = (v >> 32, v & 0xffff_ffff);
        if let Some(&s) = last.get(&pid) {
            assert!(seq > s, "per-producer FIFO violated on the single consumer");
        }
        last.insert(pid, seq);
    }

    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want, "item multiset not conserved");
}
