//! Dropping a send future must drop its item exactly once and leave the
//! channel intact, whether the future was ever polled or is parked
//! mid-send (the `consumed` exactly-once invariant on `SendFuture` /
//! `SendTimeoutFuture`; the unpolled case is upstream issue #72, fixed in
//! 3.1.22).
//!
//! `SendTimeoutFuture` additionally must not be `Send` unless the item
//! is: its `Drop` is usable without the (item-bounded) `Future` impl, so
//! an unconditional impl would allow a `!Send` item to be dropped
//! cross-thread.

use std::{
    future::{pending, Future},
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use crossfire::mpmc;

/// Counts its own drops; `id` tells which copy reached the channel.
struct Item {
    id: usize,
    drops: Arc<AtomicUsize>,
}

impl Drop for Item {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn item(id: usize, drops: &Arc<AtomicUsize>) -> Item {
    Item { id, drops: drops.clone() }
}

#[test]
fn unpolled_send_future_drops_item() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpmc::bounded_async::<Item>(1);
    tx.try_send(item(0, &drops)).unwrap();
    drop(tx.send(item(1, &drops)));
    assert_eq!(drops.load(Ordering::SeqCst), 1, "unpolled SendFuture leaked its item");
    assert_eq!(rx.try_recv().unwrap().id, 0);
}

#[test]
fn unpolled_send_timeout_future_drops_item() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpmc::bounded_async::<Item>(1);
    tx.try_send(item(0, &drops)).unwrap();
    drop(tx.send_with_timer(item(1, &drops), pending::<()>()));
    assert_eq!(drops.load(Ordering::SeqCst), 1, "unpolled SendTimeoutFuture leaked its item");
    assert_eq!(rx.try_recv().unwrap().id, 0);
}

#[test]
fn parked_send_future_dropped_after_pending_poll_drops_item() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpmc::bounded_async::<Item>(1);
    tx.try_send(item(0, &drops)).unwrap();
    {
        let mut fut = pin!(tx.send(item(1, &drops)));
        let poll = fut.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(poll, Poll::Pending));
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1, "cancelled parked SendFuture leaked its item");
    assert_eq!(rx.try_recv().unwrap().id, 0);
}
