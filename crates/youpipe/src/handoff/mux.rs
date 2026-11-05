//! The M:N async consumer driver shared by the stream engine's async stages
//! (`spawn_async_drivers`) and the graph engine's async nodes
//! (`node_async_spawn`).
//!
//! An async stage admits `io_concurrency` in-flight items. One runtime task
//! per in-flight slot puts `io_concurrency` remote spawns on the run's
//! critical path (~0.4 µs each from the calling thread: 2000 tasks delayed
//! the last item's first poll by ~1 ms, a full tokio timer tick) and pays one
//! cross-thread task wake per item handed over. Instead the slots are split
//! over at most `parallelism` driver tasks (the runtime's worker count); each
//! driver claims items while it has room, multiplexes their futures in a
//! `FuturesUnordered`, and delivers outputs in completion order. A driver
//! with room parks on its intake and its in-flight set at once, so an
//! arrival never waits behind a busy driver's IO.
//!
//! `YOUPIPE_ASYNC_MUX` (runtime knob, so A/B benches compare one binary):
//! unset/`"1"` = `min(io_concurrency, runtime workers)` drivers; `"0"` = one
//! task per in-flight slot; `"N"` (>= 2) = `min(io_concurrency, N)` drivers.

use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

use futures_util::stream::{FuturesUnordered, StreamExt};

use super::{
    channel::{AsyncReceiver, AsyncSendItem, TryRecvError, WakerSlot},
    worker::cancel_active,
};
use crate::sync::CancellationToken;

#[derive(Clone, Copy)]
enum MuxPolicy {
    /// One task per in-flight slot.
    Off,
    /// `min(concurrency, runtime parallelism)` drivers.
    Runtime,
    /// `min(concurrency, n)` drivers.
    Fixed(usize),
}

static POLICY: std::sync::OnceLock<MuxPolicy> = std::sync::OnceLock::new();

fn policy() -> MuxPolicy {
    *POLICY.get_or_init(|| match std::env::var("YOUPIPE_ASYNC_MUX") {
        Ok(v) if v == "1" || v.is_empty() => MuxPolicy::Runtime,
        Ok(v) if v == "0" => MuxPolicy::Off,
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n >= 2 => MuxPolicy::Fixed(n),
            _ => panic!(
                "YOUPIPE_ASYNC_MUX: invalid value {v:?} (leave unset, or use \"0\"/\"1\"/a driver \
                 count >= 2)"
            ),
        },
        Err(_) => MuxPolicy::Runtime,
    })
}

/// How one async stage's in-flight slots map onto driver tasks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MuxPlan {
    drivers: usize,
    concurrency: usize,
}

impl MuxPlan {
    /// Split `concurrency` in-flight slots over the drivers the knob policy
    /// grants a runtime with `parallelism` worker threads.
    pub(crate) fn new(concurrency: usize, parallelism: usize) -> Self {
        let concurrency = concurrency.max(1);
        let drivers = match policy() {
            MuxPolicy::Off => concurrency,
            // NOTE(perf): one driver per worker is the measured optimum. Two
            // per worker (`YOUPIPE_ASYNC_MUX=62` on 31 workers, 5 interleaved
            // rounds) left the instant-future `sharded_term_async/async1_*`
            // @100K rows flat and cost `graph_shapes/fanin_async_cpu_graph`
            // +49 % @100K / +17.5 % @1K, `sync_fuse/async_pair_cpu_split`
            // @1K +4.9 %.
            MuxPolicy::Runtime => concurrency.min(parallelism.max(1)),
            MuxPolicy::Fixed(n) => concurrency.min(n),
        };
        Self {
            drivers,
            concurrency,
        }
    }

    /// Number of driver tasks to spawn.
    pub(crate) fn drivers(self) -> usize {
        self.drivers
    }

    /// In-flight cap of driver `i`: the slots split as evenly as possible,
    /// summing to the stage's concurrency exactly.
    pub(crate) fn cap(self, i: usize) -> usize {
        self.concurrency / self.drivers + usize::from(i < self.concurrency % self.drivers)
    }
}

/// Non-blocking claim outcome.
pub(crate) enum TryClaim<T> {
    Item(T),
    Empty,
    /// Every input disconnected and drained.
    Closed,
}

/// One driver's input side.
pub(crate) trait AsyncIntake<T>: Send {
    /// Claim one item without parking.
    fn try_claim(&mut self) -> TryClaim<T>;
    /// Park until an item arrives (`Some`) or every input is exhausted
    /// (`None`). Must be cancel-safe: the driver drops a pending claim
    /// whenever one of its in-flight items completes first.
    fn claim(&mut self) -> impl Future<Output = Option<T>> + Send;
}

/// One driver's output side.
pub(crate) trait AsyncOutlet<T>: Send {
    /// Deliver one item; `false` once every downstream has closed (the
    /// driver exits).
    fn deliver(&mut self, item: T) -> impl Future<Output = bool> + Send;
}

/// Single-receiver intake: the stream engine's async stage input. One waker
/// slot per driver — its parks re-arm one node instead of allocating one per
/// episode.
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))] // async stages are tokio-gated
pub(crate) struct RxIntake<T: Send + Unpin + 'static> {
    rx: AsyncReceiver<T>,
    slot: WakerSlot,
}

impl<T: Send + Unpin + 'static> RxIntake<T> {
    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn new(rx: AsyncReceiver<T>) -> Self {
        Self {
            rx,
            slot: WakerSlot::new(),
        }
    }
}

impl<T: Send + Unpin + 'static> AsyncIntake<T> for RxIntake<T> {
    fn try_claim(&mut self) -> TryClaim<T> {
        match self.rx.try_recv() {
            Ok(item) => TryClaim::Item(item),
            Err(TryRecvError::Empty) => TryClaim::Empty,
            Err(TryRecvError::Closed) => TryClaim::Closed,
        }
    }

    async fn claim(&mut self) -> Option<T> {
        self.rx.recv_cached(&mut self.slot).await.ok()
    }
}

/// Single-sender outlet over any [`AsyncSendItem`] backing (the stream
/// engine's mid-chain MPMC ring, terminal MPSC ring or terminal shard).
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))] // async stages are tokio-gated
pub(crate) struct TxOutlet<Tx> {
    tx: Tx,
    slot: WakerSlot,
}

impl<Tx> TxOutlet<Tx> {
    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn new(tx: Tx) -> Self {
        Self {
            tx,
            slot: WakerSlot::new(),
        }
    }
}

impl<T: Send, Tx: AsyncSendItem<T>> AsyncOutlet<T> for TxOutlet<Tx> {
    async fn deliver(&mut self, item: T) -> bool {
        self.tx.send_cached(item, &mut self.slot).await.is_ok()
    }
}

enum Event<O, T> {
    Done(O),
    Claimed(Option<T>),
}

/// Run one driver to completion: claim → `map` → deliver with up to `cap`
/// items in flight, outputs in completion order. Exits on input EOF once its
/// in-flight items are delivered, on downstream close, or once `cancel` is
/// observed after a claim: the item that observed the token is dropped, the
/// items accepted before it are still delivered (the per-task loop's
/// contract — an accepted item is never discarded by cancellation).
///
/// `cap == 1` is the sequential loop, without an in-flight set.
pub(crate) async fn drive<T, O, Fut>(
    mut intake: impl AsyncIntake<T>,
    map: impl Fn(T) -> Fut,
    mut out: impl AsyncOutlet<O>,
    cap: usize,
    cancel: Option<&CancellationToken>,
) where
    Fut: Future<Output = O>,
{
    if cap <= 1 {
        while let Some(item) = intake.claim().await {
            if cancel_active(cancel) || !out.deliver(map(item).await).await {
                return;
            }
        }
        return;
    }
    let mut overflow = FuturesUnordered::new();
    // Inline slot: an admitted item's future lands here while the slot is
    // free and is polled at once, so futures that resolve on their first
    // poll (CPU-only async closures) run as a claim → poll → deliver loop
    // that never touches the allocating `overflow` set, which only takes
    // items admitted while the slot holds a pending future.
    let mut head = pin!(None::<Fut>);
    // Claimed by the select, admitted by the refill below.
    let mut next: Option<T> = None;
    // Input still claimable: no EOF and no cancel observed yet.
    let mut open = true;
    loop {
        let mut in_flight = overflow.len() + usize::from(head.is_some());
        while open && in_flight < cap {
            let item = match next.take() {
                Some(item) => item,
                None => match intake.try_claim() {
                    TryClaim::Item(item) => item,
                    TryClaim::Empty => break,
                    TryClaim::Closed => {
                        open = false;
                        break;
                    },
                },
            };
            if cancel_active(cancel) {
                open = false;
                break;
            }
            if head.is_some() {
                overflow.push(map(item));
                in_flight += 1;
                continue;
            }
            head.set(Some(map(item)));
            let first = poll_fn(|cx| {
                Poll::Ready(
                    head.as_mut()
                        .as_pin_mut()
                        .map_or(Poll::Pending, |f| f.poll(cx)),
                )
            })
            .await;
            match first {
                Poll::Ready(output) => {
                    head.set(None);
                    if !out.deliver(output).await {
                        return;
                    }
                },
                Poll::Pending => in_flight += 1,
            }
        }
        let event = if in_flight == 0 {
            if !open {
                return;
            }
            Event::Claimed(intake.claim().await)
        } else {
            // The first completion, or an arrival while there is room. The
            // in-flight futures are polled first: completions free slots and
            // keep downstream fed; a losing claim is dropped (cancel-safe).
            let room = open && in_flight < cap;
            let mut claim = pin!(room.then(|| intake.claim()));
            poll_fn(|cx| {
                if let Some(Poll::Ready(output)) = head.as_mut().as_pin_mut().map(|f| f.poll(cx)) {
                    head.set(None);
                    return Poll::Ready(Event::Done(output));
                }
                if let Poll::Ready(Some(output)) = overflow.poll_next_unpin(cx) {
                    return Poll::Ready(Event::Done(output));
                }
                match claim.as_mut().as_pin_mut().map(|c| c.poll(cx)) {
                    Some(Poll::Ready(item)) => Poll::Ready(Event::Claimed(item)),
                    _ => Poll::Pending,
                }
            })
            .await
        };
        match event {
            Event::Done(output) => {
                if !out.deliver(output).await {
                    return;
                }
            },
            Event::Claimed(Some(item)) => next = Some(item),
            Event::Claimed(None) => open = false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::HashMap,
        rc::Rc,
    };

    use futures::{channel::oneshot, executor::LocalPool, task::LocalSpawnExt};

    use super::*;
    use crate::handoff::channel::async_channel;

    /// Item `x`'s future records its start, then waits until `open(x)`.
    struct Gated {
        started: RefCell<Vec<u32>>,
        gates: RefCell<HashMap<u32, oneshot::Receiver<()>>>,
        keys: RefCell<HashMap<u32, oneshot::Sender<()>>>,
    }

    impl Gated {
        fn new(n: u32) -> Rc<Self> {
            let (keys, gates) = (0..n)
                .map(|x| {
                    let (key, gate) = oneshot::channel();
                    ((x, key), (x, gate))
                })
                .unzip();
            Rc::new(Self {
                started: RefCell::new(Vec::new()),
                gates: RefCell::new(gates),
                keys: RefCell::new(keys),
            })
        }

        fn map(
            self: &Rc<Self>,
        ) -> impl Fn(u32) -> std::pin::Pin<Box<dyn Future<Output = u32>>> + use<> {
            let g = Rc::clone(self);
            move |x| {
                g.started.borrow_mut().push(x);
                let gate = g.gates.borrow_mut().remove(&x).expect("one claim per item");
                Box::pin(async move {
                    let _ = gate.await;
                    x
                })
            }
        }

        fn open(&self, x: u32) {
            let key = self.keys.borrow_mut().remove(&x).expect("gate opened once");
            let _ = key.send(());
        }

        fn started(&self) -> Vec<u32> {
            self.started.borrow().clone()
        }
    }

    fn drain<T: Send + Unpin + 'static + Ord>(rx: &AsyncReceiver<T>) -> Vec<T> {
        let mut v: Vec<T> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        v.sort_unstable();
        v
    }

    /// Spawn one driver over `rx` → `out` on `pool`; the flag flips when
    /// `drive` returns.
    fn spawn_driver(
        pool: &LocalPool,
        rx: AsyncReceiver<u32>,
        out: crate::handoff::AsyncSender<u32>,
        g: &Rc<Gated>,
        cap: usize,
        cancel: Option<CancellationToken>,
    ) -> Rc<Cell<bool>> {
        let done = Rc::new(Cell::new(false));
        let flag = Rc::clone(&done);
        let map = g.map();
        pool.spawner()
            .spawn_local(async move {
                drive(
                    RxIntake::new(rx),
                    map,
                    TxOutlet::new(out),
                    cap,
                    cancel.as_ref(),
                )
                .await;
                flag.set(true);
            })
            .unwrap();
        done
    }

    /// The cap bounds the in-flight set; a completion's freed slot refills;
    /// an arrival is claimed while an earlier item is still in flight; EOF
    /// returns only after the in-flight items are delivered.
    #[test]
    fn drive_fills_cap_and_claims_while_busy() {
        let (tx, rx) = async_channel::<u32>(16);
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(6);
        let mut pool = LocalPool::new();
        let done = spawn_driver(&pool, rx, otx, &g, 3, None);
        for x in 0..5 {
            tx.try_send(x).unwrap();
        }
        pool.run_until_stalled();
        assert_eq!(g.started(), [0, 1, 2]);

        g.open(1);
        pool.run_until_stalled();
        assert_eq!(drain(&orx), [1]);
        assert_eq!(g.started(), [0, 1, 2, 3]);

        for x in [0, 2, 3] {
            g.open(x);
        }
        pool.run_until_stalled();
        assert_eq!(drain(&orx), [0, 2, 3]);
        assert_eq!(g.started(), [0, 1, 2, 3, 4]);

        // Item 4 still in flight, input empty: the driver parks on both.
        tx.try_send(5).unwrap();
        pool.run_until_stalled();
        assert_eq!(g.started(), [0, 1, 2, 3, 4, 5]);
        assert!(!done.get());

        drop(tx);
        pool.run_until_stalled();
        assert!(!done.get(), "EOF must wait for the in-flight items");
        g.open(4);
        g.open(5);
        pool.run_until_stalled();
        assert_eq!(drain(&orx), [4, 5]);
        assert!(done.get());
    }

    /// The inline slot and the set share the cap: items whose futures are
    /// ready on their first poll (gate opened before the claim) complete in
    /// passing, pending ones hold slots, and a freed slot admits the next
    /// item.
    #[test]
    fn drive_mixes_ready_and_pending_items_under_the_cap() {
        let (tx, rx) = async_channel::<u32>(16);
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(6);
        for x in [1, 2, 3, 5] {
            g.open(x);
        }
        let mut pool = LocalPool::new();
        let done = spawn_driver(&pool, rx, otx, &g, 2, None);
        for x in 0..6 {
            tx.try_send(x).unwrap();
        }
        pool.run_until_stalled();
        // 0 pends in the slot, 1..=3 pass through, 4 pends: cap 2 reached.
        assert_eq!(g.started(), [0, 1, 2, 3, 4]);
        assert_eq!(drain(&orx), [1, 2, 3]);

        drop(tx);
        g.open(0);
        pool.run_until_stalled();
        assert_eq!(drain(&orx), [0, 5]);
        assert!(!done.get());
        g.open(4);
        pool.run_until_stalled();
        assert_eq!(drain(&orx), [4]);
        assert!(done.get());
    }

    /// Cancel observed on a claim: that item is dropped unmapped, nothing
    /// further is claimed, and every item accepted before it is delivered.
    #[test]
    fn drive_cancel_delivers_accepted_items_only() {
        let (tx, rx) = async_channel::<u32>(16);
        let probe = rx.clone();
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(8);
        let mut pool = LocalPool::new();
        let token = CancellationToken::new();
        let done = spawn_driver(&pool, rx, otx, &g, 4, Some(token.clone()));
        for x in 0..8 {
            tx.try_send(x).unwrap();
        }
        pool.run_until_stalled();
        assert_eq!(g.started(), [0, 1, 2, 3]);

        token.cancel();
        g.open(0);
        pool.run_until_stalled();
        assert_eq!(g.started(), [0, 1, 2, 3], "item 4 observed the token");
        for x in 1..4 {
            g.open(x);
        }
        pool.run_until_stalled();
        assert!(done.get());
        assert_eq!(drain(&orx), [0, 1, 2, 3]);
        assert_eq!(drain(&probe), [5, 6, 7], "exactly one claim after cancel");
    }

    /// A closed downstream ends the driver at the first failed delivery,
    /// dropping the items still in flight.
    #[test]
    fn drive_exits_on_downstream_close() {
        let (tx, rx) = async_channel::<u32>(16);
        let (otx, orx) = async_channel::<u32>(16);
        drop(orx);
        let g = Gated::new(2);
        let mut pool = LocalPool::new();
        let done = spawn_driver(&pool, rx, otx, &g, 2, None);
        tx.try_send(0).unwrap();
        tx.try_send(1).unwrap();
        pool.run_until_stalled();
        assert_eq!(g.started(), [0, 1]);
        g.open(0);
        pool.run_until_stalled();
        assert!(done.get());
        assert!(
            g.keys.borrow()[&1].is_canceled(),
            "in-flight item dropped with the driver"
        );
    }

    /// The driver caps always sum to the stage's concurrency (the
    /// `io_concurrency` bound) and differ by at most one, under any knob
    /// policy.
    #[test]
    fn mux_plan_splits_concurrency_exactly() {
        for concurrency in [1, 2, 3, 31, 32, 33, 100, 4096] {
            for parallelism in [1, 4, 32] {
                let plan = MuxPlan::new(concurrency, parallelism);
                let caps: Vec<usize> = (0..plan.drivers()).map(|i| plan.cap(i)).collect();
                assert!((1..=concurrency).contains(&plan.drivers()));
                assert_eq!(caps.iter().sum::<usize>(), concurrency);
                let (lo, hi) = (caps.iter().min().unwrap(), caps.iter().max().unwrap());
                assert!(*lo >= 1 && hi - lo <= 1, "{caps:?}");
            }
        }
    }
}
