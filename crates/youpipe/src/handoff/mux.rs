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
//!
//! `YOUPIPE_ASYNC_OUTLET_BATCH` (default on) batches each driver's outlet
//! delivery — drained completion clusters go out per edge through
//! [`AsyncOutlet::deliver_batch`] instead of one awaited send per item (see
//! `handoff`'s knob table for the hop distinction).

use std::{
    future::{Future, poll_fn},
    mem::MaybeUninit,
    pin::pin,
    task::{Context, Poll},
};

use futures_util::stream::{FuturesUnordered, StreamExt};

use super::{
    channel::{AsyncReceiver, AsyncSendItem, TryRecvError, WakerSlot, stage_batch},
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
    /// Poll for one item: `Ready(Some)` = claimed, `Ready(None)` = every
    /// input closed and drained (EOF), `Pending` = `cx.waker()` is
    /// registered on every open input. Cancel-safety is structural — a poll
    /// that loses (a completion wins the select first) leaves nothing
    /// behind but the armed waker nodes in the intake's own slots.
    fn poll_claim(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>>;
}

/// One driver's output side.
pub(crate) trait AsyncOutlet<T: Send>: Send {
    /// Deliver one item; `false` once every downstream has closed (the
    /// driver exits).
    fn deliver(&mut self, item: T) -> impl Future<Output = bool> + Send;

    /// Batched delivery (the `YOUPIPE_ASYNC_OUTLET_BATCH` path): deliver
    /// every item of `batch` in order; same `false` contract. `staged` is
    /// caller-owned scratch, `clear`-reused per call. The default is the
    /// per-item [`Self::deliver`] loop — semantically identical, so an
    /// outlet without a native batch form still delivers a batch correctly
    /// (only the sender-run amortization is lost).
    fn deliver_batch(
        &mut self,
        batch: &mut Vec<T>,
        _staged: &mut Vec<MaybeUninit<T>>,
    ) -> impl Future<Output = bool> + Send {
        async move {
            for item in batch.drain(..) {
                if !self.deliver(item).await {
                    return false;
                }
            }
            true
        }
    }
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

    /// Registration-free fast path first (`try_recv`), then one poll of the
    /// cached recv — on `Pending` the slot's waker node stays registered
    /// with the ring (slot-owned, no future held between polls), so an
    /// arrival wakes the parked driver; the next poll re-validates in place.
    fn poll_claim(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.rx.try_recv() {
            Ok(item) => return Poll::Ready(Some(item)),
            Err(TryRecvError::Closed) => return Poll::Ready(None),
            Err(TryRecvError::Empty) => {},
        }
        self.rx.poll_recv_cached(cx, &mut self.slot).map(Result::ok)
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

    async fn deliver_batch(
        &mut self,
        batch: &mut Vec<T>,
        staged: &mut Vec<MaybeUninit<T>>,
    ) -> bool {
        stage_batch(batch, staged);
        self.tx.flush_staged(staged, &mut self.slot).await
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
/// `cap == 1` is the sequential loop, without an in-flight set. Delivery is
/// per-item unless the `YOUPIPE_ASYNC_OUTLET_BATCH` knob arms the batched
/// outlet ([`AsyncOutlet::deliver_batch`]); see `drive_inner`.
pub(crate) async fn drive<T, O, Fut>(
    intake: impl AsyncIntake<T>,
    map: impl Fn(T) -> Fut,
    out: impl AsyncOutlet<O>,
    cap: usize,
    cancel: Option<&CancellationToken>,
) where
    T: Send,
    O: Send,
    Fut: Future<Output = O>,
{
    drive_inner(
        intake,
        map,
        out,
        cap,
        super::async_outlet_batch_cap(),
        cancel,
    )
    .await;
}

/// [`drive`] with an explicit outlet batch cap (0 = per-item deliveries);
/// the knob-free core so tests pin the batched semantics without the
/// process-global env read.
async fn drive_inner<T, O, Fut>(
    mut intake: impl AsyncIntake<T>,
    map: impl Fn(T) -> Fut,
    mut out: impl AsyncOutlet<O>,
    cap: usize,
    batch_cap: usize,
    cancel: Option<&CancellationToken>,
) where
    T: Send,
    O: Send,
    Fut: Future<Output = O>,
{
    if cap <= 1 {
        while let Some(item) = poll_fn(|cx| intake.poll_claim(cx)).await {
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
    // Batched-outlet scratch: zero capacity when the batch cap is 0, so the
    // per-item arms below reduce to the historical per-item loop.
    let mut batch: Vec<O> = Vec::with_capacity(batch_cap);
    let mut staged: Vec<MaybeUninit<O>> = Vec::with_capacity(batch_cap);
    loop {
        let mut in_flight = overflow.len() + usize::from(head.is_some());
        while open && in_flight < cap && (batch_cap == 0 || batch.len() < batch_cap) {
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
                    // The batched arm stages the output (delivery happens
                    // at the flush below): a cheap future that resolves on
                    // its first poll keeps the claim → poll → stage loop
                    // running, so a ready burst shares one sender run.
                    if batch_cap == 0 {
                        if !out.deliver(output).await {
                            return;
                        }
                    } else {
                        batch.push(output);
                    }
                },
                Poll::Pending => in_flight += 1,
            }
        }
        // Never park the select while holding staged outputs (a pending
        // in-flight set must not delay ready items behind a batch
        // boundary), and never return on EOF/cancel with accepted outputs
        // still staged.
        if !batch.is_empty() && !out.deliver_batch(&mut batch, &mut staged).await {
            return;
        }
        let event = if in_flight == 0 {
            if !open {
                return;
            }
            Event::Claimed(poll_fn(|cx| intake.poll_claim(cx)).await)
        } else {
            // The first completion, or an arrival while there is room. The
            // in-flight futures are polled first: completions free slots and
            // keep downstream fed; the intake is polled inline only while
            // there is room — a Pending pass registers on every open input
            // alongside the in-flight set, and a losing poll leaves only
            // armed waker nodes behind (no claim future to drop).
            let room = open && in_flight < cap;
            poll_fn(|cx| {
                if let Some(Poll::Ready(output)) = head.as_mut().as_pin_mut().map(|f| f.poll(cx)) {
                    head.set(None);
                    return Poll::Ready(Event::Done(output));
                }
                if let Poll::Ready(Some(output)) = overflow.poll_next_unpin(cx) {
                    return Poll::Ready(Event::Done(output));
                }
                if room {
                    match intake.poll_claim(cx) {
                        Poll::Ready(item) => return Poll::Ready(Event::Claimed(item)),
                        Poll::Pending => {},
                    }
                }
                Poll::Pending
            })
            .await
        };
        match event {
            Event::Done(output) => {
                if batch_cap == 0 {
                    if !out.deliver(output).await {
                        return;
                    }
                } else {
                    batch.push(output);
                    // Completion drain: one wake commonly resolves several
                    // in-flight futures, so collect the rest of the cluster
                    // before delivering — per-item delivery would re-enter
                    // the whole select per output. The head slot needs no
                    // drain pass: the select polls it first, so its
                    // completion, if any, IS `output`.
                    if batch_cap > 1 {
                        poll_fn(|cx| {
                            while batch.len() < batch_cap {
                                match overflow.poll_next_unpin(cx) {
                                    Poll::Ready(Some(output)) => batch.push(output),
                                    _ => return Poll::Ready(()),
                                }
                            }
                            Poll::Ready(())
                        })
                        .await;
                    }
                    if !out.deliver_batch(&mut batch, &mut staged).await {
                        return;
                    }
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
        pin::Pin,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
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

        fn map(self: &Rc<Self>) -> impl Fn(u32) -> Pin<Box<dyn Future<Output = u32>>> + use<> {
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

    /// Order-observing variant of [`drain`] (delivery-sequence asserts).
    fn drain_seq(rx: &AsyncReceiver<u32>) -> Vec<u32> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    /// Spawn one driver over `rx` → `out` on `pool`; the flag flips when
    /// `drive_inner` returns. `batch_cap` 0 is the knob-off per-item arm.
    fn spawn_driver(
        pool: &LocalPool,
        rx: AsyncReceiver<u32>,
        out: crate::handoff::AsyncSender<u32>,
        g: &Rc<Gated>,
        cap: usize,
        batch_cap: usize,
        cancel: Option<CancellationToken>,
    ) -> Rc<Cell<bool>> {
        let done = Rc::new(Cell::new(false));
        let flag = Rc::clone(&done);
        let map = g.map();
        pool.spawner()
            .spawn_local(async move {
                drive_inner(
                    RxIntake::new(rx),
                    map,
                    TxOutlet::new(out),
                    cap,
                    batch_cap,
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
        for batch_cap in [0, 4] {
            drive_fills_cap_and_claims_while_busy_at(batch_cap);
        }
    }

    fn drive_fills_cap_and_claims_while_busy_at(batch_cap: usize) {
        let (tx, rx) = async_channel::<u32>(16);
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(6);
        let mut pool = LocalPool::new();
        let done = spawn_driver(&pool, rx, otx, &g, 3, batch_cap, None);
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
        for batch_cap in [0, 4] {
            drive_mixes_ready_and_pending_items_at(batch_cap);
        }
    }

    fn drive_mixes_ready_and_pending_items_at(batch_cap: usize) {
        let (tx, rx) = async_channel::<u32>(16);
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(6);
        for x in [1, 2, 3, 5] {
            g.open(x);
        }
        let mut pool = LocalPool::new();
        let done = spawn_driver(&pool, rx, otx, &g, 2, batch_cap, None);
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
        let done = spawn_driver(&pool, rx, otx, &g, 4, 0, Some(token.clone()));
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
        let done = spawn_driver(&pool, rx, otx, &g, 2, 0, None);
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

    /// `poll_claim`'s ready path resolves on the first poll without arming a
    /// waker (the counting waker never fires); the empty path parks armed
    /// (the next send wakes it); the closed path is EOF.
    #[test]
    fn rx_intake_poll_claim_ready_path_is_registration_free() {
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (tx, rx) = async_channel::<u32>(4);
        let mut intake = RxIntake::new(rx);
        let wake_calls = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_calls));
        let mut cx = Context::from_waker(&waker);

        tx.try_send(7).unwrap();
        assert_eq!(intake.poll_claim(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(
            wake_calls.0.load(Ordering::SeqCst),
            0,
            "ready path arms no waker"
        );

        assert_eq!(intake.poll_claim(&mut cx), Poll::Pending);
        assert_eq!(wake_calls.0.load(Ordering::SeqCst), 0, "parked, not woken");

        futures::executor::block_on(tx.send(8)).unwrap();
        assert_eq!(
            wake_calls.0.load(Ordering::SeqCst),
            1,
            "the armed node woke on arrival"
        );
        assert_eq!(intake.poll_claim(&mut cx), Poll::Ready(Some(8)));

        drop(tx);
        assert_eq!(intake.poll_claim(&mut cx), Poll::Ready(None));
        assert_eq!(
            wake_calls.0.load(Ordering::SeqCst),
            1,
            "close re-wakes nothing by itself"
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

    /// The batched outlet emits the IDENTICAL delivery sequence as the
    /// per-item arm under the same gated completion schedule — including
    /// a cluster (gates 0 and 2 opened before one poll) whose batched
    /// drain must keep the per-item arm's completion order (0 before 2:
    /// the FuturesUnordered yield order both arms share).
    #[test]
    fn drive_batched_delivery_matches_per_item_sequence() {
        let run = |batch_cap: usize| -> (Vec<u32>, Vec<u32>) {
            let (tx, rx) = async_channel::<u32>(16);
            let (otx, orx) = async_channel::<u32>(16);
            let g = Gated::new(6);
            let mut pool = LocalPool::new();
            let done = spawn_driver(&pool, rx, otx, &g, 3, batch_cap, None);
            for x in 0..6 {
                tx.try_send(x).unwrap();
            }
            pool.run_until_stalled();
            assert_eq!(g.started(), [0, 1, 2]);
            assert_eq!(drain_seq(&orx), Vec::<u32>::new());

            g.open(1); // single completion
            pool.run_until_stalled();
            assert_eq!(g.started(), [0, 1, 2, 3]);
            assert_eq!(drain_seq(&orx), [1]);

            g.open(0);
            g.open(2); // cluster: both complete before the driver polls
            pool.run_until_stalled();
            // Both arms refill the two slots the cluster freed: item 4
            // into the inline slot (pending), item 5 into the set.
            assert_eq!(g.started(), [0, 1, 2, 3, 4, 5]);
            assert_eq!(drain_seq(&orx), [0, 2]);

            g.open(3);
            g.open(4);
            pool.run_until_stalled();
            // Item 4 holds the inline head slot, which the select polls
            // before the set — its completion precedes item 3's in the
            // same wake cycle, in both arms.
            assert_eq!(g.started(), [0, 1, 2, 3, 4, 5]);
            assert_eq!(drain_seq(&orx), [4, 3]);

            g.open(5);
            drop(tx);
            pool.run_until_stalled();
            assert!(done.get());
            assert_eq!(drain_seq(&orx), [5]);
            (drain(&orx), g.started())
        };
        let per_item = run(0);
        let batched = run(4);
        assert_eq!(per_item, batched);
    }

    /// Cancel observed on a claim mid-batch: the observing item is dropped
    /// unmapped, nothing further is claimed, and every item accepted before
    /// it — including outputs still staged in the pending batch — is still
    /// delivered (the accepted-item contract, `drive_cancel_delivers_
    /// accepted_items_only`'s batched twin).
    #[test]
    fn drive_batched_cancel_delivers_accepted_items_only() {
        let (tx, rx) = async_channel::<u32>(16);
        let probe = rx.clone();
        let (otx, orx) = async_channel::<u32>(16);
        let g = Gated::new(8);
        let mut pool = LocalPool::new();
        let token = CancellationToken::new();
        let done = spawn_driver(&pool, rx, otx, &g, 4, 4, Some(token.clone()));
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

    /// A closed downstream ends the batched driver at the first failed
    /// flush; every staged output runs its destructor exactly once (the
    /// `MaybeUninit` staging must not drop them silently, and the driver
    /// must not re-deliver).
    #[test]
    fn drive_batched_downstream_close_drops_staged_once() {
        struct Counted(Arc<AtomicUsize>);
        impl std::fmt::Debug for Counted {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Counted")
            }
        }
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = async_channel::<Counted>(16);
        let (otx, orx) = async_channel::<Counted>(16);
        drop(orx);
        let mut pool = LocalPool::new();
        let done = {
            let done = Rc::new(Cell::new(false));
            let flag = Rc::clone(&done);
            pool.spawner()
                .spawn_local(async move {
                    drive_inner(
                        RxIntake::new(rx),
                        |c| async move { c },
                        TxOutlet::new(otx),
                        4,
                        4,
                        None,
                    )
                    .await;
                    flag.set(true);
                })
                .unwrap();
            done
        };
        for _ in 0..4 {
            tx.try_send(Counted(Arc::clone(&drops))).unwrap();
        }
        pool.run_until_stalled();
        assert!(done.get());
        assert_eq!(drops.load(Ordering::SeqCst), 4, "each staged output once");
    }

    /// A batch deeper than the outlet ring parks per item on the remainder
    /// (the awaited fallback) without reordering, and a same-pool drainer
    /// keeps the parks bounded — the async twin of the sync
    /// `send_batch_flushes_remainder_per_item`.
    #[test]
    fn drive_batched_flushes_full_ring_remainder_in_order() {
        let (tx, rx) = async_channel::<u32>(8);
        let (otx, orx) = async_channel::<u32>(2); // ring shallower than the batch
        let got = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&got);
        let mut pool = LocalPool::new();
        pool.spawner()
            .spawn_local(async move {
                let mut slot = WakerSlot::new();
                while let Ok(v) = orx.recv_cached(&mut slot).await {
                    sink.borrow_mut().push(v);
                }
            })
            .unwrap();
        let done = {
            let done = Rc::new(Cell::new(false));
            let flag = Rc::clone(&done);
            pool.spawner()
                .spawn_local(async move {
                    drive_inner(
                        RxIntake::new(rx),
                        |v| async move { v },
                        TxOutlet::new(otx),
                        4,
                        8,
                        None,
                    )
                    .await;
                    flag.set(true);
                })
                .unwrap();
            done
        };
        for v in 0..8 {
            tx.try_send(v).unwrap();
        }
        drop(tx);
        pool.run_until_stalled();
        assert!(done.get());
        assert_eq!(*got.borrow(), (0..8).collect::<Vec<u32>>());
    }
}
