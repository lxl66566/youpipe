//! Async execution for the typed DAG engine: async-node task bodies, the
//! async source task, the async fan-in / delivery helpers, the
//! panic-catching future wrapper and the async-to-sync bridge.
//!
//! Every task's panic is caught at the poll boundary by [`CatchPanic`]
//! (record first-wins + cancel), and task completion feeds the run's
//! worker counter via `GraphCore::spawn_task` — the same lifecycle rules the
//! sync engine's job wrapper enforces.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::{
    exec::{GraphCore, downcast_endpoints},
    plan::SpawnStep,
};
// The async-to-sync bridge forward loop (the graph bridge body).
use crate::handoff::forward_async_to_sync;
use crate::{
    builder::{PanicSlot, record_panic},
    handoff::{
        AsyncReceiver, AsyncSender, Receiver as SyncReceiver, TryRecvError, WakerSlot,
        cancel_active, channel,
        mux::{self, AsyncIntake, AsyncOutlet, TryClaim},
    },
    sync::CancellationToken,
};

/// Fair fan-in over the shared async receivers of one node's input edges,
/// owned by ONE driver task of an async node (each driver clones every
/// input — the same MPMC-competition shape as the sync
/// [`FanIn`](super::fanin::FanIn), with a poll-based any-of-N claim instead
/// of a blocking one). EOF is every input disconnected. The driver's intake
/// ([`AsyncIntake`]): a poll that returns `Pending` leaves the cursor and
/// the closed marks untouched (they change only after an input resolves).
pub(crate) struct AsyncFanIn<T: Send + Unpin + 'static> {
    rxs: Vec<AsyncReceiver<T>>,
    /// Closed inputs are never polled again.
    closed: Vec<bool>,
    /// Per-input waker-node slots: a parked claim poll re-arms the node in
    /// place instead of allocating one per episode.
    slots: Vec<WakerSlot>,
    /// Round-robin start index; rotated by every round and claim.
    cursor: usize,
    /// Open input count; the EOF signal is `open == 0`.
    open: usize,
}

impl<T: Send + Unpin + 'static> AsyncFanIn<T> {
    pub(crate) fn new(rxs: Vec<AsyncReceiver<T>>) -> Self {
        let n = rxs.len();
        Self {
            closed: vec![false; n],
            slots: (0..n).map(|_| WakerSlot::new()).collect(),
            rxs,
            cursor: 0,
            open: n,
        }
    }

    /// One non-blocking round over every open input, starting at the
    /// round-robin cursor.
    fn round(&mut self) -> TryClaim<T> {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            match self.rxs[i].try_recv() {
                Ok(item) => {
                    self.cursor = (i + 1) % n;
                    return TryClaim::Item(item);
                },
                Err(TryRecvError::Closed) => {
                    self.closed[i] = true;
                    self.open -= 1;
                },
                Err(TryRecvError::Empty) => {},
            }
        }
        if self.open == 0 {
            TryClaim::Closed
        } else {
            TryClaim::Empty
        }
    }
}

impl<T: Send + Unpin + 'static> AsyncIntake<T> for AsyncFanIn<T> {
    fn try_claim(&mut self) -> TryClaim<T> {
        self.round()
    }

    /// Fair claim in two phases: the non-blocking [`round`](Self::round)
    /// first — it resolves a ready item without touching any waker slot —
    /// then a cursor-ordered poll of every open input's cached recv future;
    /// on `Pending` the task's waker is registered with every open input,
    /// so an arrival or close on ANY input wakes it. The first ready input
    /// in cursor order wins (the round's fair pick); a poll resolving
    /// `Err(Closed)` marks its input and the same scan continues over the
    /// survivors; `Ready(None)` is EOF.
    ///
    /// Why every input, not one: the same DAG liveness hole as the sync
    /// [`FanIn::anchor`](super::fanin::FanIn::anchor) — the awaited input's
    /// producer chain can be blocked behind a ring only this node drains,
    /// so awaiting one input can deadlock the run (todo #14's graph-side
    /// shape). The one-input case needs no special path: it is this loop's
    /// natural degeneration (every other input is closed-skipped).
    ///
    /// The scan polls each open input by index (`rxs` and `slots` are
    /// disjoint fields — no collected-and-rotated pairs, no per-episode
    /// Vecs); on `Pending` each slot's waker node stays registered with its
    /// ring (slot-owned, no future held between polls), and a re-poll
    /// re-validates in place — one state check per input, no registry
    /// mutex. What this replaced: a per-episode any-of-N combinator over
    /// three fresh Vecs (`pairs`/`order`/`futs`) rebuilt and torn down on
    /// every driver wake, each teardown abandoning and re-registering every
    /// node (todo #32 measured that registration as the episode's dominant
    /// cost — with slot-owned registration it is paid once, not per
    /// completion).
    fn poll_claim(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.round() {
            TryClaim::Item(item) => return Poll::Ready(Some(item)),
            TryClaim::Closed => return Poll::Ready(None),
            TryClaim::Empty => {},
        }
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            match self.rxs[i].poll_recv_cached(cx, &mut self.slots[i]) {
                Poll::Ready(Ok(item)) => {
                    self.cursor = (i + 1) % n;
                    return Poll::Ready(Some(item));
                },
                // RecvError: senders dropped + ring drained.
                Poll::Ready(Err(_)) => {
                    self.closed[i] = true;
                    self.open -= 1;
                },
                Poll::Pending => {},
            }
        }
        if self.open == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// One async producer task's output-side delivery strategy — the async twin
/// of the sync `Delivery` trait (see `handoff::worker`): the driver's
/// [`AsyncOutlet`] plus its per-task constructor. Task bodies are generic
/// over it; the RPITIT `+ Send` bound carries through the spawned task
/// futures.
pub(crate) trait AsyncDelivery<T: Send + Unpin + 'static>: AsyncOutlet<T> + 'static {
    /// Build one task's handle from the node's output senders (each task
    /// receives its own clone of every sender).
    fn new(txs: Vec<AsyncSender<T>>) -> Self;
}

/// Single-edge delivery: the async counterpart of [`AsyncBroadcast`] for
/// producers registered without a `_broadcast` variant. Each item moves
/// into the node's one outgoing edge — no clones, no `Clone` bound. The
/// out-degree-1 invariant is validated at `run()` before any task spawns.
pub(crate) struct AsyncUnicast<T: Send + Unpin + 'static> {
    tx: AsyncSender<T>,
    slot: WakerSlot,
}

impl<T: Send + Unpin + 'static> AsyncDelivery<T> for AsyncUnicast<T> {
    fn new(txs: Vec<AsyncSender<T>>) -> Self {
        let [tx]: [AsyncSender<T>; 1] = txs
            .try_into()
            .ok()
            .expect("move producer has exactly one outgoing edge (validated)");
        Self {
            tx,
            slot: WakerSlot::new(),
        }
    }
}

impl<T: Send + Unpin + 'static> AsyncOutlet<T> for AsyncUnicast<T> {
    async fn deliver(&mut self, item: T) -> bool {
        self.tx.send_cached(item, &mut self.slot).await.is_ok()
    }
}

/// Broadcast fan-out over one async node's output edges, owned by ONE task.
/// Every open edge receives a clone; the last surviving edge gets the owned
/// item. A closed edge (send resolves `Err`) drops out of the active set —
/// one dead downstream must not kill the node's other branches.
///
/// Ordering scope: "every edge sees the same sequence" holds per driver
/// task, not per node — with `io_concurrency > 1` the drivers interleave
/// their per-item sends, so two edges can take the same items in different
/// orders (each edge stays a FIFO). Cross-edge order-sensitive consumers
/// need `io_concurrency(1)` (one driver, one item in flight); mid-edge holes
/// are never produced (an accepted item reaches every open edge).
pub(crate) struct AsyncBroadcast<T: Send + Unpin + 'static> {
    txs: Vec<AsyncSender<T>>,
    /// Per-edge waker-node slots (see [`AsyncFanIn`]).
    slots: Vec<WakerSlot>,
}

impl<T: Send + Clone + Unpin + 'static> AsyncDelivery<T> for AsyncBroadcast<T> {
    fn new(txs: Vec<AsyncSender<T>>) -> Self {
        let slots = (0..txs.len()).map(|_| WakerSlot::new()).collect();
        Self { txs, slots }
    }
}

impl<T: Send + Clone + Unpin + 'static> AsyncOutlet<T> for AsyncBroadcast<T> {
    /// Deliver one item to every open edge. Returns `false` when every edge
    /// has closed (the caller's task exits).
    async fn deliver(&mut self, item: T) -> bool {
        let mut i = 0;
        while i + 1 < self.txs.len() {
            if self.txs[i]
                .send_cached(item.clone(), &mut self.slots[i])
                .await
                .is_err()
            {
                self.txs.swap_remove(i);
                self.slots.swap_remove(i);
            } else {
                i += 1;
            }
        }
        if self.txs.is_empty() {
            return false;
        }
        let last = self.txs.len() - 1;
        if self.txs[last]
            .send_cached(item, &mut self.slots[last])
            .await
            .is_err()
        {
            self.txs.swap_remove(last);
            self.slots.swap_remove(last);
        }
        !self.txs.is_empty()
    }
}

/// The async source task: call `f` until `None` (EOF), delivering each item
/// through the source's [`AsyncDelivery`] strategy.
async fn async_source_task<T, F, Fut, D>(f: F, mut out: D, cancel: CancellationToken)
where
    T: Send + Unpin + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<T>>,
    D: AsyncDelivery<T>,
{
    loop {
        if cancel_active(Some(&cancel)) {
            return;
        }
        match f().await {
            Some(item) => {
                if !out.deliver(item).await {
                    return;
                }
            },
            None => return,
        }
    }
}

/// Forward an async upstream edge into the sync channel a sync downstream
/// consumes: a dedicated OS thread runs `block_on` over
/// [`forward_async_to_sync`] (the shared loop also driven by the stream
/// engine's `bridge_async_to_sync` — a blocking send must never run inside a
/// runtime task).
///
/// Bridge threads are always dedicated (never pool jobs), so they consume no
/// parking-lease slot; each exits when its upstream closes (its sender drop
/// is the downstream's EOF), the downstream closes, or the run cancels.
pub(crate) fn bridge<T>(core: &GraphCore, a_rx: AsyncReceiver<T>, buffer: usize) -> SyncReceiver<T>
where
    T: Send + Unpin + 'static,
{
    let (s_tx, s_rx) = channel::<T>(buffer);
    let spawner = core
        .spawner
        .as_ref()
        .expect("async-to-sync bridge requires the async spawner");
    let forward: Pin<Box<dyn Future<Output = ()> + Send>> =
        Box::pin(forward_async_to_sync(a_rx, s_tx, Some(core.cancel.clone())));
    let driver = Arc::clone(spawner);
    std::thread::spawn(move || driver.block_on(forward));
    s_rx
}

/// Type-erased spawn step for one async source (see
/// `GraphBuilder::source_async`): a single runtime task, no inputs,
/// delivering through `D`.
pub(crate) fn source_async_spawn<T, F, Fut, D>(f: F) -> SpawnStep
where
    T: Send + Unpin + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<T>> + Send,
    D: AsyncDelivery<T>,
{
    Box::new(move |core: &GraphCore, _workers: usize, _inputs, outputs| {
        let txs = downcast_endpoints::<AsyncSender<T>>(outputs, "outputs", "async source");
        let cancel = core.cancel.clone();
        core.spawn_task(async move {
            async_source_task(f, D::new(txs), cancel).await;
        });
    })
}

/// Type-erased spawn step for one async IO node (see `GraphBuilder::node_async`):
/// the node's `concurrency` in-flight slots multiplexed over the driver
/// tasks of [`GraphCore::mux_plan`] (`handoff::mux`), every driver sharing
/// the input receivers and output senders and delivering through `D`.
// Called only from `node_async`, which requires the `tokio-runtime` feature.
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) fn node_async_spawn<I, O, F, Fut, D>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + Unpin + 'static,
    O: Send + Unpin + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send,
    D: AsyncDelivery<O>,
{
    Box::new(
        move |core: &GraphCore, concurrency: usize, inputs, outputs| {
            let rxs = downcast_endpoints::<AsyncReceiver<I>>(inputs, "inputs", &name);
            let txs = downcast_endpoints::<AsyncSender<O>>(outputs, "outputs", &name);
            let plan = core.mux_plan(concurrency);
            let f = Arc::new(f);
            for i in 0..plan.drivers() {
                let f = Arc::clone(&f);
                let fanin = AsyncFanIn::new(rxs.clone());
                let out = D::new(txs.clone());
                let cancel = core.cancel.clone();
                let cap = plan.cap(i);
                core.spawn_task(async move {
                    mux::drive(fanin, |item| f(item), out, cap, Some(&cancel)).await;
                });
            }
        },
    )
}

/// Wrap one async task body so a panic inside any of its polls is recorded
/// (first wins) and the cancel token fires, instead of being discarded by
/// the runtime with the unobserved JoinHandle — the graph counterpart of
/// the stream engine's `CatchTaskPanic`. `GraphCore::spawn_task` appends the
/// completion accounting after this wrapper resolves.
///
/// No settle gate needed here (unlike `CatchTaskPanic`, todo #23): the
/// unwind drops the task's senders before the payload reaches this catch,
/// but the graph drain waits on `GraphCore::done` — decremented only after
/// this wrapper resolves, i.e. strictly after the record — never on
/// channel-close EOF, so the swallow race cannot manifest.
pub(crate) struct CatchPanic<F> {
    fut: F,
    slot: PanicSlot,
    cancel: CancellationToken,
}

impl<F: Future<Output = ()>> Future for CatchPanic<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `fut` is structurally pinned — never moved out of this
        // wrapper; the other fields have no pinned-field sensitivity.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: pinned projection of `fut` per its structural pinning.
        let fut = unsafe { Pin::new_unchecked(&mut this.fut) };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.poll(cx))) {
            Ok(poll) => poll,
            Err(payload) => {
                record_panic(&this.slot, payload);
                this.cancel.cancel();
                Poll::Ready(())
            },
        }
    }
}

/// Free-function constructor for [`CatchPanic`] (keeps the spawn site in
/// `exec.rs` reading as a plain call).
pub(crate) fn catch_panic<F>(fut: F, slot: PanicSlot, cancel: CancellationToken) -> CatchPanic<F>
where
    F: Future<Output = ()>,
{
    CatchPanic { fut, slot, cancel }
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::*;
    use crate::handoff::async_channel;

    /// Async twin of the sync anchor liveness regression (todo #14): a task
    /// parked while one input is empty must still claim from the OTHER
    /// input's arrivals. Ring A stays silent (its producer chain "blocked
    /// upstream"); items land in ring B only — a claim that only wakes for
    /// its own input's events deadlocks the run.
    #[test]
    fn anchored_async_task_claims_from_other_input() {
        let (tx_a, rx_a) = async_channel::<u32>(4);
        let (tx_b, rx_b) = async_channel::<u32>(4);
        let mut fan = AsyncFanIn::new(vec![rx_a, rx_b]);
        let claimed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&claimed);
        let task = std::thread::spawn(move || {
            futures::executor::block_on(async {
                while let Some(_item) = poll_fn(|cx| fan.poll_claim(cx)).await {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        });
        // Let the task park: both rings empty → the claim registers on both.
        std::thread::sleep(Duration::from_millis(100));
        for v in 0..4u32 {
            futures::executor::block_on(async { tx_b.send(v).await })
                .expect("ring B has capacity 4");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while claimed.load(Ordering::SeqCst) < 4 {
            assert!(
                Instant::now() < deadline,
                "parked async task never claimed the other input's items"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // EOF: close both rings, the task exits.
        drop(tx_a);
        drop(tx_b);
        task.join().unwrap();
        assert_eq!(claimed.load(Ordering::SeqCst), 4);
    }

    /// `poll_claim`'s scan contract without a spawned task: an empty pass
    /// parks armed on every open input (Pending, state untouched), a close
    /// observed mid-scan marks its input and keeps the scan armed over the
    /// survivors (Pending, not EOF), an arrival resolves Ready, and the last
    /// close resolves EOF.
    #[test]
    fn fanin_poll_claim_marks_closes_and_claims_any_input() {
        let (tx_a, rx_a) = async_channel::<u32>(4);
        let (tx_b, rx_b) = async_channel::<u32>(4);
        let mut fan = AsyncFanIn::new(vec![rx_a, rx_b]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert_eq!(fan.poll_claim(&mut cx), Poll::Pending);

        tx_b.try_send(1).unwrap();
        assert_eq!(fan.poll_claim(&mut cx), Poll::Ready(Some(1)));

        // Close A while B is open: the scan marks A and still parks armed
        // on B.
        drop(tx_a);
        assert_eq!(fan.poll_claim(&mut cx), Poll::Pending);

        tx_b.try_send(2).unwrap();
        assert_eq!(fan.poll_claim(&mut cx), Poll::Ready(Some(2)));

        drop(tx_b);
        assert_eq!(fan.poll_claim(&mut cx), Poll::Ready(None));
    }
}
