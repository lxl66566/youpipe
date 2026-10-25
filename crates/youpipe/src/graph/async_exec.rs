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
use crate::{
    builder::{PanicSlot, cancel_active, record_panic},
    handoff::{
        AsyncReceiver, AsyncSender, Receiver as SyncReceiver, TryRecvError, WakerSlot, channel,
    },
    sync::CancellationToken,
};

/// Fair fan-in over the shared async receivers of one node's input edges,
/// owned by ONE task of an async node (each task clones every input — the
/// same MPMC-competition shape as the sync [`FanIn`](super::fanin::FanIn),
/// with an any-of-N awaited anchor instead of a blocking one). EOF is
/// every input disconnected.
pub(crate) struct AsyncFanIn<T: Send + Unpin + 'static> {
    rxs: Vec<AsyncReceiver<T>>,
    /// Closed inputs are never polled again.
    closed: Vec<bool>,
    /// Per-input waker-node slots: the anchor parks on one input per burst
    /// gap; a slot lets that input's waker node be re-armed in place instead
    /// of reallocated per episode.
    slots: Vec<WakerSlot>,
    /// Round-robin start index; rotated by every round and anchor.
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

    /// Fair claim: one non-blocking round over every open input, then an
    /// awaited anchor parked on every open input at once (see
    /// [`AsyncFanIn::anchor`]). `None` means EOF.
    pub(crate) async fn claim(&mut self) -> Option<T> {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            match self.rxs[i].try_recv() {
                Ok(item) => {
                    self.cursor = (i + 1) % n;
                    return Some(item);
                },
                Err(TryRecvError::Closed) => {
                    self.closed[i] = true;
                    self.open -= 1;
                },
                Err(TryRecvError::Empty) => {},
            }
        }
        if self.open == 0 {
            return None;
        }
        self.anchor().await
    }

    /// Awaited anchor: park the task registered on every open input at
    /// once — `futures::future::select_all` over the per-input
    /// `recv_cached` futures; polling each to `Pending` registers this
    /// task's waker with that input, so an arrival or close on ANY input
    /// wakes the task. The first ready input wins and the cursor rotates
    /// past it; a future resolving `Err(Closed)` marks its input and the
    /// anchor re-arms over the survivors. `None` is EOF.
    ///
    /// Why every input, not one: the same DAG liveness hole as the sync
    /// [`FanIn::anchor`](super::fanin::FanIn::anchor) — the awaited input's
    /// producer chain can be blocked behind a ring only this node drains,
    /// so awaiting one input can deadlock the run (todo #14's graph-side
    /// shape). The one-input anchor stays sound for the sharded terminal,
    /// whose shards all hang off one producer population in a linear
    /// chain.
    ///
    /// Cost: one select cycle drops the k−1 futures that did not win, and
    /// each dropped pending future abandons (unregisters) its waker node —
    /// the next anchor re-allocates those nodes. That per-park churn is
    /// the price of the any-of-N registration; the winner's slot stays
    /// cached for reuse (P-4).
    async fn anchor(&mut self) -> Option<T> {
        if self.open == 1 {
            return self.anchor_single().await;
        }
        loop {
            if self.open == 0 {
                return None;
            }
            let n = self.rxs.len();
            // Registration order follows the cursor rotation so the first
            // ready future in poll order is the round's fair pick. The
            // per-input slots move out of `iter_mut` (disjoint borrows); the
            // rotation shuffles (ring, slot) pairs, not raw indices.
            let closed = &self.closed;
            let rxs = &self.rxs;
            let mut pairs: Vec<_> = self.slots.iter_mut().enumerate().collect();
            pairs.rotate_left(self.cursor);
            let mut order = Vec::with_capacity(self.open);
            let mut futs = Vec::with_capacity(self.open);
            for (i, slot) in pairs {
                if closed[i] {
                    continue;
                }
                order.push(i);
                futs.push(rxs[i].recv_cached_unpin(slot));
            }
            // The combinator is consumed by the await; the losing futures
            // drop inside it, ending the slot borrows before the marks
            // below.
            let (res, slot) = FirstReady { futs }.await;
            let ring = order[slot];
            // RecvError: senders dropped + ring drained.
            if let Ok(item) = res {
                self.cursor = (ring + 1) % n;
                return Some(item);
            }
            self.closed[ring] = true;
            self.open -= 1;
        }
    }

    /// Single open input: await it directly — the linear-chain park, sound
    /// for the same reason as the sync `anchor_single` (the sole live
    /// producer chain must eventually send or close; no other ring needs
    /// this task). Skips the any-of-N combinator and its Vec churn for the
    /// linear-chain majority.
    async fn anchor_single(&mut self) -> Option<T> {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            // RecvError: senders dropped + ring drained.
            if let Ok(item) = self.rxs[i].recv_cached_unpin(&mut self.slots[i]).await {
                self.cursor = (i + 1) % n;
                return Some(item);
            }
            self.closed[i] = true;
            self.open -= 1;
        }
        None
    }
}

/// Poll a set of futures in order and resolve with the first `Ready`
/// (output + index); the losing futures are dropped. On `Pending` every
/// future has been polled, so each one's waker registration is in place —
/// the any-of-N wait [`AsyncFanIn::anchor`] parks on. A private stand-in
/// for `futures::future::select_all`: the `futures` crate is a
/// dev-dependency by design and this needs none of its machinery.
struct FirstReady<F> {
    futs: Vec<F>,
}

impl<F: Future + Unpin> Future for FirstReady<F> {
    type Output = (F::Output, usize);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let futs = &mut self.futs;
        for (i, f) in futs.iter_mut().enumerate() {
            if let Poll::Ready(out) = Pin::new(f).poll(cx) {
                return Poll::Ready((out, i));
            }
        }
        Poll::Pending
    }
}

/// One async producer task's output-side delivery strategy — the async twin
/// of [`Delivery`](super::fanin::Delivery). Task bodies are generic over it;
/// the RPITIT `+ Send` bound carries through the spawned task futures.
pub(crate) trait AsyncDelivery<T: Send + Unpin + 'static>: Send + 'static {
    /// Build one task's handle from the node's output senders (each task
    /// receives its own clone of every sender).
    fn new(txs: Vec<AsyncSender<T>>) -> Self;
    /// Deliver one item. Returns `false` when every edge has closed (the
    /// caller's task exits).
    fn send(&mut self, item: T) -> impl Future<Output = bool> + Send;
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

    async fn send(&mut self, item: T) -> bool {
        self.tx.send_cached(item, &mut self.slot).await.is_ok()
    }
}

/// Broadcast fan-out over one async node's output edges, owned by ONE task.
/// Every open edge receives a clone; the last surviving edge gets the owned
/// item. A closed edge (send resolves `Err`) drops out of the active set —
/// one dead downstream must not kill the node's other branches.
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

    /// Deliver one item to every open edge. Returns `false` when every edge
    /// has closed (the caller's task exits).
    async fn send(&mut self, item: T) -> bool {
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

/// One async node task: fair fan-in claim, run the future, deliver the
/// output through the node's [`AsyncDelivery`] strategy. The tasks of one
/// node share the closure behind one `Arc` (call syntax auto-derefs it).
async fn async_node_task<I, O, F, Fut, D>(
    f: Arc<F>,
    mut fanin: AsyncFanIn<I>,
    mut out: D,
    cancel: CancellationToken,
) where
    I: Send + Unpin + 'static,
    O: Send + Unpin + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O>,
    D: AsyncDelivery<O>,
{
    loop {
        let Some(item) = fanin.claim().await else {
            return; // EOF: every input disconnected
        };
        if cancel_active(Some(&cancel)) {
            return;
        }
        let output = f(item).await;
        if !out.send(output).await {
            return; // every output edge closed
        }
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
                if !out.send(item).await {
                    return;
                }
            },
            None => return,
        }
    }
}

/// Forward an async upstream edge into the sync channel a sync downstream
/// consumes: a dedicated OS thread runs `block_on` over the async receiver
/// and pushes through a blocking `SyncSender` (the same shape as the stream
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
    let cancel = core.cancel.clone();
    let forward: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
        let mut rx_slot = WakerSlot::new();
        while let Ok(item) = a_rx.recv_cached(&mut rx_slot).await {
            if cancel_active(Some(&cancel)) {
                return;
            }
            if s_tx.send(item).is_err() {
                return;
            }
        }
    });
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
/// `tasks` runtime tasks sharing the input receivers and output senders,
/// delivering through `D`.
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
    Box::new(move |core: &GraphCore, tasks: usize, inputs, outputs| {
        let rxs = downcast_endpoints::<AsyncReceiver<I>>(inputs, "inputs", &name);
        let txs = downcast_endpoints::<AsyncSender<O>>(outputs, "outputs", &name);
        let f = Arc::new(f);
        for _ in 0..tasks {
            let f = Arc::clone(&f);
            let fanin = AsyncFanIn::new(rxs.clone());
            let out = D::new(txs.clone());
            let cancel = core.cancel.clone();
            core.spawn_task(async move {
                async_node_task(f, fanin, out, cancel).await;
            });
        }
    })
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
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::*;
    use crate::handoff::async_channel;

    /// Async twin of the sync anchor liveness regression (todo #14): a task
    /// anchored while one input is empty must still claim from the OTHER
    /// input's arrivals. Ring A stays silent (its producer chain "blocked
    /// upstream"); items land in ring B only — an anchor that only wakes
    /// for its own input's events deadlocks the run.
    #[test]
    fn anchored_async_task_claims_from_other_input() {
        let (tx_a, rx_a) = async_channel::<u32>(4);
        let (tx_b, rx_b) = async_channel::<u32>(4);
        let mut fan = AsyncFanIn::new(vec![rx_a, rx_b]);
        let claimed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&claimed);
        let task = std::thread::spawn(move || {
            futures::executor::block_on(async {
                while let Some(_item) = fan.claim().await {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        });
        // Let the task park: both rings empty → the anchor awaits ring A.
        std::thread::sleep(Duration::from_millis(100));
        for v in 0..4u32 {
            futures::executor::block_on(async { tx_b.send(v).await })
                .expect("ring B has capacity 4");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while claimed.load(Ordering::SeqCst) < 4 {
            assert!(
                Instant::now() < deadline,
                "anchored async task never claimed the other input's items"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // EOF: close both rings, the task exits.
        drop(tx_a);
        drop(tx_b);
        task.join().unwrap();
        assert_eq!(claimed.load(Ordering::SeqCst), 4);
    }
}
