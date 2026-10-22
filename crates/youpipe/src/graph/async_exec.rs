//! Async execution for the typed DAG engine: async-node task bodies, the
//! async source task, the async fan-in / broadcast helpers, the
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

use crate::{
    builder::{PanicSlot, cancel_active, record_panic},
    handoff::{
        AsyncReceiver, AsyncSender, Receiver as SyncReceiver, TryRecvError, WakerSlot, channel,
    },
    sync::CancellationToken,
};

use super::{
    exec::{GraphCore, downcast_endpoints},
    plan::SpawnStep,
};

/// Fair fan-in over the shared async receivers of one node's input edges,
/// owned by ONE task of an async node (each task clones every input — the
/// same MPMC-competition shape as the sync [`FanIn`](super::fanin::FanIn),
/// with an awaited anchor instead of a blocking one). EOF is every input
/// disconnected.
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
    /// awaited anchor on the first open one (rotating; an input that closes
    /// while parked is marked and the next open one awaited). `None` means
    /// EOF. Items landing in other inputs while anchored are picked up by
    /// the next round — the liveness argument is the sharded anchor's: every
    /// open input's producer either sends, awaits its own upstream, or drops
    /// its sender.
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
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            if let Ok(item) = self.rxs[i].recv_cached(&mut self.slots[i]).await {
                self.cursor = (i + 1) % n;
                return Some(item);
            }
            // Disconnected while parked: mark it and await the next open one.
            self.closed[i] = true;
            self.open -= 1;
        }
        None
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

impl<T: Send + Clone + Unpin + 'static> AsyncBroadcast<T> {
    pub(crate) fn new(txs: Vec<AsyncSender<T>>) -> Self {
        let slots = (0..txs.len()).map(|_| WakerSlot::new()).collect();
        Self { txs, slots }
    }

    /// Deliver one item to every open edge. Returns `false` when every edge
    /// has closed (the caller's task exits).
    pub(crate) async fn send(&mut self, item: T) -> bool {
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

/// One async node task: fair fan-in claim, run the future, broadcast the
/// output. The tasks of one node share the closure behind one `Arc` (call
/// syntax auto-derefs it).
async fn async_node_task<I, O, F, Fut>(
    f: Arc<F>,
    mut fanin: AsyncFanIn<I>,
    mut out: AsyncBroadcast<O>,
    cancel: CancellationToken,
) where
    I: Send + Unpin + 'static,
    O: Send + Clone + Unpin + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O>,
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

/// The async source task: call `f` until `None` (EOF), broadcasting each
/// item to every open edge.
async fn async_source_task<T, F, Fut>(
    f: F,
    mut out: AsyncBroadcast<T>,
    cancel: CancellationToken,
) where
    T: Send + Clone + Unpin + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<T>>,
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
/// `GraphBuilder::source_async`): a single runtime task, no inputs.
pub(crate) fn source_async_spawn<T, F, Fut>(f: F) -> SpawnStep
where
    T: Send + Clone + Unpin + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<T>> + Send,
{
    Box::new(move |core: &GraphCore, _workers: usize, _inputs, outputs| {
        let txs = downcast_endpoints::<AsyncSender<T>>(outputs, "outputs", "async source");
        let cancel = core.cancel.clone();
        core.spawn_task(async move {
            async_source_task(f, AsyncBroadcast::new(txs), cancel).await;
        });
    })
}

/// Type-erased spawn step for one async IO node (see
/// `GraphBuilder::node_async`): `tasks` runtime tasks sharing the input
/// receivers and output senders.
// Called only from `node_async`, which requires the `tokio-runtime` feature.
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) fn node_async_spawn<I, O, F, Fut>(name: Arc<str>, f: F) -> SpawnStep
where
    I: Send + Unpin + 'static,
    O: Send + Clone + Unpin + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send,
{
    Box::new(move |core: &GraphCore, tasks: usize, inputs, outputs| {
        let rxs = downcast_endpoints::<AsyncReceiver<I>>(inputs, "inputs", &name);
        let txs = downcast_endpoints::<AsyncSender<O>>(outputs, "outputs", &name);
        let f = Arc::new(f);
        for _ in 0..tasks {
            let f = Arc::clone(&f);
            let fanin = AsyncFanIn::new(rxs.clone());
            let out = AsyncBroadcast::new(txs.clone());
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
