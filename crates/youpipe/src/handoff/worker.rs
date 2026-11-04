//! The engine-agnostic claim → map → deliver worker loop plus the
//! [`Delivery`] output strategies, shared by the stream (`spawn_stage_fanout`
//! / `spawn_expand_stage_fanout`) and graph (`node_worker` / `expand_worker`)
//! engines. Claim source and delivery strategy are the engines' inputs; the
//! loop skeleton — claim-cap share, scratch buffers, the tx-batch gate's two
//! arms, cancel-observed-before-map and deliver-even-on-cancel semantics —
//! lives once here.

use std::mem::MaybeUninit;

use super::{SendItem, SyncSender, stage_batch};
use crate::sync::CancellationToken;

/// True iff `cancel` is set and the pipeline should stop feeding new work.
/// `pub(crate)`: the worker/feeder/bridge loops of both engines.
#[inline]
pub(crate) fn cancel_active(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(CancellationToken::is_cancelled)
}

/// Per-worker claim share of the global batch cap: a multi-consumer ring's
/// workers share the claim source, so each claim is capped at the batch cap
/// divided across them — one head CAS must not monopolize a whole ready burst
/// that the other workers should split. Sole-consumer sites (collector,
/// feeder, fence forwarder) keep the full cap; there a big claim starves
/// nobody. Found when a 2-worker pair became the default: a 40-item burst
/// with 5 ms items serialized onto one worker (max concurrency 1,
/// `test_stage_options_workers_pins_parallelism`).
pub(crate) fn per_worker_claim_cap(workers: usize) -> usize {
    let batch_cap = super::batch_recv_cap();
    if batch_cap == 0 {
        0
    } else {
        (batch_cap / workers.max(1)).max(1)
    }
}

/// Where the tx-batch arm of the worker loops observes the cancellation
/// token — an engine policy (the per-item arm always checks per item):
/// - `PerItem` (stream engine): before every map call, so the tx-batch knob never changes which
///   items are accepted — the mapped prefix is delivered, then the worker exits.
/// - `PerClaim` (graph engine): once before mapping the whole claim batch, so up to `claim_cap`
///   items can be accepted after the fire.
///
/// Either way an accepted item is always delivered — the sends never consult
/// the token — so the emitted set is independent of ring pressure (todo #21,
/// both engines' contract).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelCheck {
    PerItem,
    PerClaim,
}

/// The claim → map → staged-batch-deliver worker loop (plain 1-to-1 map).
///
/// `claim` pulls the next claim batch into its `scratch` argument and returns
/// the item count (`0` = EOF); `map` is the per-item stage function; `out` is
/// the worker's delivery strategy. `tx_batch` selects the staged-delivery arm
/// (the engine's knob and gate decide; zero-cap claims keep the per-item send
/// — staging single-item batches only adds moves). Everything monomorphizes
/// per call site with a constant `check`, so the policy branch folds away:
/// no dyn dispatch on the hot path.
pub(crate) fn claim_map_deliver<I, O, D>(
    claim: &mut impl FnMut(&mut Vec<I>) -> usize,
    map: impl FnMut(I) -> O,
    out: &mut D,
    cancel: Option<&CancellationToken>,
    tx_batch: bool,
    claim_cap: usize,
    check: CancelCheck,
) where
    I: Send + 'static,
    O: Send + 'static,
    D: Delivery<O>,
{
    let mut map = map;
    let mut scratch: Vec<I> = Vec::with_capacity(claim_cap.max(1));
    // Output staging for the batched arm; zero capacity when inert
    // (`with_capacity(0)` allocates nothing).
    let stage_cap = if tx_batch {
        claim_cap
    } else {
        0
    };
    let mut mapped: Vec<O> = Vec::with_capacity(stage_cap);
    let mut staged: Vec<MaybeUninit<O>> = Vec::with_capacity(stage_cap);
    loop {
        let claimed = claim(&mut scratch);
        if claimed == 0 {
            return; // EOF
        }
        if tx_batch {
            // PerItem stages the mapped outputs with the cancel token checked
            // before every map call — exactly the per-item loop's observation
            // point — then delivers even on cancel: every mapped output's map
            // call completed before the token was observed, the same item set
            // the per-item loop sends.
            let cancelled = match check {
                CancelCheck::PerClaim => {
                    if cancel_active(cancel) {
                        return;
                    }
                    mapped.clear();
                    mapped.extend(scratch.drain(..).map(&mut map));
                    false
                },
                CancelCheck::PerItem => {
                    let mut cancelled = false;
                    mapped.clear();
                    for item in scratch.drain(..) {
                        if cancel_active(cancel) {
                            cancelled = true;
                            break;
                        }
                        mapped.push(map(item));
                    }
                    cancelled
                },
            };
            if !out.send_batch(&mut mapped, &mut staged) || cancelled {
                return; // every output edge closed, or token observed
            }
        } else {
            for item in scratch.drain(..) {
                if cancel_active(cancel) {
                    return;
                }
                if !out.send(map(item)) {
                    return; // downstream closed
                }
            }
        }
    }
}

/// The claim → expand → staged-batch-deliver worker loop (1-to-N expansion).
///
/// `expand` is push-style (`Fn(I, &mut Vec<N>)` appending outputs); the
/// per-item expansion scratch is cleared and reused per item, so the steady
/// state performs zero heap allocation. In the batched arm the staged outputs
/// flush whenever they reach `flush_cap` — an expansion's fan-out must not
/// force the staging buffer to grow with the whole claim batch's expansion.
pub(crate) fn claim_expand_deliver<I, N, D>(
    claim: &mut impl FnMut(&mut Vec<I>) -> usize,
    expand: impl FnMut(I, &mut Vec<N>),
    out: &mut D,
    cancel: Option<&CancellationToken>,
    tx_batch: bool,
    claim_cap: usize,
    flush_cap: usize,
    check: CancelCheck,
) where
    I: Send + 'static,
    N: Send + 'static,
    D: Delivery<N>,
{
    let mut expand = expand;
    let mut scratch: Vec<I> = Vec::with_capacity(claim_cap.max(1));
    let stage_cap = if tx_batch {
        claim_cap
    } else {
        0
    };
    let mut mapped: Vec<N> = Vec::with_capacity(stage_cap);
    let mut staged: Vec<MaybeUninit<N>> = Vec::with_capacity(stage_cap);
    let mut buf: Vec<N> = Vec::new();
    loop {
        let claimed = claim(&mut scratch);
        if claimed == 0 {
            return; // EOF
        }
        if tx_batch {
            let per_item = check == CancelCheck::PerItem;
            let mut cancelled = false;
            mapped.clear();
            for item in scratch.drain(..) {
                if per_item && cancel_active(cancel) {
                    cancelled = true;
                    break;
                }
                buf.clear();
                expand(item, &mut buf);
                mapped.append(&mut buf);
                if mapped.len() >= flush_cap && !out.send_batch(&mut mapped, &mut staged) {
                    return; // every output edge closed
                }
            }
            // Deliver the tail (and, on cancel, the outputs whose expand
            // calls completed before the token was observed — the same item
            // set the per-item loop sends).
            if !mapped.is_empty() && !out.send_batch(&mut mapped, &mut staged) {
                return;
            }
            if cancelled {
                return;
            }
        } else {
            for item in scratch.drain(..) {
                if cancel_active(cancel) {
                    return;
                }
                buf.clear();
                expand(item, &mut buf);
                for expanded in buf.drain(..) {
                    if !out.send(expanded) {
                        return; // downstream closed mid-group
                    }
                }
            }
        }
    }
}

/// One producer's output-side delivery strategy, owned by ONE worker: the
/// worker loops are generic over it — one source, per-strategy
/// monomorphization, no dynamic dispatch on the hot path. The stream engines
/// feed any [`SendItem`] per worker through the blanket impl below.
pub(crate) trait Delivery<T: Send + 'static>: Send + 'static {
    /// Deliver one item. Returns `false` when every edge has closed (the
    /// caller's worker exits).
    fn send(&mut self, item: T) -> bool;
    /// Batched delivery (the tx-batch knobs' path); same `false` contract.
    fn send_batch(&mut self, batch: &mut Vec<T>, staged: &mut Vec<MaybeUninit<T>>) -> bool;
}

/// Any single-channel [`SendItem`] is a single-edge delivery — the stream
/// engine's per-worker sender (one sender per worker; the sharded terminal
/// passes one distinct SPSC shard each, which cloning would undo).
impl<T, Tx> Delivery<T> for Tx
where
    T: Send + 'static,
    Tx: SendItem<T>,
{
    fn send(&mut self, item: T) -> bool {
        Tx::send(self, item).is_ok()
    }

    fn send_batch(&mut self, batch: &mut Vec<T>, staged: &mut Vec<MaybeUninit<T>>) -> bool {
        SendItem::send_batch(self, batch, staged)
    }
}

/// Single-edge delivery over one [`SyncSender`]: the counterpart of
/// [`Broadcast`] for producers registered without a `_broadcast` variant.
/// Each item moves into the node's one outgoing edge — no clones, so the item
/// type carries no `Clone` bound. The out-degree-1 invariant is validated at
/// `run()` before any worker spawns; a mismatch inside the conversion is an
/// internal bug.
pub(crate) struct Unicast<T: Send + 'static> {
    tx: SyncSender<T>,
}

impl<T: Send + 'static> From<Vec<SyncSender<T>>> for Unicast<T> {
    fn from(txs: Vec<SyncSender<T>>) -> Self {
        let [tx]: [SyncSender<T>; 1] = txs
            .try_into()
            .ok()
            .expect("move producer has exactly one outgoing edge (validated)");
        Self { tx }
    }
}

impl<T: Send + 'static> Delivery<T> for Unicast<T> {
    fn send(&mut self, item: T) -> bool {
        self.tx.send(item).is_ok()
    }

    fn send_batch(&mut self, batch: &mut Vec<T>, staged: &mut Vec<MaybeUninit<T>>) -> bool {
        stage_batch(batch, staged);
        flush_staged(&self.tx, staged)
    }
}

/// Broadcast fan-out over one node's output edges, owned by ONE worker (each
/// worker receives its own clone of every edge sender). Every open edge
/// receives a clone of the item; the last surviving edge gets the owned item
/// — one clone saved per item. An edge that reports closed is dropped from
/// the active set; the worker keeps serving the rest, and exits only when
/// every edge closed (one dead downstream must not kill the node's other
/// branches).
pub(crate) struct Broadcast<T: Send + 'static> {
    txs: Vec<SyncSender<T>>,
}

impl<T: Send + 'static> From<Vec<SyncSender<T>>> for Broadcast<T> {
    fn from(txs: Vec<SyncSender<T>>) -> Self {
        Self { txs }
    }
}

impl<T: Send + Clone + 'static> Delivery<T> for Broadcast<T> {
    fn send(&mut self, item: T) -> bool {
        let mut i = 0;
        while i + 1 < self.txs.len() {
            if self.txs[i].send(item.clone()).is_err() {
                // Closed edge: drop it without advancing (swap_remove brings
                // the tail into the hole; order among edges is irrelevant).
                self.txs.swap_remove(i);
            } else {
                i += 1;
            }
        }
        // The final surviving edge takes the owned item.
        if self.txs.is_empty() {
            return false;
        }
        let last = self.txs.len() - 1;
        if self.txs[last].send(item).is_err() {
            self.txs.swap_remove(last);
        }
        !self.txs.is_empty()
    }

    /// Batched delivery of every item of `batch` to every open edge — the
    /// `try_send_batch` + per-item blocking fallback rhythm, generalized to
    /// k edges: one tail CAS per free-run prefix per edge instead of one
    /// channel op per item. Every open edge but the last receives clones
    /// (the broadcast contract); the last surviving edge takes the owned
    /// items — one clone saved per item, as [`Delivery::send`]. `staged` is
    /// caller-owned scratch, `clear`-reused per edge.
    ///
    /// The last edge stages through [`stage_batch`] — the
    /// `extend(drain(..).map(..))` shape emits a `Map<Drain<T>>` drop-glue
    /// landing pad per item type, the single-CGU layout hazard 7d3cff3
    /// eradicated from `SendItem::send_batch` (once cost horizontal
    /// cpu_balanced +33-40 % through code this never executes). The
    /// per-edge clone staging (`extend(iter().cloned())`) holds
    /// trivially-droppable iterators and can stay.
    ///
    /// Cancellation is deliberately not consulted here
    /// (`SendItem::send_batch` parity, todo #21): the acceptance point is
    /// the caller's — workers check the token once per claim batch before
    /// mapping — and an accepted batch is delivered in full. On a full ring
    /// the per-item fallback parks until the consumer claims or every
    /// receiver drops; under cancel that park always resolves (downstream
    /// workers exit on their next token check and drop their receivers,
    /// whose close wakes parked senders with `Closed`), so the emitted set
    /// never depends on ring pressure. A closed edge still drops its unsent
    /// remainder (`assume_init_drop` — a `Vec<MaybeUninit<_>>` drop would
    /// skip the destructors).
    fn send_batch(&mut self, batch: &mut Vec<T>, staged: &mut Vec<MaybeUninit<T>>) -> bool {
        let mut i = 0;
        while i + 1 < self.txs.len() {
            staged.clear();
            staged.extend(batch.iter().cloned().map(MaybeUninit::new));
            if flush_staged(&self.txs[i], staged) {
                i += 1;
            } else {
                self.txs.swap_remove(i);
            }
        }
        if self.txs.is_empty() {
            return false; // remaining batch items drop with the Vec
        }
        let last = self.txs.len() - 1;
        stage_batch(batch, staged);
        if !flush_staged(&self.txs[last], staged) {
            self.txs.swap_remove(last);
        }
        !self.txs.is_empty()
    }
}

/// Flush one fully-initialized `staged` batch through one edge: the free
/// prefix goes out with one `try_send_batch`, the remainder per item (parking
/// at most once per full ring, cancellation-blind — see
/// [`Broadcast::send_batch`]). Returns whether the edge survived.
fn flush_staged<T: Send + 'static>(tx: &SyncSender<T>, staged: &mut [MaybeUninit<T>]) -> bool {
    // SAFETY: every staged slot is initialized (the callers stage fully
    // initialized batches); the sent prefix is skipped below.
    let sent = unsafe { tx.try_send_batch(staged) };
    let mut j = sent;
    while j < staged.len() {
        // SAFETY: only [..j) was moved out; slot `j` is still initialized.
        let item = unsafe { staged[j].assume_init_read() };
        j += 1;
        if tx.send(item).is_err() {
            drop_staged(&mut staged[j..]);
            return false;
        }
    }
    true
}

/// Run the destructors of every still-initialized slot — the explicit-drop
/// half of the `MaybeUninit` staging contract (see
/// [`Broadcast::send_batch`]).
fn drop_staged<T>(slots: &mut [MaybeUninit<T>]) {
    for slot in slots {
        // SAFETY: callers only pass never-sent (still initialized) slots.
        unsafe { slot.assume_init_drop() };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel::{SyncReceiver, channel};

    fn drain_n<T: Send + 'static>(rx: &SyncReceiver<T>, n: usize) -> Vec<T> {
        (0..n).map(|_| rx.recv().expect("item buffered")).collect()
    }

    /// Every open edge receives the whole batch in order; the delivery
    /// keeps serving the surviving edges after one closes mid-batch.
    #[test]
    fn send_batch_delivers_to_every_open_edge() {
        let (tx1, rx1) = channel::<u32>(16);
        let (tx2, rx2) = channel::<u32>(16);
        let (tx3, rx3) = channel::<u32>(16);
        let mut b = Broadcast::from(vec![tx1, tx2, tx3]);
        let mut batch = vec![1, 2, 3];
        let mut staged = Vec::new();
        assert!(b.send_batch(&mut batch, &mut staged));
        assert_eq!(drain_n(&rx1, 3), vec![1, 2, 3]);
        assert_eq!(drain_n(&rx2, 3), vec![1, 2, 3]);
        assert_eq!(drain_n(&rx3, 3), vec![1, 2, 3]);
        drop(rx2); // one edge closed
        let mut batch = vec![4, 5];
        assert!(b.send_batch(&mut batch, &mut staged));
        assert_eq!(drain_n(&rx1, 2), vec![4, 5]);
        assert_eq!(drain_n(&rx3, 2), vec![4, 5]);
    }

    /// A batch larger than the ring capacity parks per item on the
    /// remainder (the blocking fallback) and still delivers everything;
    /// the consumer drains concurrently, as in a live pipeline.
    #[test]
    fn send_batch_flushes_remainder_per_item() {
        let (tx, rx) = channel::<u32>(2);
        let drainer = std::thread::spawn(move || drain_n(&rx, 5));
        let mut b = Broadcast::from(vec![tx]);
        let mut batch = vec![1, 2, 3, 4, 5];
        let mut staged = Vec::new();
        assert!(b.send_batch(&mut batch, &mut staged));
        assert_eq!(drainer.join().unwrap(), vec![1, 2, 3, 4, 5]);
    }

    /// Delivery is cancellation-blind (todo #21): a fired token must not
    /// change the emitted set — the whole batch is delivered even when the
    /// ring is full (the per-item fallback parks for the drainer), so no
    /// output depends on ring pressure. Sweep the capacity: the emitted set
    /// is identical at every pressure level.
    #[test]
    fn send_batch_cancelled_token_still_delivers_full_batch() {
        for cap in [1, 2, 7] {
            let (tx, rx) = channel::<u32>(cap);
            let drainer = std::thread::spawn(move || drain_n(&rx, 5));
            let cancel = CancellationToken::new();
            cancel.cancel();
            let mut b = Broadcast::from(vec![tx]);
            let mut batch = vec![1, 2, 3, 4, 5];
            let mut staged = Vec::new();
            assert!(b.send_batch(&mut batch, &mut staged), "cap {cap}");
            assert!(batch.is_empty(), "cap {cap}: batch must be drained");
            assert_eq!(drainer.join().unwrap(), vec![1, 2, 3, 4, 5], "cap {cap}");
        }
    }

    /// Same emitted set across the knob-equivalent paths and ring pressures
    /// (todo #21): the per-item `send` loop (tx-batch knob off) and
    /// `send_batch` (knob on) deliver identical item sets under a fired
    /// token — acceptance granularity may differ per path, delivery never
    /// does.
    #[test]
    fn send_and_send_batch_emit_identical_sets_under_cancel() {
        for cap in [1, 2, 7] {
            let items: Vec<u32> = (0..8).collect();
            let cancel = CancellationToken::new();
            cancel.cancel();
            let (tx1, rx1) = channel::<u32>(cap);
            let d1 = std::thread::spawn(move || drain_n(&rx1, 8));
            let mut per_item = Broadcast::from(vec![tx1]);
            for it in items.iter().copied() {
                assert!(per_item.send(it), "cap {cap}");
            }
            let (tx2, rx2) = channel::<u32>(cap);
            let d2 = std::thread::spawn(move || drain_n(&rx2, 8));
            let mut batched = Broadcast::from(vec![tx2]);
            let mut owned = items.clone();
            let mut staged = Vec::new();
            assert!(batched.send_batch(&mut owned, &mut staged), "cap {cap}");
            assert_eq!(d1.join().unwrap(), items, "cap {cap}");
            assert_eq!(d2.join().unwrap(), items, "cap {cap}");
        }
    }

    /// Every edge closed: `false`, the worker-exit signal.
    #[test]
    fn send_batch_all_edges_closed_returns_false() {
        let (tx1, rx1) = channel::<u32>(4);
        let (tx2, rx2) = channel::<u32>(4);
        drop(rx1);
        drop(rx2);
        let mut b = Broadcast::from(vec![tx1, tx2]);
        let mut batch = vec![1, 2];
        let mut staged = Vec::new();
        assert!(!b.send_batch(&mut batch, &mut staged));
    }

    /// Deterministic cancel-policy probe: one scripted claim batch of 0..8,
    /// the token fired from inside the map of item 3. `PerClaim` maps and
    /// delivers the whole batch (its check ran before the batch);
    /// `PerItem` observes the fire at item 4 and stops staging there — the
    /// mapped prefix 0..=3 is still delivered.
    #[test]
    fn claim_map_deliver_cancel_policies() {
        fn run(check: CancelCheck) -> Vec<u32> {
            let cancel = CancellationToken::new();
            let token = cancel.clone();
            let mut first = true;
            let mut source = move |scratch: &mut Vec<u32>| {
                if first {
                    first = false;
                    scratch.extend(0..8);
                    8
                } else {
                    0 // EOF
                }
            };
            let (tx, rx) = channel::<u32>(64);
            let mut out = tx;
            claim_map_deliver(
                &mut source,
                |x| {
                    if x == 3 {
                        token.cancel();
                    }
                    x
                },
                &mut out,
                Some(&cancel),
                true,
                8,
                check,
            );
            drop(out);
            let mut got = Vec::new();
            while let Ok(v) = rx.try_recv() {
                got.push(v);
            }
            got
        }
        assert_eq!(run(CancelCheck::PerClaim), (0..8).collect::<Vec<_>>());
        assert_eq!(run(CancelCheck::PerItem), (0..4).collect::<Vec<_>>());
    }

    /// The expand driver's batched arm flushes at `flush_cap` and delivers
    /// every accepted expansion output; the per-item arm forwards each
    /// output individually.
    #[test]
    fn claim_expand_deliver_flushes_and_delivers_all() {
        fn expand(i: u32, buf: &mut Vec<u32>) {
            buf.push(i * 10);
            buf.push(i * 10 + 1);
        }
        let scripted = |mut first: bool| {
            move |scratch: &mut Vec<u32>| {
                if first {
                    first = false;
                    scratch.extend(0..6);
                    6
                } else {
                    0
                }
            }
        };
        for tx_batch in [true, false] {
            let (tx, rx) = channel::<u32>(64);
            let mut out = tx;
            let mut source = scripted(true);
            claim_expand_deliver(
                &mut source,
                expand,
                &mut out,
                None,
                tx_batch,
                6,
                2,
                CancelCheck::PerClaim,
            );
            drop(out);
            let mut got = Vec::new();
            while let Ok(v) = rx.try_recv() {
                got.push(v);
            }
            assert_eq!(
                got,
                (0..6)
                    .flat_map(|i| [i * 10, i * 10 + 1])
                    .collect::<Vec<_>>(),
                "tx_batch {tx_batch}"
            );
        }
    }
}
