//! The engine-agnostic claim → map → deliver worker loop plus the
//! [`Delivery`] output strategies, shared by the stream (`spawn_stage_fanout`
//! / `spawn_expand_stage_fanout`) and graph (`node_worker` / `expand_worker`)
//! engines. Claim source and delivery strategy are the engines' inputs; the
//! loop skeleton — claim-cap share, scratch buffers, the tx-batch gate's two
//! arms, cancel-observed-before-map and deliver-even-on-cancel semantics —
//! lives once here.

use std::{
    mem::MaybeUninit,
    time::{Duration, Instant},
};

use super::{Claim, SendItem, SyncSender, stage_batch};
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
        SendItem::flush_staged(&self.tx, staged)
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
    /// remainder (`SendItem::flush_staged`'s `assume_init_drop` — a
    /// `Vec<MaybeUninit<_>>` drop would skip the destructors).
    fn send_batch(&mut self, batch: &mut Vec<T>, staged: &mut Vec<MaybeUninit<T>>) -> bool {
        let mut i = 0;
        while i + 1 < self.txs.len() {
            staged.clear();
            staged.extend(batch.iter().cloned().map(MaybeUninit::new));
            if SendItem::flush_staged(&self.txs[i], staged) {
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
        if !SendItem::flush_staged(&self.txs[last], staged) {
            self.txs.swap_remove(last);
        }
        !self.txs.is_empty()
    }
}

// ── Activity-gated pre-anchor spin (todo #4 fix, `YOUPIPE_SPIN_ANCHOR`) ──

/// Calibration: one `pause` + one empty `try_recv` ≈ 15 ns on the reference
/// machine, so 1 µs of window ≈ 64 polls. Doubles as the miri backstop (the
/// wall-clock deadline may never fire there — time can freeze in a busy
/// loop); on real hardware the clock check dominates.
const SPIN_ROUNDS_PER_US: u32 = 64;

/// Backoff ceiling inside the pre-anchor spin: 64 pauses ≈ 1 µs between
/// polls at the tail of a long window (see [`SpinBudget::spin_poll`]).
const MAX_POLL_PAUSES: u32 = 64;

/// Outcome of one non-blocking poll inside a [`SpinBudget`] window —
/// [`Claim`] lifted to the poll-closure contract (single-ring claim sites
/// convert via `From`; fan-in rounds map their `Option` themselves, a
/// closed input is internal state there, never a verdict).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SpinPoll {
    /// `n` items were caught mid-spin — the park is avoided.
    Ready(usize),
    /// Nothing ready right now; keep spinning while the budget lasts.
    Empty,
    /// Terminal verdict (channel disconnected mid-spin): stop the spin and
    /// propagate. Trusted like the burst phase's `Closed` (the crossfire pin
    /// at 3.1.20 or newer carries the spurious-`Closed` race fix), never
    /// second-guessed: the blocking anchor would only repeat it.
    Closed,
}

impl From<Claim> for SpinPoll {
    fn from(c: Claim) -> Self {
        match c {
            Claim::Ready(n) => SpinPoll::Ready(n),
            Claim::Empty => SpinPoll::Empty,
            Claim::Closed => SpinPoll::Closed,
        }
    }
}

/// Adaptive pre-anchor spin budget for one worker's claim loop — the shared
/// state machine behind both engines' claim sources (the stream engine's
/// single-ring `anchor_claim`, the graph engine's `FanIn` round).
///
/// The convoy pathology (streaming.md "Convoy collapse forensics") lives at
/// channels whose supply side is a single rate-limited entity (feeder job /
/// fence forwarder) and whose demand side is a crowd: the channel hovers
/// empty, every arrival pays a futex wake RTT (~1–2 µs), and the pipeline
/// clocks at items × RTT. Spinning non-blocking polls before parking lets
/// consumers ride out those micro-gaps — bursts re-form and the wake RTT
/// leaves the critical path.
///
/// The budget must be **activity-gated** (constant spin-window widening is
/// falsified, dead-ends.md): it only stays above zero while items keep
/// arriving within the knob's window after a failed spin, measured as the
/// blocking anchor's latency right after the spin gave up. All state is
/// per-worker locals touched once per anchor — no atomics, no clock reads
/// while the channel has a backlog.
pub(crate) struct SpinBudget {
    /// Current spin window in µs; 0 = park immediately (baseline behavior).
    budget_us: u32,
    /// Knob value: the largest window worth spinning through.
    max_us: u32,
}

impl SpinBudget {
    pub(crate) fn new(max_us: u32) -> Self {
        Self {
            budget_us: 0,
            max_us,
        }
    }

    /// Whether the knob arms the spin at all (max window > 0); the anchor
    /// skips its park-latency bookkeeping when not.
    pub(crate) fn armed(&self) -> bool {
        self.max_us > 0
    }

    /// First growth step (first "live" park): max/16 clamped to ≥1 µs, so
    /// geometric doubling reaches a covering window in ~4 anchors without
    /// ever spinning near the full knob on the first try.
    fn initial(max_us: u32) -> u32 {
        (max_us / 16).max(1)
    }

    /// Spin `poll` for up to the current budget.
    /// `Ready(n)`: a run was caught mid-spin — the park is avoided.
    /// `Empty`: budget exhausted — run the blocking anchor, then fold its
    /// latency back via [`SpinBudget::note_park`].
    /// `Closed`: terminal verdict, propagate.
    ///
    /// Backoff polling: the first polls are tight (fast suppliers are caught
    /// almost immediately), then the pause count doubles up to
    /// `MAX_POLL_PAUSES` — a crowd of spinning workers on one shared ring
    /// must not poll at full rate or the ring's stamp cache lines bounce
    /// between cores faster than items arrive (measured: tight spinning at
    /// 14 workers REGRESSES the collapsed cell 3x vs parking, 2026-09 convoy
    /// matrix; ~1 poll/µs/worker at the backoff tail rides out micro-gaps
    /// without the futex round-trip).
    pub(crate) fn spin_poll(&mut self, mut poll: impl FnMut() -> SpinPoll) -> SpinPoll {
        if self.budget_us == 0 {
            return SpinPoll::Empty;
        }
        let mut pauses = 1u32;
        let mut polls = 0u64;
        let max_polls = u64::from(self.budget_us) * u64::from(SPIN_ROUNDS_PER_US);
        let window = Duration::from_micros(u64::from(self.budget_us));
        let start = Instant::now();
        loop {
            match poll() {
                SpinPoll::Ready(n) => return SpinPoll::Ready(n),
                SpinPoll::Closed => return SpinPoll::Closed,
                SpinPoll::Empty => {},
            }
            // Wall-clock deadline (checked every 8 polls) keeps the window
            // honest under contention; the poll backstop bounds the loop when
            // the clock does not advance (miri).
            if polls >= max_polls || (polls & 7 == 7 && start.elapsed() >= window) {
                return SpinPoll::Empty;
            }
            for _ in 0..pauses {
                std::hint::spin_loop();
            }
            pauses = pauses.saturating_mul(2).min(MAX_POLL_PAUSES);
            polls += 1;
        }
    }

    /// Fold the blocking anchor's latency into the budget:
    /// - item arrived within `4x` the knob's window: the channel is live — a longer spin might have
    ///   caught an earlier item, double the budget. The 4x slack is load-bearing: with a crowd of
    ///   `k` parked consumers and wake-one delivery, a given worker wins an item only every ~k
    ///   gaps, so a strict `parked <= window` rule decays every loser to a pure park (measured:
    ///   parks stayed at ~190 K/run — the spin never engaged at all; with the slack the crowd ramps
    ///   collectively);
    /// - item took longer than 4x the window: quiet relative to what this worker is prepared to
    ///   spin — quarter it (an idle stage decays to pure parking within log₄(window) anchors, ≤
    ///   ~1.33x the window of wasted spin per idle episode).
    pub(crate) fn note_park(&mut self, parked: Duration) {
        let live_below = Duration::from_micros(u64::from(self.max_us) * 4);
        if parked <= live_below {
            self.budget_us = self
                .budget_us
                .saturating_mul(2)
                .max(Self::initial(self.max_us))
                .min(self.max_us);
        } else {
            self.budget_us /= 4;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel::{SyncReceiver, channel, claim_poll};

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

    /// `note_park` growth/decay: live parks double the budget up to the
    /// knob, quiet parks quarter it to zero, a zero knob is inert.
    #[test]
    fn spin_budget_adapts() {
        let mut s = SpinBudget::new(10);
        assert_eq!(s.budget_us, 0);
        // First live park: initial window (10/16 -> 1).
        s.note_park(Duration::from_micros(2));
        assert_eq!(s.budget_us, 1);
        // Doubling, capped at the knob.
        for expected in [2, 4, 8, 10, 10] {
            s.note_park(Duration::from_nanos(500));
            assert_eq!(s.budget_us, expected);
        }
        // Quiet channel: quarters down to pure parking.
        for _ in 0..4 {
            s.note_park(Duration::from_millis(1));
        }
        assert_eq!(s.budget_us, 0);
        // Knob off: stays inert regardless of park latency.
        let mut off = SpinBudget::new(0);
        off.note_park(Duration::from_nanos(1));
        assert_eq!(off.budget_us, 0);
    }

    /// `spin_poll`: a zero budget never spins; with a budget it catches a
    /// queued run without parking, exhausts to `Empty` on an empty source
    /// (bounded rounds — miri-safe, no wall clock), and propagates
    /// `Closed`.
    #[test]
    fn spin_poll_phases() {
        let (tx, rx) = channel::<u32>(4);
        let mut s = SpinBudget::new(100);
        let mut scratch: Vec<u32> = Vec::with_capacity(2);
        let poll = |scratch: &mut Vec<u32>| claim_poll(&rx, scratch, 0).into();
        assert_eq!(s.spin_poll(|| poll(&mut scratch)), SpinPoll::Empty); // budget 0: no spin
        tx.send(7).unwrap();
        s.budget_us = 100;
        assert_eq!(s.spin_poll(|| poll(&mut scratch)), SpinPoll::Ready(1));
        assert_eq!(scratch, vec![7]);
        scratch.clear();
        s.budget_us = 1; // 64-poll backstop: exhausts immediately on empty
        assert_eq!(s.spin_poll(|| poll(&mut scratch)), SpinPoll::Empty);
        drop(tx);
        s.budget_us = 100;
        assert_eq!(s.spin_poll(|| poll(&mut scratch)), SpinPoll::Closed);
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
