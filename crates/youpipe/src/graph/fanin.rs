//! Sync-node fan-in and fan-out primitives: the fair round-robin aggregation
//! over a node's input edges, the activity-gated pre-anchor backoff, and the
//! broadcast helper that fans one output out to every edge of a node.

use std::{
    mem::MaybeUninit,
    time::{Duration, Instant},
};

use crate::{
    builder::cancel_active,
    handoff::{Claim, Receiver, SyncSender, claim_poll, stage_batch},
    sync::CancellationToken,
};

// ── Graph data-plane batching knob ──
//
// `YOUPIPE_GRAPH_TX_BATCH` gates the lever the graph_shapes bench verdict
// named for the light-item regime (docs benchmarks.md "Graph-engine DAG
// shapes"): per-item channel ops on a node's output edges. On, the mapped
// claim batch is staged and each open edge takes one `try_send_batch` per
// free run (`Broadcast::send_batch`); off, every item is an individual
// blocking `send`. Runtime knob rather than a config field so A/B benches
// compare the same binary (recompiles swing tight benches by tens of
// percent through pure code layout). The sibling lever from the same
// verdict — fan-in claim sweeps (one round claiming from every open input)
// — measured as pure noise on top of this knob and was removed; see
// dead-ends.md.

static GRAPH_TX_BATCH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Batched broadcast sends (`Broadcast::send_batch`) on by default; "0"
/// recovers the per-item `send` loops.
pub(crate) fn graph_tx_batch() -> bool {
    *GRAPH_TX_BATCH.get_or_init(|| match std::env::var("YOUPIPE_GRAPH_TX_BATCH") {
        Ok(v) if v == "1" || v.is_empty() => true,
        Ok(v) if v == "0" => false,
        Ok(other) => panic!(
            "YOUPIPE_GRAPH_TX_BATCH: invalid value {other:?} (leave unset or use \"0\"/\"1\")"
        ),
        Err(_) => true,
    })
}

/// Fair fan-in over the shared receivers of one node's input edges, owned by
/// ONE worker (each worker clones every input receiver — MPMC competition
/// plus fair polling across the round).
///
/// The claim rhythm mirrors the streaming workers' anchor + burst-drain
/// shape, generalized from one ring to a round of rings (same shape as the
/// sharded terminal's `ShardSet`): one non-blocking round over every open
/// input, then an any-of-N blocking anchor (see [`FanIn::anchor`]). EOF is
/// every input disconnected — an upstream node whose workers all exited
/// closes its sender clones and the input here reports `Closed` while the
/// others keep flowing.
pub(crate) struct FanIn<T: Send + 'static> {
    rxs: Vec<Receiver<T>>,
    /// Closed inputs are never polled again (a `try_recv`/`recv` on a closed
    /// crossfire channel re-reads its disconnect state every call).
    closed: Vec<bool>,
    /// Round-robin start index; rotated by every round and anchor so no
    /// input systematically waits behind another.
    cursor: usize,
    /// Open input count; the EOF signal is `open == 0`.
    open: usize,
}

impl<T: Send + 'static> FanIn<T> {
    pub(crate) fn new(rxs: Vec<Receiver<T>>) -> Self {
        let open = rxs.len();
        Self {
            closed: vec![false; open],
            rxs,
            cursor: 0,
            open,
        }
    }

    /// One non-blocking round: poll every open input once starting at
    /// `cursor`; the first ready input's claim (≤ `claim_cap` items) lands
    /// in `scratch` and the cursor rotates past it, bounding any input's
    /// head start to one round. `None` when the whole round came back empty
    /// (open inputs may remain — the caller spins or parks).
    fn round(&mut self, scratch: &mut Vec<T>, claim_cap: usize) -> Option<usize> {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            match claim_poll(&self.rxs[i], scratch, claim_cap) {
                Claim::Ready(m) => {
                    self.cursor = (i + 1) % n;
                    return Some(m);
                },
                Claim::Closed => {
                    self.closed[i] = true;
                    self.open -= 1;
                },
                Claim::Empty => {},
            }
        }
        None
    }

    /// Claim the next batch of items into `scratch`; `0` means EOF (every
    /// input disconnected). Burst round first, knob-gated spin second
    /// (see [`RoundSpin`]), blocking anchor last — the anchor parks
    /// registered on every open input, and its park latency folds back into
    /// the spin budget.
    pub(crate) fn claim(
        &mut self,
        scratch: &mut Vec<T>,
        claim_cap: usize,
        spin: &mut RoundSpin,
    ) -> usize {
        if let Some(m) = self.round(scratch, claim_cap) {
            return m;
        }
        if let Some(m) = spin.spin_rounds(self, scratch, claim_cap) {
            return m;
        }
        if self.open == 0 {
            return 0;
        }
        let timed = spin.max_us > 0;
        let start = Instant::now();
        let got = self.anchor(scratch);
        if timed && got > 0 {
            spin.note_park(start.elapsed());
        }
        got
    }

    /// Blocking anchor: park registered on every open input at once
    /// (crossfire `Select`), so an arrival or close on ANY input wakes the
    /// worker; the winning input's item is claimed through the select's
    /// reserved token (CAS-reserved, safe under the MPMC competition of
    /// sibling workers), and the cursor rotates past it. A close event
    /// (ring drained + every sender dropped) marks the input and re-anchors
    /// over the survivors; `0` is EOF.
    ///
    /// Why an any-of-N park, not a park on one input: in a DAG the anchored
    /// input's producer chain can itself be blocked behind a ring only THIS
    /// worker drains — broadcast edges flush sequentially and block per
    /// edge, so the upstream feeding the anchored ring may be a worker
    /// parked on a full ring whose sole consumer is this node. Parking on
    /// one input then deadlocks the whole run (the graph-side shape of todo
    /// #14's intermittent hang: `diamond_broadcast_conservation`, n=10K,
    /// buffer=4 — every fan-in worker parked on the starved ring while the
    /// other ring's producers blocked on Full). The single-input park is
    /// only sound for a linear chain's sole input (stream stages, the
    /// sharded terminal), where "the producer blocks on its own upstream"
    /// cannot skip this consumer.
    fn anchor(&mut self, scratch: &mut Vec<T>) -> usize {
        if self.open == 1 {
            return self.anchor_single(scratch);
        }
        loop {
            if self.open == 0 {
                return 0;
            }
            let n = self.rxs.len();
            // Registration order follows the cursor rotation so the
            // select's round-robin starts where our round did.
            let mut order: Vec<usize> = Vec::with_capacity(n);
            let mut sel = crossfire::select::Select::new();
            for k in 0..n {
                let i = (self.cursor + k) % n;
                if !self.closed[i] {
                    sel.add(self.rxs[i].crossfire_rx());
                    order.push(i);
                }
            }
            // `open > 0` guarantees a nonempty handler set; `select()`
            // errors only when no handler is registered.
            let res = sel.select().expect("open inputs registered");
            let slot = order
                .iter()
                .position(|&i| res.is_from(self.rxs[i].crossfire_rx()))
                .expect("select result names a registered input");
            let ring = order[slot];
            if let Ok(item) = self.rxs[ring].crossfire_rx().read_select(res) {
                self.cursor = (ring + 1) % n;
                scratch.push(item);
                return 1;
            }
            // Close event: drop the input from the round and re-anchor
            // over the survivors (items always win over the close token,
            // so the drain-before-Closed contract holds).
            self.closed[ring] = true;
            self.open -= 1;
        }
    }

    /// Single open input: a plain blocking recv. The sole live producer
    /// chain either sends (waking us), parks on its own upstream, or drops
    /// its sender — no OTHER ring still needs this worker to drain it (the
    /// closed ones are drained and disconnected), so the linear-chain park
    /// is sound. Fast path for the linear-chain majority (chains' nodes
    /// have one input each), which then pays none of the any-of-N
    /// registration cost.
    fn anchor_single(&mut self, scratch: &mut Vec<T>) -> usize {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            if let Ok(item) = self.rxs[i].recv() {
                self.cursor = (i + 1) % n;
                scratch.push(item);
                return 1;
            }
            self.closed[i] = true;
            self.open -= 1;
        }
        0
    }
}

// ── Activity-gated pre-anchor backoff ──

/// Calibration and backoff ceiling copied from the stream engine's
/// `AnchorSpin` (one `pause` + one empty poll ≈ 15 ns on the reference
/// machine; 64 pauses ≈ 1 µs between polls at the tail) so the fan-in
/// round rides out empty micro-gaps with the same rhythm the single-ring
/// workers use.
const SPIN_ROUNDS_PER_US: u32 = 64;
const MAX_POLL_PAUSES: u32 = 64;

/// Adaptive pre-anchor spin budget for one fan-in worker's claim loop —
/// `AnchorSpin` generalized from "spin one ring" to "spin the round of
/// rings". Activity-gated exactly like the original: the budget only stays
/// above zero while items keep arriving within the knob's window after a
/// failed spin (measured as the anchor's park latency right after the spin
/// gave up); inert with the `YOUPIPE_SPIN_ANCHOR` knob off.
pub(crate) struct RoundSpin {
    /// Current spin window in µs; 0 = park immediately.
    budget_us: u32,
    /// Knob value: the largest window worth spinning through.
    max_us: u32,
}

impl RoundSpin {
    pub(crate) fn new(max_us: u32) -> Self {
        Self {
            budget_us: 0,
            max_us,
        }
    }

    /// First growth step: max/16 clamped to ≥ 1 µs (geometric doubling then
    /// reaches a covering window in ~4 anchors).
    fn initial(max_us: u32) -> u32 {
        (max_us / 16).max(1)
    }

    /// Spin-poll rounds for up to the current budget, with the same pause
    /// doubling as `AnchorSpin::spin_claim` (a crowd of workers on the same
    /// rings must not poll at full rate). `Some(n)` when a round caught data
    /// mid-spin — the park is avoided.
    fn spin_rounds<T: Send + 'static>(
        &mut self,
        fan: &mut FanIn<T>,
        scratch: &mut Vec<T>,
        claim_cap: usize,
    ) -> Option<usize> {
        if self.budget_us == 0 {
            return None;
        }
        let mut pauses = 1u32;
        let mut polls = 0u64;
        let max_polls = u64::from(self.budget_us) * u64::from(SPIN_ROUNDS_PER_US);
        let window = Duration::from_micros(u64::from(self.budget_us));
        let start = Instant::now();
        loop {
            if let Some(m) = fan.round(scratch, claim_cap) {
                return Some(m);
            }
            if polls >= max_polls || (polls & 7 == 7 && start.elapsed() >= window) {
                return None;
            }
            for _ in 0..pauses {
                std::hint::spin_loop();
            }
            pauses = pauses.saturating_mul(2).min(MAX_POLL_PAUSES);
            polls += 1;
        }
    }

    /// Fold the blocking anchor's latency into the budget (same policy as
    /// `AnchorSpin::note_park`): an item within 4x the knob's window means
    /// the fan-in is live — double the budget; longer means quiet relative
    /// to what this worker is prepared to spin — quarter it.
    fn note_park(&mut self, parked: Duration) {
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

/// Broadcast fan-out over one node's output edges, owned by ONE worker (each
/// worker clones every edge sender). Every open edge receives a clone of the
/// item; the last surviving edge gets the owned item — one clone saved per
/// item. An edge that reports closed is dropped from the active set; the
/// worker keeps serving the rest, and exits only when every edge closed
/// (one dead downstream must not kill the node's other branches).
pub(crate) struct Broadcast<T: Send + 'static> {
    txs: Vec<SyncSender<T>>,
}

impl<T: Send + Clone + 'static> Broadcast<T> {
    pub(crate) fn new(txs: Vec<SyncSender<T>>) -> Self {
        Self { txs }
    }

    /// Deliver one item to every open edge. Returns `false` when every edge
    /// has closed (the caller's worker exits).
    pub(crate) fn send(&mut self, item: T) -> bool {
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
    /// stream engine's `try_send_batch` + per-item blocking fallback rhythm
    /// (`push_items` / `fence_batch_send`), generalized to k edges: one
    /// tail CAS per free-run prefix per edge instead of one channel op per
    /// item. Every open edge but the last receives clones (the broadcast
    /// contract); the last surviving edge takes the owned items — one clone
    /// saved per item, as `send`. `staged` is caller-owned scratch,
    /// `clear`-reused per edge.
    ///
    /// The last edge stages through [`stage_batch`] — the
    /// `extend(drain(..).map(..))` shape emits a `Map<Drain<T>>` drop-glue
    /// landing pad per item type, the single-CGU layout hazard 7d3cff3
    /// eradicated from `SendItem::send_batch` (once cost horizontal
    /// cpu_balanced +33-40 % through code this never executes). The
    /// per-edge clone staging (`extend(iter().cloned())`) holds
    /// trivially-droppable iterators and can stay.
    ///
    /// Abort semantics match `send`'s per-item loop: on cancel or a closed
    /// edge the unsent remainder is dropped (`assume_init_drop` — a
    /// `Vec<MaybeUninit<_>>` drop would skip the destructors).
    pub(crate) fn send_batch(
        &mut self,
        batch: &mut Vec<T>,
        staged: &mut Vec<MaybeUninit<T>>,
        cancel: Option<&CancellationToken>,
    ) -> bool {
        let mut i = 0;
        while i + 1 < self.txs.len() {
            staged.clear();
            staged.extend(batch.iter().cloned().map(MaybeUninit::new));
            if self.flush_edge(i, staged, cancel) {
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
        if !self.flush_edge(last, staged, cancel) {
            self.txs.swap_remove(last);
        }
        !self.txs.is_empty()
    }

    /// Flush one fully-initialized `staged` batch through edge `i`: the
    /// free prefix goes out with one `try_send_batch`, the remainder
    /// per-item (parking at most once per full ring, cancel checked before
    /// every blocking send — `push_items` parity). Returns whether the
    /// edge survived.
    fn flush_edge(
        &self,
        i: usize,
        staged: &mut [MaybeUninit<T>],
        cancel: Option<&CancellationToken>,
    ) -> bool {
        let sent = self.txs[i].try_send_batch(staged);
        let mut j = sent;
        while j < staged.len() {
            if cancel_active(cancel) {
                drop_staged(&mut staged[j..]);
                return true;
            }
            // SAFETY: only [..j) was moved out; slot `j` is still initialized.
            let item = unsafe { staged[j].assume_init_read() };
            j += 1;
            if self.txs[i].send(item).is_err() {
                drop_staged(&mut staged[j..]);
                return false;
            }
        }
        true
    }
}

/// Run the destructors of every still-initialized slot — the explicit-drop
/// half of the `MaybeUninit` staging contract (see `send_batch`).
fn drop_staged<T>(slots: &mut [MaybeUninit<T>]) {
    for slot in slots {
        // SAFETY: callers only pass never-sent (still initialized) slots.
        unsafe { slot.assume_init_drop() };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel;

    fn drain_n<T: Send + 'static>(rx: &Receiver<T>, n: usize) -> Vec<T> {
        (0..n).map(|_| rx.recv().expect("item buffered")).collect()
    }

    /// Every open edge receives the whole batch in order; the delivery
    /// keeps serving the surviving edges after one closes mid-batch.
    #[test]
    fn send_batch_delivers_to_every_open_edge() {
        let (tx1, rx1) = channel::<u32>(16);
        let (tx2, rx2) = channel::<u32>(16);
        let (tx3, rx3) = channel::<u32>(16);
        let mut b = Broadcast::new(vec![tx1, tx2, tx3]);
        let mut batch = vec![1, 2, 3];
        let mut staged = Vec::new();
        assert!(b.send_batch(&mut batch, &mut staged, None));
        assert_eq!(drain_n(&rx1, 3), vec![1, 2, 3]);
        assert_eq!(drain_n(&rx2, 3), vec![1, 2, 3]);
        assert_eq!(drain_n(&rx3, 3), vec![1, 2, 3]);
        drop(rx2); // one edge closed
        let mut batch = vec![4, 5];
        assert!(b.send_batch(&mut batch, &mut staged, None));
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
        let mut b = Broadcast::new(vec![tx]);
        let mut batch = vec![1, 2, 3, 4, 5];
        let mut staged = Vec::new();
        assert!(b.send_batch(&mut batch, &mut staged, None));
        assert_eq!(drainer.join().unwrap(), vec![1, 2, 3, 4, 5]);
    }

    /// Cancel during the per-item fallback drops the unsent remainder
    /// without parking; the already-sent prefix stays delivered and the
    /// edges stay open (the caller's loop exits on its next check).
    #[test]
    fn send_batch_cancel_drops_remainder_without_parking() {
        let (tx, rx) = channel::<u32>(1);
        let cancel = CancellationToken::new();
        let mut b = Broadcast::new(vec![tx]);
        // Fill the ring so the fallback path is reached.
        let mut batch = vec![1, 2, 3];
        let mut staged = Vec::new();
        cancel.cancel();
        assert!(b.send_batch(&mut batch, &mut staged, Some(&cancel)));
        // Item 1 fit the free slot (try_send_batch prefix); 2 and 3 were
        // dropped by the cancel check — the call must not park on Full.
        assert_eq!(drain_n(&rx, 1), vec![1]);
        assert!(matches!(
            rx.try_recv(),
            Err(crate::handoff::TryRecvError::Empty)
        ));
    }

    /// Every edge closed: `false`, the worker-exit signal.
    #[test]
    fn send_batch_all_edges_closed_returns_false() {
        let (tx1, rx1) = channel::<u32>(4);
        let (tx2, rx2) = channel::<u32>(4);
        drop(rx1);
        drop(rx2);
        let mut b = Broadcast::new(vec![tx1, tx2]);
        let mut batch = vec![1, 2];
        let mut staged = Vec::new();
        assert!(!b.send_batch(&mut batch, &mut staged, None));
    }
    /// Liveness regression (the graph-side shape of todo #14's intermittent
    /// hang): a fan-in worker anchored while one input is empty must still
    /// claim from the OTHER input. In a DAG the anchored input's producer
    /// chain can be blocked behind a ring only this worker drains, so an
    /// anchor that only wakes for its own ring's events deadlocks the run
    /// (repro: `diamond_broadcast_conservation`, n=10K, buffer=4 — all
    /// fan-in workers parked on the starved ring while the other ring's
    /// producers block on Full).
    #[test]
    fn anchored_worker_claims_from_other_input() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::{Duration, Instant},
        };

        let (tx_a, rx_a) = channel::<u32>(4);
        let (tx_b, rx_b) = channel::<u32>(4);
        let mut fan = FanIn::new(vec![rx_a, rx_b]);
        let mut spin = RoundSpin::new(0);
        let claimed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&claimed);
        let worker = std::thread::spawn(move || {
            let mut scratch = Vec::new();
            loop {
                let n = fan.claim(&mut scratch, 0, &mut spin);
                if n == 0 {
                    return; // EOF: both inputs closed
                }
                counter.fetch_add(scratch.drain(..).count(), Ordering::SeqCst);
            }
        });
        // Let the worker park: both rings empty, cursor 0 → the anchor parks
        // on ring A. Ring A's producer then stays silent for the whole test
        // (its chain is "blocked upstream" in the deadlock shape).
        std::thread::sleep(Duration::from_millis(100));
        for v in 0..4u32 {
            tx_b.send(v).expect("ring B has capacity 4");
        }
        // The anchored worker must drain ring B without any event on ring A.
        let deadline = Instant::now() + Duration::from_secs(2);
        while claimed.load(Ordering::SeqCst) < 4 {
            assert!(
                Instant::now() < deadline,
                "fan-in worker anchored on a silent input never claimed the other input's items"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // EOF: close both rings, the worker exits.
        drop(tx_a);
        drop(tx_b);
        worker.join().unwrap();
        assert_eq!(claimed.load(Ordering::SeqCst), 4);
    }
}
