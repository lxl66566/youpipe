//! Sync-node fan-in primitives: the fair round-robin aggregation over a
//! node's input edges. The pre-anchor spin state machine (`SpinBudget`) and
//! the output side (the `Delivery` strategies) live in `handoff::worker`
//! with the shared worker loops.

use std::time::Instant;

use crate::handoff::{Claim, Receiver, SpinBudget, SpinPoll, claim_poll};

// ── Graph data-plane batching knob ──
//
// `YOUPIPE_GRAPH_TX_BATCH` gates the lever the graph_shapes bench verdict
// named for the light-item regime (docs benchmarks.md "Graph-engine DAG
// shapes"): per-item channel ops on a node's output edges. On, the mapped
// claim batch is staged and each open edge takes one `try_send_batch` per
// free run (`handoff::worker::Broadcast::send_batch`); off, every item is an
// individual blocking `send`. Runtime knob rather than a config field so A/B
// benches compare the same binary (recompiles swing tight benches by tens of
// percent through pure code layout). The sibling lever from the same
// verdict — fan-in claim sweeps (one round claiming from every open input)
// — measured as pure noise on top of this knob and was removed; see
// dead-ends.md.

static GRAPH_TX_BATCH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Batched output delivery on by default; "0" recovers the per-item `send`
/// loops.
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
    /// input disconnected). Burst round first, knob-gated spin second (the
    /// shared [`SpinBudget`] polling whole rounds; a closed input is marked
    /// and rotated past inside [`FanIn::round`], never a spin verdict),
    /// blocking anchor last — the anchor parks registered on every open
    /// input, and its park latency folds back into the spin budget.
    pub(crate) fn claim(
        &mut self,
        scratch: &mut Vec<T>,
        claim_cap: usize,
        spin: &mut SpinBudget,
    ) -> usize {
        if let Some(m) = self.round(scratch, claim_cap) {
            return m;
        }
        match spin.spin_poll(|| {
            self.round(scratch, claim_cap)
                .map_or(SpinPoll::Empty, SpinPoll::Ready)
        }) {
            SpinPoll::Ready(m) => return m,
            // A single-ring `Closed` cannot come out of a round; the empty
            // budget arm falls through to the anchor.
            SpinPoll::Closed | SpinPoll::Empty => {},
        }
        if self.open == 0 {
            return 0;
        }
        let timed = spin.armed();
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
            //
            // `order` is a fresh Vec per park episode and per close
            // re-anchor; hoisting it into per-worker scratch (a FanIn
            // field) was measured as a stable regression on the @1K
            // graph_shapes chain/diamond ids (+3.7…+6.3 %, dominance 0/25,
            // side spreads <3 %, stream controls flat; 2026-10-25 A/B) —
            // the park-path saving does not pay for the hot-path
            // disturbance. The `Select` cannot be hoisted either way: its
            // handlers borrow the receivers for the episode's lifetime, so
            // storing it in FanIn would be self-referential.
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel;

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
        let mut spin = SpinBudget::new(0);
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
