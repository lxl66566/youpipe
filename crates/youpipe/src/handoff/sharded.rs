//! Per-worker sharded terminal channel: N single-producer MPSC rings instead
//! of one shared MPSC ring between the terminal stage's workers and the
//! collector.
//!
//! Motivation (2026-09 hotpath audit, todo #1): the terminal fan-in is N
//! worker producers → 1 collector. Even on the MPSC backing, the send side
//! is a multi-producer ring — every worker's `send` executes
//! `compare_exchange` on the *same* `sender` cursor (crossfire
//! `array_queue_mpsc::push_with_ptr`), so producers contend with each other
//! on every item and the collector's dequeue shares those cache lines.
//! Sharding gives each worker its own ring: the producer's CAS has no
//! contenders, and the collector polls N rings whose heads it owns
//! exclusively (`store`-based dequeue, one cache-line pair per shard).
//!
//! EOF aggregation: a shard reports `Closed` only after its sole sender is
//! dropped *and* its ring is drained (standard channel semantics), so the
//! stream ends when every shard has closed — a worker that exits early (or
//! panics and drops its sender mid-run) merely closes its own shard while
//! the others keep flowing.

use super::channel::{ChannelError, MpscReceiver, MpscSender, TryRecvError, mpsc_channel};

/// Minimum per-shard capacity. Below this the ring degenerates into a
/// producer/consumer ping-pong (park on every `Full`), which the
/// burst-drain rhythm on both sides needs some slack to absorb.
const MIN_SHARD_BUFFER: usize = 4;

/// Create `shards` independent bounded MPSC channels presenting one logical
/// fan-in. Each returned sender feeds exactly one shard — **do not `Clone`
/// them** (a cloned sender turns the shard back into a multi-producer ring
/// and reintroduces the send-side CAS contention this type exists to
/// remove). `total_capacity` is the logical buffer budget; each shard gets
/// `max(MIN_SHARD_BUFFER, total_capacity / shards)` so the aggregate stays
/// close to the single-channel backpressure point.
#[must_use]
pub fn sharded_mpsc_channel<T: Send + 'static>(
    shards: usize,
    total_capacity: usize,
) -> (Vec<MpscSender<T>>, ShardedReceiver<T>) {
    let per = (total_capacity / shards.max(1)).max(MIN_SHARD_BUFFER);
    let mut txs = Vec::with_capacity(shards);
    let mut rxs = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (tx, rx) = mpsc_channel(per);
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, ShardedReceiver {
        rxs,
        closed: vec![false; shards],
        cursor: 0,
        open: shards,
    })
}

/// Sole consumer of a [`sharded_mpsc_channel`]: owns all shard receivers and
/// aggregates them into one logical stream. **Not `Clone`** — single
/// consumer, same contract as [`MpscReceiver`].
pub struct ShardedReceiver<T: Send + 'static> {
    rxs: Vec<MpscReceiver<T>>,
    /// Closed shards are never polled again — `try_recv`/`recv` on a closed
    /// crossfire channel re-reads its disconnect state every call, which
    /// would burn a scan slot per pass after a worker exits early.
    closed: Vec<bool>,
    /// Round-robin start index; rotated by every pass and every anchor so no
    /// shard systematically waits behind another (fairness).
    cursor: usize,
    /// Open (not yet `Closed`) shard count — the EOF signal is `open == 0`.
    open: usize,
}

impl<T: Send + 'static> ShardedReceiver<T> {
    /// One full burst pass: walk every shard round-robin from `cursor`,
    /// draining each until `Empty`, feeding `sink`. Returns `false` once
    /// every shard has closed (EOF) — items sunk in the closing pass are
    /// still delivered first (crossfire reports `Closed` only after the ring
    /// is drained).
    ///
    /// Per-shard burst (drain shard i to empty, then i+1) rather than
    /// alternating single pops: same shape as the stage workers' recv loops
    /// (burst winners absorb the backlog while the ring's cache lines are
    /// hot), and the rotating `cursor` bounds any single shard's head start
    /// to one pass.
    pub(crate) fn drain_pass<S: FnMut(T)>(&mut self, sink: &mut S) -> bool {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            loop {
                match self.rxs[i].try_recv() {
                    Ok(item) => sink(item),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        self.closed[i] = true;
                        self.open -= 1;
                        break;
                    },
                }
            }
        }
        self.cursor = (self.cursor + 1) % n;
        self.open > 0
    }

    /// Anchor: every shard is empty but at least one is still open — block
    /// on the first open shard from `cursor`. On `Ok` the cursor moves past
    /// that shard so the next anchor waits elsewhere (a parked anchor must
    /// not pin the same producer twice in a row). A shard that closes while
    /// we wait is marked and skipped; `Err` means every shard has closed
    /// (EOF).
    ///
    /// Blocking on *one* shard (not a "any-of-N" wait) is safe: every open
    /// shard's producer is a live worker that either sends (waking us),
    /// blocks on its own upstream channel (progress elsewhere eventually
    /// feeds it), or drops its sender (closing the shard) — so the anchor
    /// always wakes with the run still making progress. Items landing in
    /// other shards while we park are picked up by the next burst pass.
    pub(crate) fn recv_anchor(&mut self) -> Result<T, ChannelError> {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            match self.rxs[i].recv() {
                Ok(item) => {
                    self.cursor = (i + 1) % n;
                    return Ok(item);
                },
                Err(ChannelError::Closed) => {
                    self.closed[i] = true;
                    self.open -= 1;
                },
            }
        }
        Err(ChannelError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EOF aggregation: shards close independently (their senders drop at
    /// different times) and the receiver keeps delivering from the open ones
    /// until the last closes. Also pins the "closed shard is never polled
    /// again" behaviour — shard 0's sender drops with items never sent (the
    /// "worker left mid-run" shape, e.g. after an early exit or panic
    /// unwind), and its closure must not stall the other shards.
    #[test]
    fn eof_aggregates_across_shards() {
        // 33 / 3 shards = 11 per shard: every below send count (6) fits, so
        // the blocking sends complete before the drain starts (a send
        // exceeding the per-shard capacity would block on a collector that
        // has not started yet — deadlocking the test).
        let (txs, mut rx) = sharded_mpsc_channel::<usize>(3, 33);
        let mut it = txs.into_iter();
        let tx0 = it.next().unwrap();
        let rest: Vec<_> = it.collect();
        for tx in &rest {
            for v in 0..5 {
                tx.send(v).unwrap();
            }
        }
        // Worker 0 exits before producing anything.
        drop(tx0);
        // The remaining workers keep producing after shard 0 closed.
        for tx in &rest {
            tx.send(999).unwrap();
        }
        drop(rest);

        let mut got = Vec::new();
        while rx.drain_pass(&mut |v: usize| got.push(v)) {
            match rx.recv_anchor() {
                Ok(v) => got.push(v),
                Err(ChannelError::Closed) => break,
            }
        }
        let mut expected: Vec<usize> = (0..5).flat_map(|v| [v, v]).collect();
        expected.extend([999, 999]);
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    /// All shards empty and open: `drain_pass` sinks nothing but reports
    /// open, and `recv_anchor` blocks on the live shard (skipping the closed
    /// one) until a send lands — the wake must deliver the item exactly once.
    #[test]
    fn anchor_blocks_until_send() {
        let (txs, mut rx) = sharded_mpsc_channel::<usize>(2, 16);
        let mut it = txs.into_iter();
        let tx0 = it.next().unwrap();
        let tx1 = it.next().unwrap();
        let producer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            tx0.send(42).unwrap();
        });
        // Shard 1's producer exits immediately; the anchor must skip it and
        // park on shard 0.
        drop(tx1);
        let mut seen = Vec::new();
        assert!(
            rx.drain_pass(&mut |v: usize| seen.push(v)),
            "shard 0 still open while the producer sleeps"
        );
        assert_eq!(seen, []);
        match rx.recv_anchor() {
            Ok(v) => seen.push(v),
            Err(ChannelError::Closed) => panic!("anchor closed while shard 0 is alive"),
        }
        producer.join().unwrap();
        while rx.drain_pass(&mut |v: usize| seen.push(v)) {
            match rx.recv_anchor() {
                Ok(v) => seen.push(v),
                Err(ChannelError::Closed) => break,
            }
        }
        assert_eq!(seen, vec![42]);
    }
}
