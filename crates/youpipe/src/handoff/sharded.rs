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
//!
//! Two flavours over one [`ShardSet`] core: the sync flavour
//! ([`sharded_mpsc_channel`]) serves sync/expand terminals; the async
//! flavour ([`sharded_mpsc_async_channel`]) serves the async terminal whose
//! producers are runtime tasks sending via `send().await` (todo #1
//! residual (c)).

use super::channel::{
    ChannelError, MpscReceiver, MpscSender, RecvItem, TryRecvError, mpsc_channel,
};
#[cfg(feature = "tokio-runtime")]
use super::channel::{MpscAsyncReceiver, MpscAsyncSender, TryRecvItem, mpsc_async_channel};

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
pub(crate) fn sharded_mpsc_channel<T: Send + 'static>(
    shards: usize,
    total_capacity: usize,
) -> (Vec<MpscSender<T>>, ShardedReceiver<T>) {
    // A 0-shard set used to construct "successfully" (`shards.max(1)`
    // masked the division) and then panic far away on the first
    // `drain_pass`/`recv_anchor` (`% 0`). Fail fast at the construction
    // boundary instead.
    assert!(shards > 0, "shards must be nonzero");
    let per = (total_capacity / shards).max(MIN_SHARD_BUFFER);
    let mut txs = Vec::with_capacity(shards);
    let mut rxs = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (tx, rx) = mpsc_channel(per);
        txs.push(tx);
        rxs.push(rx);
    }
    // Batch scratch for `drain_pass` (YOUPIPE_BATCH_RECV; capacity 0 =
    // per-item draining, the historical shape).
    let scratch = Vec::with_capacity(super::batch_recv_cap());
    (txs, ShardedReceiver {
        inner: ShardSet {
            rxs,
            closed: vec![false; shards],
            cursor: 0,
            open: shards,
        },
        scratch,
    })
}

/// Create `shards` independent bounded async MPSC channels presenting one
/// logical fan-in — the async-terminal flavour of [`sharded_mpsc_channel`].
/// The producers are runtime tasks (`io_concurrency` of them) sending via
/// [`MpscAsyncSender::send`](super::channel::MpscAsyncSender::send).await.
///
/// Unlike the sync constructor, senders here MAY be shared by several tasks:
/// the caller caps `shards` at the runtime's worker-thread count (only that
/// many tasks can send concurrently), so a large-`io_concurrency` stage has
/// each shard shared by `io_concurrency / shards` tasks — per-ring
/// contention drops from `io_concurrency`-way to that quotient while the
/// collector's pass cost stays bounded by `shards`.
#[cfg(feature = "tokio-runtime")]
#[must_use]
pub(crate) fn sharded_mpsc_async_channel<T: Send + Unpin + 'static>(
    shards: usize,
    total_capacity: usize,
) -> (Vec<MpscAsyncSender<T>>, ShardedAsyncReceiver<T>) {
    // Same zero-shard guard as `sharded_mpsc_channel`.
    assert!(shards > 0, "shards must be nonzero");
    let per = (total_capacity / shards).max(MIN_SHARD_BUFFER);
    let mut txs = Vec::with_capacity(shards);
    let mut rxs = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (tx, rx) = mpsc_async_channel(per);
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, ShardedAsyncReceiver {
        inner: ShardSet {
            rxs,
            closed: vec![false; shards],
            cursor: 0,
            open: shards,
        },
    })
}

/// Receiver-side core shared by both flavours: the shard receivers, the
/// closed marks, the rotating cursor and the open count, plus the
/// round-robin burst pass. The anchor (blocking `recv` vs awaited `recv`)
/// is flavour-specific and lives on the wrappers — everything else is
/// channel-shape logic that must not drift between the sync and async
/// terminals.
struct ShardSet<R> {
    rxs: Vec<R>,
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

impl<R> ShardSet<R> {
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
    ///
    /// `T` sits on the method (not the impl) so `ShardSet<R>` needs no
    /// unconstrained parameter — the item type is fully determined by
    /// `R: TryRecvItem<T>` at the call site.
    // Async-collector only: the sync receiver drains via `drain_pass_batched`.
    #[cfg(feature = "tokio-runtime")]
    fn drain_pass<T, S: FnMut(T)>(&mut self, sink: &mut S) -> bool
    where
        R: TryRecvItem<T>,
    {
        let n = self.rxs.len();
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            loop {
                // UFCS: both wrappers also expose an inherent `try_recv`,
                // which would otherwise shadow the trait method.
                match TryRecvItem::try_recv(&self.rxs[i]) {
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

    /// Batched variant of [`ShardSet::drain_pass`] for the sync flavour
    /// under `YOUPIPE_BATCH_RECV`: each shard's ready run is claimed with
    /// one `recv`-cursor store per run (todo #1 residual (d)); a
    /// 0-capacity `scratch` keeps the per-item rhythm above
    /// byte-for-byte.
    fn drain_pass_batched<T, S: FnMut(T)>(&mut self, scratch: &mut Vec<T>, sink: &mut S) -> bool
    where
        R: RecvItem<T>,
    {
        let n = self.rxs.len();
        // 0-capacity scratch = batching off: skip the claim entirely so the
        // per-item drain below is byte-for-byte the historical loop.
        let can_batch = scratch.capacity() > 0;
        for k in 0..n {
            let i = (self.cursor + k) % n;
            if self.closed[i] {
                continue;
            }
            loop {
                // Batch claim first (one cursor store per run).
                if can_batch {
                    let claimed =
                        RecvItem::try_recv_batch(&self.rxs[i], scratch.spare_capacity_mut());
                    if claimed > 0 {
                        // SAFETY: try_recv_batch initialized exactly the
                        // prefix.
                        unsafe { scratch.set_len(claimed) };
                        for item in scratch.drain(..) {
                            sink(item);
                        }
                        continue;
                    }
                }
                // No batchable run: one per-item try_recv resolves the
                // 0-claim (empty / in-flight / closed).
                match RecvItem::try_recv(&self.rxs[i]) {
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
}

/// Sole consumer of a [`sharded_mpsc_channel`]: owns all shard receivers and
/// aggregates them into one logical stream. **Not `Clone`** — single
/// consumer, same contract as [`MpscReceiver`].
pub struct ShardedReceiver<T: Send + 'static> {
    inner: ShardSet<MpscReceiver<T>>,
    /// Per-pass batch scratch for `drain_pass_batched`
    /// (YOUPIPE_BATCH_RECV; capacity 0 = per-item draining).
    scratch: Vec<T>,
}

impl<T: Send + 'static> ShardedReceiver<T> {
    /// One full burst pass (see [`ShardSet::drain_pass_batched`]). Returns
    /// `false` once every shard has closed (EOF).
    pub(crate) fn drain_pass<S: FnMut(T)>(&mut self, sink: &mut S) -> bool {
        self.inner.drain_pass_batched(&mut self.scratch, sink)
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
        let n = self.inner.rxs.len();
        for k in 0..n {
            let i = (self.inner.cursor + k) % n;
            if self.inner.closed[i] {
                continue;
            }
            match self.inner.rxs[i].recv() {
                Ok(item) => {
                    self.inner.cursor = (i + 1) % n;
                    return Ok(item);
                },
                Err(ChannelError::Closed) => {
                    self.inner.closed[i] = true;
                    self.inner.open -= 1;
                },
            }
        }
        Err(ChannelError::Closed)
    }
}

/// Async counterpart of [`ShardedReceiver`]: aggregates the async terminal's
/// shard receivers into one logical stream for the sole async collector.
/// **Not `Clone`** (single consumer); also `!Sync` like the underlying
/// [`MpscAsyncReceiver`] — it lives on the one thread driving the
/// collector's `block_on`.
#[cfg(feature = "tokio-runtime")]
pub struct ShardedAsyncReceiver<T: Send + Unpin + 'static> {
    inner: ShardSet<MpscAsyncReceiver<T>>,
}

#[cfg(feature = "tokio-runtime")]
impl<T: Send + Unpin + 'static> ShardedAsyncReceiver<T> {
    /// One full burst pass (see [`ShardSet::drain_pass`]). Returns `false`
    /// once every shard has closed (EOF).
    pub(crate) fn drain_pass<S: FnMut(T)>(&mut self, sink: &mut S) -> bool {
        self.inner.drain_pass(sink)
    }

    /// Awaited counterpart of [`ShardedReceiver::recv_anchor`]: park on the
    /// first open shard from `cursor`; `Err` once every shard has closed.
    ///
    /// # Waker semantics (why one shard, not an any-of-N await)
    ///
    /// Awaiting one shard's `recv()` registers the collector task's waker
    /// with that shard only — crossfire's MPSC receiver registry is a
    /// per-channel single-slot `WeakCell`, so aggregating wakers across
    /// shards means re-registering with every open shard per park cycle
    /// (`ArcWaker::new_async` allocates on each registration — the
    /// per-thread immortal-waker cache only covers *blocking* wakers).
    /// At shard counts in the tens that allocation traffic per wake would
    /// exceed the anchor cost it replaces. The one-shard anchor instead
    /// trades wake latency for batching — items landing in other shards
    /// wait for the next burst pass, the same trade the sync receiver
    /// makes (measured as a net win there, todo #1). Liveness argument is
    /// identical to the sync anchor: every open shard's producer task
    /// either sends (waking us), awaits its upstream (progress elsewhere
    /// eventually feeds it), or drops its sender (closing the shard).
    pub(crate) async fn recv_anchor(&mut self) -> Result<T, ChannelError> {
        let n = self.inner.rxs.len();
        for k in 0..n {
            let i = (self.inner.cursor + k) % n;
            if self.inner.closed[i] {
                continue;
            }
            match self.inner.rxs[i].recv().await {
                Ok(item) => {
                    self.inner.cursor = (i + 1) % n;
                    return Ok(item);
                },
                Err(ChannelError::Closed) => {
                    self.inner.closed[i] = true;
                    self.inner.open -= 1;
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
        assert_eq!(seen, [] as [usize; 0]);
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

    /// Async EOF aggregation (mirror of the sync test): shard 0's task exits
    /// before producing, the others keep flowing; the anchor skips closed
    /// shards and the stream ends only when the last shard closes. Producers
    /// use `try_send` from the test thread — the sender side needs no
    /// runtime, only the collector's anchor is driven by `block_on`.
    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn async_eof_aggregates_across_shards() {
        use futures::executor::block_on;

        let (txs, mut rx) = sharded_mpsc_async_channel::<usize>(3, 33);
        let mut it = txs.into_iter();
        let tx0 = it.next().unwrap();
        let rest: Vec<_> = it.collect();
        for tx in &rest {
            for v in 0..5 {
                tx.try_send(v).unwrap();
            }
        }
        drop(tx0);
        for tx in &rest {
            tx.try_send(999).unwrap();
        }
        drop(rest);

        let got = block_on(async {
            let mut got = Vec::new();
            while rx.drain_pass(&mut |v: usize| got.push(v)) {
                match rx.recv_anchor().await {
                    Ok(v) => got.push(v),
                    Err(ChannelError::Closed) => break,
                }
            }
            got
        });
        let mut expected: Vec<usize> = (0..5).flat_map(|v| [v, v]).collect();
        expected.extend([999, 999]);
        let mut got = got;
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    /// Async anchor parks on the live shard (skipping the closed one) and is
    /// woken by a send from another OS thread — the waker round-trip through
    /// `block_on`'s park-based executor must deliver the item exactly once.
    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn async_anchor_parks_until_send() {
        use futures::executor::block_on;

        let (txs, mut rx) = sharded_mpsc_async_channel::<usize>(2, 16);
        let mut it = txs.into_iter();
        let tx0 = it.next().unwrap();
        let tx1 = it.next().unwrap();
        let producer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            tx0.try_send(42).unwrap();
        });
        drop(tx1);
        let mut seen = block_on(async {
            let mut seen = Vec::new();
            assert!(
                rx.drain_pass(&mut |v: usize| seen.push(v)),
                "shard 0 still open while the producer sleeps"
            );
            assert_eq!(seen, [] as [usize; 0]);
            match rx.recv_anchor().await {
                Ok(v) => seen.push(v),
                Err(ChannelError::Closed) => panic!("anchor closed while shard 0 is alive"),
            }
            seen
        });
        producer.join().unwrap();
        let rest = block_on(async {
            let mut got = Vec::new();
            while rx.drain_pass(&mut |v: usize| got.push(v)) {
                match rx.recv_anchor().await {
                    Ok(v) => got.push(v),
                    Err(ChannelError::Closed) => break,
                }
            }
            got
        });
        seen.extend(rest);
        seen.sort_unstable();
        assert_eq!(seen, vec![42]);
    }
}
