#[cfg(feature = "tokio-runtime")]
use crate::handoff::{AsyncRecvItem, ShardedAsyncReceiver};
use crate::{
    handoff::{RecvItem, ShardedReceiver, TryRecvError},
    state::ReorderBuffer,
    sync::CancellationToken,
};

/// Whether the ordered drains may validate their post-run accounting
/// (`emitted + dropped == expected`). The internal streaming terminal always
/// validates — a fired cancel token exempts the equality form because a
/// cancelled run legitimately emits fewer items (feeder and stage workers
/// stop early). The public [`run_ordered_collect`] cannot validate:
/// external callers feed arbitrary sender counts, so its `expected_items` is
/// a capacity hint, not an item-count contract.
#[derive(Clone, Copy)]
pub(crate) enum OrderedAccounting<'a> {
    Validate {
        cancel: Option<&'a CancellationToken>,
    },
    Skip,
}

/// Resolved reorder-window sizing for one ordered drain: the initial slot
/// capacity (a pre-size — the slot array still materializes lazily on the
/// first out-of-order arrival) and the hard capacity ceiling the
/// [`ReorderBuffer`] grows up to.
///
/// # Why the internal ceiling is `next_pow2(n)` and nothing smaller
///
/// A [`ReorderBuffer`] only ever aliases (and thus drops) when two *live*
/// (un-flushed) seqs land `capacity` slots apart, so a window is drop-free
/// iff it exceeds the maximum live span `max_arrived_seq − next_expected`.
/// For the streaming collectors that span is bounded by `n − 1`:
///
/// 1. The single feeder assigns seqs `0..n-1` in input order, each exactly once (`ordered` +
///    `expand` is rejected up front; cancellation only stops the feeder early, shrinking the fed
///    prefix; a fired cancel token additionally exempts the equality accounting).
/// 2. `next_expected == m` implies seq `m` was never *inserted* — inserting `m` flushes the
///    contiguous run through `m` immediately. So when any `s > m` is inserted, every seq in `[m,
///    s)` is still inside the pipeline, in the collector's batch scratch, or already occupying a
///    slot: distinct and live, at most `n` of them.
/// 3. All live seqs lie in `[0, n)` ⟹ every pair of live seqs differs by less than `n ≤
///    next_pow2(n)` = the ceiling.
///
/// # Why `initial` is only the pipeline occupancy (growth does the rest)
///
/// Sizing the window at the pipeline's in-flight occupancy (Σ channel
/// buffers + Σ worker batch claims) — the shape suggested by the 2026-10
/// review — is NOT a sound span bound: a straggler parked in one worker's
/// stage closure lets arbitrarily many successors overtake it through the
/// other workers (overtaking needs only transient co-residency in one
/// stage's worker set; over time the entire input can pass). Measured
/// (2026-10 probe, release build, pre-fix code): n = 2 M, default buffers
/// (256), 32 workers, seq 0 sleeping 400 ms — the live span reached ~2 M
/// against a pipeline occupancy of ~900 and the 1 Mi window silently
/// dropped ~1 M items (`got 1048576 of 2000000`); `with_buffer_size(64)`
/// behaved identically. Hence the occupancy estimate only *pre-sizes* the
/// window (the common-case reorder spread is what is physically in flight;
/// a mis-estimate costs one amortized doubling), while growth toward
/// `next_pow2(n)` carries correctness for skewed workloads. Memory stays
/// proportional to the *observed* span: in-order runs never allocate,
/// well-behaved runs allocate ~occupancy, and only a real straggler pays
/// toward the `n`-sized ceiling (which the run already carries twice as
/// input and output `Vec`s).
#[derive(Clone, Copy)]
pub(crate) struct OrderedWindow {
    initial: usize,
    max: usize,
}

impl OrderedWindow {
    /// Default sizing: grow to the sound `next_pow2(n)` ceiling, pre-sized
    /// to the pipeline occupancy estimate (`in_flight`, Σ channel buffers +
    /// Σ worker batch claims, accumulated during the spawn walk — see
    /// `StreamCtx::note_in_flight`), floored at 1 Ki slots to absorb
    /// ordinary bursts without a rehash.
    pub(crate) fn auto(n: usize, in_flight: usize) -> Self {
        let max = n.max(1).next_power_of_two();
        Self {
            initial: max.min((1 << 10).max(in_flight)),
            max,
        }
    }

    /// Sizing for [`run_ordered_collect`], whose `expected_items` is a
    /// capacity *hint*, not an item-count contract (external callers feed
    /// arbitrary sender counts and seq patterns): pre-size from the hint
    /// like the historical fixed window, but grow unboundedly — the public
    /// helper must not drop on a span the hint underestimated.
    pub(crate) fn open(expected: usize) -> Self {
        Self {
            initial: expected.max(1).next_power_of_two().clamp(1 << 10, 1 << 20),
            // 1 << 63: for all practical purposes unbounded (a Vec can hold
            // at most isize::MAX bytes), while staying a power of two.
            max: 1usize << (usize::BITS - 1),
        }
    }
}

/// Post-drain accounting check shared by the ordered collectors: every item
/// the run fed must be either emitted or counted as dropped by the
/// [`ReorderBuffer`]. Debug-only; release builds keep just the counter
/// ([`ReorderBuffer::dropped`]) — a window overflow never panics there.
fn verify_ordered_accounting<T>(
    buffer: &ReorderBuffer<T>,
    emitted: usize,
    expected_items: usize,
    cancel: Option<&CancellationToken>,
) {
    let dropped = buffer.dropped();
    debug_assert!(
        emitted + dropped <= expected_items,
        "ordered drain emitted {emitted} + dropped {dropped} items, more than the \
         {expected_items} fed"
    );
    debug_assert!(
        cancel.is_some_and(CancellationToken::is_cancelled) || emitted + dropped == expected_items,
        "ordered drain accounting: emitted {emitted} + dropped {dropped} != {expected_items} \
         expected — items vanished without being counted as dropped"
    );
}

// ── Shared terminal-drain loops ──
//
// Every streaming terminal (`.run()`'s Vec collector, `.for_each()`'s sink,
// sync and async alike) drains the final receiver with the same "burst-drain"
// loop. The four helpers below are the single implementation of unordered /
// ordered × sync / async drains, parameterized by a per-item `sink` closure —
// collect pushes into a `Vec`, for_each invokes the user's closure. After
// inlining the sink is free, so all terminals share one code path with zero
// per-item overhead.

/// Drain `rx` in arrival order, invoking `sink` per item.
///
/// # Burst-drain strategy
///
/// When multiple items land in the channel before the collector loops back
/// (common with parallel workers finishing in bursts), a tight `try_recv` loop
/// absorbs the burst without per-item blocking-recv overhead (condvar/park
/// bookkeeping inside the channel on the empty path); only the first item of
/// each burst goes through the blocking `recv()`.
pub(crate) fn drain_unordered<R, O>(rx: &R, sink: impl FnMut(O))
where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    drain_unordered_with(rx, crate::handoff::batch_recv_cap(), sink);
}

/// [`drain_unordered`] with an explicit batch cap (0 = per-item, the
/// historical loop byte-for-byte) so tests can drive the batch path
/// without env-var gymnastics.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_unordered_with<R, O>(rx: &R, batch_cap: usize, mut sink: impl FnMut(O))
where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    // Batch scratch, reused across bursts for the whole drain (one
    // allocation per run; capacity 0 = no alloc).
    let mut scratch: Vec<(u64, O)> = Vec::with_capacity(batch_cap);
    loop {
        if batch_cap > 0 {
            // Batch burst-drain: claim ready runs with one cursor update
            // per run (YOUPIPE_BATCH_RECV, todo #1 residual (d)). A 0-claim
            // is inconclusive (empty / first slot in-flight), so the single
            // `try_recv` below resolves Empty vs Closed before anchoring.
            loop {
                let n = rx.try_recv_batch(scratch.spare_capacity_mut());
                if n == 0 {
                    break;
                }
                // SAFETY: try_recv_batch initialized exactly scratch[..n].
                unsafe { scratch.set_len(n) };
                for (_, item) in scratch.drain(..) {
                    sink(item);
                }
            }
        } else {
            // Burst-drain: pop everything already queued without blocking.
            loop {
                match rx.try_recv() {
                    Ok((_, item)) => sink(item),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => return,
                }
            }
        }
        // Queue drained but channel may still be open — block for one.
        // (Batch path: this is also the Empty/Closed probe.)
        if batch_cap > 0 {
            match rx.try_recv() {
                Ok((_, item)) => {
                    sink(item);
                    continue;
                },
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Closed) => return,
            }
        }
        match rx.recv() {
            Ok((_, item)) => sink(item),
            Err(_) => return,
        }
    }
}

/// Drain `rx` in **input order** (sequence-tagged), invoking `sink` per item.
///
/// Items arrive tagged with their original sequence number `(seq, item)`;
/// out-of-order arrivals are re-sequenced through a [`ReorderBuffer`]. The
/// loop exits once all senders drop, then any remaining buffered items are
/// flushed in seq order.
///
/// Burst-drains like [`drain_unordered`]; ordering is unaffected — the
/// [`ReorderBuffer`] re-sequences by `seq` regardless of arrival order.
pub(crate) fn drain_ordered<R, O>(
    rx: &R,
    expected_items: usize,
    window: OrderedWindow,
    accounting: OrderedAccounting<'_>,
    sink: impl FnMut(O),
) where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    drain_ordered_with(
        rx,
        expected_items,
        window,
        accounting,
        crate::handoff::batch_recv_cap(),
        sink,
    );
}

/// [`drain_ordered`] with an explicit batch cap (0 = per-item, the
/// historical loop byte-for-byte) — same rationale as
/// [`drain_unordered_with`]. Ordering never depends on the claim size:
/// the [`ReorderBuffer`] re-sequences by `seq`.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_ordered_with<R, O>(
    rx: &R,
    expected_items: usize,
    window: OrderedWindow,
    accounting: OrderedAccounting<'_>,
    batch_cap: usize,
    mut sink: impl FnMut(O),
) where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    let mut buffer = ReorderBuffer::with_max(window.initial, window.max);
    // Count emissions for the post-drain accounting check; the counter folds
    // into the sink closure, so the per-item cost is one increment.
    let mut emitted = 0usize;
    let mut sink = |item: O| {
        emitted += 1;
        sink(item);
    };
    // Batch scratch (see `drain_unordered_with`).
    let mut scratch: Vec<(u64, O)> = Vec::with_capacity(batch_cap);
    loop {
        if batch_cap > 0 {
            loop {
                let n = rx.try_recv_batch(scratch.spare_capacity_mut());
                if n == 0 {
                    break;
                }
                // SAFETY: try_recv_batch initialized exactly scratch[..n].
                unsafe { scratch.set_len(n) };
                for (seq, item) in scratch.drain(..) {
                    buffer.insert_into(seq, item, &mut sink);
                }
            }
        } else {
            // Burst-drain: pop everything already queued without blocking.
            loop {
                match rx.try_recv() {
                    Ok((seq, item)) => buffer.insert_into(seq, item, &mut sink),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        for item in buffer.flush_remaining() {
                            sink(item);
                        }
                        if let OrderedAccounting::Validate { cancel } = accounting {
                            verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
                        }
                        return;
                    },
                }
            }
        }
        // Queue drained but channel may still be open — block for one.
        // (Batch path: this is also the Empty/Closed probe.)
        if batch_cap > 0 {
            match rx.try_recv() {
                Ok((seq, item)) => {
                    buffer.insert_into(seq, item, &mut sink);
                    continue;
                },
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Closed) => {
                    for item in buffer.flush_remaining() {
                        sink(item);
                    }
                    if let OrderedAccounting::Validate { cancel } = accounting {
                        verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
                    }
                    return;
                },
            }
        }
        if let Ok((seq, item)) = rx.recv() {
            buffer.insert_into(seq, item, &mut sink);
        } else {
            for item in buffer.flush_remaining() {
                sink(item);
            }
            if let OrderedAccounting::Validate { cancel } = accounting {
                verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
            }
            return;
        }
    }
}

/// Async counterpart of [`drain_unordered`]: `try_recv` bursts without
/// awaiting, one `recv().await` per burst to register a waker.
///
/// The burst phase matters more here than on the sync side: once the first
/// wave of async consumers wakes — tokio's coarse timer wheel batches
/// same-duration timeouts into one tick — every item after the first is
/// already queued, and a plain `while let Ok(..) = rx.recv().await` would pay
/// one waker-register/wake round-trip per item. The two-phase loop converts
/// that per-item `await` cost into per-burst cost.
#[cfg(feature = "tokio-runtime")]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_unordered_async<R, O>(rx: &R, mut sink: impl FnMut(O))
where
    R: AsyncRecvItem<(u64, O)>,
    O: Send + 'static,
{
    loop {
        // Burst-drain: pop everything already queued without awaiting.
        loop {
            match rx.try_recv() {
                Ok((_, item)) => sink(item),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => return,
            }
        }
        // Queue is drained but channel may still be open. Await exactly one
        // item to register a waker; the next iteration's burst-drain picks up
        // anything that arrived in the meantime.
        match rx.recv().await {
            Ok((_, item)) => sink(item),
            Err(_) => return,
        }
    }
}

/// Async counterpart of [`drain_ordered`]: re-sequences through a
/// [`ReorderBuffer`] while burst-draining like [`drain_unordered_async`].
#[cfg(feature = "tokio-runtime")]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_ordered_async<R, O>(
    rx: &R,
    expected_items: usize,
    window: OrderedWindow,
    accounting: OrderedAccounting<'_>,
    mut sink: impl FnMut(O),
) where
    R: AsyncRecvItem<(u64, O)>,
    O: Send + 'static,
{
    let mut buffer = ReorderBuffer::with_max(window.initial, window.max);
    // See `drain_ordered` for the accounting counter.
    let mut emitted = 0usize;
    let mut sink = |item: O| {
        emitted += 1;
        sink(item);
    };
    loop {
        loop {
            match rx.try_recv() {
                Ok((seq, o)) => buffer.insert_into(seq, o, &mut sink),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    for item in buffer.flush_remaining() {
                        sink(item);
                    }
                    if let OrderedAccounting::Validate { cancel } = accounting {
                        verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
                    }
                    return;
                },
            }
        }
        if let Ok((seq, o)) = rx.recv().await {
            buffer.insert_into(seq, o, &mut sink);
        } else {
            for item in buffer.flush_remaining() {
                sink(item);
            }
            if let OrderedAccounting::Validate { cancel } = accounting {
                verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
            }
            return;
        }
    }
}

/// Sharded counterpart of [`drain_unordered`]: same anchor + burst-drain
/// rhythm, but the "burst" is a full round-robin pass over every shard and
/// the anchor blocks on one open shard (see [`ShardedReceiver`]). EOF is the
/// aggregation of all shards closing.
///
/// Unordered output interleaves per-shard runs — completion order per shard
/// is preserved, arrival order across shards follows the rotating poll
/// start.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_unordered_sharded<O, S: FnMut(O)>(
    rx: &mut ShardedReceiver<(u64, O)>,
    mut sink: S,
) where
    O: Send + 'static,
{
    loop {
        let mut tagged_sink = |(_, item): (u64, O)| sink(item);
        if !rx.drain_pass(&mut tagged_sink) {
            return;
        }
        match rx.recv_anchor() {
            Ok((_, item)) => sink(item),
            Err(_) => return,
        }
    }
}

/// Sharded counterpart of [`drain_ordered`]: identical to the single-channel
/// loop except the burst pass fans across shards — ordering never depended
/// on channel arrival order ([`ReorderBuffer`] re-sequences by `seq`), so
/// sharding is transparent to the ordered contract. Accounting and the
/// window-overflow semantics are shared verbatim.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_ordered_sharded<O, S: FnMut(O)>(
    rx: &mut ShardedReceiver<(u64, O)>,
    expected_items: usize,
    window: OrderedWindow,
    accounting: OrderedAccounting<'_>,
    mut sink: S,
) where
    O: Send + 'static,
{
    let mut buffer = ReorderBuffer::with_max(window.initial, window.max);
    // See `drain_ordered` for the accounting counter.
    let mut emitted = 0usize;
    let mut sink = |item: O| {
        emitted += 1;
        sink(item);
    };
    loop {
        let mut tagged_sink = |(seq, item): (u64, O)| buffer.insert_into(seq, item, &mut sink);
        if !rx.drain_pass(&mut tagged_sink) {
            break;
        }
        match rx.recv_anchor() {
            Ok((seq, item)) => buffer.insert_into(seq, item, &mut sink),
            Err(_) => break,
        }
    }
    for item in buffer.flush_remaining() {
        sink(item);
    }
    if let OrderedAccounting::Validate { cancel } = accounting {
        verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
    }
}

/// Async counterpart of [`drain_unordered_sharded`]: the burst pass is the
/// same round-robin over every shard (plain `try_recv`, no awaiting); only
/// the anchor awaits one open shard (see
/// [`ShardedAsyncReceiver::recv_anchor`](crate::handoff::ShardedAsyncReceiver::recv_anchor)
/// for the one-shard waker rationale). EOF is the aggregation of all shards
/// closing.
#[cfg(feature = "tokio-runtime")]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_unordered_async_sharded<O, S: FnMut(O)>(
    rx: &mut ShardedAsyncReceiver<(u64, O)>,
    mut sink: S,
) where
    O: Send + Unpin + 'static,
{
    loop {
        let mut tagged_sink = |(_, item): (u64, O)| sink(item);
        if !rx.drain_pass(&mut tagged_sink) {
            return;
        }
        match rx.recv_anchor().await {
            Ok((_, item)) => sink(item),
            Err(_) => return,
        }
    }
}

/// Async counterpart of [`drain_ordered_sharded`]: identical to the
/// single-channel async loop except the burst pass fans across shards —
/// ordering never depended on channel arrival order ([`ReorderBuffer`]
/// re-sequences by `seq`), so sharding is transparent to the ordered
/// contract. Accounting and the window-overflow semantics are shared
/// verbatim.
#[cfg(feature = "tokio-runtime")]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_ordered_async_sharded<O, S: FnMut(O)>(
    rx: &mut ShardedAsyncReceiver<(u64, O)>,
    expected_items: usize,
    window: OrderedWindow,
    accounting: OrderedAccounting<'_>,
    mut sink: S,
) where
    O: Send + Unpin + 'static,
{
    let mut buffer = ReorderBuffer::with_max(window.initial, window.max);
    // See `drain_ordered` for the accounting counter.
    let mut emitted = 0usize;
    let mut sink = |item: O| {
        emitted += 1;
        sink(item);
    };
    loop {
        let mut tagged_sink = |(seq, item): (u64, O)| buffer.insert_into(seq, item, &mut sink);
        if !rx.drain_pass(&mut tagged_sink) {
            break;
        }
        match rx.recv_anchor().await {
            Ok((seq, item)) => buffer.insert_into(seq, item, &mut sink),
            Err(_) => break,
        }
    }
    for item in buffer.flush_remaining() {
        sink(item);
    }
    if let OrderedAccounting::Validate { cancel } = accounting {
        verify_ordered_accounting(&buffer, emitted, expected_items, cancel);
    }
}

/// Drain `input_rx` in input-order (sequence-tagged) fashion, returning the
/// fully ordered result vector.
///
/// Items arrive tagged with their original sequence number `(seq, item)`;
/// out-of-order arrivals are re-sequenced through a [`ReorderBuffer`]. The
/// receiver loop exits once all senders drop, then any remaining buffered
/// items are flushed in seq order.
///
/// Generic over the receiver type so it works with both MPMC (`Receiver`) and
/// MPSC (`MpscReceiver`) channels.
#[must_use]
pub fn run_ordered_collect<R, O>(input_rx: &R, expected_items: usize) -> Vec<O>
where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    let mut results = Vec::with_capacity(expected_items);
    drain_ordered(
        input_rx,
        expected_items,
        OrderedWindow::open(expected_items),
        OrderedAccounting::Skip,
        |item| results.push(item),
    );
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel;

    /// Pinned-window overflow through the real drain loop (the
    /// `with_reorder_window` / `ReorderBuffer::with_max` semantics): with a
    /// 1 Ki ceiling and seq 0 withheld, seq `1 Ki + 1` aliases seq 1's slot
    /// and the late seq 0 aliases seq `1 Ki`'s (slot 0, wrapped). Both drops
    /// are counted and the post-drain accounting (`emitted + dropped ==
    /// expected`, no cancel token) must close exactly. (The historical
    /// variant of this test exercised the 1 Mi auto clamp; the auto window
    /// now grows instead of dropping — see
    /// `drain_ordered_auto_window_grows_no_drops`.)
    #[test]
    fn drain_ordered_counts_window_overflow_drops() {
        const SPAN: u64 = (1 << 10) + 1; // seqs 1..=SPAN, skipping seq 0
        const DROPPED: usize = 2; // seqs 1 and 1 Ki (both aliased, see doc)
        let expected: usize = usize::try_from(SPAN).unwrap() + 1; // + seq 0
        let (tx, rx) = channel::<(u64, u64)>(1024);
        let feeder = std::thread::spawn(move || {
            for seq in 1..=SPAN {
                tx.send((seq, seq)).unwrap();
            }
            tx.send((0, 0)).unwrap();
        });
        let mut out = Vec::new();
        drain_ordered(
            &rx,
            expected,
            OrderedWindow {
                initial: 1 << 10,
                max: 1 << 10,
            },
            OrderedAccounting::Validate { cancel: None },
            |i| {
                out.push(i);
            },
        );
        feeder.join().unwrap();
        // expected fed == SPAN + 1 emitted + DROPPED; seqs 1 and 1 Ki were
        // overwritten. (The accounting check inside `drain_ordered` already
        // asserted `emitted + dropped == expected`; these asserts pin down
        // WHICH items vanished.)
        assert_eq!(out.len(), expected - DROPPED);
        assert_eq!(out[0], 0, "seq 0 unblocks the prefix flush");
        assert!(!out.contains(&1), "the first aliased item must not survive");
        assert!(
            !out.contains(&(1 << 10)),
            "the second aliased item must not survive"
        );
        let mut sorted = out.clone();
        sorted.sort_unstable();
        let expected_items: Vec<u64> = (0..=SPAN).filter(|&s| s != 1 && s != (1 << 10)).collect();
        assert_eq!(sorted, expected_items);
    }

    /// The auto window (B6 fix): a span far beyond the 1 Ki initial size and
    /// the pipeline occupancy must grow the reorder buffer instead of
    /// dropping — this is the drain-level shape of the review's straggler
    /// repro (seq 0 withheld while ~1 Ki +ε successors arrive).
    #[test]
    fn drain_ordered_auto_window_grows_no_drops() {
        const N: u64 = 5000; // >> initial 1 Ki, >> any in-flight estimate
        let n = usize::try_from(N).unwrap();
        let (tx, rx) = channel::<(u64, u64)>(64);
        let feeder = std::thread::spawn(move || {
            for seq in 1..N {
                tx.send((seq, seq)).unwrap();
            }
            tx.send((0, 0)).unwrap();
        });
        let mut out = Vec::new();
        drain_ordered(
            &rx,
            n,
            // Auto with a small occupancy estimate — exactly what a
            // small-buffer pipeline resolves to.
            OrderedWindow::auto(n, 64 + 8),
            OrderedAccounting::Validate { cancel: None },
            |i| out.push(i),
        );
        feeder.join().unwrap();
        assert_eq!(out, (0..N).collect::<Vec<_>>());
    }

    /// Batch-cap equivalence of the unordered drain: cap 0 (the historical
    /// per-item loop) and cap N (the batch path) must deliver the identical
    /// multiset under concurrent producers — a lost or duplicated item
    /// inside a batch claim would show up here.
    #[test]
    fn drain_unordered_batch_cap_equivalence() {
        const P: usize = 4;
        const N: u64 = 2_000;
        for cap in [0usize, 3] {
            let (tx, rx) = channel::<(u64, u64)>(16);
            let mut hs = Vec::new();
            for p in 0..P {
                let tx = tx.clone();
                let base = p as u64 * N;
                hs.push(std::thread::spawn(move || {
                    for i in 0..N {
                        tx.send((0, base + i)).unwrap();
                    }
                }));
            }
            drop(tx);
            let mut got = Vec::new();
            drain_unordered_with(&rx, cap, |v| got.push(v));
            for h in hs {
                h.join().unwrap();
            }
            got.sort_unstable();
            assert_eq!(got, (0..P as u64 * N).collect::<Vec<_>>(), "cap {cap}");
        }
    }

    /// Ordered drain over out-of-order arrivals with the batch path on: the
    /// `ReorderBuffer` must re-sequence batched arrival runs exactly as it
    /// does per-item arrivals.
    #[test]
    fn drain_ordered_batch_resequences_out_of_order() {
        const N: u64 = 1_000;
        let (tx, rx) = channel::<(u64, u64)>(64);
        // feed in a stride pattern so every batch claim (cap 8) mixes
        // non-consecutive seqs
        let feeder = std::thread::spawn(move || {
            for k in 0..8 {
                for seq in (k..N).step_by(8) {
                    tx.send((seq, seq)).unwrap();
                }
            }
            drop(tx);
        });
        let mut out = Vec::new();
        drain_ordered_with(
            &rx,
            usize::try_from(N).unwrap(),
            OrderedWindow::auto(usize::try_from(N).unwrap(), 64),
            OrderedAccounting::Validate { cancel: None },
            8,
            |v| out.push(v),
        );
        feeder.join().unwrap();
        assert_eq!(out, (0..N).collect::<Vec<_>>());
    }

    /// EOF with the batch path on: a channel whose senders dropped with
    /// items queued must deliver every item and then return (the 0-claim →
    /// try_recv probe hand-off).
    #[test]
    fn drain_unordered_batch_eof_after_close() {
        let (tx, rx) = channel::<(u64, u64)>(32);
        for i in 0..32u64 {
            tx.send((i, i)).unwrap();
        }
        drop(tx);
        let mut got = Vec::new();
        drain_unordered_with(&rx, 8, |v| got.push(v));
        assert_eq!(got, (0..32).collect::<Vec<_>>());
    }

    /// P-1 behavior pin: the auto window pre-sizes to the pipeline
    /// occupancy (floored at 1 Ki), never to a fixed 1 Mi — a large-`n`
    /// ordered run with a small pipeline allocates 16 KiB, not 16 MiB —
    /// while the growth ceiling stays at the sound `next_pow2(n)`.
    #[test]
    fn ordered_window_auto_presizes_to_occupancy() {
        // Large n, small occupancy (default buffers, few workers).
        let w = OrderedWindow::auto(1 << 21, 900);
        assert_eq!(w.initial, 1 << 10);
        assert_eq!(w.max, 1 << 21);
        // Occupancy above the floor but below the ceiling: pre-size to it
        // (rounded up by `ReorderBuffer::with_max`).
        let w = OrderedWindow::auto(1 << 21, 70_000);
        assert_eq!(w.initial, 70_000);
        assert_eq!(w.max, 1 << 21);
        // Occupancy above n (e.g. `buffer ≥ n`): the n cap wins.
        let w = OrderedWindow::auto(1_000, 1_000_000);
        assert_eq!(w.initial, 1_024);
        assert_eq!(w.max, 1_024);
        // Tiny n: the 1 Ki floor cannot exceed the sound ceiling.
        let w = OrderedWindow::auto(10, 0);
        assert_eq!(w.initial, 16);
        assert_eq!(w.max, 16);
    }

    /// A fired cancel token exempts the equality form: the run fed fewer
    /// items than `expected_items` (feeder stopped early) and must not trip
    /// the debug assertion.
    #[test]
    fn drain_ordered_cancelled_run_skips_equality() {
        // Capacity >= item count: the sends happen before the drain on this
        // same thread, so a bounded send would deadlock on backpressure.
        let (tx, rx) = channel::<(u64, u64)>(16);
        for seq in 0..10u64 {
            tx.send((seq, seq)).unwrap();
        }
        drop(tx);
        let token = CancellationToken::new();
        token.cancel();
        let mut out = Vec::new();
        drain_ordered(
            &rx,
            100,
            OrderedWindow::auto(100, 0),
            OrderedAccounting::Validate {
                cancel: Some(&token),
            },
            |i| {
                out.push(i);
            },
        );
        assert_eq!(out, (0..10).collect::<Vec<_>>());
    }
}
