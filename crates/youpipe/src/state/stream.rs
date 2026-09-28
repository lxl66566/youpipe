use crate::{
    handoff::{AsyncRecvItem, RecvItem, ShardedAsyncReceiver, ShardedReceiver, TryRecvError},
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
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_unordered<R, O>(rx: &R, mut sink: impl FnMut(O))
where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    loop {
        // Burst-drain: pop everything already queued without blocking.
        loop {
            match rx.try_recv() {
                Ok((_, item)) => sink(item),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => return,
            }
        }
        // Queue drained but channel may still be open — block for one.
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
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn drain_ordered<R, O>(
    rx: &R,
    expected_items: usize,
    accounting: OrderedAccounting<'_>,
    mut sink: impl FnMut(O),
) where
    R: RecvItem<(u64, O)>,
    O: Send + 'static,
{
    // Size the reorder window to the expected item count (power-of-two,
    // clamped to [1Ki, 1Mi] slots). Smaller windows are cheaper to allocate
    // and scan; larger windows tolerate more reordering. The clamp keeps both
    // tiny inputs (no over-allocation) and very large inputs (bounded memory)
    // sane.
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
    // Count emissions for the post-drain accounting check; the counter folds
    // into the sink closure, so the per-item cost is one increment.
    let mut emitted = 0usize;
    let mut sink = |item: O| {
        emitted += 1;
        sink(item);
    };
    loop {
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
        // Queue drained but channel may still be open — block for one.
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
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_ordered_async<R, O>(
    rx: &R,
    expected_items: usize,
    accounting: OrderedAccounting<'_>,
    mut sink: impl FnMut(O),
) where
    R: AsyncRecvItem<(u64, O)>,
    O: Send + 'static,
{
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
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
    accounting: OrderedAccounting<'_>,
    mut sink: S,
) where
    O: Send + 'static,
{
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
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
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn drain_ordered_async_sharded<O, S: FnMut(O)>(
    rx: &mut ShardedAsyncReceiver<(u64, O)>,
    expected_items: usize,
    accounting: OrderedAccounting<'_>,
    mut sink: S,
) where
    O: Send + Unpin + 'static,
{
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
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
    drain_ordered(input_rx, expected_items, OrderedAccounting::Skip, |item| {
        results.push(item);
    });
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel;

    /// Window overflow through the real drain loop. The window clamp
    /// (`next_power_of_two().clamp(1 Ki, 1 Mi)`) only lets an alias happen
    /// when the outstanding count exceeds the 1 Mi cap, so this feeds
    /// `1 Mi + 1` items while skipping seq 0, then seq 0 to unblock the
    /// prefix. Two aliases fire: seq `1 Mi + 1` lands on seq 1's slot, and
    /// the late seq 0 lands on seq `1 Mi`'s slot (slot 0 — by then the
    /// window has wrapped). A feeder thread is required — sending 1M items
    /// into a bounded channel from the drain thread itself would deadlock on
    /// backpressure. Both drops are counted and the post-drain accounting
    /// (`emitted + dropped == expected`, no cancel token) must close exactly.
    #[test]
    fn drain_ordered_counts_window_overflow_drops() {
        const SPAN: u64 = (1 << 20) + 1; // seqs 1..=SPAN, skipping seq 0
        const DROPPED: usize = 2; // seqs 1 and 1 Mi (both aliased, see doc)
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
            OrderedAccounting::Validate { cancel: None },
            |i| {
                out.push(i);
            },
        );
        feeder.join().unwrap();
        // expected fed == SPAN + 1 emitted + DROPPED; seqs 1 and 1 Mi were
        // overwritten. (The accounting check inside `drain_ordered` already
        // asserted `emitted + dropped == expected`; these asserts pin down
        // WHICH items vanished.)
        assert_eq!(out.len(), expected - DROPPED);
        assert_eq!(out[0], 0, "seq 0 unblocks the prefix flush");
        assert!(!out.contains(&1), "the first aliased item must not survive");
        assert!(
            !out.contains(&(1 << 20)),
            "the second aliased item must not survive"
        );
        let mut sorted = out.clone();
        sorted.sort_unstable();
        let expected_items: Vec<u64> = (0..=SPAN).filter(|&s| s != 1 && s != (1 << 20)).collect();
        assert_eq!(sorted, expected_items);
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
