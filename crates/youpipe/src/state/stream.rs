use crate::{
    handoff::{AsyncRecvItem, RecvItem, TryRecvError},
    state::ReorderBuffer,
};

// ── Shared terminal-drain loops ──
//
// Every streaming terminal (`.run()`'s Vec collector, `.for_each()`'s sink,
// sync and async alike) drains the final receiver with the same "burst-drain"
// loop. The four helpers below are the single implementation of unordered /
// ordered × sync / async drains, parameterized by a per-item `sink` closure —
// collect pushes into a `Vec`, for_each invokes the user's closure. After
// inlining the sink is free, so all terminals share one code path with zero
// per-item overhead.
//
// # crossfire `try_recv` quirk (load-bearing)
//
// crossfire's `try_recv` can spuriously report `Disconnected` (mapped to
// `TryRecvError::Closed`) while a sender→receiver direct-copy handoff is still
// in flight: the receiver's waker stays registered across blocking-recv
// rounds, so a sender may direct-copy an item into that waker slot — bypassing
// the queue — while the collector is in the `try_recv` burst phase, where the
// slot is never examined. If the last sender then drops, `try_recv` reports
// Closed with the item stranded, and exiting here would silently drop items
// (reproduced with pure crossfire: bounded/unbounded × mpsc/mpmc all affected;
// only the try_recv+recv *mix* triggers it — neither pure pattern does).
// Blocking `recv` *does* check the waker slot, so every `Closed` verdict below
// is confirmed with one blocking `recv` before exiting: a genuinely closed
// channel errs immediately, a spurious one yields the in-flight item.

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
                // Spurious-close guard: see the module doc above.
                Err(TryRecvError::Closed) => match rx.recv() {
                    Ok((_, item)) => sink(item),
                    Err(_) => return,
                },
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
pub(crate) fn drain_ordered<R, O>(rx: &R, expected_items: usize, mut sink: impl FnMut(O))
where
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
    loop {
        // Burst-drain: pop everything already queued without blocking.
        loop {
            match rx.try_recv() {
                Ok((seq, item)) => buffer.insert_into(seq, item, &mut sink),
                Err(TryRecvError::Empty) => break,
                // Spurious-close guard: see the module doc above.
                Err(TryRecvError::Closed) => {
                    if let Ok((seq, item)) = rx.recv() {
                        buffer.insert_into(seq, item, &mut sink);
                    } else {
                        for item in buffer.flush_remaining() {
                            sink(item);
                        }
                        return;
                    }
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
                // Spurious-close guard: see the module doc above.
                Err(TryRecvError::Closed) => match rx.recv().await {
                    Ok((_, item)) => sink(item),
                    Err(_) => return,
                },
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
    mut sink: impl FnMut(O),
)
where
    R: AsyncRecvItem<(u64, O)>,
    O: Send + 'static,
{
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
    loop {
        loop {
            match rx.try_recv() {
                Ok((seq, o)) => buffer.insert_into(seq, o, &mut sink),
                Err(TryRecvError::Empty) => break,
                // Spurious-close guard: see the module doc above.
                Err(TryRecvError::Closed) => {
                    if let Ok((seq, o)) = rx.recv().await {
                        buffer.insert_into(seq, o, &mut sink);
                    } else {
                        for item in buffer.flush_remaining() {
                            sink(item);
                        }
                        return;
                    }
                },
            }
        }
        if let Ok((seq, o)) = rx.recv().await {
            buffer.insert_into(seq, o, &mut sink);
        } else {
            for item in buffer.flush_remaining() {
                sink(item);
            }
            return;
        }
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
    drain_ordered(input_rx, expected_items, |item| results.push(item));
    results
}
