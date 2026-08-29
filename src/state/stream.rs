use crate::{
    handoff::{RecvItem, TryRecvError},
    state::ReorderBuffer,
};

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
///
/// # Burst-drain strategy
///
/// Mirrors the unordered collector: when multiple items land in the channel
/// before the collector loops back (common with parallel workers finishing in
/// bursts), a tight `try_recv` loop absorbs the burst without per-item
/// blocking-recv overhead (condvar/park bookkeeping inside the channel on the
/// empty path); only the first item of each burst goes through the blocking
/// `recv()`. Ordering is unaffected — the [`ReorderBuffer`] re-sequences by
/// `seq` regardless of arrival order.
#[must_use]
pub fn run_ordered_collect<R, O>(input_rx: &R, expected_items: usize) -> Vec<O>
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
    let mut results = Vec::with_capacity(expected_items);
    loop {
        // Burst-drain: pop everything already queued without blocking.
        loop {
            match input_rx.try_recv() {
                Ok((seq, item)) => buffer.insert_into(seq, item, &mut results),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    results.extend(buffer.flush_remaining());
                    return results;
                }
            }
        }
        // Queue drained but the channel may still be open — block for one.
        match input_rx.recv() {
            Ok((seq, item)) => buffer.insert_into(seq, item, &mut results),
            Err(_) => {
                results.extend(buffer.flush_remaining());
                return results;
            }
        }
    }
}
