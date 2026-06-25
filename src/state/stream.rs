use crate::{handoff::Receiver, state::ReorderBuffer};

/// Drain `input_rx` in input-order (sequence-tagged) fashion, returning the
/// fully ordered result vector.
///
/// Items arrive tagged with their original sequence number `(seq, item)`;
/// out-of-order arrivals are re-sequenced through a [`ReorderBuffer`]. The
/// receiver loop exits once all senders drop, then any remaining buffered
/// items are flushed in seq order.
///
/// Used by `StreamPipeline`'s ordered run paths; kept here so the reorder
/// logic lives next to its dependencies rather than inside the builder.
pub fn run_ordered_collect<O: Send + 'static>(
    input_rx: &Receiver<(u64, O)>,
    expected_items: usize,
) -> Vec<O> {
    // Size the reorder window to the expected item count (power-of-two,
    // clamped to [1Ki, 1Mi] slots). Smaller windows are cheaper to allocate
    // and scan; larger windows tolerate more reordering. The clamp keeps both
    // tiny inputs (no over-allocation) and very large inputs (bounded memory)
    // sane.
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
    let mut results = Vec::with_capacity(expected_items);
    while let Ok((seq, item)) = input_rx.recv() {
        let ready = buffer.insert(seq, item);
        results.extend(ready);
    }
    results.extend(buffer.flush_remaining());
    results
}
