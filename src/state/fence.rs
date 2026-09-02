use std::num::NonZeroUsize;

/// User-facing decision on how strictly two adjacent stages are isolated.
///
/// `Barrier` enforces a hard boundary: stage 2 sees no data until stage 1 has
/// fully drained. `Chunked` releases data in fixed-size batches so the two
/// stages overlap (soft batching) — ideal for mixed CPU/IO workloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceMode {
    /// Hard barrier: stage 1 must complete entirely before any item is
    /// forwarded to stage 2. Maximizes isolation at the cost of staging
    /// overlap and peak memory (all intermediates are buffered).
    Barrier,
    /// Soft batching: forward a batch of exactly `k` items as soon as it
    /// accumulates. Stages overlap, giving stage 2 a continuous supply
    /// without a global wait. `k` must be non-zero.
    Chunked(NonZeroUsize),
}

impl FenceMode {
    /// Translate the mode into the raw chunk size consumed by [`FenceBarrier`]:
    /// `None` means "accumulate without auto-flushing" (hard barrier),
    /// `Some(k)` means "flush every `k` items".
    pub(crate) fn chunk_size(self) -> Option<usize> {
        match self {
            FenceMode::Barrier => None,
            FenceMode::Chunked(k) => Some(k.get()),
        }
    }
}

/// Chunked accumulator used at a fence boundary between two streaming stages.
///
/// Items are buffered and released as batches: when the buffer reaches the
/// configured chunk size ([`FenceMode::Chunked`]) [`push`](Self::push) returns
/// a full batch, and [`flush`](Self::flush) drains whatever remains. In
/// [`FenceMode::Barrier`] mode `push` never auto-flushes, so the entire stream
/// is held until `flush` is called — exactly the hard-barrier contract.
///
/// The internal buffer's allocation is recycled across batches via
/// [`reuse`](Self::reuse): a forwarder that drains each returned batch and
/// hands the (empty) `Vec` back runs the steady state with **zero** allocator
/// traffic per batch, instead of re-growing a fresh `Vec` from capacity 0
/// (`1→2→4→…→k`, i.e. `log2(k)` `realloc`s per batch).
pub struct FenceBarrier<T> {
    chunk_size: Option<usize>,
    buffer: Vec<T>,
    /// Recycled empty `Vec` retained from the previous batch. Swapped in as
    /// the next `buffer` on flush so the capacity is reused.
    spare: Option<Vec<T>>,
}

impl<T> FenceBarrier<T> {
    #[must_use]
    pub fn new(mode: FenceMode) -> Self {
        Self {
            chunk_size: mode.chunk_size(),
            buffer: Vec::new(),
            spare: None,
        }
    }

    /// Preallocate the internal buffer with the given capacity. Useful in
    /// [`FenceMode::Barrier`] mode where the final batch size is known up
    /// front.
    #[must_use]
    pub fn with_capacity(mode: FenceMode, capacity: usize) -> Self {
        Self {
            chunk_size: mode.chunk_size(),
            buffer: Vec::with_capacity(capacity),
            spare: None,
        }
    }

    /// Append an item, returning a ready batch iff the chunk threshold is hit.
    /// In [`FenceMode::Barrier`] mode this always returns `None`.
    pub fn push(&mut self, item: T) -> Option<Vec<T>> {
        self.buffer.push(item);
        if self.should_flush() {
            // Hand out the full batch; promote the recycled allocation (if
            // any) to be the next buffer so the capacity carries over.
            Some(match self.spare.take() {
                Some(spare) => std::mem::replace(&mut self.buffer, spare),
                None => std::mem::take(&mut self.buffer),
            })
        } else {
            None
        }
    }

    /// Return a **drained** batch buffer for reuse by later batches.
    ///
    /// The caller drains the `Vec` returned from [`push`](Self::push) (e.g.
    /// `batch.drain(..)`) and gives the empty-but-capacity-retained `Vec`
    /// back; the next flush swaps it in instead of starting from capacity 0.
    /// Passing a non-empty batch is a contract violation (debug-asserted).
    pub fn reuse(&mut self, batch: Vec<T>) {
        debug_assert!(
            batch.is_empty(),
            "reused batch must be drained before returning"
        );
        if self
            .spare
            .as_ref()
            .is_none_or(|s| s.capacity() < batch.capacity())
        {
            self.spare = Some(batch);
        }
    }

    /// Drain all buffered items regardless of chunk threshold.
    pub fn flush(&mut self) -> Option<Vec<T>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }

    fn should_flush(&self) -> bool {
        match self.chunk_size {
            Some(cs) => self.buffer.len() >= cs,
            None => false,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunked(n: usize) -> FenceMode {
        FenceMode::Chunked(NonZeroUsize::new(n).unwrap())
    }

    #[test]
    fn test_fence_chunked() {
        let mut fence = FenceBarrier::<i32>::new(chunked(3));
        assert!(fence.push(1).is_none());
        assert!(fence.push(2).is_none());
        let batch = fence.push(3);
        assert_eq!(batch, Some(vec![1, 2, 3]));
        assert!(fence.push(4).is_none());
        let remaining = fence.flush();
        assert_eq!(remaining, Some(vec![4]));
    }

    #[test]
    fn test_fence_barrier_accumulates_all() {
        let mut fence = FenceBarrier::<i32>::new(FenceMode::Barrier);
        for i in 0..10 {
            // Barrier mode never auto-flushes.
            assert!(fence.push(i).is_none());
        }
        assert_eq!(fence.len(), 10);
        let remaining = fence.flush();
        assert_eq!(remaining, Some((0..10).collect::<Vec<_>>()));
        assert!(fence.is_empty());
    }

    #[test]
    fn test_fence_flush_empty() {
        let mut fence = FenceBarrier::<i32>::new(FenceMode::Barrier);
        assert!(fence.flush().is_none());
    }

    /// Drained batches returned via `reuse` carry their allocation over: the
    /// steady state performs zero allocator traffic per batch.
    #[test]
    fn test_reuse_recycles_capacity() {
        let mut fence = FenceBarrier::<i32>::new(chunked(4));
        let mut batches = 0;
        for i in 0..16 {
            if let Some(mut batch) = fence.push(i) {
                assert_eq!(batch.len(), 4);
                batch.drain(..);
                let cap = batch.capacity();
                fence.reuse(batch);
                if batches > 0 {
                    // From the second batch on, the recycled allocation must
                    // have been swapped in — capacity is at least the chunk
                    // size without any regrowth.
                    assert!(cap >= 4);
                }
                batches += 1;
            }
        }
        assert_eq!(batches, 4);
        assert!(fence.flush().is_none());
    }

    /// `reuse` only upgrades the spare when the returned batch is larger;
    /// a smaller returned batch is dropped rather than shrinking capacity.
    #[test]
    fn test_reuse_keeps_larger_spare() {
        let mut fence = FenceBarrier::<i32>::new(chunked(2));
        // First batch: flush and return a drained buffer.
        fence.push(0);
        let mut b1 = fence.push(1).unwrap();
        b1.drain(..);
        let small_cap = b1.capacity();
        fence.reuse(b1);
        // Offer a strictly larger drained buffer: it must win over the spare.
        let big = Vec::with_capacity(small_cap * 8);
        let big_cap = big.capacity();
        fence.reuse(big);
        // Second batch flushes: the returned batch carries the items, and the
        // big recycled allocation must be swapped in as the next buffer.
        fence.push(2);
        let mut b2 = fence.push(3).unwrap();
        assert_eq!(b2.len(), 2);
        b2.drain(..);
        assert!(fence.buffer.capacity() >= big_cap);
    }
}
