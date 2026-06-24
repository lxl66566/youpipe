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
pub struct FenceBarrier<T> {
    chunk_size: Option<usize>,
    buffer: Vec<T>,
}

impl<T> FenceBarrier<T> {
    #[must_use]
    pub fn new(mode: FenceMode) -> Self {
        Self {
            chunk_size: mode.chunk_size(),
            buffer: Vec::new(),
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
        }
    }

    /// Append an item, returning a ready batch iff the chunk threshold is hit.
    /// In [`FenceMode::Barrier`] mode this always returns `None`.
    pub fn push(&mut self, item: T) -> Option<Vec<T>> {
        self.buffer.push(item);
        if self.should_flush() {
            Some(std::mem::take(&mut self.buffer))
        } else {
            None
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
}
