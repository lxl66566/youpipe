use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::handoff::ring_buffer::SharedRingBuffer;

/// Configuration for batch draining behavior.
#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub max_wait_micros: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_wait_micros: 100,
        }
    }
}

impl BatchConfig {
    #[must_use]
    pub fn new(batch_size: usize, wait_micros: u64) -> Self {
        Self {
            max_batch_size: batch_size,
            max_wait_micros: wait_micros,
        }
    }
}

/// Batching layer on top of [`SharedRingBuffer`]. Tracks pending count and
/// drains up to `max_batch_size` items at a time.
pub struct Batcher<T> {
    ring: SharedRingBuffer<T>,
    config: BatchConfig,
    pending: AtomicUsize,
}

impl<T: Send> Batcher<T> {
    pub fn new(capacity: usize, config: BatchConfig) -> Self {
        Self {
            ring: SharedRingBuffer::new(capacity),
            config,
            pending: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let result = self.ring.push(item);
        if result.is_ok() {
            self.pending.fetch_add(1, Ordering::Release);
        }
        result
    }

    pub fn push_batch(&self, items: &[T]) -> usize
    where
        T: Clone,
    {
        let count = self.ring.push_batch(items);
        if count > 0 {
            self.pending.fetch_add(count, Ordering::Release);
        }
        count
    }

    pub fn drain(&self, dest: &mut Vec<T>) -> usize {
        let max = self.config.max_batch_size;
        let count = self.ring.pop_batch(dest, max);
        if count > 0 {
            self.pending.fetch_sub(count, Ordering::AcqRel);
        }
        count
    }

    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &BatchConfig {
        &self.config
    }
}

/// `Arc<Batcher>` wrapper for shared access.
pub struct SharedBatcher<T>(Arc<Batcher<T>>);

impl<T: Send> SharedBatcher<T> {
    #[must_use]
    pub fn new(capacity: usize, config: BatchConfig) -> Self {
        Self(Arc::new(Batcher::new(capacity, config)))
    }

    /// # Errors
    ///
    /// Returns `Err(item)` if the underlying buffer is full.
    pub fn push(&self, item: T) -> Result<(), T> {
        self.0.push(item)
    }

    pub fn push_batch(&self, items: &[T]) -> usize
    where
        T: Clone,
    {
        self.0.push_batch(items)
    }

    pub fn drain(&self, dest: &mut Vec<T>) -> usize {
        self.0.drain(dest)
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.0.pending_count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> Clone for SharedBatcher<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batcher_basic() {
        let batcher = Batcher::<i32>::new(64, BatchConfig::default());
        for i in 0..10 {
            assert!(batcher.push(i).is_ok());
        }
        assert_eq!(batcher.pending_count(), 10);
        let mut dest = Vec::new();
        let count = batcher.drain(&mut dest);
        assert_eq!(count, 10);
        assert_eq!(dest, (0..10).collect::<Vec<_>>());
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_batcher_batch_limit() {
        let config = BatchConfig::new(3, 100);
        let batcher = Batcher::<i32>::new(64, config);
        for i in 0..10 {
            assert!(batcher.push(i).is_ok());
        }
        let mut dest = Vec::new();
        let count = batcher.drain(&mut dest);
        assert_eq!(count, 3);
        assert_eq!(dest, vec![0, 1, 2]);
        let mut dest2 = Vec::new();
        let count2 = batcher.drain(&mut dest2);
        assert_eq!(count2, 3);
    }

    #[test]
    fn test_shared_batcher() {
        let b1 = SharedBatcher::<i32>::new(64, BatchConfig::default());
        let b2 = b1.clone();
        assert!(b1.push(1).is_ok());
        let mut dest = Vec::new();
        b2.drain(&mut dest);
        assert_eq!(dest, vec![1]);
    }
}
