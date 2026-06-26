use crate::handoff::SharedEventCount;

pub struct FenceBarrier<T> {
    capacity: usize,
    chunk_size: Option<usize>,
    buffer: Vec<T>,
    event: SharedEventCount,
}

impl<T> FenceBarrier<T> {
    #[must_use]
    pub fn new(capacity: usize, chunk_size: Option<usize>) -> Self {
        Self {
            capacity,
            chunk_size,
            buffer: Vec::new(),
            event: SharedEventCount::new(),
        }
    }

    pub fn push(&mut self, item: T) -> Option<Vec<T>> {
        self.buffer.push(item);
        if self.should_flush() {
            let batch = std::mem::take(&mut self.buffer);
            self.event.notify();
            Some(batch)
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<Vec<T>> {
        if self.buffer.is_empty() {
            None
        } else {
            let batch = std::mem::take(&mut self.buffer);
            self.event.notify();
            Some(batch)
        }
    }

    fn should_flush(&self) -> bool {
        match self.chunk_size {
            Some(cs) => self.buffer.len() >= cs,
            None => false,
        }
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.capacity
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

    #[test]
    fn test_fence_chunked() {
        let mut fence = FenceBarrier::<i32>::new(100, Some(3));
        assert!(fence.push(1).is_none());
        assert!(fence.push(2).is_none());
        let batch = fence.push(3);
        assert_eq!(batch, Some(vec![1, 2, 3]));
        assert!(fence.push(4).is_none());
        let remaining = fence.flush();
        assert_eq!(remaining, Some(vec![4]));
    }

    #[test]
    fn test_fence_no_chunk() {
        let mut fence = FenceBarrier::<i32>::new(100, None);
        for i in 0..10 {
            assert!(fence.push(i).is_none());
        }
        let remaining = fence.flush();
        assert_eq!(remaining, Some((0..10).collect::<Vec<_>>()));
    }
}
