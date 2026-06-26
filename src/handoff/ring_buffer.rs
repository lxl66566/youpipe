use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[repr(C, align(64))]
struct CachePadded<T>(T);

/// Lock-free single-producer single-consumer ring buffer (power-of-2 capacity).
pub struct RingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be a power of 2");
        let cap = capacity;
        let buffer: Vec<UnsafeCell<MaybeUninit<T>>> = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            buffer: buffer.into_boxed_slice(),
            cap,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn mask(&self) -> usize {
        self.cap - 1
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail - head >= self.cap {
            return Err(item);
        }
        unsafe {
            let slot = self.buffer.get_unchecked(tail & self.mask()).get();
            slot.write(MaybeUninit::new(item));
        }
        self.tail.0.store(tail + 1, Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head >= tail {
            return None;
        }
        let item = unsafe {
            let slot = self.buffer.get_unchecked(head & self.mask()).get();
            slot.read().assume_init()
        };
        self.head.0.store(head + 1, Ordering::Release);
        Some(item)
    }

    pub fn push_batch(&self, items: &[T]) -> usize
    where
        T: Clone,
    {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        let available = self.cap - (tail - head);
        let count = available.min(items.len());
        for (i, item) in items.iter().enumerate().take(count) {
            unsafe {
                let slot = self.buffer.get_unchecked((tail + i) & self.mask()).get();
                slot.write(MaybeUninit::new(item.clone()));
            }
        }
        self.tail.0.store(tail + count, Ordering::Release);
        count
    }

    pub fn pop_batch(&self, dest: &mut Vec<T>, max_count: usize) -> usize {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        let count = (tail - head).min(max_count);
        if count == 0 {
            return 0;
        }
        dest.reserve(count);
        for i in 0..count {
            let item = unsafe {
                let slot = self.buffer.get_unchecked((head + i) & self.mask()).get();
                slot.read().assume_init()
            };
            dest.push(item);
        }
        self.head.0.store(head + count, Ordering::Release);
        count
    }

    pub fn len(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        tail - head
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn remaining(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        self.cap - (tail - head)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for i in head..tail {
            unsafe {
                self.buffer[i & self.mask()].get_mut().assume_init_drop();
            }
        }
    }
}

/// `Arc<RingBuffer>` wrapper for shared access.
pub struct SharedRingBuffer<T>(Arc<RingBuffer<T>>);

impl<T: Send> SharedRingBuffer<T> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(RingBuffer::new(capacity)))
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        self.0.push(item)
    }

    #[must_use]
    pub fn pop(&self) -> Option<T> {
        self.0.pop()
    }

    pub fn push_batch(&self, items: &[T]) -> usize
    where
        T: Clone,
    {
        self.0.push_batch(items)
    }

    pub fn pop_batch(&self, dest: &mut Vec<T>, max_count: usize) -> usize {
        self.0.pop_batch(dest, max_count)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.0.remaining()
    }
}

impl<T> Clone for SharedRingBuffer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spsc_basic() {
        let buf = RingBuffer::<i32>::new(4);
        assert!(buf.push(1).is_ok());
        assert!(buf.push(2).is_ok());
        assert!(buf.push(3).is_ok());
        assert!(buf.push(4).is_ok());
        assert!(buf.push(5).is_err());
        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), Some(4));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn test_push_pop_interleaved() {
        let buf = RingBuffer::<i32>::new(8);
        for i in 0..100 {
            assert!(buf.push(i).is_ok());
            assert_eq!(buf.pop(), Some(i));
        }
    }

    #[test]
    fn test_batch_operations() {
        let buf = RingBuffer::<i32>::new(16);
        let items: Vec<i32> = (0..10).collect();
        let pushed = buf.push_batch(&items);
        assert_eq!(pushed, 10);
        let mut dest = Vec::new();
        let popped = buf.pop_batch(&mut dest, 20);
        assert_eq!(popped, 10);
        assert_eq!(dest, items);
    }

    #[test]
    fn test_batch_partial() {
        let buf = RingBuffer::<i32>::new(4);
        assert!(buf.push(1).is_ok());
        assert!(buf.push(2).is_ok());
        let items: Vec<i32> = (10..20).collect();
        let pushed = buf.push_batch(&items);
        assert_eq!(pushed, 2);
        let mut dest = Vec::new();
        assert_eq!(buf.pop_batch(&mut dest, 3), 3);
        assert_eq!(dest, vec![1, 2, 10]);
    }

    #[test]
    fn test_shared_ring_buffer() {
        let buf = SharedRingBuffer::<i32>::new(8);
        let buf2 = buf.clone();
        assert!(buf.push(42).is_ok());
        assert_eq!(buf2.pop(), Some(42));
    }
}
