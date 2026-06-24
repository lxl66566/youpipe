/// Cache-line padded wrapper to prevent false sharing.
#[repr(C, align(64))]
#[derive(Default)]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

pub(crate) fn split_chunks<T>(items: Vec<T>, num_chunks: usize) -> Vec<Vec<T>> {
    let n = items.len();
    if n == 0 {
        return Vec::new();
    }
    let num_chunks = num_chunks.max(1).min(n);
    let base = n / num_chunks;
    let remainder = n % num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut iter = items.into_iter();
    for i in 0..num_chunks {
        let size = base + usize::from(i < remainder);
        chunks.push(iter.by_ref().take(size).collect());
    }
    chunks
}

/// Pool-size helper for tests that build an explicit `ComputePool`.
///
/// `crossbeam-epoch` 0.9.x (pulled in transitively by `crossbeam-deque`) is
/// incompatible with Miri's Stacked Borrows: its intrusive `Local` list
/// reborrows pointers that were allocated by *other* threads inside
/// `element_of`, which Miri rightly rejects as UB. This is purely an upstream
/// limitation — a standalone program using only `crossbeam-deque` reproduces
/// the identical violation, so it is not something this crate can fix.
///
/// The UB is only reached when the pool has more than one worker and actually
/// performs cross-thread `Stealer::steal` (which triggers epoch GC). Under Miri
/// we therefore collapse to a single worker. That still exercises our own
/// job/latch/join/registry unsafe code — and `WorkerThread::steal` early-exits
/// when `num_threads <= 1`, so the offending epoch-GC path is never taken.
#[cfg(test)]
pub(crate) fn miri_pool_size(n: usize) -> usize {
    if cfg!(miri) { 1 } else { n }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_even() {
        let chunks = split_chunks(vec![1, 2, 3, 4], 2);
        assert_eq!(chunks, vec![vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn test_split_uneven() {
        let chunks = split_chunks(vec![1, 2, 3, 4, 5], 2);
        assert_eq!(chunks, vec![vec![1, 2, 3], vec![4, 5]]);
    }

    #[test]
    fn test_split_empty() {
        let chunks: Vec<Vec<i32>> = split_chunks(vec![], 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_more_chunks_than_items() {
        let chunks = split_chunks(vec![1, 2], 5);
        assert_eq!(chunks, vec![vec![1], vec![2]]);
    }
}
