/// Cache-line padded wrapper to prevent false sharing.
#[repr(C, align(64))]
#[derive(Default)]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> CachePadded<T> {
    /// Wrap `value` in its own cache line.
    #[inline]
    pub(crate) fn new(value: T) -> Self {
        CachePadded(value)
    }
}

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
