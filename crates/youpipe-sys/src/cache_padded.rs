/// Cache-line padded wrapper to prevent false sharing.
///
/// Under `--cfg loom` the padding is dropped: the model has no cache
/// lines to alias, and `repr(align)` around loom's simulated types only
/// inflates the explored state.
#[cfg_attr(not(loom), repr(C, align(64)))]
#[derive(Default)]
pub struct CachePadded<T>(pub T);

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
