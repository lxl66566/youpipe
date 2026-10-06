/// Cache-line padded wrapper to prevent false sharing.
///
/// Alignment follows crossbeam-utils: 128 bytes on x86_64 and aarch64
/// (some Intel CPUs have an adjacent-line prefetcher that fetches pairs
/// of 64 B lines, so 64 B padding still false-shares at the prefetch-pair
/// granularity; some aarch64 chips have 128 B lines outright), 64 bytes
/// elsewhere. This keeps every padded line in the process on the same
/// strategy — the crossbeam queues vendored under `youpipe-crossfire`
/// already pad to 128 B.
///
/// Under `--cfg loom` the padding is dropped: the model has no cache
/// lines to alias, and `repr(align)` around loom's simulated types only
/// inflates the explored state. Miri keeps the conservative 64 B floor
/// for the same reason (no cache lines to model, only more memory).
#[cfg_attr(
    all(
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(miri),
        not(loom),
    ),
    repr(C, align(128))
)]
#[cfg_attr(
    all(
        not(all(
            any(target_arch = "x86_64", target_arch = "aarch64"),
            not(miri),
            not(loom),
        )),
        not(loom),
    ),
    repr(C, align(64))
)]
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
