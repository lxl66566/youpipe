//! Alignment contract of `CachePadded` (P-5): the padding strategy is
//! target-gated like crossbeam-utils — 128 B on x86_64/aarch64 (adjacent
//! line prefetcher / 128 B lines), 64 B elsewhere; dropped under loom.
//! A regression to a flat `align(64)` would silently reintroduce
//! prefetch-pair false sharing on the pool's contended lines.

use std::mem::align_of;

use youpipe_sys::CachePadded;

#[test]
fn cache_padded_alignment_matches_target() {
    #[cfg(loom)]
    assert_eq!(align_of::<CachePadded<u8>>(), 1);
    #[cfg(all(not(loom), any(target_arch = "x86_64", target_arch = "aarch64"), not(miri)))]
    assert_eq!(align_of::<CachePadded<u8>>(), 128);
    #[cfg(all(
        not(loom),
        not(all(any(target_arch = "x86_64", target_arch = "aarch64"), not(miri)))
    ))]
    assert_eq!(align_of::<CachePadded<u8>>(), 64);
}
