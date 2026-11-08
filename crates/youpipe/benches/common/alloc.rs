//! Allocator policy pin shared by every bench harness (criterion targets
//! via `common/mod.rs`, the `harness = false` horizontal bench via a
//! `#[path]` include).

/// Pin glibc's allocator policy for the whole bench process.
///
/// Measured 2026-11-08 on `stream_pipeline/single_stage_{unord,ord}ered`
/// /100000, whose timed iteration frees two ~800 KiB buffers (input clone +
/// output): a process that only runs the 100K id is fast and flat (~62 µs in
/// every solo draw), but in a group pass — after the 1K-size ids ran in the
/// same process — it lands in a slow mode: 105–271 µs medians, per-sample
/// 102–617 µs (same session, one binary per side). strace on the two modes:
/// 7499 brk calls vs 9. The two mallopt calls below, via LD_PRELOAD or this
/// in-process pin (64 MiB threshold at measurement time), restore the solo
/// number in group passes (5 clean draws 61.2–68.6 µs medians).
///
/// Mechanism: freeing a large mmap'd chunk RAISES glibc's dynamic mmap
/// threshold to that chunk's size and sets the trim threshold to 2x it;
/// same-size allocations then come from the sbrk heap. Whether the process rests in a
/// stable heap or oscillates brk-shrink/brk-regrow + first-touch faults
/// depends on what earlier bench ids left in the heap and bins — hence the
/// id-order dependence. Per the glibc manual, an explicit mallopt disables
/// the dynamic adjustment, which makes the outcome order-independent:
/// M_TRIM_THRESHOLD = -1 (heap never shrinks — the fast steady state
/// becomes permanent; bench processes exit anyway) and M_MMAP_THRESHOLD
/// above every criterion buffer (no per-iteration mmap/munmap churn).
///
/// The channel-throughput family's documented group-pass inflation (medians
/// ~2-3x the per-id ones; attribution.md) is the same order-dependence
/// class: with the pin the group-pass youpipe rows drop toward their per-id
/// numbers (youpipe_mpmc/10000 354→171-284 µs, /100000 2.69→1.29-1.32 ms,
/// youpipe_mpsc/10000 572→278-289 µs) while the crossbeam control row stays
/// flat (420→374-409 µs @10K). Their residual within-mode wobble remains —
/// placement sensitivity itself is not the allocator's to fix.
///
/// Call before the first timed allocation (both call sites sit at harness
/// entry).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn pin_glibc_allocator() {
    use libc::{M_MMAP_THRESHOLD, M_TRIM_THRESHOLD};
    // DEFAULT_MMAP_THRESHOLD_MAX on 64-bit, mallopt(3)'s documented upper
    // limit: older glibc rejects larger values (returns 0, leaving only the
    // trim pin), 2.44 accepts them. For every buffer below it the policy is
    // identical to the 64 MiB the A/B ran with; horizontal's 4M-element
    // (32 MiB) buffers keep the mmap path they always used.
    const MMAP_THRESHOLD_VALUE: libc::c_int = 32 * 1024 * 1024;

    unsafe {
        libc::mallopt(M_TRIM_THRESHOLD, -1);
        libc::mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD_VALUE);
    }
}

/// No-op off glibc: the dynamic-threshold behavior pinned above is glibc's
/// (libc exposes `mallopt` only for linux-gnu).
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn pin_glibc_allocator() {}
