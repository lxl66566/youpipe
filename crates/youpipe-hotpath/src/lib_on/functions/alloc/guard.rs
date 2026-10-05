use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;

use crate::instant::Instant;

#[derive(Debug, Default)]
pub(crate) struct AsyncAllocBridge {
    bytes_total: AtomicU64,
    count_total: AtomicU64,
}

impl AsyncAllocBridge {
    #[inline]
    pub(crate) fn add(&self, bytes: u64, count: u64) {
        self.bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.count_total.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> (Option<u64>, Option<u64>) {
        (
            Some(self.bytes_total.load(Ordering::Relaxed)),
            Some(self.count_total.load(Ordering::Relaxed)),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllocMetric {
    Bytes,
    Count,
}

pub(crate) static ALLOC_METRIC: LazyLock<AllocMetric> =
    LazyLock::new(|| match std::env::var("HOTPATH_ALLOC_METRIC") {
        Ok(v) => match v.to_lowercase().as_str() {
            "bytes" => AllocMetric::Bytes,
            "count" => AllocMetric::Count,
            other => panic!(
                "Invalid HOTPATH_ALLOC_METRIC value: '{}'. Expected 'bytes' or 'count'.",
                other
            ),
        },
        Err(_) => AllocMetric::Bytes,
    });

pub(crate) static ALLOC_CUMULATIVE: LazyLock<bool> =
    LazyLock::new(|| crate::shared::env_flag("HOTPATH_ALLOC_CUMULATIVE"));

#[inline]
pub(crate) fn push_alloc_stack() {
    crate::functions::alloc::core::ALLOCATIONS.with(|stack| {
        let current_depth = stack.depth.get();
        // youpipe fork: assert before the increment — upstream's order
        // (set, then assert) poisoned depth on overflow, and the pops
        // running during unwind then indexed elements[MAX_DEPTH],
        // turning an overflow into a double-panic abort.
        assert!((current_depth as usize) + 1 < crate::functions::alloc::core::MAX_DEPTH);
        stack.depth.set(current_depth + 1);
        let depth = stack.depth.get() as usize;
        stack.elements[depth].bytes_total.set(0);
        stack.elements[depth].count_total.set(0);
    });
}

#[inline]
pub(crate) fn pop_alloc_stack() -> (u64, u64) {
    crate::functions::alloc::core::ALLOCATIONS.with(|stack| {
        assert!(stack.depth.get() > 0, "pop_alloc_stack called with depth 0");
        let depth = stack.depth.get() as usize;
        let bytes = stack.elements[depth].bytes_total.get();
        let count = stack.elements[depth].count_total.get();

        stack.depth.set(stack.depth.get() - 1);

        if *ALLOC_CUMULATIVE {
            let parent = stack.depth.get() as usize;
            stack.elements[parent]
                .bytes_total
                .set(stack.elements[parent].bytes_total.get() + bytes);
            stack.elements[parent]
                .count_total
                .set(stack.elements[parent].count_total.get() + count);
        }

        (bytes, count)
    })
}

/// RAII pairing for one TLS alloc-stack push (youpipe fork, review B9).
///
/// Upstream measured each instrumented inner poll with a bare push/pop: a
/// panic unwinding through the poll skipped the pop, so the polling
/// thread's depth leaked +1 per panicking poll until the depth assert in
/// `push_alloc_stack` aborted an unrelated instrumented call. Sync
/// measurements were already unwind-safe (`MeasurementGuardSync` pops in
/// Drop); this guard gives the async poll path the same guarantee.
pub(crate) struct AllocStackGuard {
    armed: bool,
}

impl AllocStackGuard {
    #[inline]
    pub(crate) fn new() -> Self {
        push_alloc_stack();
        Self { armed: true }
    }

    /// Pop the paired frame and report its totals. When `Drop` runs the pop
    /// instead (unwind path), the values are discarded: what matters there
    /// is keeping the depth balanced, not a panicking poll's metrics.
    #[inline]
    pub(crate) fn pop(mut self) -> (u64, u64) {
        self.armed = false;
        pop_alloc_stack()
    }
}

impl Drop for AllocStackGuard {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            let _ = pop_alloc_stack();
        }
    }
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn send_alloc_measurement(
    name: &'static str,
    bytes_total: Option<u64>,
    count_total: Option<u64>,
    duration_ns: Option<u64>,
    elapsed_since_start_ns: u64,
    wrapper: bool,
    tid: Option<u64>,
) {
    let _suspend = crate::lib_on::SuspendAllocTracking::new();

    crate::functions::alloc::state::send_alloc_measurement(
        name,
        bytes_total,
        count_total,
        duration_ns,
        elapsed_since_start_ns,
        wrapper,
        tid,
    );
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn send_alloc_measurement_with_log(
    name: &'static str,
    bytes_total: Option<u64>,
    count_total: Option<u64>,
    duration_ns: Option<u64>,
    elapsed_since_start_ns: u64,
    wrapper: bool,
    tid: Option<u64>,
    result_log: Option<String>,
) {
    let _suspend = crate::lib_on::SuspendAllocTracking::new();

    crate::functions::alloc::state::send_alloc_measurement_with_log(
        name,
        bytes_total,
        count_total,
        duration_ns,
        elapsed_since_start_ns,
        wrapper,
        tid,
        result_log,
    );
}

/// Wrapper guards are never sampled - their exact total is the `%` denominator.
/// Unsampled guards keep exact allocation tracking; only the duration is skipped.
#[inline]
fn sampled_start(wrapper: bool, skipped: bool) -> Option<Instant> {
    if skipped {
        return None;
    }
    if wrapper || crate::lib_on::sampling::functions_should_time() {
        Some(Instant::now())
    } else {
        None
    }
}

// youpipe fork (review R-9): counts sync guards dropped on a thread other
// than their creating one. Such a drop cannot pop the alloc-stack frame it
// pushed: the stack is thread-local and its cells unsynchronized, so a
// remote pop would race the origin thread's own accounting (and is
// impossible once that thread has exited). Synchronizing the stack would
// tax every tracked allocation, so the leak is counted and documented
// (`MeasurementGuardSync`) instead of being "fixed" with a data race.
pub(crate) static SYNC_GUARD_CROSS_THREAD_DROPS: AtomicU64 = AtomicU64::new(0);

/// Guard measuring a synchronous region: created by `#[measure]` on sync
/// functions and by the `measure_block!` macro.
///
/// # Cross-thread drop limitation (youpipe fork, review R-9)
///
/// This guard is `Send`, and a `measure_block!` block may contain `.await`,
/// so the guard can migrate to another worker thread between creation and
/// drop. The alloc stack it pushed is thread-local and unsynchronized:
/// popping it from another thread would race the creating thread's own
/// push/pop/allocation accounting (and is impossible once that thread has
/// exited), so a cross-thread drop intentionally skips the pop. The
/// creating thread's alloc depth stays one level higher for the rest of
/// its life; each such leak is counted in `SYNC_GUARD_CROSS_THREAD_DROPS`
/// so it stays observable. Prefer `#[measure]` on async functions: its
/// poll path balances via a per-poll RAII guard and attributes allocations
/// through an `AsyncAllocBridge` instead of the thread-local stack.
#[must_use = "guard is dropped immediately without measuring anything"]
pub struct MeasurementGuardSync {
    name: &'static str,
    wrapper: bool,
    tid: u64,
    start: Option<Instant>,
    skipped: bool,
    caller_scoped: bool,
}

impl MeasurementGuardSync {
    #[inline]
    pub fn new(name: &'static str, wrapper: bool, skipped: bool) -> Self {
        Self::build(name, wrapper, skipped, false)
    }

    /// Also registers `name` on the thread-local caller stack for SQL/HTTP
    /// source attribution (skipped for wrapper guards).
    #[inline]
    pub(crate) fn new_caller_scoped(name: &'static str, wrapper: bool, skipped: bool) -> Self {
        Self::build(name, wrapper, skipped, !wrapper && !skipped)
    }

    #[inline]
    fn build(name: &'static str, wrapper: bool, skipped: bool, caller_scoped: bool) -> Self {
        if !skipped {
            push_alloc_stack();
        }
        if caller_scoped {
            crate::lib_on::caller_stack::push_caller(name);
        }

        Self {
            name,
            wrapper,
            tid: crate::tid::current_tid(),
            start: sampled_start(wrapper, skipped),
            skipped,
            caller_scoped,
        }
    }
}

impl Drop for MeasurementGuardSync {
    #[inline]
    fn drop(&mut self) {
        if self.skipped {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let cross_thread = crate::tid::current_tid() != self.tid;

        if cross_thread {
            SYNC_GUARD_CROSS_THREAD_DROPS.fetch_add(1, Ordering::Relaxed);
        }

        if self.caller_scoped && !cross_thread {
            crate::lib_on::caller_stack::pop_caller();
        }
        let (bytes_total, count_total) = if cross_thread {
            (None, None)
        } else {
            let (bytes, count) = pop_alloc_stack();
            (Some(bytes), Some(count))
        };

        send_alloc_measurement(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
        );
    }
}

#[must_use = "guard is dropped immediately without measuring anything"]
pub struct MeasurementGuardAsync {
    name: &'static str,
    wrapper: bool,
    tid: u64,
    start: Option<Instant>,
    skipped: bool,
    alloc_bridge: Option<Arc<AsyncAllocBridge>>,
}

impl MeasurementGuardAsync {
    #[inline]
    pub(crate) fn new(
        name: &'static str,
        wrapper: bool,
        skipped: bool,
        alloc_bridge: Option<Arc<AsyncAllocBridge>>,
    ) -> Self {
        Self {
            name,
            wrapper,
            tid: crate::tid::current_tid(),
            start: sampled_start(wrapper, skipped),
            skipped,
            alloc_bridge,
        }
    }
}

impl Drop for MeasurementGuardAsync {
    #[inline]
    fn drop(&mut self) {
        if self.skipped {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let (bytes_total, count_total) = self
            .alloc_bridge
            .as_ref()
            .map_or((None, None), |bridge| bridge.snapshot());

        send_alloc_measurement(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
        );
    }
}

#[must_use = "guard is dropped immediately without measuring anything"]
pub(crate) struct MeasurementGuardSyncWithLog {
    name: &'static str,
    wrapper: bool,
    tid: u64,
    start: Option<Instant>,
    finished: bool,
    skipped: bool,
    caller_scoped: bool,
}

impl MeasurementGuardSyncWithLog {
    /// Also registers `name` on the thread-local caller stack for SQL/HTTP
    /// source attribution (skipped for wrapper guards).
    #[inline]
    pub(crate) fn new_caller_scoped(name: &'static str, wrapper: bool, skipped: bool) -> Self {
        let caller_scoped = !wrapper && !skipped;
        if !skipped {
            push_alloc_stack();
        }
        if caller_scoped {
            crate::lib_on::caller_stack::push_caller(name);
        }

        Self {
            name,
            wrapper,
            tid: crate::tid::current_tid(),
            start: sampled_start(wrapper, skipped),
            finished: false,
            skipped,
            caller_scoped,
        }
    }

    #[inline]
    pub fn finish_with_result<T: std::fmt::Debug>(mut self, result: &T) {
        self.finished = true;
        if self.skipped {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let result_str = crate::output::format_debug_truncated(result);
        let cross_thread = crate::tid::current_tid() != self.tid;

        if cross_thread {
            SYNC_GUARD_CROSS_THREAD_DROPS.fetch_add(1, Ordering::Relaxed);
        }

        if self.caller_scoped && !cross_thread {
            crate::lib_on::caller_stack::pop_caller();
        }
        let (bytes_total, count_total) = if cross_thread {
            (None, None)
        } else {
            let (bytes, count) = pop_alloc_stack();
            (Some(bytes), Some(count))
        };

        send_alloc_measurement_with_log(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
            Some(result_str),
        );
    }
}

impl Drop for MeasurementGuardSyncWithLog {
    #[inline]
    fn drop(&mut self) {
        if self.skipped || self.finished {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let cross_thread = crate::tid::current_tid() != self.tid;

        if cross_thread {
            SYNC_GUARD_CROSS_THREAD_DROPS.fetch_add(1, Ordering::Relaxed);
        }

        if self.caller_scoped && !cross_thread {
            crate::lib_on::caller_stack::pop_caller();
        }
        let (bytes_total, count_total) = if cross_thread {
            (None, None)
        } else {
            let (bytes, count) = pop_alloc_stack();
            (Some(bytes), Some(count))
        };

        send_alloc_measurement_with_log(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
            None,
        );
    }
}

#[must_use = "guard is dropped immediately without measuring anything"]
pub(crate) struct MeasurementGuardAsyncWithLog {
    name: &'static str,
    wrapper: bool,
    tid: u64,
    start: Option<Instant>,
    finished: bool,
    skipped: bool,
    alloc_bridge: Option<Arc<AsyncAllocBridge>>,
}

impl MeasurementGuardAsyncWithLog {
    #[inline]
    pub(crate) fn new(
        name: &'static str,
        wrapper: bool,
        skipped: bool,
        alloc_bridge: Option<Arc<AsyncAllocBridge>>,
    ) -> Self {
        Self {
            name,
            wrapper,
            tid: crate::tid::current_tid(),
            start: sampled_start(wrapper, skipped),
            finished: false,
            skipped,
            alloc_bridge,
        }
    }

    #[inline]
    pub fn finish_with_result<T: std::fmt::Debug>(mut self, result: &T) {
        self.finished = true;
        if self.skipped {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let result_str = crate::output::format_debug_truncated(result);
        let (bytes_total, count_total) = self
            .alloc_bridge
            .as_ref()
            .map_or((None, None), |bridge| bridge.snapshot());

        send_alloc_measurement_with_log(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
            Some(result_str),
        );
    }
}

impl Drop for MeasurementGuardAsyncWithLog {
    #[inline]
    fn drop(&mut self) {
        if self.skipped || self.finished {
            return;
        }

        let end = Instant::now();
        let duration_ns = self
            .start
            .map(|start| end.duration_since(start).as_nanos() as u64);
        let elapsed_since_start_ns = crate::lib_on::elapsed_since_start_ns(end);
        let (bytes_total, count_total) = self
            .alloc_bridge
            .as_ref()
            .map_or((None, None), |bridge| bridge.snapshot());

        send_alloc_measurement_with_log(
            self.name,
            bytes_total,
            count_total,
            duration_ns,
            elapsed_since_start_ns,
            self.wrapper,
            Some(self.tid),
            None,
        );
    }
}

#[cfg(all(test, feature = "hotpath-alloc"))]
mod cross_thread_tests {
    use super::*;

    fn alloc_depth() -> u32 {
        crate::functions::alloc::core::ALLOCATIONS.with(|stack| stack.depth.get())
    }

    /// R-9: a sync guard dropped on another thread cannot pop its TLS
    /// alloc-stack frame, so the creating thread keeps the +1 depth for
    /// good (documented limitation). The drop itself must stay panic-free
    /// and the leak must be counted so it is observable rather than silent
    /// depth corruption.
    // Ignored under miri: guard creation samples time through quanta's
    // TSC calibration (inline asm), which miri rejects — a pre-existing
    // limitation of every timed path in this crate, not of this test.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn cross_thread_drop_is_counted_and_leaks_exactly_one_frame() {
        let leaks_before = SYNC_GUARD_CROSS_THREAD_DROPS.load(Ordering::Relaxed);
        let depth_before = alloc_depth();

        // Control: same-thread create/drop keeps the depth balanced.
        drop(MeasurementGuardSync::new("same_thread_guard", false, false));
        assert_eq!(alloc_depth(), depth_before);

        // Create here, drop on another thread.
        let (tx, rx) = std::sync::mpsc::channel::<MeasurementGuardSync>();
        tx.send(MeasurementGuardSync::new("cross_thread_guard", false, false))
            .unwrap();
        let handle = std::thread::spawn(move || {
            drop(rx.recv().unwrap());
        });
        handle.join().unwrap();

        assert_eq!(
            alloc_depth(),
            depth_before + 1,
            "origin thread keeps the orphaned frame (documented limitation)"
        );
        assert_eq!(
            SYNC_GUARD_CROSS_THREAD_DROPS.load(Ordering::Relaxed),
            leaks_before + 1
        );
    }
}
