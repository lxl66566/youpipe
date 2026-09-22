//! Verifies the allocation contract of the two expansion APIs with a counting
//! global allocator (own test binary — the allocator is process-global, so
//! allocation-sensitive assertions cannot share a binary with unrelated
//! tests).
//!
//! * `expand` (owned `Vec`) — one `malloc` + `free` **per input item**;
//! * `expand_emit` (push-style) — per-worker scratch buffer reused across items: steady state
//!   performs no allocations of the expansion size.
//!
//! Counting is filtered by allocation size (72 bytes = the nine-`u64` temp
//! `Vec` of an owned group; the per-worker scratch buffer grows through
//! 32/64/128-byte capacities, so it never matches). Counting itself is
//! atomics-only: a locking histogram (Mutex+BTreeMap) self-deadlocks —
//! inserting a bucket allocates while the lock is held and std's Mutex is
//! not reentrant.
//!
//! Unfiltered counts are therefore nondeterministic under backpressure —
//! a 2-stage pipeline measured 17/387/2 wakers across three consecutive
//! runs — see the crossfire waker entry in `docs/todo.md` for the full
//! attribution.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    mem::size_of,
    sync::atomic::{AtomicU64, Ordering},
};

use youpipe::stream;

/// Fan-out 9: an owned-group `Vec<u64>` allocates exactly 9 × 8 = 72 bytes
/// (`collect` over an exact size hint reserves capacity 9) — a size nothing
/// else in the run produces.
const FANOUT: u64 = 9;
const GROUP_SIZE: usize = 9 * size_of::<u64>();

struct CountingAlloc;

static GROUP_SIZED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == GROUP_SIZE {
            GROUP_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() == GROUP_SIZE {
            GROUP_SIZED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

fn owned_group(x: u64) -> Vec<u64> {
    (0..FANOUT)
        .map(|i| x.wrapping_mul(FANOUT).wrapping_add(i))
        .collect()
}

fn emit_group(x: u64, out: &mut Vec<u64>) {
    for i in 0..FANOUT {
        out.push(x.wrapping_mul(FANOUT).wrapping_add(i));
    }
}

#[test]
fn expand_emit_zero_steady_state_allocations() {
    let n = 1024_u64;

    // Warm up the global pool, its worker deques, and the channels so the
    // measured phases only contain steady-state traffic.
    let _ = stream(0..n).expand_emit(emit_group).run();
    let _ = stream(0..n).expand(owned_group).run();

    // Push-style: the per-worker scratch buffer only grows through doubling
    // reallocs (never a 72-byte capacity for 8-byte items), so group-sized
    // allocations stay near zero regardless of the item count.
    let before = GROUP_SIZED.load(Ordering::Relaxed);
    let r = stream(0..n).expand_emit(emit_group).run();
    let emit = GROUP_SIZED.load(Ordering::Relaxed) - before;
    assert_eq!(u64::try_from(r.len()).unwrap(), n * FANOUT);
    assert!(
        emit <= 64,
        "expand_emit performed {emit} group-sized allocations for {n} items"
    );

    // Control: the owned-Vec API must show ≥ 1 allocation per input item.
    let before = GROUP_SIZED.load(Ordering::Relaxed);
    let r = stream(0..n).expand(owned_group).run();
    let owned = GROUP_SIZED.load(Ordering::Relaxed) - before;
    assert_eq!(u64::try_from(r.len()).unwrap(), n * FANOUT);
    assert!(
        owned >= n,
        "owned-Vec expand control: expected >= {n} group-sized allocations, saw {owned}"
    );
}
