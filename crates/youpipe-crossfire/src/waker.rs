//use crate::collections::ArcCell;
#[cfg(feature = "loom")]
// Only the atomics are model-checked; Arc/Weak/Thread stay real.
use loom::sync::atomic::{AtomicU64, Ordering};
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::Deref;
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::*;
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum WakerState {
    Init = 0, // A temporary state, https://github.com/frostyplanet/crossfire-rs/issues/22
    Waiting = 1,
    /// Retired from a completed episode: the registry entry was cleared, the
    /// node is kept in the caller's slot for in-place re-arming on the next
    /// registration (async cross-episode reuse, youpipe P-4). Never set by
    /// `wake()`/`close_wake()` (those only act on `<= Waiting`), so a retired
    /// node can only be re-registered or dropped by its owner.
    Reuse = 2,
    Woken = 3,
    Closed = 4, // Channel closed, or timeout cancellation
    Done = 5,
}

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum WakeResult {
    Woken = 0x1, // Woken, stop iteration
    Sent = 0x3,  // Woken with message direct copied
    Next = 0x2,  // Woken, but have to continued for more iteration
    Skip = 0x4,  // Waker Cancelled or Done
}

impl WakeResult {
    #[inline(always)]
    pub fn is_done(&self) -> bool {
        (*self as u8) & (WakeResult::Woken as u8) > 0
    }
}

pub struct ArcWaker(Arc<WakerInner>);

impl fmt::Debug for ArcWaker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for WakerInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({})", self.get_seq())
    }
}

impl Deref for ArcWaker {
    type Target = WakerInner;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

// TEMP probes for measurement, removed before commit

/// Heap block size of one `Arc<WakerInner>` allocation: `ArcInner` header
/// (strong + weak, 2 ptr-size words) + the node body (`WakerInner`'s align
/// is a word, so no padding is inserted). Testing hook for the
/// alloc-counting regression tests that guard cross-episode node reuse.
pub const ARC_WAKER_ALLOC_SIZE: usize =
    core::mem::size_of::<WakerInner>() + 2 * core::mem::size_of::<usize>();

impl ArcWaker {
    #[inline(always)]
    pub fn new_async(ctx: &Context) -> Self {
        Self(Arc::new(WakerInner {
            word: AtomicU64::new(0),
            waker: UnsafeCell::new(ThinWaker::Async(ctx.waker().clone())),
        }))
    }

    #[inline(always)]
    pub fn new_blocking() -> Self {
        Self(Arc::new(WakerInner {
            word: AtomicU64::new(0),
            waker: UnsafeCell::new(ThinWaker::Blocking(thread::current())),
        }))
    }
}

impl ArcWaker {
    #[inline(always)]
    pub fn from_arc(inner: Arc<WakerInner>) -> Self {
        Self(inner)
    }

    #[allow(clippy::wrong_self_convention)]
    #[inline(always)]
    pub fn to_arc(self) -> Arc<WakerInner> {
        self.0
    }

    /// Whether `inner` is this handle's node.
    #[inline(always)]
    pub(crate) fn same_node(&self, inner: &Arc<WakerInner>) -> bool {
        Arc::ptr_eq(&self.0, inner)
    }

    #[inline(always)]
    pub fn weak(&self) -> Weak<WakerInner> {
        Arc::downgrade(&self.0)
    }

    /// Test-only: clone sharing the same node, for loom thread injection.
    #[cfg(any(test, feature = "loom"))]
    #[inline(always)]
    pub(crate) fn clone_node(&self) -> Self {
        Self(self.0.clone())
    }
}

// Global registration stamp, unique process-wide. Per-registry counters
// cannot tell a stale entry from a node re-armed on another registry.
// Cross-episode re-arms bump it in WakerInner::reset().
// Starts at 1, so an unregistered node's seq (0) never matches a stamp.
#[cfg(not(feature = "loom"))]
pub(crate) static REG_STAMP: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "loom")]
loom::lazy_static! {
    // Re-initialized for every loom model run.
    pub(crate) static ref REG_STAMP: AtomicU64 = AtomicU64::new(1);
}

// The stamp lives in the upper 56 bits of the packed state word (see
// `WakerInner`), so mints are masked to 56 bits. A stamp collision needs
// 2^56 re-arms process-wide (~7.2e16; at 1e9 re-arms/s, ~2.2 years) — a
// practical non-issue.
pub(crate) const SEQ_MASK: u64 = (1 << 56) - 1;

// `wake()` stamp sentinel for registries that replace (not append) their
// waker slot on re-arm — RegistrySingle and SelectWakerWrapper: a stale
// handle cannot outlive a re-arm, so no stamp gate is needed. Never a real
// stamp: mints are masked to 56 bits.
pub(crate) const NO_STAMP: u64 = u64::MAX;

#[cfg(not(feature = "loom"))]
thread_local! {
    // The blocking wake target is thread::current(), which never changes, so
    // the node lives per-thread and is only re-armed between episodes. The
    // ThinWaker handle is written once, before any publication.
    static BLOCKING_WAKER: Arc<WakerInner> = Arc::new(WakerInner {
        word: AtomicU64::new(0),
        waker: UnsafeCell::new(ThinWaker::Blocking(thread::current())),
    });
}

/// The per-thread blocking waker, re-armed for a new episode.
#[cfg(not(feature = "loom"))]
#[inline(always)]
pub(crate) fn tl_blocking_waker() -> ArcWaker {
    BLOCKING_WAKER.with(|inner| {
        let waker = ArcWaker::from_arc(inner.clone());
        waker.reset();
        waker
    })
}

#[cfg(feature = "loom")]
#[inline(always)]
pub(crate) fn tl_blocking_waker() -> ArcWaker {
    // TLS is not modeled under loom; tests inject per-thread nodes explicitly.
    let waker = ArcWaker::new_blocking();
    waker.reset();
    waker
}

#[derive(Debug)]
pub(crate) enum ThinWaker {
    Async(Waker),
    Blocking(thread::Thread),
}

impl ThinWaker {
    #[inline(always)]
    pub fn wake_by_ref(&self) {
        match self {
            Self::Async(w) => w.wake_by_ref(),
            Self::Blocking(th) => th.unpark(),
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn wake(self) {
        match self {
            Self::Async(w) => w.wake(),
            Self::Blocking(th) => th.unpark(),
        }
    }

    #[inline(always)]
    pub fn will_wake(&self, ctx: &mut Context) -> bool {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no waking up ever
        // happened, waker still exists in registry but cannot be used to wake the current future.
        if let Self::Async(_waker) = self {
            _waker.will_wake(ctx.waker())
        } else {
            unreachable!();
        }
    }
}

// Packed state machine word: low 8 bits = `WakerState`, upper 56 bits = the
// registration stamp (`seq`). Packing makes delivery and staleness single
// atomic operations:
//
// * `wake()`'s CAS matches state AND stamp at once, so a waiter that
//   abandoned and re-armed (fresh stamp) fails the CAS directly — a fire
//   can never spend its event on a foreign episode (todo #14's pop/wake
//   migration steal), with no post-wake revalidation.
// * the commit CAS carries the stamp bits it read, so observing
//   (Waiting, s) proves s is the stamp of the episode that committed — a
//   stale queue entry from a previous registration (different stamp)
//   cannot match, by coherence alone.
// Stamp width: mints are masked to 56 bits (`SEQ_MASK`).
#[inline(always)]
fn pack(state: u8, seq: u64) -> u64 {
    (state as u64) | (seq << 8)
}
#[inline(always)]
fn state_of(word: u64) -> u8 {
    (word & 0xFF) as u8
}
#[inline(always)]
fn seq_of(word: u64) -> u64 {
    word >> 8
}

pub struct WakerInner {
    word: AtomicU64,
    waker: UnsafeCell<ThinWaker>,
}

unsafe impl Send for WakerInner {}
unsafe impl Sync for WakerInner {}

impl WakerInner {
    #[inline(always)]
    fn get_waker(&self) -> &ThinWaker {
        unsafe { &*self.waker.get() }
    }

    #[inline(always)]
    fn get_waker_mut(&self) -> &mut ThinWaker {
        unsafe { &mut *self.waker.get() }
    }

    #[inline(always)]
    pub fn reset(&self) {
        // Cross-episode re-arm: one store publishes (Init, fresh stamp).
        // Release is enough: any stale reader that still sees the old word
        // is corrected by the stamp CAS in `wake()` — a wake only lands on
        // the episode its stamp names. The fresh stamp matters for
        // registries that never re-stamp on registration (RegistrySingle),
        // so a stale entry on a foreign RegistryMulti cannot match.
        // New episode must change the seq (stale entries are matched by seq).
        let seq = REG_STAMP.fetch_add(1, Ordering::SeqCst) & SEQ_MASK;
        self.word.store(pack(WakerState::Init as u8, seq), Ordering::Release);
    }

    // Current stamp. SeqCst: the un-anchored readers (`close`'s
    // `upgrade_live`, `_clear_wakers`) have no state load to pair with, so
    // they take the stronger read.
    #[inline(always)]
    pub fn get_seq(&self) -> u64 {
        seq_of(self.word.load(Ordering::SeqCst))
    }

    /// (state, seq) read together from the packed word. The pair is atomic:
    /// a Waiting state arrives with the stamp of the episode whose commit
    /// CAS wrote it — no fence or acquire chain is needed to correlate them.
    #[inline(always)]
    pub(crate) fn _get_state_seq(&self, order: Ordering) -> (u8, u64) {
        let w = self.word.load(order);
        (state_of(w), seq_of(w))
    }

    // Rewrite the stamp, preserving the state bits. Caller is the node's
    // owner (inside the registry mutex); a concurrent stale-entry fire may
    // lose its Woken stamp to this store — the F11 spurious-wake class,
    // absorbed by the owner's next park.
    #[inline(always)]
    pub fn set_seq(&self, seq: u64) {
        let w = self.word.load(Ordering::Acquire);
        self.word.store(pack(state_of(w), seq), Ordering::Release);
    }

    #[inline(always)]
    fn update_thread_handle(&self) {
        let _waker = self.get_waker_mut();
        *_waker = ThinWaker::Blocking(thread::current());
    }

    #[inline(always)]
    pub fn commit_waiting(&self) -> u8 {
        if let Err(s) = self.try_change_state(WakerState::Init, WakerState::Waiting) {
            s
        } else {
            WakerState::Waiting as u8
        }
    }

    #[inline(always)]
    pub fn try_change_state(&self, cur: WakerState, new_state: WakerState) -> Result<(), u8> {
        // Single attempt (callers retry at their own granularity); the
        // stamp bits ride along unchanged.
        let w = self.word.load(Ordering::Relaxed);
        if state_of(w) == cur as u8 {
            match self.word.compare_exchange(
                w,
                pack(new_state as u8, seq_of(w)),
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(cur_w) => return Err(state_of(cur_w)),
            }
        }
        Err(state_of(w))
    }

    /// Retire a cleared (no longer registered) node for cross-episode
    /// reuse (youpipe P-4). SeqCst: the next registration (possibly on
    /// another thread after the slot moved) must observe it before it
    /// inspects the registry state machine — a stale `Init` read would
    /// misroute into the "still registered" branch. The stamp is preserved
    /// so foreign stale entries keep mismatching.
    #[inline(always)]
    pub fn retire(&self) {
        let w = self.word.load(Ordering::Acquire);
        self.word.store(pack(WakerState::Reuse as u8, seq_of(w)), Ordering::SeqCst);
    }

    pub fn reset_init(&self) {
        // this is before we put into registry (which will extablish happen-before relationship),
        // it safe to use Relaxed. The stamp is preserved until the registration re-stamps.
        let w = self.word.load(Ordering::Relaxed);
        self.word.store(pack(WakerState::Init as u8, seq_of(w)), Ordering::Relaxed);
    }

    /// Return current status,
    /// Closed: might be channel closed, or future successfully cancelled, the future should drop message; try to clear its waker.
    /// Done: the message actually sent, nothing to DO
    /// Woken: the future should drop message, and wake another counterpart.
    #[inline(always)]
    pub fn abandon(&self) -> Result<(), u8> {
        // it will content with close(), on_recv(), on_send()
        match self.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed) {
            Ok(_) => Ok(()),
            Err(state) => Err(state),
        }
        // NOTE: there's no Copy state, so we do not loop
    }

    #[inline(always)]
    pub fn close_wake(&self) -> bool {
        // should have lock because it will content with abandon()
        if self.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed).is_ok() {
            self.get_waker().wake_by_ref();
            return true;
        }
        false
    }

    // Return Ok(pre_state), otherwise return Err(current_state)
    #[inline(always)]
    pub fn change_state_smaller_eq(
        &self, condition: WakerState, target: WakerState,
    ) -> Result<u8, u8> {
        debug_assert!((condition as u8) < (target as u8));
        let mut w = self.word.load(Ordering::Relaxed);
        loop {
            let state = state_of(w);
            if state > condition as u8 {
                return Err(state);
            }
            match self.word.compare_exchange_weak(
                w,
                pack(target as u8, seq_of(w)),
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(state);
                }
                Err(cur_w) => {
                    w = cur_w;
                }
            }
        }
    }

    #[inline(always)]
    pub fn _get_state(&self, order: Ordering) -> u8 {
        state_of(self.word.load(order))
    }

    #[inline(always)]
    pub fn get_state(&self) -> u8 {
        state_of(self.word.load(Ordering::SeqCst))
    }

    #[inline(always)]
    pub fn get_state_relaxed(&self) -> u8 {
        state_of(self.word.load(Ordering::Relaxed))
    }

    /// Assume no lock.
    ///
    /// `stamp` is the episode the caller's queue entry belongs to
    /// (`QueueEntry::stamp`). The delivery CAS matches the packed
    /// `(Waiting, stamp)` word, so a waiter that abandoned and re-armed
    /// since the pop (fresh stamp) fails the CAS and the event is handed
    /// back — the wake can never land on a foreign episode (todo #14's
    /// pop/wake migration steal), with no post-wake revalidation.
    /// `NO_STAMP` skips the gate for registries that replace (not append)
    /// their slot on re-arm.
    #[inline(always)]
    pub fn wake(&self, stamp: u64) -> WakeResult {
        // This is after we get waker from waker_registry, which already happen before relationship.
        // both >= WakerState::Waiting is certain
        let mut w = self.word.load(Ordering::Relaxed);
        loop {
            let state = state_of(w);
            if state >= WakerState::Woken as u8 || state == WakerState::Reuse as u8 {
                // Reuse: retired node whose stale entry a late fire popped —
                // nobody is waiting through it, pass the fire budget on.
                return WakeResult::Skip;
            } else if state == WakerState::Waiting as u8 {
                // Stamp gate + CAS delivery in one locked op: a mismatch
                // means the waiter re-armed (or the entry is stale) — no
                // event is spent, the caller hands it back.
                if stamp != NO_STAMP && seq_of(w) != stamp {
                    return WakeResult::Skip;
                }
                if self.word
                    .compare_exchange(
                        w,
                        pack(WakerState::Woken as u8, seq_of(w)),
                        Ordering::SeqCst,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.get_waker().wake_by_ref();
                    return WakeResult::Woken;
                }
                return WakeResult::Skip;
            } else {
                // Init: registered but not yet committed — wake and keep
                // going (Next). The stamp does not gate this branch: the
                // episode has not committed, so there is nothing to steal.
                match self.word.compare_exchange_weak(
                    w,
                    pack(WakerState::Woken as u8, seq_of(w)),
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.get_waker().wake_by_ref();
                        return WakeResult::Next;
                    }
                    Err(cur) => {
                        w = cur;
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn will_wake(&self, ctx: &mut Context) -> bool {
        self.get_waker().will_wake(ctx)
    }
}

/*
impl<T> WakerInner<*const T> {
    #[inline(always)]
    fn get_payload(&self) -> *const T {
        *self.get_payload_mut()
    }

    #[inline(always)]
    pub fn wake_or_copy<F: FlavorImpl<Item = T>>(&self, flavor: &F) -> WakeResult {
        // This is after we get waker from waker_registry, which already happen before relationship.
        // both >= WakerState::Waiting is certain
        let mut state = self.get_state_relaxed();
        loop {
            if state >= WakerState::Woken as u8 || state == WakerState::Reuse as u8 {
                // Reuse: retired node whose stale entry a late fire popped —
                // nobody is waiting through it, pass the fire budget on.
                return WakeResult::Skip;
            } else if state == WakerState::Waiting as u8 {
                let p = self.get_payload();
                if p.is_null() {
                    self.state.store(WakerState::Woken as u8, Ordering::SeqCst);
                    self.get_waker().wake_by_ref();
                    return WakeResult::Woken;
                }
                state = if let Some(true) = flavor.try_send_oneshot(p) {
                    WakerState::Done as u8
                } else {
                    WakerState::Woken as u8
                };
                self.state.store(state, Ordering::SeqCst);
                self.get_waker().wake_by_ref();
                if state == WakerState::Done as u8 {
                    return WakeResult::Sent;
                } else {
                    return WakeResult::Woken;
                }
            } else {
                match self.state.compare_exchange_weak(
                    WakerState::Init as u8,
                    WakerState::Woken as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.get_waker().wake_by_ref();
                        return WakeResult::Next;
                    }
                    Err(s) => {
                        state = s;
                    }
                }
            }
        }
    }
}

pub struct WakerCache<P: Copy>(ArcCell<WakerInner<P>>);

impl<P: Copy> WakerCache<P> {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(ArcCell::new())
    }

    #[inline(always)]
    pub fn new_blocking(&self, payload: P) -> ArcWaker<P> {
        if let Some(inner) = self.0.pop() {
            inner.update_thread_handle();
            inner.reset(payload);
            return ArcWaker::<P>::from_arc(inner);
        }
        ArcWaker::new_blocking(payload)
    }

    #[inline(always)]
    pub(crate) fn push(&self, waker: ArcWaker<P>) {
        debug_assert!(waker.get_state() >= WakerState::Woken as u8);
        let a = waker.to_arc();
        if Arc::weak_count(&a) == 0 && Arc::strong_count(&a) == 1 {
            self.0.try_put(a);
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        !self.0.exists()
    }
}
*/

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_waker_size() {
        use std::mem::size_of;
        println!("wakertype {}", size_of::<ThinWaker>());
        println!("waker inner {}", size_of::<WakerInner>());
    }

    #[test]
    fn test_tl_blocking_waker_identity() {
        let w1 = tl_blocking_waker();
        let node = w1.0.clone(); // TL slot + w1 + node = 3
        assert_eq!(Arc::strong_count(&node), 3);
        assert_eq!(w1.get_state(), WakerState::Init as u8);
        drop(w1);
        assert_eq!(Arc::strong_count(&node), 2);

        // Next episode: same node, re-armed to Init even from Woken.
        node.word.store(pack(WakerState::Woken as u8, 0), Ordering::SeqCst);
        let w2 = tl_blocking_waker();
        assert!(Arc::ptr_eq(&node, &w2.0));
        assert_eq!(Arc::strong_count(&node), 3); // TL slot + node + w2
        assert_eq!(w2.get_state(), WakerState::Init as u8);
    }

    #[test]
    fn test_tl_blocking_waker_nodes_differ_across_threads() {
        let main_node = tl_blocking_waker().0.clone();
        let t = std::thread::spawn(|| tl_blocking_waker().0.clone());
        let other_node = t.join().expect("join");
        assert!(!Arc::ptr_eq(&main_node, &other_node));
    }
}
