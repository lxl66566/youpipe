#[allow(unused_imports)]
use crate::collections::WeakCell;
#[allow(unused_imports)]
use crate::flavor::{Flavor, FlavorImpl};
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::trace_log;
use crate::waker::*;
#[cfg(feature = "loom")]
// loom has no compiler_fence; a full fence is its modeled superset.
use loom::sync::atomic::fence as compiler_fence;
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};
#[cfg(feature = "loom")]
// loom's Mutex is the modeled equivalent; parking_lot's raw futex calls are
// opaque to the model and would hide interleavings.
use loom::sync::{Mutex, MutexGuard};
#[cfg(all(not(feature = "loom"), not(miri)))]
use parking_lot::{Mutex, MutexGuard};
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt::Debug;
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{compiler_fence, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
#[cfg(all(not(feature = "loom"), miri))]
// Miri cannot interpret parking_lot_core's futex path on Windows
// (GetModuleHandleA); std::sync is natively supported by the interpreter.
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};

// The mutex flavors differ only in lock()'s return: parking_lot hands out the
// guard directly, std (miri builds) and loom wrap it in a LockResult.
#[cfg(not(any(feature = "loom", miri)))]
#[inline(always)]
fn reg_lock(inner: &Mutex<RegistryMultiInner>) -> MutexGuard<'_, RegistryMultiInner> {
    inner.lock()
}

#[cfg(any(feature = "loom", miri))]
#[inline(always)]
fn reg_lock(inner: &Mutex<RegistryMultiInner>) -> MutexGuard<'_, RegistryMultiInner> {
    inner.lock().unwrap()
}

// pub(crate) on type alias does not matter, mpmc::List alias works because RegistryMulti is pub

pub(crate) trait Registry: Send + Sync + 'static {
    type Waker: Send + Unpin + 'static + Debug;

    fn get_waker_state(&self, o_waker: &Option<Self::Waker>, order: Ordering) -> u8;

    #[inline(always)]
    fn clear_wakers(&self, _waker: &Self::Waker) {}

    fn close(&self);

    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn commit_waiting(&self, _o_waker: &Option<Self::Waker>) -> u8 {
        WakerState::Init as u8
    }

    #[inline(always)]
    fn cancel_waker(&self, o_waker: &mut Option<Self::Waker>) {
        let _ = o_waker.take();
    }

    #[inline(always)]
    fn abandon_waker(&self, _waker: &Self::Waker) -> Result<(), u8> {
        Ok(())
    }

    #[inline(always)]
    fn fire(&self) -> WakeResult {
        WakeResult::Next
    }
}

pub(crate) trait RegistrySend: Registry {
    fn new() -> Self;

    #[inline(always)]
    fn use_direct_copy(&self) -> bool {
        false
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, _o_waker: &mut Option<<Self as Registry>::Waker>) {
        unreachable!();
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, _ctx: &mut Context, _o_waker: &mut Option<<Self as Registry>::Waker>,
    ) -> Option<Poll<()>> {
        unreachable!();
    }

    /// remove outdated waker, make sure it does not accumulate.
    ///
    /// It's ok to set state with Relaxed here, two scenario:
    /// * set Done while the state is Init, does not matter other thread see it or not.
    /// * other thread might have wake it in the process, but we are dropping it anyway, and then
    ///   reg_waker with a new one.
    #[inline(always)]
    fn cancel_reuse_waker(
        &self, o_waker: &mut Option<<Self as Registry>::Waker>, state: WakerState,
    ) -> u8 {
        let _ = o_waker.take();
        state as u8
    }

    //    #[inline(always)]
    //    fn cache_waker(
    //        &self, _o_waker: Option<<Self as Registry>::Waker>, _cache: &WakerCache<*const T>,
    //    ) {
    //    }
}

pub(crate) trait RegistryRecv: Registry {
    fn new() -> Self;

    #[inline(always)]
    fn reg_waker_blocking(&self, _o_waker: &mut Option<<Self as Registry>::Waker>) {
        unreachable!();
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, _ctx: &mut Context, _o_waker: &mut Option<<Self as Registry>::Waker>,
    ) -> Option<Poll<()>> {
        unreachable!();
    }

    //    #[inline(always)]
    //    fn cache_waker(&self, _o_waker: Option<<Self as Registry>::Waker>, _cache: &WakerCache<()>) {}

    fn reg_select_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool;

    #[inline(always)]
    fn cancel_select_waker(&self, _waker: &Arc<SelectWaker>) {}
}

#[derive(Debug)]
pub struct RegistryDummy();

impl Registry for RegistryDummy {
    type Waker = ();

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<Self::Waker>, _order: Ordering) -> u8 {
        unreachable!();
    }

    #[inline(always)]
    fn close(&self) {}
}

impl RegistrySend for RegistryDummy {
    #[inline(always)]
    fn new() -> Self {
        Self()
    }
}

type SingleWaker = ArcWaker;
//type SingleWaker = ThinWaker;

pub struct RegistrySingle {
    cell: WeakCell<WakerInner>,
    // OneSpmc has comparable speed as WeakCell and does not allocate on waker registration,
    // but since miri will report datarace issue, commented out for now.
    //cell: OneSpmc<ThinWaker>,
    _tag: &'static str,
}

impl RegistrySingle {
    #[inline(always)]
    fn _fire(&self) {
        if let Some(waker) = self.cell.pop() {
            waker.wake();
            trace_log!("{} wake", self._tag);
        }
    }

    #[inline(always)]
    fn _reg_waker_async(&self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>) {
        // XXX don't know what the waker was, always generate new
        let waker = ArcWaker::new_async(ctx);
        //let waker = ThinWaker::Async(ctx.waker().clone());
        trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
        self.cell.replace(waker.weak());
        o_waker.replace(waker);
        //self.cell.replace(waker);
        // should store into o_waker, AsyncTx need to drop item when SendFuture drop
    }

    #[inline(always)]
    fn _reg_waker_blocking(&self, o_waker: &mut Option<SingleWaker>) {
        let waker = tl_blocking_waker();
        trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
        self.cell.replace(waker.weak());
        o_waker.replace(waker);
    }
}

impl Registry for RegistrySingle {
    type Waker = SingleWaker;

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<SingleWaker>, _order: Ordering) -> u8 {
        if self.cell.is_empty() {
            WakerState::Woken as u8
        } else {
            WakerState::Init as u8
        }
    }

    #[inline(always)]
    fn close(&self) {
        self._fire();
    }

    #[inline(always)]
    fn fire(&self) -> WakeResult {
        self._fire();
        WakeResult::Next
    }
}

impl RegistrySend for RegistrySingle {
    #[inline(always)]
    fn new() -> Self {
        //Self { cell: _OneSpmc::new(), _tag: "tx" }
        Self { cell: WeakCell::new(), _tag: "tx" }
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<SingleWaker>) {
        self._reg_waker_blocking(o_waker);
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker);
        None
    }
}

impl RegistryRecv for RegistrySingle {
    #[inline(always)]
    fn new() -> Self {
        //Self { cell: OneSpmc::new(), _tag: "rx" }
        Self { cell: WeakCell::new(), _tag: "rx" }
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<SingleWaker>) {
        self._reg_waker_blocking(o_waker)
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker);
        None
    }

    #[inline(always)]
    fn reg_select_waker(&self, _channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        trace_log!("{}: reg for select", self._tag);
        self.cell.replace(waker.clone_weak());
        false
    }
}

// Global registration stamp. Per-registry counters cannot key the queue-entry
// staleness check below: an immortal per-thread node re-armed on ANOTHER
// registry would carry that registry's counter value, which can be numerically
// equal to a stale entry's stamp here (pipelines advance the counters roughly
// in lockstep), letting a dead entry pass as live and steal one fire. A single
// global counter makes stamps unique process-wide, so a stamp mismatch always
// means "re-armed since this entry was pushed". Uniqueness comes from the RMW
// itself; the registry mutex publication orders the subsequent set_seq/push.
#[cfg(not(feature = "loom"))]
static REG_STAMP: AtomicU32 = AtomicU32::new(1);
#[cfg(feature = "loom")]
loom::lazy_static! {
    // Re-initialized for every loom model run.
    static ref REG_STAMP: AtomicU32 = AtomicU32::new(1);
}

struct RegistryMultiInner {
    // Each entry records the stamp it was registered under. A node whose
    // current seq differs was re-armed afterwards (blocking nodes are
    // per-thread and immortal), so the entry is stale — the live-node
    // analogue of a dead node failing to upgrade.
    queue: VecDeque<(Weak<WakerInner>, u32)>,
    selectors: Vec<SelectWakerWrapper>,
    seq: u32,
}

impl RegistryMultiInner {
    #[inline(always)]
    fn new() -> Self {
        Self { queue: VecDeque::with_capacity(32), selectors: Vec::with_capacity(32), seq: 0 }
    }

    // it's better to use non-atomic than fetch_XXX
    #[inline(always)]
    fn check_select(&self) -> u8 {
        if self.selectors.is_empty() {
            0
        } else {
            MULTI_HAS_SELECT
        }
    }

    // it's better to use non-atomic than fetch_XXX
    #[inline(always)]
    fn check_waker(&self) -> u8 {
        if self.queue.is_empty() {
            0
        } else {
            MULTI_HAS_WAKER
        }
    }
}

const MULTI_EMPTY: u8 = 0;
const MULTI_HAS_SELECT: u8 = 1;
const MULTI_HAS_WAKER: u8 = 2;

pub struct RegistryMulti {
    state: AtomicU8,
    inner: Mutex<RegistryMultiInner>,
    _tag: &'static str,
}

impl RegistryMulti {
    #[inline(always)]
    fn reg_waker(&self, waker: &ArcWaker) {
        let weak = waker.weak();
        {
            let mut guard = reg_lock(&self.inner);
            let seq = REG_STAMP.fetch_add(1, Ordering::Relaxed);
            guard.seq = seq;
            waker.set_seq(seq);
            if guard.queue.is_empty() {
                self.state.store(guard.check_select() | MULTI_HAS_WAKER, Ordering::SeqCst);
            }
            guard.queue.push_back((weak, seq));
        }
    }

    #[inline(always)]
    fn _reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker>,
    ) -> Option<Poll<()>> {
        if let Some(waker) = o_waker.as_ref() {
            match waker.try_change_state(WakerState::Woken, WakerState::Init) {
                Ok(_) => {
                    if waker.will_wake(ctx) {
                        self.reg_waker(waker);
                        return None;
                    }
                }
                Err(state) => {
                    if state < WakerState::Woken as u8 {
                        if waker.will_wake(ctx) {
                            trace_log!(
                                "{} {:?}: will_wake {:?}",
                                self._tag,
                                tokio_task_id!(),
                                waker
                            );
                            // Normally only selection or multiplex future will get here.
                            // No need to reg again, since waker is not consumed.
                            return Some(Poll::Pending);
                        } else {
                            // Spurious woken by runtime, waker can not be re-used (issue 38)
                            // If we se Woken here, only possible otherside has woken it
                            if waker.get_state_relaxed() < WakerState::Woken as u8 {
                                self._clear_wakers(waker, true);
                            }
                            trace_log!(
                                "{} {:?}: drop waker {:?}",
                                self._tag,
                                tokio_task_id!(),
                                waker
                            );
                        }
                    } else if state == WakerState::Closed as u8 {
                        return Some(Poll::Ready(()));
                    } else {
                        panic!("state: impossible for async {:?}", state);
                    }
                }
            }
        }
        let waker = ArcWaker::new_async(ctx);
        self.reg_waker(&waker);
        o_waker.replace(waker);
        None
    }

    #[inline(always)]
    fn _reg_waker_blocking(&self, o_waker: &mut Option<ArcWaker>) {
        if let Some(waker) = o_waker.as_ref() {
            waker.reset_init();
            self.reg_waker(waker);
            trace_log!("{}{:?}: re-reg {:?}", self._tag, tokio_task_id!(), waker);
        } else {
            debug_assert!(o_waker.is_none());
            let waker = tl_blocking_waker();
            self.reg_waker(&waker);
            trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
            o_waker.replace(waker);
        }
    }

    /// If trigger all selector while not empty.
    /// return Some((waker, again))
    /// if there's more waker after pop_first, again=true
    #[inline(always)]
    fn pop_first(&self) -> Option<(ArcWaker, Option<u32>)> {
        // This is a snapshot, it's safe to ignore the new situation after acquire lock
        let flag = self.state.load(Ordering::SeqCst);
        if flag == MULTI_EMPTY {
            return None;
        }
        {
            let mut guard = reg_lock(&self.inner);
            if flag & MULTI_HAS_SELECT > 0 {
                for select in &guard.selectors {
                    select.wake();
                }
            }
            if flag & MULTI_HAS_WAKER > 0 {
                let mut has_pop = false;
                loop {
                    if let Some((weak, seq)) = guard.queue.pop_front() {
                        has_pop = true;
                        if let Some(inner) = weak.upgrade() {
                            if inner.get_seq() != seq {
                                // Re-armed since this entry was pushed: skip,
                                // same treatment as a dead node's upgrade
                                // failure.
                                continue;
                            }
                            if guard.queue.is_empty() {
                                self.state.store(guard.check_select(), Ordering::SeqCst);
                                return Some((ArcWaker::from_arc(inner), None));
                            } else {
                                return Some((ArcWaker::from_arc(inner), Some(guard.seq)));
                            }
                        }
                    } else {
                        if has_pop {
                            // might upgrade encounter weak previous loop
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return None;
                    }
                }
            }
            // nothing changed, don't need to touch the state
            None
        }
    }

    /// ignore the selectors (since triggered in pop_first())
    /// return the flags
    #[inline(always)]
    fn pop_again(&self) -> Option<ArcWaker> {
        // This is a snapshot, it's safe to ignore the new situation after acquire lock
        let flag = self.state.load(Ordering::Acquire);
        if flag == MULTI_EMPTY {
            return None;
        }
        {
            let mut guard = reg_lock(&self.inner);
            let mut has_pop = false;
            loop {
                if let Some((weak, seq)) = guard.queue.pop_front() {
                    has_pop = true;
                    if let Some(inner) = weak.upgrade() {
                        if inner.get_seq() != seq {
                            // Re-armed since this entry was pushed: skip,
                            // same treatment as a dead node's upgrade
                            // failure.
                            continue;
                        }
                        if guard.queue.is_empty() {
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return Some(ArcWaker::from_arc(inner));
                    }
                } else {
                    if has_pop {
                        // might upgrade encounter weak previous loop
                        self.state.store(guard.check_select(), Ordering::SeqCst);
                    }
                    return None;
                }
            }
        }
    }

    /// Call when waker is cancelled
    #[inline(always)]
    fn _clear_wakers(&self, old_waker: &ArcWaker, oneshot: bool) {
        // Don't need accurate, it's optional
        if self.state.load(Ordering::Acquire) & MULTI_HAS_WAKER == 0 {
            return;
        }
        let old_seq = old_waker.get_seq();
        // the macro yield true to stop, false to continue
        macro_rules! process {
            ($guard: expr, $entry: expr) => {{
                let (weak, entry_seq) = $entry;
                if let Some(waker) = weak.upgrade() {
                    let _seq = waker.get_seq();
                    if _seq == old_seq {
                        trace_log!("{}: clear {:?} hit", self._tag, waker);
                        // XXX, it's possible to reuse the waker, leave it for future review
                        true
                    } else if _seq > old_seq {
                        $guard.queue.push_front((weak, entry_seq));
                        true
                    } else {
                        // There might be later waker cancel due to success sending before commit_waiting.
                        // While earlier waker is still waiting.
                        let state = waker.get_state();
                        if state < WakerState::Woken as u8 {
                            $guard.queue.push_front((weak, entry_seq));
                            true
                        } else {
                            if oneshot {
                                trace_log!("{}: cancel {:?} one {}", self._tag, waker, old_seq);
                                true
                            } else {
                                trace_log!("{}: cancel {:?}<{}", self._tag, waker, old_seq);
                                false
                            }
                        }
                    }
                } else {
                    false
                }
            }};
        }
        let mut guard = reg_lock(&self.inner);
        if let Some(entry) = guard.queue.pop_front() {
            if process!(guard, entry) {
                if guard.queue.is_empty() {
                    self.state.store(guard.check_select(), Ordering::SeqCst);
                }
                return;
            }
            loop {
                if let Some(entry) = guard.queue.pop_front() {
                    if process!(guard, entry) {
                        if guard.queue.is_empty() {
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return;
                    }
                } else {
                    // might upgrade encounter weak previous loop
                    self.state.store(guard.check_select(), Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn _cache_waker(_o_waker: Option<ArcWaker>) {
        // XXX: skip cache for now, until we find out miri report of race
        //if let Some(waker) = o_waker {
        //    if waker.get_state() >= WakerState::Woken as u8 {
        //        cache.push(waker);
        //    }
        //}
    }
}

impl Registry for RegistryMulti {
    type Waker = ArcWaker;

    #[inline(always)]
    fn get_waker_state(&self, o_waker: &Option<ArcWaker>, order: Ordering) -> u8 {
        if let Some(waker) = o_waker {
            waker._get_state(order)
        } else {
            unreachable!();
        }
    }

    /// Cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    fn clear_wakers(&self, waker: &ArcWaker) {
        self._clear_wakers(waker, false);
    }

    #[inline(always)]
    fn close(&self) {
        let mut guard = reg_lock(&self.inner);
        for selector in &guard.selectors {
            selector.wake();
        }
        while let Some((weak, seq)) = guard.queue.pop_front() {
            if let Some(waker) = weak.upgrade() {
                if waker.get_seq() != seq {
                    // Stale entry of a node re-armed elsewhere: close_wake()
                    // would stamp Closed on a node that is waiting on
                    // another channel, surfacing as a spurious Disconnect
                    // there. Skip it, as fire() does.
                    continue;
                }
                let _r = waker.close_wake();
                trace_log!("close {} wake {:?} {}", self._tag, waker, _r);
            }
        }
        self.state.store(0, Ordering::SeqCst);
    }

    /// return waker queue size
    #[inline]
    fn len(&self) -> usize {
        let guard = reg_lock(&self.inner);
        guard.queue.len()
    }

    #[inline(always)]
    fn commit_waiting(&self, o_waker: &Option<ArcWaker>) -> u8 {
        if let Some(waker) = &o_waker {
            waker.commit_waiting()
        } else {
            unreachable!();
        }
    }

    /// return false when waker is none
    #[inline(always)]
    fn abandon_waker(&self, waker: &ArcWaker) -> Result<(), u8> {
        // which change Waiting/Init to Closed
        match waker.abandon() {
            Ok(()) => {
                trace_log!("{}: abandon cancel {:?}", self._tag, waker);
                self.clear_wakers(waker);
                Ok(())
            }
            Err(state) => Err(state),
        }
    }

    /// cancel one outdated waker, make sure it does not accumulate
    #[inline(always)]
    fn cancel_waker(&self, o_waker: &mut Option<ArcWaker>) {
        if let Some(waker) = o_waker.take() {
            // If we se Woken here, only possible otherside has woken it
            if waker.get_state_relaxed() >= WakerState::Woken as u8 {
                return;
            }
            self._clear_wakers(&waker, true);
        }
    }

    #[inline(always)]
    fn fire(&self) -> WakeResult {
        if let Some((waker, _last_seq)) = self.pop_first() {
            let r = waker.wake();
            trace_log!("wake {} {:?} {:?}", self._tag, waker, r);
            if r.is_done() {
                return r;
            }
            drop(waker);
            if let Some(mut last_seq) = _last_seq {
                last_seq = last_seq.wrapping_sub(1);
                while let Some(_waker) = self.pop_again() {
                    let r = _waker.wake();
                    trace_log!("wake {} {:?} {:?}", self._tag, _waker, r);
                    if r.is_done() {
                        return r;
                    }
                    // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                    // Because some waker (issued by sink / stream) might be INIT all the time,
                    // prevent to dead loop situation when they are wake up and re-register again.
                    if _waker.get_seq() >= last_seq {
                        trace_log!("wake {} stop at {}", self._tag, last_seq);
                        return WakeResult::Next;
                    }
                }
            }
        }
        WakeResult::Next
    }

    //    #[inline(always)]
    //    fn cache_waker(&self, o_waker: Option<ArcWaker>) {
    //        Self::_cache_waker(o_waker, cache);
    //    }
}

impl RegistrySend for RegistryMulti {
    #[inline(always)]
    fn new() -> Self {
        Self { inner: Mutex::new(RegistryMultiInner::new()), state: AtomicU8::new(0), _tag: "tx" }
    }

    #[inline(always)]
    fn use_direct_copy(&self) -> bool {
        self.state.load(Ordering::Relaxed) != MULTI_EMPTY
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<ArcWaker>) {
        self._reg_waker_blocking(o_waker)
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker)
    }

    /// remove outdated waker, make sure it does not accumulate.
    ///
    /// It's ok to set state with Relaxed here, two scenario:
    /// * set Done while the state is Init, does not matter other thread see it or not.
    /// * other thread might have wake it in the process, but we are dropping it anyway, and then
    ///   reg_waker with a new one.
    #[inline(always)]
    fn cancel_reuse_waker(&self, o_waker: &mut Option<ArcWaker>, state: WakerState) -> u8 {
        if let Some(waker) = o_waker.as_ref() {
            let cur_state = waker.get_state();
            // If we se Woken here, only possible otherside has woken it
            if cur_state >= WakerState::Woken as u8 {
                trace_log!("{}: cancel_reuse {:?} {}", self._tag, waker, cur_state);
                if cur_state < state as u8 {
                    state as u8
                } else {
                    cur_state
                }
            } else {
                self._clear_wakers(waker, true);
                let _ = o_waker.take();
                state as u8
            }
        } else {
            unreachable!();
        }
    }
}

impl RegistryRecv for RegistryMulti {
    #[inline(always)]
    fn new() -> Self {
        Self { inner: Mutex::new(RegistryMultiInner::new()), state: AtomicU8::new(0), _tag: "rx" }
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<ArcWaker>) {
        self._reg_waker_blocking(o_waker)
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker)
    }

    //    #[inline(always)]
    //    fn cache_waker(&self, o_waker: Option<ArcWaker>) {
    //        Self::_cache_waker(o_waker, cache);
    //    }

    #[inline(always)]
    fn reg_select_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        trace_log!("{}: reg for select", self._tag);
        let mut guard = reg_lock(&self.inner);
        if guard.selectors.is_empty() {
            self.state.store(guard.check_waker() | MULTI_HAS_SELECT, Ordering::SeqCst);
        }
        guard.selectors.push(SelectWaker::to_wrapper(waker.clone(), channel_id));
        true
    }

    #[inline(always)]
    fn cancel_select_waker(&self, waker: &Arc<SelectWaker>) {
        let mut guard = reg_lock(&self.inner);
        if let Some((i, _)) = guard.selectors.iter().enumerate().find(|&(_, entry)| entry.eq(waker))
        {
            guard.selectors.remove(i);
        }
        if guard.selectors.is_empty() {
            self.state.store(guard.check_waker(), Ordering::SeqCst);
        }
    }
}

// Due to it's type alias in crate::select::Mux, should be pub
pub struct SelectWakerWrapper(Arc<SelectWaker>, usize);

impl SelectWakerWrapper {
    #[inline(always)]
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.0.cell.pop() {
            trace_log!("rx: wake select");
            self.0.hint.store(self.1, Ordering::Release);
            waker.wake();
        }
    }

    #[inline(always)]
    pub(crate) fn eq(&self, waker: &Arc<SelectWaker>) -> bool {
        Arc::ptr_eq(&self.0, waker)
    }
}

// For multiplex
impl Registry for SelectWakerWrapper {
    type Waker = ArcWaker;

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<ArcWaker>, _order: Ordering) -> u8 {
        unreachable!();
    }

    #[inline(always)]
    fn close(&self) {
        // decrease the opened_channels count to hint Multiplex
        self.0.close();
        self.wake();
    }

    #[inline(always)]
    fn fire(&self) -> WakeResult {
        self.wake();
        WakeResult::Next
    }
}

// For multiplex
impl RegistryRecv for SelectWakerWrapper {
    fn new() -> Self {
        unreachable!();
    }

    fn reg_select_waker(&self, _channel_id: usize, _waker: &Arc<SelectWaker>) -> bool {
        unreachable!();
    }
}

pub(crate) struct SelectWaker {
    cell: WeakCell<WakerInner>,
    // does not need to be correct, just a hint for the try_select
    hint: AtomicUsize,
    o_waker: UnsafeCell<Option<ArcWaker>>,
    // For multiplex, not for select
    opened_channels: AtomicUsize,
}

unsafe impl Send for SelectWaker {}
unsafe impl Sync for SelectWaker {}

impl SelectWaker {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            cell: WeakCell::new(),
            hint: AtomicUsize::new(0),
            o_waker: UnsafeCell::new(None),
            opened_channels: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn init_blocking(&self) {
        let weak = if let Some(waker) = self.get_waker().as_ref() {
            waker.reset_init();
            waker.weak()
        } else {
            let waker = ArcWaker::new_blocking();
            let weak = waker.weak();
            self.get_waker().replace(waker);
            weak
        };
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn init_async(&self, ctx: &mut Context) {
        let waker = ArcWaker::new_async(ctx);
        let weak = waker.weak();
        self.get_waker().replace(waker);
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[inline(always)]
    fn get_waker(&self) -> &mut Option<ArcWaker> {
        unsafe { &mut *self.o_waker.get() }
    }

    #[inline(always)]
    fn clone_weak(&self) -> Weak<WakerInner> {
        self.get_waker().as_ref().unwrap().weak()
    }

    #[inline(always)]
    pub fn add_opened(&self) {
        self.opened_channels.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn get_opened_count(&self) -> usize {
        self.opened_channels.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn to_wrapper(self: Arc<SelectWaker>, idx: usize) -> SelectWakerWrapper {
        SelectWakerWrapper(self, idx)
    }

    #[inline(always)]
    pub fn get_hint(&self) -> usize {
        compiler_fence(Ordering::AcqRel);
        self.hint.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn close(&self) {
        self.opened_channels.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn get_waker_state(&self, order: Ordering) -> u8 {
        self.get_waker().as_ref().unwrap()._get_state(order)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::waker::ArcWaker;

    #[test]
    fn print_waker_registry_size() {
        use std::mem::size_of;
        println!("RegistryMulti size {}", size_of::<RegistryMulti>());
        println!("RegistrySingle size {}", size_of::<RegistrySingle>());
    }

    #[test]
    fn test_registry_multi_pop() {
        let reg = <RegistryMulti as RegistryRecv>::new();

        // test push
        let waker1 = ArcWaker::new_blocking();
        assert_eq!(reg.len(), 0);
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        assert!(waker1.get_seq() > 0);
        assert_eq!(reg.len(), 1);

        let waker2 = ArcWaker::new_blocking();
        reg.reg_waker(&waker2);
        waker2.commit_waiting();
        // Stamps come from the global REG_STAMP counter, so only their
        // relative order is assertable here.
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);

        if let Some((w, seq)) = reg.pop_first() {
            assert!(w.wake() == WakeResult::Next);
            assert!(seq.is_some());
        }
        assert_eq!(waker1.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 1);
        if let Some(w) = reg.pop_again() {
            assert!(w.wake() == WakeResult::Woken);
        }
        assert_eq!(waker2.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_multi_clear_waiting() {
        let reg = <RegistryMulti as RegistryRecv>::new();
        // test seq
        let waker3 = ArcWaker::new_blocking();
        reg.reg_waker(&waker3);
        waker3.commit_waiting();
        assert_eq!(waker3.get_state(), WakerState::Waiting as u8);
        let waker4 = ArcWaker::new_blocking();
        reg.reg_waker(&waker4); // Init
        assert_eq!(waker4.get_state(), WakerState::Init as u8);
        let num_workers = reg.len();
        // Because waker3 not woken up, waker4 is not clear
        reg.clear_wakers(&waker4);
        assert_eq!(reg.len(), num_workers);
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking();
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        assert_eq!(reg.len(), num_workers);
    }

    #[test]
    fn test_registry_multi_clear_oneshot() {
        let reg = <RegistryMulti as RegistryRecv>::new();
        // test seq
        let waker1 = ArcWaker::new_blocking();
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        let waker2 = ArcWaker::new_blocking();
        reg.reg_waker(&waker2); // Init
        waker2.commit_waiting();
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking();
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        println!("clear waker2 oneshot seq {}", waker2.get_seq());
        reg.cancel_waker(&mut Some(waker2));
        assert_eq!(reg.len(), num_workers); // Only nothing happen.
        reg.cancel_waker(&mut Some(waker1));
        assert_eq!(reg.len(), num_workers - 1); // Only waker1 is removed.
    }

    #[test]
    fn test_registry_multi_clear() {
        let reg = <RegistryMulti as RegistryRecv>::new();
        // test seq
        let waker1 = ArcWaker::new_blocking();
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        let waker2 = ArcWaker::new_blocking();
        reg.reg_waker(&waker2); // Init
        drop(waker2); // waker4 is dropped, weak is left
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking();
            reg.reg_waker(&_waker);
        }
        let waker3 = ArcWaker::new_blocking();
        reg.reg_waker(&waker3);
        let _num_workers = reg.len(); // Keep for debugging context, though not used in assertion
        println!("clear waker3 seq={}", waker3.get_seq());
        reg.clear_wakers(&waker3); // nothing happen, because waker3 is there
        assert_eq!(reg.len(), 13);
        reg.clear_wakers(&waker1);
        assert_eq!(reg.len(), 12);
        reg.clear_wakers(&waker3);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_multi_close() {
        let reg = <RegistryMulti as RegistryRecv>::new();
        println!("test close");
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking();
            reg.reg_waker(&_waker);
        }
        assert!(reg.len() > 0);
        reg.close();
        assert_eq!(reg.len(), 0);
    }

    // Design C: blocking waker nodes are immortal (per-thread), so a queue
    // entry whose node has been re-armed must be skipped, not woken. The
    // `node` binding standing in for the thread-local slot keeps the node
    // alive across "episodes".
    #[test]
    fn test_registry_multi_stale_entry_skipped_after_rearm() {
        let reg1 = <RegistryMulti as RegistryRecv>::new();
        let reg2 = <RegistryMulti as RegistryRecv>::new();

        // Episode 1 on reg1; the entry is left behind as residue.
        let node = ArcWaker::new_blocking();
        reg1.reg_waker(&node);

        // Re-arm the same node for reg2 (a new episode elsewhere).
        node.reset();
        reg2.reg_waker(&node);
        node.commit_waiting();
        assert_eq!(node.get_state(), WakerState::Waiting as u8);

        // A fresh waiter on reg1.
        let other = ArcWaker::new_blocking();
        reg1.reg_waker(&other);
        other.commit_waiting();

        // fire() must skip the stale entry and wake the fresh waiter.
        assert!(reg1.fire() == WakeResult::Woken);
        assert_eq!(other.get_state(), WakerState::Woken as u8);
        assert_eq!(node.get_state(), WakerState::Waiting as u8);
    }

    #[test]
    fn test_registry_multi_close_skips_stale_entries() {
        let reg1 = <RegistryMulti as RegistryRecv>::new();
        let reg2 = <RegistryMulti as RegistryRecv>::new();
        let node = ArcWaker::new_blocking();
        reg1.reg_waker(&node);

        // Re-armed node currently waiting on another channel.
        node.reset();
        reg2.reg_waker(&node);
        node.commit_waiting();

        reg1.close();
        assert_eq!(reg1.len(), 0);
        // close() must not stamp Closed on a node waiting elsewhere.
        assert_eq!(node.get_state(), WakerState::Waiting as u8);
    }

    // Design C under miri/tree-borrows: the real thread-local path (lazy init,
    // first registration, spurious-wakeup re-arm, cross-registry re-arm with
    // real park/unpark, thread-exit residue) exercised with real threads.
    // The write-once ThinWaker handle (N1) is what tree-borrows checks here.

    #[test]
    fn test_tl_waker_rearm_reuses_node_across_registrations() {
        // One thread, one episode with an internal spurious wakeup, driven
        // through the real reg_waker_blocking protocol (None -> Some branch).
        let reg = Arc::new(<RegistryMulti as RegistryRecv>::new());
        let t = {
            let reg = reg.clone();
            std::thread::spawn(move || {
                let mut o_waker: Option<ArcWaker> = None;
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                let first_seq = o_waker.as_ref().expect("waker").get_seq();

                // Spurious wakeup inside the episode: re-arm without a new node.
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                let waker = o_waker.as_ref().expect("waker");
                assert!(waker.get_seq() > first_seq);
                // Duplicate entries pointing at the same node are the existing
                // F3 behavior; the older one is stale after the re-arm.
                assert_eq!(reg.len(), 2);
                assert_eq!(waker.get_state(), WakerState::Init as u8);
                reg.commit_waiting(&o_waker);
            })
        };
        t.join().expect("join");

        // fire() must skip the stale duplicate and wake the current entry.
        let r = reg.fire();
        assert!(r.is_done() || r == WakeResult::Next);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_rearmed_node_not_stolen_by_old_registry() {
        // The design docs' 5.4 stall interleaving end-to-end with real
        // park/unpark: T1 leaves an entry on reg1, re-arms on reg2 and parks;
        // T2 waits on reg1. Firing reg1 must skip T1's stale entry and wake
        // T2; T1 is only woken by reg2's own fire.
        use std::sync::mpsc::channel;
        let reg1 = Arc::new(<RegistryMulti as RegistryRecv>::new());
        let reg2 = Arc::new(<RegistryMulti as RegistryRecv>::new());

        let (tx1, rx1) = channel();
        let t1 = {
            let reg1 = reg1.clone();
            let reg2 = reg2.clone();
            std::thread::spawn(move || {
                let mut o_waker: Option<ArcWaker> = None;
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg1, &mut o_waker);
                // Episode 1 ends: the node survives in the thread-local slot.
                o_waker.take();

                let mut o_waker: Option<ArcWaker> = None;
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg2, &mut o_waker);
                let state = reg2.commit_waiting(&o_waker);
                tx1.send(()).expect("send");
                if state == WakerState::Waiting as u8 {
                    std::thread::park();
                }
                assert_eq!(o_waker.as_ref().expect("waker").get_state(), WakerState::Woken as u8);
            })
        };

        let (tx2, rx2) = channel();
        let t2 = {
            let reg1 = reg1.clone();
            std::thread::spawn(move || {
                let mut o_waker: Option<ArcWaker> = None;
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg1, &mut o_waker);
                let state = reg1.commit_waiting(&o_waker);
                tx2.send(()).expect("send");
                if state == WakerState::Waiting as u8 {
                    std::thread::park();
                }
                assert_eq!(o_waker.as_ref().expect("waker").get_state(), WakerState::Woken as u8);
            })
        };

        rx1.recv().expect("t1 committed");
        rx2.recv().expect("t2 committed");
        // Both waiters are Waiting: fire() skips T1's stale entry and wakes T2.
        assert_eq!(reg1.fire(), WakeResult::Woken);
        // T1's node is still armed on reg2; reg2's own event wakes it.
        assert_eq!(reg2.fire(), WakeResult::Woken);
        t1.join().expect("join t1");
        t2.join().expect("join t2");
    }

    #[test]
    fn test_thread_exit_residue_popped_without_panic() {
        // The thread-local slot drops the node at thread exit; the weak
        // entries left in the queue must take the existing dead-node path
        // (upgrade failure) on both fire() and close().
        let reg = Arc::new(<RegistryMulti as RegistryRecv>::new());
        let t = {
            let reg = reg.clone();
            std::thread::spawn(move || {
                let mut o_waker: Option<ArcWaker> = None;
                <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                reg.commit_waiting(&o_waker);
                // Return without cancelling: the episode is abandoned and the
                // thread exits, dropping the immortal node with it.
            })
        };
        t.join().expect("join");
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.fire(), WakeResult::Next);
        reg.close();
        assert_eq!(reg.len(), 0);
    }
}

// Model-checked tests for design C. Run with:
//   cargo test --lib --release --features loom loom_ -- --nocapture
//
// TLS is not modeled (see tl_blocking_waker's loom branch): the modeled
// object is the node identity + re-arm protocol, so per-thread nodes are
// injected explicitly. Parking itself is not simulated either — unpark/park
// correctness is std's contract — instead the observable effects of fire()
// and close() (the returned WakeResult and the waker state machine) are
// asserted deterministically after the interleaving, which loom enumerates
// in full. Spin-based fake parking does not work under loom: a path where
// the scheduler keeps picking the spinner is legal, so any bounded spin
// loop would fail spuriously.
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;

    fn wait_committed(phase: &AtomicUsize) {
        while phase.load(Ordering::SeqCst) == 0 {
            loom::thread::yield_now();
        }
    }

    #[test]
    fn loom_cross_registry_stale_entry_skipped_by_fire() {
        // The design docs' 5.4 stall interleaving: T1 leaves an entry on reg1,
        // re-arms on reg2 and commits; T2 fires reg1 with a fresh waiter
        // waiting. The re-arm is fully published before the fire (the barrier
        // gives fire's relaxed seq read a happens-before edge to the re-arm's
        // set_seq), so the stale entry is skipped deterministically.
        loom::model(|| {
            let reg1 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let reg2 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let committed1 = Arc::new(AtomicUsize::new(0));
            let committed3 = Arc::new(AtomicUsize::new(0));

            // T1's per-thread node, injected explicitly; kept alive by the
            // main thread so the weak entries stay upgradable after T1 ends.
            let node = ArcWaker::new_blocking();

            let t1 = {
                let reg1 = reg1.clone();
                let reg2 = reg2.clone();
                let committed1 = committed1.clone();
                let node = node.clone_node();
                loom::thread::spawn(move || {
                    // Episode on reg1, then re-arm on reg2.
                    reg1.reg_waker(&node);
                    node.reset();
                    reg2.reg_waker(&node);
                    node.commit_waiting();
                    committed1.store(1, Ordering::SeqCst);
                })
            };

            let t3 = {
                let reg1 = reg1.clone();
                let committed3 = committed3.clone();
                loom::thread::spawn(move || {
                    let other = ArcWaker::new_blocking();
                    reg1.reg_waker(&other);
                    other.commit_waiting();
                    committed3.store(1, Ordering::SeqCst);
                    other
                })
            };

            wait_committed(&committed3);
            wait_committed(&committed1);
            // fire() must skip the stale entry and wake the fresh waiter.
            assert_eq!(reg1.fire(), WakeResult::Woken);
            let other = t3.join().unwrap();
            assert_eq!(other.get_state(), WakerState::Woken as u8);
            // The skip means the node waiting on reg2 was not touched at all.
            assert_eq!(node.get_state(), WakerState::Waiting as u8);

            // Its own registry's event wakes it normally.
            assert_eq!(reg2.fire(), WakeResult::Woken);
            t1.join().unwrap();
            assert_eq!(node.get_state(), WakerState::Woken as u8);
            // The stale entry may remain when `other` sat at the queue head
            // (the wake returned Woken before reaching it); the next fire
            // skips it (the barrier keeps the seq read fresh) and drains.
            assert_eq!(reg1.fire(), WakeResult::Next);
            assert_eq!(reg1.len(), 0);
        });
    }

    #[test]
    fn loom_fire_vs_rearm_race_recovers_within_one_event() {
        // The known narrow window (design docs 3.4): the re-arm's set_seq and
        // fire's seq read sit under different mutexes with no hb edge, so a
        // relaxed read may still observe the pre-re-arm stamp and the stale
        // entry steals one fire. loom enumerates that interleaving too; the
        // contract asserted here is the bounded recovery: the delayed waiter
        // is woken by the very next event, and nothing is ever Closed.
        loom::model(|| {
            let reg1 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let reg2 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let committed1 = Arc::new(AtomicUsize::new(0));
            let committed3 = Arc::new(AtomicUsize::new(0));

            let node = ArcWaker::new_blocking();

            let t1 = {
                let reg1 = reg1.clone();
                let reg2 = reg2.clone();
                let committed1 = committed1.clone();
                let node = node.clone_node();
                loom::thread::spawn(move || {
                    reg1.reg_waker(&node);
                    node.reset();
                    reg2.reg_waker(&node);
                    node.commit_waiting();
                    committed1.store(1, Ordering::SeqCst);
                })
            };

            let t3 = {
                let reg1 = reg1.clone();
                let committed3 = committed3.clone();
                loom::thread::spawn(move || {
                    let other = ArcWaker::new_blocking();
                    reg1.reg_waker(&other);
                    other.commit_waiting();
                    committed3.store(1, Ordering::SeqCst);
                    other
                })
            };

            // Only the fresh waiter is barriered before the fire; the re-arm
            // races it freely.
            wait_committed(&committed3);
            assert_eq!(reg1.fire(), WakeResult::Woken);
            let other = t3.join().unwrap();
            if other.get_state() != WakerState::Woken as u8 {
                // The stale entry stole this fire: its node got the wake.
                assert_eq!(node.get_state(), WakerState::Woken as u8);
                // Recovery is one event away.
                assert_eq!(reg1.fire(), WakeResult::Woken);
            }
            assert_eq!(other.get_state(), WakerState::Woken as u8);

            wait_committed(&committed1);
            let r = reg2.fire();
            assert!(r == WakeResult::Woken || r == WakeResult::Next);
            t1.join().unwrap();
            // Never Closed; woken by reg2's event or by the stolen fire above.
            assert_eq!(node.get_state(), WakerState::Woken as u8);
            assert!(reg1.len() <= 1);
        });
    }

    #[test]
    fn loom_close_skips_stale_entry_and_keeps_foreign_waiter_intact() {
        // close() on reg1 must not stamp Closed on the node that is currently
        // Waiting on reg2 (that would surface as a spurious Disconnect
        // there), while the reg1-local waiter still gets its Closed.
        loom::model(|| {
            let reg1 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let reg2 = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let committed1 = Arc::new(AtomicUsize::new(0));
            let committed3 = Arc::new(AtomicUsize::new(0));

            let node = ArcWaker::new_blocking();

            let t1 = {
                let reg1 = reg1.clone();
                let reg2 = reg2.clone();
                let committed1 = committed1.clone();
                let node = node.clone_node();
                loom::thread::spawn(move || {
                    reg1.reg_waker(&node);
                    node.reset();
                    reg2.reg_waker(&node);
                    node.commit_waiting();
                    committed1.store(1, Ordering::SeqCst);
                })
            };

            let t3 = {
                let reg1 = reg1.clone();
                let committed3 = committed3.clone();
                loom::thread::spawn(move || {
                    let other = ArcWaker::new_blocking();
                    reg1.reg_waker(&other);
                    other.commit_waiting();
                    committed3.store(1, Ordering::SeqCst);
                    other
                })
            };

            wait_committed(&committed1);
            wait_committed(&committed3);
            reg1.close();
            let other = t3.join().unwrap();
            // The live reg1 waiter was closed-woken.
            assert_eq!(other.get_state(), WakerState::Closed as u8);
            // The stale entry was skipped: the node waiting on reg2 is still
            // exactly Waiting, not spuriously Disconnected.
            assert_eq!(node.get_state(), WakerState::Waiting as u8);
            // Its own registry's event wakes it normally.
            assert_eq!(reg2.fire(), WakeResult::Woken);
            t1.join().unwrap();
            assert_eq!(node.get_state(), WakerState::Woken as u8);
            assert_eq!(reg1.len(), 0);
        });
    }

    #[test]
    fn loom_thread_exit_residue_is_popped_safely() {
        // The node dies with its thread; the weak entry left in the queue
        // must take the dead-node upgrade path on every pop site.
        loom::model(|| {
            let reg = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let t = {
                let reg = reg.clone();
                loom::thread::spawn(move || {
                    let mut o_waker: Option<ArcWaker> = None;
                    <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                    reg.commit_waiting(&o_waker);
                    // Thread exits without cancelling the episode.
                })
            };
            t.join().unwrap();
            assert_eq!(reg.fire(), WakeResult::Next);
            reg.close();
            assert_eq!(reg.len(), 0);
        });
    }

    #[test]
    fn loom_spurious_rearm_duplicate_entry_fire_wakes_current() {
        // A spurious wakeup re-arms through the Some branch, leaving a
        // duplicate entry; fire() must skip the stale duplicate and wake the
        // current registration.
        loom::model(|| {
            let reg = Arc::new(<RegistryMulti as RegistryRecv>::new());
            let committed = Arc::new(AtomicUsize::new(0));
            let t = {
                let reg = reg.clone();
                let committed = committed.clone();
                loom::thread::spawn(move || {
                    let mut o_waker: Option<ArcWaker> = None;
                    <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                    // Spurious wakeup inside the episode: re-arm, no new node.
                    <RegistryMulti as RegistryRecv>::reg_waker_blocking(&reg, &mut o_waker);
                    reg.commit_waiting(&o_waker);
                    committed.store(1, Ordering::SeqCst);
                    o_waker
                })
            };
            wait_committed(&committed);
            assert_eq!(reg.fire(), WakeResult::Woken);
            let o_waker = t.join().unwrap();
            assert_eq!(o_waker.as_ref().unwrap().get_state(), WakerState::Woken as u8);
            assert_eq!(reg.len(), 0);
        });
    }
}
