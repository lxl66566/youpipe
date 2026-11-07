use crate::backoff::*;
pub(crate) use crate::crossbeam::err::*;
pub(crate) use crate::flavor::{ChannelSharedSelect, Flavor, FlavorSelect, Token};
use crate::trace_log;
pub(crate) use crate::waker::*;
pub(crate) use crate::waker_registry::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// What a cancellation-abandon (`abandon_send_waker`/`abandon_recv_waker`)
/// leaves in the caller's slot: whether the Drop may keep the node for the
/// next registration to re-arm in place, and — send side — whether item
/// ownership already left the future.
pub(crate) enum AbandonOutcome {
    /// Unregistered and retired in place: keep the node in the slot
    /// (cancel-path node reuse, youpipe P-4).
    Reusable,
    /// Dead episode (close_wake'd Closed / Done) or a registry without
    /// cancel-path reuse (the node stays registered): drop the node.
    Dead,
    /// Direct-copy consumed the item; ownership already left the future
    /// (send side only; the node is dead too).
    Consumed,
}

pub struct ChannelShared<F: Flavor> {
    pub(crate) inner: F,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    pub(crate) senders: F::Send,
    pub(crate) recvs: F::Recv,
    pub(crate) backoff_limit: u16,
    pub(crate) large: bool,
}

// youpipe fork extension (ring recycling): every endpoint wrapper
// (`MTx`/`MRx`/`MAsyncTx`/...) is `Sync` and derefs into this type, so
// the fork already asserts cross-thread `&ChannelShared` sharing for
// every `Flavor`; surfacing it as an impl lets the recycling cache erase
// the handle as `dyn Any + Send + Sync`.
unsafe impl<F: Flavor> Sync for ChannelShared<F> {}

impl<F: Flavor> ChannelShared<F> {
    pub(crate) fn new(inner: F, senders: F::Send, recvs: F::Recv) -> Arc<Self> {
        let mut large = false;
        if let Some(bound) = inner.capacity() {
            if bound >= 10 {
                large = true;
            }
        }
        Arc::new(Self {
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            senders,
            recvs,
            backoff_limit: inner.backoff_limit(),
            large,
            inner,
        })
    }

    #[inline(always)]
    pub(crate) fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        if !self.is_tx_closed() {
            if let Some(item) = self.inner.try_recv() {
                self.on_recv();
                return Ok(item);
            }
        } else {
            if let Some(item) = self.inner.try_recv_final() {
                self.on_recv();
                return Ok(item);
            }
            return Err(TryRecvError::Disconnected);
        }
        Err(TryRecvError::Empty)
    }

    /// youpipe fork extension (batch recv): claim a run of ready items with
    /// amortized cursor updates. Each claimed item frees one slot for a
    /// parked producer, so the batch fires one recv event per item —
    /// merged into a single registry lock acquisition via `fire_n` (P-3),
    /// with the same wake accounting as the per-item `on_recv` path.
    /// Returns the count claimed (0 = nothing ready right now; use
    /// `try_recv` to distinguish Empty from Disconnected).
    #[inline(always)]
    pub(crate) fn try_recv_batch(&self, out: &mut [MaybeUninit<F::Item>]) -> usize {
        let n = self.inner.try_recv_batch(out);
        self.senders.fire_n(n);
        n
    }

    /// youpipe fork extension (batch send): non-blocking prefix send.
    /// Each new item owes one consumer wake; the fires are merged into a
    /// single registry lock acquisition via `fire_n` (P-3), with the same
    /// wake accounting as the per-item `on_send` path. Returns 0 once the
    /// receiver is dropped — callers fall back to per-item `send` to
    /// obtain the `Disconnected` error.
    #[inline(always)]
    pub(crate) unsafe fn try_send_batch(&self, values: &mut [MaybeUninit<F::Item>]) -> usize {
        if self.is_rx_closed() {
            return 0;
        }
        let n = self.inner.try_send_batch(values);
        self.recvs.fire_n(n);
        n
    }

    #[inline(always)]
    pub(crate) fn read_with_token(&self, token: Token) -> Result<F::Item, RecvError>
    where
        F: FlavorSelect,
    {
        if token.pos.is_null() {
            Err(RecvError)
        } else {
            let item = self.inner.read_with_token(token);
            self.on_recv();
            Ok(item)
        }
    }

    /// youpipe fork extension (ring recycling): reset a fully-drained,
    /// fully-closed channel so its endpoints can be rebuilt over the same
    /// allocation (a recycling cache holds the shared handle between
    /// incarnations). Atomically claims both endpoint refcounts (0 -> 1);
    /// on success the ring cursors and both waker registries are back at
    /// construction state and the refcounts are left at 1/1 for the
    /// caller's `SenderType::new` / `ReceiverType::new` pair. Returns
    /// false — touching nothing beyond the rolled-back claims — when any
    /// endpoint is still alive, the ring was not drained (leftover items
    /// must drop through the normal teardown), or the flavor has no
    /// recyclable ring.
    ///
    /// Ordering: each claim CAS is Acquire on success, so the Release
    /// decrement of the last endpoint drop (`close_tx`/`close_rx`, with
    /// the registry `close()` sequenced after it) happens-before
    /// everything this method writes — the reset runs on quiescent state.
    ///
    /// Precondition (the recycling cache's contract): no `WeakTx` may
    /// upgrade while `try_reset` runs — a resurrection grafted onto the
    /// claimed counts would corrupt the refcount the rebuilt endpoints
    /// assume.
    pub fn try_reset(&self) -> bool {
        if self.tx_count.compare_exchange(0, 1, Ordering::SeqCst, Ordering::Acquire).is_err() {
            return false;
        }
        if self.rx_count.compare_exchange(0, 1, Ordering::SeqCst, Ordering::Acquire).is_err() {
            // Roll back with a decrement, not a store: a racing weak
            // upgrade that bumped the claimed count must survive it.
            self.tx_count.fetch_sub(1, Ordering::Release);
            return false;
        }
        if !self.inner.try_reset() {
            self.rx_count.fetch_sub(1, Ordering::Release);
            self.tx_count.fetch_sub(1, Ordering::Release);
            return false;
        }
        self.senders.reset();
        self.recvs.reset();
        true
    }

    /// The number of messages in the channel.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// The capacity of the channel. Returns `None` for unbounded channels.
    #[inline(always)]
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    /// Returns `true` if the channel is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns the number of senders for the channel.
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::SeqCst)
    }

    /// Returns the number of receivers for the channel.
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn might_congest(&self) -> bool {
        F::IS_BOUNDED && self.senders.use_direct_copy()
    }

    /// Returns the number of wakers for senders and receivers. For debugging purposes.
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn is_tx_closed(&self) -> bool {
        self.tx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn is_rx_closed(&self) -> bool {
        self.rx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        // The drop will close_tx, which has release fence
        let _ = self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    /// for Upgrade of WeakTx
    #[inline(always)]
    pub(crate) fn try_add_tx(&self) -> bool {
        let mut count = self.tx_count.load(Ordering::Relaxed);
        loop {
            if count == 0 {
                return false;
            }
            match self.tx_count.compare_exchange(
                count,
                count + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(_count) => {
                    count = _count;
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        // The drop will close_rx, which has release fence
        let _ = self.rx_count.fetch_add(1, Ordering::Relaxed);
    }

    /// This method is called when a sender is dropped.
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        let old = self.tx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from tx");
            fence(Ordering::SeqCst);
            self.recvs.close();
        } else {
            trace_log!("drop tx {}", old - 1);
        }
    }

    /// This method is called when a receiver is dropped.
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        let old = self.rx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from rx");
            fence(Ordering::SeqCst);
            // There's SeqCst fence inside RegistrySender::close
            self.senders.close();
        } else {
            trace_log!("drop rx {}", old - 1);
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    ///
    /// NOTE: when return state=Done, the waker is not set to Done
    #[inline]
    pub(crate) fn sender_double_check<const SINK: bool>(
        &self, item: &MaybeUninit<F::Item>, o_waker: &mut Option<<F::Send as Registry>::Waker>,
    ) -> u8 {
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = self.inner.try_send_oneshot(item.as_ptr()) {
            if res {
                self.on_send();
                self.senders.cancel_reuse_waker(o_waker, WakerState::Done)
            } else {
                let state = if SINK {
                    WakerState::Init as u8
                } else {
                    self.senders.commit_waiting(o_waker)
                };
                if self.is_rx_closed() {
                    return WakerState::Closed as u8;
                }
                state
            }
        } else {
            // Unlikely to be disconnected,
            self.senders.cancel_reuse_waker(o_waker, WakerState::Woken)
        }
    }

    /// Wait a little more for the waker state change,
    /// NOTE: it's important to yield when you have more sender than receiver
    #[inline(always)]
    pub(crate) fn sender_snooze(
        &self, o_waker: &Option<<F::Send as Registry>::Waker>, backoff: &mut Backoff,
    ) -> u8 {
        backoff.reset();
        loop {
            let state = self.senders.get_waker_state(o_waker, Ordering::Relaxed);
            compiler_fence(Ordering::AcqRel);
            if state >= WakerState::Woken as u8 {
                return state;
            }
            if backoff.snooze() {
                return state;
            }
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire();
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        self.senders.fire();
        //        if WakeResult::Sent == self.senders.fire(&self.inner) {
        //            self.on_send();
        //        }
    }

    /// Call on cancellation. The outcome tells a slot-borrowing (cached)
    /// future's Drop what may happen to the node (youpipe P-4 cancel-path
    /// reuse): `Reusable` — no longer registered and retired in place, keep
    /// it in the slot for the next registration to re-arm in place;
    /// `Dead` — close_wake'd, Done, or a registry without cancel-path
    /// reuse, drop it; `Consumed` — direct-copy already moved the item
    /// (send side only), ownership left the future.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(
        &self, waker: &<F::Send as Registry>::Waker,
    ) -> AbandonOutcome {
        match self.senders.abandon_waker(waker) {
            Ok(_) => {
                if self.senders.retire_abandoned_waker(waker) {
                    AbandonOutcome::Reusable
                } else {
                    AbandonOutcome::Dead
                }
            }
            Err(state) => {
                trace_log!("tx: abandon err  {:?} {}", waker, state);
                if state == WakerState::Woken as u8 {
                    // We are awake, but give up sending, should notify another sender for safety
                    self.on_recv();
                    AbandonOutcome::Reusable
                } else if state == WakerState::Closed as u8 {
                    AbandonOutcome::Dead
                } else if state == WakerState::Reuse as u8 {
                    // Retired node (not registered): nothing to pass on.
                    AbandonOutcome::Reusable
                } else {
                    debug_assert_eq!(state, WakerState::Done as u8);
                    // Unused code for direct_copy
                    AbandonOutcome::Consumed
                }
            }
        }
    }

    /// Call on cancellation; the outcome semantics are those of
    /// [`Self::abandon_send_waker`] (`Consumed` cannot occur on the recv
    /// side).
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(
        &self, waker: &<F::Recv as Registry>::Waker,
    ) -> AbandonOutcome {
        if let Err(state) = self.recvs.abandon_waker(waker) {
            trace_log!("rx: abandon err {:?} {}", waker, state);
            if state == WakerState::Woken as u8 {
                // We are awake, but give up receiving, should notify another receiver for safety
                self.on_send();
                AbandonOutcome::Reusable
            } else if state == WakerState::Closed as u8 {
                // Closed
                AbandonOutcome::Dead
            } else if state == WakerState::Reuse as u8 {
                // Retired node (not registered): nothing to pass on.
                AbandonOutcome::Reusable
            } else {
                debug_assert_eq!(state, WakerState::Done as u8);
                // Unused code for direct_copy
                AbandonOutcome::Dead
            }
        } else if self.recvs.retire_abandoned_waker(waker) {
            AbandonOutcome::Reusable
        } else {
            AbandonOutcome::Dead
        }
    }

    #[inline(always)]
    pub(crate) fn get_async_backoff(&self) -> Option<Backoff> {
        if self.large {
            return None;
        }
        let cfg = BackoffConfig::detect();
        if cfg.spin_limit == 0 {
            // 1 core don't backoff
            return None;
        }
        // It's effective to yield for size=1
        Some(Backoff::from(cfg.limit(self.backoff_limit)))
    }
}

impl<F: Flavor + FlavorSelect> ChannelSharedSelect for ChannelShared<F> {
    #[inline(always)]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        // NOTE: we cannot use generic const for final_check because ChannelSharedSelect trait
        // require Object Safety
        let closed = if !final_check { false } else { self.is_tx_closed() };
        if let Some(token) = self.inner.try_select(final_check) {
            return Some(token);
        }
        if closed {
            return Some(Token::default());
        }
        None
    }

    #[inline(always)]
    fn reg_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        self.recvs.reg_select_waker(channel_id, waker)
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &Arc<SelectWaker>) {
        self.recvs.cancel_select_waker(waker)
    }
}

/// On timed out, returns Err(())
#[inline(always)]
pub fn check_timeout(deadline: Option<Instant>) -> Result<Option<Duration>, ()> {
    if let Some(end) = deadline {
        let now = Instant::now();
        if now < end {
            return Ok(Some(end - now));
        } else {
            return Err(());
        }
    }
    Ok(None)
}

// youpipe fork extension (ring recycling, todo #45): the contract of
// `ChannelShared::try_reset` — exclusive claim, drained-only, and a
// fully functional rebuilt channel (park/wake included: the reset
// registries must serve a new incarnation exactly like fresh ones).
#[cfg(test)]
mod recycle_tests {
    use crate::mpmc;
    use crate::mpsc;
    use crate::{MRx, MTx, ReceiverType, Rx, SenderType};

    #[test]
    fn try_reset_refuses_live_endpoints_and_undrained_ring() {
        let (tx, rx) = mpmc::bounded_blocking::<usize>(4);
        let shared = tx.shared_handle();
        assert!(!shared.try_reset(), "endpoints alive must refuse");
        assert_eq!(shared.get_tx_count(), 1);
        assert_eq!(shared.get_rx_count(), 1);

        // Undrained: drop both endpoints with an item still inside.
        drop(rx);
        drop(tx);
        let (tx2, rx2) = mpmc::bounded_blocking::<usize>(4);
        let shared2 = tx2.shared_handle();
        tx2.send(7).unwrap();
        drop(tx2);
        drop(rx2);
        assert!(!shared2.try_reset(), "leftover items must refuse");
        // The refused claim rolled both refcounts back to 0.
        assert_eq!(shared2.get_tx_count(), 0);
        assert_eq!(shared2.get_rx_count(), 0);
    }

    #[test]
    fn try_reset_rebuilds_fully_functional_mpmc_channel() {
        let (tx, rx) = mpmc::bounded_blocking::<usize>(4);
        let shared = tx.shared_handle();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        drop(tx);
        drop(rx);
        assert!(shared.try_reset());
        assert_eq!(shared.get_tx_count(), 1);
        assert_eq!(shared.get_rx_count(), 1);
        assert!(shared.is_empty());
        assert_eq!(shared.get_wakers_count(), (0, 0));

        // Rebuild over the same allocation and prove the full contract:
        // data plane, EOF, and a real park/wake round through the reset
        // registries.
        let tx: MTx<mpmc::Array<usize>> = SenderType::new(shared.clone());
        let rx: MRx<mpmc::Array<usize>> = ReceiverType::new(shared);
        tx.send(10).unwrap();
        let worker = {
            let tx = tx.clone();
            std::thread::spawn(move || {
                // Fill past capacity: the second send parks on the reset
                // senders registry until the main thread drains.
                tx.send(11).unwrap();
                tx.send(12).unwrap();
            })
        };
        assert_eq!(rx.recv(), Ok(10));
        assert_eq!(rx.recv(), Ok(11));
        assert_eq!(rx.recv(), Ok(12));
        worker.join().unwrap();
        drop(tx);
        assert!(rx.recv().is_err(), "EOF must survive the recycle");
        drop(rx);
    }

    #[test]
    fn try_reset_rebuilds_mpsc_channel() {
        let (tx, rx) = mpsc::bounded_blocking::<usize>(4);
        let shared = tx.shared_handle();
        tx.send(1).unwrap();
        assert_eq!(rx.recv(), Ok(1));
        drop(tx);
        drop(rx);
        assert!(shared.try_reset());
        let tx: MTx<mpsc::Array<usize>> = SenderType::new(shared.clone());
        let rx: Rx<mpsc::Array<usize>> = ReceiverType::new(shared);
        tx.send(2).unwrap();
        assert_eq!(rx.recv(), Ok(2));
        drop(tx);
        assert!(rx.recv().is_err());
        drop(rx);
    }

    #[test]
    fn try_reset_across_many_laps_keeps_stamp_protocol() {
        // Wrap the ring through several laps first so the reset must
        // overwrite real residue stamps, then prove the protocol accepts
        // the restored state on both sides.
        let (tx, rx) = mpmc::bounded_blocking::<usize>(3);
        let shared = tx.shared_handle();
        for i in 0..20 {
            tx.send(i).unwrap();
            assert_eq!(rx.recv(), Ok(i));
        }
        drop(tx);
        drop(rx);
        assert!(shared.try_reset());
        let tx: MTx<mpmc::Array<usize>> = SenderType::new(shared.clone());
        let rx: MRx<mpmc::Array<usize>> = ReceiverType::new(shared);
        for i in 0..20 {
            tx.send(i).unwrap();
            assert_eq!(rx.recv(), Ok(i));
        }
        drop(tx);
        drop(rx);
    }
}
