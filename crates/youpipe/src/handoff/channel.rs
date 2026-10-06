use std::{future::Future, mem::MaybeUninit};

use crossfire::{mpmc, mpsc};

// Channel identity for `crossfire-trace` forensics: the shared `ChannelShared`
// address is stable for the channel's lifetime, letting per-thread trace logs
// be grouped by channel when replaying a hang. Same feature gate as the
// crossfire `trace_log` episodes (zero cost when off).
#[cfg(feature = "crossfire-trace")]
macro_rules! trace_ch {
    ($shared:expr, $op:expr) => {
        log::debug!("{} @{:p}", $op, std::ptr::from_ref($shared));
    };
}

// ── hotpath data-plane probes (feature-gated, zero-cost when off) ──
//
// Every crossfire wrapper below carries `#[hotpath::measure(impl_type)]`,
// making the data plane attributable: producers blocked inside crossfire
// previously showed zero worker-side activity (todo P0 #1 evidence). Caliber:
// - attribution only — the per-call guard inflates absolute ns/item; compare distributions across
//   scenarios, never against unprobed wall time.
// - generic methods aggregate all `T` monomorphizations under one label.
// - the trait impls (SendItem/RecvItem/AsyncRecvItem) delegate to these inherent methods, so every
//   call funnels through exactly one probe.

/// Blocking MPMC sender.
pub struct SyncSender<T: Send + 'static> {
    tx: crossfire::MTx<mpmc::Array<T>>,
}

/// Blocking MPMC receiver.
pub struct SyncReceiver<T: Send + 'static> {
    rx: crossfire::MRx<mpmc::Array<T>>,
}

/// Alias for [`SyncSender`].
pub type Sender<T> = SyncSender<T>;
/// Alias for [`SyncReceiver`].
pub type Receiver<T> = SyncReceiver<T>;

/// Create a bounded blocking MPMC channel.
#[must_use]
pub fn channel<T: Send + 'static>(capacity: usize) -> (SyncSender<T>, SyncReceiver<T>) {
    let (tx, rx) = mpmc::bounded_blocking::<T>(capacity);
    (SyncSender { tx }, SyncReceiver { rx })
}

impl<T: Send + 'static> SyncSender<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncSender"))]
    pub fn send(&self, item: T) -> Result<(), ChannelError> {
        #[cfg(feature = "crossfire-trace")]
        trace_ch!(&**self.tx, "tx send");
        self.tx.send(item).map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncSender"))]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TrySendError::Full(v),
            crossfire::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
    }

    /// Non-blocking batch send of the longest free prefix of `buf`; returns
    /// the count sent. `buf` must be fully initialized; the sent prefix is
    /// moved out (do not read it again), the remainder stays owned by the
    /// caller. Backpressure granularity is unchanged: callers fall back to
    /// per-item blocking [`Self::send`] for the remainder, parking per item
    /// exactly as the per-item path would.
    ///
    /// Probe caliber: one hotpath guard per *batch* (see
    /// [`SyncReceiver::try_recv_batch`]).
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncSender"))]
    pub fn try_send_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        unsafe { self.tx.try_send_batch(buf) }
    }
}

impl<T: Send + 'static> Clone for SyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: Send + 'static> SyncReceiver<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncReceiver"))]
    pub fn recv(&self) -> Result<T, ChannelError> {
        #[cfg(feature = "crossfire-trace")]
        trace_ch!(&**self.rx, "rx recv");
        self.rx.recv().map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncReceiver"))]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }

    /// Non-blocking batch claim of the ready run into `buf` (a typed Vec's
    /// `spare_capacity_mut()`); returns the count claimed. 0 means nothing
    /// is ready right now — NOT an Empty/Closed verdict (`try_recv`
    /// distinguishes). Amortizes the ring's per-item cursor CAS over the
    /// batch ([`Self::try_recv`] stays the single-item contract).
    ///
    /// Probe caliber: one hotpath guard per *batch*, not per item —
    /// per-item attribution lives in the p50 divided by the mean batch
    /// size; do not compare its per-call p50 against `try_recv`'s directly.
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncReceiver"))]
    pub fn try_recv_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        self.rx.try_recv_batch(buf)
    }

    /// Whether every sender has been dropped — one load of the tx-count
    /// line, written only on sender drop, so it never bounces while the
    /// channel is open. Lets a 0-count [`Self::try_recv_batch`] resolve to
    /// Empty without re-walking the ring via [`Self::try_recv`] (review
    /// P-6: that second walk doubled the stamp/head cache-line traffic of
    /// empty-ring polling under a crowd of workers).
    #[must_use]
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "SyncReceiver"))]
    pub fn is_disconnected(&self) -> bool {
        self.rx.is_disconnected()
    }
}

impl<T: Send + 'static> Clone for SyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

/// Async MPMC sender.
pub struct AsyncSender<T: Send + Unpin + 'static> {
    tx: crossfire::MAsyncTx<mpmc::Array<T>>,
}

/// Async MPMC receiver.
pub struct AsyncReceiver<T: Send + Unpin + 'static> {
    rx: crossfire::MAsyncRx<mpmc::Array<T>>,
}

/// Create a bounded async MPMC channel.
#[must_use]
pub fn async_channel<T: Send + Unpin + 'static>(
    capacity: usize,
) -> (AsyncSender<T>, AsyncReceiver<T>) {
    let (tx, rx) = mpmc::bounded_async::<T>(capacity);
    (AsyncSender { tx }, AsyncReceiver { rx })
}

/// Create a bounded *mixed-mode* MPMC channel: a blocking sync sender paired
/// with an async receiver over the same underlying queue.
///
/// The right primitive for a sync→async bridge: the producer calls the
/// naturally blocking [`SyncSender::send`] (crossfire parks it until the async
/// consumer drains an item) instead of `try_send` + `yield_now` busy-spinning
/// on `Full`; the consumer stays fully async. Both endpoints share one
/// `mpmc::Array` — no extra hop relative to [`async_channel`].
#[must_use]
pub fn sync_async_channel<T: Send + Unpin + 'static>(
    capacity: usize,
) -> (SyncSender<T>, AsyncReceiver<T>) {
    let (tx, rx) = mpmc::bounded_blocking_async::<T>(capacity);
    (SyncSender { tx }, AsyncReceiver { rx })
}

impl<T: Send + Unpin + 'static> AsyncSender<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "AsyncSender"))]
    pub async fn send(&self, item: T) -> Result<(), ChannelError> {
        self.tx.send(item).await.map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "AsyncSender"))]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TrySendError::Full(v),
            crossfire::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
    }
}

impl<T: Send + Unpin + 'static> Clone for AsyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: Send + Unpin + 'static> AsyncReceiver<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "AsyncReceiver"))]
    pub async fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().await.map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "AsyncReceiver"))]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }
}

impl<T: Send + Unpin + 'static> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

// ── MPSC (single-consumer) channel types ──
//
// Wraps `crossfire::mpsc` whose receiver is `!Clone + !Sync` (single-consumer
// enforced at the type level). The sender side (`MTx`) is identical to the
// MPMC sender — `Clone + Sync` — so multi-producer topologies are unaffected.
//
// The recv-side ring buffer uses `store` instead of `lock cmpxchg` (single
// consumer → no contention to CAS against), and the waker registry is a
// lock-free `WeakCell` instead of `Mutex<VecDeque>`. Profiling showed the MPMC
// ring-buffer CAS dominates per-item cost; switching the collector channel
// (always single-consumer) to MPSC eliminates that CAS on every collected item.

/// Multi-producer, single-consumer blocking sender. Send semantics as
/// [`SyncSender`]; see [`mpsc_channel`] for why the MPSC backing is lighter.
pub struct MpscSender<T: Send + 'static> {
    tx: crossfire::MTx<mpsc::Array<T>>,
}

/// Multi-producer, single-consumer blocking receiver. **Not `Clone`** — the
/// type system enforces a single consumer, enabling a CAS-free recv path.
pub struct MpscReceiver<T: Send + 'static> {
    rx: crossfire::Rx<mpsc::Array<T>>,
}

/// Create a bounded MPSC (multi-producer, single-consumer) channel.
///
/// Prefer this over [`channel`] when there is exactly one consumer — see the
/// MPSC section comment above for the profiled rationale.
#[must_use]
pub fn mpsc_channel<T: Send + 'static>(capacity: usize) -> (MpscSender<T>, MpscReceiver<T>) {
    let (tx, rx) = mpsc::bounded_blocking::<T>(capacity);
    (MpscSender { tx }, MpscReceiver { rx })
}

impl<T: Send + 'static> MpscSender<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscSender"))]
    pub fn send(&self, item: T) -> Result<(), ChannelError> {
        #[cfg(feature = "crossfire-trace")]
        trace_ch!(&**self.tx, "tx send");
        self.tx.send(item).map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscSender"))]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TrySendError::Full(v),
            crossfire::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
    }
}

impl<T: Send + 'static> SendItem<T> for MpscSender<T> {
    #[inline]
    fn send(&self, item: T) -> Result<(), ChannelError> {
        MpscSender::send(self, item)
    }

    #[inline]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        MpscSender::try_send(self, item)
    }
}

impl<T: Send + 'static> Clone for MpscSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: Send + 'static> MpscReceiver<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscReceiver"))]
    pub fn recv(&self) -> Result<T, ChannelError> {
        #[cfg(feature = "crossfire-trace")]
        trace_ch!(&*self.rx, "rx recv");
        self.rx.recv().map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscReceiver"))]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }

    /// Non-blocking batch claim of the ready run into `buf` (a typed Vec's
    /// `spare_capacity_mut()`); returns the count claimed. 0 means nothing
    /// is ready right now — NOT an Empty/Closed verdict (`try_recv`
    /// distinguishes). One `recv` cursor store per batch replaces the
    /// per-item store — the collector-side amortization this type exists
    /// for (todo #1 residual (d)).
    ///
    /// Probe caliber: one hotpath guard per *batch* (see
    /// [`SyncReceiver::try_recv_batch`]).
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscReceiver"))]
    pub fn try_recv_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        self.rx.try_recv_batch(buf)
    }

    /// MPSC counterpart of [`SyncReceiver::is_disconnected`] (review P-6).
    #[must_use]
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscReceiver"))]
    pub fn is_disconnected(&self) -> bool {
        self.rx.is_disconnected()
    }
}

impl<T: Send + 'static> RecvItem<T> for MpscReceiver<T> {
    #[inline]
    fn recv(&self) -> Result<T, ChannelError> {
        MpscReceiver::recv(self)
    }

    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MpscReceiver::try_recv(self)
    }

    #[inline]
    fn try_recv_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        MpscReceiver::try_recv_batch(self, buf)
    }

    #[inline]
    fn is_disconnected(&self) -> bool {
        MpscReceiver::is_disconnected(self)
    }
}

// ── MPSC mixed-mode (sync sender + async single-consumer receiver) ──

/// Async receiver for the MPSC mixed-mode channel. **Not `Clone`** — single
/// consumer, enabling the lighter MPSC ring buffer.
pub struct MpscAsyncReceiver<T: Send + Unpin + 'static> {
    rx: crossfire::AsyncRx<mpsc::Array<T>>,
}

/// Multi-producer, single-consumer async sender. Send semantics as
/// [`AsyncSender`]; pairs with [`MpscAsyncReceiver`] (see [`mpsc_async_channel`]).
pub struct MpscAsyncSender<T: Send + Unpin + 'static> {
    tx: crossfire::MAsyncTx<mpsc::Array<T>>,
}

impl<T: Send + Unpin + 'static> MpscAsyncSender<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscAsyncSender"))]
    pub async fn send(&self, item: T) -> Result<(), ChannelError> {
        self.tx.send(item).await.map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscAsyncSender"))]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TrySendError::Full(v),
            crossfire::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
    }
}

impl<T: Send + Unpin + 'static> Clone for MpscAsyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Create a bounded MPSC async channel: async sender + async single-consumer
/// receiver over the same queue.
///
/// Use this when async-stage consumer tasks feed the sole async collector —
/// the recv side avoids the per-item MPMC CAS (see the MPSC section comment).
#[must_use]
pub fn mpsc_async_channel<T: Send + Unpin + 'static>(
    capacity: usize,
) -> (MpscAsyncSender<T>, MpscAsyncReceiver<T>) {
    let (tx, rx) = mpsc::bounded_async::<T>(capacity);
    (MpscAsyncSender { tx }, MpscAsyncReceiver { rx })
}

impl<T: Send + Unpin + 'static> MpscAsyncReceiver<T> {
    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscAsyncReceiver"))]
    pub async fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().await.map_err(|_| ChannelError::Closed)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(impl_type = "MpscAsyncReceiver"))]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
    }
}

/// Outcome of one non-blocking claim attempt ([`claim_poll`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    /// `n` items now sitting in `scratch[..n]`.
    Ready(usize),
    /// Nothing ready right now (the caller decides: spin or park).
    Empty,
    /// Channel disconnected — no further claim can succeed.
    Closed,
}

/// One non-blocking claim attempt: the whole ready run at once when
/// `batch_cap > 0`, otherwise exactly one `try_recv` (the historical
/// per-item burst rhythm). Scratch contract: empty on entry (call sites
/// drain it fully — `drain(..)` also on their abort paths), capacity for
/// at least one item; the batch claim writes at offset 0 via the spare
/// capacity.
///
/// Probe shape (review P-6): a 0-count batch on an open channel resolves to
/// Empty via [`RecvItem::is_disconnected`] — one load of the cold tx-count
/// line — instead of a second ring walk through `try_recv`. Multi-worker
/// empty-ring polling previously paid that walk twice, doubling exactly the
/// stamp/head cache-line bouncing the spin backoff exists to throttle
/// (measured on the 32-core bench host, 6 interleaved A/B rounds, 4/8/16
/// pollers over one ring, with and without a 2 us/item supplier:
/// 1.24-1.52x more empty polls/s, per-cell sample ranges non-overlapping).
/// Only
/// the terminal closed case still falls through to `try_recv`, whose
/// `try_recv_final` drain keeps the half-close contract: Closed is reported
/// only after the drain came back empty, and stragglers pushed before the
/// last sender dropped are still delivered.
pub(crate) fn claim_poll<R, T>(rx: &R, scratch: &mut Vec<T>, batch_cap: usize) -> Claim
where
    R: RecvItem<T>,
{
    // Contract: scratch must be empty on entry (call sites drain it fully —
    // `drain(..)` also on their abort paths) and have capacity for at least
    // one item; the batch claim writes at offset 0 via the spare capacity.
    debug_assert!(scratch.is_empty());
    if batch_cap > 0 {
        let n = rx.try_recv_batch(scratch.spare_capacity_mut());
        if n > 0 {
            // SAFETY: try_recv_batch initialized exactly scratch[..n].
            unsafe { scratch.set_len(n) };
            return Claim::Ready(n);
        }
        // 0-claim verdict without a second ring walk. On an open channel a
        // 0-batch can only mean "nothing ready right now": the sole false 0
        // is a lost head CAS under consumer contention, which degrades to
        // Empty here — the caller's spin/blocking retry re-probes, and
        // crossfire's blocking recv pops a ready item without parking, so
        // the item is not delayed beyond that one retry.
        if !rx.is_disconnected() {
            return Claim::Empty;
        }
    }
    // Batch disabled, or the senders are gone: `try_recv` resolves the
    // remaining tri-state. On the closed arm it drains stragglers before
    // ever reporting Disconnected (the half-close contract confirmed in
    // review round 3 §5 — do not bypass it for the probe saving).
    match rx.try_recv() {
        Ok(item) => {
            scratch.clear();
            scratch.push(item);
            Claim::Ready(1)
        },
        Err(TryRecvError::Empty) => Claim::Empty,
        Err(TryRecvError::Closed) => Claim::Closed,
    }
}

/// Channel is closed (all senders/receivers dropped).
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelError {
    Closed,
}

// ── SendItem trait: abstracts over SyncSender and MpscSender ──

/// A sender that can deliver one item synchronously (blocking until space is
/// available in a bounded channel). Implemented by both [`SyncSender`] (MPMC
/// backing) and [`MpscSender`] (MPSC backing) so that [`spawn_stage`] etc. can
/// be generic over either — letting the collector channel use the lighter MPSC
/// ring buffer (store-based recv, no mutex waker registry) when there is only
/// one consumer.
pub trait SendItem<T>: Clone + Send + 'static {
    /// Deliver `item`, blocking until the channel has space. Returns
    /// `ChannelError::Closed` if all receivers have been dropped.
    fn send(&self, item: T) -> Result<(), ChannelError>;

    /// Deliver `item` without blocking. Returns `Err(TrySendError::Full(item))`
    /// when the bounded ring is full (the item is handed back) or
    /// `Err(TrySendError::Closed(item))` when every receiver is gone. Used by
    /// batch senders that park at most once per batch instead of per item.
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>>;
}

impl<T: Send + 'static> SendItem<T> for SyncSender<T> {
    #[inline]
    fn send(&self, item: T) -> Result<(), ChannelError> {
        SyncSender::send(self, item)
    }

    #[inline]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        SyncSender::try_send(self, item)
    }
}

/// A sync receiver that can `recv` (blocking) and `try_recv` (non-blocking).
/// Implemented by both [`SyncReceiver`] (MPMC) and [`MpscReceiver`] (MPSC) so
/// collector functions can be generic over either.
pub trait RecvItem<T> {
    fn recv(&self) -> Result<T, ChannelError>;
    fn try_recv(&self) -> Result<T, TryRecvError>;
    /// Non-blocking batch claim of the ready prefix into `buf`; returns the
    /// count. 0 = nothing ready right now — NOT an Empty/Closed verdict
    /// (call [`Self::try_recv`] to distinguish). Default: per-item fallback
    /// for backings without a native batch op.
    fn try_recv_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        let mut n = 0;
        while n < buf.len() {
            match self.try_recv() {
                Ok(v) => {
                    buf[n].write(v);
                    n += 1;
                },
                Err(_) => break,
            }
        }
        n
    }

    /// Whether every sender has been dropped — one load of the tx-count
    /// line, no ring walk (see [`SyncReceiver::is_disconnected`]). Lets a
    /// 0-count batch claim resolve to Empty/Closed without a second probe
    /// through `try_recv` (review P-6).
    fn is_disconnected(&self) -> bool;
}

impl<T: Send + 'static> RecvItem<T> for SyncReceiver<T> {
    #[inline]
    fn recv(&self) -> Result<T, ChannelError> {
        SyncReceiver::recv(self)
    }

    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        SyncReceiver::try_recv(self)
    }

    #[inline]
    fn try_recv_batch(&self, buf: &mut [MaybeUninit<T>]) -> usize {
        SyncReceiver::try_recv_batch(self, buf)
    }

    #[inline]
    fn is_disconnected(&self) -> bool {
        SyncReceiver::is_disconnected(self)
    }
}

/// `try_recv`-only subset shared by the sync and async MPSC receivers, so
/// the sharded terminals' round-robin burst pass (`ShardSet::drain_pass`)
/// is written once for both flavours (the full `RecvItem` cannot cover the
/// async side: its `recv` returns a future, not a blocking call).
pub trait TryRecvItem<T> {
    fn try_recv(&self) -> Result<T, TryRecvError>;
}

impl<T: Send + 'static> TryRecvItem<T> for MpscReceiver<T> {
    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MpscReceiver::try_recv(self)
    }
}

impl<T: Send + Unpin + 'static> TryRecvItem<T> for MpscAsyncReceiver<T> {
    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MpscAsyncReceiver::try_recv(self)
    }
}

/// Async counterpart to [`RecvItem`]: `recv().await` and `try_recv`. Both
/// [`AsyncReceiver`] (MPMC) and [`MpscAsyncReceiver`] (MPSC) implement this
/// so the async collector can drain either backing with one implementation,
/// avoiding code duplication between the MPMC and MPSC terminal paths.
pub trait AsyncRecvItem<T> {
    /// Wait for one item. Resolves to `Err(ChannelError::Closed)` once every
    /// sender has been dropped.
    fn recv(&self) -> impl Future<Output = Result<T, ChannelError>>;
    fn try_recv(&self) -> Result<T, TryRecvError>;
}

impl<T: Send + Unpin + 'static> AsyncRecvItem<T> for AsyncReceiver<T> {
    #[inline]
    async fn recv(&self) -> Result<T, ChannelError> {
        AsyncReceiver::recv(self).await
    }

    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        AsyncReceiver::try_recv(self)
    }
}

impl<T: Send + Unpin + 'static> AsyncRecvItem<T> for MpscAsyncReceiver<T> {
    #[inline]
    async fn recv(&self) -> Result<T, ChannelError> {
        MpscAsyncReceiver::recv(self).await
    }

    #[inline]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MpscAsyncReceiver::try_recv(self)
    }
}

/// Non-blocking send error.
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    Full(T),
    Closed(T),
}

/// Non-blocking receive error.
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_basic() {
        let (tx, rx) = channel::<i32>(4);
        tx.send(42).unwrap();
        tx.send(7).unwrap();
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(rx.recv().unwrap(), 7);
    }

    #[test]
    fn test_channel_bounded() {
        let (tx, rx) = channel::<i32>(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert!(matches!(tx.try_send(3), Err(TrySendError::Full(3))));
        rx.recv().unwrap();
        tx.try_send(3).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_channel_close_on_drop() {
        let (tx, rx) = channel::<i32>(4);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
        assert!(matches!(rx.recv(), Err(ChannelError::Closed)));
    }

    #[test]
    fn test_channel_mpmc() {
        let (tx, rx) = channel::<i32>(16);
        let tx2 = tx.clone();
        let rx2 = rx.clone();
        tx.send(1).unwrap();
        tx2.send(2).unwrap();
        tx.send(3).unwrap();
        tx2.send(4).unwrap();
        let mut all = vec![
            rx.recv().unwrap(),
            rx.recv().unwrap(),
            rx2.recv().unwrap(),
            rx2.recv().unwrap(),
        ];
        all.sort_unstable();
        assert_eq!(all, vec![1, 2, 3, 4]);
    }

    /// Probe-counting `RecvItem` mock: pins the P-6 probe shape (a 0-count
    /// batch on an open channel must not fall through to `try_recv`) and the
    /// closed-path confirmation semantics.
    struct ProbeMock {
        disconnected: bool,
        batch_calls: std::cell::Cell<usize>,
        recv_calls: std::cell::Cell<usize>,
    }

    impl ProbeMock {
        fn open() -> Self {
            Self {
                disconnected: false,
                batch_calls: std::cell::Cell::new(0),
                recv_calls: std::cell::Cell::new(0),
            }
        }
    }

    impl RecvItem<i32> for ProbeMock {
        fn recv(&self) -> Result<i32, ChannelError> {
            unreachable!("claim_poll never blocks")
        }
        fn try_recv(&self) -> Result<i32, TryRecvError> {
            self.recv_calls.set(self.recv_calls.get() + 1);
            if self.disconnected {
                Err(TryRecvError::Closed)
            } else {
                Err(TryRecvError::Empty)
            }
        }
        fn try_recv_batch(&self, _buf: &mut [MaybeUninit<i32>]) -> usize {
            self.batch_calls.set(self.batch_calls.get() + 1);
            0
        }
        fn is_disconnected(&self) -> bool {
            self.disconnected
        }
    }

    #[test]
    fn claim_poll_zero_batch_open_channel_is_single_probe() {
        let rx = ProbeMock::open();
        let mut scratch: Vec<i32> = Vec::new();
        assert_eq!(claim_poll(&rx, &mut scratch, 64), Claim::Empty);
        assert_eq!(rx.batch_calls.get(), 1);
        assert_eq!(
            rx.recv_calls.get(),
            0,
            "0-batch on an open channel must not re-walk the ring (P-6)"
        );
    }

    #[test]
    fn claim_poll_zero_batch_closed_channel_confirms_via_try_recv() {
        let rx = ProbeMock {
            disconnected: true,
            ..ProbeMock::open()
        };
        let mut scratch: Vec<i32> = Vec::new();
        assert_eq!(claim_poll(&rx, &mut scratch, 64), Claim::Closed);
        assert_eq!(
            rx.recv_calls.get(),
            1,
            "Closed verdicts must flow through try_recv's final drain"
        );
    }

    #[test]
    fn claim_poll_batch_cap_zero_uses_try_recv_only() {
        let rx = ProbeMock::open();
        let mut scratch: Vec<i32> = Vec::new();
        assert_eq!(claim_poll(&rx, &mut scratch, 0), Claim::Empty);
        assert_eq!(rx.batch_calls.get(), 0);
        assert_eq!(rx.recv_calls.get(), 1);
    }

    /// Real channel, closed with stragglers in the ring: the batch claims
    /// them (or the try_recv drain does), then the next poll reports Closed
    /// — no item lost to the disconnect, no premature Closed.
    #[test]
    fn claim_poll_closed_drain_delivers_stragglers() {
        let (tx, rx) = channel::<i32>(8);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        let mut scratch = Vec::with_capacity(8);
        match claim_poll(&rx, &mut scratch, 8) {
            Claim::Ready(n) => {
                assert!((1..=2).contains(&n));
                let mut got = std::mem::take(&mut scratch);
                got.sort_unstable();
                assert_eq!(got, vec![1, 2][..n]);
            },
            other => panic!("expected Ready, got {other:?}"),
        }
        assert_eq!(claim_poll(&rx, &mut scratch, 8), Claim::Closed);
    }

    /// MPSC try_send mirrors the MPMC semantics: Full hands the item back,
    /// Closed after the receiver is dropped and the ring drained.
    #[test]
    fn test_mpsc_try_send() {
        let (tx, rx) = mpsc_channel::<i32>(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert!(matches!(tx.try_send(3), Err(TrySendError::Full(3))));
        assert_eq!(rx.try_recv().unwrap(), 1);
        tx.try_send(3).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(rx.recv().unwrap(), 3);
        drop(rx);
        assert!(matches!(tx.try_send(4), Err(TrySendError::Closed(4))));
    }
}
