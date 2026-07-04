use crossfire::{mpmc, mpsc};

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
    pub fn send(&self, item: T) -> Result<(), ChannelError> {
        self.tx.send(item).map_err(|_| ChannelError::Closed)
    }

    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TrySendError::Full(v),
            crossfire::TrySendError::Disconnected(v) => TrySendError::Closed(v),
        })
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
    pub fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().map_err(|_| ChannelError::Closed)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
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
/// This is the right primitive for a sync→async bridge: the producer side can
/// call the naturally blocking [`SyncSender::send`] (letting crossfire's
/// internal waker park the producer thread until the async consumer drains an
/// item), instead of `try_send` + `yield_now` busy-spinning on `Full`. The
/// consumer side stays fully async (`AsyncReceiver::recv`). Both endpoints
/// share one `mpmc::Array`, so there is no extra hop relative to
/// [`async_channel`].
#[must_use]
pub fn sync_async_channel<T: Send + Unpin + 'static>(
    capacity: usize,
) -> (SyncSender<T>, AsyncReceiver<T>) {
    let (tx, rx) = mpmc::bounded_blocking_async::<T>(capacity);
    (SyncSender { tx }, AsyncReceiver { rx })
}

impl<T: Send + Unpin + 'static> AsyncSender<T> {
    pub async fn send(&self, item: T) -> Result<(), ChannelError> {
        self.tx.send(item).await.map_err(|_| ChannelError::Closed)
    }

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
    pub async fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().await.map_err(|_| ChannelError::Closed)
    }

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

/// Multi-producer, single-consumer blocking sender. Same send semantics as
/// [`SyncSender`] but paired with a [`MpscReceiver`] that uses a lighter
/// ring-buffer algorithm.
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
/// Prefer this over [`channel`] when there is exactly one consumer — the
/// receiver uses `store`-based dequeue (no `lock cmpxchg`) and a lock-free
/// waker registry, eliminating the dominant per-item CAS cost that the MPMC
/// ring buffer pays on every `recv`.
#[must_use]
pub fn mpsc_channel<T: Send + 'static>(capacity: usize) -> (MpscSender<T>, MpscReceiver<T>) {
    let (tx, rx) = mpsc::bounded_blocking::<T>(capacity);
    (MpscSender { tx }, MpscReceiver { rx })
}

impl<T: Send + 'static> MpscSender<T> {
    pub fn send(&self, item: T) -> Result<(), ChannelError> {
        self.tx.send(item).map_err(|_| ChannelError::Closed)
    }
}

impl<T: Send + 'static> SendItem<T> for MpscSender<T> {
    #[inline]
    fn send(&self, item: T) -> Result<(), ChannelError> {
        MpscSender::send(self, item)
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
    pub fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().map_err(|_| ChannelError::Closed)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
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
}

// ── MPSC mixed-mode (sync sender + async single-consumer receiver) ──

/// Async receiver for the MPSC mixed-mode channel. **Not `Clone`** — single
/// consumer, enabling the lighter MPSC ring buffer.
pub struct MpscAsyncReceiver<T: Send + Unpin + 'static> {
    rx: crossfire::AsyncRx<mpsc::Array<T>>,
}

/// Create a bounded MPSC mixed-mode channel: blocking sync sender paired with
/// an async single-consumer receiver over the same queue.
///
/// This is the MPSC analogue of [`sync_async_channel`]. Use it when the
/// collector runs as a single async task — the recv side avoids the MPMC
/// ring-buffer CAS.
#[must_use]
pub fn mpsc_sync_async_channel<T: Send + Unpin + 'static>(
    capacity: usize,
) -> (MpscSender<T>, MpscAsyncReceiver<T>) {
    let (tx, rx) = mpsc::bounded_blocking_async::<T>(capacity);
    (MpscSender { tx }, MpscAsyncReceiver { rx })
}

impl<T: Send + Unpin + 'static> MpscAsyncReceiver<T> {
    pub async fn recv(&self) -> Result<T, ChannelError> {
        self.rx.recv().await.map_err(|_| ChannelError::Closed)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.rx.try_recv().map_err(|e| match e {
            crossfire::TryRecvError::Empty => TryRecvError::Empty,
            crossfire::TryRecvError::Disconnected => TryRecvError::Closed,
        })
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
}

impl<T: Send + 'static> SendItem<T> for SyncSender<T> {
    #[inline]
    fn send(&self, item: T) -> Result<(), ChannelError> {
        SyncSender::send(self, item)
    }
}

/// A sync receiver that can `recv` (blocking) and `try_recv` (non-blocking).
/// Implemented by both [`SyncReceiver`] (MPMC) and [`MpscReceiver`] (MPSC) so
/// collector functions can be generic over either.
pub trait RecvItem<T> {
    fn recv(&self) -> Result<T, ChannelError>;
    fn try_recv(&self) -> Result<T, TryRecvError>;
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
}
