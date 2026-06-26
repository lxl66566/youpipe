use crossfire::mpmc;

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

/// Channel is closed (all senders/receivers dropped).
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelError {
    Closed,
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
