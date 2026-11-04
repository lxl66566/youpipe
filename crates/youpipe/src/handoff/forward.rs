//! The engine-agnostic bridge forward loops: recv → (cancel check) → send,
//! shared by the stream engine's cross-mode bridges (`bridge_async_to_sync`,
//! `bridge_final_rx_to_async`, the `StreamStart` `spawn_single` /
//! `spawn_for_async` / `spawn_async_feeder_single` adapters) and the graph
//! engine's `async_exec::bridge`.
//!
//! Cancel parity (the contract every copy carried by hand before): the token
//! is observed *after* a successful recv and *before* the send, so an item
//! sitting in the bridge's hand when the token fires is dropped, never
//! forwarded. The loop exits on upstream EOF (recv `Closed`), cancel, or
//! downstream close (send `Closed`); the channel endpoints drop with the
//! loop's frame, which is itself load-bearing — the bridge's sender drop is
//! the downstream's EOF signal.

use super::{
    channel::{AsyncReceiver, AsyncSendItem, SendItem, SyncReceiver, WakerSlot},
    worker::cancel_active,
};
use crate::sync::CancellationToken;

/// Blocking forward loop, both endpoints sync: the three OS-thread bodies
/// that copy one sync ring into another (`StreamStart::spawn_single`'s MPSC
/// bridge, `spawn_for_async`'s mixed-mode bridge, `bridge_final_rx_to_async`'s
/// sync arm). Generic over [`SendItem`] so the MPMC and MPSC backings plug
/// in unchanged. Runs on a dedicated OS thread (or a leased pool job) — both
/// channel ops park.
pub(crate) fn forward_sync<T: Send + 'static>(
    rx: &SyncReceiver<T>,
    tx: &impl SendItem<T>,
    cancel: Option<&CancellationToken>,
) {
    while let Ok(item) = rx.recv() {
        if cancel_active(cancel) {
            return;
        }
        if tx.send(item).is_err() {
            return; // downstream closed
        }
    }
}

/// Awaited forward loop, both endpoints async: the runtime-task twins
/// (`spawn_async_feeder_single`'s MPSC bridge, `bridge_final_rx_to_async`'s
/// async arm). One rx and one tx [`WakerSlot`] live for the whole task — a
/// parking loop otherwise pays one waker-node allocation per park episode.
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))] // async bridges are tokio-gated
pub(crate) async fn forward_async<T, Tx>(
    rx: AsyncReceiver<T>,
    tx: Tx,
    cancel: Option<CancellationToken>,
) where
    T: Send + Unpin + 'static,
    Tx: AsyncSendItem<T>,
{
    let mut rx_slot = WakerSlot::new();
    let mut tx_slot = WakerSlot::new();
    while let Ok(item) = rx.recv_cached(&mut rx_slot).await {
        if cancel_active(cancel.as_ref()) {
            return;
        }
        if tx.send_cached(item, &mut tx_slot).await.is_err() {
            return; // downstream closed
        }
    }
}

/// Cross-mode forward loop, async recv into a **blocking** sync send
/// (`bridge_async_to_sync`, the graph engine's `async_exec::bridge`). The
/// send parks the *driving* thread, so the caller MUST run this under
/// `block_on` on a dedicated OS thread — never a runtime task (a blocked
/// send inside an executor worker stalls the runtime). This is why the
/// flavour is a separate function and not an [`AsyncSendItem`] impl on
/// [`super::SyncSender`]: the type system must not let a blocking send look
/// awaitable to task-spawning call sites.
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))] // async bridges are tokio-gated
pub(crate) async fn forward_async_to_sync<T: Send + Unpin + 'static>(
    rx: AsyncReceiver<T>,
    tx: super::SyncSender<T>,
    cancel: Option<CancellationToken>,
) {
    let mut rx_slot = WakerSlot::new();
    while let Ok(item) = rx.recv_cached(&mut rx_slot).await {
        if cancel_active(cancel.as_ref()) {
            return;
        }
        if tx.send(item).is_err() {
            return; // downstream closed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel::{channel, mpsc_channel};

    /// The full contract over a real pair: items flow in order; cancel
    /// drops the in-hand item and stops the loop; the loop returns on
    /// upstream EOF and its tx drop closes the downstream.
    #[test]
    fn forward_sync_orders_and_honors_cancel() {
        // Uncancelled: EOF drain, order kept, downstream closed by return.
        let (in_tx, in_rx) = channel::<u32>(4);
        let (out_tx, out_rx) = mpsc_channel::<u32>(4);
        for v in 0..4 {
            in_tx.send(v).unwrap();
        }
        drop(in_tx);
        forward_sync(&in_rx, &out_tx, None);
        drop(out_tx);
        assert_eq!(
            (0..4).map(|_| out_rx.recv().unwrap()).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        // Cancelled with an item in hand: received but never forwarded.
        let (in_tx, in_rx) = channel::<u32>(4);
        let (out_tx, out_rx) = mpsc_channel::<u32>(4);
        let cancel = CancellationToken::new();
        cancel.cancel();
        in_tx.send(7).unwrap();
        drop(in_tx);
        forward_sync(&in_rx, &out_tx, Some(&cancel));
        drop(out_tx);
        assert!(out_rx.recv().is_err(), "cancelled item must be dropped");
    }
}
