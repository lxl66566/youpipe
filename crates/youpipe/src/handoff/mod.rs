pub mod channel;
pub mod sharded;

// ── Batched data-plane knob (todo #1 residual (d)) ──
//
// `YOUPIPE_BATCH_RECV` batches the streaming data plane's hottest cursor
// updates: the collector's terminal drain, the stage workers' burst phase
// and the feeder's push loop claim/send runs of consecutive ring slots
// with one atomic cursor update instead of one per item. Runtime knob (not
// a config field) so A/B benchmarks compare the same binary — recompile
// layout noise swings tight benches by tens of percent. Default off.
//
// Values: unset or "0" = off; "1" = on with the default batch cap;
// "N" (>=2) = on with cap N, clamped to 4096 (the cap bounds both the
// claim size and the per-consumer scratch buffer).

/// Default batch cap: comfortably above the observed terminal burst sizes
/// (p50 burst ~3, ordered ~24) while keeping the scratch buffer in one or
/// two cache pages.
pub(crate) const DEFAULT_BATCH_RECV_CAP: usize = 64;

static BATCH_RECV: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Batch cap for the streaming data plane; 0 = batching off.
pub(crate) fn batch_recv_cap() -> usize {
    *BATCH_RECV.get_or_init(|| match std::env::var("YOUPIPE_BATCH_RECV") {
        Ok(v) if v == "1" => DEFAULT_BATCH_RECV_CAP,
        Ok(v) if v == "0" || v.is_empty() => 0,
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if (2..=4096).contains(&n) => n,
            _ => panic!(
                "YOUPIPE_BATCH_RECV: invalid value {v:?} (leave unset, or use \"0\"/\"1\"/a cap \
                 in 2..=4096)"
            ),
        },
        Err(_) => 0,
    })
}

// Worker-side batch burst helper (todo #1 residual (d)); crate-internal.
pub(crate) use channel::claim_burst;
pub use channel::{
    AsyncReceiver, AsyncRecvItem, AsyncSender, ChannelError, MpscAsyncReceiver, MpscAsyncSender,
    MpscReceiver, MpscSender, Receiver, RecvItem, SendItem, Sender, SyncReceiver, SyncSender,
    TryRecvError, TrySendError, async_channel, channel, mpsc_async_channel, mpsc_channel,
    sync_async_channel,
};
pub use sharded::{ShardedReceiver, sharded_mpsc_channel};
