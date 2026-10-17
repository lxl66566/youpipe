pub mod channel;
// Crate-internal: every receiver method here is `pub(crate)`, so a public
// module would only expose vestigial constructors (review R-5: an external
// `sharded_mpsc_channel(0, ..)` constructed fine, then panicked at `% 0` on
// the first drain pass).
pub(crate) mod sharded;

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

// ── Sharded terminal channel (runtime knob, todo #1) ──
//
// `YOUPIPE_SHARDED_TERM=1` swaps the terminal stage's output channel from
// one shared MPSC ring to one SPSC ring per worker (see `sharded.rs` for
// the contention rationale). Runtime knob — not a config field — so A/B
// benchmarks compare the same binary (recompiles swing tight benchmarks
// ±30 % through pure code layout). Default off.
static SHARDED_TERM: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn sharded_term_enabled() -> bool {
    *SHARDED_TERM.get_or_init(|| match std::env::var("YOUPIPE_SHARDED_TERM") {
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(other) => {
            panic!("YOUPIPE_SHARDED_TERM: invalid value {other:?} (leave unset or use \"0\"/\"1\")")
        },
        Err(_) => false,
    })
}

// ── Convoy fix knobs (todo #4; attribution in docs/src/dev/streaming.md
// "Convoy collapse forensics") ──
//
// `YOUPIPE_SPIN_ANCHOR=<µs>` (unset/"0" = off): adaptive pre-anchor spin in
// the sync worker recv loops (`spawn_stage` / `spawn_expand_stage` /
// `forward_fenced`) — see `AnchorSpin`. `YOUPIPE_FWD_BATCH=1` (default
// off): the fence forwarder pushes each released batch with `try_send`,
// parking at most once per full ring instead of paying a blocking send per
// item. Runtime knobs rather than config fields so A/B benches compare the
// same binary (recompiles swing tight benchmarks by tens of percent through
// pure code layout).

static SPIN_ANCHOR_US: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Max pre-anchor spin window in µs; 0 = the anchor is a plain blocking
/// recv (knob off, every `AnchorSpin` inert).
pub(crate) fn spin_anchor_us() -> u32 {
    *SPIN_ANCHOR_US.get_or_init(|| match std::env::var("YOUPIPE_SPIN_ANCHOR") {
        Ok(v) if v == "0" || v.is_empty() => 0,
        Ok(other) => other.parse().unwrap_or_else(|_| {
            panic!(
                "YOUPIPE_SPIN_ANCHOR: invalid value {other:?} (leave unset or use \"0\" or a spin \
                 window in µs, e.g. \"10\")"
            )
        }),
        Err(_) => 0,
    })
}

static FWD_BATCH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn fwd_batch_enabled() -> bool {
    *FWD_BATCH.get_or_init(|| match std::env::var("YOUPIPE_FWD_BATCH") {
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(other) => {
            panic!("YOUPIPE_FWD_BATCH: invalid value {other:?} (leave unset or use \"0\"/\"1\")")
        },
        Err(_) => false,
    })
}

// Worker-side batch burst helpers (todo #1 residual (d)); crate-internal.
pub use channel::{
    AsyncReceiver, AsyncRecvItem, AsyncSendItem, AsyncSender, ChannelError, MpscAsyncReceiver,
    MpscAsyncSender, MpscReceiver, MpscSender, Receiver, RecvItem, SendItem, Sender, SyncReceiver,
    SyncSender, TryRecvError, TryRecvItem, TrySendError, WakerSlot, async_channel, channel,
    mpsc_async_channel, mpsc_channel, sync_async_channel,
};
pub(crate) use channel::{Claim, claim_poll};
#[cfg(feature = "tokio-runtime")]
pub(crate) use sharded::{ShardedAsyncReceiver, sharded_mpsc_async_channel};
pub(crate) use sharded::{ShardedReceiver, sharded_mpsc_channel};
