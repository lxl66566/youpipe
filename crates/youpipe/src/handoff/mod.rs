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
// layout noise swings tight benches by tens of percent.
//
// Default ON (cap = `DEFAULT_BATCH_RECV_CAP`), promoted 2026-10-20 with
// `YOUPIPE_SHARDED_TERM`: batched claiming is what removes the sharded
// terminal's bimodal slow mode (the pair measured tight tails and mean
// wins on every soak cell; batch ALONE on a shared terminal ring is
// bimodal — see docs benchmarks.md "Default-policy soak"). Set "0" to
// recover the per-item loops.
//
// Values: unset = on with the default batch cap; "0" = off; "1" = on with
// the default cap; "N" (>=2) = on with cap N, clamped to 4096 (the cap
// bounds both the claim size and the per-consumer scratch buffer).

/// Default batch cap: comfortably above the observed terminal burst sizes
/// (p50 burst ~3, ordered ~24) while keeping the scratch buffer in one or
/// two cache pages.
pub(crate) const DEFAULT_BATCH_RECV_CAP: usize = 64;

static BATCH_RECV: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Batch cap for the streaming data plane; 0 = batching off.
pub(crate) fn batch_recv_cap() -> usize {
    *BATCH_RECV.get_or_init(|| match std::env::var("YOUPIPE_BATCH_RECV") {
        Ok(v) if v == "1" || v.is_empty() => DEFAULT_BATCH_RECV_CAP,
        Ok(v) if v == "0" => 0,
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if (2..=4096).contains(&n) => n,
            _ => panic!(
                "YOUPIPE_BATCH_RECV: invalid value {v:?} (leave unset, or use \"0\"/\"1\"/a cap \
                 in 2..=4096)"
            ),
        },
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => DEFAULT_BATCH_RECV_CAP,
    })
}

// ── Sharded terminal channel (runtime knob, todo #1) ──
//
// `YOUPIPE_SHARDED_TERM=1` swaps a multi-worker sync/expand terminal
// stage's output channel from one shared MPSC ring to one SPSC ring per
// worker (see `sharded.rs` for the contention rationale). Runtime knob —
// not a config field — so A/B benchmarks compare the same binary
// (recompiles swing tight benchmarks ±30 % through pure code layout).
//
// Default ON (promoted 2026-10-20 together with `YOUPIPE_BATCH_RECV`):
// sharded alone is bimodal on single-stage fan-in shapes (median wins but
// the convoy slow mode returns in ~30 % of back-to-back runs); the pair
// with batched claiming is tight and wins every soak cell. "0" recovers
// the shared ring. See docs benchmarks.md "Default-policy soak".
static SHARDED_TERM: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn sharded_term_enabled() -> bool {
    *SHARDED_TERM.get_or_init(|| match std::env::var("YOUPIPE_SHARDED_TERM") {
        Ok(v) if v == "1" || v.is_empty() => true,
        Ok(v) if v == "0" => false,
        Ok(other) => {
            panic!("YOUPIPE_SHARDED_TERM: invalid value {other:?} (leave unset or use \"0\"/\"1\")")
        },
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => true,
    })
}

// `YOUPIPE_SHARDED_TERM_ASYNC=1` is the async-terminal flavour of the same
// channel swap (one async shard ring per task group). Deliberately a
// separate knob: the async flavour measured +42…+51 % on sync-fed async
// terminals (the one-shard anchor's waker asymmetry) and only wins the
// async-only feeder-saturated corner, so it must not ride along with the
// sync knob (benchmarks.md "Async sharded-terminal A/B"). Default off.
static SHARDED_TERM_ASYNC: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn sharded_term_async_enabled() -> bool {
    *SHARDED_TERM_ASYNC.get_or_init(|| match std::env::var("YOUPIPE_SHARDED_TERM_ASYNC") {
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(other) => panic!(
            "YOUPIPE_SHARDED_TERM_ASYNC: invalid value {other:?} (leave unset or use \"0\"/\"1\")"
        ),
        Err(_) => false,
    })
}

// `YOUPIPE_ASYNC_CHAIN_FLOOR` lifts the unpinned stage-output channel
// capacities (the feeder ring included) of async-tailed chains — any chain
// containing an async stage: each such channel floors at
// `min(n, ASYNC_CHAIN_FLOOR_SLOTS)` slots, byte-clamped per ring.
// Mechanism (todo #4, "Async face re-attribution" in docs/src/dev/
// streaming.md): the surviving async2 slow mode is a backpressure-driven
// per-item wake chain; a capacity sweep showed every ring lifted to ~16K
// slots removes it entirely (terminal-only decoupling to the full
// in-flight bound `n` does NOT — the mid/mixed rings re-seed the chain —
// and `buffer = n` measures slightly worse than 16K, paying the bigger
// eager allocation's cache footprint). Pure-sync chains do not lift: sync2
// shows no slow mode at the small default, so the floor binds only where
// the runtime-task wake-RTT pacing exists. Explicit per-stage buffer pins
// win verbatim (the caller opted into that exact backpressure). Runtime
// knob so A/B benches compare the same binary.
//
// Default ON (promoted with the A/B above: async2 soak 0/120 collapsed
// runs vs 18-30/120 off, async0_cheap/100K −41.8 %, every async shape
// 9/9 dominant, all sync families inert noise). "0" recovers the unfloored
// sizing.

/// Slot floor for the lifted channels: the smallest sweep value with zero
/// collapsed runs (async2 @100K, 4 processes x 12 runs per cell; 16K and
/// 64K both clean, 16K keeps the eager allocation at 256 KB/slot-ring for
/// u64 payloads).
pub(crate) const ASYNC_CHAIN_FLOOR_SLOTS: usize = 16384;

/// Byte budget for the lift: bounded rings allocate every slot up front,
/// so one lifted ring costs `cap x size_of::<SLOT>()` bytes. Beyond the
/// budget the floored `buffer_size` stays — large payloads amortize the
/// wake-RTT pathology anyway (per-item work dwarfs the ~1-2 µs RTT).
pub(crate) const ASYNC_CHAIN_FLOOR_BYTES: usize = 8 << 20;

static ASYNC_CHAIN_FLOOR: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn async_chain_floor_enabled() -> bool {
    *ASYNC_CHAIN_FLOOR.get_or_init(|| match std::env::var("YOUPIPE_ASYNC_CHAIN_FLOOR") {
        Ok(v) if v == "1" || v.is_empty() => true,
        Ok(v) if v == "0" => false,
        Ok(other) => panic!(
            "YOUPIPE_ASYNC_CHAIN_FLOOR: invalid value {other:?} (leave unset or use \"0\"/\"1\")"
        ),
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => true,
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

// `YOUPIPE_STREAM_TX_BATCH` batches the sync stage/expand workers' output
// delivery: the mapped claim batch is staged and flushed with
// `SendItem::send_batch` (one `try_send_batch` per free run + per-item
// blocking fallback) instead of one blocking `send` per item — the graph
// engine's tx-batch lever ported to the streaming data plane (it is what
// let the graph chain overtake the stream chain on the identical shape).
// Zero-cap claim caps (YOUPIPE_BATCH_RECV=0) keep the per-item send:
// staging single-item claims only adds moves. Runtime knob so A/B benches
// compare the same binary. Default ON; "0" recovers the per-item loops.
static STREAM_TX_BATCH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn stream_tx_batch() -> bool {
    *STREAM_TX_BATCH.get_or_init(|| match std::env::var("YOUPIPE_STREAM_TX_BATCH") {
        Ok(v) if v == "1" || v.is_empty() => true,
        Ok(v) if v == "0" => false,
        Ok(other) => panic!(
            "YOUPIPE_STREAM_TX_BATCH: invalid value {other:?} (leave unset or use \"0\"/\"1\")"
        ),
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => true,
    })
}

// Worker-side batch burst helpers (todo #1 residual (d)); crate-internal.
pub use channel::{
    AsyncReceiver, AsyncRecvItem, AsyncSendItem, AsyncSender, ChannelError, MpscAsyncReceiver,
    MpscAsyncSender, MpscReceiver, MpscSender, Receiver, RecvItem, SendItem, Sender, SyncReceiver,
    SyncSender, TryRecvError, TryRecvItem, TrySendError, WakerSlot, async_channel, channel,
    mpsc_async_channel, mpsc_channel, sync_async_channel,
};
pub(crate) use channel::{Claim, claim_poll, stage_batch};
#[cfg(feature = "tokio-runtime")]
pub(crate) use sharded::{ShardedAsyncReceiver, sharded_mpsc_async_channel};
pub(crate) use sharded::{ShardedReceiver, sharded_mpsc_channel};
