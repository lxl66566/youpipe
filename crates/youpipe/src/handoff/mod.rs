pub mod channel;
// Crate-internal: the bridge forward loops (recv -> cancel-check -> send)
// shared by both engines' cross-mode bridges.
pub(crate) mod forward;
// Crate-internal: the M:N async consumer driver shared by the stream
// engine's async stages and the graph engine's async nodes.
pub(crate) mod mux;
// Crate-internal: every receiver method here is `pub(crate)`, so a public
// module would only expose vestigial constructors (review R-5: an external
// `sharded_mpsc_channel(0, ..)` constructed fine, then panicked at `% 0` on
// the first drain pass).
pub(crate) mod sharded;
// Crate-internal: the shared worker loop and delivery strategies serve the
// stream and graph engines' spawn cores.
pub(crate) mod worker;

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

// `YOUPIPE_CHAIN_FLOOR` lifts the unpinned stage-output channel
// capacities (the feeder ring included) of two chain families — any chain
// containing an async stage, and any chain containing a `Chunked`-mode
// fence: each such channel floors at `min(n, CHAIN_FLOOR_SLOTS)` slots,
// byte-clamped per ring.
//
// Async-tailed chains (todo #4, "Async face re-attribution" in
// docs/src/dev/streaming-forensics.md): the surviving async2 slow mode is
// a backpressure-driven per-item wake chain; a capacity sweep showed every
// ring lifted to ~16K slots removes it entirely (terminal-only decoupling
// to the full in-flight bound `n` does NOT — the mid/mixed rings re-seed
// the chain — and `buffer = n` measures slightly worse than 16K, paying
// the bigger eager allocation's cache footprint).
//
// Chunked-fence chains: the fence convoy (per-item park+wake trickle at
// every serial-supplier→crowd interface; "Convoy collapse forensics") —
// the same capacity slack gives the feeder a `min(n, slots)` head start
// and the forwarder's chunk releases a ring deep enough to never park,
// so consumer bursts re-form (soak @100K: 67 % collapsed runs → ~1 %,
// mean 99.6 → 17-23 ms; @200K 87 % → 10-20 %; the rescue is
// coverage-graded and never fully zero — `YOUPIPE_SPIN_ANCHOR` remains
// the complete opt-in fix). `Barrier`-mode fences are excluded: nothing
// overlaps a barrier, so deep rings only add eager allocation (mean +28 %
// on the barrier probe). Pure-sync chains without a fence do not lift:
// sync2 shows no slow mode at the small default. Explicit per-stage
// buffer pins win verbatim (the caller opted into that exact
// backpressure). Runtime knob so A/B benches compare the same binary.
//
// Default ON (promoted with the A/Bs above: async2 soak 0/120 collapsed
// runs vs 18-30/120 off, async0_cheap/100K −41.8 %, every async shape
// 9/9 dominant, all sync families inert noise; fence soak per the
// paragraph above). "0" recovers the unfloored sizing; a decimal slot
// count overrides the tier ("16384" reproduces the 2026-10-22..25 async
// default — kept for the zero-CPU latency corner where the 1 MiB/ring L3
// recycling costs ~2 % fast-mode median).

/// Slot floor for the lifted channels. 2026-10-22 landed at 16K (the
/// smallest sweep value with zero collapsed runs, 4 proc x 12 runs per
/// cell; 64K also read clean but kept a bigger eager allocation); promoted
/// to the 64K tier 2026-10-25 by a tier A/B (todo #4 residual): async2
/// deep-soak slow runs 10/2160 -> 0/2160 and mean -6%, async0_cheap/100K
/// -30% stable (the async-only chain's single mixed ring stays
/// backpressure-bound at 16K), every other async family noise, pure-sync
/// chains inert. The one cost: +2% fast-mode median on the zero-CPU
/// forensic probe only (1 MiB/ring recycles through L3 where 256 KiB
/// stayed inside one core's L2) — invisible on every cpu-bearing shape.
/// The fence tier shares this constant: @100K 64K beat `buffer = n` on the
/// fast mode (p10 5.2 vs 12.6 ms, same eager-alloc mechanism as the async
/// sweep); higher tiers only trade small-n quality for a residual that
/// never reaches zero (n=200K: 64K 20 % collapsed vs n 10 %).
pub(crate) const CHAIN_FLOOR_SLOTS: usize = 65536;

/// Byte budget for the lift: bounded rings allocate every slot up front,
/// so one lifted ring costs `cap x size_of::<SLOT>()` bytes. Beyond the
/// budget the floored `buffer_size` stays — large payloads amortize the
/// wake-RTT pathology anyway (per-item work dwarfs the ~1-2 µs RTT).
pub(crate) const CHAIN_FLOOR_BYTES: usize = 8 << 20;

static CHAIN_FLOOR: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();

/// Resolved floor tier: `None` = the knob is off (unfloored sizing),
/// `Some(slots)` = lift to `min(n, slots)` (byte-clamped per ring).
/// Values: unset/`"1"` = on at the `CHAIN_FLOOR_SLOTS` tier, `"0"` =
/// off, a decimal `>= 2` = on at that explicit slot count (the tier A/B
/// caliber; `1` stays boolean so the bool reading cannot silently become a
/// one-slot floor).
pub(crate) fn chain_floor_slots() -> Option<usize> {
    *CHAIN_FLOOR.get_or_init(|| match std::env::var("YOUPIPE_CHAIN_FLOOR") {
        Ok(v) if v == "1" || v.is_empty() => Some(CHAIN_FLOOR_SLOTS),
        Ok(v) if v == "0" => None,
        Ok(other) => match other.parse::<usize>() {
            Ok(n) if n >= 2 => Some(n),
            _ => panic!(
                "YOUPIPE_CHAIN_FLOOR: invalid value {other:?} (leave unset or use \"0\" \"1\", or \
                 an explicit slot count >= 2)"
            ),
        },
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => Some(CHAIN_FLOOR_SLOTS),
    })
}

// ── Convoy fix knobs (todo #4; attribution in docs/src/dev/streaming.md
// "Convoy collapse forensics") ──
//
// `YOUPIPE_SPIN_ANCHOR=<µs>` (unset/"0" = off): adaptive pre-anchor spin in
// the sync worker recv loops (`spawn_stage` / `spawn_expand_stage` /
// `forward_fenced`) — see `AnchorSpin`. `YOUPIPE_FWD_BATCH=0` (default
// on): the fence forwarder pushes each released batch with `try_send_batch`,
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

// `YOUPIPE_FWD_BATCH`: the fence forwarder's released batches go out through
// `SendItem::send_batch` (one `try_send_batch` per free run + per-item
// blocking fallback) instead of one blocking `send` per item. Default ON;
// "0" recovers the per-item loop. The 2026-09 standalone A/B read "no
// independent rescue" because the default ring (256) sat full in steady
// state — a full ring hands `try_send_batch` a zero-length free prefix every
// time, so the batch arm never engaged. With the chain capacity floor
// (default-on 2026-11) the fenced ring holds slack, the batch claims whole
// chunks under one CAS, and `fire_n` merges the consumer wakes the per-item
// sends paid one registry lock each; the convoy tail collapses with it
// (readings in docs/src/dev/streaming-forensics.md "Forwarder batch send
// promoted to default").
static FWD_BATCH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn fwd_batch_enabled() -> bool {
    *FWD_BATCH.get_or_init(|| match std::env::var("YOUPIPE_FWD_BATCH") {
        Ok(v) if v == "1" || v.is_empty() => true,
        Ok(v) if v == "0" => false,
        Ok(other) => {
            panic!("YOUPIPE_FWD_BATCH: invalid value {other:?} (leave unset or use \"0\"/\"1\")")
        },
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => true,
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

// `YOUPIPE_ASYNC_TX_BATCH` refines `YOUPIPE_STREAM_TX_BATCH`'s async-tail
// gate (todo #34 re-measure): the old gate kept per-item sends on EVERY
// hop of a chain containing an async stage, falsified against the per-slot
// consumer tasks (+2.2…+4.9 % on both hops). The M:N drivers (2026-11-06)
// changed the consumer shape, and the todo #34 four-side A/B split the old
// verdict by hop: the hop INTO the async stage (the mixed ring, consumers
// = burst-refilling drivers) now WINS batched — but only with the
// backlog-scaled claim window armed (`YOUPIPE_CLAIM_SCALE`): with the
// fixed share-2 claims the 2-item staged sends LOSE on the split shape
// (async_pair_cpu_split/1K +6.4 % 6/25) while the window's grown claims
// amortize the staging (split/1K +0.5 % noise, merged/1K −33 % 25/25 with
// both). The mid hops of async chains lose at either claim shape
// (async_pair_cpu_split/100K +17.4 % 0/25 fully lifted), so the default
// (`MixedOnly`) batches exactly the mixed hop; "1" re-lifts the gate on
// every hop (falsified), "0" restores the historical per-item gate on
// every async-chain hop (also the fallback for `YOUPIPE_CLAIM_SCALE=0`).
// Independent of `YOUPIPE_STREAM_TX_BATCH` (the global switch).
#[derive(Clone, Copy, Debug)]
pub(crate) enum AsyncTxGate {
    /// Every hop of an async-tailed chain keeps per-item sends (the
    /// 2026-10 gate; the pre-todo-#34 default).
    Off,
    /// Batched delivery only on hops whose consumers are async (the mixed
    /// ring); sync→sync hops of async chains keep per-item sends.
    MixedOnly,
    /// Batched delivery on every hop (measured +17.4 % on
    /// `sync_fuse/async_pair_cpu_split/100K`, 0/25 — kept for A/B
    /// reproduction).
    Full,
}

static ASYNC_TX_BATCH: std::sync::OnceLock<AsyncTxGate> = std::sync::OnceLock::new();

pub(crate) fn async_tx_gate() -> AsyncTxGate {
    *ASYNC_TX_BATCH.get_or_init(|| match std::env::var("YOUPIPE_ASYNC_TX_BATCH") {
        Ok(v) if v == "1" => AsyncTxGate::Full,
        Ok(v) if v == "0" => AsyncTxGate::Off,
        Ok(v) if v.is_empty() => AsyncTxGate::MixedOnly,
        Ok(other) => panic!(
            "YOUPIPE_ASYNC_TX_BATCH: invalid value {other:?} (leave unset or use \"0\"/\"1\")"
        ),
        // Unset = the mixed-hop-only default (see the knob comment above).
        Err(_) => AsyncTxGate::MixedOnly,
    })
}

// `YOUPIPE_ASYNC_OUTLET_BATCH` batches the ASYNC OUTLET hop — an async
// producer task (the M:N mux drivers of `handoff::mux`, and the graph
// engine's async broadcast/unicast deliveries) handing completions to its
// downstream — the async twin of the sync engines' tx-batch lever
// (`SendItem::send_batch`). A DISTINCT HOP from `YOUPIPE_ASYNC_TX_BATCH`
// (that one gates the sync worker -> async consumer send on async-tailed
// stream chains; this one gates the async producer -> downstream send of
// both engines). When on, a driver that has clustered completions (one
// wake commonly resolves several in-flight futures; a cheap future
// resolves on its first inline poll) drains them into one batch and
// delivers it per edge with the `try_send_batch` prefix + per-item awaited
// fallback rhythm (`AsyncSendItem::flush_staged`) instead of one awaited
// `send_cached` per item — one ring CAS per free run and one merged
// consumer-wake pass per batch per edge. Broadcast keeps its k-1 clones
// per item (one clone per open edge is semantically required); what
// amortizes is the ring/waker traffic per edge. Default ON (promoted
// 2026-11-06 by the same-binary knob A/B: `fanout_async_*_graph` @100K
// -15.5..-16.0 % and @1K -4.0..-4.4 %, all 25/25; the unicast fan-in twin
// and the async-terminal `async1_*` @100K improved too (-3.8..-7.7 %,
// 25/25), while the parky `io_async` rows and `mixed_load` stayed dead
// flat — the driver's flush-before-park keeps staging from delaying first
// delivery; readings in docs benchmarks/graph.md). Values: unset = on with
// the default batch cap; "0" = the per-item deliveries; "1" = on with the
// default cap; "N" (>= 2) = on with batch cap N (the driver's in-flight
// bound caps the batch anyway). Runtime knob so A/B benches compare the
// same binary.
static ASYNC_OUTLET_BATCH: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Batch cap for the async outlet delivery; 0 = per-item deliveries.
pub(crate) fn async_outlet_batch_cap() -> usize {
    *ASYNC_OUTLET_BATCH.get_or_init(|| match std::env::var("YOUPIPE_ASYNC_OUTLET_BATCH") {
        Ok(v) if v == "1" || v.is_empty() => DEFAULT_BATCH_RECV_CAP,
        Ok(v) if v == "0" => 0,
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n >= 2 => n,
            _ => panic!(
                "YOUPIPE_ASYNC_OUTLET_BATCH: invalid value {v:?} (leave unset, or use \
                 \"0\"/\"1\"/a batch cap >= 2)"
            ),
        },
        // Unset = the default-on policy (see the knob comment above).
        Err(_) => DEFAULT_BATCH_RECV_CAP,
    })
}

// `YOUPIPE_CLAIM_SCALE` (todo #34): backlog-scaled claim shares for the
// stream engine's multi-consumer stage/expand workers. The fixed share
// (`per_worker_claim_cap` = cap / workers, 2 items at 31 workers) costs
// one ring-head CAS per ~2 items: on async-fed chains with trivial
// per-item work the sync stage's whole span is that CAS rhythm (perf:
// 31 % of samples in `SyncSender::send`'s tail-CAS backoff, then 29 % in
// `claim_poll` after lifting the tx-batch gate). The window starts at the
// share and doubles while claims keep coming back full (the ready backlog
// covered the window — bigger claims amortize the head CAS), collapsing
// to the share on the first partial claim (the backlog was shallower, the
// remainder belongs to the parked peers — the fairness contract
// `per_worker_claim_cap` exists for). Refresh-based on purpose: the
// fork's `len()` reads the producer-side tail line (SeqCst loop) on every
// claim, re-sharing the hottest cache line exactly when claiming is
// already contended; the claim's own result is a free backlog probe.
// Default ON (async-tailed chains only): with the mixed-hop tx gate it
// stacks (`sync_fuse/async_pair_cpu_merged/1K` −33 % 25/25,
// `sharded_term_async/async1_cpu/100K` −15 % 25/25, every other gated row
// noise-or-better, the pure-sync controls structurally untouched);
// alone it still wins every gated row (merged/1K −16 % 25/25) — the
// combination is the measured default pair. Scope: stream engine only
// (the graph `FanIn` round keeps the fixed share; no graph gate was
// measured), and ASYNC-TAILED CHAINS only: on
// pure-sync chains the window's coarser claim rhythm regressed the fence
// family hard (`stream_pipeline/with_fence/100K` +60 % 0/25 — the chunk
// release/worker park rhythm coarsens with 64-item claims; @1K it read
// −15.8 %, size-contradicted) while a pure-sync chain's stages drain the
// ring as fast as they claim, so there is no backlog to amortize — the
// same chains the capacity floor excludes (`ctx.has_async_stage`, the
// shared predicate). Default set by the todo #34 A/B; "1" = adaptive
// window on async-tailed chains, "0" = the fixed share everywhere.
static CLAIM_SCALE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn claim_scale_enabled() -> bool {
    *CLAIM_SCALE.get_or_init(|| match std::env::var("YOUPIPE_CLAIM_SCALE") {
        Ok(v) if v == "1" => true,
        Ok(v) if v == "0" || v.is_empty() => false,
        Ok(other) => {
            panic!("YOUPIPE_CLAIM_SCALE: invalid value {other:?} (leave unset or use \"0\"/\"1\")")
        },
        // Unset = the default-on policy, async-tailed chains only (the
        // scope predicate lives at the stream spawn sites).
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
pub(crate) use forward::forward_async;
// `forward_async_to_sync` is re-exported ungated: the graph engine's bridge
// compiles (dead) without the tokio backend too.
pub(crate) use forward::{forward_async_to_sync, forward_sync};
#[cfg(feature = "tokio-runtime")]
pub(crate) use sharded::{ShardedAsyncReceiver, sharded_mpsc_async_channel};
pub(crate) use sharded::{ShardedReceiver, sharded_mpsc_channel};
pub(crate) use worker::{
    Broadcast, CancelCheck, ClaimWindow, Delivery, SpinBudget, SpinPoll, Unicast, cancel_active,
    claim_expand_deliver, claim_map_deliver, per_worker_claim_cap,
};
