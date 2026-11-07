//! End-to-end pin of the per-pipeline data-plane policy fields (r4 audit):
//! `with_sharded_terminal` / `with_chain_floor` / `with_async_tx_gate`
//! thread from `StreamPipe` through `TermPolicy::resolve` /
//! `ClaimKnobs::resolve` into the spawned topology. This binary sets NO
//! `YOUPIPE_*` env var, so these runs exercise exactly the config tier
//! (env unset + config set; the tier order itself is unit-tested against
//! injected env values in the lib tests). Channel-shape knobs only —
//! output must be identical on every tier, which is what is asserted.
//!
//! `with_cancel` (an un-fired token) forces the streaming topology on pure
//! sync chains — otherwise they take the fused pass-through and never
//! build a terminal channel at all.

use youpipe::{stream, sync::CancellationToken};

/// An un-fired token: disables the fused pass-through, keeps cancellation
/// inert.
fn inert_cancel() -> CancellationToken {
    CancellationToken::new()
}

fn n() -> usize {
    if cfg!(miri) {
        500
    } else {
        20_000
    }
}

#[test]
fn sharded_terminal_config_tier_end_to_end() {
    let items: Vec<u64> = (0..n() as u64).collect();
    let expected: Vec<u64> = items.iter().map(|x| x * 3 + 1).collect();
    for on in [true, false] {
        let mut got = stream(items.clone())
            .with_cancel(inert_cancel())
            .with_sharded_terminal(on)
            .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
            .run();
        got.sort_unstable();
        assert_eq!(got, expected, "output is channel-shape independent ({on})");
    }
}

/// The shard-depth floor only changes the sync sharded terminal's ring
/// sizing (todo #48), never the output — every tier must complete
/// identically. Like the sharded-terminal field above this exercises the
/// config tier (this binary sets no `YOUPIPE_*` env var).
#[test]
fn shard_depth_config_tier_end_to_end() {
    use std::num::NonZeroUsize;

    use youpipe::ShardDepth;

    let items: Vec<u64> = (0..n() as u64).collect();
    let expected: Vec<u64> = items.iter().map(|x| x * 3 + 1).collect();
    for depth in [
        ShardDepth::ClaimCap,
        ShardDepth::Off,
        ShardDepth::Slots(NonZeroUsize::new(1024).unwrap()),
    ] {
        let mut got = stream(items.clone())
            .with_cancel(inert_cancel())
            .with_shard_depth(depth)
            .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
            .run();
        got.sort_unstable();
        assert_eq!(
            got, expected,
            "output is ring-sizing independent ({depth:?})"
        );
    }
}

/// The chain floor and the async tx gate only bind on async-tailed chains,
/// so this run pins both config fields on the shape that reads them.
#[cfg(feature = "tokio-runtime")]
#[test]
fn chain_floor_and_tx_gate_config_tier_end_to_end() {
    use youpipe::{AsyncTxGate, ChainFloor, PipelineConfig};

    let items: Vec<u64> = (0..n() as u64).collect();
    let expected: Vec<u64> = items.iter().map(|x| x * 2 + 1).collect();
    // Every tier of both knobs: output identical, run completes on each.
    let tier_16k = std::num::NonZeroUsize::new(16384).unwrap();
    for floor in [
        ChainFloor::Default,
        ChainFloor::Off,
        ChainFloor::Slots(tier_16k),
    ] {
        for gate in [AsyncTxGate::MixedOnly, AsyncTxGate::Off, AsyncTxGate::Full] {
            let config = PipelineConfig::default()
                .with_chain_floor(floor)
                .with_async_tx_gate(gate);
            let mut got = stream(items.clone())
                .with_cancel(inert_cancel())
                .with_config(config)
                .stage(|x: u64| x * 2)
                .stage_async(|x: u64| async move { x + 1 })
                .run();
            got.sort_unstable();
            assert_eq!(
                got, expected,
                "output is policy independent ({floor:?}, {gate:?})"
            );
        }
    }
}
