//! Shared bench-harness configuration for all youpipe bench targets.
//!
//! Full-suite runs with criterion's defaults (100 samples, 3 s warm-up,
//! 5 s measurement per id) add up to >1 h wall time. The proven verdict
//! methodology (see docs/benchmarks.md) is *interleaved* alternating A/B
//! rounds at `--sample-size 20`: extra rounds beat extra samples-per-round
//! on a machine with ±10 % inter-run drift, because interleaving cancels
//! the drift while more samples in one pass cannot.
//!
//! Defaults here target that methodology: 20 samples, 1 s warm-up, 2 s
//! measurement (~3.5 s per bench id). Every knob is env-overridable so a
//! deep dive on a single bench still gets the full treatment without code
//! edits:
//!
//! ```sh
//! BENCH_SAMPLE_SIZE=100 BENCH_WARMUP_MS=3000 BENCH_MEASUREMENT_MS=5000 \
//!     cargo bench --bench sync_vs_rayon
//! ```
//!
//! Fairness note: these knobs only change statistical resolution, never the
//! benchmarked code path, and A/B runs always apply the same environment to
//! both sides (perf/bench-suite does this by construction).

use std::time::Duration;

use criterion::Criterion;

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Build the shared `Criterion` config (see module docs).
pub fn criterion() -> Criterion {
    Criterion::default()
        .sample_size(env_parse("BENCH_SAMPLE_SIZE", 20))
        .warm_up_time(Duration::from_millis(env_parse("BENCH_WARMUP_MS", 1_000)))
        .measurement_time(Duration::from_millis(env_parse(
            "BENCH_MEASUREMENT_MS",
            2_000,
        )))
}
