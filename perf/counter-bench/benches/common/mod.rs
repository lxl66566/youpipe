//! Copy of the root `benches/common/mod.rs` (this crate is standalone, so
//! the shared harness config cannot be imported across packages). Keep in
//! sync with the original — same env knobs, same defaults, same rationale.

use std::time::Duration;

use criterion::Criterion;

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Build the shared `Criterion` config (see the root benches/common/mod.rs).
pub fn criterion() -> Criterion {
    Criterion::default()
        .sample_size(env_parse("BENCH_SAMPLE_SIZE", 20))
        .warm_up_time(Duration::from_millis(env_parse("BENCH_WARMUP_MS", 1_000)))
        .measurement_time(Duration::from_millis(env_parse(
            "BENCH_MEASUREMENT_MS",
            2_000,
        )))
}
