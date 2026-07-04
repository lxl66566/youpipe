//! Compile-time guard for the downstream profile setting that silently
//! regresses youpipe's per-item throughput.
//!
//! youpipe ships a `.cargo/config.toml` override (`opt-level=3`,
//! `panic=unwind`) that applies inside the youpipe workspace. Downstream
//! crates do NOT inherit that override — their own `[profile.release]` wins,
//! and `opt-level = "s"` / `"z"` (size) is known to be harmful: it disables
//! the auto-vectorizer heuristics the leaf loops depend on, regressing the
//! lightweight warm path ~2×.
//!
//! This script emits a `cargo:warning` when size optimization is in effect
//! without a rustflags override, so the user sees it at build time. The build
//! proceeds regardless — youpipe remains correct — but the user is told
//! exactly how to force `opt-level = 3` for youpipe alone (without forcing it
//! on the rest of their dependency graph).
//!
//! `panic = "abort"` is NOT checked here: cargo's build-script env vars do not
//! reliably reflect the target crate's panic strategy (`CARGO_CFG_PANIC`
//! mirrors the build-script's own compilation, always `unwind`). That guard
//! lives in `lib.rs` via an accurate `#[cfg(panic = "abort")]` check.
//!
//! # Effective-vs-profile detection
//!
//! `OPT_LEVEL` reflects the *profile* setting, NOT the final value after
//! `[build] rustflags` overrides. Since rustc takes the last `-C` flag for
//! each setting, a `rustflags = ["-C", "opt-level=3"]` override (the pattern
//! youpipe's own `.cargo/config.toml` uses) makes the profile value
//! irrelevant. To avoid false positives we parse `CARGO_ENCODED_RUSTFLAGS` /
//! `RUSTFLAGS` for a trailing `-C opt-level=` and let that override the
//! profile-derived env var.

fn main() {
    // Only rerun when the script itself changes: the inputs are cargo-defined
    // env vars which change only on a profile edit, and a profile edit already
    // triggers a full rebuild.
    println!("cargo:rerun-if-changed=build.rs");

    let rustflags = collect_rustflags();
    if effective_opt_level(&rustflags).is_some_and(|lvl| matches!(lvl.as_str(), "s" | "z")) {
        let lvl = std::env::var("OPT_LEVEL").unwrap_or_default();
        println!(
            "cargo:warning=youpipe: compiled with `opt-level = \"{lvl}\"` (size optimization). \
             youpipe's leaf loops depend on LLVM's auto-vectorizer, which size optimization \
             disables — measured ~2× regression on the lightweight warm path. For a perf-first \
             crate, prefer `opt-level = 3`. You can force this for youpipe only via a \
             `.cargo/config.toml` override: `[build] rustflags = [\"-C\", \"opt-level=3\"]`. \
             See youpipe's own `.cargo/config.toml` for the worked example."
        );
    }
}

/// Collect every rustflag cargo will pass to rustc for this build, in order.
///
/// `CARGO_ENCODED_RUSTFLAGS` (Rust 1.55+) is a `\x1f`-separated list and is the
/// preferred source — it survives flags containing spaces. The legacy
/// `RUSTFLAGS` (space-separated) is the fallback for older toolchains.
fn collect_rustflags() -> Vec<String> {
    if let Ok(encoded) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        if !encoded.is_empty() {
            return encoded.split('\x1f').map(String::from).collect();
        }
    }
    std::env::var("RUSTFLAGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(String::from)
        .collect()
}

/// Scan `flags` left-to-right; the LAST `-C opt-level=X` wins (rustc
/// semantics). Falls back to the `OPT_LEVEL` profile env var when no rustflag
/// override is present.
fn effective_opt_level(flags: &[String]) -> Option<String> {
    last_c_setting(flags, "opt-level").or_else(|| std::env::var("OPT_LEVEL").ok())
}

/// Return the value of the last `-C <key>=<value>` or `-C<key>=<value>` flag
/// in `flags`. rustc takes the last `-C` setting for each key, mirroring how
/// `[build] rustflags` (appended after cargo's own `-C` flags) override the
/// profile.
fn last_c_setting(flags: &[String], key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let mut found: Option<String> = None;
    let mut iter = flags.iter().peekable();
    while let Some(flag) = iter.next() {
        // `-C opt-level=3` (two args) — rustc accepts the value as the next arg.
        if flag == "-C" {
            if let Some(next) = iter.peek() {
                if let Some(value) = next.strip_prefix(&needle) {
                    found = Some(value.to_string());
                }
            }
            continue;
        }
        // `-Copt-level=3` (single arg).
        if let Some(rest) = flag.strip_prefix("-C") {
            if let Some(value) = rest.strip_prefix(&needle) {
                found = Some(value.to_string());
            }
        }
    }
    found
}
