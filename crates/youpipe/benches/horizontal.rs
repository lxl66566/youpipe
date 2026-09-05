//! Horizontal cross-library comparison benchmark (exported as JSON for
//! `perf/plot-horizontal.py`).
//!
//! Unix-only: the realistic web scenario serves HTTP/1.1 over a loopback
//! Unix domain socket, and tokio does not export UDS types on Windows — the
//! implementation is cfg-gated there. Methodology and scenarios: [`unix`].
//!
//! ```sh
//! cargo bench --bench horizontal -- --rounds 5 --out target/horizontal/results.json
//! uv run perf/plot-horizontal.py target/horizontal/results.json
//! ```

// Not `#![cfg(unix)]`: this target is `harness = false`, so a fully
// cfg'd-out file would ship no `main` and fail to compile on Windows.
#[cfg(unix)]
#[path = "horizontal/unix.rs"]
mod unix;

fn main() {
    #[cfg(unix)]
    unix::main();
}
