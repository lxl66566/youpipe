#!/usr/bin/env bash
# Loom model-check runner. Loom is switched on by the `--cfg loom` rustflag
# (NOT a cargo feature — see Cargo.toml / docs/testing.md), so this script
# is the canonical entry point:
#
#   * youpipe's sync-core models (`#[cfg(all(test, loom))] mod loom_tests`)
#   * the vendored youpipe-concurrent-queue models (cfg(loom))
#   * the vendored youpipe-st3 models (cfg(st3_loom))
#
# LOOM_MAX_PREEMPTIONS=2 (upstream CI's setting) is essential for the
# vendored queue: without it the spsc models explore for hours (round-2
# lesson; with it the whole suite finishes in seconds).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

export LOOM_MAX_PREEMPTIONS=${LOOM_MAX_PREEMPTIONS:-2}

echo "==> youpipe loom models"
RUSTFLAGS="--cfg loom" cargo test --lib -- loom_tests

echo "==> vendored youpipe-concurrent-queue loom models"
RUSTFLAGS="--cfg loom" timeout 600 cargo test -p youpipe-concurrent-queue --test loom

echo "==> vendored youpipe-st3 loom models"
# st3 keeps upstream's own `st3_loom` cfg name, so the rustflag cannot leak
# into other dependencies' cfg(loom) shims. LOOM_MAX_PREEMPTIONS=3 is
# upstream CI's setting; loom must be a regular target-gated dependency of
# the lib for these models to see simulated primitives at all (see
# crates/youpipe-st3/Cargo.toml). Release + report-time: single models take
# 0.3s-2min under real exploration.
LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg st3_loom" \
    timeout 1800 cargo test -p youpipe-st3 --release --test integration -- \
    -Z unstable-options --report-time

echo "==> vendored youpipe-crossfire loom models (waker registry)"
# No RUSTFLAGS here: crossfire gates its models on the `loom` cargo feature
# (a --cfg loom rustflag would leak into dependencies that carry cfg(loom)
# test shims without the loom crate linked, e.g. event-listener).
LOOM_MAX_PREEMPTIONS=${LOOM_MAX_PREEMPTIONS:-2} \
    timeout 600 cargo test -p youpipe-crossfire --features loom --lib --release -- loom_
