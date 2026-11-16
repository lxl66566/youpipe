#!/usr/bin/env bash
# Loom model-check runner. The switches differ per crate (see Cargo.toml /
# docs/src/dev/testing.md), so this script is the canonical entry point:
#
#   * youpipe's sync-core models (`#[cfg(all(test, loom))] mod loom_tests`),
#     `--cfg loom`
#   * the same crate plus the st3-deque model, in a second build that also
#     carries the vendored st3's `--cfg st3_loom`
#   * the vendored youpipe-concurrent-queue models (cfg(loom))
#   * the vendored youpipe-st3 models (cfg(st3_loom))
#   * the vendored youpipe-crossfire waker-registry models (feature loom)
#
# LOOM_MAX_PREEMPTIONS=2 (upstream CI's setting) is essential for the
# vendored queue: without it the spsc models explore for hours (round-2
# lesson; with it the whole suite finishes in seconds).
#
# The five blocks are independent (different packages / cfg names / target
# dirs), so they run CONCURRENTLY as background jobs; each keeps its own
# per-block timeout (embedded in the block functions), block output goes to
# a temp file and is cat'ed in order once all blocks finish (no interleaved
# stdout), and the exit code aggregates all five (any failure -> nonzero).
# Set LOOM_SERIAL=1 for the old one-at-a-time execution (e.g. when bisecting
# a model failure and co-running blocks' CPU contention is suspected).
#
# LOOM_BLOCKS="youpipe youpipe_st3 queue st3 crossfire" (default: all five)
# runs a subset — a local convenience: loom exploration is deterministic, so
# a vendored block whose inputs are unchanged since its last green run (fork
# source, block flags, Cargo.lock's loom version) repeats itself. CI runs
# the full default.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

export LOOM_MAX_PREEMPTIONS=${LOOM_MAX_PREEMPTIONS:-2}

# wrap_timeout <secs> <cmd...>: `timeout` may be missing (plain git bash
# without coreutils) — degrade to an unwrapped run rather than failing.
wrap_timeout() {
    local tsecs=$1; shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$tsecs" "$@"
    else
        "$@"
    fi
}

# Each block's command mirrors the serial-era invocation verbatim (cfg
# names, env overrides, --release, --report-time, filters). Per-block
# timeouts: youpipe 1200, youpipe+st3 1200, queue 600, st3 1800, crossfire
# 600 — the two youpipe ones budget a cold full dep-tree build each (their
# rustflags differ, so the two builds cannot share a target dir).
block_youpipe() {
    RUSTFLAGS="--cfg loom" wrap_timeout 1200 \
        cargo test -p youpipe --lib -- loom_tests
}
block_youpipe_st3() {
    # Separate build from block_youpipe: the st3-deque models need the vendored
    # st3 compiled with its own `st3_loom` cfg, so the crate is rebuilt with
    # both rustflags. Under `--cfg loom` alone those models still pass, but
    # vacuously (st3 keeps real atomics, loom sees one schedule).
    #
    # Separate target dir too: in the shared one this block's extra rustflag
    # flips youpipe's dep tree between two fingerprints — a full rebuild per
    # block per script run, and one block's compile co-running against the
    # other's model exploration slows the heavy models down ~100x until the
    # block timeout fires (measured 2026-11-17: rc=124 at 600 s against 3.9 s
    # of tests on an idle host). A private dir keeps the fingerprint stable.
    CARGO_TARGET_DIR=target/loom-st3 \
        RUSTFLAGS="--cfg loom --cfg st3_loom" wrap_timeout 1200 \
        cargo test -p youpipe --lib -- loom_tests
}
block_queue() {
    RUSTFLAGS="--cfg loom" wrap_timeout 600 \
        cargo test -p youpipe-concurrent-queue --test loom
}
block_st3() {
    # st3 keeps upstream's own `st3_loom` cfg name, so the rustflag cannot
    # leak into other dependencies' cfg(loom) shims. LOOM_MAX_PREEMPTIONS=3
    # is upstream CI's setting; loom must be a regular target-gated
    # dependency of the lib for these models to see simulated primitives at
    # all (see crates/youpipe-st3/Cargo.toml). Release + report-time:
    # single models take 0.3s-2min under real exploration.
    LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg st3_loom" wrap_timeout 1800 \
        cargo test -p youpipe-st3 --release --test integration -- \
        -Z unstable-options --report-time
}
block_crossfire() {
    # No RUSTFLAGS here: crossfire gates its models on the `loom` cargo
    # feature (a --cfg loom rustflag would leak into dependencies that
    # carry cfg(loom) test shims without the loom crate linked, e.g.
    # event-listener).
    wrap_timeout 600 \
        cargo test -p youpipe-crossfire --features loom --lib --release -- loom_
}

LABEL_YOUPIPE="youpipe loom models"
LABEL_YOUPIPE_ST3="youpipe loom models with the st3 deque modeled"
LABEL_QUEUE="vendored youpipe-concurrent-queue loom models"
LABEL_ST3="vendored youpipe-st3 loom models"
LABEL_CROSSFIRE="vendored youpipe-crossfire loom models (waker registry)"

ALL_BLOCKS=(youpipe youpipe_st3 queue st3 crossfire)

# LOOM_BLOCKS selects a subset (see the header); the default runs all five.
if [[ -n ${LOOM_BLOCKS:-} ]]; then
    read -ra BLOCKS <<<"$LOOM_BLOCKS"
    [[ ${#BLOCKS[@]} -gt 0 ]] || { echo "LOOM_BLOCKS selects nothing" >&2; exit 2; }
    for block in "${BLOCKS[@]}"; do
        [[ " ${ALL_BLOCKS[*]} " == *" $block "* ]] || {
            echo "unknown LOOM_BLOCKS entry: $block (valid: ${ALL_BLOCKS[*]})" >&2
            exit 2
        }
    done
else
    BLOCKS=("${ALL_BLOCKS[@]}")
fi

block_cmd() { # <name>: run that block's command
    case $1 in
        youpipe) block_youpipe ;;
        youpipe_st3) block_youpipe_st3 ;;
        queue) block_queue ;;
        st3) block_st3 ;;
        crossfire) block_crossfire ;;
    esac
}

block_label() { # <name>
    case $1 in
        youpipe) echo "$LABEL_YOUPIPE" ;;
        youpipe_st3) echo "$LABEL_YOUPIPE_ST3" ;;
        queue) echo "$LABEL_QUEUE" ;;
        st3) echo "$LABEL_ST3" ;;
        crossfire) echo "$LABEL_CROSSFIRE" ;;
    esac
}

overall=0

finish_block() { # <label> <rc> <logfile>
    local label=$1 rc=$2 log=$3
    if [[ $rc -eq 0 ]]; then
        echo "==> [ok] $label"
    elif [[ $rc -eq 124 ]]; then
        echo "==> [TIMEOUT] $label (rc=124)" >&2
        overall=1
    else
        echo "==> [FAIL rc=$rc] $label" >&2
        overall=1
    fi
    [[ $log != /dev/null ]] && cat "$log" && rm -f "$log"
    return 0
}

if [[ ${LOOM_SERIAL:-0} == 1 ]]; then
    for block in "${BLOCKS[@]}"; do
        echo "==> $(block_label "$block")"
        block_cmd "$block" || finish_block "$(block_label "$block")" $? /dev/null
    done
else
    tmpdir=$(mktemp -d)
    names=()
    pids=()
    for block in "${BLOCKS[@]}"; do
        block_cmd "$block" >"$tmpdir/$block.log" 2>&1 &
        names+=("$block")
        pids+=($!)
    done

    # Deterministic output order (same as the serial era), one block at a
    # time; the [ok]/FAIL/TIMEOUT markers preserve which output is whose.
    # wait <pid> returns the job's exit status without tripping set -e.
    for i in "${!names[@]}"; do
        rc=0
        wait "${pids[$i]}" || rc=$?
        finish_block "$(block_label "${names[$i]}")" "$rc" "$tmpdir/${names[$i]}.log"
    done
    rmdir "$tmpdir" 2>/dev/null || true
fi

exit $overall
