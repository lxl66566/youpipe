#!/usr/bin/env bash
# Loom model-check runner. Loom is switched on by the `--cfg loom` rustflag
# (NOT a cargo feature — see Cargo.toml / docs/src/dev/testing.md), so this script
# is the canonical entry point:
#
#   * youpipe's sync-core models (`#[cfg(all(test, loom))] mod loom_tests`)
#   * the vendored youpipe-concurrent-queue models (cfg(loom))
#   * the vendored youpipe-st3 models (cfg(st3_loom))
#
# LOOM_MAX_PREEMPTIONS=2 (upstream CI's setting) is essential for the
# vendored queue: without it the spsc models explore for hours (round-2
# lesson; with it the whole suite finishes in seconds).
#
# The four blocks are independent (different packages / cfg names), so they
# run CONCURRENTLY as background jobs; each keeps its own per-block timeout
# (embedded in the block functions — unchanged from the serial era), block
# output goes to a temp file and is cat'ed in order once all blocks finish
# (no interleaved stdout), and the exit code aggregates all four (any
# failure -> nonzero). Set LOOM_SERIAL=1 for the old one-at-a-time
# execution (e.g. when bisecting a model failure and co-running blocks'
# CPU contention is suspected).
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
# names, env overrides, --release, --report-time, filters, per-block
# timeouts: youpipe unbounded, queue 600, st3 1800, crossfire 600).
block_youpipe() {
    RUSTFLAGS="--cfg loom" cargo test --lib -- loom_tests
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
LABEL_QUEUE="vendored youpipe-concurrent-queue loom models"
LABEL_ST3="vendored youpipe-st3 loom models"
LABEL_CROSSFIRE="vendored youpipe-crossfire loom models (waker registry)"

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
    echo "==> $LABEL_YOUPIPE"
    block_youpipe || finish_block "$LABEL_YOUPIPE" $? /dev/null
    echo "==> $LABEL_QUEUE"
    block_queue || finish_block "$LABEL_QUEUE" $? /dev/null
    echo "==> $LABEL_ST3"
    block_st3 || finish_block "$LABEL_ST3" $? /dev/null
    echo "==> $LABEL_CROSSFIRE"
    block_crossfire || finish_block "$LABEL_CROSSFIRE" $? /dev/null
else
    tmpdir=$(mktemp -d)
    block_youpipe   >"$tmpdir/youpipe.log"   2>&1 & pid_youpipe=$!
    block_queue     >"$tmpdir/queue.log"     2>&1 & pid_queue=$!
    block_st3       >"$tmpdir/st3.log"       2>&1 & pid_st3=$!
    block_crossfire >"$tmpdir/crossfire.log" 2>&1 & pid_crossfire=$!

    # wait <pid> returns the job's exit status without tripping set -e.
    wait $pid_youpipe   || rc_youpipe=$?
    wait $pid_queue     || rc_queue=$?
    wait $pid_st3       || rc_st3=$?
    wait $pid_crossfire || rc_crossfire=$?

    # Deterministic output order (same as the serial era), one block at a
    # time; the [ok]/FAIL/TIMEOUT markers preserve which output is whose.
    finish_block "$LABEL_YOUPIPE"   "${rc_youpipe:-0}"   "$tmpdir/youpipe.log"
    finish_block "$LABEL_QUEUE"     "${rc_queue:-0}"     "$tmpdir/queue.log"
    finish_block "$LABEL_ST3"       "${rc_st3:-0}"       "$tmpdir/st3.log"
    finish_block "$LABEL_CROSSFIRE" "${rc_crossfire:-0}" "$tmpdir/crossfire.log"
    rmdir "$tmpdir" 2>/dev/null || true
fi

exit $overall
