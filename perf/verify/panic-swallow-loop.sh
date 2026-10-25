#!/usr/bin/env bash
# Fresh-process loop over the async-panic family of pipeline_integration
# (todo #23 verification harness): one pass = one process running the
# panic-propagation subset at the default parallel width, rc != 0 counts as
# a failure. Companion to hang-stakeout.sh — no watchdog/forensics here
# (the swallow returns, it does not hang); this only measures the rate.
#
# Usage:
#   cargo test -p youpipe --release --test pipeline_integration --no-run
#   perf/verify/panic-swallow-loop.sh <path-to-test-binary> [passes]
# Environment: TEST_THREADS (default 32), LABEL (default bin name).
# Run under /tmp/youpipe-locks/bench.lock — CPU-heavy.
set -u
cd "$(git rev-parse --show-toplevel)"

BIN=${1:?usage: panic-swallow-loop.sh <test-binary> [passes]}
PASSES=${2:-200}
TEST_THREADS=${TEST_THREADS:-32}
LABEL=${LABEL:-$(basename "$BIN" | cut -c1-20)}
OUTDIR=${OUTDIR:-target/panic-swallow-loop}
mkdir -p "$OUTDIR"
TSV="$OUTDIR/${LABEL}-$(date +%Y%m%d-%H%M%S).tsv"
echo -e "pass\trc\twall_s" > "$TSV"

fails=0
for i in $(seq 1 "$PASSES"); do
    s=$(date +%s%N)
    "$BIN" --test-threads "$TEST_THREADS" panic_propagates run_in_ >/dev/null 2>&1
    rc=$?
    e=$(date +%s%N)
    wall_ms=$(((e - s) / 1000000))
    echo -e "$i\t$rc\t${wall_ms}ms" >> "$TSV"
    if [ "$rc" -ne 0 ]; then
        fails=$((fails + 1))
        echo "pass $i: FAILED (rc=$rc)" >&2
    fi
done
echo "label=$LABEL passes=$PASSES threads=$TEST_THREADS fails=$fails"
