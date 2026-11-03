#!/usr/bin/env bash
# One-shot full-suite timed-bench runner at the QUICK sampling budget.
#
# Scope: every timed bench in the repo, run serially — the 12 youpipe
# criterion binaries, the vendored forks' criterion benches
# (youpipe-concurrent-queue, youpipe-st3), the unix-only `horizontal`
# cross-library comparison and the deterministic gungraun instruction-count
# benches (valgrind). NOT in scope: miri/loom verification (perf/verify/)
# and A/B verdicts (bench_ab.sh interleaved rounds) — quick-tier numbers
# are smoke-grade and criterion reads are never cross-time comparable on
# this fleet anyway.
#
# Budget tiers (docs/src/dev/benchmarks/README.md "Suite budget"):
#   * verdict  20 samples / 1 s warm-up / 2 s measure — default in
#              benches/common/mod.rs, driven by bench_ab.sh for release-gate
#              A/B decisions (~3.5 s per id, ~16 min per criterion pass).
#   * quick    10 samples / 0.5 s warm-up / 1 s measure — THIS script.
#              ~1.5 s fixed criterion window per id; full pass measured
#              2026-11-04 (Windows dev box, background load): 277 youpipe
#              ids ~8.9 min + 68 fork ids ~2.1 min ≈ 11 min of timing
#              wall, plus a cold build measured in minutes. The same-day
#              default-grid trim (277 -> 246 ids, testing.md "Bench grid
#              trim") brings the youpipe pass to ~8 min, ~10 min total.
#
# Fork benches carry no env knobs (their sources must stay diffable
# against the upstream forks), so the same budget is forced through
# criterion CLI flags: --sample-size 10 --warm-up-time 0.5
# --measurement-time 1. NOTE: both criterion versions in play (0.8 for
# youpipe/youpipe-concurrent-queue, 0.5 for youpipe-st3) parse the
# duration flags as plain f64 SECONDS — "500ms" is a clap parse error
# (verified against both crates' arg definitions).
#
# Lock protocol (co-runner etiquette, shared with bench_ab.sh): build
# segments hold /tmp/youpipe-locks/cargo.lock, timed segments hold
# /tmp/youpipe-locks/bench.lock. Every timed stage re-verifies its targets
# with `cargo bench --no-run` under cargo.lock first, so a mid-run source
# edit can never trigger a compile while bench.lock is held. flock is not
# shipped with git bash on Windows — the script warns once and proceeds
# without the protocol locks there (cargo's own target-dir lock still
# serializes builds; only bench-vs-bench overlap is unprotected).
#
# Contract: perf/verify/full.sh (release gate) invokes this script with
# defaults; it must stay non-interactive, print one wall-clock line per
# timed binary plus a total, and exit non-zero if anything failed.
#
# Usage:
#   perf/bench-suite/bench-full.sh                     # everything
#   perf/bench-suite/bench-full.sh --stage build,youpipe
#   perf/bench-suite/bench-full.sh --bench sync_vs_rayon --bench unbalanced
#   perf/bench-suite/bench-full.sh --rounds 5          # horizontal rounds
#
# Options:
#   --stage LIST  comma-separated subset of build,youpipe,forks,horizontal,
#                 gungraun (repeatable, default all five in that order)
#   --bench NAME  restrict the youpipe criterion pass to these targets
#                 (repeatable, default all 12)
#   --rounds N    horizontal rounds (default 3; the bench's own default
#                 is 5 — the quick tier trims it)
#   -h, --help    this text
#
# Environment: BENCH_SAMPLE_SIZE / BENCH_WARMUP_MS / BENCH_MEASUREMENT_MS
# override the quick defaults (10/500/1000) for the youpipe criterion
# pass; LOCK_WAIT_SECS caps lock acquisition (default 7200). Quick-tier
# criterion data must not feed later verdict baselines, so CRITERION_HOME
# defaults to target/criterion-quick (export it beforehand to override).
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"
START=$SECONDS

usage() { sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1; exit "${1:-0}"; }

# ── knobs ──────────────────────────────────────────────────────────────
SAMPLES=${BENCH_SAMPLE_SIZE:-10}
WARMUP_MS=${BENCH_WARMUP_MS:-500}
MEASURE_MS=${BENCH_MEASUREMENT_MS:-1000}
HROUNDS=3
LOCK_WAIT_SECS=${LOCK_WAIT_SECS:-7200}
# criterion 0.5/0.8 duration flags take f64 SECONDS (see header note)
WARMUP_S=$(awk "BEGIN{printf \"%.4g\", $WARMUP_MS/1000}")
MEASURE_S=$(awk "BEGIN{printf \"%.4g\", $MEASURE_MS/1000}")
QUICK_HOME=${CRITERION_HOME:-$PWD/target/criterion-quick}
mkdir -p "$QUICK_HOME"

# Master list = every youpipe criterion bench target (manifest order is
# irrelevant; heavy binaries first so a fresh break surfaces early).
# `horizontal` is NOT here: it is not criterion-driven and unix-only, it
# has its own stage. Keep in sync with crates/youpipe/Cargo.toml [[bench]].
MASTER_BENCHES=(sync_vs_rayon unbalanced sync_fuse io_async expand
                graph_shapes pool_reuse sharded_term channel_bench
                async_vs_tokio sharded_term_async mixed_load)
STAGES=""
declare -a BENCHES=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage)        STAGES="${STAGES:+$STAGES,}$2"; shift 2 ;;
        --bench)        BENCHES+=("$2"); shift 2 ;;
        --rounds)       HROUNDS="$2"; shift 2 ;;
        -h|--help)      usage 0 ;;
        -*)             echo "unknown option: $1" >&2; usage 1 ;;
        *)              echo "unexpected argument: $1" >&2; usage 1 ;;
    esac
done

# --stage SELECTS a subset (repeatable, comma-separated); the default runs
# every stage. Found the hard way: initializing STAGES to the full list and
# appending made `--stage horizontal` silently run everything — the subset
# flag was a no-op — so the default is only filled in after parsing.
if [[ -z "$STAGES" ]]; then
    STAGES="build,youpipe,forks,horizontal,gungraun"
fi

for s in ${STAGES//,/ }; do
    [[ ",build,youpipe,forks,horizontal,gungraun," == *",$s,"* ]] \
        || { echo "unknown stage: $s" >&2; exit 1; }
done
if (( ${#BENCHES[@]} == 0 )); then
    BENCHES=("${MASTER_BENCHES[@]}")
else
    for b in "${BENCHES[@]}"; do
        [[ " ${MASTER_BENCHES[*]} " == *" $b "* ]] \
            || { echo "unknown --bench: $b (known: ${MASTER_BENCHES[*]})" >&2; exit 1; }
    done
fi
stage_selected() { [[ ",$STAGES," == *",$1,"* ]]; }

# ── co-runner locks (see header) ───────────────────────────────────────
LOCK_DIR=/tmp/youpipe-locks
mkdir -p "$LOCK_DIR"
NOFLOCK_NOTED=0
have_flock() { command -v flock >/dev/null 2>&1; }
note_noflock() {
    if (( NOFLOCK_NOTED == 0 )); then
        NOFLOCK_NOTED=1
        echo "warning: flock not found (git bash?) — proceeding WITHOUT the" \
             "/tmp/youpipe-locks co-runner protocol; builds still serialize" \
             "on cargo's target-dir lock" >&2
    fi
}
cargo_lock() {
    if have_flock; then
        exec 3>"$LOCK_DIR/cargo.lock"
        flock -w "$LOCK_WAIT_SECS" 3 \
            || { echo "FATAL: timed out after ${LOCK_WAIT_SECS}s waiting for cargo.lock" >&2; exit 1; }
    else
        note_noflock
    fi
}
cargo_unlock() { have_flock && exec 3>&- || true; }
bench_lock() {
    if have_flock; then
        exec 4>"$LOCK_DIR/bench.lock"
        flock -w "$LOCK_WAIT_SECS" 4 \
            || { echo "FATAL: timed out after ${LOCK_WAIT_SECS}s waiting for bench.lock" >&2; exit 1; }
    else
        note_noflock
    fi
}
bench_unlock() { have_flock && exec 4>&- || true; }

# Build-segment helper: freshen the given targets under cargo.lock. On a
# cold cache a 32-way build has been earlyoom-killed on the shared box
# (see bench_ab.sh's 2026-09-29 memory lesson) — if a build dies here
# silently, export CARGO_BUILD_JOBS=6 and re-run.
ensure_built() { # cargo-args...
    cargo_lock
    cargo bench --no-run "$@"
    cargo_unlock
}

# soft stray-process warning (bench_ab.sh hard-refuses; an unattended
# full-suite run should not die on a harmless match — warn only)
if command -v pgrep >/dev/null 2>&1; then
    strays=$(pgrep -a -f 'cargo miri|/miri/|criterion-|cargo bench' 2>/dev/null || true)
    if [[ -n "$strays" ]]; then
        echo "warning: possibly bench-polluting processes are running:" >&2
        echo "$strays" >&2
    fi
fi

# ── per-binary timing with failure collection ──────────────────────────
declare -a REPORT=()
FAILURES=0
timed() { # label cmd...
    local label=$1; shift
    local s=$SECONDS
    echo "==> [$label] start"
    if ! "$@"; then
        echo "==> [$label] FAILED" >&2
        FAILURES=$((FAILURES + 1))
        REPORT+=("FAIL  $label  ($((SECONDS - s))s)")
    else
        REPORT+=("ok    $label  $((SECONDS - s))s")
        echo "==> [$label] done in $((SECONDS - s))s"
    fi
}

# ── stage: build (everything except gungraun, which builds lazily in its
#    own stage so hosts without valgrind don't pay the debug-info compile)
if stage_selected build; then
    echo "==> stage build"
    ensure_built -p youpipe
    ensure_built -p youpipe-concurrent-queue --bench bench
    # explicit --bench selection: youpipe-st3's [lib] has no bench=false,
    # a bare `cargo bench -p youpipe-st3` would also run its libtest target
    ensure_built -p youpipe-st3 --bench benchmark --bench micro
fi

# ── stage: youpipe criterion binaries (budget via env, see common/mod.rs)
if stage_selected youpipe; then
    echo "==> stage youpipe (samples=$SAMPLES warmup=${WARMUP_MS}ms measure=${MEASURE_MS}ms)"
    ensure_built -p youpipe
    bench_lock
    for b in "${BENCHES[@]}"; do
        timed "youpipe/$b" \
            env BENCH_SAMPLE_SIZE="$SAMPLES" BENCH_WARMUP_MS="$WARMUP_MS" \
                BENCH_MEASUREMENT_MS="$MEASURE_MS" CRITERION_HOME="$QUICK_HOME" \
            cargo bench -p youpipe --bench "$b" -- --noplot
    done
    bench_unlock
fi

# ── stage: fork criterion benches (budget via CLI flags: fork sources
#    carry no env knobs and must stay diffable against their upstreams)
if stage_selected forks; then
    echo "==> stage forks (samples=$SAMPLES warmup=${WARMUP_S}s measure=${MEASURE_S}s)"
    ensure_built -p youpipe-concurrent-queue --bench bench
    ensure_built -p youpipe-st3 --bench benchmark --bench micro
    bench_lock
    timed "fork/concurrent-queue:bench" \
        env CRITERION_HOME="$QUICK_HOME" \
        cargo bench -p youpipe-concurrent-queue --bench bench -- \
            --sample-size "$SAMPLES" --warm-up-time "$WARMUP_S" \
            --measurement-time "$MEASURE_S" --noplot
    timed "fork/st3:benchmark" \
        env CRITERION_HOME="$QUICK_HOME" \
        cargo bench -p youpipe-st3 --bench benchmark -- \
            --sample-size "$SAMPLES" --warm-up-time "$WARMUP_S" \
            --measurement-time "$MEASURE_S" --noplot
    timed "fork/st3:micro" \
        env CRITERION_HOME="$QUICK_HOME" \
        cargo bench -p youpipe-st3 --bench micro -- \
            --sample-size "$SAMPLES" --warm-up-time "$WARMUP_S" \
            --measurement-time "$MEASURE_S" --noplot
    bench_unlock
fi

# ── stage: horizontal cross-library comparison (unix-only; on Windows the
#    target compiles to a no-op main — skip instead of wasting a run)
if stage_selected horizontal; then
    case "$OSTYPE" in
        msys*|cygwin*|win32*)
            echo "==> stage horizontal: skipped (unix-only: tokio UDS web scenario; Windows binary is a no-op)"
            REPORT+=("skip  horizontal (unix-only)")
            ;;
        *)
            echo "==> stage horizontal (rounds=$HROUNDS)"
            ensure_built -p youpipe --bench horizontal
            bench_lock
            timed "horizontal" \
                cargo bench -p youpipe --bench horizontal -- \
                    --rounds "$HROUNDS" --out target/horizontal/results.json
            bench_unlock
            ;;
    esac
fi

# ── stage: gungraun deterministic instruction counts (default config;
#    needs Linux + valgrind >= 3.20 and a version-matched gungraun-runner
#    on PATH, see crates/youpipe-gungraun/Readme.md)
if stage_selected gungraun; then
    if command -v valgrind >/dev/null 2>&1 && command -v gungraun-runner >/dev/null 2>&1; then
        echo "==> stage gungraun"
        ensure_built -p youpipe-gungraun
        bench_lock
        timed "gungraun" cargo bench -p youpipe-gungraun
        bench_unlock
    else
        v=$(command -v valgrind || echo none)
        g=$(command -v gungraun-runner || echo none)
        echo "==> stage gungraun: skipped (valgrind: $v, gungraun-runner: $g — needs both on PATH)"
        REPORT+=("skip  gungraun (no valgrind/runner)")
    fi
fi

# ── summary ────────────────────────────────────────────────────────────
echo
echo "==> summary (quick budget: $SAMPLES samples / ${WARMUP_MS}ms warmup / ${MEASURE_MS}ms measure)"
if (( ${#REPORT[@]} > 0 )); then
    printf '  %s\n' "${REPORT[@]}"
fi
echo "==> total wall: $((SECONDS - START))s"
if (( FAILURES > 0 )); then
    echo "==> $FAILURES timed binary(ies) FAILED" >&2
    exit 1
fi
