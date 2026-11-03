#!/usr/bin/env bash
# Pre-release full verification suite: one entry point that runs every
# stage a release gate needs, prints per-stage wall time and a summary,
# and exits nonzero if anything failed. Stages can be selected/re-run
# individually (long stages must fit interactive command budgets — a
# single-stage run is resumable by name):
#
#   perf/verify/full.sh                     # all stages, canonical order
#   perf/verify/full.sh --stage native,miri # subset (comma/space separated)
#   perf/verify/full.sh --stage loom
#
# Stages:
#   native  cargo nextest (workspace) + doctests + clippy -D warnings
#           (default features), plus the CI-parity --all-features clippy
#           line on unix only — youpipe-hotpath's hotpath-cpu feature
#           compile_error!s off macOS/Linux
#   miri    perf/verify/miri.sh (nextest-parallel; see its header)
#   loom    perf/verify/loom.sh (concurrent blocks; see its header)
#   bench   perf/bench-suite/bench-full.sh (owned by perf/bench-suite,
#           do not reimplement here; full criterion pass)
#
# Deliberately NOT in this suite (skip, do not fail):
#   - youpipe-gungraun: needs Linux + valgrind >= 3.20 + a version-matched
#     gungraun-runner binary; its Ir benches are not wall-time comparable
#     anyway (deterministic instruction counts, run via cargo bench -p).
#   - youpipe-bench one-shot bins (file-encrypt, convoy-probe,
#     hotpath-profile, horizontal-counters) and perf_events: manual-run lab
#     tools, several are Linux-only (perf counters, disk paths) — not part
#     of the timed gate.
#   - youpipe-criterion-perf-counters / youpipe-bench / youpipe-gungraun
#     are additionally EXCLUDED from the workspace builds below: their
#     perf-event / gungraun dependency chains do not compile on Windows
#     (perf-event only builds against Linux headers).
#
# Locks: each stage builds in its own worktree target dir (cargo's own
# build-dir lock serializes intra-worktree cargo), and the bench stage
# owns its timing locks internally (perf/bench-suite convention), so this
# script takes no cross-stage locks itself.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Linux-only / valgrind-dependent crates, excluded from workspace stages
# on every host (see header): their tests either cannot run or are not
# part of the wall-time gate.
EXCLUDES=(--exclude youpipe-bench
          --exclude youpipe-gungraun
          --exclude youpipe-criterion-perf-counters)

STAGES=()
selected=""
while (($#)); do
    case "$1" in
        --stage)
            shift
            [[ -n ${1:-} ]] || { echo "full.sh: --stage needs an argument" >&2; exit 2; }
            selected="${selected:+$selected,}$1"
            ;;
        --stage=*) selected="${selected:+$selected,}${1#--stage=}" ;;
        -h|--help) grep '^#' "$0" | tail -n +2 | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "full.sh: unknown argument: $1 (usage: --stage a,b)" >&2; exit 2 ;;
    esac
    shift
done

ALL_STAGES=(native miri loom bench)
if [[ -n "$selected" ]]; then
    IFS=',' read -ra picked <<< "$selected"
    for p in "${picked[@]}"; do
        [[ " ${ALL_STAGES[*]} " == *" $p "* ]] \
            || { echo "full.sh: unknown stage: $p (valid: ${ALL_STAGES[*]})" >&2; exit 2; }
        STAGES+=("$p")
    done
else
    STAGES=("${ALL_STAGES[@]}")
fi

declare -A STAGE_SECS STAGE_RC
overall=0

run_stage() { # <name> <cmd...>: run with wall-time bookkeeping
    local name=$1; shift
    local start=$SECONDS
    echo ""
    echo "==================== stage: $name ===================="
    if "$@"; then
        STAGE_RC[$name]=0
    else
        STAGE_RC[$name]=$?
        overall=1
        echo "full.sh: stage $name FAILED (rc=${STAGE_RC[$name]})" >&2
    fi
    STAGE_SECS[$name]=$((SECONDS - start))
}

stage_native() {
    # NOTE: run_stage invokes this function inside `if "$@"`, which
    # suspends set -e for everything below — so every command accumulates
    # rc explicitly; a trailing echo must never mask a failure.
    local rc=0
    # nextest over the workspace (default-members' 360 tests + the fork
    # crates' suites). -j default 8 + one retry: crossfire's spin-consumer
    # MPSC tests starve under high process parallelism on a loaded shared
    # Windows host ("consumer stalled" — the busy-poll budget trips while
    # sibling processes starve its producer threads; observed at -j 32
    # always, at -j 8 only under colleague bench load). --retries 1
    # absorbs that scheduler flake while a deterministic failure still
    # fails twice and trips the gate. Doctests are not run by nextest
    # (known limitation) — cargo test --doc covers them, exactly mirroring
    # CI's nextest+doc-test pair. --no-fail-fast keeps the stage's
    # continue-on-failure semantics aligned with miri.sh's.
    cargo nextest run -j "${FULL_NATIVE_JOBS:-8}" --retries 1 \
        --no-fail-fast --workspace "${EXCLUDES[@]}" || rc=1
    cargo test --doc --workspace "${EXCLUDES[@]}" || rc=1
    # Clippy, default features, every host.
    cargo clippy --workspace "${EXCLUDES[@]}" --all-targets -- -D warnings \
        || rc=1
    # CI-parity clippy line, unix only: youpipe's hotpath feature pulls
    # hotpath-cpu, whose non-unix build is a deliberate compile_error!
    # (CI runs this exact gate on ubuntu.)
    case "${OSTYPE:-}" in
        msys|cygwin|win32*|windows*)
            echo "(skipped --all-features clippy: hotpath-cpu is macOS/Linux-only by compile_error!; CI runs this gate on Linux)"
            ;;
        *)
            cargo clippy --workspace "${EXCLUDES[@]}" --all-features \
                --all-targets -- -D warnings || rc=1
            ;;
    esac
    echo "(skipped youpipe-gungraun: needs Linux + valgrind + gungraun-runner; run cargo bench -p youpipe-gungraun there)"
    echo "(skipped youpipe-bench one-shot bins + perf_events: manual lab tools, Linux-only counters; see perf/verify/full.sh header)"
    return "$rc"
}

stage_miri() {
    bash perf/verify/miri.sh
}

stage_loom() {
    bash perf/verify/loom.sh
}

stage_bench() {
    # Owned by perf/bench-suite. Do NOT reimplement here.
    if [[ ! -f perf/bench-suite/bench-full.sh ]]; then
        echo "full.sh: perf/bench-suite/bench-full.sh not found." >&2
        echo "  (the bench stage is owned by perf/bench-suite; if the file is" >&2
        echo "   absent on this branch, run the criterion pass manually:" >&2
        echo "   cargo bench, or perf/bench-suite/bench_ab.sh --sweep -a wt)" >&2
        return 1
    fi
    bash perf/bench-suite/bench-full.sh
}

total_start=$SECONDS
for s in "${STAGES[@]}"; do
    case "$s" in
        native) run_stage native stage_native ;;
        miri)   run_stage miri   stage_miri ;;
        loom)   run_stage loom   stage_loom ;;
        bench)  run_stage bench  stage_bench ;;
    esac
done

echo ""
echo "==================== full.sh summary ===================="
for s in "${ALL_STAGES[@]}"; do
    if [[ -n ${STAGE_SECS[$s]:-} ]]; then
        printf '  %-8s %6ss   rc=%s\n' "$s" "${STAGE_SECS[$s]}" "${STAGE_RC[$s]}"
    else
        printf '  %-8s   ----   (not selected)\n' "$s"
    fi
done
printf '  %-8s %6ss\n' "total" "$((SECONDS - total_start))"

exit $overall
