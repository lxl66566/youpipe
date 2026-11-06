#!/usr/bin/env bash
# Miri runner that engineers out the two operational hazards seen in the
# 2026-09 rounds:
#
#   1. stale/hung miri processes left in the background silently pin a core
#      at 100% and pollute every later benchmark (the round-2 war story:
#      "every bench run must ps-aux-check for miri first"). Killed up front,
#      with a self-match-safe pattern.
#   2. a single wedged test hanging the whole run for hours. The default
#      path runs `cargo miri nextest run`: every test is its own process,
#      so a wedge (or a miri abort, e.g. an unsupported-op on a foreign
#      function) is contained to that one test and every other test still
#      executes — the serial path's per-binary `timeout` wrapper is kept
#      as the group-level deadline (mapping in run_nextest_group below).
#   3. editing this script while an instance is mid-run corrupts it (bash
#      reads incrementally) — copy-edit out-of-run only.
#
# Also fixes the MIRIFLAGS trap: `.cargo/config.toml` sets
# `-Zmiri-ignore-leaks` (intentionally leaked global-pool workers), but a
# caller-supplied MIRIFLAGS replaces it entirely — so this script defaults
# to "-Zmiri-tree-borrows -Zmiri-ignore-leaks" (the recommended strict
# combo) and only exports it when the caller hasn't set one.
#
# Usage:
#   perf/verify/miri.sh [test-filter...]   # filters pass to the harness
#   MIRI_TIMEOUT_SECS=600 perf/verify/miri.sh
#   MIRI_JOBS=16 perf/verify/miri.sh       # nextest test-threads (default 8)
#   MIRI_TARGETS="lib pipeline_integration" perf/verify/miri.sh
#   MIRI_SERIAL=1 perf/verify/miri.sh      # pre-nextest serial loop
#
# Workloads in the integration tests are scaled down under miri via
# `cfg!(miri)` (same code paths, fewer repetitions) — see the test sources.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

TIMEOUT_SECS=${MIRI_TIMEOUT_SECS:-1800}
JOBS=${MIRI_JOBS:-8}
FILTERS=("$@")

# ── 1. reap stale miri processes (patterns avoid matching this script;
#       matches both the serial `miri test` and the `miri nextest run`
#       invocations plus the spawned interpreter processes under
#       target/miri/. pgrep may be absent (git bash) — degrade to a no-op.)
stale=$(pgrep -f 'cargo [m]iri |[t]arget/miri/' 2>/dev/null || true)
if [[ -n "$stale" ]]; then
    echo "==> killing stale miri process(es): $(echo $stale | tr '\n' ' ')"
    kill -9 $stale 2>/dev/null || true
    sleep 1
fi

export MIRIFLAGS=${MIRIFLAGS:--Zmiri-tree-borrows -Zmiri-ignore-leaks}

# ── 2. target selection; the default list covers lib + doctests + every
#       miri-meaningful integration binary. Three binaries are deliberately
#       absent (unchanged from the serial era — see testing.md's census):
#         - hybrid_assist: its only test is #[cfg_attr(miri, ignore)]
#           (24-thread timing stress); the safety angle of assist stays
#           covered by compute_pool's deterministic hybrid-driver test.
#         - profile_guard: asserts native debug-assertions config — not
#           unsafe-adjacent, meaningless under the interpreter.
#         - ordered_window: the B6/P-1 repro shapes are size-pinned (n > 1 Mi
#           IS the 1 Mi-window-clamp bug boundary); scaling vacates the
#           assertion and unscaled n costs hours interpreted. The window
#           bookkeeping itself has lib unit tests (miri-covered via lib).
#         - stream_reduce_alloc: the counter's signal needs N ≥ 1<<17
#           (≥1 MiB output allocation) and the streaming topology measures
#           ~33 ms/item-pass under miri — hours for 5 passes. Its
#           delegating-only GlobalAlloc shim class is miri-covered via
#           expand_alloc / pipe_range_alloc / submit_batch_alloc.
#       Override with MIRI_TARGETS="lib compute_pool ...".
#       Valid names: lib, doc, or any --test target.
if [[ -n ${MIRI_TARGETS:-} ]]; then
    TARGETS=($MIRI_TARGETS)
else
    TARGETS=(lib doc handoff_channel scope_integration
             submit_batch_alloc filter_collect_panic pipe_range_alloc
             on_pool_nested pool_cache stream_reduce expand_alloc
             cancel_outcome async_waker_alloc sharded_terminal
             batch_recv graph_stream_diff graph_dag sharded_terminal_async
             sharded_terminal_async_one_worker compute_pool pipeline_integration)
fi

has() { # is target $1 in TARGETS[]?
    local t
    for t in "${TARGETS[@]}"; do [[ $t == "$1" ]] && return 0; done
    return 1
}

overall=0

# ── 3. serial fallback (MIRI_SERIAL=1) — the pre-nextest loop, one
#       `cargo miri test` per target under its own `timeout` wrapper.
#       Kept for hosts/toolchains where `cargo miri nextest` is unavailable
#       and for bisecting a nextest-specific reporting difference.
run_serial() {
    local t
    for t in "${TARGETS[@]}"; do
        local sel
        sel=$( [[ $t == doc || $t == lib ]] && echo "--$t" || echo "--test $t" )
        echo "==> cargo miri test $sel ${FILTERS[*]:-}"
        local start=$SECONDS
        # shellcheck disable=SC2086
        if ! timeout "$TIMEOUT_SECS" cargo miri test $sel -- ${FILTERS[@]+"${FILTERS[@]}"}; then
            echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): $t" >&2
            overall=1
        fi
        echo "==> $t done in $((SECONDS - start))s"
    done
}

# ── 4. default parallel path: `cargo miri nextest run` (cargo-miri accepts
#       nextest flags verbatim). Timeout semantics mapping:
#         serial:  one `timeout $MIRI_TIMEOUT_SECS` per BINARY
#         nextest: one `timeout $MIRI_TIMEOUT_SECS` per GROUP below, and a
#       group is either a single binary (lib, pipeline_integration — exact
#       serial equivalence) or a bundle of binaries whose full serial sum
#       sits far below one timeout budget (per testing.md's census), so no
#       legitimate run can be truncated. A wedged test inside a group no
#       longer blocks its group-mates (per-test processes), so the deadline
#       only matters when the group as a whole wedges — the same condition
#       that killed the serial run. nextest 0.9.92 has NO per-test hard
#       timeout (profile `timeout`/`timeouts.*` keys are silently ignored —
#       verified empirically); revisit this mapping once a newer nextest
#       ships real per-test timeouts.
#       Per-test interpreter startup (~1 s) is the price of process
#       isolation; at -j8 it is amortized by the parallelism win.
#       `--no-fail-fast` mirrors the serial loop's continue-on-failure.
#       Doctests cannot run under nextest (known limitation) — `doc` is
#       diverted to `cargo miri test --doc` in run_doc.
run_nextest_group() { # group-label target-names...
    local label=$1; shift
    local args=() t
    for t in "$@"; do
        case "$t" in
            lib) args+=(--lib) ;;
            *)   args+=(--test "$t") ;;
        esac
    done
    echo "==> cargo miri nextest run ${args[*]} -j $JOBS --no-fail-fast ${FILTERS[*]:-}"
    local start=$SECONDS
    if ! timeout "$TIMEOUT_SECS" \
         cargo miri nextest run "${args[@]}" -j "$JOBS" --no-fail-fast --no-tests warn -- ${FILTERS[@]+"${FILTERS[@]}"}; then
        echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): $label" >&2
        overall=1
    fi
    echo "==> $label done in $((SECONDS - start))s"
}

run_doc() { # doctests: nextest does not run them — serial libtest, per-binary timeout
    echo "==> cargo miri test --doc ${FILTERS[*]:-}"
    local start=$SECONDS
    if ! timeout "$TIMEOUT_SECS" cargo miri test --doc -- ${FILTERS[@]+"${FILTERS[@]}"}; then
        echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): doc" >&2
        overall=1
    fi
    echo "==> doc done in $((SECONDS - start))s"
}

if [[ ${MIRI_SERIAL:-0} == 1 ]]; then
    run_serial
else
    # Split TARGETS into nextest groups. Default TARGETS get the curated
    # groups below (light / medium / single-test-heavies / async-heavy /
    # lib / pipeline_integration); a caller-supplied MIRI_TARGETS list runs
    # as ONE nextest invocation (the caller picked a subset; doc is still
    # diverted to the libtest path and filtered out of the group).
    if [[ -z ${MIRI_TARGETS:-} ]]; then
        run_nextest_group "light" \
            handoff_channel scope_integration submit_batch_alloc \
            filter_collect_panic pipe_range_alloc on_pool_nested \
            pool_cache stream_reduce expand_alloc
        run_nextest_group "medium" \
            cancel_outcome batch_recv sharded_terminal \
            graph_stream_diff graph_dag
        run_nextest_group "heavy" async_waker_alloc compute_pool
        run_nextest_group "async" sharded_terminal_async sharded_terminal_async_one_worker
        run_nextest_group "lib" lib
        run_nextest_group "pipeline" pipeline_integration
    else
        nextest_targets=()
        for t in "${TARGETS[@]}"; do
            [[ $t == doc ]] && continue
            nextest_targets+=("$t")
        done
        if [[ ${#nextest_targets[@]} -gt 0 ]]; then
            run_nextest_group "targets" "${nextest_targets[@]}"
        fi
    fi
    has doc && run_doc
fi

# ── 5. vendored crossfire's lib tests: the design-C waker protocol under
#       tree-borrows. Not a workspace default member, so it needs an
#       explicit -p. Its registry mutex routes to std::sync under cfg(miri)
#       (parking_lot's Windows futex path is not miri-interpretable), so
#       this runs on every platform. Kept an independent block (same as
#       the serial era); the youpipe-side groups above do not mix it in.
#       MIRI_SERIAL=1 swaps in the libtest invocation for the same reason
#       as the youpipe-side targets.
start=$SECONDS
if [[ ${MIRI_SERIAL:-0} == 1 ]]; then
    echo "==> cargo miri test -p youpipe-crossfire --lib ${FILTERS[*]:-}"
    if ! timeout "$TIMEOUT_SECS" \
         cargo miri test -p youpipe-crossfire --lib -- ${FILTERS[@]+"${FILTERS[@]}"}; then
        echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): youpipe-crossfire lib" >&2
        overall=1
    fi
else
    echo "==> cargo miri nextest run -p youpipe-crossfire --lib -j $JOBS --no-fail-fast ${FILTERS[*]:-}"
    if ! timeout "$TIMEOUT_SECS" \
         cargo miri nextest run -p youpipe-crossfire --lib -j "$JOBS" \
         --no-fail-fast --no-tests warn -- ${FILTERS[@]+"${FILTERS[@]}"}; then
        echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): youpipe-crossfire lib" >&2
        overall=1
    fi
fi
echo "==> youpipe-crossfire lib done in $((SECONDS - start))s"

exit $overall
