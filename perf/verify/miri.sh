#!/usr/bin/env bash
# Miri runner that engineers out the two operational hazards seen in the
# 2026-09 rounds:
#
#   1. stale/hung miri processes left in the background silently pin a core
#      at 100% and pollute every later benchmark (the round-2 war story:
#      "every bench run must ps-aux-check for miri first"). Killed up front,
#      with a self-match-safe pattern.
#   2. a single wedged test hanging the whole run for hours. Every test
#      binary runs under `timeout` and the run proceeds to the next one.
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
#   MIRI_TARGETS="lib pipeline_integration" perf/verify/miri.sh
#
# Workloads in the integration tests are scaled down under miri via
# `cfg!(miri)` (same code paths, fewer repetitions) — see the test sources.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

TIMEOUT_SECS=${MIRI_TIMEOUT_SECS:-1800}

# ── 1. reap stale miri processes (patterns avoid matching this script)
stale=$(pgrep -f 'cargo [m]iri test|[t]arget/miri/' 2>/dev/null || true)
if [[ -n "$stale" ]]; then
    echo "==> killing stale miri process(es): $(echo $stale | tr '\n' ' ')"
    kill -9 $stale 2>/dev/null || true
    sleep 1
fi

export MIRIFLAGS=${MIRIFLAGS:--Zmiri-tree-borrows -Zmiri-ignore-leaks}

# ── 2. per-binary timeout; the default target list covers lib + doctests +
#       every miri-meaningful integration binary, ordered cheap→heavy so a
#       fresh regression surfaces early (per-target durations: testing.md
#       "Suite duration census"). Two binaries are deliberately absent:
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
             batch_recv graph_dag sharded_terminal_async
             compute_pool pipeline_integration)
fi

run_target() { # name -> cargo args on stdout
    case "$1" in
        lib) echo "--lib" ;;
        doc) echo "--doc" ;;
        *)   echo "--test $1" ;;
    esac
}

overall=0
for t in "${TARGETS[@]}"; do
    echo "==> cargo miri test $(run_target "$t") $*"
    start=$SECONDS
    # shellcheck disable=SC2086
    if ! timeout "$TIMEOUT_SECS" cargo miri test $(run_target "$t") -- "$@"; then
        echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): $t" >&2
        overall=1
    fi
    echo "==> $t done in $((SECONDS - start))s"
done

# ── 3. vendored crossfire's lib tests: the design-C waker protocol under
#       tree-borrows. Not a workspace default member, so it needs an
#       explicit -p. Its registry mutex routes to std::sync under cfg(miri)
#       (parking_lot's Windows futex path is not miri-interpretable), so
#       this runs on every platform.
echo "==> cargo miri test -p youpipe-crossfire --lib $*"
start=$SECONDS
# shellcheck disable=SC2086
if ! timeout "$TIMEOUT_SECS" cargo miri test -p youpipe-crossfire --lib -- "$@"; then
    echo "==> FAILED (or timed out after ${TIMEOUT_SECS}s): youpipe-crossfire lib" >&2
    overall=1
fi
echo "==> youpipe-crossfire lib done in $((SECONDS - start))s"

exit $overall
