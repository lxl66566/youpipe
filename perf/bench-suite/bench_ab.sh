#!/usr/bin/env bash
# Interleaved (ABAB… / ABCABC…) criterion A/B runner with CPU pinning.
#
# Why this exists (hard-won lessons, see docs/src/dev/benchmarks.md "measurement
# traps"): back-to-back full-group passes have ±10% inter-run drift on this
# machine, and `target/criterion` accumulates stale baseline subdirs that
# produce phantom regressions in naive diff scripts. This runner instead:
#
#   * materializes each side once per outdir (git worktree, or a snapshot of
#     the working tree for `wt`) and KEEPS it — appending rounds to the same
#     outdir later reuses the compiled binaries (the working tree is never
#     touched mid-run); cleaning up is `rm -rf <outdir> && git worktree
#     prune`
#   * interleaves sides within every round and ALTERNATES which side goes
#     first on odd rounds, so slow drift cancels instead of biasing one side
#   * pins every bench process to the same CPU set (default: all cores but
#     core 0, which is left to the OS/IRQs) — identical on both sides
#   * forces the SAME criterion sampling budget via CLI flags on every
#     invocation. CLI flags override whatever the bench files program on
#     ANY revision (criterion_group! calls configure_from_args), which
#     locks fairness even when side A is an old commit with different
#     in-file defaults
#   * gives every (round, side) its own CRITERION_HOME so aggregates only
#     ever read fresh `new/estimates.json` files — no stale-base reads
#
# Memory lesson (2026-09-29, found the hard way): this box runs earlyoom
# (-m5,3; avoid-list covers only sshd/systemd/journald) with 14 GB RAM, and
# the default 32-way release `cargo bench --no-run` of a fresh worktree
# drove it below the threshold — earlyoom SIGKILLed the compile silently
# (no kernel OOM line, no cargo error in this script's log; the run just
# vanished at the "side X: cargo bench --no-run" line, twice). If a run
# dies there, export CARGO_BUILD_JOBS=6 (or similar) and pre-build both
# worktrees before re-running — the materialized worktrees are reused, so
# the pre-build carries over and the script skips straight to the rounds.
#
# Afterwards `compare.py <outdir>` aggregates per-id medians across rounds.
#
# Usage:
#   perf/bench-suite/bench_ab.sh -a <rev> -b <rev> [-c <rev>] [-d <rev>] [options] [filter...]
#
#   rev       git rev-ish, or `wt` for a snapshot of the current working
#             tree (uncommitted changes allowed — snapshotted up front),
#             optionally LABEL=REV to name the side in the report.
#   filter    criterion regex fragment(s); a bench runs if its id matches.
#             Default: everything.
#
# Options:
#   -r, --rounds N        interleaved rounds (default 3)
#   -t, --taskset CPUS    cpu list passed to taskset (default: 1..N-1)
#   -o, --outdir DIR      output dir (default target/bench-ab/run-<ts>)
#   -B, --bench NAME      only these bench targets (repeatable, default all)
#   -1, --per-id          interleave per bench-id (isolated runs — use for
#                         the 100K fused family); default interleaves per
#                         bench-target pass
#   -s, --samples N       criterion --sample-size (default 20)
#   -w, --warmup-ms N     criterion --warm-up-time (default 1000)
#   -m, --measurement-ms N criterion --measurement-time (default 2000)
#       --force           skip the stray-process guard
#       --sweep           exploratory single-revision sweep (one side is
#                         enough): enumerates every criterion id (--list),
#                         drops the cross-library drift-control benches
#                         (rayon_*/tokio_*/sequential) and the middle sizes
#                         of >2-size families, runs the kept ids, then prints
#                         a per-id median-of-rounds table. NOT for A/B
#                         verdicts — compare.py needs both sides AND the
#                         drift-control ids on them.
#
# Examples:
#   # classic two-sided A/B of everything, 3 interleaved rounds
#   perf/bench-suite/bench_ab.sh -a base=9b31fb0 -b new=HEAD
#
#   # isolate the drift-sensitive ordered-stream family, more rounds
#   perf/bench-suite/bench_ab.sh -a base -b wt -r 5 --per-id \
#       'stream_pipeline/single_stage_ordered' 'with_fence'
#
#
#   # same-binary runtime-knob A/B (per-side env, -E LABEL=VAR=VAL):
#   perf/bench-suite/bench_ab.sh -a off=wt -b on=wt \
#       -E off=YOUPIPE_ONPOOL_HYBRID=0 -E on=YOUPIPE_ONPOOL_HYBRID=1 \
#   # add two more rounds to an existing run (same outdir continues)
#   perf/bench-suite/bench_ab.sh -o target/bench-ab/run-XXX -a ... -b ... -r 2
#
#   # exploratory sweep of one revision (no second side, trimmed id set)
#   perf/bench-suite/bench_ab.sh --sweep -a wt
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

ROUNDS=3
TASKSET_CPUS=""
OUTDIR=""
declare -a BENCH_TARGETS=() FILTERS=() SIDE_REVS=() SIDE_ENVS=()
PER_ID=0
SWEEP=0
SAMPLES=20
WARMUP_MS=1000
MEASURE_MS=2000
FORCE=0

usage() { sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -a|-b|-c|-d) SIDE_REVS+=("$2"); shift 2 ;;
        -r|--rounds) ROUNDS="$2"; shift 2 ;;
        -t|--taskset) TASKSET_CPUS="$2"; shift 2 ;;
        -o|--outdir) OUTDIR="$2"; shift 2 ;;
        -B|--bench) BENCH_TARGETS+=("$2"); shift 2 ;;
        -E|--env) SIDE_ENVS+=("$2"); shift 2 ;;
        -1|--per-id) PER_ID=1; shift ;;
        -s|--samples) SAMPLES="$2"; shift 2 ;;
        -w|--warmup-ms) WARMUP_MS="$2"; shift 2 ;;
        -m|--measurement-ms) MEASURE_MS="$2"; shift 2 ;;
        --force) FORCE=1; shift ;;
        --sweep) SWEEP=1; shift ;;
        -h|--help) usage 0 ;;
        --) shift; FILTERS+=("$@"); break ;;
        -*) echo "unknown option: $1" >&2; usage 1 ;;
        *) FILTERS+=("$1"); shift ;;
    esac
done

if (( SWEEP )); then
    [[ ${#SIDE_REVS[@]} -ge 1 ]] || { echo "--sweep needs at least one -a side" >&2; usage 1; }
else
    [[ ${#SIDE_REVS[@]} -ge 2 ]] || { echo "need at least -a and -b" >&2; usage 1; }
fi

# ── stray-process guard: leftover miri/loitering benches poison every number
if [[ $FORCE -eq 0 ]]; then
    strays=$(pgrep -a -f 'cargo miri|/miri/|criterion-|cargo bench' 2>/dev/null || true)
    if [[ -n "$strays" ]]; then
        echo "REFUSING TO START: stray benchmark-polluting processes found:" >&2
        echo "$strays" >&2
        echo "kill them (or rerun with --force if they are harmless):" >&2
        exit 1
    fi
fi

# ── CPU pinning: keep core 0 free for OS/kernel housekeeping by default
if [[ -z "$TASKSET_CPUS" ]]; then
    n=$(nproc)
    if (( n > 1 )); then TASKSET_CPUS="1-$((n-1))"; else TASKSET_CPUS="0"; fi
fi
echo "==> pinning benches to CPUs $TASKSET_CPUS (taskset)"

# ── output dir; rounds continue numbering across re-runs of the same outdir
OUTDIR=${OUTDIR:-target/bench-ab/run-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUTDIR"
WTROOT="$OUTDIR/worktrees"
mkdir -p "$WTROOT"
echo "==> output: $OUTDIR"

# labels/revs
declare -a LABELS=() REVS=()
for spec in "${SIDE_REVS[@]}"; do
    if [[ "$spec" == *=* ]]; then
        LABELS+=("${spec%%=*}"); REVS+=("${spec#*=}")
    else
        # shellcheck disable=SC2001
        LABELS+=("$(echo "$spec" | sed 's/[^A-Za-z0-9._-]/-/g')"); REVS+=("$spec")
    fi
done

# ── env guard: a CARGO_TARGET_DIR inherited from the launching shell makes
#    every side's `cargo bench --no-run` build into ONE shared dir — the
#    later side's binary overwrites the earlier side's at the same path, and
#    every round then runs the LAST side's binary on both labels (found
#    2026-10-26: an r15 recompile A/B came back with byte-identical behavior
#    on both sides). Each side must build in its own worktree target dir.
unset CARGO_TARGET_DIR

# ── materialize each side exactly once per outdir; refresh on rev change.
# Worktrees persist across runs so appended rounds reuse the build.
materialize() {
    # NOTE: separate `local` statements — bash 5.3 + `set -u` expands the
    # whole `local a=$1 b="$a"` statement before defining `a`.
    local label=$1 rev=$2
    local dir="$WTROOT/$label"
    local stamp="$dir/.bench_ab_rev"
    if [[ -f "$stamp" && "$(cat "$stamp")" == "$rev" && -d "$dir" ]]; then
        echo "==> side $label: reusing $dir ($(git rev-parse --short "$rev" 2>/dev/null || echo "$rev"))"
        return
    fi
    if [[ -d "$dir/.git" ]]; then
        git worktree remove --force "$dir" >/dev/null 2>&1 || rm -rf "$dir"
    else
        rm -rf "$dir"
    fi
    git worktree prune >/dev/null 2>&1 || true
    mkdir -p "$dir"
    if [[ "$rev" == "wt" ]]; then
        # Snapshot the current working tree (tracked files + untracked, minus
        # build artifacts). The tree may keep changing afterwards — the
        # snapshot is what runs, which is the round-2 lesson ("never A/B
        # against a half-finished working tree") made structural.
        echo "==> side $label: snapshotting working tree -> $dir"
        rsync -a --delete \
            --exclude '/target' --exclude '/.git' \
            --exclude '/crates/*/target' \
            ./ "$dir/"
    else
        git rev-parse --verify -q "$rev^{commit}" >/dev/null \
            || { echo "bad rev: $rev" >&2; exit 1; }
        echo "==> side $label: git worktree $rev -> $dir"
        git worktree add --detach "$dir" "$rev" >/dev/null
    fi
    echo "$rev" > "$stamp"
}
for i in "${!LABELS[@]}"; do materialize "${LABELS[$i]}" "${REVS[$i]}"; done

# ── build each side (own target dir per worktree, shared flags) and collect
#    the bench executables via cargo's JSON messages
declare -A SIDE_BINS=()
for i in "${!LABELS[@]}"; do
    label=${LABELS[$i]}; dir="$WTROOT/$label"
    echo "==> side $label: cargo bench --no-run"
    # Build phase under the shared cargo lock (co-workers' protocol): the
    # interleaved ROUNDS below run the binaries directly (no cargo) and must
    # NOT hold the lock for the whole session.
    bins=$(flock -w 3600 -o /tmp/youpipe-locks/cargo.lock -c "cd '$dir' && cargo bench --no-run --message-format=json" 2>/dev/null \
        | python3 -c '
import json, sys
# `${BENCH_TARGETS[*]}` arrives space-separated (bash array join), so split on
# commas AND/OR whitespace — the original comma-only split silently matched
# nothing when more than one -B was passed ("no bench binaries for side ...").
import re as _re
bin_args = [a for a in _re.split(r"[,\s]+", sys.argv[1] if len(sys.argv) > 1 else "") if a]
for line in sys.stdin:
    try: m = json.loads(line)
    except ValueError: continue
    if m.get("reason") != "compiler-artifact": continue
    if "bench" not in (m.get("target") or {}).get("kind", []): continue
    exe = m.get("executable")
    if not exe: continue
    if bin_args and m["target"]["name"] not in bin_args: continue
    print(exe)
' "${BENCH_TARGETS[*]-}")
    [[ -n "$bins" ]] || { echo "no bench binaries for side $label" >&2; exit 1; }
    # Drop non-criterion bench binaries: revisions before channel_bench was
    # registered with harness=false compile it as a libtest target whose
    # getopts CLI rejects criterion flags ("Unrecognized option"). Probe
    # --help once per binary (~50ms) instead of failing every round.
    kept=""
    while read -r bin; do
        if "$bin" --help 2>&1 | grep -q -- '--sample-size'; then
            kept+="$bin"$'\n'
        else
            echo "    note: $(basename "$bin" | sed 's/-[0-9a-f]*$//') is not a criterion binary on this revision — skipping"
        fi
    done <<< "$bins"
    SIDE_BINS[$label]=$kept
done

# ── sweep mode: build the exploratory id list. Criterion's filter is a
#    positive regex only, so enumerate each binary's ids once (--list, no
#    measurement) and trim in python:
#      * drift-control benches (rayon_*/tokio_*/sequential) exist to catch
#        cross-library drift BETWEEN the sides of a verdict A/B — a
#        single-sided sweep cannot read that signal
#      * families with >2 numeric sizes keep their two anchors; the dropped
#        midpoints have interpolated monotonously in every scan to date
#    (A/B mode is unaffected: it never enters this block.)
if (( SWEEP )); then
    [[ ${#FILTERS[@]} -eq 0 ]] \
        || { echo "--sweep computes its own id list; drop the explicit filters" >&2; exit 1; }
    all_ids=$(
        for i in "${!LABELS[@]}"; do
            while read -r bin; do
                [[ -n "$bin" ]] || continue
                "$bin" --list 2>/dev/null | sed -n 's/: benchmark$//p'
            done <<< "${SIDE_BINS[${LABELS[$i]}]}"
        done | sort -u
    )
    [[ -n "$all_ids" ]] || { echo "--sweep: criterion --list returned no ids" >&2; exit 1; }
    kept=$(printf '%s\n' "$all_ids" | python3 -c '
import sys

# id = group/bench[/size]. Drift-control names live in the bench segment;
# a 2-segment id has no size axis.
DRIFT = ("rayon_", "tokio_", "sequential")

fams: dict[str, list[str]] = {}
for line in sys.stdin:
    ident = line.strip()
    if not ident:
        continue
    seg = ident.split("/")
    bench = seg[-2] if len(seg) >= 3 else seg[-1]
    if bench.startswith(DRIFT):
        continue
    fams.setdefault("/".join(seg[:-1]) if len(seg) >= 3 else ident, []).append(ident)

kept: list[str] = []
for _, members in sorted(fams.items()):
    def sz(m: str):
        tail = m.rsplit("/", 1)[1]
        return int(tail) if tail.isdigit() else None
    unnumbered = [m for m in members if sz(m) is None]
    numbered = [m for m in members if sz(m) is not None]
    kept += unnumbered
    sizes = sorted({sz(m) for m in numbered})
    keep = {sizes[0], sizes[-1]} if len(sizes) > 2 else None
    kept += [m for m in numbered if keep is None or sz(m) in keep]
print("\n".join(kept))
')
    mapfile -t FILTERS <<< "$kept"
    echo "==> sweep: ${#FILTERS[@]} of $(printf '%s\n' "$all_ids" | wc -l) ids kept" \
         "(drift-control benches + family midpoints dropped)"
fi

# combined criterion filter (alternation) — criterion matches it against the
# full "group/bench/size" id
if [[ ${#FILTERS[@]} -eq 0 ]]; then
    COMBINED='.*'
else
    COMBINED=$(IFS='|'; echo "(${FILTERS[*]})")
fi
if (( PER_ID )) && [[ ${#FILTERS[@]} -eq 0 ]]; then
    echo "WARNING: -1 without explicit filters degrades to ONE combined pass per" >&2
    echo "         round (criterion cannot enumerate ids); pass the id list for true" >&2
    echo "         per-id isolation — the 100K fused family requires it." >&2
fi

run_one() { # label bin round filter
    local label=$1 bin=$2 round=$3 filter=$4
    local home="$OUTDIR/round-$round/$label"
    # see materialize() note: keep `$label` uses out of the same local stmt
    mkdir -p "$home" "$OUTDIR/logs"
    # criterion 0.8 parses durations as plain seconds (float), and a
    # standalone bench binary needs the explicit `--bench` to leave its
    # test mode; the forced flags are the fairness lock — they override
    # whatever any revision's bench files program (configure_from_args).
    local warmup measure
    warmup=$(awk "BEGIN{printf \"%.4g\", $WARMUP_MS/1000}")
    measure=$(awk "BEGIN{printf \"%.4g\", $MEASURE_MS/1000}")
    local log="$OUTDIR/logs/r$round-$label-$(basename "$bin" | sed 's/-[0-9a-f]*$//').log"
    echo "    [$label] r$round $filter"
    # Per-side env overrides (-E LABEL=VAR=VAL, repeatable): the canonical
    # way to A/B a runtime knob in the SAME binary — recompiles swing tight
    # benchmarks by tens of percent through pure code-layout shifts. Pair
    # with two labels over the same rev, e.g.
    #   -a off=wt -b on=wt -E off=YOUPIPE_X=0 -E on=YOUPIPE_X=1
    local -a envs=()
    local spec
    for spec in "${SIDE_ENVS[@]}"; do
        if [[ "${spec%%=*}" == "$label" ]]; then
            envs+=("${spec#*=}")
        fi
    done
    CRITERION_HOME="$home" \
        env "${envs[@]}" taskset -c "$TASKSET_CPUS" \
        "$bin" --bench \
               --sample-size "$SAMPLES" \
               --warm-up-time "$warmup" \
               --measurement-time "$measure" \
               --noplot "$filter" > "$log" 2>&1 \
        || { echo "    [$label] r$round $filter FAILED (see $log)" >&2; return 1; }
}

# round numbering continues across re-runs into the same outdir
first_round=$(( $(ls -1d "$OUTDIR"/round-* 2>/dev/null \
    | sed 's/.*round-//' | sort -n | tail -1 || echo 0) + 1 ))

echo "==> running $ROUNDS interleaved rounds, sides: ${LABELS[*]}"
fail=0
for (( r=0; r<ROUNDS; r++ )); do
    round=$((first_round + r))
    # alternate which side goes first each round (cancels position bias).
    # NOTE: plain `for x in $order` expands only ${order[0]} for arrays —
    # both branches build a newline-separated STRING so word splitting sees
    # every index (a bash footgun that silently skipped the second side on
    # even rounds).
    if (( round % 2 == 0 )); then
        order=$(printf '%s\n' "${!LABELS[@]}")
    else
        order=$(for ((j = ${#LABELS[@]} - 1; j >= 0; j--)); do echo "$j"; done)
    fi
    for idx in $order; do
        label=${LABELS[$idx]}
        while read -r bin; do
            [[ -n "$bin" ]] || continue # trailing-newline artifact
            bname=$(basename "$bin" | sed 's/-[0-9a-f]*$//')
            if (( PER_ID )); then
                # isolated per-id: every filter gets its own criterion run
                # (non-matching binaries exit in ~50ms). This is the mode the
                # 100K-fused family needs — see benchmarks.md.
                # NOTE: the default must NOT be quoted — `${FILTERS[@]:-'.*'}`
                # passes the literal string `'.*'` (quotes included), which
                # matches no bench id and silently runs a no-op A/B (found
                # 2026-07: three "green" rounds with empty compare tables).
                for f in "${FILTERS[@]:-.*}"; do
                    run_one "$label" "$bin" "$round" "$f" || fail=1
                done
            else
                run_one "$label" "$bin" "$round" "$COMBINED" || fail=1
            fi
        done <<< "${SIDE_BINS[$label]}"
    done
    echo "==> round $round done"
done

echo
if (( SWEEP )); then
    echo "==> sweep medians (per id: median of the round medians):"
    python3 - "$OUTDIR" <<'PYREADOUT'
import statistics
import sys
from pathlib import Path

sys.path.insert(0, "perf/bench-suite")
from compare import fmt_ns, load_side

outdir = Path(sys.argv[1])
rounds = sorted((d for d in outdir.glob("round-*") if d.is_dir()),
                key=lambda d: int(d.name.removeprefix("round-")))
label = next((c.name for rd in rounds for c in sorted(rd.iterdir()) if c.is_dir()), None)
if label is None:
    sys.exit(f"no round data under {outdir}")
side = load_side(rounds, label)
print(f"| id ({label}) | median | spread % |")
print("|---|---|---|")
for ident in sorted(side):
    vals = [side[ident][k] for k in sorted(side[ident])]
    med = statistics.median(vals)
    spread = (max(vals) - min(vals)) / med * 100 if len(vals) > 1 else 0.0
    print(f"| {ident} | {fmt_ns(med)} | {spread:.1f} |")
PYREADOUT
else
    echo "==> done. aggregate with:"
    echo "    python3 perf/bench-suite/compare.py $OUTDIR"
fi
exit $fail
