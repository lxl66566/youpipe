#!/usr/bin/env bash
# Bounded reproduction stakeout for the intermittent pipeline hang (todo #14):
# run one test binary in a loop, fresh process per pass, and capture forensics
# BEFORE killing anything when a pass trips the watchdog.
#
# Why this exists: the 2026-09-28 occurrence (pipeline_integration, 107
# threads in futex wait, 0 CPU for 13 min, under the default in-binary
# parallel harness) and the 2026-10-22 graph-side twin were both killed
# without a stack dump, costing the root-cause investigation its primary
# evidence. The next sighting must not repeat that: this script encodes the
# trip policy and the forensics order (gdb first, kill last).
#
# Trip policy: wall > watchdog (default max(20s, 5x the max of 3 calibration
# passes; the functional test binaries here run 1-3 s, so 3x-of-max floors
# at 120 s and would mask slow-mode anomalies) -> sample /proc/<pid>/stat
# utime+stime, sleep 3, sample again:
#   * 0 ticks advanced  => 0-CPU hang signature  -> full forensics + SIGKILL
#   * ticks advanced    => busy (livelock/slow)  -> allowed 2 more watchdog
#     windows, then forensics + SIGKILL anyway
# Forensics set per trip: gdb `thread apply all bt` (build the binary with
# debuginfo — release has none by default: CARGO_PROFILE_RELEASE_DEBUG=true),
# /proc/<pid>/status, per-task state+wchan, CPU-delta note. Everything lands
# in forensics_<label>_<run>/ next to the TSV.
#
# Usage:
#   # release binary with symbols:
#   CARGO_PROFILE_RELEASE_DEBUG=true cargo test -p youpipe --release \
#       --test pipeline_integration --no-run
#   perf/verify/hang-stakeout.sh <path-to-test-binary> [label]
#   MINUTES=45 WATCHDOG_SECS=20 TEST_THREADS=32 perf/verify/hang-stakeout.sh ...
#
# The binary runs with the harness defaults (TEST_THREADS overrides
# --test-threads). Output: <outdir>/runs.tsv (run label start wall_s rc trip
# note), run_<label>_<n>.out per pass, forensics_* on trips, summary on
# stdout. Run it under /tmp/youpipe-locks/bench.lock — it is CPU-heavy.
set -u
cd "$(git rev-parse --show-toplevel)"

BIN=${1:?usage: hang-stakeout.sh <test-binary> [label]}
LABEL=${2:-$(basename "$BIN" | cut -c1-12)}
MINUTES=${MINUTES:-30}
WATCHDOG_SECS=${WATCHDOG_SECS:-0} # 0 = auto (max(20, 5x calib max))
TEST_THREADS=${TEST_THREADS:-}
OUTDIR=${OUTDIR:-target/hang-stakeout/${LABEL}-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUTDIR"
TSV="$OUTDIR/runs.tsv"
echo -e "run\tlabel\tstart\twall_s\trc\ttrip\tnote" > "$TSV"
START=$(date +%s)
DEADLINE=$(( START + MINUTES*60 ))
THREAD_ARGS=()
[ -n "$TEST_THREADS" ] && THREAD_ARGS=(--test-threads "$TEST_THREADS")

calibrate() { # 3 passes -> max wall seconds on stdout
  local max=0 s e w rc
  for _ in 1 2 3; do
    s=$(date +%s); "$BIN" "${THREAD_ARGS[@]}" >/dev/null 2>&1; rc=$?; e=$(date +%s); w=$((e-s))
    echo "calib: ${w}s rc=$rc" >&2
    [ "$w" -gt "$max" ] && max=$w
  done
  echo "$max"
}
if [ "$WATCHDOG_SECS" -eq 0 ]; then
  CAL_MAX=$(calibrate)
  WATCHDOG_SECS=$(( CAL_MAX*5 )); [ "$WATCHDOG_SECS" -lt 20 ] && WATCHDOG_SECS=20
fi
echo "stakeout $(date -Is): bin=$BIN label=$LABEL watchdog=${WATCHDOG_SECS}s deadline=$(date -d "@$DEADLINE" +%H:%M:%S) out=$OUTDIR" | tee -a "$TSV.note"

run_one() { # $1=runidx
  local idx=$1 out s e now start_s pid rc trip=0 ext=0 c1 c2 d D
  out="$OUTDIR/run_${LABEL}_$idx.out"
  s=$(date +%s%N); start_s=$(( s/1000000000 ))
  "$BIN" "${THREAD_ARGS[@]}" > "$out" 2>&1 &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    now=$(date +%s)
    if [ $(( now - start_s )) -ge $(( WATCHDOG_SECS + ext*WATCHDOG_SECS )) ]; then
      # CPU-advance probe: utime+stime ticks = fields 14+15 of /proc/pid/stat
      c1=$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null)
      sleep 3
      c2=$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null)
      d=$(( ${c2:-0} - ${c1:-1} ))
      if [ "$d" -eq 0 ] || [ "$ext" -ge 2 ]; then
        trip=$(( d==0 ? 1 : 2 ))
        D="$OUTDIR/forensics_${LABEL}_$idx"
        mkdir -p "$D"
        gdb -p "$pid" -batch -ex 'set pagination off' -ex 'thread apply all bt' \
          > "$D/gdb_bt.txt" 2>&1
        cp "/proc/$pid/status" "$D/status.txt" 2>/dev/null
        : > "$D/task_states.txt"
        for t in /proc/$pid/task/*; do
          echo "$(basename "$t") state=$(cut -d' ' -f3 "$t/stat") wchan=$(cat "$t/wchan" 2>/dev/null)" >> "$D/task_states.txt"
        done
        echo "cpudelta_ticks=$d ext=$ext" >> "$D/task_states.txt"
        kill -KILL "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null; rc=$?
        e=$(date +%s%N)
        echo -e "$idx\t$LABEL\t$(date -d "@$start_s" +%H:%M:%S)\t$(( (e-s)/1000000000 ))\t$rc\t$trip\tcpudelta=$d" >> "$TSV"
        return 0
      fi
      ext=$((ext+1))
    fi
    sleep 2
  done
  wait "$pid"; rc=$?
  e=$(date +%s%N)
  echo -e "$idx\t$LABEL\t$(date -d "@$start_s" +%H:%M:%S)\t$(( (e-s)/1000000000 )).$(( ((e-s)/1000000)%1000 ))\t$rc\t0\t" >> "$TSV"
}

idx=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  idx=$((idx+1))
  run_one "$idx"
done
echo "=== end $(date -Is): runs=$idx wall=$(( $(date +%s) - START ))s over by design ==="
echo "-- per-label walls (s) --"
awk -F'\t' 'NR>1 && $4+0>0 {n[$2]++; if($4+0>mx[$2])mx[$2]=$4+0; if(mn[$2]==""||$4+0<mn[$2])mn[$2]=$4+0} END{for(k in n) printf "%s: n=%d min=%s max=%s\n",k,n[k],mn[k],mx[k]}' "$TSV"
echo "-- trips (trip=1 zero-CPU hang, 2 busy-killed) --"
awk -F'\t' 'NR>1 && ($6==1||$6==2)' "$TSV"
awk -F'\t' 'NR>1 && ($6==1||$6==2){c++} END{print "trip rows: " c+0}' "$TSV"
