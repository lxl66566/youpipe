#!/usr/bin/env bash
# Drift experiment for the perf-event counters (see docs/benchmarks.md
# "perf-event counters"): run the perf_events bench N times per measurement
# kind — walltime plus the hardware events — each run in an isolated
# CRITERION_HOME, then let summarize-drift.py compare cross-run drift
# (stability across invocations) against within-run precision.
#
# Reproduces the methodology A/B: identical bench code, identical sampling
# budget, identical CPU pinning; only the Measurement differs.
#
# Env knobs: RUNS (default 3), SAMPLES (default 20), CPUS (default 1-31,
# i.e. bench_ab.sh's "all cores but core 0"), EVENTS, OUT.
set -euo pipefail
cd "$(dirname "$0")"

RUNS=${RUNS:-3}
SAMPLES=${SAMPLES:-20}
CPUS=${CPUS:-1-31}
EVENTS=${EVENTS:-"walltime instructions cycles ref-cycles cache-misses"}
OUT=${OUT:-/tmp/counter-bench-drift}

for event in $EVENTS; do
    for run in $(seq 1 "$RUNS"); do
        home="$OUT/$event-r$run"
        echo "=== $event run $run (CRITERION_HOME=$home)"
        PERF_EVENT=$event \
        BENCH_SAMPLE_SIZE=$SAMPLES BENCH_WARMUP_MS=1000 BENCH_MEASUREMENT_MS=2000 \
        CRITERION_HOME=$home \
        taskset -c "$CPUS" cargo bench --bench perf_events --quiet
    done
done

python3 summarize-drift.py "$OUT" "$RUNS"
