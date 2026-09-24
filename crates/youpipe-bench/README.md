# youpipe-bench

Opt-in lab benches for maintainers — everything that needs heavyweight deps
(perfcnt, zstd, aes-gcm) or a non-criterion shape, collected in ONE crate so
the workspace doesn't carry one package per bench. Not in `default-members`:
plain `cargo build/test/clippy` at the workspace root never compiles any of
this; every target is selected explicitly with `-p youpipe-bench`.

Methodology, recorded results and lessons live in
[docs/src/dev/benchmarks.md](../../docs/src/dev/benchmarks.md); the
criterion micro-suite (sync_vs_rayon, mixed_load, io_async, horizontal, …)
stays in [crates/youpipe/benches](../youpipe/benches).

## Targets

| Target | Kind | What it does |
|---|---|---|
| `--bench perf_events` | criterion | Runs the CPU fused benches driven by Linux hardware perf counters (instructions / cycles / ref-cycles / cache-misses / …) instead of wall time, via `youpipe-criterion-perf-counters`. Use it for work metrics (instr/elem, IPC) and as the cheapest "this change should touch nothing" check. |
| `--bin file-encrypt` | one-shot app | Real-disk mixed CPU/IO: read skewed-size files (log-uniform 8 KiB..8 MiB), zstd + AES-256-GCM, write back with fsync. youpipe (tuned 3-stage stream) vs rayon `par_iter` vs tokio spawn-per-file. Point `FC_DATA_DIR` at real storage — on tmpfs there is no blocking IO to pipeline. Recorded results: [results-file-encrypt.txt](results-file-encrypt.txt). |
| `--bin hotpath-profile --features hotpath` | one-shot profiler | Drives youpipe's permanent `#[hotpath::measure]` probes under a `HotpathGuard` — per-function timing/alloc/CPU percentiles without `perf`. The `hotpath` feature is required (it instruments all of youpipe), which is exactly why it is not on by default in this crate. |

## Usage

```sh
# perf-event counters (Linux, kernel.perf_event_paranoid >= 2 works: user-space-only)
cargo bench -p youpipe-bench --bench perf_events
PERF_EVENT=ref-cycles cargo bench -p youpipe-bench --bench perf_events

# real-disk file encrypt (see module docs of the binary for FC_* knobs)
FC_DATA_DIR=/var/tmp/feb cargo run --release -p youpipe-bench --bin file-encrypt

# hotpath profiling (sweep, focused, or stream scenario)
cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath
HOTPATH_OUTPUT_FORMAT=json-pretty HOTPATH_OUTPUT_PATH=target/hotpath-report.json \
  cargo run --release -p youpipe-bench --bin hotpath-profile --features hotpath -- 1000000 light 20

# counter-stability drift experiment (docs: benchmarks.md "perf-event counters")
crates/youpipe-bench/run-drift-exp.sh
```

## History

This crate absorbed (2026-10) what used to be four separate bench crates —
`youpipe-bench-counter`, `youpipe-bench-file-encrypt`,
`youpipe-bench-hotpath-profile`, `youpipe-bench-pipeline`. The fourth
(`pipeline-bench`, simulated-IO 5-strategy document pipeline) was deleted
outright: its scenario was already adapted into the main suite's
`benches/horizontal` `real_doc` row (interleaved rounds, better methodology),
and its "youpipe all-sync 3 stages" row is moot since pure-sync `stream`
chains now execute on the fused core. Deterministic instruction-count
benches moved out to [`youpipe-gungraun`](../youpipe-gungraun).
