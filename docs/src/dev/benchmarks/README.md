# Benchmarks and Methodology

> All numbers are from a 32-core AMD (Zen) Linux machine, criterion `--sample-size 30 --measurement-time 5` (the historical full-treatment config, see below), tables refreshed with the current quick config.

Caliber note: comparisons against rayon borrow the input on both sides (`pipe_ref(&data)` vs `par_iter`), the same warm slice, nothing cloned or freed inside the timed region, so no lifecycle-alignment tricks are needed. The engines that must take ownership (`pipe(v)`, `stream(v)`) rebuild the input in the untimed setup and pull it into cache (`warm_clone`): glibc's large `memcpy` uses non-temporal stores that bypass the cache, so a naive `data.clone()` arrives cold-from-RAM and measures allocator and memory latency instead of the framework. The `_owned_cold` variant in the lightweight group documents that one-shot cold cost.

## Suite budget (quick config)

A full-suite pass with criterion's defaults costs over an hour. All bench targets share `crates/youpipe/benches/common/mod.rs`, which defaults to the verdict-proven interleaved-A/B regime, 20 samples, 1 s warm-up, 2 s measurement, about 3.5 s per bench id and a full suite of about 10 min including compile, and shrinks each group's size axis to its two anchors (the dropped midpoints interpolate monotonically, see the group comments for specifics). Every knob is env-overridable without code edits, and the override applies to both sides of any A/B equally, so fairness is preserved:

```sh
# full-treatment config for a single deep-dive bench
BENCH_SAMPLE_SIZE=100 BENCH_WARMUP_MS=3000 BENCH_MEASUREMENT_MS=5000 \
    cargo bench --bench sync_vs_rayon -- youpipe_par_map/1000
```

For A/B verdicts use `perf/bench-suite` (interleaved rounds, CPU pinning, median-of-rounds comparison) rather than back-to-back full-group passes: the stream family has ±10 % inter-run drift, and the 100 K fused family is subject to the whole-group measurement trap documented in [attribution.md](attribution.md).

### Exploratory sweeps (`--sweep`)

When the goal is scanning one revision for hot ids rather than a verdict, part of the full 273-id grid is ballast: the rayon/tokio/sequential drift-control ids (24 bench call sites across the grid; `sync_vs_rayon` + `unbalanced` hold 49 % of all ids and roughly a third of those two are drift anchors) exist to catch cross-library drift *between the two sides of an A/B* — a single-sided sweep cannot read that signal — and >2-size families interpolate monotonously between their anchors. `bench_ab.sh --sweep -a wt` runs one side over the trimmed core (criterion `--list` + a python filter; ~220 of 273 ids, ~3 min off the ~16 min criterion pass) and prints a per-id median-of-rounds table at the end:

```sh
perf/bench-suite/bench_ab.sh --sweep -a wt              # whole trimmed grid
perf/bench-suite/bench_ab.sh --sweep -a wt -r 1 -B sync_vs_rayon   # one binary, one round
```

What a sweep loses by design: cross-library drift visibility (no rayon/tokio anchors on any side) and the dropped midpoints of the few >2-size families. Never use `--sweep` for verdicts — `compare.py` needs both sides and the drift-control ids on them; the flag refuses explicit filters to keep the two modes from being mixed.

## Interleaved A/B tooling (`perf/bench-suite`)

`bench_ab.sh` materializes each side (git worktree, or `wt` for a snapshot of the working tree), then runs interleaved rounds: every round runs every side, and odd rounds reverse the side order to cancel position bias. Every bench process is pinned via `taskset` (default cores `1..N-1`, keeping core 0 for OS and IRQ housekeeping), the criterion sampling budget is forced through CLI flags so both sides measure identically even across historical revisions with different in-file defaults, and each (round, side) pair gets its own `CRITERION_HOME` so aggregation only ever reads fresh `new/estimates.json` files. `compare.py` reduces each id to the median of its per-round medians and grades every delta with two signals. *Stable*: the delta clearly exceeds the observed round-to-round spread with every round leaning the same way. *Dominant* (the `dom b` column): how many of the na×nb cross-side round pairs side b won; full separation survives a single outlier round, which the spread scale cannot, since one slow round inflates a side's spread to ~30 % and once buried a real −19.9 % improvement as `noise` while the improved side won all 25/25 pairwise rounds. `--fail-on-regression` trips on stable-or-dominant regressions.

```sh
# full two-sided A/B, 3 interleaved rounds (both sides build once)
perf/bench-suite/bench_ab.sh -a base=9b31fb0 -b new=HEAD

# drift-sensitive families: isolated per-id interleaving, extra rounds
perf/bench-suite/bench_ab.sh -a base -b wt -r 5 --per-id \
    'stream_pipeline/single_stage_ordered' 'with_fence'

# same-binary runtime-knob A/B (scheduler-class changes: recompiles swing
# tight benchmarks ±30 % through pure code layout)
perf/bench-suite/bench_ab.sh -a off=wt -b on=wt \
    -E off=YOUPIPE_ONPOOL_HYBRID=0 -E on=YOUPIPE_ONPOOL_HYBRID=1 \
# three-way: rounds interleave A,B,C (label=rev syntax)
perf/bench-suite/bench_ab.sh -a old=HEAD~2 -b mid=HEAD~1 -c wt

python3 perf/bench-suite/compare.py target/bench-ab/run-<ts>   # verdict
```

Appending `-r N` to the same outdir later adds rounds, and the worktrees and their builds are reused. Cleanup is `rm -rf <outdir> && git worktree prune`.

## Measurement hygiene

This machine is bistable on some benches: the ordered drain bench produces ~29 ms rounds in one mode and ~80–150 ms rounds in the other, same binary, minutes apart. Criterion comparisons across points in time are therefore invalid on this machine. Any conclusion must come from the same-binary interleaved A/B of `bench_ab.sh`.

Criterion filter arguments are regexes anchored loosely at both ends: the filter `sync_filter/youpipe_filter_map` also matches the `_owned` bench. Terminate filters with `/` (`youpipe_filter_map/`) so a filter matches exactly one id.

Further traps are documented with the campaigns that exposed them: cross-commit codegen layout drift and the whole-group 100 K fused collapse in [attribution.md](attribution.md), and the placement sensitivity of the 1P1C channel-throughput group in [streaming.md](streaming.md).

## Topic index

- [sync-fused.md](sync-fused.md): fused and sync-engine benches against rayon, from `sync_cpu_heavy` and pipeline fusion through input materialization, `try_collect`, the filter-chain collect variants, reduce terminals, on-pool nesting, expand-heavy, and adjacent-sync fusion.
- [streaming.md](streaming.md): streaming benches, mixed load, async IO, sharded-terminal and batched-ring knob A/Bs with the default-policy soaks, the convoy fix, channel throughput, and stream worker tx batching.
- [attribution.md](attribution.md): attribution and forensics, hotpath and data-plane probes, measurement traps (layout lottery, LLVM folding), NT-store and 2M/4M gap attribution, wake accounting, and the async-chain capacity floor.
- [counters.md](counters.md): the horizontal cross-library comparison (charts, methodology, results) plus perf-event counters, deterministic instruction counts, and the lab bench crate layout.
- [graph.md](graph.md): graph engine benches, the `graph_shapes` A/B and data-plane batching.
