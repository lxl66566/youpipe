# youpipe-gungraun

Deterministic, machine-independent instruction-count benchmarks for youpipe,
built on [gungraun](https://github.com/gungraun/gungraun) (the iai-callgrind
successor). Where the criterion suite needs interleaved A/B rounds to fight
±10 % wall-clock drift, these benches run each case exactly once under the
Valgrind simulator and count executed instructions (`Ir`) — a property of the
binary and its input, not of machine temperature, frequency scaling or
background load.

## Measured determinism (32-core host, valgrind 3.27, back-to-back runs)

| Bench | Worst run-to-run Ir drift |
| --- | --- |
| `channel` youpipe MPMC / MPSC rows | **0** (exact to the instruction) |
| `channel` crossbeam row | **0** |
| `channel` std `sync_channel` row | ±0.04 % (std's adaptive mpsc strategy) |
| `fused` youpipe rows | 0 .. 0.003 % |
| `fused` rayon rows | ±0.06 % (rayon's timer-based idle sleeps) |
| `stream` rows | ±0.02 % |

This is the whole point: a criterion A/B needs 3–5 interleaved rounds to reach
±2 %; these numbers are one-shot stable to ~4 significant digits, and the
youpipe-only rows reproduce exactly. Use them for "did this change add work"
checks and for CI regression gating.

## Requirements

- Linux + valgrind ≥ 3.20.
- `gungraun-runner`, version-matched to the `gungraun` dev-dependency
  (0.19.4), on `PATH` at benchmark runtime:

  ```bash
  cargo install --version 0.19.4 gungraun-runner
  ```

The crate (lib + bench targets) compiles on any host; the bench targets set
`test = false`, so `cargo test --workspace` never executes them on hosts
without valgrind.

## The counting caliber (read this before adding benches)

gungraun's default `EntryPoint::Default` toggles Callgrind collection around
the benchmark function — but collection state is **per-thread**, so pool
worker threads (and any thread spawned inside the bench) are invisible: a
`pipe(..).collect()` bench reported only ~26 kIr of driver-side dispatch, and
a 10 k-item channel ping-pong reported 8.7 kIr. Useless for a thread-pool
library.

`benches/common/mod.rs` therefore runs every bench with
`EntryPoint::None` + `--collect-atstart=yes`: instructions are counted on all
threads from process start, and the reported number is a **process total**
(scaffolding + setup + op + teardown). Two consequences:

1. Every row of a comparison group must run the *same* setup work
   (`youpipe_gungraun::both_pools()` spawns a 4-worker youpipe pool AND a
   4-worker rayon pool even where a row uses only one), so the fixed offset
   cancels in `compare_by_id` deltas. Pool teardown also lands inside the
   window on every row.
2. Absolute numbers are not "framework instructions for the op" — read the
   **deltas between rows** and the run-to-run/baseline diffs, not the totals.

`callgrind::zero_stats()` at bench-function entry would shave the setup
offset instead, but that needs gungraun's `client_requests` feature (valgrind
headers + libclang at build time); the symmetric-setup trick achieves the
same cancellation without the environment dependency.

## Machine independence

Pools are pinned (`ComputePool::new(4)`, `rayon::num_threads(4)`) in the
setup expressions because default pools size to `available_parallelism()` —
an 8-core and a 32-core host would run different numbers of worker idle/
backoff rounds and the counts would not be comparable across machines. The
pinned pools still exercise the full dispatch/steal/wake surface.

Not covered here, on purpose:

- **Async stages (`stage_async`)** — tokio's timers make instruction counts
  time- and scheduling-dependent.
- **Wall-clock time** — use the criterion suite (`crates/youpipe/benches`,
  `crates/youpipe-bench`).

## What is measured

| Bench | Groups | Rows |
| --- | --- | --- |
| `fused` | `cpu_heavy_{1k,100k}`, `light_{10k,100k}` | youpipe `pipe().with_compute_pool(..).map(..).collect()` vs rayon `ThreadPool::install(par_iter)` vs sequential, sharing bench ids so `compare_by_id` pairs them |
| `channel` | `channel_1p1c` | two-thread ping-pong, 10 k items, capacity 256: youpipe MPMC (`channel`) and MPSC (`handoff::mpsc_channel`) calibers vs crossbeam `bounded` vs std `sync_channel` |
| `stream` | `stream_{single,two}_stage` | `stream().stage()` fused pass-through and the two-stage channel path vs sequential, 10 k items |

First cross-library readings (process totals, so read deltas): at 1 K
cpu_heavy youpipe pays ~8.7 % more instructions than rayon (fixed dispatch
cost), at 100 K it is ~2.3 % ahead; `light_100k` youpipe −8 % vs rayon,
+21 % vs sequential; the single-stage stream pass-through adds only ~3 %
over a sequential loop at 10 K.

## Running

```bash
cargo bench -p youpipe-gungraun                     # everything (~1 min)
cargo bench -p youpipe-gungraun --bench fused -- '*cpu_heavy*'
cargo bench -p youpipe-gungraun -- --list           # all benchmark ids
```

Arguments after `--` go to the gungraun runner; the position filter is an
anchored wildcard over `<file>::<group>::<function>::<id>`.
`--output-format=json` emits one JSON object per benchmark on stdout
(per-thread breakdowns included); callgrind dumps land under
`target/gungraun/` for `callgrind_annotate`/kcachegrind.

### Baselines and regression gating

```bash
cargo bench -p youpipe-gungraun -- --save-baseline=main
# ...apply a change...
cargo bench -p youpipe-gungraun -- --baseline=main                  # new|old with deltas
cargo bench -p youpipe-gungraun -- --baseline=main --callgrind-limits='ir=2%'
```

`--callgrind-limits` makes the run exit with code 3 on regression — directly
usable as a CI gate. Given the measured drift (≤0.06 % on external-lib rows,
exact on youpipe rows), a 2 % `ir` limit is comfortably tight.

Determinism caveat: identical binaries and inputs reproduce exactly, but any
change to compiler version, flags, dependency versions, pool size or input
size shifts absolute numbers — record a fresh baseline after such changes
instead of comparing across them.
