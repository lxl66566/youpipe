# Examples

Each example is a self-contained, runnable program demonstrating one feature of
youpipe. Run any of them with:

```bash
cargo run --release --example <name>
```

> Always use `--release` — debug builds don't exercise the work-stealing
> scheduler or the tokio runtime realistically, and most examples compare
> against production-grade baselines (rayon, tokio).

## Two equivalent API styles

The library exposes two ways to start a pipeline, and the examples deliberately
mix them so you can see both:

```rust
// 1. Free functions:
use youpipe::{pipe, stream};
let r = pipe(items).map(|x| x + 1).collect();
let r = stream(items).stage(|x| x + 1).run();

// 2. Extension methods on any IntoIterator (recommended for new code):
use youpipe::prelude::*;
let r = items.pipe().map(|x| x + 1).collect();
let r = items.stream().stage(|x| x + 1).run();
```

Both produce identical types — the prelude is just a thin extension trait
(`IterExt`) blanket-implemented for every `IntoIterator`, plus a curated
re-export of the common types (`Pipe`, `StreamPipe`, `Workload`,
`PipelineConfig`, `FenceMode`, `CancellationToken`, …). Pick whichever reads
better at the call site. Most examples in this directory use the free-function
form for explicitness; user code typically uses the prelude form.

## By use case (read in this order if you're new)

| If you want to… | Read this | What it shows |
|---|---|---|
| See the simplest possible youpipe program | [`basic_pipe`](basic_pipe.rs) | `pipe(items).map().collect()` — the data-first fused API |
| Compare fused CPU throughput with rayon | [`fused_vs_rayon`](fused_vs_rayon.rs) | Same 3-stage chain, side-by-side timing |
| Tune for skewed (unbalanced) workloads | [`unbalanced`](unbalanced.rs) | `Workload::Balanced` vs `Unbalanced` vs rayon vs std |
| Do parallel lookups on borrowed data | [`scoped_lookup`](scoped_lookup.rs) | `scope()` borrowing a stack-local table, no clone / Arc |
| Chain async IO with M:N concurrency | [`async_io`](async_io.rs) | `stream().stage_async()` vs `tokio::spawn` per item |
| Mix sync CPU and async IO stages | [`mixed_cpu_io`](mixed_cpu_io.rs) | `.stage().stage_async()` vs `spawn_blocking` for both |
| Insert a batching/barrier between stages | [`fence`](fence.rs) | Two independent `.fence()` calls, Chunked vs Barrier timing |
| Handle errors with short-circuit | [`try_map`](try_map.rs) | `try_map` / `try_collect` chain |
| Do 1-to-N flatMap-style expansion | [`nested_expand`](nested_expand.rs) | `.expand()` vs rayon `flat_map` vs std |
| Build a multi-stage streaming CPU chain | [`stream_chain`](stream_chain.rs) | `stream().stage().stage()` vs tokio `spawn_blocking` |
| Cancel a pipeline mid-flight | [`cancellation`](cancellation.rs) | `with_cancel(token)` aborts feeder + workers + bridges |
| Profile internal hot paths | [`hotpath_profile`](hotpath_profile.rs) | `--features hotpath` only — per-function timing without `perf` |

## Detailed notes

### `basic_pipe`

The 30-line hello-world. Two chains: a single `.map()` and a fused
`.map().filter().map()`. Verifies output equality against std iterators. Start
here.

### `fused_vs_rayon`

A 3-stage CPU chain over 1M items. Side-by-side timing of youpipe's fused
`pipe().map().map().map().collect()` vs rayon's `par_iter().map().map().map()`
vs a single-threaded std iterator baseline. Both youpipe and rayon fuse the
chain at compile time, so per-item throughput should be in the same league.

### `unbalanced`

~10 % of items run 1000× slower than the rest. Compares youpipe with
`Workload::Balanced` (4× oversplit) vs `Workload::Unbalanced` (8× oversplit,
finer-grained stealing) vs rayon's `par_iter` vs std sequential. Demonstrates
why recursive `join` + work-stealing beats naive "split into N chunks"
strategies on skewed loads.

### `scoped_lookup`

The headline `scope()` feature: borrow a non-`'static` stack-local table from
every parallel worker without cloning or `Arc`-ing it. The table has 200k
`String` rows — cloning it per worker would dominate the runtime. Compared
head-to-head with rayon's `par_iter` (which also supports scoped borrowing)
and a single-threaded std baseline.

### `async_io`

Pure async IO with M:N concurrency. Each item does a `tokio::time::sleep`
with skewed latency (90 % × 1 ms, 10 % × 8 ms tail — realistic network/disk
tail latency). Compares youpipe's `stream().stage_async()` (M:N, default
`io_concurrency=128`) vs `tokio::spawn` per item (the async ceiling). The two
should land within a small constant of each other.

This is the simplest async example: no explicit `AsyncPool`, no
`PipelineConfig` — defaults only. The module header documents the tuning form.

### `mixed_cpu_io`

Sync CPU stage on the compute pool → async IO stage on the runtime, connected
by a sync→async bridge. The two stages overlap (IO consumers start as soon as
the first CPU result lands). Compared with tokio's `spawn_blocking` for both
stages (the all-blocking baseline), which serialises the stages and holds an
OS thread per item for the IO wait.

### `fence`

Two independent fences in one chain: `stage(s1).fence(m).stage(s2).fence(m).stage(s3)`.
Demonstrates three things:

1. **Fence scope is per-boundary, not whole-stream.** Each `.fence()` gates
   exactly one adjacent stage transition; the two fences don't interfere.
2. **`FenceMode::Chunked(k)` overlaps stages.** Stage 2 starts consuming the
   moment the first batch of `k` items clears the fence, long before stage 1
   finishes. Visible via per-stage first/last-seen timestamps.
3. **`FenceMode::Barrier` is a hard cut.** Stage 2 sees nothing until stage 1
   is fully drained.

The output is a side-by-side timing table contrasting the two modes.

### `try_map`

A fallible chain: parse → range-check → format. The first `Err` aborts the
chain; `.try_collect()` returns `Result<Vec<_>, _>`. Also shows a clean run
with no errors.

### `nested_expand`

1-to-N expansion: each input produces 5 outputs. youpipe's `stream().expand()`
vs rayon's `flat_map` vs std's `flat_map`. Output arrives in completion order
(unordered) — sorted for correctness comparison.

### `stream_chain`

Two-stage CPU chain via `stream().stage().stage().run()`. Stages are connected
by lock-free channels; stage 2 starts consuming the moment stage 1 emits its
first item. Compared with tokio's `spawn_blocking` for each stage, which
serialises them (every stage-1 task must finish before any stage-2 task is
spawned).

### `cancellation`

`stream().with_cancel(token)` aborts a long-running pipeline mid-flight. A
canceller thread signals the token after 5 ms; the feeder, every stage worker,
and every bridge thread check the token per iteration. In-flight items are
drained to completion but no new items are accepted.

### `hotpath_profile`

Not a user-facing example — it's a one-shot profiling driver for the internal
work-stealing pool. Builds only with `--features hotpath`:

```bash
# Human-readable table
cargo run --release --example hotpath_profile --features hotpath

# Focused scenario
cargo run --release --example hotpath_profile --features hotpath -- 10000 heavy 200

# Structured JSON for A/B comparison
HOTPATH_OUTPUT_FORMAT=json-pretty HOTPATH_OUTPUT_PATH=target/hotpath-report.json \
  cargo run --release --example hotpath_profile --features hotpath
```

Every `#[cfg_attr(feature = "hotpath", hotpath::measure)]` probe planted in
`src/pool/` and `src/builder/` records call-count / latency / percentile data.
The probes are permanent (feature-gated to no-ops in normal builds), so you
can re-run this whenever the scheduler changes to see — without `perf` and
without reading disassembly — exactly how many times each worker parked, how
long each `join`/`steal`/`inject` took, and where the per-call fixed overhead
is actually spent.

## Related benchmarks

The examples are didactic — they print timing comparisons but use small inputs
and run once. For rigorous, repeatable measurements see `../benches/`:

| Bench | What it measures |
|---|---|
| `sync_vs_rayon` | CPU-heavy + lightweight fused `pipe` vs rayon `par_iter` |
| `mixed_load` | Mixed CPU/IO `stream` vs `tokio::spawn_blocking` |
| `io_async` | Async IO (pure + mixed sync+async) — yielding IO, M:N |
| `async_vs_tokio` | Stream vs `tokio::spawn_blocking` |
| `unbalanced` | Skewed workloads, Balanced vs Unbalanced |
| `channel_bench` | Channel throughput vs crossbeam / std mpsc |

Run any of them with `cargo bench --bench <name>`.
