# youpipe

English | [简体中文](https://github.com/lxl66566/youpipe/blob/main/README.zh-CN.md)

youpipe is a high-performance, data-first parallel pipeline supporting mixed
CPU workloads and streaming async IO. Items enter at the front, stages chain
naturally, and a single terminal call (`.collect()` / `.run()`) executes the
whole chain. Two pipeline engines cover different regimes:

- `Pipe` — compile-time fused CPU chains. `.map().filter().map()` becomes a
  single monomorphized closure per worker with no intermediate allocations.
- `StreamPipe` — channel-backed streaming for cases fusion cannot cover:
  async IO, cancellation, fences, 1-to-N expansion, and more. Pure sync
  chains (`.stage()`s only, unpinned) auto-fuse onto the fused core at
  `.run()` — fused performance with the streaming API.

A rayon-style work-stealing scheduler (`st3` LIFO deque + packed atomic
sleep counters) handles balanced and unbalanced loads. `scope()` supports
non-`'static` closures that borrow stack-local data.

Usage: `cargo add youpipe`.

## Quick start

`pipe(items)` / `items.pipe()` produce the same types — either works.

```rust
use youpipe::prelude::*;

// fused CPU chain: one monomorphized closure per worker
let r: Vec<i32> = (0..1000).pipe()
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0)
    .map(|x| x * 10)
    .collect();

// aggregation terminals: same fusion, no output Vec at all
let s: i64 = (0..1000).pipe().map(|x| x + 1).sum();

// sync CPU stage + async IO stage, overlapped on separate pools
let r: Vec<u64> = (0..1000).stream()
    .stage(|x: u64| x + 1)
    .stage_async(|x: u64| async move { fetch(x).await })
    .run();
```

For index-shaped work (`0..n`), `pipe_range(0..n)` skips input materialization
entirely — items are generated in the parallel leaves instead of serially
collected on the calling thread (the dominant cost of `pipe(0..n)` at 1M+
items).

Pick the engine and tuning knobs by workload — see
[Choosing the right engine](https://lxl66566.github.io/youpipe/advanced/choosing-engine.html).

## Documentation

Full manual at **[lxl66566.github.io/youpipe](https://lxl66566.github.io/youpipe/)**:

- [User guide](https://lxl66566.github.io/youpipe/guide/getting-started.html) —
  fused, streaming, borrowing and fallible pipelines
- [Performance tuning](https://lxl66566.github.io/youpipe/advanced/choosing-engine.html) —
  engine choice, workload hints, pools, per-stage knobs
- [Benchmarks and methodology](https://lxl66566.github.io/youpipe/dev/benchmarks.html) —
  cross-library comparison and how it is measured
- [Developer guide](https://lxl66566.github.io/youpipe/dev/design.html) —
  scheduler, channels, verification (miri/loom)

Sources live under `docs/` (`mdbook build docs` to build locally).

## Fuzzing

Coverage-guided fuzzing lives in `crates/youpipe-fuzz` (cargo-fuzz, excluded
from the workspace):

```sh
cargo fuzz run --fuzz-dir crates/youpipe-fuzz pipeline -- -max_total_time=60
```

Targets: `pipeline` (fused chains), `stream` (streaming topology),
`channel` (handoff data plane), `reorder` (ReorderBuffer) — each asserts
against a serial reference model. See
[the verification guide](https://lxl66566.github.io/youpipe/dev/testing.html)
for details, Windows specifics, and known findings.

## Performance

youpipe is the only library at or near the top in every
workload class — up to 5× faster than rayon on CPU pipelines, up to 23%
ahead of hand-written tokio plumbing on realistic three-stage pipelines,
and within a few percent of the async ceiling on pure IO.

Cross-library comparison against rayon, tokio, `futures::stream`, and
hand-written `std::thread` pipelines over seven workloads (balanced/skewed
CPU, async/blocking IO, mixed sync+async, and two realistic three-stage
pipelines, including HTTP over a loopback mock server):

<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-cpu.svg" alt="CPU pipelines: youpipe vs rayon vs hand-written std threads">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-io.svg" alt="IO pipelines: youpipe vs tokio vs futures">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/lxl66566/youpipe/main/docs/src/assets/bench-real.svg" alt="Mixed sync + async pipelines: youpipe vs tokio vs futures vs rayon">
</p>

See [Benchmarks and methodology](https://lxl66566.github.io/youpipe/dev/benchmarks.html) for more details.

Below ~10 µs of total work or ~100 ns per item, youpipe is not recommended —
the parallel setup overhead won't pay off. Sequential `iter().map().collect()`
is faster in that range.

## Attribution

The work-stealing scheduler in `crates/youpipe/src/pool/` is adapted from
[rayon-core](https://github.com/rayon-rs/rayon).
