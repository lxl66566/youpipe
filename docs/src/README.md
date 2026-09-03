# youpipe

youpipe is a high-performance, data-first parallel pipeline library for Rust.
Items enter at the front, stages chain naturally, and a single terminal call
executes the whole chain. Two engines cover different regimes:

- **`Pipe`** — compile-time fused CPU chains (`.map().filter().map()` becomes
  one monomorphized closure per worker, no intermediate allocations).
- **`StreamPipe`** — channel-backed streaming for what fusion cannot cover:
  async IO, cancellation, fences, 1-to-N expansion, ordered output.

This book has three parts:

| Part | Read it for |
| ---- | ----------- |
| [User guide](guide/getting-started.md) | Feature walkthroughs, minimal snippets. |
| [Performance tuning](advanced/choosing-engine.md) | Pick the right engine and knobs for your workload. |
| [Developer guide](dev/design.md) | Internals: scheduler, channels, verification, methodology. |

Benchmark charts and cross-library comparisons live in the repository
[README](https://github.com/lxl66566/youpipe) and
[dev/benchmarks.md](dev/benchmarks.md).
