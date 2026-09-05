# Choosing the right engine

youpipe has two engines — fused `pipe()` for CPU chains and streaming `stream()` for everything else — plus two regimes where youpipe is the wrong tool entirely.

## Decision table

| Workload | Engine | Entry |
| --- | --- | --- |
| Balanced CPU map/filter | fused | `pipe(items)` or `pipe_ref(&slice)` |
| Skewed CPU costs (a few slow items) | fused | `pipe(items).with_workload(Workload::Unbalanced)` |
| Borrow a slice or stack-local data | fused | `pipe_ref(&slice)` / `scope(\|s\| s.pipe(..))` |
| Async IO, mixed sync CPU + async IO | streaming | `stream(items).stage(..).stage_async(..)` |
| Blocking IO in sync stages | streaming | `stream(items).with_compute_pool(ComputePool::new(512))` |
| Cancellation, fences, 1-to-N, ordered output | streaming | `stream(items).with_cancel(..).fence(..).expand(..).ordered()` |

Knob details: [tuning](tuning.md). Pool sizing: [pools](pools.md).

## How the engines differ

The fused engine compiles the whole `.map()/.filter()` chain into one
monomorphized closure and runs it over a fork-join work-stealing tree,
writing results into a pre-allocated buffer in input order. The streaming
engine gives every stage its own workers connected by bounded channels: sync
stages share the compute pool, async stages multiplex `io_concurrency` tasks
over a tokio runtime, and results reach the collector in completion order.
Fused wins on per-item overhead; streaming wins whenever stages must be
separate units (async IO, backpressure, fences, cancellation). Internals are
covered in the [developer guide](../dev/design.md).

## When youpipe is the wrong tool

**Total work below ~10 µs, or ~100 ns per item.** Fixed dispatch overhead
(a few µs per run on a 32-core machine; it was ~50 µs before the
`available_parallelism` result was cached process-wide) exceeds the parallel
gain. Use a sequential `iter().map().collect()`.

**Pure async IO with no CPU stages.** If the chain is only `.await`-based IO
and you need nothing beyond the futures themselves, raw tokio or
`futures::stream` combinators are equal or better — youpipe rides the same
tokio runtime and adds channel infrastructure for nothing. Measured on
512–2000 sleep-IO items (32-core, median of 5 interleaved rounds):

| Scenario | Best | youpipe |
| --- | --- | --- |
| `io_async`, 500 items | futures 9.10 ms | 9.60 ms (tokio native 9.53) |
| `io_async`, 2000 items | futures 17.5 ms | 18.3 ms (tokio native 18.4) |

## When youpipe wins

Mixed and multi-stage pipelines, where stages overlap instead of running
back-to-back and CPU stays isolated from IO (same benchmark harness):

| Scenario | youpipe | Best alternative |
| --- | --- | --- |
| Mixed sync CPU + async IO, 2K items | 10.7 ms | tokio hand-written 13.4 ms |
| Realistic 3-stage doc pipeline, 4K docs | 13.9 ms | tokio hand-written 17.2 ms (rayon 137) |
| Realistic HTTP pipeline, 2K requests | 23.0 ms | tokio 27.9 ms, futures 30.4 ms |

Balanced CPU: youpipe wins 1K–100K items (−57 % vs rayon at 100K) and ties
at 1M (bandwidth-bound). Full
data and methodology: [benchmarks](../dev/benchmarks.md).
