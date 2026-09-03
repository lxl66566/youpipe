# youpipe internals — Documentation Index

Contributor documentation for youpipe's internals. Start with
[design.md](design.md), then follow links by topic. Each file is
self-contained and cross-linked.

| Document | Contents |
| -------- | -------- |
| [design.md](design.md) | Design philosophy (data-first API, compile-time fusion, streaming engine split, async-runtime abstraction, `scope` lifetimes), the `crates/youpipe/src/` module map, and the recipe for extending the system with new fused stages. |
| [core-types.md](core-types.md) | The core types and execution paths: `Workload`, `Slots` zero-copy buffers, `Pipe` / `TryPipe` / `ScopedPipe` builders, `FusedStage` fusion traits, `collect` / `try_collect` / `for_each` dispatch (hybrid flat/tree), `StreamPipe` stage typestate, async-IO stages, and the feeder. |
| [scheduler.md](scheduler.md) | The `ComputePool` work-stealing thread pool: injector + local deques + stealers architecture, sleep/wake governance, task submission flow, graceful shutdown, and the vendored `youpipe-st3` / `youpipe-concurrent-queue` forks (provenance, optimization lines, rejected alternatives). |
| [streaming.md](streaming.md) | The streaming data plane: MPMC/MPSC channel selection (`crossfire` wrappers), `WaitGroup`, `ReorderBuffer` ordered-output restoration (lazy slot array, scalar fast path), and the `FenceBarrier` chunked/barrier isolation with batch-allocation recycling. |
| [testing.md](testing.md) | Verification strategy: the `youpipe-sys` miri/loom-transparent primitive shims, what the loom model tests cover and how to run them, miri workload scaling (`cfg!(miri)`), the canonical runners under `perf/verify/`, and the downstream build-profile guards (`panic=abort` detection, `opt-level` overrides). |
| [publishing.md](publishing.md) | Crate inventory (what publishes and what stays local), the path+version dual-dependency mechanism, publish order, and the per-release checklist. |
| [benchmarks.md](benchmarks.md) | Performance results vs rayon/tokio across all bench families, the hotpath instrumentation rounds, per-round A/B verdicts, the suite budget config (`benches/common`), the interleaved A/B tooling under `perf/bench-suite/`, and the hard-won measurement methodology (isolated alternating runs; the stale-criterion-directory, stream-family variance, and 100 K full-group collapse traps). |

Historical note: these files were split from a single `ARCHITECTURE.md`
(2026-09); per-round experiment logs live in the git history of that file.
