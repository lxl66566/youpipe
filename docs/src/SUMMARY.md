# Summary

- [Introduction](README.md)

# User guide

- [Getting started](guide/getting-started.md)
- [Fused CPU pipelines](guide/pipe.md)
- [Streaming pipelines](guide/stream.md)
- [DAG pipelines](guide/graph.md)
- [Borrowing data](guide/scope.md)
- [Fallible pipelines](guide/errors.md)

# Performance tuning

- [Choosing the right engine](advanced/choosing-engine.md)
- [Tuning knobs by workload](advanced/tuning.md)
- [Pools and concurrency sizing](advanced/pools.md)

# Developer guide

- [Design and architecture](dev/design.md)
- [Core types and execution paths](dev/core-types.md)
- [Work-stealing scheduler](dev/scheduler.md)
- [Streaming data plane](dev/streaming.md)
- [Streaming forensics: convoy collapse and async slow modes](dev/streaming-forensics.md)
- [Verification: miri, loom and fuzzing](dev/testing.md)
- [Falsified directions](dev/dead-ends.md)
- [Benchmarks and methodology](dev/benchmarks/README.md)
    - [Fused and sync benches vs rayon](dev/benchmarks/sync-fused.md)
    - [Streaming benches](dev/benchmarks/streaming.md)
    - [Attribution and forensics](dev/benchmarks/attribution.md)
    - [Cross-library comparison and counters](dev/benchmarks/counters.md)
    - [Graph engine benches](dev/benchmarks/graph.md)
- [Publishing](dev/publishing.md)
