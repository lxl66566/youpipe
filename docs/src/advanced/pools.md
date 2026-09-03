# Pools and concurrency sizing

youpipe runs sync work on a `ComputePool` (work-stealing OS threads) and async work on an async runtime (`TokioPool` by default); size them per workload.

## Defaults

| Knob | Default |
| --- | --- |
| `compute_workers` | `available_parallelism` |
| `async_workers` | `available_parallelism` |
| `io_concurrency` | 128 |
| `buffer_size` | 256 |
| `Workload` | `Balanced` |

Pools are capped at `MAX_COMPUTE_WORKERS = 511` (the scheduler's sleep
bitmask is 9 bits). Larger values are **silently clamped**, never rejected.

## ComputePool: create once, reuse

`ComputePool` is cheap to clone (`Arc` + one atomic). Pre-create it and share
it across runs — per-call construction (~ms of thread spawn + priming)
dominates tight loops:

```rust
use youpipe::{ComputePool, stream};

// Blocking IO in a sync .stage(): oversubscribe. The global pool has one
// thread per core, so each blocked thread idles its core; 512 threads fill
// the stall gaps. (512 clamps to 511 — immaterial.)
let pool = ComputePool::new(512);
let r = (0..1000).stream()
    .with_compute_pool(pool.clone()) // budget = the pool's worker count
    .stage(|x: u64| blocking_io(x))
    .run();
```

The fused path has the same knob (`with_compute_pool`) plus a convenience,
`.with_oversubscribe(factor)`, which builds a transient `factor × num_cpus`
pool at terminal time and tears it down after. Fine for one-shot pipelines;
in loops prefer the pre-created pool. Factor guidance: CPU + fast IO → 1 (no
benefit), CPU + slow disk IO → 2–3, network/lock contention → 3–4, mostly IO
→ 4–8. Never oversubscribe pure-CPU work — measured 10–30 % regression.

## Blocking IO: the numbers

Blocking work in a sync `.stage()` must oversubscribe, or waits serialize.
Blocking-IO scenario (500 items of ~1 ms sleeps, skewed tail, 32-core,
median of 5 interleaved rounds):

| Configuration | Wall time |
| --- | --- |
| youpipe, 512-thread pool | 8.65 ms |
| tokio `spawn_blocking` (512 threads) | 8.88 ms |
| youpipe, default 32 threads | 34.1 ms |

At 2000 items: 12.6 ms (512 threads) vs 122 ms (default). Correctly sized,
youpipe matches `spawn_blocking`; the failure mode is a pool-sizing problem,
not a framework one. Alternatively use `.stage_async()` when the wait can
`.await` ([tuning](tuning.md)).

## TokioPool: share the async runtime

Without `with_async_pool`, each `.run()` builds a tokio runtime lazily on
first use and drops it at the end (~ms). Share one across runs instead:

```rust
use youpipe::prelude::*;

// build_default() sizes workers from available_parallelism(); TokioPool is
// cheap to clone (Arc'd runtime, duplicated Handle).
let pool = TokioPool::build_default()?;
let r1 = batch_a.stream().with_async_pool(pool.clone())
    .stage_async(|x| async move { io(x).await })
    .run();
let r2 = batch_b.stream().with_async_pool(pool)
    .stage_async(|x| async move { io(x).await })
    .run();
```

`TokioPool::build(n)` pins the worker count; `TokioPool::new(handle, n)`
wraps an externally-managed runtime (you keep the `Runtime` alive). If
runtime construction can fail and `.run()`'s panic is unacceptable, use
`.try_run()`.

## Separate pools: CPU isolation from IO

Sync stages of a run share one `ComputePool`; async stages always run on the
async runtime, so CPU and IO are already on different executors. Add a
*further* split when a blocking-IO stage would otherwise monopolize the
global pool: give it a dedicated oversized `ComputePool` (as above) while
pure-CPU pipelines keep using the global pool — the blocking waits can no
longer starve unrelated CPU work.
