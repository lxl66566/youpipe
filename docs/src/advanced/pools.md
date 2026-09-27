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
it across runs — construction costs ~15 µs per worker (thread spawn +
priming), which dominates tight loops:

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

The fused path has the same knobs: `with_compute_pool`, plus two conveniences
— `.with_compute_workers(n)`, which runs the terminal on a pool of exactly
`n` threads whenever `n` differs from the machine default, and
`.with_oversubscribe(factor)`, which uses a `factor × compute_workers` pool
at terminal time. Both resolve through `ComputePool::new`, which **recycles**:
a dropped pool is parked (threads stay alive, idle) and the next call with
the same worker count reuses it, so one-shot pipelines sitting in loops no
longer pay per-run spawn/join costs.

Recycling rules:

* At most 4 recent worker counts stay parked; anything evicted joins its
  threads for real.
* `ComputePool::clear_cached_pools()` drops all parked pools, joining their
  threads — call it when the process must give the threads back (strict
  thread-count budgeting, teardown, tests). It returns the number of pools
  dropped.
* `new_pinned` pools are never cached: pinned workers hold scarce CPU
  placement and always join when the last handle drops. The global pool
  bypasses the cache too (process-lifetime already).
* In steady-state loops an explicit pre-created pool is still the best form:
  it keeps the pool alive under your control and documents intent; the cache
  is a safety net for one-shot call sites.

Factor guidance: CPU + fast IO → 1 (no benefit), CPU + slow disk IO → 2–3,
network/lock contention → 3–4, mostly IO → 4–8. Never oversubscribe pure-CPU
work — measured 10–30 % regression.

## Core pinning for tight batch loops (opt-in)

`ComputePool::new_pinned(n)` pins worker `i` to the i-th CPU the process is
allowed to run on. A worker that parks between back-to-back batches then
always wakes on its own — idle, cache-warm — core instead of wherever CFS
places it (measured 7.6 cold-core migrations per iteration unpinned).

```rust
use youpipe::{ComputePool, prelude::*};

// Hot loop of large saturated batches: −4…−7 % vs the unpinned pool.
let pool = ComputePool::new_pinned(num_cpus);
loop {
    let r: Vec<_> = pipe_ref(&batch).map(transform).with_compute_pool(&pool).collect();
}
```

The regime matters — pinning trades away the scheduler's ability to steer a
woken thread to an idle CPU:

| Workload shape | Measured effect of pinning |
| --- | --- |
| back-to-back saturated batches (≥ ~1 ms of even work) | **−4…−7 %** |
| small/medium batches (driver participates, most workers parked) | +5…+22 % |
| streaming pipelines (stage workers park on channels) | **+17…+48 % — do not pin** |
| oversubscribed pools (> #CPUs threads) | meaningless (16+ threads per CPU) |

Use exactly one pinned, ≤-CPU-sized pool for fused batch terminals; never
share it with `stream` pipelines or run several pinned pools on overlapping
CPUs. `YOUPIPE_PIN_WORKERS=1` is the same-binary benchmarking knob (it pins
every pool in the process); the supported API is the constructor.

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
