# Streaming pipelines

`stream` chains stages as separate units connected by bounded channels — for async IO, cancellation, fences, and 1-to-N expansion.

## Sync and async stages

```rust
use youpipe::prelude::*;

// Sync closures run on the compute pool; async futures run as M:N tasks on
// a tokio runtime (built lazily on the first `.run()`).
let r: Vec<u64> = (0..1000).stream()
    .stage(|x: u64| x + 1)                          // CPU stage
    .stage_async(|x: u64| async move { io(x).await }) // IO stage
    .run();
```

| Method | Runs on | Use for |
| --- | --- | --- |
| `.stage(f)` | work-stealing compute pool | sync CPU work |
| `.stage_async(f)` | tokio runtime, `io_concurrency` tasks | IO whose waits `.await` (yield the thread) |

Do not put thread-blocking work (`std::thread::sleep`, blocking file IO)
inside `.stage_async` — it stalls a runtime worker. Use `.stage()` with an
oversized pool instead ([pools](../advanced/pools.md)).

## Terminals and ordering

`.run()` returns a `Vec` in **completion order**; `.ordered()` restores input
order via a reorder pass. `.try_run()` returns `std::io::Result<Vec<O>>`
instead of panicking when the async runtime cannot be built. `.for_each(f)`
drains item-by-item on the calling thread with no output `Vec` — plain `&mut`
capture works, no atomics needed:

```rust
use youpipe::stream;

let mut total = 0u64;
stream(0..10_000).stage(|x: u64| x * 2).for_each(|x| total += x);
```

**Fused pass-through.** When the chain is only `.stage()`s (no `expand` /
`fence` / `stage_async`, no `with_cancel`, no `with_compute_workers` or
per-stage `workers`/`buffer` pin), `.run()` detects it at the type level and
executes the composed chain on the fused core — the same engine `pipe()`
uses, with no channels or feeder at all. Consequences: no backpressure (peak
memory is input + output, not bounded by `buffer_size`), unordered output
becomes input order, and stage panics propagate to the caller instead of
aborting the process. `.ordered()` output is identical either way. Add any
streaming-only feature (a pin, `with_cancel`, a fence…) to opt back into the
channel topology.

## Fences

By default, stages overlap: stage 2 starts consuming as soon as stage 1
produces. `.fence(mode)` controls exactly one adjacent stage boundary:

```rust
use std::num::NonZeroUsize;
use youpipe::prelude::*;

// Stage 2 receives items in batches of 64 while stage 1 keeps producing —
// use FenceMode::Barrier instead for a hard cut (drain stage 1 fully first).
let r: Vec<i32> = (0..1000).stream()
    .stage(|x: i32| x + 1)
    .fence(FenceMode::Chunked(NonZeroUsize::new(64).unwrap()))
    .stage(|x: i32| x * 2)
    .run();
```

`FenceMode::Barrier` maximizes isolation at the cost of overlap and peak
memory; `FenceMode::Chunked(k)` is the right default for mixed CPU/IO loads.

## Expansion and cancellation

`.expand(f)` is a 1-to-N stage (`Fn(O) -> Vec<N>`, like `flat_map`). Note:
`.expand()` combined with `.ordered()` panics — the reorder buffer assumes a
1-to-1 item mapping.

```rust
use youpipe::prelude::*;

let lines: Vec<String> = vec!["a b".into(), "c".into()];
// Each input yields zero or more outputs; expanded items inherit the
// parent's sequence tag.
let words: Vec<String> = lines.stream()
    .expand(|line: String| line.split_whitespace().map(String::from).collect())
    .run();
assert_eq!(words.len(), 3);
```

`.expand(f)` allocates one `Vec` per input item. For expand-heavy loads the
push-style `.expand_emit(|item, out| …)` appends outputs to a per-worker
scratch buffer that is cleared and reused across items — zero steady-state
allocation, same output semantics:

```rust
use youpipe::prelude::*;

let lines: Vec<String> = vec!["a b".into(), "c".into()];
let words: Vec<String> = lines.stream()
    .expand_emit(|line: String, out: &mut Vec<String>| {
        out.extend(line.split_whitespace().map(String::from));
    })
    .run();
assert_eq!(words.len(), 3);
```

`.with_cancel(token)` checks a [`CancellationToken`] in the feeder, every
stage worker, and every bridge, once per iteration. In-flight items drain to
completion; no new items are accepted after `token.cancel()`:

```rust
use youpipe::prelude::*;

let token = CancellationToken::new();
let r = (0..10_000).stream()
    .with_cancel(token.clone())
    .stage(|x: u32| expensive(x))
    .run();
// From another thread: token.cancel() aborts the run early.
```

[`CancellationToken`]: https://docs.rs/youpipe/latest/youpipe/struct.CancellationToken.html
