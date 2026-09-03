# Getting started

youpipe runs a chain of transforms over an iterator in parallel: build the chain, then execute it with one terminal call.

## Install

```sh
cargo add youpipe
```

## Minimal pipeline

```rust
use youpipe::pipe;

// `pipe` accepts any `IntoIterator`; `.collect()` is the terminal call that
// runs the whole chain on the work-stealing pool and returns a `Vec`.
let result: Vec<i32> = pipe(0..10_000).map(|x| x * 2).collect();
```

## Two equivalent entry styles

Free functions and a prelude extension trait produce identical types:

```rust
use youpipe::pipe;
let a: Vec<i32> = pipe(0..1000).map(|x| x + 1).collect();
```

```rust
use youpipe::prelude::*; // IterExt: .pipe() / .stream() on any IntoIterator
let b: Vec<i32> = (0..1000).pipe().map(|x| x + 1).collect();
```

Pick whichever reads better at the call site.

## Which entry point for which workload

| Workload | Entry |
| --- | --- |
| Pure CPU map/filter | `pipe(items)` |
| Read-only transform over an existing slice | `pipe_ref(&slice)` |
| Closures borrowing stack-local data | `scope(\|s\| s.pipe(items)...)` |
| Side effects only, no output `Vec` | `pipe(items).for_each(..)` |
| Fallible stages | `pipe(items).try_map(..).try_collect()` |
| Skewed item costs (a few slow items) | `pipe(items).with_workload(Workload::Unbalanced)` |
| Async IO, mixed sync CPU + async IO | `stream(items).stage_async(..)` |
| Cancellation, fences, 1-to-N expansion | `stream(items).with_cancel(..).fence(..).expand(..)` |

`pipe` covers fused CPU chains; `stream` covers everything that needs stages
as separate units: async IO, cancellation, fences, expansion. See
[pipe](pipe.md) and [stream](stream.md).

## Is youpipe the right tool?

Below ~10 µs of total work or ~100 ns per item, a sequential
`iter().map().collect()` is faster — parallel dispatch overhead dominates.
For the full decision table, see [choosing the right engine](../advanced/choosing-engine.md);
for per-scenario knobs, see [tuning](../advanced/tuning.md).
