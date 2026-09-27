# Fused CPU pipelines

`pipe` builds compile-time fused CPU chains: all stages collapse into one closure executed per item by a work-stealing pool.

## Chaining stages

```rust
use youpipe::pipe;

// All three stages fuse into a single monomorphized closure per worker —
// there is no intermediate Vec between map/filter steps.
let r: Vec<i32> = pipe(0..1000)
    .map(|x| x + 1)
    .filter(|x: &i32| x % 2 == 0) // filter closures see `&O`
    .map(|x| x * 10)
    .collect();
```

Type-changing maps compose freely: `.map(i32 -> String)` followed by
`.map(String -> usize)` type-checks, because input and output types are
tracked separately through the chain.

## Side-effect terminal: `for_each`

`for_each` skips the output `Vec` entirely — the counterpart of rayon's
`par_iter().for_each()`:

```rust
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use youpipe::pipe;

// The closure is `Fn + Sync + 'static`: it cannot borrow stack locals or
// mutate captured state, so accumulate through atomics (or a Mutex).
let total = Arc::new(AtomicU64::new(0));
let t = total.clone();
pipe(0..1000u64).for_each(move |x| t.fetch_add(x, Ordering::Relaxed));
```

To borrow stack-local data in the closure instead, use
[`pipe_ref`](#borrowed-input-pipe_ref) or [scope](scope.md).

## Aggregation terminals: `reduce` / `fold` / `sum` / …

Aggregation skips the output `Vec` the same way `for_each` does: each
parallel leaf folds its range into one partial accumulator and the tree
combines partials in parallel — rayon's `.par_iter().sum()` shape without
materializing anything:

```rust
use youpipe::pipe;

let max = pipe(0..1000).map(|x: i64| x * 3).reduce(i64::max);
let sum: i64 = pipe(0..1000).map(|x: i64| x * 3).sum();
let n = pipe(0..1000).filter(|x: &i64| x % 2 == 0).count();
```

`reduce(op)` returns `Option<O>` — `None` for an empty or fully filtered
input. `fold(init, f, combine)` folds into an accumulator of a different
type (each leaf seeds from `init.clone()`); `count()` counts post-filter
outputs; `min()`/`max()` are `reduce(Ord::min/max)` conveniences.

The fold/combine pair must be **associative** for the result to be
independent of the split layout — outputs combine as a deterministic tree
over input order, but the exact association depends on batch size and
worker count, so e.g. float `+` may differ in the last ulp run-to-run.
Fallible chains get `try_reduce`/`try_fold` (the first `Err`
short-circuits); `pipe_ref` carries the same conveniences.

## Borrowed input: `pipe_ref`

`pipe_ref(&slice)` is the counterpart of rayon's `slice.par_iter()`. Items
flow through the chain as `&T` — zero copies, and the input stays usable
after the run:

```rust
use youpipe::pipe_ref;

let data: Vec<u64> = (0..1000).collect();
// `|&x|` destructures the `&u64` item; only the output Vec is allocated.
let doubled: Vec<u64> = pipe_ref(&data).map(|&x| x * 2).collect();
assert_eq!(data.len(), 1000); // `data` was only read, not consumed
```

`pipe_ref` also has `filter`, `try_map`, `for_each`, `collect`, and the
[aggregation terminals](#aggregation-terminals-reduce--fold--sum--). Because
its closures are bounded by the input borrow instead of `'static`, they may
capture other stack-local data for free — see [borrowing data](scope.md).

## Index input: `pipe_range`

`pipe(0..n)` materializes any non-`Vec` input on the calling thread before
the parallel phase starts — for a range that is a serial O(n) fill, which
dominates lightweight maps at 1M+ items (measured 56–70 % of the whole call
at 1M/4M, see the dev benchmarks). `pipe_range(0..n)` removes the input
buffer entirely: items are *generated* inside the parallel leaves — the item
at index `i` is `i` — so there is nothing to fill, allocate, or read:

```rust
use youpipe::pipe_range;

let r: Vec<u64> = pipe_range(0..1_000_000)
    .map(|i: usize| (i as u64).wrapping_mul(31).wrapping_add(7))
    .collect();
```

`pipe_range` has the same builder surface as `pipe` (`map`, `filter`,
`try_map`, the tuning setters). Chains that can `filter` — and the fallible
`try_collect()` terminal — materialize the indices once at the terminal
(the same serial fill `pipe(range)` always paid); the filter-free `collect()`
and `for_each()` run the generation core. If you already own a `Vec`, keep
using `pipe(v)` — it reuses your buffer with zero copies.

## No global stage waits

A fused chain has no stage boundaries at all — each worker applies the whole
chain per item, so the question "does stage 2 wait for stage 1?" does not
arise here. In streaming chains (`stream()`), stages *do* overlap by default
and [fences](stream.md#fences) exist when you need isolation.
