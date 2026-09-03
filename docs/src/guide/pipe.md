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

`pipe_ref` also has `filter`, `try_map`, `for_each`, and `collect`. Because
its closures are bounded by the input borrow instead of `'static`, they may
capture other stack-local data for free — see [borrowing data](scope.md).

## No global stage waits

A fused chain has no stage boundaries at all — each worker applies the whole
chain per item, so the question "does stage 2 wait for stage 1?" does not
arise here. In streaming chains (`stream()`), stages *do* overlap by default
and [fences](stream.md#fences) exist when you need isolation.
