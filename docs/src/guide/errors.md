# Fallible pipelines

Stages that can fail use `try_map`; the first `Err` aborts the run and `try_collect` returns it as `Result<Vec<O>, E>`.

## Basic fallible chain

```rust
use youpipe::prelude::*;

// `try_map` switches the builder to a fallible chain; `.try_collect()`
// yields `Result<Vec<i32>, ParseError>` instead of a plain `Vec`.
enum ParseError { NotANumber }
let input: Vec<&str> = vec!["1", "23", "42"];
let r: Result<Vec<i32>, ParseError> = input
    .pipe()
    .try_map(|s: &str| s.parse::<i32>().map_err(|_| ParseError::NotANumber))
    .try_collect();
```

Short-circuit semantics: with parallel execution, the first `Err` *observed*
by any worker aborts the run; items already in flight are discarded, and that
error is returned. Panics inside stages still propagate as panics — they are
not routed through `Result`.

## Mixing fallible and infallible stages

`map` and `filter` work on a fallible chain; their effects compose with the
`Result` via `?`. The error type `E` is fixed across the chain — every
`try_map` must produce the same `E`, and `.map_err(f)` converts it:

```rust
use youpipe::pipe;

let r: Result<Vec<String>, String> = pipe(0..100)
    .try_map(|x: i32| if x == 50 { Err("bad") } else { Ok(x * 2) })
    .map_err(|e: &str| e.to_string()) // &str -> String: unify error types
    .map(|x| format!("{x}"))          // infallible stage, `E` unchanged
    .try_collect();
```

## Constraints

| Rule | Detail |
| --- | --- |
| Error type | `E: Send + 'static` — owned errors (`&'static str`, enums, `anyhow::Error`) |
| Chained `try_map` | same `E` required; convert upstream with `.map_err` |
| `filter` after `try_map` | drops items silently, never signals an error |

## Fallible + borrowed input

`pipe_ref` and `scope` have the same `try_map` / `map_err` / `try_collect`
trio. Inputs may be borrowed via the closure's lifetime, but the error `E`
must still be `'static`:

```rust
use youpipe::pipe_ref;

let data: Vec<i32> = (0..100).collect();
// Items are `&i32`; the error is owned.
let r: Result<Vec<i32>, &str> =
    pipe_ref(&data).try_map(|&x: &i32| if x > 90 { Err("too big") } else { Ok(x) }).try_collect();
```

Streaming chains (`stream()`) have no fallible stages — `.try_run()`'s
`Result` reports async-runtime construction failure only, not stage errors.
