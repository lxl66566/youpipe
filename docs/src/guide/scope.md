# Borrowing data

youpipe closures can borrow stack-local data concurrently — no `Arc`, no clones — through `pipe_ref` for slices or `scope` for everything else.

## Which one to use

| Situation | Tool |
| --- | --- |
| Input is a slice (`&[T]`), closures read items as `&T` | `pipe_ref(&slice)` |
| Closures capture other stack-local data (lookup table, config) | `pipe_ref` if the input is a slice, otherwise `scope` |
| Input is a range/iterator and closures capture non-`'static` data | `scope(\|s\| s.pipe(items)...)` |

## `pipe_ref`: borrowed slices without `scope`

`pipe_ref(&slice)` brands every closure with the input's lifetime, so
closures may also capture surrounding stack locals — the terminal blocks
until all workers finish, which is what makes the borrow sound:

```rust
use youpipe::pipe_ref;

let rows: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
let prefix = "row-"; // borrowed by every worker — no clone, no Arc
let suffixes: Vec<Option<usize>> = pipe_ref(&rows)
    .map(|s: &String| s.strip_prefix(prefix).map(str::len))
    .collect();
```

## `scope`: non-`'static` closures on any input

`scope(|s| ...)` opens a context whose `s.pipe(items)` accepts closures
borrowing the enclosing frame — use it when the input is *not* a slice
(ranges, iterators) or you prefer the explicit scope:

```rust
use youpipe::scope;

// `table` and `factor` live on this stack frame; every worker borrows them.
let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
let factor = 7;
let r: Vec<usize> = scope(|s| {
    s.pipe(0..table.len())
        .map(|i: usize| table[i].len() * factor)
        .collect()
});
```

Passing `&table` (any `&[T]`) to `s.pipe` makes items flow as `&T`: the only
allocation is one `Vec<&T>` of pointers, never a clone of `T`. Passing
indices (`0..table.len()`) avoids even that.

## Fallible and side-effect variants

Both forms support the full terminal set: `.filter`, `.try_map(...).try_collect()`,
`.for_each`, `.collect`, `.reduce`/`.fold` (and the `sum`/`count`
conveniences), plus the config knobs from
[tuning](../advanced/tuning.md) (`with_workload`, `with_compute_pool`,
`with_oversubscribe`).
