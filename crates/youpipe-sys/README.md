# youpipe-sys

The miri/loom-transparent primitive layer of
[youpipe](https://crates.io/crates/youpipe): a unified
`Mutex`/`Condvar`/atomics/`CachePadded` API that transparently switches
backend by compilation context.

| Environment  | `Mutex`/`Condvar`                                | Atomics              |
| ------------ | ------------------------------------------------ | -------------------- |
| Production   | `parking_lot` (fairer, never poisons)            | `std::sync::atomic`  |
| Miri         | `std::sync` (newtype shim, infallible `lock()`)  | `std::sync::atomic`  |
| `--cfg loom` | `loom::sync` (newtype shim, infallible `lock()`) | `loom::sync::atomic` |

Why not plain `parking_lot` everywhere:

* **Miri** — `parking_lot_core` resolves `WaitOnAddress` through
  `GetModuleHandleA`, a Windows foreign function Miri cannot emulate, whereas
  the std primitives are natively supported by the interpreter.
* **`--cfg loom`** — the model checker must observe the atomics and
  lock/condvar interleavings instead of real OS primitives. Loom is
  activated by the `--cfg loom` rustflag (the ecosystem-standard switch,
  same as crossbeam / tokio), not by a cargo feature: a feature would be
  part of the public semver surface and could be silently unified on by an
  unrelated downstream crate.

All backends expose identical, infallible APIs so callers never branch on
`cfg`.

## Status

Internal support crate of youpipe: the API surface follows youpipe's needs
and carries no stability guarantees outside youpipe releases.

License: Apache-2.0
