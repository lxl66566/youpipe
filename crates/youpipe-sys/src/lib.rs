//! `youpipe-sys` — the miri/loom-transparent primitive layer of the
//! [youpipe](https://crates.io/crates/youpipe) pipeline framework.
//!
//! Extracted into its own crate so youpipe and its scheduler/channel share
//! one loom/miri-tested definition of the synchronization primitives.
//!
//! | Environment  | `Mutex`/`Condvar`                                | Atomics              |
//! | ------------ | ------------------------------------------------ | -------------------- |
//! | Production   | `parking_lot` (fairer, never poisons)            | `std::sync::atomic`  |
//! | Miri         | `std::sync` (newtype shim, infallible `lock()`)  | `std::sync::atomic`  |
//! | `--cfg loom` | `loom::sync` (newtype shim, infallible `lock()`) | `loom::sync::atomic` |
//!
//! All backends expose identical, infallible APIs so callers never branch on
//! `cfg`.
//!
//! # Internal crate
//!
//! The API surface follows youpipe's needs and carries no stability
//! guarantees outside youpipe releases; do not depend on it directly.

mod affinity;
mod cache_padded;
mod sync;

pub use affinity::{allowed_cpus, pin_current_thread_to};
pub use cache_padded::CachePadded;
pub use sync::{AtomicU64, AtomicUsize, Condvar, Mutex, MutexGuard, Ordering, fence, thread_yield};
