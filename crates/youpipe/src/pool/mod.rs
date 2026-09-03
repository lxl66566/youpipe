//! Rayon-inspired pool core: type-erased jobs, atomic latches, packed sleep
//! counters, and a work-stealing scheduler.
//!
//! Adapted from rayon-core (`MIT OR Apache-2.0`):
//! <https://github.com/rayon-rs/rayon>. This crate complies with the
//! Apache-2.0 terms for that code; see `LICENSE`. Per-file headers note which
//! upstream module each file derives from.

#![allow(dead_code)] // Scope/spawn infrastructure not yet wired to public API.

pub(crate) mod job;
pub(crate) mod join;
pub(crate) mod latch;
pub(crate) mod registry;
pub(crate) mod sleep;
pub(crate) mod sleep_mask;
pub(crate) mod unwind;

pub(crate) use registry::{Registry, global_registry};
