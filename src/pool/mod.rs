//! Rayon-inspired pool core: type-erased jobs, atomic latches, packed sleep
//! counters, and a work-stealing scheduler.

#![allow(dead_code)] // Scope/spawn infrastructure not yet wired to public API.

pub(crate) mod job;
pub(crate) mod join;
pub(crate) mod latch;
pub(crate) mod registry;
pub(crate) mod sleep;
pub(crate) mod unwind;

pub(crate) use registry::{Registry, global_registry};
