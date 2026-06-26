mod config;

mod typed;

pub use config::{PipelineConfig, Workload};
// `pub(crate)` re-export so `crate::scope::ScopedPipe` can drive the same
// `fused_collect_scoped` machinery as `Pipe::collect`, but with `'env`
// (non-`'static`) closure bounds.
pub(crate) use typed::fused_collect_scoped;
pub use typed::{Pipe, StreamPipe, TryPipe, pipe, stream};
pub use typed::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, StageMarker, SyncMap,
    TryMap,
};
