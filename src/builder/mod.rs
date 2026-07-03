mod config;

mod typed;

pub use config::{PipelineConfig, Workload};
// `pub(crate)` re-export so `crate::scope::ScopedPipe` can drive the same
// `fused_collect_scoped` machinery as `Pipe::collect`, but with `'env`
// (non-`'static`) closure bounds.
pub(crate) use typed::fused_collect_scoped;
// `pub(crate)` re-export so `crate::scope::ScopedPipe::for_each` can drive
// the same `par_for_each` sink core as `Pipe::for_each`, but with `'env`
// closure bounds.
pub(crate) use typed::fused_for_each_scoped;
pub use typed::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, Pipe, StageMarker,
    StreamPipe, StreamStart, SyncMap, TryMap, TryPipe, pipe, stream,
};
