mod config;

mod typed;

pub use config::{PipelineConfig, Workload};
pub use typed::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, Pipe, PipeRef, RangePipe,
    StageMarker, StageOptions, StreamPipe, StreamStart, SyncMap, TryMap, TryPipe, TryPipeRef, pipe,
    pipe_range, pipe_ref, stream,
};
// `pub(crate)` re-export so `crate::scope::ScopedPipe` can drive the same
// `fused_collect_scoped` machinery as `Pipe::collect`, but with `'env`
// (non-`'static`) closure bounds. Also re-exports `resolve_exec_pool` /
// `ExecPool` so `ScopedPipe` can resolve the oversubscribe / custom-pool
// hint identically to `Pipe`.
pub(crate) use typed::{
    fused_collect_scoped, fused_for_each_scoped, fused_try_collect_scoped, resolve_exec_pool,
};
