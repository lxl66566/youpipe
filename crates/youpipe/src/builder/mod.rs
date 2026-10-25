mod config;

mod typed;

pub use config::{FilterCollectMode, PipelineConfig, Workload};
pub use typed::{
    AsyncStageOptions, FenceOptions, Filter, FusedStage, FusedTryStage, Grouped, Identity,
    InfallibleChain, MapErr, Ordered, Pipe, PipeRef, RangePipe, StageMarker, StreamPipe,
    StreamStart, SyncMap, SyncStageOptions, TryMap, TryPipe, TryPipeRef, Unordered, pipe,
    pipe_range, pipe_ref, stream,
};
// `pub(crate)` re-export so `crate::scope::ScopedPipe` can drive the same
// `fused_collect_scoped` machinery as `Pipe::collect`, but with `'env`
// (non-`'static`) closure bounds. Also re-exports `ExecOptions` /
// `exec_setters!` (the shared exec-tuning field + setter macro every fused
// builder carries) and the reducer wrappers so the scoped reduce/sum/count
// terminals mirror `Pipe`'s.
pub(crate) use typed::{
    CountReducer, ExecOptions, OptionReducer, SumReducer, exec_setters, fused_collect_scoped,
    fused_fold_scoped, fused_for_each_scoped, fused_reduce_scoped, fused_try_collect_scoped,
    fused_try_fold_scoped, fused_try_reduce_scoped,
};
// Shared lifecycle helpers the graph engine reuses (see typed/mod.rs).
pub(crate) use typed::{
    PanicSlot, ParkingLease, cancel_active, floored_channel_capacity, record_panic,
    resume_stored_panic,
};
