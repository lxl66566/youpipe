mod config;
mod typed;

pub use config::{PipelineConfig, Workload};
// `pub(crate)` re-export so `crate::scope::ScopedPipeline` can drive the same
// `par_index_collect` machinery as `Pipeline::collect`, but with `'env`
// (non-`'static`) closure bounds.
pub(crate) use typed::fused_collect_scoped;
pub use typed::{
    Fence, Filter, FusedStage, Identity, Ordered, Pipeline, StageMarker, StreamPipeline, SyncMap,
    par_chunks_map, par_map, par_map_with_workload, try_par_map,
};
