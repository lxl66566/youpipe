mod config;
mod typed;

pub use config::{PipelineConfig, Workload};
pub use typed::{
    Fence, Filter, FusedStage, Identity, Ordered, Pipeline, StageMarker, StreamPipeline, SyncMap,
    par_chunks_map, par_map, par_map_with_workload, try_par_map,
};
