mod borrowed;
mod fused;
mod slots;
mod stream;
mod traits;

pub(crate) use self::fused::{
    fused_collect_scoped, fused_for_each_scoped, fused_try_collect_scoped, resolve_exec_pool,
};
pub use self::{
    borrowed::{PipeRef, TryPipeRef, pipe_ref},
    fused::{Pipe, RangePipe, TryPipe, pipe, pipe_range},
    stream::{StageOptions, StreamPipe, StreamStart, stream},
    traits::{
        Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, StageMarker, SyncMap,
        TryMap,
    },
};
