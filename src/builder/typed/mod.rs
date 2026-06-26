mod fused;
mod slots;
mod stream;
mod traits;

pub(crate) use self::fused::fused_collect_scoped;
pub use self::{
    fused::{Pipe, TryPipe, pipe},
    stream::{StreamPipe, stream},
    traits::{
        FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, StageMarker, SyncMap, TryMap,
        Filter,
    },
};
