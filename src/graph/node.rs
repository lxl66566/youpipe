use std::{future::Future, pin::Pin};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageKind {
    Sync,
    Async,
}

pub enum NodeKind<I, O> {
    Sync(Box<dyn FnMut(I) -> O + Send + Sync + 'static>),
    Async(Box<dyn Fn(I) -> BoxFuture<O> + Send + Sync + 'static>),
    Fence { chunk_size: Option<usize> },
}

pub struct PipelineNode<I, O> {
    pub kind: NodeKind<I, O>,
    pub stage_kind: StageKind,
    pub parallelism: usize,
    pub name: &'static str,
}

impl<I, O> PipelineNode<I, O> {
    pub fn sync(f: impl FnMut(I) -> O + Send + Sync + 'static) -> Self {
        Self {
            kind: NodeKind::Sync(Box::new(f)),
            stage_kind: StageKind::Sync,
            parallelism: 0,
            name: "",
        }
    }

    pub fn async_stage(f: impl Fn(I) -> BoxFuture<O> + Send + Sync + 'static) -> Self {
        Self {
            kind: NodeKind::Async(Box::new(f)),
            stage_kind: StageKind::Async,
            parallelism: 0,
            name: "",
        }
    }

    #[must_use]
    pub fn fence(chunk_size: Option<usize>) -> PipelineNode<I, I> {
        PipelineNode {
            kind: NodeKind::Fence { chunk_size },
            stage_kind: StageKind::Sync,
            parallelism: 1,
            name: "fence",
        }
    }

    #[must_use]
    pub fn with_parallelism(mut self, n: usize) -> Self {
        self.parallelism = n;
        self
    }

    #[must_use]
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
}
