pub struct PipelineEdge {
    pub buffer_size: usize,
    pub ordered: bool,
}

impl PipelineEdge {
    #[must_use]
    pub fn new(buffer_size: usize, ordered: bool) -> Self {
        Self {
            buffer_size,
            ordered,
        }
    }

    #[must_use]
    pub fn default_unordered() -> Self {
        Self::new(256, false)
    }

    #[must_use]
    pub fn default_ordered() -> Self {
        Self::new(256, true)
    }
}
