use std::future::Future;

#[cfg(feature = "tokio-runtime")]
pub struct AsyncPool {
    handle: tokio::runtime::Handle,
    num_workers: usize,
}

#[cfg(feature = "tokio-runtime")]
impl AsyncPool {
    #[must_use]
    pub fn new(handle: tokio::runtime::Handle, num_workers: usize) -> Self {
        Self {
            handle,
            num_workers: num_workers.max(1),
        }
    }

    /// # Errors
    ///
    /// Returns an error if the tokio runtime cannot be built.
    pub fn from_global(num_workers: usize) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_workers)
            .enable_all()
            .build()?;
        Ok(Self::new(rt.handle().clone(), num_workers))
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handle.spawn(future);
    }

    #[must_use]
    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_async_pool_basic() {
        let pool = AsyncPool::from_global(2).unwrap();
        let result = pool.block_on(async { 42 });
        assert_eq!(result, 42);
    }
}
