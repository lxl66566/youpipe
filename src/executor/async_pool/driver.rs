#[cfg(feature = "tokio-runtime")]
use std::future::Future;

#[cfg(feature = "tokio-runtime")]
pub struct AsyncPool {
    /// `Some` when this pool owns the runtime (constructed via
    /// [`Self::from_global`]); `None` when the caller manages the runtime and
    /// handed us only a [`Handle`](tokio::runtime::Handle).
    ///
    /// Holding the `Runtime` here is load-bearing: dropping it would tear down
    /// the worker threads and turn `handle` into a reference to a dead runtime.
    /// The field is never read — its sole purpose is to be kept alive (and
    /// eventually dropped) alongside this `AsyncPool`.
    #[allow(dead_code)]
    runtime: Option<tokio::runtime::Runtime>,
    handle: tokio::runtime::Handle,
    num_workers: usize,
}

#[cfg(feature = "tokio-runtime")]
impl AsyncPool {
    /// Wrap an externally-managed tokio runtime.
    ///
    /// The caller is responsible for keeping the source [`Runtime`] alive for
    /// at least as long as this `AsyncPool` is in use.
    #[must_use]
    pub fn new(handle: tokio::runtime::Handle, num_workers: usize) -> Self {
        Self {
            runtime: None,
            handle,
            num_workers: num_workers.max(1),
        }
    }

    /// Build a private multi-threaded tokio runtime sized to `num_workers` and
    /// own it for the lifetime of this pool.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the tokio runtime cannot be built.
    pub fn from_global(num_workers: usize) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_workers)
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        Ok(Self {
            runtime: Some(runtime),
            handle,
            num_workers: num_workers.max(1),
        })
    }

    /// Convenience wrapper around [`Self::from_global`] that picks the worker
    /// count from `available_parallelism()` (clamped to ≥ 4 on failure).
    ///
    /// Useful when the caller just wants "one OS thread per core" without
    /// having to thread the count through. For `StreamPipe` users this is
    /// rarely needed — omitting `with_async_pool` gives the same effect lazily
    /// inside a single `run()` call.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the tokio runtime cannot be built.
    pub fn from_default() -> std::io::Result<Self> {
        let n = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        Self::from_global(n)
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

#[cfg(all(test, feature = "tokio-runtime"))]
mod tests {
    use super::*;

    #[test]
    fn test_async_pool_basic() {
        let pool = AsyncPool::from_global(2).unwrap();
        let result = pool.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    /// Regression: `from_global` previously dropped the `Runtime` after taking
    /// its `Handle`, leaving the handle pointing at a torn-down runtime whose
    /// workers were dead. Spawned futures would silently never run. This test
    /// spawns a future that must actually execute.
    #[test]
    fn test_from_global_runtime_stays_alive() {
        let pool = AsyncPool::from_global(2).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        pool.spawn(async move {
            tx.send(99).unwrap();
        });
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
            99
        );
    }
}
