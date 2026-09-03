//! Tokio backend for [`AsyncRuntime`](super::AsyncRuntime).

use std::{future::Future, sync::Arc};

use super::AsyncRuntime;

/// Async runtime backed by a `tokio` multi-threaded runtime.
///
/// Cheap to clone: the owned `Runtime` is held in an `Arc` (kept alive for the
/// pool's lifetime) and only the `Handle` is duplicated per clone.
///
/// Construct via [`TokioPool::new`] (wrap an externally-managed runtime) or
/// [`TokioPool::build`] (build + own a private runtime).
#[derive(Clone)]
pub struct TokioPool {
    /// `Some` when this pool owns the runtime (constructed via [`build`]);
    /// `None` when the caller manages the runtime and handed us only a
    /// [`Handle`].
    ///
    /// Holding the `Runtime` here is load-bearing: dropping it would tear down
    /// the worker threads and turn `handle` into a reference to a dead runtime.
    /// The field is never read — its sole purpose is to be kept alive
    /// alongside every clone of this pool (via the shared `Arc`).
    ///
    /// [`build`]: TokioPool::build
    /// [`Handle`]: tokio::runtime::Handle
    #[allow(dead_code)]
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    handle: tokio::runtime::Handle,
    num_workers: usize,
}

impl TokioPool {
    /// Wrap an externally-managed tokio runtime.
    ///
    /// The caller is responsible for keeping the source runtime alive for at
    /// least as long as this `TokioPool` (and every clone) is in use.
    #[must_use]
    pub fn new(handle: tokio::runtime::Handle, num_workers: usize) -> Self {
        Self {
            runtime: None,
            handle,
            num_workers: num_workers.max(1),
        }
    }

    /// Build a private multi-threaded tokio runtime sized to `num_workers` and
    /// own it (shared across clones via `Arc`) for the lifetime of this pool.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the tokio runtime cannot be built.
    pub fn build(num_workers: usize) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_workers)
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        Ok(Self {
            runtime: Some(Arc::new(runtime)),
            handle,
            num_workers: num_workers.max(1),
        })
    }

    /// Convenience wrapper around [`build`](Self::build) that picks the worker
    /// count from `available_parallelism()` (clamped to ≥ 4 on failure).
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the tokio runtime cannot be built.
    pub fn build_default() -> std::io::Result<Self> {
        let n = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        Self::build(n)
    }

    /// The underlying tokio handle. Useful for callers that need direct tokio
    /// APIs (e.g. spawning into the same runtime from user code).
    #[must_use]
    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

impl AsyncRuntime for TokioPool {
    fn build_default(workers: usize) -> std::io::Result<Self> {
        Self::build(workers)
    }

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // `Handle::spawn` is explicit (does not depend on the TLS current-runtime
        // context set by `Handle::enter`), so no enter-guard is needed here.
        // This is what lets the runtime abstraction drop the `enter()` concept
        // entirely — and keeps the door open for a future backend whose
        // runtime-context mechanism has no RAII guard at all.
        self.handle.spawn(fut);
    }

    fn block_on<T, F>(&self, fut: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T>,
    {
        // No delegation: tokio's M:N runtime drives the future directly on the
        // calling thread while the worker threads poll spawned tasks. `F` is
        // not required to be `Send` — `Handle::block_on` runs the future inline
        // on this thread, matching the streaming collector that borrows
        // crossfire's `!Sync` async receiver.
        self.handle.block_on(fut)
    }

    fn num_workers(&self) -> usize {
        self.num_workers
    }
}

#[allow(clippy::missing_fields_in_debug)] // the omitted `Handle`/`Runtime`
// fields have no useful Debug repr; the owned-vs-borrowed flag is the only
// diagnostic that matters.
impl std::fmt::Debug for TokioPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioPool")
            .field("owns_runtime", &self.runtime.is_some())
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

#[cfg(all(test, feature = "tokio-runtime"))]
mod tests {
    use super::*;

    #[test]
    fn test_tokio_pool_basic() {
        let pool = TokioPool::build(2).unwrap();
        let result = pool.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    /// Regression: the owned `Runtime` must stay alive across clones. An
    /// earlier version dropped the runtime after taking the handle, leaving
    /// spawned futures silently never running. This spawns a future that must
    /// actually execute.
    #[test]
    fn test_owned_runtime_stays_alive_across_clones() {
        let pool = TokioPool::build(2).unwrap();
        let _clone = pool.clone();
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
