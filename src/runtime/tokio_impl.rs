use std::future::Future;

use super::traits::{BoxFuture, Runtime};

pub struct TokioRuntime {
    runtime: tokio::runtime::Runtime,
}

impl TokioRuntime {
    pub fn new() -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self { runtime })
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

impl Runtime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.runtime.spawn(future);
    }

    fn spawn_blocking<F, R>(&self, f: F) -> BoxFuture<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let handle = self.runtime.handle().clone();
        Box::pin(async move {
            handle
                .spawn_blocking(f)
                .await
                .expect("spawn_blocking task panicked")
        })
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.runtime.block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokio_runtime_block_on() {
        let rt = TokioRuntime::new().unwrap();
        let val = rt.block_on(async { 7 * 6 });
        assert_eq!(val, 42);
    }
}
