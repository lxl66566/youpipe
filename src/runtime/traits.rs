use std::{future::Future, pin::Pin};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub trait Runtime: Send + Sync + 'static {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;

    fn spawn_blocking<F, R>(&self, f: F) -> BoxFuture<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future;
}
