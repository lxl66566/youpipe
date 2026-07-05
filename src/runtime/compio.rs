//! compio backend for [`AsyncRuntime`](super::AsyncRuntime).
//!
//! compio's [`Runtime`] is **thread-local** (`!Send`, `Rc`-backed) and
//! **single-threaded**. This backend uses a hybrid model to bridge compio's
//! thread-locality with youpipe's streaming topology, which spawns consumer
//! tasks from one thread and may drive them from another:
//!
//! # The hybrid split
//!
//! - **`spawn`** (the async-stage consumer tasks): these futures are `Send`
//!   (they hold crossfire's mpmc `AsyncReceiver`, which is `Sync`, plus the
//!   user's `Send` `Fut`). They are dispatched to a **dedicated worker thread**
//!   that owns one compio `Runtime` and drives it forever via
//!   `rt.block_on(inbox-driver)`. The worker's `block_on` loop ticks the
//!   executor on every iteration, so dispatched consumers run concurrently with
//!   the inbox driver.
//! - **`block_on`** (the streaming collector `collect_async`): this future is
//!   `!Send` — it borrows crossfire's MPSC async receiver, whose waker registry
//!   is `Cell`-based (`!Sync`). It runs **locally** on the calling thread's own
//!   lazily-created thread-local compio runtime. Nothing crosses a thread
//!   boundary, so `!Send` is fine.
//!
//! The two runtimes (worker thread's + caller thread's) communicate purely
//! through crossfire channels, which are runtime-agnostic: a crossfire wake
//! calls the compio task waker that was registered when the future was polled,
//! and compio's `Local::schedule` forwards that wake to the runtime's driver
//! waker (see `compio-executor/src/task/local.rs`), interrupting the proactor
//! `poll()`. That is the load-bearing bridge that makes cross-channel wakeups
//! actually re-poll compio futures.
//!
//! # Why not "delegate everything" or "local everything"?
//!
//! - Delegating `block_on` to the worker thread requires the future be `Send`,
//!   which `collect_async` (MPSC receiver) is not.
//! - Running `spawn` only locally fails in the async→sync topology: the
//!   `try_run` thread reaches a sync collector and never calls `block_on`, so
//!   locally-spawned consumers would never be driven.
//!
//! The hybrid is the smallest design that satisfies both constraints.
//!
//! # The compio `!Send`-primitive caveat
//!
//! compio's own IO/timer futures (`compio_runtime::time::sleep`, file/net
//! ops) are `!Send` because they hold `Rc`-refs to the driver. `stage_async`
//! requires `Fut: Send` (tokio's multi-thread backend needs it), so those
//! primitives cannot appear directly inside a `stage_async` closure. The
//! streaming consumers themselves (crossfire + Send user logic) are Send, so
//! the common streaming use case works; integrating compio-native IO is left
//! to a future `!Send`-aware spawn path.

use std::{cell::RefCell, future::Future, sync::Arc, thread::JoinHandle};

use compio_runtime::Runtime;

use super::{AsyncRuntime, BoxFuture};
use crate::handoff::{AsyncReceiver, SyncSender, sync_async_channel};

/// Generous inbox for futures awaiting the worker runtime. See
/// `INBOX_CAPACITY` rationale — spawns arrive in bounded bursts consumed
/// near-instantly as the worker `rt.spawn`s them.
const INBOX_CAPACITY: usize = 1024;

thread_local! {
    /// Lazily-created compio [`Runtime`] for the **calling** thread — used only
    /// by [`CompioPool::block_on`] to drive the `!Send` collector future. One
    /// runtime per thread, reused across pipeline runs; dies with the thread.
    static LOCAL_RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// Obtain a cloned handle to this thread's local compio runtime (creating it
/// on first use), for `block_on`.
fn local_runtime() -> Runtime {
    LOCAL_RT.with(|cell| {
        let mut guard = cell.borrow_mut();
        let rt = guard.get_or_insert_with(|| {
            Runtime::new()
                .expect("compio Runtime::new failed — proactor init (io-uring/polling) unavailable")
        });
        rt.clone()
    })
}

/// Async runtime backed by compio: one dedicated worker thread (for spawned
/// consumer tasks) + a per-caller-thread local runtime (for `block_on`).
///
/// Cheap to clone: only the shared [`Arc`] control block is duplicated. The
/// worker thread and its runtime live until the last clone drops.
///
/// Construct via [`CompioPool::build`] or [`AsyncRuntime::build_default`].
#[derive(Clone)]
pub struct CompioPool {
    inner: Arc<CompioInner>,
}

struct CompioInner {
    /// Mixed-mode sender feeding the worker thread's runtime inbox. `None`
    /// after [`CompioInner::drop`] takes it (to signal the worker to exit).
    sender: Option<SyncSender<BoxFuture<'static, ()>>>,
    worker: Option<JoinHandle<()>>,
}

impl CompioPool {
    /// Build a compio backend with a dedicated worker thread.
    ///
    /// `num_workers` is advisory: compio runtimes are single-threaded, so the
    /// worker count is always 1 (async-stage concurrency is governed by
    /// `io_concurrency`, the cooperative task fan-out, not by OS threads).
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the worker thread cannot be spawned.
    pub fn build(_num_workers: usize) -> std::io::Result<Self> {
        let (sender, receiver) = sync_async_channel::<BoxFuture<'static, ()>>(INBOX_CAPACITY);
        let worker = std::thread::Builder::new()
            .name("youpipe-compio".into())
            .spawn(move || worker_main(receiver))?;
        // The worker owns the only receiver clone; when `sender` is dropped
        // (pool teardown) the worker's `recv` observes disconnect.
        Ok(Self {
            inner: Arc::new(CompioInner {
                sender: Some(sender),
                worker: Some(worker),
            }),
        })
    }

    /// Convenience wrapper around [`build`](Self::build).
    ///
    /// # Errors
    ///
    /// Same as [`build`](Self::build).
    pub fn build_default() -> std::io::Result<Self> {
        Self::build(1)
    }
}

/// Worker thread entry point: own a compio [`Runtime`] and drive it forever,
/// spawning every future that arrives on `inbox`.
///
/// The `rt` clone captured by the driver future lets `Runtime::spawn` run
/// without the scoped-tls current-runtime context. compio's `block_on` loop
/// interleaves polling the inbox driver with ticking the executor and polling
/// the proactor, so spawned consumers progress concurrently with inbox reads.
fn worker_main(inbox: AsyncReceiver<BoxFuture<'static, ()>>) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(e) => panic!("compio Runtime::new failed on worker thread: {e}"),
    };
    let rt_for_driver = rt.clone();
    rt.block_on(async move {
        while let Ok(fut) = inbox.recv().await {
            // `detach` is load-bearing: compio's `JoinHandle` cancels its task
            // on drop (unlike tokio's detached spawn). Without it every
            // consumer would be cancelled the instant this loop iterates.
            rt_for_driver.spawn(fut).detach();
        }
        // `sender` dropped on the pool side → `recv` returns `Closed` → loop
        // exits → `block_on` returns → `rt` drops → thread exits.
    });
}

impl AsyncRuntime for CompioPool {
    fn build_default(_workers: usize) -> std::io::Result<Self> {
        Self::build(1)
    }

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Dispatch to the worker thread. `SyncSender::send` blocks under
        // backpressure, but the only producers are stage-assembly / bridge
        // threads (never a compio worker), so parking them cannot deadlock the
        // runtime. `Closed` means the worker died; propagate loudly.
        let boxed: BoxFuture<'static, ()> = Box::pin(fut);
        self.inner
            .sender
            .as_ref()
            .expect("spawn after drop — CompioInner sender taken")
            .send(boxed)
            .expect("compio worker thread died; runtime pool is no longer usable");
    }

    fn block_on<T, F>(&self, fut: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T>,
    {
        // Drive locally on the calling thread's runtime. The future need not
        // be `Send` (nothing crosses threads) — this is what lets
        // `collect_async` borrow crossfire's `!Sync` MPSC receiver.
        //
        // compio's `block_on` interleaves polling `fut` with ticking the
        // executor: if `fut` awaits a crossfire `recv`, a producer (running on
        // the worker thread's separate runtime) wakes the waker registered
        // here, which compio routes to the local driver waker, interrupting
        // `poll()` so `fut` is re-polled and the value arrives.
        let rt = local_runtime();
        rt.block_on(fut)
    }

    fn num_workers(&self) -> usize {
        1
    }
}

impl Drop for CompioInner {
    fn drop(&mut self) {
        // Drop the sender first → the worker's inbox `recv` observes
        // disconnect → its `block_on` returns → its runtime drops → thread
        // exits. Then join so the thread is fully stopped before the channel
        // backings are freed.
        self.sender.take();
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }
}

impl std::fmt::Debug for CompioPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompioPool").finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "compio-runtime"))]
mod tests {
    use super::*;
    use crate::handoff::{AsyncReceiver, async_channel};

    #[test]
    fn test_compio_block_on_immediate() {
        let pool = CompioPool::build(2).unwrap();
        let result = pool.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    /// Worker-thread spawn path + local block_on path meet through a crossfire
    /// channel: this is the exact mechanism the streaming topology relies on
    /// (consumers on the worker runtime, collector on the caller runtime,
    /// crossfire bridges them, compio routes the wake to the driver waker).
    #[test]
    fn test_compio_spawn_then_recv_via_crossfire() {
        let pool = CompioPool::build(1).unwrap();
        let (tx, rx): (crate::handoff::AsyncSender<u64>, AsyncReceiver<u64>) = async_channel(4);
        pool.spawn(async move {
            tx.send(99).await.unwrap();
        });
        let v = pool.block_on(async move { rx.recv().await.unwrap() });
        assert_eq!(v, 99);
    }

    /// Multiple concurrent spawns all complete — the worker runtime drives
    /// them concurrently within its single thread.
    #[test]
    fn test_compio_multi_spawn_completes() {
        const N: u64 = 100;
        let pool = CompioPool::build(1).unwrap();
        let (tx, rx): (crate::handoff::AsyncSender<u64>, AsyncReceiver<u64>) = async_channel(128);
        for i in 0..N {
            let tx = tx.clone();
            pool.spawn(async move {
                tx.send(i).await.unwrap();
            });
        }
        drop(tx);
        let collected: Vec<u64> = pool.block_on(async move {
            let mut v = Vec::with_capacity(usize::try_from(N).unwrap());
            while let Ok(x) = rx.recv().await {
                v.push(x);
            }
            v
        });
        assert_eq!(collected.len(), usize::try_from(N).unwrap());
    }
}
