//! Pluggable async runtime backend for streaming pipelines.
//!
//! Streaming stages that need async IO (`.stage_async(..)`) run on an
//! [`AsyncRuntime`] — an abstraction over the concrete executor (tokio, compio,
//! …). The fused CPU path (`pipe` / `scope`) is fully sync and never touches
//! this trait.
//!
//! # Why a trait, not tokio directly
//!
//! youpipe originally called tokio APIs directly (`tokio::spawn`,
//! `Handle::block_on`). Supporting compio — whose `Runtime` is thread-local
//! (`!Send`, `Rc`-backed) and single-threaded — without leaking tokio types
//! required a narrow abstraction. The runtime is touched only at
//! **per-run** stage-assembly time (spawn `io_concurrency` consumer tasks) and
//! once at collection (`block_on`), never in the per-item hot path, so the
//! abstraction is cost-free in steady state.
//!
//! # Generic over the backend
//!
//! [`StreamPipe`](crate::StreamPipe) carries an `R: AsyncRuntime` type
//! parameter (defaulting to [`DefaultRuntime`]) so every spawn / block_on
//! monomorphises to the concrete backend — zero virtual dispatch. Sync-only
//! chains never instantiate `R`, so [`NoRuntime`] (whose methods panic) is a
//! safe default when no backend feature is enabled.
//!
//! # Backends
//!
//! - [`TokioPool`] (`tokio-runtime`): wraps a `tokio::runtime::Handle`. The
//!   runtime's M:N scheduler multiplexes many tasks over `n` OS threads.
//! - [`CompioPool`] (`compio-runtime`): owns `n` OS threads, each with its own
//!   thread-local compio `Runtime` (compio runtimes are `!Send`). A shared
//!   mixed-mode channel feeds futures to whichever worker is idle; `block_on`
//!   delegates by spawning + joining on a oneshot.

use std::{future::Future, pin::Pin};

/// Owned, sendable future. Used internally by backends that must move a future
/// across threads before spawning it (notably [`CompioPool`]).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A pluggable async runtime backend for streaming pipelines.
///
/// Implementations are `Clone` (cheap — `Arc` / `Handle` clones) so that
/// `acquire_async` can hand out an owned handle per call site without
/// re-issuing a runtime construction.
///
/// All methods are generic (not `dyn`-safe) so that `StreamPipe` monomorphises
/// the per-run spawn / block_on calls to the concrete backend — there is no
/// vtable on the streaming path.
pub trait AsyncRuntime: Clone + Send + Sync + 'static {
    /// Build a default-configured runtime with `workers` OS threads backing it.
    ///
    /// Used by `StreamCtx::acquire_async` when the caller did not attach a pool
    /// via `with_async_pool`. For [`NoRuntime`] this panics — it must never be
    /// reached for a sync-only chain (no `.stage_async(..)`).
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the runtime cannot be constructed (e.g. OS thread
    /// / resource limits).
    fn build_default(workers: usize) -> std::io::Result<Self>;

    /// Spawn a fire-and-forget future onto the runtime.
    ///
    /// The future runs concurrently with other spawned tasks and the
    /// [`block_on`](Self::block_on) driver. Dropping the returned handle (if
    /// any) does not cancel the task — termination is observed through channel
    /// disconnect, matching the streaming topology's completion model.
    ///
    /// `F: Send` is required because the tokio multi-thread backend spawns into
    /// a shared scheduler accessed from multiple OS threads. (compio's
    /// thread-local backend does not need it, but the bound is harmless since
    /// every future youpipe actually spawns — the async consumer tasks — is
    /// `Send`.)
    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static;

    /// Block the calling thread until `fut` completes, returning its output.
    ///
    /// Deliberately **not** `F: Send`: the collector future (`collect_async`)
    /// borrows crossfire's MPSC async receiver, which is `!Sync`, making the
    /// future `!Send`. tokio's `Handle::block_on` has no `Send` bound, and
    /// compio runs `block_on` on a thread-local single-threaded runtime, so
    /// neither backend requires the future to cross a thread boundary. (An
    /// earlier draft tried delegating compio's `block_on` to a worker thread,
    /// but that demands `F: Send` — incompatible with the crossfire receiver.)
    fn block_on<T, F>(&self, fut: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T>;

    /// Number of OS threads backing the runtime (tokio worker threads, compio
    /// worker threads, …). Used only for reporting / sizing, not for spawning.
    fn num_workers(&self) -> usize;
}

/// No-op runtime used as the default `R` when no backend feature is enabled.
///
/// Every method panics — but a sync-only streaming chain (one without
/// `.stage_async(..)`) never instantiates the runtime, so the panic paths are
/// unreachable in that configuration. The type exists purely so that
/// `StreamPipe<S, I, O, R = DefaultRuntime>` compiles with `R = NoRuntime` when
/// neither `tokio-runtime` nor `compio-runtime` is on.
///
/// Adding `.stage_async(..)` to a chain requires a real backend; the builder
/// method is feature-gated to disappear entirely unless at least one backend
/// feature is enabled, so misuse surfaces as a compile error rather than the
/// runtime panic here.
#[derive(Clone, Copy, Debug)]
pub struct NoRuntime;

impl AsyncRuntime for NoRuntime {
    fn build_default(_workers: usize) -> std::io::Result<Self> {
        panic!(
            "NoRuntime::build_default: no async runtime backend is enabled. \
             Enable the `tokio-runtime` or `compio-runtime` feature on youpipe."
        );
    }

    fn spawn<F>(&self, _fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        panic!("NoRuntime::spawn: no async runtime backend is enabled");
    }

    fn block_on<T, F>(&self, _fut: F) -> T
    where
        T: Send + 'static,
        F: Future<Output = T>,
    {
        panic!("NoRuntime::block_on: no async runtime backend is enabled");
    }

    fn num_workers(&self) -> usize {
        0
    }
}

// ── Backend re-exports ──

#[cfg(feature = "tokio-runtime")]
mod tokio;

#[cfg(feature = "tokio-runtime")]
pub use self::tokio::TokioPool;

#[cfg(feature = "compio-runtime")]
mod compio;

#[cfg(feature = "compio-runtime")]
pub use self::compio::CompioPool;

/// The default runtime backend, selected by feature flags.
///
/// - `tokio-runtime` (default) → [`TokioPool`]
/// - else `compio-runtime` → [`CompioPool`]
/// - else [`NoRuntime`] (sync-only streaming)
#[cfg(feature = "tokio-runtime")]
pub type DefaultRuntime = TokioPool;

#[cfg(all(not(feature = "tokio-runtime"), feature = "compio-runtime"))]
pub type DefaultRuntime = CompioPool;

#[cfg(not(any(feature = "tokio-runtime", feature = "compio-runtime")))]
pub type DefaultRuntime = NoRuntime;

/// `true` iff at least one async runtime backend feature is enabled. Gates the
/// `stage_async` / `AsyncStage` machinery in `stream.rs`.
#[must_use]
pub const fn backend_enabled() -> bool {
    cfg!(any(feature = "tokio-runtime", feature = "compio-runtime"))
}
