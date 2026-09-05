//! Borrowed-input fused pipelines: `pipe_ref(&data)` — the counterpart of
//! rayon's `slice::par_iter()`.
//!
//! [`pipe`](super::fused::pipe) is the *general* entry point (any
//! `IntoIterator`: ranges, generators, owned `Vec`s), so it takes ownership.
//! [`pipe_ref`] is the *borrowed* entry point for the most common parallel
//! shape — a read-only transform over existing data: items flow through the
//! stage chain as `&T`, the input buffer is never consumed, materialized into
//! a `Vec<&T>`, or freed. See the borrowed-core section in `fused.rs` for how
//! the same hybrid dispatch drives a shared `&[T]`.

use std::{marker::PhantomData, num::NonZeroUsize};

use super::{
    Filter, FusedStage, FusedTryStage, Identity, InfallibleChain, MapErr, StageMarker, SyncMap,
    TryMap,
    fused::{
        fused_collect_by_ref, fused_for_each_by_ref, fused_try_collect_by_ref, resolve_exec_pool,
    },
};
use crate::{
    builder::{PipelineConfig, Workload},
    executor::compute::ComputePool,
};

/// Borrowed-input entry point. Builds a fused pipeline that reads `items` by
/// shared reference — the youpipe counterpart of rayon's `slice::par_iter()`.
///
/// Unlike [`pipe`](crate::pipe) (which consumes its input), the input buffer
/// is only read: nothing is freed inside the terminal, and the data remains
/// usable after `.collect()` / `.for_each()`. Closures receive `&T`, so a
/// non-`Copy` payload (`String`, `PathBuf`, …) is inspected without cloning.
///
/// Comparing the two entry points:
///
/// | | [`pipe`](crate::pipe) | `pipe_ref` |
/// |---|---|---|
/// | input | any `IntoIterator` (ranges, generators, `Vec`) | `&[T]` |
/// | item type in closures | `T` (owned) | `&T` (borrowed) |
/// | input freed inside terminal | yes (consumed) | never |
/// | element bound | `T: Send` (items move) | `T: Sync` (items shared) |
///
/// # Example
///
/// ```
/// use youpipe::pipe_ref;
///
/// let data: Vec<u64> = (0..1000).collect();
/// let doubled: Vec<u64> = pipe_ref(&data).map(|&x| x * 2).collect();
/// assert_eq!(doubled[7], 14);
/// // `data` was only read — still usable:
/// assert_eq!(data.len(), 1000);
/// ```
///
/// Because the pipeline's lifetime is bounded by the input borrow, closures
/// may also borrow other stack-local data for free (no [`scope`]
/// wrapper needed — the terminal blocks until every worker is done):
///
/// ```
/// use youpipe::pipe_ref;
///
/// let data: Vec<String> = (0..10).map(|i| format!("row-{i}")).collect();
/// let prefix = "row-"; // borrowed by every worker, no clone / Arc
/// let suffix_lens: Vec<usize> = pipe_ref(&data)
///     .map(|s: &String| s.strip_prefix(prefix).map(str::len))
///     .filter(Option::is_some)
///     .map(Option::unwrap)
///     .collect();
/// assert_eq!(suffix_lens, vec![1; 10]); // "0".."9" — 1 char each
/// ```
///
/// [`scope`]: crate::scope
pub fn pipe_ref<T>(items: &[T]) -> PipeRef<'_, Identity, T, &T>
where
    T: Sync,
{
    PipeRef {
        items,
        stages: Identity,
        config: PipelineConfig::default(),
        compute_pool: None,
        oversubscribe: None,
        _marker: PhantomData,
    }
}

/// A type-state, data-first fused pipeline over a **borrowed** input slice.
/// Obtained from [`pipe_ref`]; mirrors [`Pipe`](crate::Pipe) with the input
/// fixed to `&'a [T]` and items flowing as `&'a T`.
///
/// The `'a` lifetime brands every stage closure, so closures may borrow
/// stack-local data without [`scope`](crate::scope) — soundness rests on the
/// terminal blocking until every worker finishes (same invariant as
/// [`ScopedPipe`](crate::ScopedPipe)).
pub struct PipeRef<'a, S = Identity, T = (), O = ()> {
    items: &'a [T],
    stages: S,
    config: PipelineConfig,
    /// Custom compute pool — see [`Pipe::with_compute_pool`](crate::Pipe::with_compute_pool).
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor — see [`Pipe::with_oversubscribe`](crate::Pipe::with_oversubscribe).
    oversubscribe: Option<NonZeroUsize>,
    _marker: PhantomData<(&'a (), O)>,
}

impl<'a, S, T, O> PipeRef<'a, S, T, O> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Tune the workload split factor. Default is [`Workload::Balanced`].
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.config.workload = workload;
        self
    }

    /// Set the compute-pool worker budget — see
    /// [`Pipe::with_compute_workers`](crate::Pipe::with_compute_workers).
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Attach a custom [`ComputePool`] — see
    /// [`Pipe::with_compute_pool`](crate::Pipe::with_compute_pool).
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Oversubscribe the compute pool — see
    /// [`Pipe::with_oversubscribe`](crate::Pipe::with_oversubscribe).
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = NonZeroUsize::new(factor.max(1));
        self
    }

    /// Append a synchronous map stage: `Fn(O) -> N`. The output type changes
    /// to `N`; the input item type `&'a T` is unchanged.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Sync + 'a,
    ) -> PipeRef<'a, SyncMap<S, impl Fn(O) -> N + Sync + 'a>, T, N>
    where
        S: StageMarker<&'a T, Output = O>,
        O: Send,
        N: Send,
    {
        PipeRef {
            items: self.items,
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage. Keeps items where `f` returns `true`.
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Sync + 'a,
    ) -> PipeRef<'a, Filter<S, impl Fn(&O) -> bool + Sync + 'a>, T, O>
    where
        S: StageMarker<&'a T, Output = O>,
    {
        PipeRef {
            items: self.items,
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }

    /// Append a fallible map stage: `Fn(O) -> Result<N, E>`. Transitions the
    /// pipeline into a [`TryPipeRef`] whose `.try_collect()` returns
    /// `Result<Vec<N>, E>`. The first `Err` short-circuits the chain.
    ///
    /// The error type `E` must be `'static` (owned errors): the fast collect
    /// path routes failures through the hybrid dispatcher's type-erased
    /// slots, which downcast by concrete type. Fallible closures borrow
    /// *inputs* via `'a` freely.
    #[allow(clippy::type_complexity)] // typestate chain return — same shape as
    // `Pipe::try_map`; the `InfallibleChain` adapter lets the infallible
    // prefix compose with fallible stages.
    pub fn try_map<N, E>(
        self,
        f: impl Fn(O) -> Result<N, E> + Sync + 'a,
    ) -> TryPipeRef<
        'a,
        TryMap<InfallibleChain<S, E>, impl Fn(O) -> Result<N, E> + Sync + 'a>,
        T,
        N,
        E,
    >
    where
        S: StageMarker<&'a T, Output = O> + FusedStage<&'a T>,
        O: Send,
        N: Send,
        E: Send + 'static,
    {
        TryPipeRef {
            items: self.items,
            stages: TryMap {
                prev: InfallibleChain(self.stages, PhantomData),
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }
}

impl<'a, S, T, O> PipeRef<'a, S, T, O>
where
    S: FusedStage<&'a T, Output = O> + Sync,
    T: Sync,
    O: Send,
{
    /// Execute the fused pipeline and collect results. Drives the same
    /// recursive work-stealing / hybrid dispatch core as [`Pipe::collect`]
    /// (crate::Pipe::collect); the input slice is only read.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn collect(self) -> Vec<O> {
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        fused_collect_by_ref(self.items, self.stages, self.config.workload, pool)
    }

    /// Execute the fused pipeline, applying `f` to each output for its side
    /// effect. The scoped counterpart of [`Pipe::for_each`](crate::Pipe::for_each);
    /// no output `Vec` is allocated.
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `f`. The borrowed
    /// input is never touched (no cleanup needed on any path).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn for_each<F>(self, f: F)
    where
        F: Fn(O) + Sync,
    {
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        fused_for_each_by_ref(self.items, self.stages, f, self.config.workload, pool);
    }
}

/// A fallible fused pipeline over a **borrowed** input slice. Obtained from
/// [`PipeRef::try_map`]; mirrors [`TryPipe`](crate::TryPipe) with `'a`
/// closure bounds.
///
/// The error type `E` must be `'static` (see [`PipeRef::try_map`]).
pub struct TryPipeRef<'a, S = Identity, T = (), O = (), E = std::convert::Infallible> {
    items: &'a [T],
    stages: S,
    config: PipelineConfig,
    /// Custom compute pool — see [`Pipe::with_compute_pool`](crate::Pipe::with_compute_pool).
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor — see [`Pipe::with_oversubscribe`](crate::Pipe::with_oversubscribe).
    oversubscribe: Option<NonZeroUsize>,
    _marker: PhantomData<(&'a (), O, E)>,
}

impl<'a, S, T, O, E> TryPipeRef<'a, S, T, O, E> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Tune the workload split factor. Default is [`Workload::Balanced`].
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.config.workload = workload;
        self
    }

    /// Set the compute-pool worker budget — see
    /// [`Pipe::with_compute_workers`](crate::Pipe::with_compute_workers).
    #[must_use]
    pub fn with_compute_workers(mut self, n: usize) -> Self {
        self.config.set_compute_workers(n);
        self
    }

    /// Attach a custom [`ComputePool`] — see
    /// [`Pipe::with_compute_pool`](crate::Pipe::with_compute_pool).
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Oversubscribe the compute pool — see
    /// [`Pipe::with_oversubscribe`](crate::Pipe::with_oversubscribe).
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = NonZeroUsize::new(factor.max(1));
        self
    }

    /// Append an infallible map stage. The error type `E` is unchanged.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Sync + 'a,
    ) -> TryPipeRef<'a, SyncMap<S, impl Fn(O) -> N + Sync + 'a>, T, N, E>
    where
        S: StageMarker<&'a T, Output = O>,
        O: Send,
        N: Send,
    {
        TryPipeRef {
            items: self.items,
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage. Items where `f` returns `false` are dropped from
    /// the output (no error is signalled).
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Sync + 'a,
    ) -> TryPipeRef<'a, Filter<S, impl Fn(&O) -> bool + Sync + 'a>, T, O, E>
    where
        S: StageMarker<&'a T, Output = O>,
    {
        TryPipeRef {
            items: self.items,
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }

    /// Append another fallible map stage. The closure must produce the same
    /// error type `E` (use `.map_err()` upstream to convert).
    #[allow(clippy::type_complexity)] // typestate chain return — see `Pipe::try_map`.
    pub fn try_map<N>(
        self,
        f: impl Fn(O) -> Result<N, E> + Sync + 'a,
    ) -> TryPipeRef<'a, TryMap<S, impl Fn(O) -> Result<N, E> + Sync + 'a>, T, N, E>
    where
        S: StageMarker<&'a T, Output = O> + FusedTryStage<&'a T, Error = E>,
        O: Send,
        N: Send,
    {
        TryPipeRef {
            items: self.items,
            stages: TryMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }

    /// Convert the error type from `E` to `E2`.
    pub fn map_err<E2>(
        self,
        f: impl Fn(E) -> E2 + Sync + 'a,
    ) -> TryPipeRef<'a, MapErr<S, impl Fn(E) -> E2 + Sync + 'a>, T, O, E2>
    where
        E: Send + 'static,
        E2: Send + 'static,
    {
        TryPipeRef {
            items: self.items,
            stages: MapErr {
                prev: self.stages,
                f,
            },
            config: self.config,
            compute_pool: self.compute_pool,
            oversubscribe: self.oversubscribe,
            _marker: PhantomData,
        }
    }
}

impl<'a, S, T, O, E> TryPipeRef<'a, S, T, O, E>
where
    S: FusedTryStage<&'a T, Output = O, Error = E> + Sync,
    T: Sync,
    O: Send,
    E: Send + 'static,
{
    /// Execute the fused fallible pipeline, short-circuiting on the first
    /// error. Drives the same work-stealing core as
    /// [`TryPipe::try_collect`](crate::TryPipe::try_collect); the input slice
    /// is only read.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn try_collect(self) -> Result<Vec<O>, E> {
        let exec = resolve_exec_pool(
            self.compute_pool.as_ref(),
            self.oversubscribe,
            self.config.compute_workers,
        );
        let pool = exec.as_pool();
        fused_try_collect_by_ref(self.items, self.stages, self.config.workload, pool)
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use super::*;

    /// Parallel correctness across the hybrid chunk/steal paths: a large
    /// input must produce exactly the sequential result.
    #[test]
    fn test_pipe_ref_par_map_checksum() {
        let data: Vec<u64> = (0..20_000u64)
            .map(|i| i.wrapping_mul(2_654_435_761))
            .collect();
        let got: Vec<u64> = pipe_ref(&data)
            .map(|&x| x.wrapping_mul(3).wrapping_add(7))
            .collect();
        let want: Vec<u64> = data
            .iter()
            .map(|&x| x.wrapping_mul(3).wrapping_add(7))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn test_pipe_ref_map_filter_type_change() {
        let data: Vec<u32> = (0..1000).collect();
        let got: Vec<String> = pipe_ref(&data)
            .map(|&x: &u32| x * 2)
            .filter(|x: &u32| x % 3 == 0)
            .map(|x: u32| format!("v{x}"))
            .collect();
        let want: Vec<String> = (0..1000u32)
            .map(|x| x * 2)
            .filter(|x| x % 3 == 0)
            .map(|x| format!("v{x}"))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn test_pipe_ref_try_collect() {
        let data: Vec<i32> = (0..1000).collect();
        let got: Result<Vec<i32>, &str> = pipe_ref(&data)
            .try_map(|&x: &i32| {
                if x > 900 {
                    Err("too big")
                } else {
                    Ok(x + 1)
                }
            })
            .try_collect();
        assert_eq!(got.unwrap_err(), "too big");
    }

    #[test]
    fn test_pipe_ref_empty_and_single() {
        let empty: Vec<u64> = vec![];
        let r: Vec<u64> = pipe_ref(&empty).map(|&x| x + 1).collect();
        // Explicit element type: with serde_json in the graph (hotpath feature
        // unification), Vec<u64> == Vec<_> has two candidate PartialEq impls
        // for u64 and the empty vec's element type no longer infers.
        assert_eq!(r, Vec::<u64>::new());
        let r: Vec<u64> = pipe_ref(&[41u64]).map(|&x| x + 1).collect();
        assert_eq!(r, vec![42]);
    }

    #[test]
    fn test_pipe_ref_for_each_and_workload_pool() {
        let data: Vec<u64> = (0..10_000).collect();
        let sum = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let s = sum.clone();
        pipe_ref(&data)
            .with_workload(Workload::Unbalanced)
            .for_each(move |&x: &u64| {
                s.fetch_add(x, std::sync::atomic::Ordering::Relaxed);
            });
        assert_eq!(
            sum.load(std::sync::atomic::Ordering::Relaxed),
            (0..10_000u64).sum::<u64>()
        );

        let pool = ComputePool::new(4);
        let sum2 = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let s2 = sum2.clone();
        pipe_ref(&data)
            .with_compute_pool(pool)
            .for_each(move |&x: &u64| {
                s2.fetch_add(x, std::sync::atomic::Ordering::Relaxed);
            });
        assert_eq!(
            sum2.load(std::sync::atomic::Ordering::Relaxed),
            (0..10_000u64).sum::<u64>()
        );
    }

    /// The borrowed contract: a panicking pipeline must propagate the panic
    /// AND leave the input slice fully intact (it is shared, never consumed).
    /// Exercises both the index fast path (no filter) and the merge path
    /// (with filter) — every cleanup path must drop output state only.
    #[test]
    fn test_pipe_ref_panic_leaves_input_intact() {
        let data: Vec<u64> = (0..20_000).collect();
        let snapshot = data.clone();

        // Index fast path (no filter → MAY_FILTER == false).
        let boom = |&x: &u64| {
            assert!(x != 9_999, "boom at {x}");
            x + 1
        };
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let out: Vec<u64> = pipe_ref(&data).map(boom).collect();
            out
        }));
        assert!(r.is_err(), "panic must propagate (fast path)");
        assert_eq!(data, snapshot, "input must be untouched (fast path)");

        // Merge path (filter → MAY_FILTER == true).
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let out: Vec<u64> = pipe_ref(&data).map(boom).filter(|_: &u64| true).collect();
            out
        }));
        assert!(r.is_err(), "panic must propagate (merge path)");
        assert_eq!(data, snapshot, "input must be untouched (merge path)");
    }

    /// Panic propagation through `for_each` (sink path, no cleanup at all).
    #[test]
    fn test_pipe_ref_for_each_panic_propagates() {
        let data: Vec<u64> = (0..20_000).collect();
        let snapshot = data.clone();
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| {
            pipe_ref(&data).for_each(|&x: &u64| {
                assert!(x != 12_345, "sink boom");
            });
        }));
        assert!(r.is_err());
        assert_eq!(data, snapshot);
    }

    /// Panic propagation through `try_collect`'s fast path.
    #[test]
    fn test_pipe_ref_try_panic_propagates() {
        let data: Vec<u64> = (0..20_000).collect();
        let snapshot = data.clone();
        let r = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let out: Result<Vec<u64>, &'static str> = pipe_ref(&data)
                .try_map(|&x: &u64| {
                    assert!(x != 500, "try boom");
                    Ok(x)
                })
                .try_collect();
            out.ok();
        }));
        assert!(r.is_err());
        assert_eq!(data, snapshot);
    }

    /// Closures may borrow stack-local data without `scope` — the `'a`
    /// branding covers both the input and captured environment.
    #[test]
    fn test_pipe_ref_closure_borrows_stack_data() {
        let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
        let threshold = 3;
        let lens: Vec<usize> = pipe_ref(&table)
            .map(|s: &String| s.len())
            .filter(|&l: &usize| l > threshold + 1) // borrows `threshold`
            .collect();
        let want: Vec<usize> = table
            .iter()
            .map(String::len)
            .filter(|&l| l > threshold + 1)
            .collect();
        assert_eq!(lens, want);
    }
}
