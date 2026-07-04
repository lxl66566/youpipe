use std::marker::PhantomData;

use crate::{
    builder::{
        Filter, FusedStage, Identity, PipelineConfig, StageMarker, SyncMap, Workload,
        fused_collect_scoped, fused_for_each_scoped,
    },
    executor::compute::ComputePool,
};

/// Opens a scoped execution context for non-`'static` closures.
///
/// Closures passed to [`ScopedPipe`]'s `.map()` / `.filter()` / `.collect()`
/// may borrow local variables by shared reference for the duration of `scope`.
/// This is the headline feature: a stack-local lookup table, cache, or config
/// can be read concurrently from every worker without cloning or `Arc`-ing it.
///
/// ```rust
/// # use youpipe::scope;
/// // A non-Copy, non-'static lookup table borrowed by every parallel worker.
/// let table: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
/// let lengths: Vec<usize> = scope(|s| {
///     s.pipe(0..table.len())
///         .map(|i: usize| table[i].len()) // borrows `table`
///         .collect()
/// });
/// ```
pub fn scope<'env, F, R>(f: F) -> R
where
    F: FnOnce(&PipelineScope<'env>) -> R,
{
    let scope = PipelineScope {
        _marker: PhantomData,
    };
    f(&scope)
}

/// Scoped execution context — the lifetime `'env` brands every closure passed
/// to [`ScopedPipe`] so the borrow checker enforces "outlives the scope".
pub struct PipelineScope<'env> {
    _marker: PhantomData<&'env ()>,
}

impl<'env> PipelineScope<'env> {
    /// Start a scoped, lazily-fused, **data-first** pipeline.
    ///
    /// `items` may be any iterator; they are eagerly collected into a `Vec`
    /// (matching the rest of the youpipe API). `T` is inferred from the
    /// iterator's item type.
    ///
    /// **Borrowing a slice without cloning.** Passing `&[T]` (or `&Vec<T>`)
    /// yields `ScopedPipe<'env, _, &'env T, &'env T>`: items enter the
    /// pipeline as shared references, so chained closures read each item
    /// without owning or cloning it. The only allocation is one `Vec<&T>` of
    /// `n` pointers — the youpipe counterpart of rayon's `slice.par_iter()`,
    /// and much cheaper than cloning `T` for non-`Copy` types like `PathBuf`.
    ///
    /// ```rust
    /// # use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// # use youpipe::scope;
    /// let files: Vec<String> = (0..10).map(|i| format!("file{i}")).collect();
    /// let total_len = Arc::new(AtomicUsize::new(0));
    /// let t = total_len.clone();
    /// scope(|s| {
    ///     // `&files` borrows — no String clone, just one Vec<&String>.
    ///     s.pipe(&files).for_each(move |f: &String| {
    ///         t.fetch_add(f.len(), Ordering::Relaxed);
    ///     });
    /// });
    /// assert_eq!(total_len.load(Ordering::Relaxed),
    ///            files.iter().map(String::len).sum::<usize>());
    /// ```
    ///
    /// For zero input allocation, pass indices instead:
    /// `s.pipe(0..slice.len()).for_each(|i: usize| f(&slice[i]))` — only one
    /// `Vec<usize>` (8 bytes/item) and no `T` clone.
    #[must_use]
    pub fn pipe<I, It>(&self, items: It) -> ScopedPipe<'env, Identity, I, I>
    where
        It: IntoIterator<Item = I>,
        I: Send,
    {
        ScopedPipe {
            items: items.into_iter().collect(),
            stages: Identity,
            config: PipelineConfig::default(),
            compute_pool: None,
            oversubscribe: None,
            _marker: PhantomData,
        }
    }
}

/// A pipeline that may borrow non-`'static` data from the enclosing
/// [`scope`]. Mirrors [`crate::Pipe`]'s stage chain but with the `'env`
/// lifetime propagated through every bound, so closures like `|x| x * factor`
/// can capture stack-local `factor` by reference.
///
/// `I` / `O` track the pipeline input and current output types separately
/// (same design as [`crate::Pipe`]), so type-changing maps like
/// `i32 -> String` compile.
///
/// Unlike the previous `ScopedPipeline` (which required `T: 'static`,
/// pre-allocating `parallelism` chunks, and serialising results through
/// `Arc<Vec<Mutex<Vec<T>>>>`), this version:
///
/// - has no `'static` bound on `T` or the closure,
/// - is **data-first** (items are passed to `pipe(items)`, not to
///   `.collect()`),
/// - fuses `.map`/`.filter`/ at compile time (lazy chain),
/// - drives `.collect()` through the same recursive work-stealing
///   `par_index_collect` core as the top-level [`crate::Pipe`].
pub struct ScopedPipe<'env, S = Identity, I = (), O = ()> {
    items: Vec<I>,
    stages: S,
    config: PipelineConfig,
    /// Custom compute pool — see [`crate::Pipe::with_compute_pool`].
    compute_pool: Option<ComputePool>,
    /// Oversubscribe factor — see [`crate::Pipe::with_oversubscribe`].
    oversubscribe: Option<std::num::NonZeroUsize>,
    _marker: PhantomData<(&'env (), O)>,
}

impl<'env, S, I, O> ScopedPipe<'env, S, I, O> {
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

    /// Attach a custom [`ComputePool`] — see
    /// [`crate::Pipe::with_compute_pool`].
    ///
    /// The primary use case inside a [`scope`] is oversubscribing threads for
    /// blocking-IO sync workloads (e.g. file encryption/decryption) while still
    /// borrowing stack-local data (key caches, lookup tables) by reference.
    #[must_use]
    pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
        self.compute_pool = Some(pool);
        self
    }

    /// Oversubscribe the compute pool — see
    /// [`crate::Pipe::with_oversubscribe`] for the full guidance. Same
    /// semantics: creates a transient `factor × num_cpus` thread pool at
    /// `.collect()` / `.for_each()` time.
    ///
    /// Inside a [`scope`] this is the shortest path to oversubscription for a
    /// blocking-IO workload that borrows stack-local data:
    ///
    /// ```rust
    /// use youpipe::scope;
    ///
    /// let key_cache: Vec<u8> = vec![0x42; 32];
    /// scope(|s| {
    ///     s.pipe(0..100)
    ///         .with_oversubscribe(2)
    ///         .map(|i: i32| i + key_cache[0] as i32)
    ///         .for_each(|_| { /* blocking IO */ });
    /// });
    /// ```
    #[must_use]
    pub fn with_oversubscribe(mut self, factor: usize) -> Self {
        self.oversubscribe = std::num::NonZeroUsize::new(factor.max(1));
        self
    }

    /// Append a synchronous map stage: `Fn(O) -> N`. The output type changes
    /// to `N`; the pipeline input `I` is unchanged.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Sync + 'env,
    ) -> ScopedPipe<'env, SyncMap<S, impl Fn(O) -> N + Sync + 'env>, I, N>
    where
        S: StageMarker<I, Output = O>,
        O: Send,
        N: Send,
    {
        ScopedPipe {
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

    /// Append a filter stage.
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Sync + 'env,
    ) -> ScopedPipe<'env, Filter<S, impl Fn(&O) -> bool + Sync + 'env>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        ScopedPipe {
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
}

impl<S, I, O> ScopedPipe<'_, S, I, O>
where
    S: FusedStage<I, Output = O> + Sync,
    I: Send,
    O: Send,
{
    /// Execute the fused pipeline over the stored items and collect results.
    ///
    /// Uses the same recursive work-stealing `par_index_collect` core as
    /// top-level `Pipe::collect` — no `'static` bound required. When the
    /// stage chain contains a `Filter`, falls back to per-leaf `Vec` merge.
    pub fn collect(self) -> Vec<O> {
        let exec =
            crate::builder::resolve_exec_pool(self.compute_pool.as_ref(), self.oversubscribe);
        let pool = exec.as_pool();
        fused_collect_scoped(self.items, self.stages, self.config.workload, pool)
    }

    /// Execute the fused pipeline, applying `f` to each output for its side
    /// effect. The scoped counterpart of [`crate::Pipe::for_each`].
    ///
    /// No output `Vec` is allocated — optimal for side-effect terminals that
    /// borrow stack-local data from the surrounding [`scope`]. Filter stages
    /// are honoured: items dropped by an upstream filter are not passed to
    /// `f`.
    ///
    /// ```rust
    /// # use std::sync::{Arc, atomic::{AtomicI32, Ordering}};
    /// # use youpipe::scope;
    /// let table: Vec<i32> = (0..10).collect();
    /// // `for_each` takes `Fn + Sync` (matches rayon): accumulate via
    /// // atomics, not `&mut` capture. `&table` borrows — no clone of i32.
    /// let sum = Arc::new(AtomicI32::new(0));
    /// let s = sum.clone();
    /// scope(|scope| {
    ///     scope.pipe(&table).for_each(move |v: &i32| {
    ///         s.fetch_add(*v, Ordering::Relaxed);
    ///     });
    /// });
    /// assert_eq!(sum.load(Ordering::Relaxed), (0..10).sum::<i32>());
    /// ```
    ///
    /// # Panics
    ///
    /// Propagates any panic raised by the stage chain or `f`.
    pub fn for_each<F>(self, f: F)
    where
        F: Fn(O) + Sync,
    {
        let exec =
            crate::builder::resolve_exec_pool(self.compute_pool.as_ref(), self.oversubscribe);
        let pool = exec.as_pool();
        fused_for_each_scoped(self.items, self.stages, f, self.config.workload, pool);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_basic() {
        let multiplier = 3i32;
        let result = scope(|s| {
            s.pipe([1, 2, 3, 4, 5])
                .map(|x: i32| x * multiplier)
                .collect()
        });
        assert_eq!(result, vec![3, 6, 9, 12, 15]);
    }

    #[test]
    fn test_scope_par_map_via_collect() {
        // ScopedPipe::collect now drives the recursive work-stealing core,
        // so large inputs parallelise automatically.
        let offset = 100i32;
        let result = scope(|s| s.pipe(0..1000).map(|x: i32| x + offset).collect());
        let expected: Vec<i32> = (100..1100).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scope_chained_map_filter() {
        let factor = 7i32;
        let result = scope(|s| {
            s.pipe(0..30)
                .map(|x: i32| x * factor)
                .filter(|x: &i32| *x % 2 == 0)
                .map(|x: i32| x + 1)
                .collect()
        });
        let expected: Vec<i32> = (0..30)
            .map(|x| x * 7)
            .filter(|x| x % 2 == 0)
            .map(|x| x + 1)
            .collect();
        assert_eq!(result, expected);
    }

    /// The headline scope feature: borrow a *non-Copy, non-`'static`* value
    /// from outside the scope and use it inside the parallel closures. The
    /// previous `T: 'static` bound made this impossible — the closure would
    /// have failed to capture `&cached` because `'env` did not satisfy
    /// `'static`.
    #[test]
    fn test_scope_truly_non_static_borrow() {
        let cached: Vec<String> = (0..5).map(|i| format!("val{i}^2={}", i * i)).collect();
        // Borrow `cached` (non-Copy, lifetime-locked to the test frame) inside
        // the closure. Same-type map (`usize -> usize`) keeps us inside the
        // type-state constraint that `Pipeline` also imposes.
        let expected: Vec<usize> = cached.iter().map(String::len).collect();
        let result: Vec<usize> = scope(|s| {
            s.pipe(0..cached.len())
                .map(|i: usize| cached[i].len())
                .collect()
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scope_empty() {
        let result = scope(|s| {
            let items: Vec<i32> = vec![];
            s.pipe(items).map(|x: i32| x * 2).collect()
        });
        assert!(result.is_empty());
    }

    /// Scoped pipelines support type-changing maps too (`i32 -> String`),
    /// mirroring the top-level `Pipeline<S, I, O>` fix.
    #[test]
    fn test_scope_type_changing_map() {
        let suffix = "!".to_string();
        let result: Vec<String> = scope(|s| {
            s.pipe(0..3)
                .map(|x: i32| x * 2)
                .map(|x: i32| format!("{x}{suffix}"))
                .collect()
        });
        assert_eq!(result, vec!["0!", "2!", "4!"]);
    }

    /// Multiple parallel pipelines in the same scope sharing one stack-local
    /// lookup table — the practical pattern that `scope` unlocks. Each
    /// `pipe(...)` borrows `table` by reference; without scope you'd have to
    /// `Arc` it or `clone()` per pipeline.
    #[test]
    fn test_scope_shared_lookup_across_pipelines() {
        let table: Vec<u64> = (0..1000u64)
            .map(|i| i.wrapping_mul(2_654_435_761))
            .collect();
        let total = scope(|s| {
            let lookup_hits: usize = s
                .pipe(0..table.len())
                .map(|i: usize| usize::from(table[i] % 2 == 0))
                .collect()
                .into_iter()
                .sum();
            let sum: u64 = s
                .pipe(0..table.len())
                .map(|i: usize| table[i])
                .collect()
                .into_iter()
                .sum();
            (lookup_hits, sum)
        });
        // Sanity: every value the table holds equals the same computation done
        // sequentially. If the scope's borrowed refs were unsound, the sums
        // would diverge here.
        let expected_total: u64 = (0..1000u64).map(|i| i.wrapping_mul(2_654_435_761)).sum();
        let expected_hits: usize = (0..1000u64)
            .map(|i| i.wrapping_mul(2_654_435_761))
            .filter(|v| v % 2 == 0)
            .count();
        assert_eq!(total.0, expected_hits);
        assert_eq!(total.1, expected_total);
    }

    /// ScopedPipe supports a custom ComputePool — the headline use case is
    /// oversubscribing threads for blocking-IO workloads (e.g. file
    /// encryption) while still borrowing stack-local data by reference.
    #[test]
    fn test_scope_with_compute_pool() {
        let multiplier = 7i32;
        let pool = ComputePool::new(4);
        let result = scope(|s| {
            s.pipe(0..1000)
                .with_compute_pool(pool)
                .map(|x: i32| x * multiplier)
                .collect()
        });
        let expected: Vec<i32> = (0..1000).map(|x| x * 7).collect();
        assert_eq!(result, expected);
    }

    /// `for_each` with a custom pool inside a scope, borrowing a stack-local
    /// table.
    #[test]
    fn test_scope_with_compute_pool_for_each() {
        let table: Vec<i32> = (0..500).collect();
        let pool = ComputePool::new(4);
        let sum = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let s = sum.clone();
        scope(|scope| {
            scope
                .pipe(&table)
                .with_compute_pool(pool)
                .for_each(move |v: &i32| {
                    s.fetch_add(i64::from(*v), std::sync::atomic::Ordering::Relaxed);
                });
        });
        let expected: i64 = (0..500i64).sum();
        assert_eq!(sum.load(std::sync::atomic::Ordering::Relaxed), expected);
    }

    /// `with_oversubscribe` inside a scope — the convenience method works
    /// identically to `with_compute_pool` but without manual pool creation.
    #[test]
    fn test_scope_with_oversubscribe() {
        let multiplier = 5i32;
        let result = scope(|s| {
            s.pipe(0..1000)
                .with_oversubscribe(2)
                .map(|x: i32| x * multiplier)
                .collect()
        });
        let expected: Vec<i32> = (0..1000).map(|x| x * 5).collect();
        assert_eq!(result, expected);
    }
}
