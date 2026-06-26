use std::marker::PhantomData;

use crate::builder::{
    Filter, FusedStage, Identity, PipelineConfig, StageMarker, SyncMap, Workload,
    fused_collect_scoped,
};

/// Opens a scoped execution context for non-`'static` closures.
///
/// Closures passed to [`ScopedPipeline`]'s `.map()` / `.filter()` /
/// `.collect()` may borrow local variables by shared reference for the duration
/// of `scope`.
///
/// ```rust
/// # use youpipe::scope;
/// let factor = 7i32;
/// let result = scope(|s| {
///     let items: Vec<i32> = (0..10).collect();
///     s.pipeline()
///         .map(|x: i32| x * factor)   // borrows stack-local `factor`
///         .collect(items)
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
/// to [`ScopedPipeline`] so the borrow checker enforces "outlives the scope".
pub struct PipelineScope<'env> {
    _marker: PhantomData<&'env ()>,
}

impl<'env> PipelineScope<'env> {
    /// Start a scoped, lazily-fused pipeline. `T` is inferred from the first
    /// `.map` / `.filter` call, so callers do not need to spell it out.
    #[must_use]
    pub fn pipeline<T: Send>(&self) -> ScopedPipeline<'env, Identity, T, T> {
        ScopedPipeline {
            stages: Identity,
            config: PipelineConfig::default(),
            _marker: PhantomData,
        }
    }
}

/// A pipeline that may borrow non-`'static` data from the enclosing
/// [`scope`]. Mirrors [`crate::builder::Pipeline`]'s stage chain but with the
/// `'env` lifetime propagated through every bound, so closures like
/// `|x| x * factor` can capture stack-local `factor` by reference.
///
/// `I` / `O` track the pipeline input and current output types separately
/// (same design as [`crate::builder::Pipeline`]), so type-changing maps like
/// `i32 -> String` compile.
///
/// Unlike the previous `ScopedPipeline` (which required `T: 'static`,
/// pre-allocating `parallelism` chunks, and serialising results through
/// `Arc<Vec<Mutex<Vec<T>>>>`), this version:
///
/// - has no `'static` bound on `T` or the closure,
/// - fuses `.map`/`.filter`/`.par_map` at compile time (lazy chain),
/// - drives `.collect()` through the same recursive work-stealing
///   `par_index_collect` core as the top-level `Pipeline`.
pub struct ScopedPipeline<'env, S = Identity, I = (), O = ()> {
    stages: S,
    config: PipelineConfig,
    _marker: PhantomData<(&'env (), I, O)>,
}

impl<'env, S, I, O> ScopedPipeline<'env, S, I, O> {
    /// Override the default [`PipelineConfig`].
    #[must_use]
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// Append a synchronous map stage: `Fn(O) -> N`. The output type changes
    /// to `N`; the pipeline input `I` is unchanged.
    pub fn map<N>(
        self,
        f: impl Fn(O) -> N + Sync + 'env,
    ) -> ScopedPipeline<'env, SyncMap<S, impl Fn(O) -> N + Sync + 'env>, I, N>
    where
        S: StageMarker<I, Output = O>,
        O: Send,
        N: Send,
    {
        ScopedPipeline {
            stages: SyncMap {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }

    /// Append a filter stage.
    pub fn filter(
        self,
        f: impl Fn(&O) -> bool + Sync + 'env,
    ) -> ScopedPipeline<'env, Filter<S, impl Fn(&O) -> bool + Sync + 'env>, I, O>
    where
        S: StageMarker<I, Output = O>,
    {
        ScopedPipeline {
            stages: Filter {
                prev: self.stages,
                f,
            },
            config: self.config,
            _marker: PhantomData,
        }
    }
}

impl<'env, S, I, O> ScopedPipeline<'env, S, I, O>
where
    S: FusedStage<I, Output = O> + Sync + 'env,
    I: Send + 'env,
    O: Send + 'env,
{
    /// Execute the fused pipeline over `items` and collect results.
    ///
    /// Uses the same recursive work-stealing `par_index_collect` core as
    /// top-level `Pipeline::collect` — no `'static` bound required. When the
    /// stage chain contains a `Filter`, falls back to per-leaf `Vec` merge.
    pub fn collect(self, items: Vec<I>) -> Vec<O> {
        fused_collect_scoped(items, self.stages, self.config.workload)
    }

    /// Tune the workload split factor. Default is `Workload::Balanced`.
    #[must_use]
    pub fn with_workload(mut self, workload: Workload) -> Self {
        self.config.workload = workload;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_basic() {
        let multiplier = 3i32;
        let result = scope(|s| {
            let items = vec![1, 2, 3, 4, 5];
            s.pipeline().map(|x: i32| x * multiplier).collect(items)
        });
        assert_eq!(result, vec![3, 6, 9, 12, 15]);
    }

    #[test]
    fn test_scope_par_map_via_collect() {
        // ScopedPipeline::collect now drives the recursive work-stealing core,
        // so large inputs parallelise automatically.
        let offset = 100i32;
        let result = scope(|s| {
            let items: Vec<i32> = (0..1000).collect();
            s.pipeline().map(|x: i32| x + offset).collect(items)
        });
        let expected: Vec<i32> = (100..1100).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scope_chained_map_filter() {
        let factor = 7i32;
        let result = scope(|s| {
            let items: Vec<i32> = (0..30).collect();
            s.pipeline()
                .map(|x: i32| x * factor)
                .filter(|x: &i32| *x % 2 == 0)
                .map(|x: i32| x + 1)
                .collect(items)
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
            let items: Vec<usize> = (0..cached.len()).collect();
            s.pipeline().map(|i: usize| cached[i].len()).collect(items)
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_scope_empty() {
        let result = scope(|s| {
            let items: Vec<i32> = vec![];
            s.pipeline().map(|x: i32| x * 2).collect(items)
        });
        assert!(result.is_empty());
    }

    /// Scoped pipelines support type-changing maps too (`i32 -> String`),
    /// mirroring the top-level `Pipeline<S, I, O>` fix.
    #[test]
    fn test_scope_type_changing_map() {
        let suffix = "!".to_string();
        let result: Vec<String> = scope(|s| {
            let items: Vec<i32> = (0..3).collect();
            s.pipeline()
                .map(|x: i32| x * 2)
                .map(|x: i32| format!("{x}{suffix}"))
                .collect(items)
        });
        assert_eq!(result, vec!["0!", "2!", "4!"]);
    }
}
