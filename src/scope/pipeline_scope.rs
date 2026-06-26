use std::{marker::PhantomData, sync::Arc};

use crate::util::split_chunks;

/// Opens a scoped execution context for non-`'static` closures.
///
/// Closures can borrow local variables by reference.
pub fn scope<'env, F, R>(f: F) -> R
where
    F: FnOnce(&PipelineScope<'env>) -> R,
{
    let scope = PipelineScope {
        _marker: PhantomData,
    };
    f(&scope)
}

/// Scoped execution context — allows borrowing non-`'static` data.
pub struct PipelineScope<'env> {
    _marker: PhantomData<&'env ()>,
}

impl<'env> PipelineScope<'env> {
    /// Create a scoped pipeline from a `Vec<T>`.
    #[must_use]
    pub fn pipeline<T: Send + 'static>(&self, items: Vec<T>) -> ScopedPipeline<'env, T> {
        ScopedPipeline {
            items: Some(items),
            runners: Vec::new(),
            result_collector: None,
            _marker: PhantomData,
        }
    }
}

type SlotCollector<T> = Arc<Vec<std::sync::Mutex<Vec<T>>>>;

/// A pipeline that can borrow non-`'static` data from the enclosing scope.
pub struct ScopedPipeline<'env, T: Send + 'static> {
    items: Option<Vec<T>>,
    runners: Vec<Box<dyn FnOnce() + Send + 'env>>,
    result_collector: Option<(SlotCollector<T>, usize)>,
    _marker: PhantomData<&'env ()>,
}

impl<'env, T: Send + 'static> ScopedPipeline<'env, T> {
    /// Sequential map within the scope.
    pub fn map<O: Send + 'static>(
        self,
        f: impl Fn(T) -> O + Send + Clone + 'env,
    ) -> ScopedPipeline<'env, O> {
        let items = self.items.expect("items already consumed");
        let mapped_items = items.into_iter().map(f).collect();
        ScopedPipeline {
            items: Some(mapped_items),
            runners: self.runners,
            result_collector: None,
            _marker: PhantomData,
        }
    }

    /// Parallel map within the scope using `parallelism` workers.
    pub fn par_map<O: Send + 'static>(
        self,
        f: impl Fn(T) -> O + Send + Sync + Clone + 'env,
        parallelism: usize,
    ) -> ScopedPipeline<'env, O> {
        let items = self.items.expect("items already consumed");
        let n = items.len();
        if n == 0 || parallelism <= 1 {
            let mapped: Vec<O> = items.into_iter().map(&f).collect();
            return ScopedPipeline {
                items: Some(mapped),
                runners: self.runners,
                result_collector: None,
                _marker: PhantomData,
            };
        }

        let chunks = split_chunks(items, parallelism);
        let num_chunks = chunks.len();

        let slots: SlotCollector<O> = Arc::new(
            (0..num_chunks)
                .map(|_| std::sync::Mutex::new(Vec::new()))
                .collect(),
        );

        let mut runners = self.runners;
        for (idx, chunk) in chunks.into_iter().enumerate() {
            let f = f.clone();
            let slots = slots.clone();
            runners.push(Box::new(move || {
                let mapped: Vec<O> = chunk.into_iter().map(f).collect();
                *slots[idx].lock().unwrap() = mapped;
            }));
        }

        ScopedPipeline {
            items: None,
            runners,
            result_collector: Some((slots as SlotCollector<O>, num_chunks)),
            _marker: PhantomData,
        }
    }

    /// Execute all deferred work and collect results.
    #[must_use]
    pub fn collect(self) -> Vec<T> {
        if let Some(items) = self.items {
            return items;
        }
        let runners = self.runners;
        std::thread::scope(|s| {
            for runner in runners {
                s.spawn(runner);
            }
        });

        if let Some((slots, num_chunks)) = self.result_collector {
            let mut result = Vec::new();
            for i in 0..num_chunks {
                result.extend(std::mem::take(&mut *slots[i].lock().unwrap()));
            }
            return result;
        }

        Vec::new()
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
            s.pipeline(items).map(|x: i32| x * multiplier).collect()
        });
        assert_eq!(result, vec![3, 6, 9, 12, 15]);
    }

    #[test]
    fn test_scope_par_map() {
        let offset = 100i32;
        let result = scope(|s| {
            let items: Vec<i32> = (0..20).collect();
            s.pipeline(items).par_map(|x: i32| x + offset, 4).collect()
        });
        let mut r = result;
        r.sort_unstable();
        let expected: Vec<i32> = (100..120).collect();
        assert_eq!(r, expected);
    }

    #[test]
    fn test_scope_non_static_borrow() {
        let factor = 10i32;
        scope(|s| {
            let items: Vec<i32> = (0..5).collect();
            let _results = s.pipeline(items).map(|x: i32| x * factor).collect();
        });
    }
}
