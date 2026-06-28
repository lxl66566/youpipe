use std::hint;

// ── RangeOp: how a leaf transforms an input item ──
//
/// Compile-time-fused transform applied to every item by the range-based core.
///
/// The leaf loop calls `apply` directly (no `Option`/branch) — this is critical
/// for vectorizing the lightweight `x + 1`-style hot loop, where an `Option`
/// discriminant + branch cuts LLVM's auto-vectorizer and costs ~2.5× on the 1 M
/// warm `par_map` path (measured: 710 µs → 290 µs, matching rayon).
///
/// `RangeOp` is therefore only ever constructed for stages whose
/// `FusedStage::MAY_FILTER == false`; the filtering path uses the per-leaf
/// `Vec` merge in `join_fused_collect` instead. This invariant is what makes
/// `Slots::drop_range` sound over arbitrary sub-ranges in the panic cleanup:
/// every output slot the leaf visits is unconditionally written.
pub(super) trait RangeOp<T>: Sync {
    type Out: Send;
    fn apply(&self, item: T) -> Self::Out;
}

// ── Marker traits ──

/// Type-level marker for a pipeline stage. Maps `Input` to `Self::Output`.
pub trait StageMarker<Input> {
    type Output;
}

/// Identity stage — passes items through unchanged.
#[derive(Clone)]
pub struct Identity;

impl<T> StageMarker<T> for Identity {
    type Output = T;
}

/// Synchronous map stage: `Fn(T) -> O`. Used by both infallible `Pipe` and
/// fallible `TryPipe` chains — it impls both `FusedStage` and `FusedTryStage`.
#[derive(Clone)]
pub struct SyncMap<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I, O> StageMarker<I> for SyncMap<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
}

/// Filter stage: keeps items where `Fn(&T) -> bool` returns `true`.
#[derive(Clone)]
pub struct Filter<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I> StageMarker<I> for Filter<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
}

/// Fallible map stage: `Fn(T) -> Result<O, E>`. Short-circuits the chain on
/// `Err`. The error type `E` is fixed across the whole fallible chain — every
/// subsequent `try_map` must produce the same `E`.
#[derive(Clone)]
pub struct TryMap<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I, O, E> StageMarker<I> for TryMap<Prev, F>
where
    Prev: StageMarker<I>,
    F: Fn(Prev::Output) -> Result<O, E>,
{
    type Output = O;
}

// ── FusedStage trait (infallible chain) ──

/// Compile-time fused stage: applies multiple pipeline stages in a single pass
/// without intermediate allocations. Used by the infallible `Pipe` chain.
pub trait FusedStage<T> {
    type Output;

    /// Whether `apply` may return `None` for an input it received (i.e. the
    /// stage chain contains a `Filter`). When `false`, the index-based collect
    /// fast path can assume every output slot it visits is init, which makes
    /// panic cleanup trivially sound (no per-slot validity tracking).
    const MAY_FILTER: bool = false;

    /// Apply the full fused chain. `Filter` stages may return `None`.
    fn apply(&self, item: T) -> Option<Self::Output>;

    /// Apply the full fused chain without the `Option` wrapper.
    ///
    /// Used by the hot path (`RangeOp` → `par_index_leaf`) so the leaf loop
    /// stays branch-free and vectorizable. Default impl extracts the `Option`
    /// payload, which is sound IFF the entire chain has `MAY_FILTER = false`.
    ///
    /// Each stage overrides this to thread the value through `prev.apply_pure`
    /// so no `Option` is ever constructed on the pure path.
    ///
    /// # Panics
    ///
    /// May panic (caught by the leaf's `LeafGuard`).
    #[inline]
    fn apply_pure(&self, item: T) -> Self::Output {
        // SAFETY: contract — only call `apply_pure` when `Self::MAY_FILTER`
        // is false throughout the chain. `Pipe::collect` enforces this.
        match self.apply(item) {
            Some(v) => v,
            // SAFETY: caller guarantees `MAY_FILTER = false`, so this is
            // unreachable.
            None => unsafe { hint::unreachable_unchecked() },
        }
    }
}

impl<T> FusedStage<T> for Identity {
    type Output = T;
    fn apply(&self, item: T) -> Option<T> {
        Some(item)
    }
    #[inline]
    fn apply_pure(&self, item: T) -> T {
        item
    }
}

impl<Prev, F, I, O> FusedStage<I> for SyncMap<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<O> {
        self.prev.apply(item).map(|v| (self.f)(v))
    }
    #[inline]
    fn apply_pure(&self, item: I) -> O {
        let v = self.prev.apply_pure(item);
        (self.f)(v)
    }
}

impl<Prev, F, I> FusedStage<I> for Filter<Prev, F>
where
    Prev: FusedStage<I>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
    // A filter can drop items, so the fast path cannot assume all slots init.
    const MAY_FILTER: bool = true;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item).filter(|v| (self.f)(v))
    }
    // No `apply_pure` override: `Filter` always has `MAY_FILTER = true`, so
    // the pure path is never taken through a `Filter` chain.
}

/// `RangeOp` wrapper around a `FusedStage` so the index-based core can drive
/// the compile-time-fused stage chain.
///
/// Only constructable when `S::MAY_FILTER == false` (enforced by
/// `Pipe::collect`'s dispatch on `S::MAY_FILTER`). The `RangeOp::apply`
/// impl goes through `FusedStage::apply_pure`, which avoids constructing an
/// `Option` at all — keeping the leaf loop branch-free for the vectorizer.
pub(super) struct FusedOp<S>(pub(super) S);

impl<S, T> RangeOp<T> for FusedOp<S>
where
    S: FusedStage<T> + Sync,
    S::Output: Send,
{
    type Out = S::Output;
    #[inline]
    fn apply(&self, item: T) -> S::Output {
        self.0.apply_pure(item)
    }
}

// ── RangeTryOp: fallible variant for the try-index fast path ──

/// Fallible transform applied to every item by the try-index-based core.
///
/// Like [`RangeOp`] but returns `Result<R, E>`. Used by `par_index_try_leaf`
/// for `TryPipe::try_collect` when the chain has `MAY_FILTER == false`. The
/// `Result` lets the leaf short-circuit on the first error without
/// constructing an `Option` per item.
pub(super) trait RangeTryOp<T>: Sync {
    type Out: Send;
    type Error: Send;
    fn try_apply(&self, item: T) -> Result<Self::Out, Self::Error>;
}

/// `RangeTryOp` wrapper around a `FusedTryStage` chain. Only constructable
/// when `S::MAY_FILTER == false` — the impl unwraps the `Option` from
/// `FusedTryStage::try_apply` via `unreachable_unchecked`, keeping the leaf
/// branch-free.
pub(super) struct FusedTryOp<S>(pub(super) S);

impl<S, T> RangeTryOp<T> for FusedTryOp<S>
where
    S: FusedTryStage<T> + Sync,
    S::Output: Send,
    S::Error: Send,
{
    type Out = S::Output;
    type Error = S::Error;
    #[inline]
    fn try_apply(&self, item: T) -> Result<S::Output, S::Error> {
        // SAFETY: `FusedTryOp` is only constructed when `MAY_FILTER == false`,
        // so `try_apply` always returns `Ok(Some(_))` on success.
        match S::try_apply(&self.0, item) {
            Ok(Some(o)) => Ok(o),
            Ok(None) => unsafe { hint::unreachable_unchecked() },
            Err(e) => Err(e),
        }
    }
}

// ── FusedTryStage trait (fallible chain) ──

/// Compile-time fused stage for a fallible pipeline. The chain threads
/// `Result<_, E>` through every stage via `?`, so the first `Err` aborts the
/// per-item transform. The error type `E` is fixed across the whole chain
/// (every `try_map` must produce the same `E`); convert upstream with
/// `.map_err(|e| AppError::from(e))` if a downstream stage produces a different
/// error type.
///
/// `try_apply` returns `Result<Option<Output>, Error>`: the `Option` allows
/// `Filter` stages to drop items even after a `try_map` boundary, and keeps the
/// stage types composable between `Pipe` and `TryPipe`. For chains without any
/// filter, `Option` is always `Some(_)` and the discriminant is a no-op in the
/// monomorphized leaf.
pub trait FusedTryStage<T> {
    type Output;
    type Error;

    /// Whether `try_apply` may return `Ok(None)` (i.e. the chain contains a
    /// `Filter`). When `false`, every `Ok` result carries a value and the
    /// output cardinality equals the input cardinality — the index-based fast
    /// path (`par_index_try_collect`) can pre-allocate the output buffer and
    /// write results at known indices, avoiding the `Vec`-merge overhead.
    const MAY_FILTER: bool = false;

    /// Apply the chain.
    ///
    /// * `Ok(Some(o))` — stage produced a value.
    /// * `Ok(None)` — item filtered out by an upstream `Filter`.
    /// * `Err(e)` — stage failed; abort the chain.
    fn try_apply(&self, item: T) -> Result<Option<Self::Output>, Self::Error>;
}

impl<T> FusedTryStage<T> for Identity {
    type Output = T;
    type Error = std::convert::Infallible;

    #[inline]
    fn try_apply(&self, item: T) -> Result<Option<T>, std::convert::Infallible> {
        Ok(Some(item))
    }
}

impl<Prev, F, I, O, E> FusedTryStage<I> for SyncMap<Prev, F>
where
    Prev: FusedTryStage<I, Error = E>,
    F: Fn(Prev::Output) -> O,
{
    type Output = O;
    type Error = E;
    const MAY_FILTER: bool = Prev::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<O>, E> {
        match self.prev.try_apply(item)? {
            Some(v) => Ok(Some((self.f)(v))),
            None => Ok(None),
        }
    }
}

impl<Prev, F, I, E> FusedTryStage<I> for Filter<Prev, F>
where
    Prev: FusedTryStage<I, Error = E>,
    F: Fn(&Prev::Output) -> bool,
{
    type Output = Prev::Output;
    type Error = E;
    const MAY_FILTER: bool = true;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<Prev::Output>, E> {
        match self.prev.try_apply(item)? {
            Some(v) => {
                if (self.f)(&v) {
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

impl<Prev, F, I, O, E> FusedTryStage<I> for TryMap<Prev, F>
where
    Prev: FusedTryStage<I, Error = E>,
    F: Fn(Prev::Output) -> Result<O, E>,
{
    type Output = O;
    type Error = E;
    const MAY_FILTER: bool = Prev::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<O>, E> {
        match self.prev.try_apply(item)? {
            Some(v) => {
                let out = (self.f)(v)?;
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }
}

/// Adapter that wraps an infallible [`FusedStage`] chain and exposes it as a
/// [`FusedTryStage`] with an arbitrary error type `E`. Used at the
/// `Pipe` → `TryPipe` transition (`.try_map()`): the upstream chain never
/// produces an `Err`, so the `E` parameter is unconstrained.
///
/// Without this adapter, `try_map` would require `Infallible: Into<E>` for
/// the upstream chain — a bound that has no blanket impl in `std`. The adapter
/// sidesteps it by directly producing `Ok(..)` and never touching `E`.
#[derive(Clone)]
pub struct InfallibleChain<S, E>(pub(crate) S, pub(crate) std::marker::PhantomData<E>);

impl<S, T, E> StageMarker<T> for InfallibleChain<S, E>
where
    S: StageMarker<T>,
{
    type Output = S::Output;
}

impl<S, T, E> FusedTryStage<T> for InfallibleChain<S, E>
where
    S: FusedStage<T>,
{
    type Output = S::Output;
    type Error = E;
    const MAY_FILTER: bool = S::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: T) -> Result<Option<S::Output>, E> {
        // The infallible chain never produces `Err`; `E` is here only to
        // satisfy the trait's associated type.
        Ok(self.0.apply(item))
    }
}

/// Error-conversion stage: wraps a fallible chain and maps `E1` to `E2`. Used
/// when chaining `try_map` calls whose closures return different error types —
/// the upstream error is folded into the downstream type via `Fn(E1) -> E2`.
#[derive(Clone)]
pub struct MapErr<Prev, F> {
    pub(crate) prev: Prev,
    pub(crate) f: F,
}

impl<Prev, F, I> StageMarker<I> for MapErr<Prev, F>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

impl<Prev, F, I, E1, E2> FusedTryStage<I> for MapErr<Prev, F>
where
    Prev: FusedTryStage<I, Error = E1>,
    F: Fn(E1) -> E2,
{
    type Output = Prev::Output;
    type Error = E2;
    const MAY_FILTER: bool = Prev::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<Prev::Output>, E2> {
        match self.prev.try_apply(item) {
            Ok(v) => Ok(v),
            Err(e) => Err((self.f)(e)),
        }
    }
}
