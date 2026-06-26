use std::hint;

// ── RangeOp: how a leaf transforms an input item ──
//
/// Compile-time-fused transform applied to every item by the range-based core.
///
/// The leaf loop calls `apply` directly (no `Option`/branch) — this is critical
/// for vectorizing the lightweight `x + 1`-style hot loop, where an `Option`
/// discriminant + branch cuts LLVM's auto-vectorizer and costs ~2.5× on the
/// 1 M warm `par_map` path (measured: 710 µs → 290 µs, matching rayon).
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

/// Map closure wrapper (`Fn(T) -> R`). Equivalent to `SyncMap<Identity, F>`,
/// kept as a thin separate type so `par_map` doesn't pay for the `Identity`
/// passthrough call in its monomorphized leaf.
pub(super) struct FnMap<F>(pub(super) F);

impl<T, R, F> RangeOp<T> for FnMap<F>
where
    F: Fn(T) -> R + Sync,
    R: Send,
{
    type Out = R;
    #[inline]
    fn apply(&self, item: T) -> R {
        (self.0)(item)
    }
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

/// Synchronous map stage: `Fn(T) -> O`.
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

/// Barrier / fence stage. Forces a materialization boundary in the streaming
/// pipeline.
#[derive(Clone)]
pub struct Fence<Prev> {
    pub(super) prev: Prev,
    #[allow(dead_code)]
    pub(super) chunk_size: Option<usize>,
}

impl<Prev, I> StageMarker<I> for Fence<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

/// Ordered output stage. Preserves input ordering in the final collection.
#[derive(Clone)]
pub struct Ordered<Prev> {
    pub(super) prev: Prev,
}

impl<Prev, I> StageMarker<I> for Ordered<Prev>
where
    Prev: StageMarker<I>,
{
    type Output = Prev::Output;
}

// ── FusedStage trait ──

/// Compile-time fused stage: applies multiple pipeline stages in a single pass
/// without intermediate allocations.
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
    /// May panic (caught by the leaf's `catch_unwind`).
    #[inline]
    fn apply_pure(&self, item: T) -> Self::Output {
        // SAFETY: contract — only call `apply_pure` when `Self::MAY_FILTER`
        // is false throughout the chain. `Pipeline::collect` enforces this.
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

impl<Prev, I> FusedStage<I> for Fence<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
    #[inline]
    fn apply_pure(&self, item: I) -> Prev::Output {
        self.prev.apply_pure(item)
    }
}

impl<Prev, I> FusedStage<I> for Ordered<Prev>
where
    Prev: FusedStage<I>,
{
    type Output = Prev::Output;
    const MAY_FILTER: bool = Prev::MAY_FILTER;
    fn apply(&self, item: I) -> Option<Prev::Output> {
        self.prev.apply(item)
    }
    #[inline]
    fn apply_pure(&self, item: I) -> Prev::Output {
        self.prev.apply_pure(item)
    }
}

/// `RangeOp` wrapper around a `FusedStage` so the index-based core can drive
/// the compile-time-fused stage chain.
///
/// Only constructable when `S::MAY_FILTER == false` (enforced by
/// `Pipeline::collect`'s dispatch on `S::MAY_FILTER`). The `RangeOp::apply`
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
