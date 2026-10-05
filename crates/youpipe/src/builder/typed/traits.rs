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
pub trait RangeOp<T>: Sync {
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
    /// fast path can assume every output slot it visits is init, making panic
    /// cleanup trivially sound (no per-slot validity tracking).
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
    type Error = S::Error;
    type Out = S::Output;

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

// ── SinkOp: side-effect terminal (for_each) ──
//
// `for_each` does not produce an output buffer. The leaf consumes each item
// by applying the fused chain and handing the result to the user's `Fn(O)`
// closure. `apply` (Option-wrapping) is used when `MAY_FILTER` so filtered
// items simply skip the closure; the branch on `MAY_FILTER` is a compile-time
// constant, so the pure path stays branch-free for the vectorizer.

/// Sink-only transform: applies the fused chain + user closure for side
/// effects. Drives the `for_each` terminal — no output buffer is allocated,
/// which is the structural advantage over `.map(f).collect::<Vec<()>>()`.
pub(super) trait SinkOp<T>: Sync {
    fn consume(&self, item: T);
}

/// Wrapper combining a fused stage chain `S` with a side-effect closure `F`.
/// The chain produces `O`, which `F` consumes (returning `()`).
pub(super) struct FusedSink<S, F>(pub(super) S, pub(super) F);

impl<S, F, I, O> SinkOp<I> for FusedSink<S, F>
where
    S: FusedStage<I, Output = O> + Sync,
    F: Fn(O) + Sync,
{
    #[inline]
    fn consume(&self, item: I) {
        if S::MAY_FILTER {
            if let Some(o) = self.0.apply(item) {
                (self.1)(o);
            }
        } else {
            // Pure path — never constructs an `Option`, keeping the leaf loop
            // branch-free for chains without `Filter` (same rationale as
            // `RangeOp`/`FusedOp`).
            let o = self.0.apply_pure(item);
            (self.1)(o);
        }
    }
}

// ── Reducer / ReduceOp: accumulator terminals (reduce/fold/sum/…) ──
//
// The reduction terminals are the aggregation counterpart of `for_each`'s
// structural win: `.map(f).sum()` on the collect path must materialize the
// whole `Vec<O>` (n-slot `Slots` allocation + n slot writes + a serial fold
// afterwards), while the reduce core never allocates an output buffer —
// each leaf folds its range into one partial accumulator and the tree
// combines partials bottom-up (see `par_reduce_rec` in fused.rs).
//
// Two layers, mirroring `RangeOp`/`SinkOp`:
//   * [`Reducer`] covers the accumulator side (seed / per-item fold / partial-combine) and is fed
//     **post-stage** outputs;
//   * [`ReduceOp`] / [`TryReduceOp`] wrap a stage chain + a `Reducer` into the item-level op the
//     core's leaves drive.

/// Accumulator-side combinator, fed **post-stage** outputs by the
/// [`ReduceOp`] wrappers.
pub trait Reducer<O>: Sync {
    /// Partial-accumulator type. Must be `Send` — partials cross threads
    /// through `pool::join`.
    type Acc: Send;
    /// Seed for a fresh partial — called once per leaf / driver chunk.
    fn identity(&self) -> Self::Acc;
    /// Fold one output into `acc`. Called once per surviving item (items
    /// dropped by a `Filter` never reach it).
    fn fold(&self, acc: Self::Acc, item: O) -> Self::Acc;
    /// Combine two partials — at tree internal nodes and the driver's final
    /// pass. `fold`/`combine` must form an associative pair for the result
    /// to be independent of the (split-layout-dependent) association.
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc;
}

/// `reduce(op)` reducer: `Acc = Option<O>` — `None` until the first item,
/// so empty input and empty filter survival naturally fold to `None`.
pub(crate) struct OptionReducer<F>(pub(crate) F);

impl<O, F> Reducer<O> for OptionReducer<F>
where
    O: Send,
    F: Fn(O, O) -> O + Sync,
{
    type Acc = Option<O>;

    fn identity(&self) -> Option<O> {
        None
    }

    #[inline]
    fn fold(&self, acc: Option<O>, item: O) -> Option<O> {
        Some(match acc {
            Some(a) => (self.0)(a, item),
            None => item,
        })
    }

    #[inline]
    fn combine(&self, lhs: Option<O>, rhs: Option<O>) -> Option<O> {
        match (lhs, rhs) {
            (Some(a), Some(b)) => Some((self.0)(a, b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        }
    }
}

/// `fold(init, f, combine)` reducer: `Acc = A`, each partial seeded from
/// `init.clone()` (one clone per leaf — cheap for numeric accumulators,
/// the documented cost for heavy ones).
pub(crate) struct FoldReducer<A, F, C> {
    pub(crate) init: A,
    pub(crate) f: F,
    pub(crate) combine: C,
}

impl<O, A, F, C> Reducer<O> for FoldReducer<A, F, C>
where
    A: Clone + Send + Sync,
    F: Fn(A, O) -> A + Sync,
    C: Fn(A, A) -> A + Sync,
{
    type Acc = A;

    fn identity(&self) -> A {
        self.init.clone()
    }

    #[inline]
    fn fold(&self, acc: A, item: O) -> A {
        (self.f)(acc, item)
    }

    #[inline]
    fn combine(&self, lhs: A, rhs: A) -> A {
        (self.combine)(lhs, rhs)
    }
}

/// `sum()` reducer: `Acc = O` via [`iter::Sum`] — the identity is the empty
/// sum and fold/combine are two-element sums (fully inlined to `+` for the
/// numeric types this terminal targets).
pub(crate) struct SumReducer<O>(pub(crate) std::marker::PhantomData<fn() -> O>);

impl<O> Reducer<O> for SumReducer<O>
where
    O: std::iter::Sum + Send,
{
    type Acc = O;

    fn identity(&self) -> O {
        std::iter::empty().sum()
    }

    #[inline]
    fn fold(&self, acc: O, item: O) -> O {
        std::iter::once(acc).chain(std::iter::once(item)).sum()
    }

    #[inline]
    fn combine(&self, lhs: O, rhs: O) -> O {
        std::iter::once(lhs).chain(std::iter::once(rhs)).sum()
    }
}

/// `count()` reducer: `Acc = usize`, one increment per surviving item.
pub(crate) struct CountReducer;

impl<O> Reducer<O> for CountReducer {
    type Acc = usize;

    fn identity(&self) -> usize {
        0
    }

    #[inline]
    fn fold(&self, acc: usize, _item: O) -> usize {
        acc + 1
    }

    #[inline]
    fn combine(&self, lhs: usize, rhs: usize) -> usize {
        lhs + rhs
    }
}

/// Item-level fold op for the reduce core — the `SinkOp` sibling that
/// returns a partial accumulator instead of `()`. Wraps a stage chain and
/// a [`Reducer`]: `fold` applies the (possibly filtering) chain per item and
/// folds the surviving output.
pub(super) trait ReduceOp<T>: Sync {
    type Acc: Send;
    fn identity(&self) -> Self::Acc;
    fn fold(&self, acc: Self::Acc, item: T) -> Self::Acc;
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc;
}

/// Stage-chain + [`Reducer`] wrapper for the infallible reduce terminals.
pub(super) struct FusedReduce<S, R>(pub(super) S, pub(super) R);

impl<S, R, I> ReduceOp<I> for FusedReduce<S, R>
where
    S: FusedStage<I> + Sync,
    S::Output: Send,
    R: Reducer<S::Output>,
{
    type Acc = R::Acc;

    fn identity(&self) -> Self::Acc {
        self.1.identity()
    }

    #[inline]
    fn fold(&self, acc: Self::Acc, item: I) -> Self::Acc {
        if S::MAY_FILTER {
            match self.0.apply(item) {
                Some(o) => self.1.fold(acc, o),
                None => acc,
            }
        } else {
            // Pure path — never constructs an `Option` (same rationale as
            // `RangeOp`/`FusedOp`).
            self.1.fold(acc, self.0.apply_pure(item))
        }
    }

    #[inline]
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc {
        self.1.combine(lhs, rhs)
    }
}

/// Item-level fold op for the fallible reduce core: folds short-circuit on
/// the first `Err`.
pub(super) trait TryReduceOp<T>: Sync {
    type Acc: Send;
    type Error: Send;
    fn identity(&self) -> Self::Acc;
    fn try_fold(&self, acc: Self::Acc, item: T) -> Result<Self::Acc, Self::Error>;
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc;
}

/// Stage-chain + [`Reducer`] wrapper for the fallible reduce terminals
/// (`try_reduce` / `try_fold`).
pub(super) struct FusedTryReduce<S, R>(pub(super) S, pub(super) R);

impl<S, R, I> TryReduceOp<I> for FusedTryReduce<S, R>
where
    S: FusedTryStage<I> + Sync,
    S::Output: Send,
    S::Error: Send,
    R: Reducer<S::Output>,
{
    type Acc = R::Acc;
    type Error = S::Error;

    fn identity(&self) -> Self::Acc {
        self.1.identity()
    }

    #[inline]
    fn try_fold(&self, acc: Self::Acc, item: I) -> Result<Self::Acc, Self::Error> {
        match self.0.try_apply(item)? {
            Some(o) => Ok(self.1.fold(acc, o)),
            None => Ok(acc),
        }
    }

    #[inline]
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc {
        self.1.combine(lhs, rhs)
    }
}

/// [`ReduceOp`] over a bare [`RangeOp`] — the streaming pass-through's
/// entry into the reduce core (`fused_pass_reduce`), where the composed
/// chain is a `RangeOp`, not a `FusedStage`.
pub(super) struct RangeReduce<'a, OP: ?Sized, R: ?Sized>(pub(super) &'a OP, pub(super) &'a R);

impl<T, OP, R> ReduceOp<T> for RangeReduce<'_, OP, R>
where
    OP: ?Sized + RangeOp<T>,
    R: ?Sized + Reducer<OP::Out>,
{
    type Acc = R::Acc;

    fn identity(&self) -> Self::Acc {
        self.1.identity()
    }

    #[inline]
    fn fold(&self, acc: Self::Acc, item: T) -> Self::Acc {
        self.1.fold(acc, self.0.apply(item))
    }

    #[inline]
    fn combine(&self, lhs: Self::Acc, rhs: Self::Acc) -> Self::Acc {
        self.1.combine(lhs, rhs)
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
    type Error = std::convert::Infallible;
    type Output = T;

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
    type Error = E;
    type Output = O;

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
    type Error = E;
    type Output = Prev::Output;

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
            },
            None => Ok(None),
        }
    }
}

impl<Prev, F, I, O, E> FusedTryStage<I> for TryMap<Prev, F>
where
    Prev: FusedTryStage<I, Error = E>,
    F: Fn(Prev::Output) -> Result<O, E>,
{
    type Error = E;
    type Output = O;

    const MAY_FILTER: bool = Prev::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<O>, E> {
        match self.prev.try_apply(item)? {
            Some(v) => {
                let out = (self.f)(v)?;
                Ok(Some(out))
            },
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
    type Error = E;
    type Output = S::Output;

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
    type Error = E2;
    type Output = Prev::Output;

    const MAY_FILTER: bool = Prev::MAY_FILTER;

    #[inline]
    fn try_apply(&self, item: I) -> Result<Option<Prev::Output>, E2> {
        match self.prev.try_apply(item) {
            Ok(v) => Ok(v),
            Err(e) => Err((self.f)(e)),
        }
    }
}
