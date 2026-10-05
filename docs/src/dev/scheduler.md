# ComputePool — Work-Stealing Scheduler


### Architecture

```
Injector (global queue)
    ↓ steal
Worker₀ ←→ Stealer₀
Worker₁ ←→ Stealer₁
Worker₂ ←→ Stealer₂
Worker₃ ←→ Stealer₃
```

- Built on `st3` (bounded lock-free LIFO deque): each worker has a local LIFO deque (FIFO stealing); other workers steal via `Stealer`
- Global injector is a lock-free `concurrent_queue::ConcurrentQueue` (unbounded) that accepts externally submitted tasks and local-queue overflow
- `EventCount`-style packed atomic counters (`pool/sleep.rs`) wake idle workers
- `SleepMask` (`pool/sleep_mask.rs`): fixed-size inline `[AtomicU64; N]` bitmask tracking which workers are parked, so `wake_any_threads` jumps straight to set bits instead of linearly locking every worker's `is_blocked` mutex. `THREADS_BITS` is sized so the mask fits in one cache line (8 words = 64 B on 64-bit, covering up to 511 workers). The single-`AtomicUsize` predecessor silently aliased bits for `worker_index >= 64` (`1usize << 64` wraps to bit 0 in Rust) and deadlocked under heavy oversubscription; see `sleep_mask.rs` module doc.

#### Vendored scheduler dependencies (`crates/`)

The two lock-free primitives the pool is built on are **vendored forks**, so
their hot paths can be tuned in-tree without waiting on upstream releases:

- `crates/youpipe-st3` (package `youpipe-st3`, lib name `st3`) — fork of
  [asynchronics/st3](https://github.com/asynchronics/st3) v0.4.1. Carries an
  optimization line on top of upstream: bulk segmented steal copies
  (`transfer::transfer_items` splits both ring ranges at the union of their
  wrap points and relocates up to 3 chunks with `copy_nonoverlapping`; ≤8-item
  chunks keep an inline scalar loop — measured −22…−51 % on 16–128-item
  steals), hot-path bounds-check removal, fast empty check before touching
  destination atomics, hoisted tail load out of the pop CAS loop,
  uninitialized buffer allocation, in-place `Arc` construction, and a
  single-item transfer fast path. Provenance + per-commit detail live in the
  vendored `Cargo.toml` header and `st3_PERF_OPTIMIZATION.md`.
- `crates/youpipe-concurrent-queue` (package `youpipe-concurrent-queue`, lib
  name `concurrent_queue`) — fork of
  [smol-rs/concurrent-queue](https://github.com/smol-rs/concurrent-queue)
  v2.5.0 with the adaptive-backoff change (contention paths spin with
  exponential `pause` via `crossbeam_utils::Backoff` instead of bare CAS
  retries / immediate yield; measured 2–5.6× on contended paths) plus the
  remaining crossbeam-queue micro-optimizations from
  `concurrent-queue-PERFORMANCE_REVIEW.md`: bounds-check-free slot addressing
  on the bounded push/pop hot path (`get_unchecked` + `debug_assert`; the
  wrap invariant is not compiler-provable because the mask width
  `(cap+1).next_power_of_two()` can exceed `cap`), zeroed (`alloc_zeroed`)
  SegQueue block allocation (all-zero bytes are a valid block; skips the
  per-slot const-array copy and the memset for fresh pages), and a
  `needs_drop` guard that skips `Bounded`'s per-slot drop sweep for
  destructor-free `T`. Additionally `Unbounded::push_n` (exposed as
  `ConcurrentQueue::push_n`, per-item fallback on the other variants)
  reserves a contiguous run of up to 31 tail slots with **one** CAS and fills
  it with plain stores — segment semantics identical to `push` (WRITE-flag
  `Release` publication, block-boundary install order, closed check before
  each reservation). `Registry::inject_batch` uses it, collapsing the fused
  dispatcher's `num_threads−1 ≤ 31` chunk-job CASes into one per dispatch
  (−0.5…−2.6 % on the cpu_heavy fused path); the streaming `submit_batch`
  benefits identically. The review also records what was tried and rejected —
  e.g. x86 `lock not` SeqCst fences are a
  ~8–10 % pessimization on modern LLVM (it already lowers `fence(SeqCst)` to
  `lock or`).

  Why not crossbeam-queue directly: its `SegQueue::push` is a SeqCst CAS with
  block allocation every `LAP − 1` (31) items, and the pool's stream-dispatch
  path does exactly one high-frequency `inject` per task — the exact regime
  where concurrent-queue's design wins (replacing concurrent-queue with
  crossbeam-queue wholesale regressed the stream dispatch path 4–7 %, per-item
  `inject` punishing SegQueue's SeqCst-CAS push + per-31-items block churn).
  The forks keep concurrent-queue's push/pop shape and backfill the
  waiting-strategy gap instead.

`crossfire` stays a normal crates.io dependency (nothing left to squeeze).
The rename (`youpipe-*`) exists purely to prevent version confusion with the
crates.io releases; both crates keep their upstream lib names, so call sites
don't change. The vendored crates are excluded from youpipe's own lint/
profile config except where noted in their `Cargo.toml` headers.

### Task Submission Flow

1. `pool.submit(job)` boxes the closure in a `HeapJob`, type-erases it to a `JobRef`, and calls `inject_or_push` — external callers go to the global injector, an on-pool caller pushes its own local deque
2. `Sleep::new_injected_jobs` bumps the packed atomic counters and wakes parked workers via `wake_any_threads`
3. Worker wakes → `find_work()` searches by priority

Local-deque invariant: unlike rayon (whose `spawn` always injects, so its
local deques hold only self-capturing `StackJob`s), our on-pool fast path
parks uncaptured `HeapJob`s in the caller's local deque, and join's wait
loop executes popped jobs bare. Every job type admitted to a local deque
must therefore uphold `Job::execute`'s no-unwind contract (`StackJob`
captures into its result slot, hybrid `ChunkJob` into its fail slot,
`HeapJob` aborts via `AbortIfPanic`) — see `WorkerThread::push`.

### On-pool callers of the fused terminals (`Stealing` latch)

A fused terminal (`.collect()` / `.for_each()` / `.try_collect()`) reached
from a worker of the driving pool — a `pool.submit` task, a stream stage
closure, a nested `scope`/`.run()` — dispatches through the same hybrid
dispatcher as an off-pool caller. The only difference is how the driver
waits: `ComputePool::on_this_pool_owner()` yields the current worker's
`(registry, index)` to `CountLatch::with_count`, selecting the `Stealing`
variant, whose wait is `WorkerThread::wait_until` — the same work-stealing
loop (and sleep-module latch park, woken by `CoreLatch::set` →
`notify_worker_latch_is_set`) that `join`'s `SpinLatch` uses. An off-pool
caller instead gets the `Blocking` variant (spin-then-condvar).

Historically on-pool callers fell back to the single `par_index_rec` tree
because the `LockLatch` condvar would deadlock a same-pool worker; the
cost was a log2(num_threads) fork/join ramp-up per nested terminal. The
`Stealing` routing removes that for large batches (`chunk_splits > 0`).
Small batches keep the tree — see the regime comment in `hybrid_dispatch`
for the measured split (P concurrent nested small batches under hybrid
collapse the single injector, +430 %). One behavioural note: the
off-pool driver's assist reserve is 0 on-pool (`wait_spin_assist`'s
`Stealing` arm cannot run the reserve-chunk hook, so a withheld chunk
would have no executor).

`YOUPIPE_ONPOOL_HYBRID` is a three-level same-binary A/B knob (0 = tree,
default; 1 = injector hybrid; 2 = local-deque hybrid, 2026-09-25). Level 2
pushes the driver's whole chunk batch onto its OWN local LIFO deque
(`WorkerThread::push_batch`: one JEC increment + one wake cascade for the
batch, overflow spills to the injector — measured 0 spills, 32 chunks vs
a 256-slot deque); peers steal from the FIFO end, so P concurrent nested
batches distribute across P deques instead of converging on the one
global MPMC, and the driver's own `Stealing` wait consumes the LIFO end
exactly like `join`'s B branch (`counter == 0` ⇒ every JobRef consumed
holds on every consumption surface). Measured (5 interleaved rounds,
taskset 1–31, 32 cores):

* level 2 vs level 1 (the mechanism check): `nested_single/100K` −2.9 %
  (24/25 dominant), `nested_saturated/100K` −1.7 % (25/25) — the
  +2 %-lean injector convergence penalty under saturation is gone; level 1
  is dominated in every measured row.
* level 2 vs tree: the margin is small and session-unstable (one session
  +3.3 % for the tree on `nested_single/100K`, another −6.4 % for level 2;
  `nested_saturated/100K` lean-better for level 2 in both, −0.4 %/−1.0 %).
  No stable cross-session win either way ⇒ the default stays 0; level 2 is
  the opt-in for saturation-heavy shapes and the A/B instrument.
* fused 1M family (`sync_lightweight*`): ±1.5 % noise on all sides — the
  off-pool dispatch path is shared and untouched.
* ungating the small-batch `chunk_splits > 0` gate under level 2
  (`YOUPIPE_ONPOOL_HYBRID_SMALL`, experiment only, reverted):
  `nested_single/1K` +80 % (0/25 — the parked-peers single-driver regime
  still collapses; the tree's incremental one-push-per-join ramp stays
  the right shape), `nested_saturated/1K` −8 % (25/25). Per-regime mixed
  verdict ⇒ gate stays; an adaptive gate is the cost-EMA class already
  twice falsified above.
### Generic chunk tree `par_tree_rec` and the reduce core (2026-10)

The eight per-terminal tree recursions (`par_index_rec[(_by_ref)]`,
`par_index_try_rec[(_by_ref)]`, `par_for_each_rec[(_by_ref)]`,
`par_range_gen_rec`, `par_range_gen_sink_rec` — each a verbatim copy of the
`(Ok,Ok)/(Err,Ok)/(Ok,Err)/(Err,Err)` sibling-drop match) converged into one
generic `par_tree_rec` (crates/youpipe/src/builder/typed/fused.rs): a `leaf`
closure plus a `drop_success_range` hook — the internal-node granularity of
`HybridStrategy::cleanup_success_chunk`. The hooks are closure references
monomorphized per strategy, so every leaf loop stays independently inlined;
the auto-vectorization argument on `par_index_leaf` only holds for concrete,
fully inlined leaves, which is why the leaves themselves were never unified.
The eight per-leaf RAII guards (`LeafGuard` / `TryLeafGuard` / `RefLeafGuard`
/ `TryRefLeafGuard` / `ForEachGuard` / `FilterGuard` / `PlaceLeafGuard` /
`GenLeafGuard`) collapsed the same way into one `LeafCleanup<T, R, OUT, IN>`
whose const halves select output-prefix / input-tail drops (dead halves
compile away; the dead-half pointers are never dereferenced). Pure refactor,
net −361 lines: full test suite + miri (tree-borrows, drop-accounting)
green; interleaved A/B over the `sync_vs_rayon` families (per-id isolated,
3 rounds) — every id graded noise — worst youpipe delta +0.8 % (the one leaning id, sync_lightweight 10 K borrowed, confirmed noise by 2 extra rounds: dom 13/25, spreads 11–14 %), tightest family (sync_lightweight 1 M borrowed) −0.1 % at ±0.4 % spread, rayon anchors within ±1 %.

The reduction terminals (todo perf #3) landed on a sibling core, *not*
`par_tree_rec`: `par_reduce_rec` is a value-carrying recursion (`join`
returns both child partials, the node combines them), so there is no
shared-buffer sibling-drop path at all — panic safety is structural (every
partial is a local dropped by unwind / `join`; the leaf guard owns only the
input tail). `ReduceStrategy` plugs into `hybrid_dispatch` unchanged: each
chunk's tree publishes its partial into a one-shot `ChunkSlots` cell — a
plain store to the chunk's own cell, sequenced before the latch `set` —
and the driver folds the cells in chunk ordinal order (the boundary
formula's inverse; no sort, no lock). The first design published into a
`Mutex<Vec<(start, Acc)>>` instead: correct, but the ~num_threads pushes
pile onto one lock at the batch tail, and at small n the per-chunk work is
too short to hide it — measured +80…+112 % vs collect-then-sum at 1K and
+40…+73 % at 10K (the 100K/1M wins of −40 %/−80 % stayed, publication
overlapping real compute there). Per-chunk cells remove the shared
writable line entirely; unpublished (`Empty`) cells are skipped, which also
covers the on-pool single-tree shortcut (one chunk). On failure paths
`cleanup_success_chunk` (`ChunkSlots::drop_published`) eagerly drops each
successful chunk's published partial — the slot box's own drop is the
backstop — a failed batch having no user-visible accumulator. After the
slot fix the reduce core sits within noise of collect-then-sum at 1K
(+9.5 % median-of-5 with 18 % bimodal round spread) and wins from 10K up
(−16 % @ 10K, −74 % @ 100K, −87 % @ 1M; see dev/benchmarks.md).

API note: the streaming reduce pass-through threads the reducer through
`StageSpawn::fuse_exec_reduce` with the composed chain's output type as an
explicit parameter bound by equality (`OP: RangeOp<Self::Out, Out = B>`,
`R: Reducer<B>`) — a projection of one method-generic inside another
method-generic's bound (`R: Reducer<OP::Out>`) defeats rustc's
implied-bounds elaboration and the impls fail with a bogus
"`OP: RangeOp<..>` is not satisfied" (minimal repro verified; nightly
1.100).

### Work Search Strategy

`find_work()` tries sources in priority order:

1. `local.pop()` — own LIFO deque
2. `injector.steal()` — global queue (cheap CAS-free dequeue, checked before peers since external submits arrive here)
3. peer stealers — randomized full scan with `steal_and_pop`

The yield/spin/sleep backoff is **not** in `find_work()`; it lives in the idle
loop of `wait_until_cold`; each round that finds no work calls
`Sleep::no_work_found`, which ramps from `spin_loop` → `thread::yield_now` →
parking on the `EventCount`-style counters.

#### Dispatch granularity — cost-adaptive chunk counts (tried, rejected)

Hypothesis (2026-09): at small batches (1 K `cpu_heavy`), the hybrid
dispatcher's fixed cost is dominated by the inject + wake cascade, so
shrinking the top-level chunk count from `num_threads` to
`ceil(estimated_total_work / 8 µs)` — estimated via a per-pool chunk-cost EMA
sampled by `ChunkJob::execute` — should cut overhead. **Rejected by
measurement**: p50 wall at 1 K went 32.1 → 37.7 µs (+17 %), consistently
across interleaved runs. The causal read: hotpath showed ~13 workers per
batch reach the condvar park *between* back-to-back batches, and each park's
wake latency is skewed; `num_threads` fine-grained chunks are precisely the
load-balancing unit that hides that skew (late-waking workers pick up
remaining chunks instead of gating the batch with their wake latency).
Fewer, meatier chunks hand the batch's tail to whichever workers wake last.

(2026-09-05 postscript: the hypothesis's premise was later disproved — the
1 K fixed cost was dominated by two uncached, cgroup-reading
`available_parallelism()` calls per terminal (~50 µs), not the wake
cascade. The experiment's verdict stands on its own interleaved A/B, but
"wake cascade dominates small-batch fixed cost" is no longer the right
mental model; see dev/benchmarks.md "CPU-Heavy `pipe_ref()` vs rayon".)
The remaining wall-time floor at tiny batches is worker wake latency, whose
backoff constants are already A/B-tuned (see `sleep.rs` — widening the spin
or yield windows was measured as a global regression in 2026-06).

#### Flat top-level dispatch (tried, rejected)

Hypothesis (2026-09): on expensive-item unbalanced batches (zstd-style
compression of many differently-sized files, µs–ms per item), youpipe
trailed rayon by 8–17 % because the hybrid dispatcher injects a fixed set
of top-level chunks whose `oversplit` leaves hide inside each owner's
fork/join tree, consumed LIFO — once a worker parks between back-to-back
batches it wakes to a drained injector and self-consumed trees,
contributing zero items for that batch. Instrumentation (per-thread
first/last-item timestamps + engine-side ring-buffer trace) confirmed 1–6
such workers per iteration on a 31-thread pool; a never-park control
(`ROUNDS_UNTIL_SLEEPY += 100_000`) flipped every scenario to a win,
proving the gap is park/wake handoff, not stealing throughput.

Two changes were built on that diagnosis:

- `TopChunking::FlatLeaves`: for `Unbalanced`/`Custom` batches with
  `n ≥ num_threads × oversplit`, spend the whole split budget at the top
  level — every leaf becomes a single-leaf chunk injected flat into the
  FIFO injector, so late arrivals always find work and heavy items surface
  in FIFO order instead of hiding behind an owner's sequential prefix.
- Activity-gated park delay (`Sleep::note_flat_batch`): within 2 ms of a
  flat-batch completion, idle workers restart their backoff ramp instead
  of parking — a bounded never-park.

**Rejected by measurement**, on both ends:

- The criterion `unbalanced` family (n=5000, ns-scale items) regressed
  **+164…+194 %** (3 interleaved A/B rounds, stable) — attributed almost
  entirely to the flat layout itself (disabling the park delay moved it
  <2 %): ~256 single-leaf chunks means one contended injector pop per
  leaf, which exceeds the whole batch's runtime when items are cheap.
  The dispatch site cannot see per-item cost (`Workload::Unbalanced`
  carries no such information), and `n` cannot gate the two regimes apart
  — the zstd repro (n=2000/8000, µs-scale items) and criterion (n=5000,
  ns-scale) overlap in batch size.
- On the target regime itself (independent 5-round interleaved repro,
  lognormal sizes × ~3 ns/byte vs same-round rayon) the flat layout's net
  effect was ~0.5 % with half the scenarios regressing (capped shapes:
  +7.2 → +9.9 %); the park delay recovered ~10 % on uniform-cost batches
  but cost 5–6 % on every straggler shape (idle workers restart
  spin/steal scans while the tail runs — the 2026-06 "wider spin window"
  lesson in miniature), plus unbounded idle burn while batches repeat.
- Variants also rejected without reaching A/B: flat-2 (62 chunks × 2-level
  trees) hid monster items back inside sequential prefixes — the original
  problem; a passive driver was ±1 %.

What survives from that round: the `zstd_shape` criterion group
(`benches/unbalanced.rs`), which reproduces the expensive-item regime with
runtime iteration counts (LLVM cannot fold them — see dev/benchmarks.md
"runtime iteration counts") and matches the standalone repro's baseline
deltas exactly (heavy-tail +13 %, capped +7 %, uniform +14 % vs rayon).

#### Execute-time split-back (tried, rejected) and the layout-noise lesson

The follow-up hypothesis (2026-10): rayon's root job splits *on demand*
(a stolen-from job re-splits, so the work supply never exhausts and no
park/wake handoff gates the batch), whereas youpipe decides the entire tree
at inject time. An execute-time split-back was built: when a worker pops a
top-level chunk, the injector is empty (the signal that a late worker
would find nothing), and recent chunks were expensive (a per-pool chunk-cost
EMA, 200 µs threshold separating the two regimes with ~7×/8× margin), it
re-injects the chunk's back half as a fresh job (one latch increment, one
heap box on an intrusive chain) and keeps the front. Failure cleanup walks
the half-chain with `[start, kept_end)` scoping; panic and `try_collect`
drop-accounting tests plus miri (tree-borrows) all passed.

**Rejected by same-binary A/B**: with the mechanism gated at *runtime* in
the experimental build (identical binary), zstd_shape deltas were
identical (±0.3 %, interleaved rounds), and a debug counter showed exactly
**one split per process** — the cold-start EMA window. In steady state the
two gates almost never hold simultaneously: by the time the injector is
empty (last chunk popped), the EMA of cheap-regime batches already sits
under the threshold, and expensive-regime chunks that split cannot re-arm
the condition for their halves. A mechanism that measures zero must go,
whatever its elegance — the whole apparatus (EMA field, per-chunk `Instant`
timing, half-chains, kept-end cleanup) was removed.

The same investigation exposed a **methodology trap** worth more than the
mechanism itself: compile-time A/B (flipping a `const ENABLED: bool` and
recompiling) swings `sync_cpu_heavy` n=100k by a "+32 % stable regression"
across interleaved recompile rounds — pure code-layout sensitivity
(identical logic, different binary), first suspected as a real regression
and chased through CAS contention, vDSO cost and EMA-stuck-at-zero
theories before a runtime-switch experiment (env knob in the
since-removed code) collapsed it to ±1 %. Rule: **A/B performance claims
on tight benchmarks require the same binary with a runtime knob** (see
`YOUPIPE_OVERSPLIT` below); recompile-pair results are only directional.

#### `UNBALANCED_OVERSPLIT` 8 → 32 (accepted trade-off)

Same-binary A/B (`YOUPIPE_OVERSPLIT`, one binary, interleaved rounds,
31-thread pool) — the factor is monotone in both regimes and cannot
satisfy both, so the default picks the expensive-item side:

| scenario | ov=8 | ov=16 | ov=32 |
|---|---|---|---|
| zstd heavy-tail (vs rayon) | +5.2 % | +4.4 % | **+2.5 %** |
| zstd capped (vs rayon) | +9.2 % | +7.1 % | **+2.8 %** |
| zstd uniform (vs rayon) | +12.5 % | +6.4 % | **+3.0 %** |
| cpu_unbalanced skewed n=5000 | **22.2 µs** | 25.0 µs | 29.9 µs |
| cpu_unbalanced log_uniform n=5000 | 29.7 µs | **29.1 µs** | 32.1 µs |

n=200 cheap batches are flat across factors (splits bottom out at the
same leaves). The cheap n=5000 cost is real (−28 % vs ov=8) but stays far
ahead of rayon (29.9 µs vs ~70 µs); `Workload::Custom(n)` pins any factor
for workloads that want the cheap side. Balanced, stream, IO and
horizontal suites re-measured clean (±1 %) — the factor only feeds the
`Unbalanced`/`Custom` dispatch path.

#### Latecomer slack: extra top-level chunks (accepted)

The follow-up round (2026-09) instrumented the scheduler itself (temporary
probe arrays: per-worker park/wake/inject timestamps, `find_work` scan log,
pop/steal outcome counters; repro lives in `examples/zstd_shape.rs` modes
`instr`/`instr2` of that era) and found the real reason the residual zstd
gap did not respond to any tree-shape change. On the 16C/32T SMT bench
machine the batch actually runs on ~30 of 32 workers; the missing 1–2 are
**latecomers** that lose the first scheduling round after `inject_batch`
and never rejoin:

- A parked worker's futex wake can be **delivered 100 µs–1.7 ms late**
  (measured: notifier side `WOKE=+13 µs`, sleeper side `RETURNED=+1723 µs`)
  — CFS wakeup preemption under 2× SMT oversubscription.
- A worker in the `sched_yield` backoff phase degrades to **one
  `find_work` scan per ~50–300 µs** (starved by 30 CPU-bound siblings);
  spin-phase workers scan at ~µs cadence.
- All `num_threads` chunks are claimed within ~20 µs of inject, and the
  stealable tree subtrees live only **µs-scale windows** — the spin-phase
  majority drains every victim deque at µs cadence, so a slow-cadence
  arrival never catches one (measured: idle worker scans 19× mid-batch,
  0 successful steals, `Busy` never seen).
- Rayon does not have this hole because its steal unit is a coarse leaf
  (~`total/(2·nthreads)` items ≈ hundreds of µs of work): a queued half
  stays stealable for ~leaf-time, so a 300 µs-cadence arrival still finds
  one.

The injector is the one work source with no race: a leftover chunk sits
until popped. `UNBALANCED_CHUNK_SLACK = 8` (Unbalanced only, runtime knob
`YOUPIPE_CHUNK_SLACK`) injects `num_threads + 8` top-level chunks so
latecomers at any cadence find one. The per-chunk tree depth uses
`floor(log2(num_chunks))` when slack is on — rounding up (the historical
power-of-two formula) would coarsen every tree one level and give back
~3 pt on uniform (the fine-leaf budget is what ov=32 bought).

Same-binary A/B (`YOUPIPE_CHUNK_SLACK`, criterion `zstd_shape` +
interleaved repro; within-session ratios, lower better):

| scenario (vs same-run rayon) | slack=0 | slack=8 |
|---|---|---|
| zstd heavy-tail n=2000 | +4…+5 % | **−6.5…−10 %** (ahead of rayon) |
| zstd capped n=2000 | +2…+6 % | +1…+4 % |
| zstd uniform n=2000 | +5…+11 % | +1…+8 % (−3 pt, machine-state dependent) |
| heavy-tail n=8000, 3 seeds | +5.1 % avg | **−0.9 % avg** |
| cpu_unbalanced skewed/log_uniform n=200/5000 | — | +5…+7 % (still >2× ahead of rayon) |

Two measurement lessons from this round:

- **Heavy-tail results are boundary-lucky**: chunk boundaries shift with
  the chunk count, which reshuffles which chunk a monster item lands in.
  A single-seed n=8000 A/B showed slack=8 "regressing" +8 %; across three
  seeds (`ZSTD_SEED` on the example) slack=8 won every seed pair. Never
  accept a heavy-tail verdict from one seed.
- Absolute vs-rayon ratios drift several points between sessions
  (thermal/frequency state); only within-session interleaved A/B deltas
  are decision-grade — the table above reports deltas, not absolutes.

`Workload::Custom` gets no slack (it pins the oversplit factor only), so
`Custom(n)` remains a clean manual control. Balanced/stream/IO/horizontal
families re-measured clean with slack=8 (±2 %, probes removed).

#### Adaptive slack tiers (accepted)

The next round chased the residual uniform gap and instead found a second,
n-independent effect: **boundary luck is a function of items per chunk**.
With 200-item chunks (heavy-tail n=8000), which fixed chunk the rare 2 MB
items land in decides the straggler — the per-seed vs-rayon spread was
14.6 pt (worst seed +6.9 %, best −7.7 %) while rayon's own adaptive
splitting self-repairs the imbalance. More, smaller chunks scatter that
luck: slack 8→16 (40→48 chunks) took the worst seed to +1.3 %, the mean
to −1.1 %, and the spread to 4.2 pt (6 seeds, interleaved). n=4000
(83 items/chunk) improves on all three shapes; n=2000 (≈42 items/chunk)
regresses +2…+4 pt — per-chunk dispatch overhead dominates once chunks
get small.

Hence two tiers (`unbalanced_chunk_slack`): slack 8, upgraded to 16 when
`n / (num_threads + 16) ≥ UNBALANCED_SLACK_WIDE_MIN_PER_CHUNK`
(`ZSTD_SHAPE_N` on the example sweeps the boundary). Cheap skewed/log-uniform n=5000 lands in the wide tier and
pays +1.5…+5 % (µs-scale absolute, still >2× ahead of rayon) — the same
trade-off face `UNBALANCED_OVERSPLIT` 8→32 already accepted; n=200
stays narrow and clean. Guards: narrow tier is bit-identical to the old
slack=8 path (criterion zstd_shape n=2000 unchanged), horizontal
`cpu_unbalanced` n=10k flat / n=100k +2 % (−30 % ahead of rayon).

Rejected in the same round — **dropping the default pool to physical
cores** (the "SMT excludes ~2 workers" hypothesis suggested it): zstd
gets ~1.9× from SMT on this machine, so 16 threads lose +78…+89 % wall
time on every shape (`zstd_shape` mode `threads`, youpipe and rayon
alike, 6-seed). Straggler cost and throughput cost are not in the same
league; the latecomer-slack approach above attacks the straggler side
without giving up SMT throughput. Also rejected: flat slack 32/64
(uniform n=2000 +3 % — per-chunk overhead returns at small chunks).

#### Wide-tier boundary scan (accepted: 64 → 48)

The 64 boundary was interpolated between two measured points, so a
dedicated scan pinned it down (2026-09, criterion `zstd_shape` extended
with `ZSTD_SHAPE_NS`/`ZSTD_SEEDS` id-grid overrides): n ∈ {2000, 3000,
4000} = 42/63/85 items per wide chunk (31-thread bench taskset) × 3
shapes × 6 seeds × 3 interleaved same-binary rounds of flat
`YOUPIPE_CHUNK_SLACK` 8 vs 16.

Measurement lesson: round 1's slack-16 pass ran ~25 % slower than its
slack-8 neighbour — a co-tenant build; the env knob cannot affect
rayon, yet that pass's rayon ids drifted the same way. Pass-level
machine state biases naive side-vs-side deltas, so the decision signal
pairs each youpipe id with the same pass's rayon id and compares the
two ratios (`(yp16/ry16)/(yp8/ry8)`); the rayon cross-side ratio
doubles as a live drift detector.

| items/chunk | heavy-tail | capped | uniform | verdict |
|---|---|---|---|---|
| 42 (n=2000) | −1.2 % (4/6) | +0.5 % (3/6) | −0.4 % (5/6) | neutral — stays narrow |
| 63 (n=3000) | −3.0 % (4/6) | −1.4 % (5/6) | −0.3 % (3/6) | wide favored |
| 85 (n=4000) | −2.2 % (3/6) | −0.2 % (3/6) | −1.0 % (6/6) | wide (already default) |

(seed medians of the drift-cancelled tier delta; (k/6) = seeds where
the wide tier's round-median is faster.) The earlier "+2…+4 pt"
n=2000 wide regression did not reproduce — per-seed tier deltas swing
±26 pt there, pure chunk-boundary luck. n=3000 flipping to wide closes
the heavy-tail vs-rayon gap from +8.6 % to +2.5 % (seed medians).
Hence the boundary 64 → 48: the [48, 64) items/chunk band goes wide,
42-chunk batches stay narrow. Guards: every cheap-side family keeps its
tier bit-for-bit (cpu_unbalanced n=200 → 4/chunk, n=5000 → 106/chunk;
fused 200/1000 → 4/21 — none crosses 48), and on the reference
31/32-thread machine the default `zstd_shape` n=2000 ids stay narrow
(42 items/chunk), so no guard bench changes behavior. The per-chunk
boundary is machine-independent by design — a smaller pool holds
proportionally more items per chunk and may cross into the wide tier.

#### Worker affinity: opt-in 1:1 core pinning (accepted, opt-in)

The fused 2M/4M occupancy attribution (benchmarks.md) showed the residual
per-batch cost is parked workers' futex wake + cold-core placement
(7.6 migrations/iter vs rayon's 0.5, 99 vs 13–19 ctx switches). The
sanctioned structural lever from that round — keep workers hot across
back-to-back batches — is not reachable by spin-window policy: a causal
probe (`YOUPIPE_SPIN_ROUNDS=YOUPIPE_YIELD_ROUNDS=2048`, same-binary,
2026-09) recovers capped −3.8…−5.3 % / uniform −1.5…−3.9 % but regresses
heavy-tail +1.1…+4.1 % (spinning during the ms-scale straggler burns its
SMT sibling), and a quiescence-gated extension (extend the spin phase only
while `inactive == num_threads`) measured no win — in these shapes the
early parkers park *before* quiescence (the straggler is still running),
so the gate always fires too late (reverted; see the falsified list in
[dead-ends.md](dead-ends.md)).

`ComputePool::new_pinned(n)` attacks the same residual without burning
anything: worker `i` is pinned to the i-th CPU of the process's allowed
set, so a worker that parks between batches always wakes on its own —
idle, cache-warm — core. Same-binary A/B (`YOUPIPE_PIN_WORKERS=1`,
16C/32T, taskset 1-31, 5 interleaved rounds, rayon columns all noise):

| family (youpipe ids) | off → pinned |
| --- | --- |
| zstd capped n=2000 (2 seeds) | **−6.9 / −6.1 %** (25/25) |
| zstd uniform n=2000 (2 seeds) | **−4.0 / −4.6 %** (25/25) |
| zstd heavy-tail n=2000 (2 seeds) | −1.6 / **−3.8 %** (21-25/25) |
| horizontal cpu_balanced 1M/2M/4M | −7.2 / −5.1 / −6.5 % (5/5 rounds each) |

Pinning is **not** a default because it trades away CFS's wake steering,
which other regimes need (a woken pinned thread cannot move to an idle
CPU if its own is busy). Same-session regression sweep (3 rounds, ids
where the verdict was stable):

| family (youpipe ids) | off → pinned |
| --- | --- |
| cpu_unbalanced_stream unordered/ordered 5000 | **+47.7 / +42.8 %** |
| sync_nested_on_pool nested_saturated/1K | **+37.1 %** |
| sync_for_each cpu_heavy/1K | +22.1 % |
| cpu_unbalanced_stream 200 | +17.2…+18.2 % |
| sync_lightweight par_map_borrowed/10K | +17.1 % |
| sync_filter filter_map(_owned) 1K/10K | +5.2…+12.9 % |
| mixed_load stream_cpu/100K | +6.1 % |
| (improvements) nested_saturated/100K, io_unbalanced unordered/1K | −2.2 / −2.4 % |

The dividing line is batch saturation: when a batch occupies (nearly)
all workers for its whole duration, every wake lands on an idle core and
pinning is pure win; when few workers are active (small/medium batches
with a participating driver) or work is item-granular and wake-heavy
(streaming stage workers parked on inter-stage channels), forcing
placement loses to the scheduler. Hence: opt-in constructor, single
≤-CPU-sized pool serving fused batch terminals only, never shared with
`stream` pipelines; `YOUPIPE_PIN_WORKERS=1` remains as the same-binary
A/B knob (it pins *every* pool in the process — that is exactly how the
regression column above was measured).

#### Wake-path layout hygiene: cold attributes and counter padding (2026-09)

Two layout-level cleanups (todo micro-optimization round), verified as
no-regression rather than win-seeking. Both change struct/code layout by
necessity, so recompile-pair A/B on the tight families is layout-noise
bound (the compile-time trap in dev/benchmarks.md; +32 % pure layout
swings on `sync_cpu_heavy` n=100k are on record) and only directional
evidence is claimable:

- `wake_any_threads` lost its `#[cold]`: it is the entry of every
  dispatch's wake cascade — the path whose p99 tails (100–270 µs)
  motivated the `SleepMask` scan and the lock-drop-before-notify — so
  evicting it from the main code layout was backwards. The policy, now
  documented at both functions: the wake side
  (`wake_any_threads`/`wake_specific_thread`) stays hot, the park side
  (`sleep`/`announce_sleepy`) stays cold (each runs once per idle
  episode).
- `AtomicCounters`' packed word is now `CachePadded`, mirroring
  `sleeping_mask`: the counters line is the pool's most contended (every
  idle round loads it, every dispatch CASes the JEC) and previously
  dodged false sharing only by field-layout luck inside `Sleep`. Loom
  drops the padding (`youpipe_sys::CachePadded`); miri semantics are
  unchanged.

Criterion A/B (`bench_ab.sh`, per-id interleaved, 3 rounds × 2 sessions,
`cc0dfbc` → this round): `sync_lightweight` 10K/1M ±1.7 % (noise);
`stream_pipeline/single_stage_ordered` 1K **−10.6 %** (9/9 dominant),
100K −6.1 %. The one tight family that moved — `sync_cpu_heavy` 100K
youpipe +29/+42 % — moved its *untouched* rayon and sequential anchors
the same way in the same passes (+13.6 % / +6.3 %, spread ≤1.8 %), the
documented code-layout signature; drift-cancelled youpipe-vs-rayon
ratios still shifted +13/+25 %, inside the recorded layout swing band
but not provably clean. Layout cannot be knob-gated for a struct-layout
change, so the cpu_heavy verdict stays "unattributable under recompile
noise" — exactly the trap that motivates the runtime-knob rule — and
the directional read (stream faster, lightweight flat) stands as the
no-regression evidence.

### Graceful Shutdown

`ComputePool::Drop` calls `Registry::terminate()`, which decrements a ref-count
(`terminate_count`); when the last clone drops (count 1→0) it sets each worker's
`terminate` OnceLatch and tickles it awake. Each worker's `wait_until_out_of_work`
then drains its remaining local-deque work **and the injector** (a `JobRef` owns
its heap box — an unexecuted job is a leak, and a revived pool would run it at a
random later point), sets its `stopped` latch, and exits; `Registry::Drop` blocks
on every spawned worker's `stopped` before returning. Dropping the last handle
therefore waits for every submitted job, not just the running ones. Note a
submit-job closure that panics aborts the process wherever it executes
(`HeapJob::execute`'s `AbortIfPanic` — rayon `spawn` semantics).

### Transient pool recycling (accepted, 2026-09)

`ComputePool::new` parks dropped pools in a process-wide LRU keyed by the
clamped worker count (`executor/compute/pool_cache.rs`, capacity 4) instead
of joining them; the next `new` of the same size reuses the parked pool for
an `Arc` clone + `terminate_count` increment.

Motivation (`pool_reuse` bench, 32C/32T, criterion at 3089274): per-terminal
pool build+join costs ~15 µs per worker — 8 workers ≈ 125 µs,
`with_oversubscribe(2)`'s 64 workers ≈ 1.2 ms — against 2.7 / 7.8 / 44 µs
for the actual fused run on 1K / 10K / 100K cheap-map items: the pool
lifecycle was 98 % / 94 % / 80 % of the terminal call. With recycling the
`transient_workers8` variants collapse onto `prebuilt_pool8` (2–4 µs
overhead per run).

Design decisions:

- The cache holds its own handle per slot; handing a pool out clones it, so
  external drops park the pool instead of joining — a deliberate,
  documented semantic change (threads stay alive, idle).
- Idle-timeout reaping rejected: a timer per pool makes thread counts
  nondeterministic and the "the pool is gone" belief *less* predictable.
  Instead: bounded capacity + `ComputePool::clear_cached_pools()` explicit
  join entry. Auditable invariant: at most 4 parked pools exist; everything
  evicted, cleared, or built through `new_pinned` joins for real.
- `new_pinned` bypasses the cache (pinned workers hold scarce CPU placement);
  so does the global pool (process-lifetime already).
- Locking: the hit path is a mutex + linear scan of ≤4 entries; pool
  *construction* happens outside the lock; and every pool drop (eviction,
  lost build race, clear) happens outside the lock too — dropping the last
  handle joins worker threads, and a worker can itself be blocked on the
  cache mutex (a detached job `pool.submit(|| ..nested terminal..)` followed
  by `drop(pool)`), which would deadlock joiner↔job under the lock.
- Teardown stays asynchronous (see Graceful Shutdown: each worker holds its
  own registry `Arc`, the last one to exit runs `Registry::Drop`), so
  `clear_cached_pools()` may return just before the OS threads are gone —
  tests poll briefly instead of asserting an exact instant.
- A recycled pool keeps the name/affinity it was created with
  (`yp-pool-<i>`, unpinned for `new`); under the `YOUPIPE_PIN_WORKERS=1`
  benchmark knob recycling therefore preserves affinity — benign, the knob
  pins every pool in the process anyway.
