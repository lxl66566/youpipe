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
`n / (num_threads + 16) ≥ 64` (`ZSTD_SHAPE_N` on the example sweeps the
boundary). Cheap skewed/log-uniform n=5000 lands in the wide tier and
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

### Graceful Shutdown

`ComputePool::Drop` calls `Registry::terminate()`, which decrements a ref-count
(`terminate_count`); when the last clone drops (count 1→0) it sets each worker's
`terminate` OnceLatch and tickles it awake. Each worker's `wait_until_out_of_work`
then drains its remaining local-deque work, sets its `stopped` latch, and exits;
`Registry::Drop` blocks on every spawned worker's `stopped` before returning.
