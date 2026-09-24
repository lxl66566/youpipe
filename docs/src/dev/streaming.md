# Data Transfer, Ordering & Fences

## Fused pass-through (pure sync chains)

`stream(v).stage(f1)….stage(fk).run()` is semantically identical to
`pipe(v).map(f1)….map(fk).collect()` when every stage is a plain `SyncStage`.
`try_run` detects this shape and skips the whole streaming topology (feeder
job, k channels, stage workers, seq tagging, collector drain), executing one
composed `g ∘ f` pass on the fused index core (`fused_pass_collect` → hybrid
dispatch + zero-copy slots). Measured on `mixed_load/youpipe_stream_cpu`
(3-round isolated interleaved A/B, 32 cores): 1 K **310.5 µs → 9.98 µs**,
100 K **30.45 ms → 139.3 µs** — the streaming infrastructure is gone;
what remains is the fused core's own level (vs rayon 39.9/92.3 µs at the
same shapes).

Mechanism — no specialization needed:

- `StageSpawn::fuse_exec(&self, downstream: OP, items, workload, pool)` is a
  trait method whose `OP` generic carries the composed op type through the
  recursion: each `SyncStage` wraps `downstream` with its own closure
  (`FuseCompose` = `next ∘ f`, closure held by reference), and `StreamStart`
  executes `fused_pass_collect` on the fully composed op.
- Eligibility is the impl set: only `StreamStart`/`SyncStage` override
  `fuse_exec`; `ExpandStage`/`FenceLink`/`AsyncStage` keep the default
  `Err(items)`, so any non-sync link falls back to streaming. `&self` keeps
  the chain intact for that fallback (the `Err` variant hands `items` back).

Runtime guards (things the type cannot see):

- `with_cancel` ⇒ no pass-through (the fused core has no cancel checks).
- `with_compute_workers` pin ⇒ no pass-through: the pin caps stage
  concurrency on a larger pool, and the fused core has no notion of "use
  only N threads of a pool" (`test_compute_workers_pin_survives_compute_pool`
  fails otherwise). Also avoids ~ms transient-pool construction per run.
- Per-stage `StageOptions::workers`/`buffer` pins ⇒ no pass-through — same
  policy, the pins express per-stage topology with no fused equivalent.
- `.ordered()` stays **eligible**: pass-through output IS input order.

Semantic changes vs the streaming path (documented in `StreamPipe::run`):
no backpressure (peak memory = input + output `Vec`s), unordered output
becomes input order (within the any-order contract), stage panics propagate
to the caller after partial-state cleanup instead of aborting the process.
`for_each` never passes through (its drain is a caller-thread `FnMut`).

---

### MPMC Channels (`channel.rs`)

Wraps `crossfire` with a unified API:

| Type                                  | Implementation                      |
| ------------------------------------- | ----------------------------------- |
| `SyncSender<T>` / `SyncReceiver<T>`   | `crossfire::mpmc::bounded_blocking` |
| `AsyncSender<T>` / `AsyncReceiver<T>` | `crossfire::mpmc::bounded_async`    |

Closure detection is delegated entirely to crossfire: `send`/`recv` return
`Closed` once crossfire's internal disconnect logic observes that all peers
have been dropped. No extra flag is maintained.

### MPSC Channels — Collector Optimisation

The streaming pipeline's collector is always the **sole consumer** of the final
output channel (multiple worker producers → one collector). This is an MPSC
topology, yet crossfire's `mpmc` module uses a CAS-based ring buffer
(`lock cmpxchg` on every dequeue) and a `Mutex<VecDeque>` waker registry —
both unnecessary for single-consumer patterns.

crossfire ships an `mpsc` module whose receiver uses:

- **`store`-based dequeue** instead of `lock cmpxchg` (single consumer → no
  contention to CAS against). Profiling showed the MPMC ring-buffer CAS
  dominates per-item cost (~20–40 % of channel throughput depending on
  contention).
- **`WeakCell` waker registry** (lock-free) instead of `Mutex<VecDeque>`.

youpipe exposes this via `MpscSender<T>` / `MpscReceiver<T>` (sync sender +
sync receiver) and `mpsc_async_channel` (`MpscAsyncSender` +
`MpscAsyncReceiver` — both ends async, used when async-stage consumer tasks
feed the sole async collector). The `StageSpawn` trait gains a
`spawn_single` method that creates the terminal stage's output channel as
MPSC instead of MPMC; `StreamPipe::try_run` calls `spawn_single` for the
terminal path — covering sync stages, fence links, expand, and
`AsyncStage` (whose `spawn_single` override builds the output channel as
`mpsc_async_channel`). Intermediate stage channels remain MPMC (their
receivers are shared across multiple worker threads via `clone`).

The collector itself is generic over a `RecvItem` (sync) or `AsyncRecvItem`
(async) trait, so `collect_sync` / `collect_async` drain either the MPMC or
MPSC backing with one implementation.

`SendItem<T>` / `RecvItem<T>` / `AsyncRecvItem<T>` traits abstract over the
channel backings (MPMC vs MPSC, sync vs async) so `spawn_stage` and the
collector functions are generic without virtual dispatch.

### Worker recv loops: anchor + burst-drain

Every sync consumer loop (`spawn_stage` / `spawn_expand_stage` workers, the
fence forwarder) uses a two-phase recv loop: one blocking `recv` anchors the
iteration (parks correctly when the channel is empty, exits on disconnect),
followed by a `try_recv` burst phase that absorbs already-queued items without
re-entering the blocking-recv preamble. A `Closed` verdict in either phase is
trusted as-is: the `crossfire >= 3.1.20` pin (crates/youpipe/Cargo.toml)
carries the upstream fix (issue #70) for the spurious-`Closed` race that once
required a confirming blocking `recv` after every `Closed`.

Expansion workers (`spawn_expand_stage`) take the user closure push-style
(`Fn(I, &mut Vec<N>)`) and own one scratch `Vec` per worker, cleared per
item: the `expand_emit` API runs allocation-free in the steady state,
while the owned-`Vec` `expand` API is a thin wrapper that mallocs per item
(see the `expand_heavy` bench notes in benchmarks.md — the malloc share of
wall time is small under glibc's tcache, but the API removes it
deterministically and keeps allocator jitter out of tails).

The async terminal collectors (`drain_*_async` in `state/stream.rs`) use the
awaited analogue — one `recv().await` per burst, `try_recv` in between —
because tokio's coarse timer wheel batch-completes same-duration timeouts, so
one wake often finds several items already queued.

The async **stage consumers** (`spawn_async_consumers_body`) deliberately keep
the plain per-item `rx.recv().await` loop: porting the burst shape there
measured a pure wash (±0.5 %, spreads <1 %, `io_async_pure`/`io_async_mixed`
at 200/500/2000, 5 interleaved rounds). Mechanism: crossfire's
`MAsyncRx::recv` polls `try_recv` first and only registers a waker (after a
bounded spin) when the queue is empty, so an awaited recv on a non-empty queue
never pays the waker round-trip the burst loop would amortize — see the
`NOTE(perf)` on `spawn_async_consumers_body`.

The burst phase is what keeps multi-worker stages civil on the MPMC ring:
when `k` workers contend on one input channel, the burst winners drain the
backlog while the laggards park at the anchor — the contending population
thins itself instead of every worker hammering the ring once per item.
Without it, a light stage (per-item cost below the feeder's push interval)
hits a contention cliff: measured 8 workers burning 30 cores for 1/4 the
throughput of 2 workers (best case unreachable; convoy bistability). The
terminal collectors have used the same shape since their burst-drain
introduction.

---

## Ordered Output (`state/reorder.rs`)

`ReorderBuffer<T>` restores original element order after parallel processing. It is a fixed-size array of `2^k` slots addressed by bitmask: `seq & mask` maps a sequence number to its slot.

1. Each element is sent with a sequence number `(seq, item)`
2. `insert_into(seq, item, &mut Vec<T>)` writes the item directly into slot
   `seq & mask` (constant time, no comparison), then drains any contiguous run
   ready at the tail **straight into the caller's sink** — zero per-item
   allocation. (The older `insert` returned a fresh `Vec<T>` per call; in the
   in-order steady state that returned `Vec` had length 1, so the ordered
   collector paid a `malloc` + `free` per item purely to move a single value.
   `insert_into` is the hot-path variant; `insert` remains as a thin wrapper
   for tests / ergonomic callers.)
3. `flush_remaining()` collects whatever is still outstanding (e.g. on disconnect) and returns it sorted by `seq` — the only path that pays for a comparison sort

Slot layout: each slot is one `u64` tag + the `MaybeUninit` item, with
`occupied` folded into the tag (`seq.wrapping_add(1)`; `0` = unoccupied) —
16 B per slot for `u64` items instead of the 24 B a `seq + bool + item`
layout costs (+50 % window density, half the bytes read per slot probe in
the flush scan; `test_slot_density` guards the layout). Measured on
`stream_pipeline/single_stage_ordered`: 1K −8.3 % (25/25 dominant, 5-round
isolated interleaved A/B); 100K noise (the family's ±20 % round drift).

Capacity contract: because of the bitmask mapping, the number of simultaneously outstanding (un-flushed) items must stay below the slot count or two distinct `seq`s alias the same slot and the older item is dropped. Callers size the buffer to at least the maximum out-of-order window; the streaming collectors clamp it to `[1 Ki, 1 Mi]` slots.

---

A fence lets the caller decide how strictly two adjacent stages are isolated, via `FenceMode`:

- **`FenceMode::Barrier`** — hard isolation: stage 1 must fully drain before stage 2 receives any item.
- **`FenceMode::Chunked(k)`** — soft batching: forward a batch of `k` items as soon as it accumulates, so stage 2 overlaps stage 1 (the right default for mixed CPU/IO workloads).

Data flow:

1. Stage1 workers pull from `in_rx` → process → send to `mid_tx`
2. Fence thread **eagerly drains** `mid_rx` into a `FenceBarrier<T>`, releasing batches to `fenced_tx` per `mode` (immediately in `Chunked`, or all at once on disconnect in `Barrier`)
3. Stage2 workers pull from `fenced_rx` → process → send to `out_tx`

Stage completion is signalled purely by channel disconnect (all sender clones dropped) — no per-stage counter barrier is needed. Eager draining is essential: it prevents stage 1 from blocking on a full `mid` channel, which previously deadlocked when `items.len()` exceeded the channel buffer.

Batch-allocation recycling: `push` hands each full chunk to the forwarder via `mem::take`, which historically dropped the allocation and regrew the next batch from capacity 0 (`log2(k)` reallocs per batch). `FenceBarrier::reuse` lets the forwarder return the drained `Vec`; the next flush swaps it in, so the steady state is zero allocator traffic per batch (−23 % on `stream_pipeline/with_fence/100 K`).
