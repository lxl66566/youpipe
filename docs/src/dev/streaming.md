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
- Per-stage `SyncStageOptions::workers`/`buffer` pins ⇒ no pass-through — same
  policy, the pins express per-stage topology with no fused equivalent.
- `.ordered()` stays **eligible**: pass-through output IS input order.

Semantic changes vs the streaming path (documented in `StreamPipe::run`):
no backpressure (peak memory = input + output `Vec`s), unordered output
becomes input order (within the any-order contract), stage panics propagate
to the caller after partial-state cleanup instead of aborting the process.
`for_each` never passes through (its drain is a caller-thread `FnMut`).

#### Panic semantics

Three execution planes, one contract: a stage closure's panic must reach the
`run()` caller — never a silent truncation.

- Fused pass-through: caught per chunk, resumed on the caller after
  partial-state cleanup (`ErasedFailure` slot).
- Streaming sync/expand workers and fence forwarders: pool jobs and
  dedicated threads abort the process on panic (channel-parking jobs cannot
  unwind past a pool worker; see `spawn_stage_jobs`).
- Streaming feeder: caught, payload stored, resumed by `Feeder::finish`
  after the collect.
- Streaming async stages: the runtime isolates panics at the task boundary
  and drops the payload with the unobserved JoinHandle, so each task is
  wrapped in `CatchTaskPanic` — the payload is recorded in a per-run
  first-wins `PanicSlot` and re-raised by `try_exec` after the collector
  returns (which the panicking task itself releases by ending and dropping
  its channel ends). The same mechanism surfaces nested `run()` misuse:
  `block_on` inside a runtime task re-raises with the actionable
  `spawn_blocking` hint instead of silently killing every consumer task.
  The ordered drain runs under `catch_unwind` so a recorded payload wins
  over the `emitted + dropped == n` accounting assertion (a panicked task
  legitimately drops its in-flight item).

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
`mpsc_async_channel`). A zero-stage chain pinned to the streaming path
(`with_cancel` / worker pins) dispatches on `StreamStart`, whose
`spawn_single` bridges the MPMC feeder receiver into an MPSC ring on a
dedicated OS thread (the standard cross-mode bridge shape and lease
exemption) — without the override the terminal-channel debug_assert
rejected the identity chain's MPMC receiver. Intermediate stage channels
remain MPMC (their receivers are shared across multiple worker threads via
`clone`).

The collector itself is generic over a `RecvItem` (sync) or `AsyncRecvItem`
(async) trait, so `collect_sync` / `collect_async` drain either the MPMC or
MPSC backing with one implementation.

`SendItem<T>` / `RecvItem<T>` / `AsyncRecvItem<T>` traits abstract over the
channel backings (MPMC vs MPSC, sync vs async) so `spawn_stage` and the
collector functions are generic without virtual dispatch.

#### Sharded terminal fan-in (per-worker SPSC rings)

Even on the MPSC backing, the terminal fan-in is still a **multi-producer**
ring: every terminal worker's `send` runs `compare_exchange` on the same
`sender` cursor (crossfire `array_queue_mpsc::push_with_ptr`), so producers
contend with each other per item and the collector's dequeue shares those
cache lines — the hotpath audit (todo #1, 2026-09-28) found the engine paced
by exactly this collector-side data plane (~365 ns/item, ~20x the channel's
bare capacity).

`YOUPIPE_SHARDED_TERM=1` (opt-in, default off) swaps the terminal channel
for **one SPSC ring per worker** (`handoff/sharded.rs`):

```
          OFF (one shared MPSC ring)          ON (per-worker SPSC shards)
                                        w1 ──[ring 1]──┐
w1 ─┐                                   w2 ──[ring 2]──┤ round-robin
w2 ─┼──[one ring, CAS on send]── collector   ...       ├── burst pass +
wk ─┘                                   wk ──[ring k]──┘ anchor block
```

The producer's CAS has no contenders, the collector's `store`-based
dequeue owns each shard's cache lines, and a slow worker parks on its own
ring instead of holding the shared one. Channel shape only — worker count,
parking-lease budget, and every semantic contract are unchanged.

Collector loop (`drain_*_sharded`, `state/stream.rs`): a full round-robin
**burst pass** drains each shard until `Empty` (same per-shard burst shape
as the stage workers' recv loops; the pass start rotates for fairness),
then an **anchor** blocks on the first open shard from the rotating cursor.
Blocking on one shard is deadlock-free: every open shard's producer either
sends (waking the anchor), parks upstream (progress elsewhere feeds it), or
drops its sender (closing the shard) — the anchor always wakes with the run
progressing; items landing in other shards meanwhile are picked up by the
next pass. Boundary parking is NOT used (falsified for burst rhythms,
e1684fc → aa842a6) — the anchor parks on crossfire's own spin-then-park
recv.

EOF aggregation: a shard reports `Closed` only after its sole sender is
dropped *and* the ring is drained, so the stream ends when every shard has
closed. A worker that exits early (panic unwind drops its sender) merely
closes its own shard while the others keep flowing — same contract as the
shared ring's all-senders-drop, verified by the `handoff::sharded` unit
tests and the `tests/sharded_terminal.rs` end-to-end suite (unordered,
ordered — the `ReorderBuffer` never depended on arrival order —, expand,
fence mid-chain, for_each, cancel, small-worker shapes).

Per-shard capacity is `max(4, buffer / workers)`: the aggregate stays at
the single-channel backpressure point while each ring keeps enough slack
for the burst rhythm. Scope: sync and expand terminals (both feeder
flavours) and the async terminal (see the async flavour subsection below);
a single-worker terminal never shards (one SPSC ring already is the plain
channel).

A/B (`sharded_term` evidence bench, 5-round isolated interleaved same-binary
knob runs, 32 cores): **no regression anywhere**, all 12 ids dominant or
stable improvements — 100K: single cheap −31.7 %, single cpu −29.1 %,
ordered −29.6 %, expand cheap −58.5 %, workers2 −23.2 %, multi2 −8.0 %;
1K: −4.9 % … −42.5 % (workers2 included, so no auto threshold is needed).
Full table in benchmarks.md. Default stays off pending broader soak (fence
/ convoy interactions, todo #4); the pathological long-run convoy seen on
the OFF side during smoke (26-core burn, collector spinning inside
crossfire `_read`'s stamp wait) did not reproduce on the ON side — tracked
under todo #4.

#### Async terminal flavour (per-task-group async shards)

The async terminal (`spawn_async_consumers_body_single` — both feeder
flavours funnel there, 2026-10) is the same fan-in shape with runtime
tasks as producers: `io_concurrency` tasks all `send().await` on one
shared `MpscAsyncSender` clone set, so every send CASes the same sender
cursor. Under the same `YOUPIPE_SHARDED_TERM` knob,
`sharded_mpsc_async_channel` swaps it for one async shard ring per
**task group**: the shard count is `min(io_concurrency, async_workers)` —
only `async_workers` tasks can sit inside `send` concurrently (they run on
that many runtime threads), so shards beyond that number cannot reduce
producer contention and only add collector pass cost and per-run channel
construction (a small run would otherwise build up to `io_concurrency`
rings up front). Task `i` owns shard `i % shards` for its whole life, so a
large fan-out shares each ring among `io_concurrency / shards` producers —
per-ring contention drops from `io_concurrency`-way to that quotient. Both
shapes drain through `drain_*_async_sharded`, the awaited analogue of the
sync sharded loops (`FinalRx::AsyncSingle` / `FinalRx::AsyncSharded`).

The anchor keeps the sync one-shard semantics: await the first open shard
from the rotating cursor. Waker aggregation across shards (registering the
collector task's waker with every open shard) was considered and rejected:
crossfire's MPSC receiver registry is a per-channel single-slot `WeakCell`
and `reg_waker_async` allocates a fresh `ArcWaker` per registration (the
per-thread immortal-waker cache only covers *blocking* wakers), so an
any-of-N await pays N allocations per wake — at shard counts in the tens
that exceeds the anchor cost it replaces. The one-shard anchor trades wake
latency for batching instead (items landing in other shards wait for the
next burst pass), the same trade the sync side measured as a win. Liveness
transfers verbatim: a shard's producers are live tasks that either send
(firing that shard's recv waker), await upstream (progress elsewhere
eventually feeds them), or drop their senders (closing the shard); a task
that panics mid-run closes its shard while the others keep flowing — its
payload is recorded (`CatchTaskPanic`, see [Panic
semantics](#panic-semantics)) and re-raised on the `run()` caller after the
collector returns — verified by `tests/sharded_terminal_async.rs`
(unordered/ordered/expand/for_each, `io_concurrency` 1 / 2 / above
`async_workers` sharing, producer panic, cancel, single-item input).

Measured verdict (2026-09-29, `sharded_term_async` bench, 10 interleaved
same-binary knob rounds): the async flavour **regresses the shapes it was
built for** — one-sync-prefix-into-`stage_async` chains read +42…+51 %
@100K (stable, 0/100 cross-round wins; +2.5…+3.8 % @1K). Only the
async-only feeder-saturated chain improves (−57.5 % @100K, where 128
tasks sending densely make single-ring sender contention dominate). The
anchor trade the sync side measured as a win inverts here: with the
collector's waker on ONE shard, a full ring's producers park on space only
the collector's next burst pass releases — while the collector waits on
the anchored shard's next item — so global pacing collapses onto the
anchor ring's rhythm; with the single shared ring every send wakes the
collector directly. Producers as runtime tasks (not OS threads) are what
makes the asymmetry bite. The flavour stays opt-in under the same knob;
(c) closed as falsified — readings and the mixed-pipeline warning in
benchmarks.md "Async sharded-terminal A/B".

#### Batched ring operations (`YOUPIPE_BATCH_RECV`)

Sharding removes the send-side fan-in but every ring op is still per item:
the collector's `try_recv` pays one `recv` SeqCst store per item on lines the
senders keep invalidating (hotpath: p50 551 ns/item, 31-sender ping-pong),
and the single feeder pays one tail CAS per item into the 31-consumer input
ring (901 ns/item avg, 94 % of feeder wall — see benchmarks.md
"true-streaming"). `YOUPIPE_BATCH_RECV` (unset = off) amortizes the cursor
updates over runs of consecutive slots (fork extension, same opt-in family
as `YOUPIPE_SHARDED_TERM`; A/B through the runtime knob, same binary):

- **MPSC `pop_batch`** (`array_queue_mpsc`, the terminal rings): one `recv`
  load + one `recv` SeqCst store per claimed *run*; per item only the stamp
  Acquire load and the value move. The stamp walk needs no tail bound — a
  slot beyond the producers' tail carries an older lap's stamp and stops the
  run; a slot whose producer has claimed-but-not-yet-stored also stops it
  (the blocking `pop`'s spin keeps the bounded-wait contract). Consumers
  are single by construction, no CAS.
- **MPMC `pop_batch`** (`array_queue`, the mid/worker rings): a read-only
  stamp walk over positions `head, head+1, …` (the per-item readiness rule
  `stamp == pos + 1`), then ONE head CAS claims the whole run — any
  interfering consumer moves the head and fails the CAS, and a producer
  cannot touch a slot whose stamp equals `pos + 1`, so the checked values
  are stable until popped. Bounded retries keep it non-blocking; the
  per-item `pop` path keeps the unbounded loop.
- **MPMC `try_push_batch`** (feeder side): symmetric — one tail CAS claims a
  run of free slots, then value writes + Release stamps. Prefix semantics:
  sends the longest free prefix, returns the count; the caller pushes the
  remainder with per-item blocking `send`. **Backpressure semantics are
  unchanged**: park granularity stays per item (only when full), item order
  and capacity are identical. MPSC send-side batching is deliberately absent
  — producers would have to buffer outputs to batch them, which changes
  streaming latency (not semantically equal).

Integration points (all behind the one knob, cap clamped 2..=4096, default
64): the collector's burst phase (`drain_unordered/ordered[_sharded]`,
batch claim then a single per-item `try_recv` probe to resolve the
inconclusive 0-claim into Empty/Closed), the worker/fence-forwarder burst
phase (`claim_burst` in `handoff/channel.rs` — batch first, anchor on miss),
and the feeder push loop (`push_items`: stage a batch, send the free
prefix, per-item send the tail). Cap 0 = the historical per-item loops
byte-for-byte. Async terminals keep per-item `try_recv` (p50 ≈ 20 ns,
nothing to amortize).

Correctness notes: the batch claims exactly-once runs (stamp-walk + single
CAS protocol, fork unit tests + `tests/batch_recv.rs` exactly-once suites
under multi-producer/multi-consumer interleaving, miri tree-borrows
included); `on_recv`/`on_send` fire once per *item* so parked-producer wake
counts are unchanged; cancel checks stay per item (staged-but-unsent feeder
items get their destructors run explicitly on abort — a
`Vec<MaybeUninit<_>>` drop would skip them, leaking every `I: Drop`; same
contract as in-flight items).

**MPSC hint invariant** (hard-won, 2026-09): `_start_read` treats any
`tail_cached != head` as "an item exists" and `_read` then *spins* for the
matching stamp (bounded-wait assumption). The per-item path advances `head`
one slot per call, so it can never step past the cached tail hint — each
step re-runs the `==` emptiness check and refreshes on exact hit. A batch
JUMP can: the walk is stamp-bounded, not tail-bounded, and legitimately
consumes past a stale-low hint, leaving `tail_cached < head` in `recv` —
which the next per-item `try_recv` misreads as "an item exists" and spins on
a stamp from a dead lap forever (reproduced: 4 producers / batch drain,
`recv={head=40000, tail_cached=16}`, collector burning a core at 100 %).
`pop_batch` therefore clamps the stored hint up to the new head
(`tail_cached.max(head)`) — hint == head reads as "maybe empty, refresh
from sender", and the hint still never exceeds the real tail because the
walk only covered published slots. The MPMC ring is immune (no tail hints:
its `_start_read` checks the stamp and returns, never spins).

Testing note (`tests/batch_recv.rs`): a liveness bug in this family can sit
INSIDE the fork's uninterruptible stamp spin, so budgets on our own loops
cannot bound it — every potentially-hanging scenario (concurrent
exactly-once suites, each e2e `run()`, the send/recv interplay test) runs
its whole body on a worker thread behind a `recv_timeout` watchdog that
fails the test in seconds; the stalled worker leaks until process exit
(which is why a hung test thread must never be the one running the body —
libtest joins test threads, so an inline hang holds the process and the
shared cargo lock hostage).

## Worker recv loops: anchor + burst-drain

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

2026-09: the same bistability shows up shape-dependent at scale — a zero-CPU
`stage(bump).fence(Chunked(500)).stage(bump)` chain runs 226 ms @100K while
the 3-stage variant runs 36 ms, and a two-sync-prefix → `stage_async` chain
flips between 23 and 244 ms across runs of the same shape (fusion-of-
adjacent-sync-stages was falsified on this evidence; canary
`sync_fuse/fence_infra`). Fully attributed 2026-09-29 — see the next
section.

### Convoy collapse forensics (`convoy-probe`, 2026-09-29)

The bistability above is attributed with a dedicated harness
(`crates/youpipe-bench/src/bin/convoy-probe.rs`): one shape × R in-process
runs, per-run wall time plus per-thread `/proc` context-switch deltas
grouped by thread name (`yp-pool` = stage workers / feeder / forwarder,
`tokio-worker`, `main`), optional crossfire episode dump
(`--features crossfire-trace` + `CONVOY_TRACE_PATH`). Calibre: 100 K `u64`
items, `taskset -c 1-31` (pool = 31 threads), 8 runs per cell; bistable
cells report min–max because the median is a mode mixture that shifts
across processes. "parks/run" = voluntary switches of the `yp-pool` group —
the futex-park proxy.

**The collapsed regime is a per-item park+wake trickle, not a contention
meltdown.** Signatures of the slow mode (`stage(bump).fence(Chunked(500))
.stage(bump)`, 14+14 workers, ~208 ms median):

- ~195 K voluntary parks per run ≈ **2 per item**; the collector parks
  ~1 K/run. The surgical cells pin it exactly: `--w1 1 --w2 20`
  (1.19 µs/item) shows **100.5 K parks/run = one park + one crossfire
  `fire()` wake per item** on the fence-output channel (1 serial producer →
  20 parked consumers); `--w1 20 --w2 1` shows 232 parks/run — the
  zero-park pipelined regime.
- perf stat per run: 170 K context-switches, 2 037 cpu-migrations,
  9.2 G cycles / 3.9 G instructions (IPC 0.42), vs the fast cell's
  938 / 16 / 2.3 G / 0.35 G. perf record: **80.6 % of cycles in the kernel
  scheduler** (futex wake/wait, `yield_task_fair`, `select_task_rq_fair`);
  top user symbols: crossfire `blocking_tx::Tx<mpmc::Array>::send` 7.9 %,
  youpipe `SyncReceiver::try_recv`/`recv` 3.7 / 3.0 %, `parking_lot
  RawMutex::lock_slow` 1.0 % (the `RegistryMulti` waker-queue mutex).
- crossfire-trace episode classes (n = 2 000, tracing itself distorts
  timing — only the class mix is usable): anchor `recv` entered ~1.9×/item,
  real parks ~2×/item, `wake rx` 1.7×/item, `wake tx` ~0.02×/item —
  producers essentially never park on `Full`; **bursts never form** (the
  anchor is re-entered before the burst `try_recv` ever sees a second
  item).

Mechanism: crossfire's blocking recv parks after a fixed 6-PAUSE spin
(`Backoff::SPIN_LIMIT`, crossfire `backoff.rs`) — far shorter than any
inter-arrival gap set by a parked serial neighbour (~1–2 µs wake RTT).
Whenever a channel's supply side is a single rate-limited entity (feeder
job, fence forwarder) and its demand side is a crowd (≳ 10–12 workers),
the channel hovers empty, every arrival wakes one-of-many parked consumers,
and the wake RTT becomes the pipeline's clock: wall ≈ items × RTT
(100 K × ~2 µs ≈ 200 ms). Each additional serial-supplier→crowd interface
adds one more park per item (the 14+14 default has two — feeder→stage1 and
forwarder→stage2 — hence 2/item).

Boundary matrix (fence shape, medians in ms; w1/w2 = stage workers around
the fence; "auto" = 14/14):

| cell | med | note |
| --- | --- | --- |
| w1/w2 = 20/1, 24/1 | 25–28 | zero-park; upstream population irrelevant |
| 1/1, 2/2, 4/4, 8/8 | 5–31 | fast (process-to-process spread at the small end) |
| 9/9 → 14/14 (auto) | 49 → 209 | cliff on **w2** ≈ 10–13, not on w1 |
| 1/15, 1/20, 1/24 | 35 → 113 | slow via the fence-output crowd alone |
| 15/15, 16/16 (both dedicated mode) | 209, 50 | pool and dedicated both collapse |
| chunk 1/10/64/500/5000/Barrier @14/14 | 189–225 | batch rhythm irrelevant |
| per-stage buffer 64…4096 @14/14 | 203–211 | depth of every non-feeder channel irrelevant |
| per-stage buffer 16 384 / config 4096 | 75 / 71 | partial: fast mode returns (min ≈ 22), unstable mixture |
| buffer = n (feeder inline) | 25 | fully rescued |
| `--cost cpu` @14/14 vs 8/8 | 210 / 12.5 | CPU work neither triggers nor rescues |

async family (`stage(f1).stage(f2).stage_async(g)`): collapses from a
smaller total than the fence family — 4+4 bimodal 26↔175 across processes,
8+8 ≈ 200, auto 15+15 ≈ 230, and **every asymmetry stays slow** (15/1 =
151, 1/8 = 228, 8/2 = 118): both the feeder→stage1 crowd and the
mixed-mode channel's async waker fan-out contribute (`io_concurrency` = 128
tasks register in one `RegistryMulti`; a `fire()` wake fans out to the
whole queued crowd), and the async terminal adds a collector-side park
stream (main ≈ 2.2 parks/item in slow runs — the todo #1 signature). The
immune references: single sync prefix `.stage(f).stage_async(g)` = 26 ms,
async-only = 79 ms (feeder-paced), and pure sync 15→15 (`sync2` anchor) =
22.8 ms with **31 parks/run at full pool occupancy** — balanced populations
keep a backlog, so consumer bursts always re-form and nobody parks.

Bistability seeding is cross-run pool state: 500 ms idling between runs
restores the fast mode (median 21 ms); back-to-back runs stay collapsed;
fresh processes mostly start collapsed (independent-process medians
239/164/155 ms).

Hypothesis verdicts (todo #4):

1. **mixed-channel backpressure wake storm — falsified as the root cause**:
   the fence family reproduces the full pathology with no async anywhere;
   the mixed channel's async `RegistryMulti` fan-out is an amplifier in the
   async shape, not the common denominator (single-prefix async chains are
   fast).
2. **fence `Chunked` release rhythm — falsified**: chunk sweep is flat.
3. **burst-drain contention collapse — confirmed in corrected form**: the
   pathology is burst-drain *failing to engage* — no bursts ever form, so
   there is nothing for the burst winners to drain; the collapse is one
   park + one wake per item at every serial-supplier→crowd interface.

Fix directions (candidates, not implemented): (a) **pre-anchor adaptive
spin** in the youpipe recv loops (`spawn_stage` / `spawn_expand_stage` /
`forward_fenced`): before the blocking `recv()`, spin on `try_recv` for a
bounded budget gated on recent channel activity, so consumers ride out
trickle gaps and bursts re-form while genuinely idle stages still park —
attacks both interfaces of the fence shape and the worker side of the async
shape; (b) **forwarder batch-send**: let the fence forwarder push a
released chunk with `try_send` and park at most once per chunk boundary,
removing the fence-output interface's per-item send parks; (c)
crossfire-level: a larger pre-park spin for blocking recv or a wake-one
(not wake-all-queued) `fire()` policy in `RegistryMulti` — the fork is
in-repo, but global backoff widening is a falsified pattern in the pool
context (dead-ends.md), so any widening must be activity-gated rather than
a constant.

Same-root notes: todo #1 (terminal fan-in) is the collector-side face of
the same serial-endpoint × crowd park/wake economy — the 2.2
collector-side parks/item in slow async runs are exactly the
`drain_unordered` occupancy reported there, so a sharded terminal channel
(#1) and a pre-anchor spin (#4a) fix opposite ends of one pathology. The
`yield_now()` crossfire performs after every full-channel send completion
(`blocking_tx::return_ok!`) supplies the scheduler churn seen under strace
(~16 `sched_yield` per item in slow runs) and is worth keeping in mind for
the #14 hang forensics.

### Fix (a)+(b) landed: adaptive pre-anchor spin, fence batch send (2026-09-30)

Both fix candidates above are implemented as **default-off runtime
knobs** (`builder/typed/stream.rs`, `AnchorSpin` / `fence_batch_send`):

- `YOUPIPE_SPIN_ANCHOR=<µs>` — per-worker spin budget in the three sync
  recv loops (`spawn_stage` / `spawn_expand_stage` / `forward_fenced`).
  Before the blocking anchor, spin `try_recv` for the current budget with
  **backoff polling** (pause count doubles per poll up to 64 ≈ 1 µs between
  polls at the tail); the budget is gated on the measured anchor latency —
  a park that returned within 4× the knob window doubles the budget (the
  4× slack is load-bearing: with wake-one delivery a worker wins an item
  only every ~k gaps, so a strict `parked ≤ window` rule decays the whole
  crowd back to parking), a quiet park quarters it to zero. Constant spin
  windows stay falsified (dead-ends.md) — the gate is per-worker state, and
  an idle stage reaches pure parking within log₄(window) anchors.
- `YOUPIPE_FWD_BATCH=1` — the fence forwarder pushes a released batch with
  `try_send` back-to-back, parking at most once per full ring instead of
  per item (needs `MpscSender::try_send`, also added).

Readings (`convoy-probe`, same binary, fresh process per cell, 3
interleaved off/on rounds; the off side collapses in ~1/3 of processes,
medians in ms):

| cell | off | spin30 | verdict |
| --- | --- | --- | --- |
| fence auto 14/14 | 120–254 (bimodal) | 27–57 | catastrophic ≥180 ms mode eliminated |
| fence 9/9 | 12–107 (drifting) | 23–28 | stabilized at ~25 |
| fence 1/15 | 90–127 (bimodal, lucky 5.5) | 24–33 | fixed |
| fence 20/1 (pipelined ref) | 24–27 | 24–28 | untouched, no leak |
| async2/15-1 | 25–188 (bimodal) | 29–309 (bimodal) | **not fixed — different face** |
| soak 100K×10 ×3 procs | mode wanders, 646 ms outlier | 26–107, no ≥180 run | parks 90–197K/run → 0.3–2.6K |

The spin's cost side is real but bounded: in the rare lucky fast seed
(fence 1/15 r3: 5.5 ms) spin-on costs ~4× (25 ms) because consumers
poll-contend the single forwarder's stamp writes; `nv` ctx-switches rise
to 40–80K/run while voluntary parks collapse. In the sparse 20/1 shape both
`v` and `nv` *drop* under the knob (budget decays between gaps) — idle
stages do not burn CPU.

**Poll-ceiling sweep** (temporary env override, same binary): tight
polling regresses the fence family (pause cap 1 → 219 ms med, 8 → 193,
16 → 181 vs 64 → 33–67, 128 → 43–102): a crowd spinning at full rate
bounces the ring stamps faster than items arrive. `MAX_POLL_PAUSES = 64`
is kept. The async family is unmoved at every ceiling — see below.

**The async family is out of this knob's reach.** `async1` with 15 sync
workers (single prefix, mixed channel into 128 io tasks) reproduces the
collapse (236 ms off, 226 ms spin-on) while `w1=1` is fast both ways — the
slow face is the 15-worker crowd **sending into** the mixed-mode channel
plus the terminal drain: per-run ctx-switches in slow runs are 2.3
voluntary parks/item on the collector (`main`) and 1.2/item on `yp-pool`
(send-side `Full` parks) — not recv-anchor parks. Fixing it needs the
crossfire `RegistryMulti` wake policy (candidate (c)) and/or the async
terminal fan-in (todo #1 (c) closed as falsified, so the async terminal
stays a single MPSC). `async2/15-1` therefore remains open under #4.

`YOUPIPE_FWD_BATCH` standalone shows no independent rescue (fence auto med
205/165 with only the knob on): removing the forwarder's per-item send
parks does not help while the downstream crowd still parks per item on
recv. Combined with the spin it is within noise (occasionally slightly
better: fence-auto r3 min 15.6 vs 28.7 ms). Default stays off; the knob is
retained since it is free when the ring is not full.

Default policy: **both knobs stay off** (opt-in). The spin is a large win
exactly when the convoy pathology is present (serial supplier → worker
crowd ≥ ~10) and a measurable cost in the lucky fast regime; user guidance
lives in tuning.md.

## Pool-wide parking lease (deadlock freedom across concurrent runs)

A channel-parking job — a non-inline feeder, a sync/expand stage worker, a
fence forwarder — holds its pool worker until the run's channels drain: it
never returns to `find_work` while parked inside a crossfire send/recv. The
per-run liveness budget (`feeder + Σ stage workers + Σ fences ≤
pool_threads`, computed in `try_exec`)
is therefore only sound while a **single** run uses the pool. Concurrent
full-budget runs on a shared pool (the global pool under the parallel test
harness, or multi-threaded user code) each individually fit yet jointly
oversubscribe it: every worker parks on a full/empty inter-stage channel
while the still-queued jobs — either run's stage workers, or fused chunks
behind them in the injector FIFO — can never be popped. This was reproduced
deterministically (4 barrier-aligned 2-stage runs, 5/5 hangs; gdb: every
worker parked on `Tx::send`, collectors on `MpscReceiver::recv`, the fused
victim on `LockLatch::wait`) and is one confirmed component of the
intermittent `pipeline_integration` hang (todo P1 #14).

The fix is a **pool-wide lease** (`Registry::parking_slots` +
`ParkingLease` in `stream.rs`): before submitting its first job, a run
computes its upper bound of parking jobs (`feeder_slot + fences +
explicit_workers + unpinned_stages × min(per_stage, n)`, where the feeder
slot is predicted against the smallest possible buffer so it never misses
the pool-job case, and each fence contributes exactly one forwarder job)
and atomically CAS-leases that many slots from the registry
(`try_reserve_parking`, total ≤ `num_threads`). A run that does not fit the
remaining capacity — or executes on a worker of the same pool (nested
`run()`) — falls back to dedicated threads, exactly the pre-existing
fallback shape. Release is per job: each wrapped job returns its slot on
completion via a drop guard (unwind-safe), and the lease's own drop returns
the never-spawned remainder (`reserved − spawned`), so a run that granted
fewer workers than reserved (small `n` clamping) still drains fully.

Invariant restored: whenever a parking job is still queued, at least one
worker is free of parking jobs and eventually pops it (regular jobs are
finite by the pool's basic contract). Bench impact is one CAS per run plus
one `fetch_sub` per parking job — `mixed_load` A/B (3 interleaved rounds)
is all noise. Regression tests:
`test_concurrent_full_budget_runs_share_pool_no_deadlock` (deadlocked
pre-fix),
`test_concurrent_pinned_runs_mixed_admission_no_deadlock`.

Note the lease deliberately does **not** cover: async-bridge threads
(dedicated OS threads already), async consumers (runtime tasks), and fused
chunks (finite, never park on channels). Fence forwarders **are** covered
(since the pool-job conversion, 2026-10): in pool mode the forwarder is
submitted through `StreamCtx::spawn_stage_jobs` like a one-worker stage —
parks on the mid channel, wrapped by the lease's per-job drop guard, and
the `fences` term reserves its slot up front. In the dedicated-thread
fallback it keeps the pre-conversion shape (one OS thread per fence).

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

Capacity contract: because of the bitmask mapping, two *live* (un-flushed) `seq`s `capacity` slots apart alias one slot. Instead of silently overwriting, an aliasing insert **grows** the slot array first — doubling and rehashing the occupied slots (collision-free: occupied seqs are pairwise distinct mod the current capacity, which implies distinct mod double) — up to `max_capacity` (`ReorderBuffer::with_max`). Only at that caller-pinned ceiling is the older item dropped; a duplicate `seq` is dropped immediately (single-item-per-seq).

Window sizing (`state::stream::OrderedWindow`, 2026-10 B6/P-1 fix): the streaming collectors pre-size the window to the pipeline's in-flight occupancy (Σ channel buffers + Σ worker batch claims, accumulated during the spawn walk via `StreamCtx::note_in_flight`; floored at 1 Ki) and let it grow up to `next_pow2(n)`. The ceiling is the sound span bound: the single feeder assigns `0..n-1` exactly once, and `next_expected == m` implies seq `m` was never inserted (inserting it flushes through it), so the live span never exceeds `n - 1`. No *smaller* bound is sound — a straggler parked in one worker's closure lets arbitrarily many successors overtake it through the other workers (overtaking needs only transient co-residency in one stage's worker set), so the occupancy is a pre-size, not a correctness bound (measured pre-fix: n = 2 M, default buffers, 32 workers, seq 0 sleeping 400 ms → span ~2 M vs occupancy ~900, the old 1 Mi clamp dropped ~1 M items). Memory stays proportional to the *observed* span: in-order runs never allocate, well-behaved runs allocate ~occupancy, a real straggler grows toward the `n`-sized ceiling. `StreamPipe::with_reorder_window` pins the ceiling explicitly for callers that must bound memory, accepting counted drops beyond it.

Drop observability: a drop (span overflow at the pinned `max_capacity`, or a duplicate `seq` from an `expand` misuse) drops the older item, counts it in `ReorderBuffer::dropped`, and the duplicate arm trips a `debug_assert` at the drop site. The ordered collectors validate `emitted + dropped == expected` after the drain in debug builds (a fired cancel token exempts the equality — a cancelled run legitimately emits fewer items); release builds keep only the counter. With the default auto window the drop arm is unreachable (span ≤ n − 1 < `next_pow2(n)`).

---

A fence lets the caller decide how strictly two adjacent stages are isolated, via `FenceMode`:

- **`FenceMode::Barrier`** — hard isolation: stage 1 must fully drain before stage 2 receives any item.
- **`FenceMode::Chunked(k)`** — soft batching: forward a batch of `k` items as soon as it accumulates, so stage 2 overlaps stage 1 (the right default for mixed CPU/IO workloads).

Data flow:

1. Stage1 workers pull from `in_rx` → process → send to `mid_tx`
2. The fence forwarder **eagerly drains** `mid_rx` into a `FenceBarrier<T>`, releasing batches to `fenced_tx` per `mode` (immediately in `Chunked`, or all at once on disconnect in `Barrier`)
3. Stage2 workers pull from `fenced_rx` → process → send to `out_tx`

Stage completion is signalled purely by channel disconnect (all sender clones dropped) — no per-stage counter barrier is needed. Eager draining is essential: it prevents stage 1 from blocking on a full `mid` channel, which previously deadlocked when `items.len()` exceeded the channel buffer.

Forwarder placement (2026-10): in pool mode the forwarder is a **leased
pool job** (see "Pool-wide parking lease"), same shape as a one-worker
stage — this removed the per-run `thread::spawn` + join (measured −3.8 %
on `stream_pipeline/with_fence/1K`, 61 µs/run, 25/25 dominant; 100 K noise
— see benchmarks.md) that the feeder side shed earlier. When the lease cannot host the
run, or `run()` executes on a worker of the same pool, the forwarder keeps
a dedicated OS thread. `fence_with(FenceOptions, mode)` reads the fence's
output-channel capacity from `FenceOptions::buffer` (only meaningful knob —
the forwarder is single-threaded); `fence(mode)` keeps the default
(`buffer_size` with the `parallelism * 4` floor).

Batch-allocation recycling: `push` hands each full chunk to the forwarder via `mem::take`, which historically dropped the allocation and regrew the next batch from capacity 0 (`log2(k)` reallocs per batch). `FenceBarrier::reuse` lets the forwarder return the drained `Vec`; the next flush swaps it in, so the steady state is zero allocator traffic per batch (−23 % on `stream_pipeline/with_fence/100 K`).
