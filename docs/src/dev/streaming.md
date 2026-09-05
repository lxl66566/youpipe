# Data Transfer, Ordering & Fences


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
