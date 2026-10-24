# Miri & Loom Compatibility


Canonical runners (they reap stale miri processes, apply per-binary
timeouts and the required flag combinations):

```sh
perf/verify/miri.sh            # lib + integration binaries + vendored crossfire lib
perf/verify/loom.sh            # youpipe + vendored queue + crossfire + st3 models
perf/verify/linkcheck.sh       # mdbook intra-book link targets (docs/src)
```

The `youpipe-sys` crate (workspace member `crates/youpipe-sys`) provides a
unified `Mutex`/`Condvar`/atomics API via
`cfg`:

| Environment | Mutex/Condvar | Atomics |
| ----------- | ------------- | ------- |
| Production  | `parking_lot` (fairer, no poisoning) | `std::sync::atomic` |
| Miri        | `std::sync` (newtype shim, infallible `lock()`) | `std::sync::atomic` |
| `--cfg loom` | `loom::sync` (newtype shim, infallible `lock()`) | `loom::sync::atomic` |

`parking_lot_core` resolves `WaitOnAddress` through `GetModuleHandleA`, a
Windows foreign function Miri cannot emulate, whereas the std primitives are
natively supported by the interpreter. The unified API lets callers write
`mutex.lock()` once and stay transparent to which backend is active.

The vendored `youpipe-crossfire` crate solves the same miri portability
problem with a lighter seam: its sources must stay byte-identical to the
fork repo, so instead of a newtype shim its `waker_registry.rs` picks the
mutex type by `cfg` (`parking_lot` normally, `std::sync` under `cfg(miri)`,
`loom::sync` under the `loom` feature) and a `reg_lock` helper absorbs the
`LockResult` shape difference. That crate's dev-dependency `captains-log`
was also dropped (both sides) — its unix libc assumptions failed the
Windows build of the test profile — so `perf/verify/miri.sh` runs the
vendored lib tests (`-p youpipe-crossfire --lib`) on every platform. The
handoff layer's blocking send/recv paths over that waker are exercised
end-to-end by `tests/handoff_channel.rs` (park/wake both directions,
disconnect-while-parked, the close-vs-rearm stale-entry interleaving,
`park_timeout`, multi-threaded contention), also part of `miri.sh`.

The pool's synchronization cores (`pool/sleep.rs`, `pool/latch.rs`,
`pool/sleep_mask.rs`) source their atomics, locks, and
`thread_yield` from `youpipe-sys` — *nothing else in youpipe does* — so under
`--cfg loom` exactly those primitives become model-checked simulations
while the rest of the crate keeps real ones. `Registry` spawns real OS
threads (which loom cannot simulate), so the model tests drive the
primitives directly with `loom::thread`:

```sh
# Model tests live in #[cfg(all(test, loom))] mod loom_tests
# inside each primitive's file. Filter to them — the regular (real-thread)
# tests cannot run on simulated primitives.
#
# The `--cfg loom` rustflag (NOT a cargo feature — see Cargo.toml for the
# rationale) is the ecosystem-standard loom switch, matching crossbeam and
# the vendored youpipe-concurrent-queue.
RUSTFLAGS="--cfg loom" cargo test --lib -- loom_tests
```

The vendored `youpipe-crossfire` is the one deliberate exception to the
rustflag convention: its waker-registry models (`mod loom_tests` in
`waker_registry.rs`) are gated on the `loom` **cargo feature** instead.
`--cfg loom` would leak into dependencies that carry their own `cfg(loom)`
test shims without the loom crate linked (e.g. event-listener), so that
crate gates explicitly and `loom.sh` runs it without RUSTFLAGS:

```sh
LOOM_MAX_PREEMPTIONS=2 cargo test -p youpipe-crossfire --features loom --lib -- loom_
```

The `-- loom_` filter in that command is mandatory, not cosmetic: running
the whole lib under `--features loom` false-fails the non-loom tests that
use std concurrency.

The vendored `youpipe-st3` is a second deliberate exception, in two ways:
it keeps upstream's own `st3_loom` cfg name (so its rustflag cannot leak
into dependencies carrying `cfg(loom)` shims), and loom is a **regular
target-gated dependency of the lib** (`[target."cfg(st3_loom)".dependencies]`),
not a dev-dependency. The lib itself swaps its atomics and buffer cells for
loom simulations under that cfg, so the integration models (`tests/loom.rs`)
link a genuinely loom-backed rlib. Upstream PR #10 demoted loom to a
dev-dep and gated the shims on `all(test, st3_loom)` — but integration
tests link the lib *without* `cfg(test)`, so under that form every model
degenerates to one real-atomic schedule and passes vacuously in ~0.00s
(caught by the 2026-10 review; the fork's 12 models had never actually
explored). Re-enabling them also required constructing buffer slots via
`UnsafeCell::new` under loom: loom's `UnsafeCell` is not plain data —
model locations must be registered — so exposing uninitialized slots via
`set_len` explodes loom on the first write (symptom: index-out-of-bounds
in `loom::rt::cell::Cell::start_write`); build the buffers per-slot under
`cfg(st3_loom)`.

```sh
# Upstream CI's preemption budget; models take 0.3s-2min each under real
# exploration (before the fix: 0.00s each).
LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg st3_loom" \
    cargo test -p youpipe-st3 --release --test integration
```

Toolchain-upgrade precondition for st3: the fork's `arc_new_in_place`
(`youpipe-st3/src/lib.rs`) hand-writes std's private `ArcInner` layout
(`{strong, weak, data}`, effectively repr(C)). It is backed by the
`debug_or_loom_assert!` ref-count checks at the end of that function, but
std does not contract that layout — after a toolchain upgrade, run the
crate's debug tests first (`cargo test -p youpipe-st3`) and only then trust
release results: a layout change trips the assertions loudly instead of
corrupting memory silently (2026-10 review R-10).

What the models cover:

- **SleepMask** — set/clear vs `wake_scan` interleavings, including the
  stale-bit rescan loop whose termination is the fix for the >64-thread
  deadlock (see the `sleep_mask.rs` module docs).
- **CoreLatch** — the UNSET→SLEEPY→SLEEPING→SET state machine raced against
  `set()`.
- **CountLatch (Blocking)** — two setters + `wait_spin` waiter; relaxed flag
  reads after the wait must observe the setters' writes (happens-before via
  the SeqCst counter + LockLatch mutex).
- **Sleep** — the full park/wake protocol: `announce_sleepy` → `sleep()`
  (mask pre-publish under the `is_blocked` mutex, counters CAS, final
  queue check, condvar park) vs `new_injected_jobs` (fence, JEC increment,
  wake scan). The models verify exhaustively that no interleaving loses a
  wake, and that `sleeping_threads` returns to 0 on every path. A fourth
  model races the same park against the `Stealing` `CountLatch`'s set arm
  (`CoreLatch::set` → conditional `notify_worker_latch_is_set`) — the wake
  path the on-pool hybrid dispatcher depends on.

Developing these tests surfaced one latent landmine, now guarded: the
wake heuristic's `awake_but_idle_threads()` computes
`inactive − sleeping`, which relies on the caller protocol invariant
(`start_looking` precedes any sleep attempt) — a violation wraps in
release and silently skips wakes. A `debug_assert` now catches it.

`CountLatch::wait_spin`'s spin budget (`OFF_POOL_SPIN_ITERS`) shrinks to 2
under loom to keep the model's state space small; the budget's only
synchronization role (the final mutex acquire) is exercised either way.
`CachePadded` drops its alignment under loom (no cache lines in the model).

### Miri workload scaling (`cfg!(miri)`)

Miri interprets ~1000× slower than native, so the integration tests scale
their heavy workloads down under `cfg!(miri)` instead of being skipped:
the same dispatch/queue/latch code paths run with fewer repetitions (e.g.
`cpu_heavy` 200 → 4 iterations, 50K-item batches → 2K — still far above
miri's 1-worker serial threshold of 64 items). Only tests whose
*assertions* are wall-clock based (`Instant`/heartbeat-gap) or that require
real multi-worker concurrency (fence-over-large-input, which deadlocks on
miri's single emulated worker) remain `#[cfg_attr(miri, ignore)]`d, with
the reason documented at each site.

Miri budget: tests that grind on real-channel feeder ping-pong must also
be scaled down under miri (e.g. 1280 items / 2x200 / 300), and the scaling
must cover the whole family, not only the obviously heavy tests — a
filter-ctp by-ref test kept n=10_007 where its wtc sibling had scaled to
503 and blew the 1800 s budget once debug assertions were re-enabled.

### Build-profile guard (`lib.rs`)

youpipe ships a `.cargo/config.toml` override (`opt-level=3`, `panic=unwind`) that applies inside its workspace but is **not** inherited by downstream crates. Two downstream profile settings are known to be harmful; one is detected at compile time, the other is documented only:

- `panic = "abort"` — disables `catch_unwind`, so the `LeafGuard` / `ForEachGuard` panic-safety paths never run; any panic inside a pool worker aborts the process instead of propagating. Detected in `lib.rs` via `#[cfg(panic = "abort")]` (the deprecated-const warning trick) — this is accurate inside the library compilation, unlike cargo's build-script `CARGO_CFG_PANIC` env var which mirrors the build-script's own panic strategy (always `unwind`), not the target crate's.
- `opt-level = "s"` / `"z"` — disables the leaf-loop auto-vectorizer (~2× regression on the lightweight warm path). No longer detected at compile time: the rationale and the `[build] rustflags = ["-C", "opt-level=3"]` override recipe live in youpipe's own `.cargo/config.toml` comment for downstream users to copy.

Separately, the workspace's own profile wiring has a cargo pitfall: cargo emits `-C debug-assertions` only when it differs from the default implied by the profile's opt-level, and it is blind to `[build]` rustflags, so opt-level overrides must live in profile tables, never in profile-blind rustflags (an opt-level override sitting in `[build]` rustflags silently left dev/test without debug assertions until it moved into `[profile.dev]`). Enabling the assertion layer immediately exposed two latent test bugs: a reorder alias assert that contradicted the deliberate-overflow unit tests (the invariant is the caller's, so the assert was removed), and an integration test doing u64 arithmetic that overflow checks reject.

# Fuzzing (cargo-fuzz)

Coverage-guided fuzzing lives in `crates/youpipe-fuzz` — a cargo-fuzz crate
excluded from the workspace (its nightly sanitizer flags would break plain
`cargo build --workspace`). Four targets, all structured-input harnesses:
each decodes a random program (item values, stage chain, knobs) from the
fuzzer byte stream and asserts the concurrent result against a serial
reference model.

| Target   | Covers |
| -------- | ------ |
| `pipeline` | fused `pipe`/`pipe_ref` chains (`map`/`filter`/`try_map`/`map_err`), knob combinations (`with_compute_workers`, `with_oversubscribe`, `with_workload`), `collect`/`for_each`/`try_collect` terminals |
| `stream`  | streaming topology and the fused pass-through (via the worker-pin flag), `expand_emit`, `fence`, ordered/unordered collection, tiny buffers for backpressure |
| `channel` | handoff channel: strict FIFO and exact capacity accounting in a single-threaded interleave, close propagation across sender/receiver clones, batch send/claim exact-count + EOF-transition semantics, MPMC and MPSC count/multiset/per-producer-FIFO conservation under real threads |
| `reorder` | `ReorderBuffer` re-sequencing against a `BTreeMap` model, interleaved with `flush_remaining`/`reset` |

Run from the repo root (nightly toolchain required):

```sh
cargo fuzz run --fuzz-dir crates/youpipe-fuzz <target> -- -max_total_time=60
```

Design notes:

- The builders are type-state, so stage kinds cannot be spliced at runtime.
  Each harness picks from a fixed set of chain templates with data-driven
  parameters — the fused core composes a chain into one closure per worker,
  so what coverage needs is every builder method and terminal exercised, not
  every chain shape.
- `stream` shares one process-global 2-worker `TokioPool` across iterations;
  `run()` would otherwise build and tear down a multi-thread runtime per
  iteration, dominating fuzzing throughput.
- The `channel` batch cases mirror the engine's call patterns, so they fuzz
  the `MaybeUninit` staging/`set_len` unsafe paths as production uses them:
  fresh values staged in a `MaybeUninit` slice for `try_send_batch` (the
  feeder), claims into a typed Vec's `spare_capacity_mut()` for
  `try_recv_batch` (`claim_poll`, the sharded terminal), and the MPSC case
  drives the same batch-first / blocking-`recv`-fallback drain rhythm the
  sharded receiver uses.
- Caller contracts the harness never violates: `.ordered()` + `.expand()`
  (documented panic), and `ReorderBuffer`'s outstanding-window `< capacity`
  precondition.
- `for_each` terminals assert multisets — rayon `par_iter().for_each` order
  semantics; `collect` and ordered `run` assert exact order.

Windows specifics:

- The MSVC ASan runtime is a DLL the loader must find next to the fuzz
  executables (or on `PATH`), otherwise the target exits with
  `STATUS_DLL_NOT_FOUND` before running a single input. Copy it once per
  `cargo fuzz build`:
  ```sh
  cp "/c/Program Files/Microsoft Visual Studio/2022/<edition>/VC/Tools/MSVC/<ver>/bin/Hostx64/x64/clang_rt.asan_dynamic-x86_64.dll" \
     crates/youpipe-fuzz/target/x86_64-pc-windows-msvc/release/
  ```
- Harness assertion panics abort via fast-fail before libFuzzer's death
  callback can write a `crash-*` artifact; the panic message on stderr is
  the evidence. Corpus replay usually does not reproduce low-probability
  races — re-fuzzing does.

## Fuzz findings, resolved

### `.ordered()` output order violation — harness template-folding bug

The `stream` target's ordered assertion fired on inputs decoding as
`ordered=true` with the expand template. That template's body never calls
`.ordered()` (expand + ordered is a documented panic), but the fold
`r.pick(5) % 2` mapped the ordered set onto `{0, 1}`, keeping the expand
template under `finish`'s exact-order assertion — legitimate
completion-order interleavings of an *unordered* pipeline were flagged as
divergences (adjacent swaps, 4-item displacements; the multiset was always
intact — the tell that this was not a library ordering race). The harness
now folds via an explicit ordered-compatible template-ID array, and the
library's ordered contract is pinned by `test_stream_ordered_exact_order_stress`
(`tests/pipeline_integration.rs`): randomized rounds over
workers/buffer/fence-mode/terminal shapes with exact input-order
assertions.

### Blocking channel spurious `Disconnected` — vendored crossfire

Re-fuzzing with the corrected harness found a real library bug: an
unordered expand chain sporadically lost an entire expand group. Root
cause: the per-thread blocking waker node is shared across every channel
a thread blocks on, and a foreign registry's `close()` racing the node's
re-arm validates a stale queue entry (no hb edge between two registry
mutexes) and stamps `Closed` onto the live episode — `commit_waiting`
then fails Init→Waiting with `Closed`, and `_send_bounded` /
`_recv_blocking` translated it into `Disconnected` while the peer was
alive. Artifact/corpus replay does not reproduce it; seed-driven fuzzing
does (~1 in 300k executions).

Fix (fork `waker-tl` @ `2894038`, vendored sync): the Closed stamp is a
hint, not a verdict — the blocking decision points re-verify against the
peer counter (`close_rx`/`close_tx` decrement it *before* closing the
registry, so count > 0 proves the stamp foreign) and re-arm on mismatch.
Checking after re-registration plus the decrement-before-close protocol
closes the lost-notification window, so the retry cannot park through a
real close. Async paths were never exposed: their waker nodes are
per-future, not per-thread. Deterministic regression tests (fork and
vendored) stamp the shared node Closed from outside mid-episode, on both
the send and recv sides.

# Suite duration census (2026-10-25)

Wall-clock audit of the full test/miri/bench suite (32-core NixOS host,
`CARGO_BUILD_JOBS=8`, warm registry, `bench.lock` held for every timing
run; native per-test times from `cargo nextest run --message-format
libtest-json` — requires `NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1` on nextest
0.9.146 — miri from per-target subset runs under the miri.sh flag combo,
bench ids from `cargo bench --no-run` + criterion `--list`). Trim proposals
with their protection costs are todo #18/#19; numbers below are the
baseline they trim against.

## Native (default members, debug)

| step                                               | wall                      |
| -------------------------------------------------- | ------------------------- |
| `cargo nextest run`, cold worktree (build + 360 tests) | 40 s                   |
| `cargo nextest run`, warm                          | 7 s (Σ per-test exec 32.4 s) |
| `cargo test` (libtest, all binaries + doctests)    | 12 s                      |
| `cargo test --doc`                                 | 2 s (32 passed, 1 ignored) |
| `cargo clippy --all-targets` cold / warm           | 3 s / 0 s, 0 warnings     |

The native suite is not the cost. Two tests hold 37 % of Σ exec:
`stream_reduce_alloc$streaming_aggregations_no_output_materialisation`
6.07 s (the 1 Mi-item shape) and `hybrid_assist$stress_streams_plus_
assist_drivers` 6.01 s (a fixed `sleep(6 s)` watchdog soak window). Next
three: `test_sync_to_async_does_not_stall_tokio_driver` 1.69 s,
`test_stream_ordered_exact_order_stress` 1.59 s, ordered_window
`ordered_straggler_tiny_buffer_returns_everything` 1.26 s. Largest
binaries by Σ exec: pipeline_integration 108 tests / 7.1 s, lib 155 /
2.1 s, scope_integration 26 / 0.9 s, compute_pool 6 / 0.8 s,
handoff_channel 8 / 0.7 s. Doctests are not run by nextest (known
limitation); `cargo test` covers them at the 2 s figure above.

## Miri (miri.sh default targets, tree-borrows + ignore-leaks)

Coverage refresh 2026-10-25 (todo #18): every integration binary added to
the default list was measured IN FULL under `bench.lock` (per-target
`MIRI_TIMEOUT_SECS`, miri.sh's `==>` timestamps); lib / doc / handoff /
scope / compute_pool / pipeline_integration keep the census estimates.
youpipe-crossfire lib is now measured (27 s full — the old ~4 min was an
extrapolation artifact).

| target                | listed / miri-ignored | duration (full binary)        |
| --------------------- | --------------------- | ----------------------------- |
| lib                   | 152 / 2               | ~9 min (extrapolated)         |
| doc                   | 33 / 1                | 2 min (measured)              |
| handoff_channel       | 8 / 0                 | 3 s (measured)                |
| scope_integration     | 26 / 3                | ~21 s (extrapolated)          |
| submit_batch_alloc    | 1 / 0                 | 5 s                           |
| filter_collect_panic  | 1 / 0                 | 5 s                           |
| pipe_range_alloc      | 1 / 0                 | 12 s                          |
| on_pool_nested        | 6 / 0                 | 26 s                          |
| pool_cache            | 6 / 3                 | 21 s                          |
| stream_reduce         | 4 / 0                 | 10 s (miri /20 divisor added; was 116 s unscaled) |
| expand_alloc          | 1 / 0                 | 20 s (miri /8 divisor added; was 187 s unscaled) |
| cancel_outcome        | 5 / 0                 | 63 s (already /20-scaled)     |
| async_waker_alloc     | 1 / 0                 | 169 s (miri /4 divisor added; was 238 s unscaled — the fixed floor-proof loop dominates) |
| sharded_terminal      | 1 / 0                 | 48 s                          |
| batch_recv            | 5 / 0                 | 30 s                          |
| graph_dag             | 20 / 2                | 85 s                          |
| sharded_terminal_async| 1 / 0                 | 281 s (12 full async topologies; scenario count is the floor) |
| compute_pool          | 5 / 0                 | 72 s (hybrid-driver test dominates) |
| pipeline_integration  | 108 / 14              | **~31 min** (extrapolated ±50 %; heavy tier 55–76 s/test) |
| youpipe-crossfire lib | 52 / 0                | 27 s (measured)               |

Sum of the 13 added integration binaries ≈ **13 min**; projected default
pass ≈ **57 min** with pipeline_integration's ±50 % dominating the error
bar. Bringing the pass back under ~48 min is todo #19's divisor work on
exactly that block (its heavy tier is 2/3 of the pass) plus compute_pool.

Deliberately NOT in the default list (reasons inlined in miri.sh too):

- `hybrid_assist` — its only test is `#[cfg_attr(miri, ignore)]`
  (24-thread timing stress); the safety angle stays covered by
  compute_pool's deterministic hybrid-driver test.
- `profile_guard` — asserts native debug-assertions config; not
  unsafe-adjacent, meaningless under the interpreter.
- `ordered_window` — the B6/P-1 repro shapes are size-pinned (n > 1 Mi IS
  the 1 Mi-window-clamp bug boundary); scaling vacates the assertion and
  unscaled n = 100 K–1.1 M items projects to hours (killed at 150 s on
  test 2/4). The `OrderedWindow` bookkeeping itself has lib unit tests
  (miri-covered via lib).
- `stream_reduce_alloc` — the counter's signal needs N ≥ 1<<17 (≥1 MiB
  output allocation vs the size filter) and the streaming topology
  measures ~33 ms/item-pass under miri (stream_reduce scaled: 10 s for
  ~300 item-passes), so 5 × 131 K passes projects to hours (killed at
  125 s). Its delegating-only GlobalAlloc shim class is miri-covered via
  expand_alloc / pipe_range_alloc / submit_batch_alloc.

Miri amplification over native is shape-dependent (35× on the
ordered-stress shape, ~1700× on fence_large_input, ~0.1 ms/item on the
park-free pipe_range path vs ~33 ms/item-pass on the streaming topology),
so per-test extrapolation inside pipeline_integration keeps its ±50 %; a
one-off timed full pass (with `MIRI_TIMEOUT_SECS=1800` per binary) is the
cheap follow-up that pins it.

The miri build itself is cheap (`cargo miri test --no-run` 6 s after a
native build; per-binary interpreter startup ≤ 4 s).

## Loom

31 models total: pool cores 16 (sleep 4, latch 3, sleep_mask 9), st3 6,
concurrent-queue 3, crossfire waker-registry 6. Documented per-model range
0.3 s–2 min under exploration → full `perf/verify/loom.sh` pass ≈ 10–30
min. Not re-timed this round (no loom-relevant changes in the audited
range).

## Benches

`cargo bench --no-run` (default members): 64 s release build at jobs=8.
**273 criterion ids across 12 binaries** at the shared tuned config
(~3.5 s per id: 20 samples, 1 s warmup, 2 s measure — benches/common/
mod.rs) ≈ **16 min for one full criterion pass**. The grid:
sync_vs_rayon 88, unbalanced 46, sync_fuse 26, io_async 24, expand 16,
graph_shapes 16, pool_reuse 12, sharded_term 12, channel_bench 10,
async_vs_tokio 9, sharded_term_async 8, mixed_load 6. `horizontal` is a
custom scenario harness (not criterion; 69 scenario×lib×n cells in the
current results.json × 5 rounds × ~0.7 s measure ≈ 8–15 min a pass);
youpipe-bench adds 4 one-shot bins (convoy-probe, file-encrypt,
horizontal-counters, hotpath-profile) plus perf_events;
youpipe-gungraun's Ir benches are valgrind-count driven.

## Where the time goes

native + clippy ≈ 1 min · miri ≈ 0.8 h (pipeline_integration ≈ 2/3) ·
loom ≈ 10–30 min · benches ≥ 30 min for full sweeps. The levers and their
protection costs: todo #19; the miri.sh coverage gap discovered by the
census (4 of 20 integration binaries in the default target list): todo
#18.

## 2026-10-25 code-review audit (8ab8610..5ff8f5d): verified non-issues

One line each; real findings are todo #20–#22.

- `FanIn::anchor`'s `select().expect(..)`: crossfire's `Select::select`
  returns `Err` only with zero registered handlers; `open > 0` guards it.
- crossfire `try_push_batch`: a torn snapshot can only overcount the free
  run with a stale tail, which the claim CAS then rejects;
  `real_positions`' slot-backed count handles the cap..one_lap phantom
  positions (cap-3 lap-wrap test); the claim-then-write window is bounded
  by `pop_batch`'s stamp walk and `_read`'s spin.
- `stage_batch` (7d3cff3): panic-free pointer loop (`reserve` precedes
  every move), `batch.set_len(0)` prevents double-drop; matches the
  miri-clean claim.
- `dispatch_jobs`' wrapper: the done-counter decrements on every path
  (panic caught → recorded → cancel); async→sync bridge threads are
  uncounted but their EOF propagates channel-wise before downstream
  workers can finish, so `done.wait()` ordering holds.
- `resolve_budget`: `need` is gated atomically by `try_reserve_parking`;
  nested-run detection (`is_on_this_pool`) falls back to dedicated
  threads (tested both ways).
- `Broadcast::send` / `AsyncBroadcast::send` swap_remove survivor loops
  are correct (the async side keeps `txs`/`slots` pairing).
- `claim_poll`'s half-close contract survives the batch path (stragglers
  drained before Closed; the ProbeMock tests pin the single-probe shape).
- Stream tx-batch gate `!has_async_stage` matches the falsification
  record; cancel item-set parity between the batched and per-item arms
  holds on the stream side (the graph side is todo #21).
