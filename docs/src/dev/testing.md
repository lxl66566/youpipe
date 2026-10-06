# Miri & Loom Compatibility


Canonical runners (they reap stale miri processes, apply per-binary
timeouts and the required flag combinations):

```sh
perf/verify/miri.sh            # lib + integration binaries + vendored crossfire lib
perf/verify/loom.sh            # youpipe + vendored queue + crossfire + st3 models
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
`pool/sleep_mask.rs`, `handoff/notify.rs`) source their atomics, locks, and
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
`UnsafeCell::new` under loom: loom cells carry model state that the
fork's `set_len`-over-uninitialized-memory allocation cannot provide
(symptom: index-out-of-bounds in `loom::rt::cell::Cell::start_write`).

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

### Build-profile guard (`lib.rs`)

youpipe ships a `.cargo/config.toml` override (`opt-level=3`, `panic=unwind`) that applies inside its workspace but is **not** inherited by downstream crates. Two downstream profile settings are known to be harmful; one is detected at compile time, the other is documented only:

- `panic = "abort"` — disables `catch_unwind`, so the `LeafGuard` / `ForEachGuard` panic-safety paths never run; any panic inside a pool worker aborts the process instead of propagating. Detected in `lib.rs` via `#[cfg(panic = "abort")]` (the deprecated-const warning trick) — this is accurate inside the library compilation, unlike cargo's build-script `CARGO_CFG_PANIC` env var which mirrors the build-script's own panic strategy (always `unwind`), not the target crate's.
- `opt-level = "s"` / `"z"` — disables the leaf-loop auto-vectorizer (~2× regression on the lightweight warm path). No longer detected at compile time: the rationale and the `[build] rustflags = ["-C", "opt-level=3"]` override recipe live in youpipe's own `.cargo/config.toml` comment for downstream users to copy.

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
