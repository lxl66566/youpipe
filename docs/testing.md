# Miri & Loom Compatibility

> [← Documentation index](README.md)

Canonical runners (they reap stale miri processes, apply per-binary
timeouts and the required flag combinations):

```sh
perf/verify/miri.sh            # lib + all integration binaries
perf/verify/loom.sh            # youpipe models + vendored queue models
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

What the models cover:

- **SleepMask** — set/clear vs `wake_scan` interleavings, including the
  stale-bit rescan loop whose termination is the fix for the >64-thread
  deadlock (see the `sleep_mask.rs` module docs).
- **CoreLatch** — the UNSET→SLEEPY→SLEEPING→SET state machine raced against
  `set()`.
- **CountLatch (Blocking)** — two setters + `wait_spin` waiter; relaxed flag
  reads after the wait must observe the setters' writes (happens-before via
  the SeqCst counter + LockLatch mutex).
- **WaitGroup** — `done()`/`wait()` visibility and the 1→0 notify transition.
- **Sleep** — the full park/wake protocol: `announce_sleepy` → `sleep()`
  (mask pre-publish under the `is_blocked` mutex, counters CAS, final
  queue check, condvar park) vs `new_injected_jobs` (fence, JEC increment,
  wake scan). The models verify exhaustively that no interleaving loses a
  wake, and that `sleeping_threads` returns to 0 on every path.

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
