# criterion-perf-counters

A [Criterion.rs](https://github.com/criterion-rs/criterion) measurement
plugin for Linux `perf` hardware events — retired instructions, cycles,
cache misses, branch misses, and friends — as a drop-in alternative to
wall-clock time.

Maintained fork of
[criterion-perf-events](https://github.com/criterion-rs/criterion-perf-events)
(dead upstream since 2023-05, pinned to criterion 0.4). Linux only.

## What it changes over upstream

- **criterion 0.8.** The `Measurement`/`ValueFormatter` trait shapes are
  unchanged, so this is a version bump plus the API gaps below.
- **Process-wide per-thread counters.** Upstream opens a single counter on
  the calling thread, so for any workload that runs on other threads (thread
  pools, async runtimes) it measures only the coordinator loop. This fork
  opens one counter per task in `/proc/self/task` — rescanned lazily, at
  most every 100 ms — and sums user-space counts across all of them. Pool
  workers spawned during criterion's (unmeasured) warm-up are covered.
- **Correct throughput formatting** for the criterion 0.8 `Throughput`
  variants, and reports name the event (`instructions`) instead of a
  hardcoded `cycles`.

## Usage

```toml
[dev-dependencies]
criterion = "0.8"
criterion-perf-counters = "0.5"
perfcnt = "0.8"
```

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_perf_counters::Perf;
use perfcnt::linux::{HardwareEventType as Hardware, PerfCounterBuilderLinux as Builder};

fn bench(c: &mut Criterion<Perf>) {
    let mut group = c.benchmark_group("fibonacci");
    group.bench_function("fast", |b| b.iter(|| 7_u64.pow(13)));
    group.finish()
}

criterion_group!(
    name = my_bench;
    config = Criterion::default().with_measurement(Perf::with_event_name(
        || Builder::from_hardware_event(Hardware::Instructions),
        "instructions",
    ));
    targets = bench
);
criterion_main!(my_bench);
```

The builder factory is called once per discovered thread, so set your
attribution flags there:

```rust
let mut builder = Builder::from_hardware_event(Hardware::Instructions);
builder.exclude_kernel().exclude_hv(); // user-space only
```

`exclude_kernel()` + `exclude_hv()` is required for unprivileged use at
`kernel.perf_event_paranoid >= 2` and keeps interrupt/timer noise out of
the counts (at the price of not counting syscall/kernel work).

## Counting semantics and measurement-window cost

- One counter per task, summed: covers every thread of the process, but
  threads that spawn **and** exit inside a single measurement window (e.g.
  `thread::spawn` inside a bench closure) contribute nothing.
- Every window pays `4 × n_threads` syscalls (enable/disable/read/reset per
  counter). Criterion opens **one measurement window per `b.iter` sample** —
  the whole sample's iterations run inside one window — which makes this
  cost negligible. `iter_batched(.., BatchSize::PerIteration)` instead opens
  one window *per iteration*: on a fast routine that multiplies the overhead
  by thousands and inflates the counts. Prefer plain `b.iter` (or a batched
  `BatchSize`) with perf events.

## What the counters are good for

- **Frequency-invariant work metrics.** Retired instructions do not depend
  on CPU boost, background load, or machine speed — a deterministic
  single-threaded code path reproduces its instruction count across runs
  essentially exactly, which wall-clock time never does.
- **Per-element work** (`Throughput`): instructions/element is a
  machine-independent code-quality signal; cycles vs instructions exposes
  stalls (IPC), and cache-misses is a stable memory-behavior signal.
- They do **not** rank implementations by themselves (more instructions can
  still mean faster), do not see kernel time, and cannot localize hot spots
  — for that, use `perf record`. Wall time remains the metric for
  "which is faster"; counters explain *why*.

## License

MIT OR Apache-2.0, same as upstream. See [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
