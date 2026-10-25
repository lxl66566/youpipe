//! `Perf` measures the selected perf events using the perf interface of the Linux kernel.
//!
//! Maintained fork of `criterion-perf-events` v0.4.0 — see `Cargo.toml` for
//! the list of changes on top of upstream (criterion 0.8, process-wide
//! per-thread counters, perf-event backend). Linux only.
//!
//! # Example
//!
//! ```rust
//! use criterion::{criterion_group, criterion_main, Criterion};
//! use criterion_perf_counters::{HardwareEvent, Perf};
//!
//! fn bench(c: &mut Criterion<Perf>) {
//!     let mut group = c.benchmark_group("fibonacci");
//!     group.bench_function("fast", |b| b.iter(|| 7_u64.pow(13)));
//!     group.finish()
//! }
//!
//! criterion_group!(
//!     name = my_bench;
//!     config = Criterion::default().with_measurement(Perf::new(HardwareEvent::Instructions));
//!     targets = bench
//! );
//! criterion_main!(my_bench);
//! ```
//!
//! # Counting semantics and measurement-window cost
//!
//! One counter is opened per task in `/proc/self/task` and summed, so the
//! measurement covers every thread of the process — including workers
//! spawned *before* the first measurement window (persistent pools such as
//! rayon's global pool spawn during warm-up, which is unmeasured). Threads
//! that spawn and exit inside a single window (e.g. hand-rolled
//! `thread::spawn` inside a bench closure) contribute nothing.
//!
//! Every window pays `4 × n_threads` syscalls (enable / disable / read /
//! reset per counter). Criterion opens **one measurement window per
//! `b.iter` sample** (the whole sample's iterations run inside one window),
//! which makes this cost negligible. `iter_batched(.., BatchSize::PerIteration)`
//! instead opens one window *per iteration* — on a fast routine that
//! multiplies the overhead by thousands and inflates the counts; prefer
//! plain `b.iter` (or a batched `BatchSize`) with perf events.
//!
//! Counters are opened user-space-only (`exclude_kernel` + `exclude_hv`,
//! the backend's defaults): required for unprivileged use at
//! `kernel.perf_event_paranoid >= 2`, and it keeps interrupt/timer noise
//! out of the counts — at the price of not counting syscall/kernel work.

use std::{
    cell::RefCell,
    collections::HashMap,
    io,
    time::{Duration, Instant},
};

use criterion::{
    Throughput,
    measurement::{Measurement, ValueFormatter},
};
use perf_event::{Builder, Counter, events::Hardware as Hw};

/// Lower bound between thread rescans. Persistent pools spawn their workers
/// during (unmeasured) warm-up, so a scan at the first window of each
/// benchmark id suffices in practice; the interval just bounds the cost for
/// workloads that do spawn threads mid-run, without paying a `/proc` walk on
/// every window of `iter_batched`-style callers.
const THREAD_RESCAN_INTERVAL: Duration = Duration::from_millis(100);

/// The hardware event a [`Perf`] measurement counts. The PERF_TYPE_HARDWARE
/// subset that is stable across x86/ARM and useful for bench reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HardwareEvent {
    /// Retired instructions (frequency-invariant work metric).
    Instructions,
    /// Elapsed CPU cycles.
    CpuCycles,
    /// Cycles at a fixed reference frequency (frequency-invariant time).
    RefCpuCycles,
    /// Last-level cache misses.
    CacheMisses,
    /// Retired branch instructions.
    BranchInstructions,
    /// Mispredicted branches.
    BranchMisses,
}

impl HardwareEvent {
    /// Canonical short name for criterion reports and axis labels.
    pub const fn report_name(self) -> &'static str {
        match self {
            HardwareEvent::Instructions => "instructions",
            HardwareEvent::CpuCycles => "cycles",
            HardwareEvent::RefCpuCycles => "ref-cycles",
            HardwareEvent::CacheMisses => "cache-misses",
            HardwareEvent::BranchInstructions => "branches",
            HardwareEvent::BranchMisses => "branch-misses",
        }
    }

    /// Open a fresh counter for this event, attached to the thread `tid`.
    ///
    /// The `perf_event::Builder` defaults are load-bearing: the counter
    /// starts *disabled* (warm-up must not leak into the first measurement
    /// window) and excludes kernel and hypervisor time (user-space-only
    /// attribution, see the module docs).
    fn open_for(self, tid: i32) -> io::Result<Counter> {
        let hw = match self {
            HardwareEvent::Instructions => Hw::INSTRUCTIONS,
            HardwareEvent::CpuCycles => Hw::CPU_CYCLES,
            HardwareEvent::RefCpuCycles => Hw::REF_CPU_CYCLES,
            HardwareEvent::CacheMisses => Hw::CACHE_MISSES,
            HardwareEvent::BranchInstructions => Hw::BRANCH_INSTRUCTIONS,
            HardwareEvent::BranchMisses => Hw::BRANCH_MISSES,
        };
        Builder::new().kind(hw).observe_pid(tid).build()
    }
}

/// `Perf` implements `criterion::measurement::Measurement` so it can be used
/// in criterion to measure perf events. Create it via `Perf::new()` with the
/// selected [`HardwareEvent`].
pub struct Perf {
    event: HardwareEvent,
    counters: RefCell<HashMap<i32, Counter>>,
    last_scan: RefCell<Option<Instant>>,
    formatter: PerfFormatter,
}

impl Perf {
    /// Creates a new criterion measurement plugin that measures the given
    /// hardware event across all threads of the process.
    ///
    /// # Panics
    ///
    /// Panics at the first measurement window if no counter can be opened
    /// (event unsupported by the CPU, or insufficient access rights); the
    /// panic message points at `kernel.perf_event_paranoid` in that case.
    pub fn new(event: HardwareEvent) -> Perf {
        Perf {
            event,
            counters: RefCell::new(HashMap::new()),
            last_scan: RefCell::new(None),
            formatter: PerfFormatter {
                event_name: event.report_name(),
            },
        }
    }

    /// Open counters for tasks that appeared since the last scan and return
    /// the set of open counters.
    fn counters(&self) -> std::cell::RefMut<'_, HashMap<i32, Counter>> {
        let rescan = match *self.last_scan.borrow() {
            Some(t) => t.elapsed() >= THREAD_RESCAN_INTERVAL,
            None => true,
        };
        if rescan {
            *self.last_scan.borrow_mut() = Some(Instant::now());
            let mut counters = self.counters.borrow_mut();
            for tid in task_tids() {
                if counters.contains_key(&tid) {
                    continue;
                }
                // The task may exit between the readdir and the open; that
                // race is expected and the thread's counts are simply not
                // representable then.
                if let Ok(counter) = self.event.open_for(tid) {
                    counters.insert(tid, counter);
                }
            }
            if counters.is_empty() {
                panic!(
                    "no perf counter could be opened — check kernel.perf_event_paranoid (>= 2 \
                     requires user-space-only counters) and kernel.perf_event_access sysctl"
                );
            }
        }
        self.counters.borrow_mut()
    }

    fn sum_all(&self) -> u64 {
        let mut counters = self.counters.borrow_mut();
        let mut total = 0;
        // Disable → read → reset: the reset keeps the next window independent
        // of everything counted before it. A failing counter belongs to a
        // task that exited mid-window; drop it (and its uncountable
        // remainder) rather than aborting the benchmark.
        counters.retain(|_, counter| {
            match (|| {
                counter.disable().ok()?;
                let v = counter.read().ok()?;
                counter.reset().ok()?;
                Some(v)
            })() {
                Some(v) => {
                    total += v;
                    true
                },
                None => false,
            }
        });
        total
    }
}

impl Measurement for Perf {
    type Intermediate = ();
    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        let mut counters = self.counters();
        if counters.is_empty() {
            panic!(
                "no perf counter could be opened — check kernel.perf_event_paranoid (>= 2 \
                 requires user-space-only counters)"
            );
        }
        for counter in counters.values_mut() {
            // A task can exit between our scan and this enable; its counter
            // is stale and gets dropped at the next end().
            let _ = counter.enable();
        }
    }

    fn end(&self, _i: Self::Intermediate) -> Self::Value {
        self.sum_all()
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &self.formatter
    }
}

/// Enumerate the task ids of this process (`/proc/self/task`).
fn task_tids() -> Vec<i32> {
    std::fs::read_dir("/proc/self/task")
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().into_string().ok())
                .filter_map(|name| name.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

struct PerfFormatter {
    event_name: &'static str,
}

impl ValueFormatter for PerfFormatter {
    fn format_value(&self, value: f64) -> String {
        format!("{value:.4} {}", self.event_name)
    }

    fn format_throughput(&self, throughput: &Throughput, value: f64) -> String {
        match throughput {
            Throughput::Bits(bits) => format!("{:.4} events/bit", value / *bits as f64),
            Throughput::Bytes(bytes) => format!("{:.4} events/byte", value / *bytes as f64),
            Throughput::BytesDecimal(bytes) => {
                let event_per_byte = value / *bytes as f64;

                let (denominator, unit) = if *bytes < 1000 {
                    (1.0, "events/byte")
                } else if *bytes < 1000 * 1000 {
                    (1000.0, "events/kilobyte")
                } else if *bytes < 1000 * 1000 * 1000 {
                    (1000.0 * 1000.0, "events/megabyte")
                } else {
                    (1000.0 * 1000.0 * 1000.0, "events/gigabyte")
                };

                format!("{:.4} {}", event_per_byte / denominator, unit)
            },
            Throughput::Elements(elements) => {
                format!("{:.4} events/element", value / *elements as f64)
            },
            Throughput::ElementsAndBytes { elements, .. } => {
                format!("{:.4} events/element", value / *elements as f64)
            },
        }
    }

    fn scale_values(&self, _typical_value: f64, _values: &mut [f64]) -> &'static str {
        self.event_name
    }

    fn scale_throughputs(
        &self,
        _typical_value: f64,
        throughput: &Throughput,
        values: &mut [f64],
    ) -> &'static str {
        match throughput {
            Throughput::Bits(bits) => {
                for val in values {
                    *val /= *bits as f64;
                }
                "events/bit"
            },
            Throughput::Bytes(bytes) => {
                for val in values {
                    *val /= *bytes as f64;
                }
                "events/byte"
            },
            Throughput::BytesDecimal(bytes) => {
                let (denominator, unit) = if *bytes < 1000 {
                    (1.0, "events/byte")
                } else if *bytes < 1000 * 1000 {
                    (1000.0, "events/kilobyte")
                } else if *bytes < 1000 * 1000 * 1000 {
                    (1000.0 * 1000.0, "events/megabyte")
                } else {
                    (1000.0 * 1000.0 * 1000.0, "events/gigabyte")
                };

                for val in values {
                    *val /= *bytes as f64;
                    *val /= denominator;
                }

                unit
            },
            Throughput::Elements(elements) => {
                for val in values {
                    *val /= *elements as f64;
                }
                "events/element"
            },
            Throughput::ElementsAndBytes { elements, .. } => {
                for val in values {
                    *val /= *elements as f64;
                }
                "events/element"
            },
        }
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        self.event_name
    }
}
