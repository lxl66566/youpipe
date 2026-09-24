//! Deterministic channel-throughput benches: the two-thread 1P1C ping-pong
//! shape of the criterion `channel_bench`, counted as instructions instead of
//! wall time. youpipe's inter-stage MPMC caliber and collector MPSC caliber
//! against crossbeam and std bounded channels, all at capacity 256.
//!
//! Counting caliber: all threads, process total (see `common/mod.rs`) — the
//! producer/consumer threads' work is only visible with collection on from
//! process start. The two `thread::spawn`/`join` pairs run inside the
//! measured region, identical on every row.
//!
//! ```sh
//! cargo bench -p youpipe-gungraun --bench channel
//! ```

// gungraun's macros emit `::`-rooted paths into the generated harness;
// the allow keeps that harmless if this crate ever inherits a workspace
// `unused_qualifications` lint.
#![allow(unused_qualifications)]

mod common;

use std::hint::black_box;

use gungraun::prelude::*;

/// Canonical two-thread shape: one producer sends `size` items, one consumer
/// counts recvs until the senders drop. Returns the recv count.
///
/// Macro, not a generic helper: the impls expose no common send/recv trait
/// (youpipe returns `ChannelError`, std/crossbeam their own error types), so
/// monomorphization would need a per-row adapter trait costing more code than
/// the four duplicated lines it removes. (Same trade as the criterion
/// channel_bench's `row!`.)
macro_rules! ping_pong {
    ($name:ident, $mk:expr) => {
        fn $name(size: usize) -> usize {
            let (tx, rx) = $mk();
            let producer = std::thread::spawn(move || {
                for i in 0..size as u64 {
                    // Every row's send is infallible at runtime but returns
                    // its own error type; unwrap is the honest caliber.
                    #[allow(clippy::unwrap_used)]
                    tx.send(i).unwrap();
                }
                // tx drops here: closing the channel terminates the consumer.
            });
            let consumer = std::thread::spawn(move || {
                let mut count = 0usize;
                while rx.recv().is_ok() {
                    count += 1;
                }
                count
            });
            producer.join().unwrap();
            #[allow(clippy::unwrap_used)]
            let count = consumer.join().unwrap();
            count
        }
    };
}

// Inter-stage handoff caliber: bounded MPMC (crossfire mpmc::Array).
ping_pong!(yp_mpmc, || youpipe::channel::<u64>(256));
// Terminal-collector caliber: bounded MPSC — the recv side skips the MPMC
// CAS (see youpipe's handoff/channel.rs "MPSC section").
ping_pong!(yp_mpsc, || youpipe::handoff::mpsc_channel::<u64>(256));
// External anchors, same bounded caliber.
ping_pong!(crossbeam, || crossbeam_channel::bounded::<u64>(256));
ping_pong!(std_sync, || std::sync::mpsc::sync_channel::<u64>(256));

macro_rules! channel_row {
    ($row:ident, $f:expr) => {
        #[library_benchmark]
        #[bench::size_10k(10_000)]
        fn $row(size: usize) -> usize {
            black_box($f(size))
        }
    };
}

channel_row!(yp_mpmc_row, yp_mpmc);
channel_row!(yp_mpsc_row, yp_mpsc);
channel_row!(crossbeam_row, crossbeam);
channel_row!(std_sync_row, std_sync);

library_benchmark_group!(
    name = channel_1p1c,
    compare_by_id = true,
    benchmarks = [yp_mpmc_row, yp_mpsc_row, crossbeam_row, std_sync_row]
);

// Invoked at item position: the macro expands to the `fn main` itself.
main!(
    config = common::count_all_threads(),
    library_benchmark_groups = [channel_1p1c]
);
