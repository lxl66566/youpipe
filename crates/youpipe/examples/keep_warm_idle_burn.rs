//! Idle-burn probe for the caller-declared keep-warm hint (todo #40).
//!
//! Measures the worst case the API docs promise to bound: a declaration left
//! armed (a leaked guard) after real work. Protocol mirrors the C2 knob
//! accounting (scheduler.md "Adaptive pre-park spin window"): 3x2M-item
//! fused batches to grow the adaptive window under load, then a 10 s idle
//! window; the reading is whole-process CPU time (getrusage utime+stime)
//! consumed during the idle window. Verdict band from the C2 session:
//! 0.03-0.67 ms per 10 s idle (pure parking reads the same); a permanently
//! spinning worker reads ~10,000 ms. Linux-only (getrusage); other
//! platforms no-op like the horizontal bench.
//!
//! ```sh
//! # leaked guard (worst case) vs no declaration (baseline)
//! cargo run --release --example keep_warm_idle_burn -- --leak
//! cargo run --release --example keep_warm_idle_burn
//! ```

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        hint::black_box as bb,
        time::{Duration, Instant},
    };

    use youpipe::ComputePool;

    // getrusage(RUSAGE_SELF): std has no CPU-time API and /proc/self/stat's
    // clock-tick granularity (10 ms) cannot resolve the 0.03-0.67 ms band.
    // Only the two leading timevals are read; the rest of rusage is ignored.
    #[repr(C)]
    struct Timeval {
        secs: i64,
        micros: i64,
    }
    #[repr(C)]
    struct RUsage {
        utime: Timeval,
        stime: Timeval,
        // `getrusage` writes the WHOLE struct (14 trailing longs: maxrss,
        // ru_minflt, ...). A 2-timeval struct lets it smash the stack —
        // found the hard way as a segfault at return, 2026-11-07.
        _rest: [u64; 14],
    }
    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    }
    fn cpu_time() -> Duration {
        let mut ru = RUsage {
            utime: Timeval { secs: 0, micros: 0 },
            stime: Timeval { secs: 0, micros: 0 },
            _rest: [0; 14],
        };
        let ru_ptr = std::ptr::from_mut(&mut ru);
        assert_eq!(unsafe { getrusage(0, ru_ptr) }, 0); // 0 = RUSAGE_SELF
        Duration::from_secs(u64::try_from(ru.utime.secs).unwrap())
            + Duration::from_secs(u64::try_from(ru.stime.secs).unwrap())
            + Duration::from_micros(ru.utime.micros.unsigned_abs() + ru.stime.micros.unsigned_abs())
    }

    let leak = std::env::args().any(|a| a == "--leak");

    let cpu_work = |x: u64| {
        let mut r = x;
        for _ in 0..100 {
            r = r.wrapping_mul(7).wrapping_add(13);
        }
        r
    };

    // 3x2M fused batches through the global pool: the window grows to its
    // ceiling under the same load shape the C2 readings came from.
    let data: Vec<u64> = (0..2_000_000).collect();
    for _ in 0..3 {
        bb(youpipe::pipe_ref(&data).map(|&x| bb(cpu_work(x))).collect());
    }

    if leak {
        // Worst case: the declaration outlives its producer. mem::forget
        // keeps the refcount armed for the rest of the process.
        std::mem::forget(ComputePool::global().keep_warm());
    }

    let before = cpu_time();
    let idle_start = Instant::now();
    while idle_start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(100));
    }
    let burned = cpu_time().saturating_sub(before);
    println!(
        "keep_warm_idle_burn: leaked={leak} idle={}ms cpu={:.3}ms",
        idle_start.elapsed().as_millis(),
        burned.as_secs_f64() * 1000.0,
    );
}

#[cfg(not(target_os = "linux"))]
fn main() {}
