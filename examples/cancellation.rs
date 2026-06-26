//! Cooperative cancellation: stream().with_cancel(token) lets an external
//! signal abort the pipeline mid-flight.
//!
//! `CancellationToken` is checked by the feeder, every stage worker, and every
//! bridge thread on each iteration. Once cancelled, in-flight items are
//! drained to completion but no new items are accepted.
//!
//! ```text
//! cargo run --example cancellation
//! ```

use std::{thread, time::Duration};

use youpipe::{CancellationToken, stream};

fn main() {
    let token = CancellationToken::new();
    let items: Vec<u32> = (0..10_000).collect();

    // Simulate slow per-item work so cancellation has time to fire.
    let slow = |x: u32| -> u32 {
        thread::sleep(Duration::from_micros(50));
        x * 2
    };

    // Canceller thread: signal abort after a short delay.
    let cancel_handle = {
        let token = token.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            token.cancel();
        })
    };

    let start = std::time::Instant::now();
    let result = stream(items)
        .with_cancel(token)
        .stage(slow)
        .run();
    let elapsed = start.elapsed();

    cancel_handle.join().unwrap();

    // We should have processed only a fraction of the 10_000 items.
    let n = result.len();
    println!("cancelled: processed {n}/10000 items in {elapsed:?}");
    assert!(n < 10_000, "expected cancellation to abort early, got {n}");

    // Without cancellation the full 10_000 items would take
    // 10000 × 50 µs ≈ 0.5 s on a single worker. With cancellation + the
    // pool's parallelism, we finish as soon as the token fires + drain.
    assert!(
        elapsed < Duration::from_millis(500),
        "cancellation should shortcut the run"
    );
}
