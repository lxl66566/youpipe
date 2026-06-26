use std::{thread, time::Duration};

use youpipe::{CancellationToken, PipelineConfig, StreamPipeline};

fn main() {
    let token = CancellationToken::new();
    let config = PipelineConfig::default().with_compute_workers(4);
    let sp = StreamPipeline::new(config).with_cancel(token.clone());

    let items: Vec<i32> = (0..10_000).collect();

    let cancel_handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        token.cancel();
    });

    let result = sp.run(
        items,
        |x: i32| -> i32 {
            thread::sleep(Duration::from_micros(50));
            x * 2
        },
        false,
    );

    cancel_handle.join().unwrap();
    println!("cancelled: {}/10000 items processed", result.len());
    assert!(result.len() < 10_000);
}
