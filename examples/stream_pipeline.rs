use youpipe::{PipelineConfig, StreamPipeline};

fn main() {
    let config = PipelineConfig::default().with_compute_workers(4);
    let sp = StreamPipeline::new(config);
    let items: Vec<i32> = (0..100).collect();

    let result = sp.run(items, |x: i32| x * 2, true);
    assert_eq!(result.len(), 100);
    assert_eq!(result[0], 0);
    assert_eq!(result[99], 198);
    println!(
        "stream pipeline (ordered): first={}, last={}",
        result[0], result[99]
    );
}
