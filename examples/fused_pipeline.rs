use youpipe::{Pipeline, PipelineConfig, Workload};

fn main() {
    let items: Vec<i32> = (0..10_000).collect();
    let result = Pipeline::new()
        .map(|x: i32| x + 1)
        .filter(|x: &i32| x % 3 == 0)
        .map(|x: i32| x * 10)
        .with_config(PipelineConfig::default().with_workload(Workload::Balanced))
        .collect(items);

    assert!(!result.is_empty());
    println!(
        "fused pipeline: {} items after map+filter+map",
        result.len()
    );
}
