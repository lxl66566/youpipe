use youpipe::scope;

fn main() {
    let multiplier = 7i32;
    let result = scope(|s| {
        let items: Vec<i32> = (0..20).collect();
        s.pipeline(items)
            .map(|x: i32| x * multiplier)
            .par_map(|x: i32| x + 1, 4)
            .collect()
    });

    let mut sorted = result;
    sorted.sort_unstable();
    println!("scoped pipeline: {:?}", sorted);
}
