use youpipe::par_map;

fn main() {
    let data: Vec<i32> = (0..10_000).collect();
    let result = par_map(data, |x| x * 2);
    assert_eq!(result.len(), 10_000);
    println!("par_map: {} items processed", result.len());
}
