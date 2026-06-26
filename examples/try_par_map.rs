use youpipe::try_par_map;

fn main() {
    let items: Vec<i32> = (0..100).collect();
    let result = try_par_map(items, |x: i32| -> Result<i32, String> {
        if x == 42 {
            Err(format!("error at {x}"))
        } else {
            Ok(x * 2)
        }
    });

    match result {
        Ok(v) => println!("all ok: {} items", v.len()),
        Err(e) => println!("failed: {e}"),
    }
}
