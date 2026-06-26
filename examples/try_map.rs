//! Fallible parallel pipelines — chainable `try_map` with short-circuit on
//! first error.
//!
//! `try_par_map` is gone; the new chainable API lets you interleave
//! infallible `.map()` and fallible `.try_map()` stages freely.
//!
//! ```text
//! cargo run --example try_map
//! ```

use youpipe::prelude::*;

#[derive(Debug)]
enum ParseError {
    NotANumber,
    OutOfRange,
}

fn main() {
    let inputs: Vec<&str> = vec!["1", "23", "42", "x", "99", "-7"];

    // Chain: parse → range-check → format. The first `Err` aborts the chain;
    // `.try_collect()` returns `Result<Vec<_>, _>`.
    let result = inputs
        .pipe()
        .try_map(|s: &str| s.parse::<i32>().map_err(|_| ParseError::NotANumber))
        .try_map(|n| {
            if !(0..=50).contains(&n) {
                Err(ParseError::OutOfRange)
            } else {
                Ok(n * n)
            }
        })
        .map(|n| format!("n²={n}"))
        .try_collect();

    match result {
        Ok(v) => println!("all ok: {v:?}"),
        Err(e) => println!("aborted with {e:?}"),
    }

    // A clean run — same chain, no errors.
    let clean: Vec<&str> = vec!["1", "23", "42"];
    let ok = clean
        .pipe()
        .try_map(|s: &str| s.parse::<i32>().map_err(|_| ParseError::NotANumber))
        .try_map(|n| -> Result<i32, ParseError> { Ok(n * n) })
        .try_collect()
        .unwrap();
    assert_eq!(ok, vec![1, 529, 1764]);
    println!("clean run: {ok:?}");
}
