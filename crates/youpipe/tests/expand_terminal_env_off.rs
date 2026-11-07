//! Env-override path of the built-in expand shard-depth tier: an explicit
//! `YOUPIPE_SHARD_DEPTH=0` pin must shadow the per-terminal-kind default
//! exactly as it shadows any config tier (`resolve_knob`: env > config >
//! default). One test per binary — the env tier reads once into a
//! process-global OnceLock, so the pin must be set before the first run.

use youpipe::stream;

#[test]
fn expand_terminal_env_off_pin() {
    unsafe { std::env::set_var("YOUPIPE_SHARD_DEPTH", "0") };
    let src: Vec<u64> = (0..2_000).collect();
    // The expand terminal whose built-in default is Slots(16): pinned back
    // to the plain split depth, output semantics are unchanged.
    let mut got: Vec<u64> = stream(src.clone())
        .expand_emit(|x: u64, out: &mut Vec<u64>| {
            out.push(x);
            out.push(x.wrapping_add(1));
            out.push(x.wrapping_add(2));
        })
        .run();
    got.sort_unstable();
    let mut expected = Vec::with_capacity(src.len() * 3);
    for x in &src {
        expected.extend_from_slice(&[*x, x + 1, x + 2]);
    }
    expected.sort_unstable();
    assert_eq!(got, expected);
}
