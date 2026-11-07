//! End-to-end semantics of the expand terminal under the BUILT-IN
//! shard-depth tier (env unset, config unset): the expand-family default
//! floors the terminal shards deeper than the plain split
//! (`ShardDepth::terminal_default`), and this file pins that the deeper
//! bounded rings change nothing observable — output multiset, group
//! integrity, EOF — while exercising exactly the construction path the
//! default promotes. One test per binary: the knobs are process-global
//! OnceLocks, so any sibling test initializing them first would shadow
//! the default tier this file exists to read.

use youpipe::stream;

fn items() -> Vec<u64> {
    (0..2_000).collect()
}

#[test]
fn expand_terminal_default_depth_semantics() {
    let src = items();
    // Unordered expand fan-out (a terminal kind the default lifts):
    // x -> (x, x+1, x+2). No cancel token needed — expand stages decline
    // the fused pass-through, so this IS the streaming sharded terminal.
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

    // The ordered expand terminal rides the same default tier.
    let got = stream(src.clone())
        .ordered()
        .expand_emit(|x: u64, out: &mut Vec<u64>| {
            out.push(x);
            out.push(x.wrapping_add(1));
        })
        .run();
    let expected: Vec<u64> = src.iter().flat_map(|x| [*x, x + 1]).collect();
    assert_eq!(got, expected);

    // Multi-worker sanity behind the same default tier: a plain sync
    // terminal (kind: not expand — default stays Off) must be unaffected.
    let mut got: Vec<u64> = stream(src.clone())
        .stage(|x: u64| x.wrapping_mul(3).wrapping_add(1))
        .run();
    got.sort_unstable();
    let mut expected: Vec<u64> = src.iter().map(|x| x * 3 + 1).collect();
    expected.sort_unstable();
    assert_eq!(got, expected);
}
