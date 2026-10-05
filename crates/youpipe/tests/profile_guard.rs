//! Guards the workspace profile configuration. `[build] rustflags` is
//! profile-blind: while `-C opt-level=3` lived there, rustc inferred
//! `debug-assertions = off` for dev/test and the whole debug-side invariant
//! layer (reorder accounting, fused geometry asserts, push_n length guards,
//! overflow checks — whose rustc default follows debug-assertions) was
//! silently compiled out of every `cargo test` run. If this test fails, that
//! layer is compiled out again.

#[test]
fn debug_assertions_are_active() {
    assert!(cfg!(debug_assertions));
}
