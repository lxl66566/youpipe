//! Guards the workspace profile configuration. `[build] rustflags` is
//! profile-blind: while `-C opt-level=3` lived there, rustc inferred
//! `debug-assertions = off` for dev/test and the whole debug-side invariant
//! layer (reorder accounting, fused geometry asserts, push_n length guards,
//! overflow checks — whose rustc default follows debug-assertions) was
//! silently compiled out of every `cargo test` run. If this test fails, that
//! layer is compiled out again.

#[test]
// `cfg!` is a constant, but the check must stay a *runtime* failure: a
// compile-time assert would break `cargo test --release`, where
// debug-assertions are legitimately off.
#[allow(clippy::assertions_on_constants)]
fn debug_assertions_are_active() {
    assert!(cfg!(debug_assertions));
}
