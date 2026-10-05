# Publishing


How the workspace crates ship to crates.io, and how local development stays
on relative paths while published builds resolve registry versions.

## Crate inventory

| Crate | Directory | Published |
| ----- | --------- | --------- |
| `youpipe` | `crates/youpipe` | yes |
| `youpipe-sys` | `crates/youpipe-sys` | yes |
| `youpipe-st3` | `crates/youpipe-st3` | yes (fork of upstream st3) |
| `youpipe-concurrent-queue` | `crates/youpipe-concurrent-queue` | yes (fork of upstream concurrent-queue) |
| `youpipe-crossfire` | `crates/youpipe-crossfire` | yes (fork of upstream crossfire, per-thread blocking waker) |
| `youpipe-hotpath` | `crates/youpipe-hotpath` | yes (fork of upstream hotpath 0.24, patched alloc-stack depth) |
| `youpipe-criterion-perf-counters` | `crates/youpipe-criterion-perf-counters` | no (`publish = false`; criterion-perf-events fork for the lab benches) |
| `youpipe-bench` | `crates/youpipe-bench` | no (`publish = false`; opt-in lab benches) |
| `youpipe-gungraun` | `crates/youpipe-gungraun` | no (`publish = false`; deterministic Ir benches) |

## Path + version dual dependencies

Workspace-internal dependencies are declared with **both** a `path` and a
`version`:

```toml
youpipe-sys = { path = "../youpipe-sys", version = "0.6" }
st3         = { package = "youpipe-st3", path = "../youpipe-st3", version = "0.6" }
```

* **In this repo** cargo always builds against the `path` — no registry
  round-trip, edits apply instantly.
* **On `cargo publish`** the path is stripped from the shipped manifest, so
  downstream users resolve the crate from crates.io at the declared
  requirement. `cargo publish` refuses to publish a pair whose versions
  don't match the requirement, so the two can't drift silently.

This is the standard mechanism (tokio, rand, crossbeam all work this way);
no `[patch]` or feature tricks involved.

## Publish order

A crate's manifest must resolve from the registry alone, so dependencies go
first. All publishing crates share the workspace version train
(`[workspace.package]`, currently `0.6`); every non-publishing crate carries
`publish = false`, so `cargo publish --workspace` selects exactly the six
below:

```sh
cargo publish --workspace --keep-going   # one-shot; retry stragglers
# or, explicitly ordered:
cargo publish -p youpipe-concurrent-queue
cargo publish -p youpipe-crossfire
cargo publish -p youpipe-hotpath
cargo publish -p youpipe-st3
cargo publish -p youpipe-sys
cargo publish -p youpipe                            # last: depends on all above
```

`-p` form is required from the workspace root; publishing a member from
inside its directory works too (cargo resolves the workspace).

## Pre-flight checks

```sh
# What would ship (file list) — run per publishable crate
cargo package -p youpipe-sys --list

# Full packaging + verification build; only fails while a workspace
# dependency is NOT yet on crates.io (path deps are rewritten to registry
# deps for the verification). Before the first release this is expected;
# use --list + the plain test/clippy suites until then.
cargo package -p youpipe-sys
```

Checklist per release:

1. Bump `[workspace.package] version` (one place; every publishing crate
   moves together — the forks inherit it too) and the internal requirement
   strings in `[workspace.dependencies]` in the same commit.
2. `cargo test --workspace && cargo clippy --workspace --all-targets`, plus
   a `cargo test -p youpipe --no-default-features` pass: nothing exercises
   that feature combination in regular development (gungraun's bench build
   is what surfaced the 0.6.0 gating gaps — dead async paths, an ungated
   test binary, a doctest calling `stage_async_with`), so it needs an
   explicit check or it silently rots.
3. `cargo package -p <crate> --list` for each crate: no stray files. The
   `youpipe` package directory is self-contained (`src/`, `benches/`,
   `tests/`, `examples/`, README symlink), so docs/tooling can't leak into
   the tarball; cargo dereferences the README symlink to a regular file.
4. Publish in the order above.
