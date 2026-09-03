# Publishing

> [← Documentation index](README.md)

How the workspace crates ship to crates.io, and how local development stays
on relative paths while published builds resolve registry versions.

## Crate inventory

| Crate | Directory | Published |
| ----- | --------- | --------- |
| `youpipe` | `/` (workspace root) | yes |
| `youpipe-sys` | `crates/youpipe-sys` | yes |
| `youpipe-st3` | `crates/youpipe-st3` | yes (fork of upstream st3) |
| `youpipe-concurrent-queue` | `crates/youpipe-concurrent-queue` | yes (fork of upstream concurrent-queue) |
| `youpipe-criterion-perf-counters` | `crates/youpipe-criterion-perf-counters` | yes (fork of criterion-perf-events) |
| `youpipe-bench-counter` | `crates/youpipe-bench-counter` | no (`publish = false`) |
| `youpipe-bench-file-encrypt` | `crates/youpipe-bench-file-encrypt` | no |
| `youpipe-bench-hotpath-profile` | `crates/youpipe-bench-hotpath-profile` | no |
| `youpipe-bench-pipeline` | `crates/youpipe-bench-pipeline` | no |

## Path + version dual dependencies

Workspace-internal dependencies are declared with **both** a `path` and a
`version`:

```toml
youpipe-sys = { path = "crates/youpipe-sys", version = "0.5" }
st3         = { package = "youpipe-st3", path = "crates/youpipe-st3", version = "0.5" }
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
first. All crates share the workspace version train (`[workspace.package]`,
currently `0.5`):

```sh
cargo publish -p youpipe-concurrent-queue
cargo publish -p youpipe-st3
cargo publish -p youpipe-sys
cargo publish -p youpipe-criterion-perf-counters   # independent, order-free
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

1. Bump `[workspace.package] version` (one place; all inheriting crates move
   together). The vendored forks pin their versions in their own manifests —
   bump those in the same commit.
2. `cargo test --workspace && cargo clippy --workspace --all-targets`.
3. `cargo package -p <crate> --list` for each crate: no stray files, the
   root package's `exclude` keeps `crates/`, `perf/`, docs and tooling
   out of the `youpipe` tarball.
4. Publish in the order above.
