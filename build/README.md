# Build support

`git_identity.rs` and `windows_resources.rs` are included by crate build scripts.

## Project check

`python build/check.py [base-commit]` is the project check. It needs Python 3.9 or newer and
the Rust toolchain, on Windows or Linux; no shell is required.

The base is the argument, or `ELIOT_TASK_BASE` when no argument is given. It is
resolved once to a full commit id, and that id is used for the whole run. The check
fails without a base.

Cargo output is isolated from parent `.cargo/config.toml` settings. When
`ELIOT_ARTIFACT_ROOT` is set, the check uses `<artifact-root>/target` and clears
`RUSTC_WRAPPER`; without it, the check uses the worktree's ignored `target/project-check/` and
also clears the wrapper.

Changed files are everything that differs from the base: commits since it, staged and
unstaged edits, and untracked files that are not ignored. Git output is NUL-delimited
and renames are not followed.

Each changed file is mapped to the workspace member whose manifest directory contains
it, using the longest match. Members come from `cargo metadata --no-deps --locked`.

A change to the root `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `clippy.toml`,
anything under `.cargo/`, or a top-level `build/*.rs` selects every workspace member.
Changes that touch no member run only the fmt check.

Steps, stopping at the first failure:

1. `cargo fmt --check` for the workspace.
2. For each selected package, in name order: `cargo build --locked -p <pkg> --all-targets`,
   then `cargo clippy --locked -p <pkg> --all-targets -- -D warnings`.

```sh
python build/check.py origin/main
```
