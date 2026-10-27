# Eliot

A local Rust controller for native coding-agent harnesses. The implementation is being
rebuilt in small connected layers: core/Store/bus → Codex/Luna → monitoring → automation
and scripts → agent communication → selected additional harnesses.

## Current implementation

L0 is complete: one dependency-free Rust 2024 binary with `--version` and `--help`.
The daemon, event bus and harness control are **not implemented yet**. The next task is
[L1.1 in the roadmap](docs/roadmap.md#l1--core-store-bus-and-one-fake-command-path).
Do not advertise future commands as working or restore the old controller to fill gaps.

## Build

Install Rust through rustup; `rust-toolchain.toml` pins 1.99.1 with rustfmt and Clippy.
From the repository root on Windows or Linux:

```text
cargo fmt --check
cargo clippy --locked -p eliot -- -D warnings
cargo run --locked -p eliot -- --version
cargo run --locked -p eliot -- --help
```

No model account, Codex install, Node, Python, database service or `just` is required for
this skeleton. CI runs the same checks on Windows and Ubuntu. Add dependencies and
modules only with their first connected caller.

## Implement next

Read [AGENTS.md](AGENTS.md), [architecture](docs/architecture.md), the current
[roadmap task](docs/roadmap.md) and its [donor entry](docs/reference/donors.md).
[All documentation](docs/README.md) describes the target contract; later sections are
not prerequisites for the first Codex path.

## v1 archive

The old implementation, documentation and third-party notices remain on branch
`archive/v1-final` at commit `dc6f70aa08ea5fad3da04624ce50c46d45edd57c`.
For a full clone:

```text
git show dc6f70aa:crates/swarm-process/src/process_group.rs
```

A shallow clone may first fetch `archive/v1-final`. Borrow only the narrow helpers in
the donor map, retaining applicable notices; do not port v1's workflow/proof machinery.
The new skeleton contains no copied vendor code or third-party dependencies.
