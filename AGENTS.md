# Working on Eliot

Eliot v2 is built from scratch in layers. Read, in this order:
[vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → the spec pages your task links to.

## The tree right now

Until roadmap task **L0.1** lands, `crates/`, `modules/`, `migrations/`, `src/`,
`tests/`, `tools/`, `vendor/`, `config/`, `Justfile` and the CI workflows are **v1
legacy code**. Do not change it, do not take requirements from it, and do not build
on it. Borrow from it only what [donors](docs/reference/donors.md) lists.

## How to do a task

1. Take the first unchecked task of the current layer in the
   [roadmap](docs/roadmap.md) (or the task you were given). One task = one branch =
   one PR.
2. Read only the spec sections it links to and the code it touches.
3. Implement it with tests. Run `just check` (fmt, clippy `-D warnings`, tests).
4. Tick the task's box in `docs/roadmap.md` in the same PR.
5. PR description, three short parts: **What** (behavior added), **Tested** (commands
   and what they showed), **Spec changes** (if any). CI prints line counts.

If the spec is wrong or silent, fix the owning spec page in the same PR with the
smallest edit and say so under *Spec changes*. If a vendor fact on a harness page is
wrong, correct it there with the source URL and date. If a real product decision is
needed, pick the simplest choice that fits the vision, record it in the
[defaults](docs/decisions.md#defaults) table, and keep going.

## Rules

- **Build what the task says, through the real path.** No shims, fallbacks,
  compatibility layers, placeholder code or code without a caller.
- **Replace means delete.** When new code replaces old code, the old code goes in the
  same PR.
- **No paperwork.** Do not add reports, audits, inventories, status files, plans or
  new docs pages. The roadmap checkbox and the tests are the status.
- **Stay small.** Respect the [code budgets](docs/roadmap.md#code-budgets). No new
  crate, trait or abstraction without two real users today.
- **Unknown stays unknown.** Never retry a command whose outcome is unknown (only
  `interrupt`/`stop` are retried once, see [core](docs/spec/core.md#commands)); never
  treat silence or a missing source as idle, zero or done.
- **Native first.** Use the harness's own mechanism. Where the plugin protocol allows an
  equivalent mechanism (reported in `detail.mechanism`, e.g. `inject` by prepending),
  use it; never substitute a weaker action silently — reject the verb instead.
- **No secrets** in code, config, events, messages, fixtures or logs.
- **No model calls in CI.** CI replays recordings. Real harness runs
  (`just record-*`, `just live-*`) are fine on a machine where the harness is
  installed and logged in; keep them short and cheap.

## Code

- Rust 2024, stable toolchain from `rust-toolchain.toml`, workspace lints.
- `thiserror` in libraries, `anyhow` in the binary, `tracing` for logs, `tokio` runtime.
- No `unwrap()` outside tests; `expect("why this cannot fail")` for real invariants.
- `unsafe` only in `eliot-daemon::platform` (Windows/Unix process and pipe calls).
- Windows is a first-class target: CI runs on Windows; UTF-8 without BOM; no console
  windows for child processes; long inputs via files, not argv.
- Before adding a dependency, check its current version, license and maintenance on
  docs.rs/crates.io, and name it in the PR.

## Tests

- `eliot-core`: table-driven unit tests (state reducer, matchers, selectors).
- `eliot-store`: temp-file databases, including reopen and migration.
- Everything else: end-to-end through the `eliot` binary with the fake plugin
  (`plugins/fake`) in a temp data dir.
- Harness plugins: recordings + `eliot plugin-test` in replay mode; live runs via
  `just live-<name>`.
