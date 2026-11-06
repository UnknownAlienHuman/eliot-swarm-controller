# Working on Eliot

Build the next useful path in [roadmap](docs/roadmap.md), not the entire final design.
Read [vision](docs/vision.md) and [architecture](docs/architecture.md) once, then only
that task's owning spec, touched code and donor entry. Later-layer sections are not
prerequisites. v1 is archived at `dc6f70aa` on `archive/v1-final`; do not restore its
runtime or tooling. Select the next unchecked increment in the roadmap.

## Work loop

1. One manager owns each coherent increment, its branch/worktree and pull request. One
   PR may cover several tightly coupled roadmap checkboxes. Never push directly to `main`.
   The manager integrates and merges only after scoped checks and independent review;
   delegate disjoint work and keep integration with that manager.
2. Select the next unchecked task. Read its donor entry: **dependency**, **extract** or
   **pattern**. Use existing code where it fits; do not import the donor's application.
3. Implement a connected caller-to-result path. Add only the schema, commands and
   modules used by that path. Unsupported later features return `unsupported`.
4. Run `cargo fmt --check`, `cargo build --locked -p <changed-package> --all-targets`, and
   `cargo clippy -p <changed-package> --all-targets -- -D warnings`. Repeat for other
   changed packages only. This works for library-only and binary-only packages; no
   unconditional `--lib`.
5. Runtime qualification uses actual installed vendor agents through Swarm on real tasks,
   not synthetic/fake execution, test harness loops, or test edits. CI enforces static
   validation only (formatting, warnings-denied Clippy, locked all-targets compilation);
   it does not execute tests, call models, or provide runtime proof. Do not build test
   frameworks or fake harnesses as detours. Record the exact binary hash, model, principal,
   outcomes, and owned cleanup in the PR.
6. Tick completed work and record the actual static checks and real runtime evidence in
   the PR. Roadmap checkboxes are the only project status list; do not add permanent
   test/readiness reports or a readiness engine. Do not claim a runtime property from
   compilation or source review alone.

## Keep the implementation small

- Core types/reducer contain no I/O. Store owns SQLite; plugins own vendor protocols.
  Automation and coordination call the router, never another harness executor.
- Use one serializable DTO per implemented wire message. Validate syntax at its owning
  boundary; retain live generation, authority and state checks at transaction/effect boundaries.
- Factor repeated current behavior. Specs own semantics and link code DTOs/constants
  instead of copying field catalogs.
- Preserve generation/turn/request binding, commit-before-ack and explicit unknown
  outcomes from the owning specs. Do not replay an uncertain external effect.
- Add a crate or trait for a real boundary, not a future caller or a line-count target.
  A simple internal module is enough for most later features.
- Replace obsolete code in place. No hidden fallback, compatibility executor, proof
  packets, audit/readiness documents, task-acceptance engine or generated status registry.
- Change an incorrect owning spec in the same PR; remove contradictory text rather
  than adding another exception. Roadmap checkboxes are the only project status list.
- Keep secrets out of persisted semantic data, fixtures and logs. Use explicit argv
  and scoped process ownership; Windows is a first-class platform.

## Donor use

[donors.md](docs/reference/donors.md) names source, target and extraction boundary.
Use the listed reference revision to understand the mechanism. At the first actual
code import/dependency addition, check that revision's license, retain required notices,
record the source in the code/PR, and verify toolchain compatibility. This is a one-time
import check, not a gate on every operation or PR. Pattern-only sources are not copied.

The complete API/spec is the target contract, not permission to build everything before
Codex works. First deliver core/bus with a tiny fake, then direct Codex/Luna; automation,
mail and additional harnesses come afterward in the order defined by the roadmap.
