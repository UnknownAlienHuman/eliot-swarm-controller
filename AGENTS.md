# Working on Eliot

Eliot v2 is built from scratch in vertical layers. Read, in order:
[vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the spec pages linked by your task.

## The tree right now

Until **L0.1** lands, `crates/`, `modules/`, `migrations/`, `src/`, `tests/`, `tools/`,
`vendor/`, `config/`, `Justfile` and current CI workflows are v1 legacy. Do not modify
or extend them. Borrow only the exact mechanisms named in
[donors](docs/reference/donors.md); copy the small part into v2 and remove its old
coupling.

## One manager, one worktree

- One manager owns one worktree, branch and PR at a time.
- The manager may delegate narrow, disjoint reading or coding assignments, but reviews
  every returned diff and integrates it in that one worktree.
- No two managers share a Git index/worktree. Do not create ceremony, queues or task
  packets around the rule; it is simply the ownership boundary.
- One roadmap task normally produces one PR. Split only when the first PR is already a
  useful connected vertical path, not a placeholder framework.

## How to implement a task

1. Take the first unchecked task of the current layer, or the exact task assigned.
2. Read its linked specs and the code it changes. Read donor code only for the named
   mechanism; do not refactor v1.
3. Write the real path first. No test framework, compatibility shell or abstraction
   before its caller exists.
4. Run only:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add a second package only when the changed contract requires it to compile
   together. Broad tests and live qualification start after feature-complete L6, in
   L7. A tiny deterministic regression test is allowed when review cannot establish
   safety without it; do not expand it into a campaign.
5. Tick the task in `docs/roadmap.md` in the same PR.
6. PR body has three compact sections: **What**, **Checked** (fmt/scoped Clippy and any
   essential manual proof), **Spec changes**.

When a spec is wrong or incomplete, repair the owning page in the same PR with the
smallest complete change. When an installed harness contradicts its page, update the
page with the official/live source and date. A genuinely new product decision goes in
`docs/decisions.md`; do not create an addendum or report.

## Non-negotiable rules

- **Build the connected behavior.** No shim, fallback, compatibility family,
  placeholder, dead path or API without a caller.
- **Replace means delete.** The old route leaves in the same PR that activates the new
  one.
- **No paperwork.** Do not add audit, readiness, packet, inventory, progress or status
  documents. Roadmap checkboxes and executable behavior are the status.
- **Stay inside budgets.** No crate, trait, queue or service without two real consumers
  today. Prefer a direct function and explicit data.
- **Exact scope before effect.** Agent commands carry current generation; steer/
  interrupt carry exact turn; approval/answer carry exact request. Stale input is
  rejected before native I/O.
- **Unknown stays unknown.** Never retry any command or external action whose outcome
  may have applied—not even `interrupt` or `stop`. Use exact readback or explicit owner
  resolution. Silence, timeout, missing source and list absence are not idle, zero or
  complete.
- **One lifecycle source.** A live stream, transcript or hook owns each normalized
  event family at a time. Do not emit duplicate turn/session completion from two
  sources; an unbridgeable handoff is a visible gap.
- **Native first, semantics intact.** Use the harness mechanism that implements the
  requested verb and report it. Do not silently turn steer into send, resume into
  restart, answer into a default choice, or force-stop into killing a shared service.
- **No secrets** in source, config values, Store, events, messages, fixtures or logs.
- **No model calls in CI.** Recordings support later replay. Live qualification is L7
  and uses the cheapest route that proves the behavior.

## Code

- Rust 2024; stable pinned toolchain; workspace lints.
- `thiserror` in libraries, `anyhow` at the binary boundary, `tracing` for logs,
  `tokio` for async I/O.
- No `unwrap()` in production. `expect("why this cannot fail")` only for a real
  invariant.
- `unsafe` only in the documented platform boundary for process/pipe OS calls.
- Windows is first-class: no console windows for children, explicit inherited handles,
  UTF-8 without BOM, long input through stdin/files, case-insensitive environment-key
  merge.
- Resolve executable + argv separately; never insert an implicit shell.
- Before adding a dependency, verify current version, license, maintenance and MSRV;
  state it in the PR.

## Qualification after feature completion

L7 builds and runs the complete suite once the product path exists:

- pure reducer/Store cases;
- end-to-end fake harness through the CLI;
- crash/restart, generation and unknown-outcome cases;
- plugin recordings/conformance and short live runs;
- coordination/automation simulated-time cases;
- Windows/Linux process, security and recovery cases;
- load and retention.

Do not claim a runtime property from source review, Clippy or a fixture alone. During
L0–L6, report only what the implementation and scoped compile gate actually establish.
