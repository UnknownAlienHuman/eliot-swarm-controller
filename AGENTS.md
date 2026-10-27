# Working on Eliot

Read, in order: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the specifications/harness page linked by your task →
[donors](docs/reference/donors.md) for the exact mechanism being implemented.

Until L0.1 lands, current crates/modules/migrations/tests/tools/vendor/config/old
workflows are v1 legacy. Do not repair or extend them. Read only the exact donor path
named by v2 docs, copy the small mechanism and remove its old coupling.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegated assignments are narrow and disjoint; the manager reviews/integrates every
  returned diff.
- No two managers share an index/worktree.
- One roadmap task normally lands one connected useful path, not scaffolding.

## Implementation loop

1. Take the first unchecked current-layer task or the exact assigned task.
2. Read its linked contract and touched code.
3. Before inventing a mechanism, read the exact donor source named in
   `docs/reference/donors.md`. Use its proven boundary unless a concrete Eliot
   constraint makes it incompatible; state that incompatibility in the PR.
4. Write the real caller → Store/router → plugin/native → readback path first.
5. Run only the implementation gate:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add another package only when the changed contract must compile together. Do not
   pause partial layers to construct a broad unit/E2E/live/load campaign. A tiny local
   regression is allowed only when review cannot establish a critical contract without
   it. The complete deterministic, crash/restart, plugin conformance, live-harness,
   platform and load suite is built/run after the connected product exists.
6. Tick the roadmap task in the same PR.
7. PR description: **What**, **Checked**, **Spec changes**—short and factual.

Fix an incorrect owning spec in the same PR, replacing text in place. Correct unstable
harness facts from installed/official source with date. Do not add audits, readiness
reports, packets, inventories, progress documents or addenda.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  dead API or abstraction without a current caller.
- **Replace means delete.** The old route leaves in the PR that activates its
  replacement.
- **Stable Agent, one generation fence.** Every Agent command/fact carries the current
  generation and exact turn/request where relevant. Do not add binding revisions,
  epochs or another identity layer.
- **Unknown stays unknown.** Never replay a possible native/external effect—not start,
  input, interrupt, stop, setup, script or push. Exact readback or explicit owner
  resolution only. Its narrow lane remains blocked.
- **Start ≠ input ≠ turn result.** `session.ready`, input admission/association and
  terminal turn outcome are separate facts.
- **One source per lifecycle family.** A stream, transcript or hook owns each family for
  one generation. Handoff uses an opaque cursor or explicit gap. Never duplicate
  lifecycle through two sources.
- **Commit before acknowledgement.** Plugin facts, source cursor and reducer projection
  commit before success is returned or broadcast.
- **Explicit principal.** Owner CLI uses its private local token automatically; Agents
  and scripts use scoped tokens. Missing/mismatched identity rejects and never becomes
  owner.
- **Native first.** Use the harness's exact mechanism. Unsupported semantics reject;
  never silently turn steer into send, resume into fresh unrelated launch or
  force-stop into killing a shared service.
- **Actions are commands.** Do not introduce a second automation operation graph/run
  engine.
- **No secrets** in source, config values, Store semantic data, messages, fixtures or
  logs.
- **No model calls in CI.** Record native traffic for the later replay/conformance
  campaign.

## Code

- Rust 2024, pinned stable, workspace lints.
- `thiserror` in libraries, `anyhow` at binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states a true invariant.
- `unsafe` only in the documented platform boundary.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows first-class: assign-before-resume Job, explicit inherited handles, no child
  console, UTF-8 without BOM/LF, long input via stdin/private file, environment keys
  case-insensitive.
- SQLite ids fit signed 64-bit and exhaustion fails before mutation.
- Lists, histories, fanouts and discovery are bounded/paginated.
- Before adding a dependency, verify current version, license, maintenance and MSRV and
  state it in the PR.

## Final qualification

After L0–L6 produce the connected local product, build and run one complete campaign:

- reducer/Store/API and migration invariants;
- fake-harness E2E through the CLI;
- crash/restart and unknown-outcome boundaries;
- plugin recordings/conformance and compact live runs;
- coordination/automation simulated-time cases;
- Windows/degraded-Unix custody/security/recovery;
- retention/WAL/load.

Do not claim runtime correctness from documentation, Clippy or fixtures alone. During
implementation report only the connected behavior and compile gate actually completed.
