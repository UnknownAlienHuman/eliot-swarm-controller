# Working on Eliot

Read in order: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the specs/harness page linked by your task.

Until L0.1 lands, existing code/tools/workflows are v1 legacy. Do not extend or repair
them. Read only the exact donor path named in `docs/reference/donors.md`, copy the
small mechanism into v2, and remove v1 coupling.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegation is narrow/disjoint; the manager reviews/integrates every diff.
- No two managers share an index/worktree.
- One task normally lands one connected useful path. Do not split out placeholder
  crates/APIs/frameworks.

## Implementation loop

1. Take the first unchecked current-layer task or the exact assigned task.
2. Read linked contracts and touched code; read vendor docs/source before relying on an
   unstable protocol fact.
3. Implement the real caller→router→Store→plugin/native→readback path first.
4. Run only:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add only packages whose changed contract must compile together. Broad tests/live
   qualification begin at feature-complete L7. A tiny deterministic regression is
   allowed where source review cannot establish safety.
5. Tick the roadmap task in the same PR.
6. PR body: **What**, **Checked**, **Spec changes**—short and factual.

Repair a wrong owning spec in the same PR. Replace text in place; no addendum/report.
Correct harness facts with exact installed/official source and date.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  placeholder, dead API or abstraction without current callers.
- **Replace means delete.** Old path leaves when the new path becomes active.
- **Exact scope before effect.** Carry expected/target generation and exact turn/
  request. Stale input is rejected before native I/O.
- **Session start ≠ first prompt.** Launch/resume and optional initial input are linked
  commands with separate outcomes. `session.ready` never proves prompt admission.
- **One native subject, one Agent row.** Attach/detach changes control; observation
  subscribes to the same canonical row.
- **Unknown stays unknown.** Never retry an effect that may have applied—not start,
  input, interrupt, stop, script, push or webhook. Settle by exact readback or explicit
  owner resolution.
- **Restart is level-reconciled.** Durable event cursors, timers, goals, unread mail,
  launch queues and provider holds must recover from current Store state, not a lost
  callback/wakeup.
- **One lifecycle source.** Stream/transcript/hook have explicit family ownership.
  Duplicate/stale/wrong-turn native input cannot enter the normal automation bus.
- **Custody claims only what the OS proves.** Windows Job is exact. Linux exact mode
  requires qualified cgroup/equivalent + owner-death guardian. Process-group fallback
  is degraded and may not claim exact force/daemon-death cleanup.
- **Shared control requires real isolation.** Per-session environment/tool attribution,
  native subject, attach and isolated stop are mandatory. Otherwise use per-agent mode
  or observe-only.
- **Minimal environment by default.** Pass only named variables. Never inherit/leak
  unrelated credentials merely because the daemon has them.
- **Secrets are semantic hazards.** Store token hashes, resolve named secrets only at
  execution, mask native output, reject known secrets in durable command/mail/var/rule
  payloads rather than persisting altered text.
- **No paperwork.** No audit/readiness/packet/inventory/progress docs. Roadmap and
  executable results are status.
- **No model calls in CI.** Recordings support L7 replay; live checks are compact and
  cheapest-route.

## Code rules

- Rust 2024, pinned stable, workspace lints.
- `thiserror` in libraries, `anyhow` at binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states a true invariant.
- `unsafe` only in the documented platform boundary.
- IDs persisted in SQLite fit signed 64-bit; fixed derived ids are bounded.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows: explicit inherited handles, assign-before-resume Job, no child console,
  UTF-8 no BOM/LF, long input by stdin/file, case-insensitive env keys.
- Reads/replays are paginated and release SQLite transactions; WAL is monitored.
- Before dependency: verify current version/license/maintenance/MSRV and name it in PR.

## Qualification

L7 builds/runs the full reducer/Store/API, crash/restart, plugin conformance/live,
coordination/automation, Windows/Linux custody/security, retention/WAL and load suite.
Do not claim runtime correctness from source review, Clippy or fixtures. During L0–L6,
claim only implemented behavior and the scoped compile gate actually run.
