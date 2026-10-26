# Working on Eliot

Read in order: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the specs/harness page linked by your task.

Until L0.1 lands, existing code/tools/workflows are v1 legacy. Do not extend or repair
them. Read only the exact donor path named in `docs/reference/donors.md`, copy the
small mechanism into v2, and remove v1 coupling.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegation is narrow/disjoint; manager reviews/integrates every diff.
- No two managers share an index/worktree.
- One task lands one connected useful path, not placeholder crates/APIs/frameworks.

## Implementation loop

1. Take the first unchecked current-layer task or exact assignment.
2. Read linked contracts/touched code; read vendor docs/source before relying on an
   unstable protocol fact.
3. Implement the real caller→router→Store→plugin/native→readback/recovery path first.
4. Run only:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add only packages whose changed contract must compile together. Broad tests/live
   qualification begin at feature-complete L7. Tiny deterministic regression is
   allowed where source review cannot establish safety.
5. Tick roadmap task in the same PR.
6. PR body: **What**, **Checked**, **Spec changes**—short and factual.

Repair a wrong owning spec in the same PR. Replace text in place; no addendum/report.
Correct harness facts with exact installed/official source and date.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  placeholder, dead API or abstraction without current callers.
- **Replace means delete.** Old path leaves when new path becomes active.
- **Exact scope before effect.** Carry expected/target generation and exact turn/
  request. Stale input rejects before native I/O.
- **Session start ≠ first prompt.** Launch/resume and optional initial input are linked
  commands with separate outcomes. Ready never proves prompt admission.
- **One native subject, one Agent row.** Attach/detach changes control; observation
  subscribes to the canonical row.
- **Unknown stays unknown.** Never retry an effect that may have applied—not start,
  input, interrupt, stop, setup, script, push or webhook. Settle by exact readback or
  explicit owner resolution.
- **Persisted plugin facts are acknowledged.** Advance native cursor/drop local spool
  only after Store commit reply. Lost reply resends same native event id; Store dedupes.
- **Restart is level-reconciled.** Event cursors, timers, goals, unread mail,
  workspace/launch queues and provider holds recover from Store state, not a lost wake.
- **One lifecycle source.** Stream/transcript/hook have explicit family ownership.
  Duplicate/stale/wrong-turn native input cannot enter normal automation.
- **Event replay is deterministic.** Match stored event-time subject, never a later
  mutable Agent row.
- **Automation authority is explicit.** Global rules are owner-configured; project
  rules/scripts stay in their registered project and receive scoped tokens.
- **Custody claims only what OS proves.** Windows Job is exact. Linux exact needs a
  qualified cgroup/equivalent + monitored owner-death guardian. Process-group fallback
  is degraded and cannot claim exact force/daemon-death cleanup.
- **Shared control requires real isolation.** Per-session environment/tool attribution,
  native identity, attach/readback and isolated stop are mandatory. Otherwise use
  per-agent mode or observe-only.
- **Minimal environment by default.** Pass named variables only; do not leak unrelated
  credentials inherited by daemon.
- **Secrets are semantic hazards.** Store token hashes, resolve named secrets at
  execution, pattern/value-mask native output, reject known secrets in durable
  command/mail/var/rule payloads rather than persisting altered text.
- **No paperwork.** No audit/readiness/packet/inventory/progress docs. Roadmap and
  executable results are status.
- **No model calls in CI.** Recordings support L7 replay; live checks are compact and
  cheapest-route.

## Code rules

- Rust 2024, pinned stable, workspace lints.
- `thiserror` in libraries, `anyhow` at binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states a true invariant.
- `unsafe` only in documented platform boundary.
- SQLite ids fit signed 64-bit; derived ids are fixed/bounded.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows: explicit handles, assign-before-resume Job, no child console, UTF-8 no
  BOM/LF, long input via stdin/file, case-insensitive env keys.
- Linux exact custody code must verify guardian/cgroup health; fallback reports
  degraded rather than weakening the contract.
- Reads/replays are paginated/deadlined and release SQLite transactions; WAL is
  monitored.
- Before dependency: verify current version/license/maintenance/MSRV and name it in PR.

## Qualification

L7 builds/runs full reducer/Store/API, crash/restart, plugin conformance/live,
coordination/automation, Windows/Linux custody/security, retention/WAL and load suite.
Do not claim runtime correctness from source review, Clippy or fixtures. During L0–L6,
claim only implemented behavior and scoped compile gate actually run.
