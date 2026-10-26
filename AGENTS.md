# Working on Eliot

Read in order: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the specs/harness page linked by your task.

Until L0.1 lands, existing code/tools/workflows are v1 legacy. Do not extend or repair
them. Read only the exact donor path named in `docs/reference/donors.md`, copy the
small mechanism into v2, and remove v1 coupling.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegation is narrow/disjoint; the manager reviews and integrates every diff.
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
   qualification begin at feature-complete L7. A tiny deterministic regression is
   allowed where source review cannot establish safety.
5. Tick the roadmap task in the same PR.
6. PR body: **What**, **Checked**, **Spec changes**—short and factual.

Repair a wrong owning spec in the same PR. Replace text in place; no addendum/report.
Correct harness facts with exact installed/official source and date.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  placeholder, dead API or abstraction without current callers.
- **Replace means delete.** The old path leaves when the new path becomes active.
- **Exact scope before effect.** Carry expected/target generation and exact turn/
  request. Stale input rejects before native I/O.
- **Session start ≠ first prompt.** Launch/resume and optional initial input are linked
  commands with separate outcomes. Ready never proves prompt admission.
- **One native subject, one Agent row.** Attach/detach changes control; observation
  subscribes to the canonical row.
- **Token follows routing lifecycle.** A controlled live generation has one hash;
  generation 0, terminal and observe-only/detached rows have none. Revoke without
  resetting generation; resume creates a new token.
- **Unknown stays unknown.** Never retry an effect that may have applied—not start,
  input, interrupt, stop, setup, script, push or webhook. Settle by exact readback or
  explicit owner resolution.
- **Persisted plugin facts are sequenced and acknowledged.** Each source ownership epoch
  has one stable key, contiguous source sequence and at most one unacknowledged event.
  Advance native cursor/drop spool only after Store commit reply. Lost reply resends the
  same sequence/envelope; Store watermark/digest survives event retention. Never build
  one permanent receipt row per event.
- **No projection-miss owner escalation.** Before no-token owner fallback, pin the peer
  process and live-check every current Job/cgroup/process-group custody. Ambiguous
  membership rejects; a missing `processes` row proves nothing.
- **Restart is level-reconciled.** Event cursors, timers, goals, unread mail,
  workspace/launch queues and provider holds recover from Store state, not a lost wake.
- **One lifecycle source.** Stream/transcript/hook have explicit family ownership.
  Duplicate/stale/wrong-turn native input cannot enter normal automation.
- **Event replay is deterministic.** Match the stored event-time subject, never a later
  mutable Agent row.
- **Rule edits have a boundary.** For enabled edit, capture H, drain old revision only
  through H and start the new revision after H. Disable/pause explicitly records and
  skips through H; do not silently lose the old backlog.
- **A bad event cannot wedge a rule.** Missing placeholder, invalid action, authority
  violation or secret-bearing render creates one durable failed firing with no actions
  and advances the cursor. Store failure alone retries the same event.
- **Automation authority is explicit.** Global rules are owner-configured; project
  rules/scripts stay in their registered project and receive scoped tokens.
- **Project scripts cannot borrow another project's environment.** Resolve a named
  environment value only after it passes both the global script allowlist and the
  registered project's allowlist. Validate before process start.
- **Custody claims only what the OS proves.** Windows Job is exact. Linux exact needs a
  qualified cgroup/equivalent plus monitored owner-death guardian. Process-group
  fallback is degraded and cannot claim exact force/daemon-death cleanup.
- **Shared control requires real isolation.** Per-session environment/tool attribution,
  native identity, attach/readback and isolated stop are mandatory. Otherwise use
  per-agent mode or observe-only.
- **Minimal environment by default.** Pass named variables only; do not leak unrelated
  credentials inherited by the daemon.
- **Secrets are semantic hazards.** Store token hashes, resolve named secrets at
  execution, pattern/value-mask native output, and reject known secrets in durable
  command/mail/var/rule payloads rather than persisting altered text.
- **No paperwork.** No audit/readiness/packet/inventory/progress docs. Roadmap and
  executable results are status.
- **No model calls in CI.** Recordings support L7 replay; live checks are compact and
  cheapest-route.

## Code rules

- Rust 2024, pinned stable, workspace lints.
- `thiserror` in libraries, `anyhow` at binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states a true invariant.
- `unsafe` only in the documented platform boundary.
- SQLite ids fit signed 64-bit; derived ids are fixed and bounded; exhaustion fails
  before mutation.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows: explicit handles, assign-before-resume Job, no child console, UTF-8 without
  BOM/LF, long input via stdin/file, case-insensitive environment keys.
- Linux exact custody code verifies guardian/cgroup health; fallback reports degraded
  rather than weakening the contract.
- Reads/replays are paginated/deadlined and release SQLite transactions; WAL is
  monitored.
- Before adding a dependency, verify current version/license/maintenance/MSRV and name
  it in the PR.

## Qualification

L7 builds and runs the full reducer/Store/API, crash/restart, plugin conformance/live,
coordination/automation, Windows/Linux custody/security, retention/WAL and load suite.
Do not claim runtime correctness from source review, Clippy or fixtures. During L0–L6,
claim only implemented behavior and the scoped compile gate actually run.
