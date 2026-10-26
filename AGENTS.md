# Working on Eliot

Read: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only the specs/harness page linked by your task.

Until L0.1 lands, the current implementation, migrations, vendor tree and old workflows
are v1 legacy. Do not extend or repair them. Read only the donor path named in
`docs/reference/donors.md`, copy the small mechanism into v2 and remove old coupling.
The following are already-v2 tooling and **must not be deleted** by L0.1:

- `.github/workflows/v2-docs.yml`;
- `tools/ci/validate-v2-docs.py`;
- `tools/ci/validate-v2-schema.py`.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegation is narrow/disjoint; manager reviews and integrates every diff.
- No two managers share an index/worktree.
- One task lands one connected useful path, not placeholder framework/APIs.

## Implementation loop

1. Take the first unchecked current-layer task or exact assignment.
2. Read linked contracts/touched code and current vendor docs/source for unstable facts.
3. Implement the real caller → router → Store → plugin/native → readback/recovery path.
4. Run only:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add only packages whose changed contract must compile together. Broad tests/live
   qualification begin at feature-complete L7. A tiny regression is allowed only where
   source review cannot establish safety.
5. Tick the roadmap task in the same PR.
6. PR body: **What**, **Checked**, **Spec changes**—short and factual.

Repair an owning spec in the same PR; replace text in place. Correct harness facts with
installed/official source and date. Do not create audit/report/addendum documents.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  dead API or abstraction without current callers.
- **Replace means delete.** Old route leaves when the new route activates.
- **Every connected caller has an explicit principal credential.** Owner CLI uses
  `--owner` to read the private owner token; agents and automation receive scoped
  tokens. Missing/incomplete/revoked credentials reject. Never infer owner authority
  from absence of a token, PID lookup or process-table miss.
- **External and internal identity differ.** Every external daemon mutation has a
  canonical ULID WriteId receipt. Linked input, selector child and automation command
  use a fixed derived id and no fake external receipt. Retry an uncertain client reply
  with the same WriteId.
- **Receipt retention is a minimum.** Do not delete a receipt while any retained domain
  row references it. Advance the ULID floor only through receipts actually deleted in
  the same transaction.
- **Exact scope before effect.** Carry expected/target generation and exact turn/request;
  stale input rejects before native I/O.
- **Session start ≠ initial input ≠ turn completion.** Open authoritative source epochs
  before the first native start/input effect. Launch/resume and first input are linked
  commands. Inbox/HTTP acceptance is not send applied; send applies only at exact native
  turn association; terminal result is separate.
- **One native subject, one Agent row.** Attach/detach changes control on the canonical
  row. Observe-only attach uses a synthetic observe profile; control attach uses a
  frozen allowed profile, exact environment/readback and capacity reservation.
- **Native control and direct routing are different.** A native child may be controllable
  through its parent plugin while `routing_enabled=false`. Store a routing-token hash
  only when that Agent can actually receive its own token/environment.
- **Unknown stays unknown.** Never retry a possible effect—not start, input, interrupt,
  stop, workspace setup, script, push or webhook. Settle by exact readback or constrained
  owner assertion; do not synthesize ready/turn/completion.
- **Persisted plugin facts are opened, sequenced and acknowledged.** Plugin chooses a
  stable `open_id`; daemon returns source key. Every source has contiguous sequence and
  at most one unacknowledged envelope. Advance cursor/spool only after Store commit
  reply. Lost reply resends the identical sequence.
- **Vendor replay survives event pruning.** Every persisted lifecycle fact has a stable
  native event id. Store keeps a compact native-event receipt until the generation and
  its replay sources are safely closed; deleting the event row must not permit a second
  lifecycle firing.
- **One current source owner per family.** Initial open and source replacement are
  transactional. Replacement names the predecessor and exact cursor/gap boundary.
  Undeclared families reject; vendor duplicate may advance source sequence but never
  fires lifecycle twice.
- **OS monitor owns process lifecycle.** A per-agent plugin may register only a verified
  descendant in that Agent custody. Shared server PIDs belong to service custody, never
  an Agent. Agent stop never kills a shared service.
- **Workspace is frozen before admission.** Resolve requested base ref read-only to one
  exact local commit OID; never fetch/re-resolve at dispatch. Journal worktree/setup and
  never blindly rerun unknown setup.
- **Selector fanout and discovery are bounded/paginated.** Sort concrete targets, reject
  over limit before rows, store count/digest and paginate children/results. Plugin
  `discover` and snapshots are paged so one reply cannot exceed framing limits.
- **Restart is level-reconciled.** Sources, active/retiring rule activations, timers,
  goals, unread mail, workspaces/launch queues and provider holds recover from Store.
- **Rule digest is not activation identity.** Every semantic install/reload creates a
  fresh activation id, even A→B→A. Persist compiled definition and immutable artifacts
  until active/retiring/replay intervals close.
- **Rule edits have one high-water boundary.** Old activation drains through H; new
  starts after H. Disable/pause records skipped range. Deterministic render/authority
  error creates one failed run/no actions and advances cursor; only Store failure retries.
- **Goal rounds have a durable eligibility key.** `on_idle` uses the latest committed
  eligibility sequence; cadence uses the exact schedule slot. The same state cannot
  create duplicate continuation after restart, while a real eligibility change can.
- **Automation authority is explicit.** Global rules are owner configuration; project
  rules stay in project. Managed script/setup/check tokens are read-only; effects return
  as stdout actions for daemon admission.
- **Project profiles only narrow a global profile.** Project overlays cannot replace
  harness/plugin/account/proxy/argv/inherit policy or broaden permissions/control/env.
  Owner creates new global route variants explicitly.
- **Environment is minimal and scoped.** Project effective profiles and pass-env names
  are allowlisted. Project script environment is global script allowlist ∩ project
  allowlist before any value lookup.
- **Secrets are semantic hazards.** Resolve named secrets only in memory, mask native
  output/spools, reject known secrets in durable semantic payloads, never persist raw
  owner/routing token or key.
- **Protocol payloads have explicit bounds.** Large immutable notes/scripts are passed by
  artifact reference, not inline JSON. Persisted event/final text and discovery pages
  have fixed limits below the plugin frame limit.
- **Native child guards never erase reality.** Eliot launch/depth/fanout limits block new
  controller-created work. An already-created native child is still recorded; when a
  guard is exceeded it gets no routing token/project authority or automatic wake.
- **Custody claims only what the OS proves.** Windows Job exact. Linux exact needs a
  qualified cgroup/equivalent + monitored guardian. Process-group fallback is degraded
  and never claims exact force/complete descendant death.
- **No model calls in CI.** Recordings support L7 replay; live checks are compact and use
  the cheapest route.

## Code rules

- Rust 2024, pinned stable, workspace lints.
- `thiserror` in libraries, `anyhow` at binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states a true invariant.
- `unsafe` only in the documented platform boundary.
- SQLite ids are signed-64; fixed derived ids are bounded; exhaustion fails before
  mutation.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows: explicit handles, assign-before-resume Job, no child console, UTF-8 no BOM/
  LF, long input via stdin/file, case-insensitive env keys.
- Linux exact verifies guardian/cgroup; fallback reports degraded.
- Reads, fanouts, discovery and replay are paginated/deadlined and release SQLite
  transactions; WAL is monitored.
- Before adding a dependency, verify current version/license/maintenance/MSRV and state
  it in the PR.

## Qualification

L7 runs the full reducer/SQLite schema/API, crash/restart, plugin conformance/live,
coordination/automation, Windows/Linux custody/security, retention/WAL and load suite.
Do not claim runtime correctness from docs, Clippy or fixtures. During L0–L6, claim only
implemented behavior and the scoped compile gate actually run.
