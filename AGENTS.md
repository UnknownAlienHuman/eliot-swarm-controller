# Working on Eliot

Read: [vision](docs/vision.md) → [architecture](docs/architecture.md) →
[roadmap](docs/roadmap.md) → only specs/harness page linked by your task.

Until L0.1 lands, current code/tools/workflows are v1 legacy. Do not extend or repair
them. Read only the exact donor path named in `docs/reference/donors.md`, copy the small
mechanism into v2 and remove old coupling.

## Ownership

- One manager owns one worktree, branch and PR.
- Delegation is narrow/disjoint; manager reviews and integrates every diff.
- No two managers share an index/worktree.
- One task lands one connected useful path, not placeholder framework/APIs.

## Implementation loop

1. Take first unchecked current-layer task or exact assignment.
2. Read linked contracts/touched code and current vendor docs/source for unstable facts.
3. Implement real caller → router → Store → plugin/native → readback/recovery path.
4. Run only:

   ```text
   cargo fmt --check
   cargo clippy -p <changed-package> --lib --bins -- -D warnings
   ```

   Add only packages whose changed contract must compile together. Broad tests/live
   qualification begin at feature-complete L7. A tiny regression is allowed only where
   source review cannot establish safety.
5. Tick roadmap task in same PR.
6. PR body: **What**, **Checked**, **Spec changes**—factual and short.

Repair an owning spec in the same PR; replace text in place. Correct harness facts with
installed/official source and date. Do not create audit/report/addendum documents.

## Runtime rules

- **Connected path, no substitutes.** No shim, hidden fallback, compatibility family,
  dead API or abstraction without current callers.
- **Replace means delete.** Old route leaves when new route activates.
- **External and internal identity differ.** Every external daemon mutation has a
  canonical ULID WriteId receipt. Linked input, selector child and automation command
  use fixed derived id and no fake external receipt. Retry uncertain client reply with
  the same WriteId.
- **Receipt retention is a minimum.** Do not delete a receipt while any retained command
  or domain row references it. Advance the ULID floor only through receipts actually
  deleted transactionally.
- **Exact scope before effect.** Carry expected/target generation and exact turn/request;
  stale input rejects before native I/O.
- **Session start ≠ initial input ≠ turn completion.** Launch/resume and first input are
  linked commands. Inbox/HTTP acceptance is not send applied; send applies only at
  exact native turn association; terminal result is separate.
- **One native subject, one Agent row.** Attach/detach changes control on canonical row.
  Native child registration includes exact scope/ref and never silently reparents.
- **Routing token follows control lifecycle.** Derive current raw token from private
  routing key plus `(agent,generation,token_version)`; Store only hash. Terminal/detach
  revokes without rewriting generation. Lost child/reattach reply returns same version,
  not a replacement token.
- **Unknown stays unknown.** Never retry possible effect—not start, input, interrupt,
  stop, workspace setup, script, push or webhook. Settle by exact readback or constrained
  owner assertion; do not synthesize ready/turn/completion.
- **Persisted plugin facts are opened, sequenced and acknowledged.** Plugin chooses
  stable `open_id`; daemon returns source key. Every source has contiguous sequence and
  at most one unacknowledged envelope. Advance cursor/spool only after Store commit
  reply. Lost reply resends identical sequence; watermark/digest survives event pruning.
- **One current source owner per family.** Opening/handoff/closing family ownership is
  transactional. An undeclared family is rejected. Vendor duplicate may advance source
  sequence but never fires lifecycle twice.
- **OS monitor owns process lifecycle.** A per-agent plugin may register only a verified
  descendant in its agent custody. Shared server PIDs belong to service custody, never
  an Agent. Agent stop never kills shared service.
- **No projection-miss owner escalation.** Pin peer birth and live-check every Job/
  cgroup/process group before no-token owner fallback. Ambiguity rejects.
- **Workspace is frozen before admission.** Resolve requested base ref read-only to one
  exact local commit OID; never fetch/re-resolve at dispatch. Journal worktree/setup and
  never blindly rerun unknown setup.
- **Selector fanout is bounded and atomic.** Sort concrete targets, reject over configured
  limit before rows, store count/digest and paginate children/results. Never return an
  unbounded child array or partially admit.
- **Restart is level-reconciled.** Sources, active/retiring rule activations, timers,
  goals, unread mail, workspaces/launch queues and provider holds recover from Store.
- **Rule digest is not activation identity.** Every install/reload creates fresh
  activation id, even A→B→A. Persist canonical compiled definition for active,
  retiring and replay activations until their intervals close.
- **Rule edits have one high-water boundary.** Old activation drains through H; new
  starts after H. Disable/pause records skipped range. Deterministic render/authority
  error creates one failed run/no actions and advances cursor; only Store failure retries.
- **Automation journal is generic.** Event rules, timers, manual runs and Goal rounds use
  `(kind, owner activation, trigger)` identity. Rate limits are durable across restart.
- **Automation authority is explicit.** Global rules are owner configuration; project
  rules stay in project. Managed script/setup/check tokens are read-only; effects return
  as stdout actions for daemon admission.
- **Environment is minimal and scoped.** Project effective profiles/extends/accounts/
  proxies/pass-env must all be allowlisted. Project script environment is global script
  allowlist ∩ project allowlist before any value lookup.
- **Secrets are semantic hazards.** Resolve named secrets only in memory, mask native
  output/spools, reject known secrets in durable semantic payloads, never persist raw
  routing key/token.
- **Custody claims only what OS proves.** Windows Job exact. Linux exact needs qualified
  cgroup/equivalent + monitored guardian. Process-group fallback is degraded and never
  claims exact force/complete descendant death.
- **No model calls in CI.** Recordings support L7 replay; live checks are compact and
  cheapest route.

## Code rules

- Rust 2024, pinned stable, workspace lints.
- `thiserror` libraries, `anyhow` binary boundary, `tracing`, Tokio async I/O.
- No production `unwrap`; `expect` states true invariant.
- `unsafe` only documented platform boundary.
- SQLite ids are signed-64; fixed derived ids bounded; exhaustion fails before mutation.
- Explicit executable/argv/cwd/env; no implicit shell.
- Windows: explicit handles, assign-before-resume Job, no child console, UTF-8 no BOM/
  LF, long input via stdin/file, case-insensitive env keys.
- Linux exact verifies guardian/cgroup; fallback reports degraded.
- Reads, fanouts and replays are paginated/deadlined and release SQLite transactions;
  WAL is monitored.
- Before dependency: verify current version/license/maintenance/MSRV and state in PR.

## Qualification

L7 runs full reducer/SQLite schema/API, crash/restart, plugin conformance/live,
coordination/automation, Windows/Linux custody/security, retention/WAL and load suite.
Do not claim runtime correctness from docs, Clippy or fixtures. During L0–L6, claim only
implemented behavior and scoped compile gate actually run.
