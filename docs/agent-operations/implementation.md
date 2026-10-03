# Implementation Plan — Agent Operations

Revision 1 · 2026-10-03 · baseline `35e499ae73b622d873c44873f6993ee3fcbea87b`.

Read [README](README.md), then the relevant section of [architecture.md](architecture.md) and its [donor-map.md](donor-map.md) source. This plan contains proposed contracts and acceptance scenarios, not completed work.

## 1. Coordination with PR #22

[PR #22](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/22) currently proposes the deferred catalog, Participant role, peer communication, watches, dashboard and launcher. The reviewed head is `7204ea096ddecfde5d83661127735ddad2903441`; those proposed files are not treated as implemented main code.

Ownership split:

| Contract | Owner |
|---|---|
| Peer messages/cards/integration cells/Concilium | #22 communication work |
| MCP hard-profile/surface/catalog separation and launcher context | #22 catalog/launcher work |
| Continuous source collectors/live streams | This program |
| Server event rules, cron/action admission, Goal and scripts | This program |
| Custom role/grant expansion | One shared authorization implementation, coordinated with #22 Participant work |
| Notifications/reminders | One shared #22 watch/mailbox service, extended here; never a second service |
| Task/Attempt/Operation/artifacts/checks/publication | Existing core, extended only through its normal boundaries |

If #22 lands first, integrate its actual public names and schema. If it has not landed, implement the common prerequisite once and record ownership in the Issue. Do not add parallel Participant enums, role registries, watch stores, dashboard methods or launch state machines. This documentation PR is independently based on main; it does not merge or modify #22.

## 2. Implementation cadence

One manager, one worktree/candidate per Issue. Writers receive non-overlapping files and do not run Cargo. Manager integrates and reviews code, then runs scoped formatting and the repository's minimal warnings-denied Clippy on the finished Issue candidate. No broad live models, installation, service changes or global configuration changes in ordinary implementation slices.

The scenarios below are behavior requirements to implement. Integrated tests, load tests and owner-machine live qualification occur after the complete product path is wired, unless the owner explicitly advances a named check. Do not turn every callback into another test campaign or weaken a contract merely to avoid testing it later.

Every delivery updates current documentation and capability status. `source_reviewed` is not `implemented`; `compiled` is not `live_qualified`.

## 3. Dependency sequence

```text
O1 shared authority/action contract
  -> O2 event intake and current projections
       -> O3 monitoring + native live streams
       -> O4 GitHub/Git intake + work-pool mapping
       -> O5 native hooks setup/intake
  -> O6 script artifact/runner + native action registry
       -> O7 event rules + commit-to-auditor vertical path
       -> O8 existing scheduler upgrade: cron and dynamic actions
            -> O9 server Goal and shared reminders
  -> O10 deferred MCP/CLI/dashboard integration
       -> O11 complete qualification and installation handoff
```

O3/O4/O5 may proceed in parallel after O2 only with named file ownership. O6 can proceed after O1 with agreed O2 event types. Keep all external I/O outside Store transactions. Do not fragment one end-to-end action into orphaned unused modules; every slice names its producer, carrier, consumer and result reader.

## 4. O1 — Authorization and action admission

### Sources to inspect

Existing `src/model.rs`, `src/policy.rs`, `src/store/mod.rs`, `src/mcp/profiles.rs`, `src/store/operations.rs`, `src/store/producers.rs` and the current Participant/launcher implementation if merged.

### Planned units

```text
src/automation/mod.rs             new public internal types
src/automation/actions.rs         new closed action descriptors
src/store/automation.rs           new admission/linkage
src/authorization.rs              shared role/grant evaluator, if not already supplied by #22
```

Names are proposed source ownership, not claims those files exist now.

### Work

- Enumerate current method-specific authority before changing roles. Replace permissive negative checks such as "not observer" with explicit capability checks where custom roles reach the route.
- Define versioned finite role presets and scoped grants. Retain legacy operator/manager/observer/module behavior until explicitly migrated. Module credentials do not gain ordinary user APIs.
- Pin author, activator and execution principal; bound delegation to the grantor's delegable scope. GM designation/epoch remains a separate exact check.
- Implement one action admission entry point used by manual, schedule, event and Goal causes. Delegate to existing operation handlers; never recursively call public IPC while holding the same Store transaction.
- An invocation is an existing Operation. Metadata links cause, definition revision, grant revision and target; there is no parallel authoritative job status.
- Return an explicit unsupported action instead of dispatching an arbitrary method string.

### Required records

```text
cause: manual request | native/source event | schedule occurrence | Goal transition
source_event_key / schedule_due_utc / goal_decision_revision
rule_or_definition_id + immutable revision + action_slot
original caller/sponsor and current execution grant
resolved target identities and expected revisions
operation_id + input digest + completion/replay contract
parent/root cause + bounded ancestry
```

Uniqueness is per semantic cause, definition revision and action slot. Repeated delivery returns the same receipt. Different payload under the same identity conflicts. Grant/target revalidation occurs immediately before an external effect, not only at draft creation.

### Done when

A custom auditor can read its assigned candidate and run its allowed diagnostic action but cannot edit source, start writers, define stronger roles or accept work. A revoked grant prevents a queued start; an already uncertain effect is read back, not retried.

## 5. O2 — Event intake and projection infrastructure

### Existing anchors

`src/store/projection.rs`, `src/store/capacity.rs`, `src/mcp/subscriptions.rs`, existing Observation insertion/deduplication and Store change signals.

### Planned units

```text
src/monitoring/events.rs
src/monitoring/projection.rs
src/monitoring/pump.rs
src/store/monitoring.rs
```

### Event envelope

```json
{
  "contract": "eliot-observed-event-v1",
  "source_kind": "native_adapter",
  "source_id": "registered-source",
  "source_epoch": "connection-or-native-epoch",
  "source_event_id": "native-id-or-null",
  "source_cursor": null,
  "received_at_ms": 1780000000000,
  "native_time_ms": null,
  "scope": {
    "project_id": "project-id",
    "attempt_id": null,
    "binding_id": null,
    "binding_generation": null,
    "native_session_id": null,
    "native_turn_id": null
  },
  "kind": "tool.completed",
  "evidence_class": "native_observation",
  "payload_ref": null,
  "coverage": "partial",
  "gaps": []
}
```

Illustrative nullable fields are selected by the actual event schema. IDs come from verified source/Store mappings, not arbitrary request claims. Native cursors stay opaque unless their own contract defines ordering.

### Work

- Distinguish durable control/evidence facts from volatile live deltas. Reject untrusted sources claiming controller acceptance or forge receipts.
- Assign a controller sequence for committed events; preserve source cursor and native identity separately.
- Reuse one canonical event/Operation result and small references. Do not persist the same large output per recipient or dashboard subscriber.
- Share source pumps and coalesced revision signals. Replace per-viewer high-frequency scans incrementally while preserving existing subscriptions and lag/resync compatibility.
- Build scoped snapshot + cursor + changes without the snapshot/subscribe race. Authorization applies before selection and on every page, not after a global dump.
- Bound live queues, CPU parsing and serialized output; slow readers lose presentation continuity with an explicit gap, not control facts.

### Done when

A source gap, malformed event or slow viewer cannot block native permission replies or unrelated participants. Reconnect produces a bounded authoritative resync without a model call.

## 6. O3 — Passive fleet monitoring and streams

### Existing anchors

`src/runtime/owner.rs`, `src/platform/process_group.rs`, `src/runtime/warm_stream.rs`, OpenCode execution readers, Muse/Claude/Command stream mappings, and `src/doctor.rs`.

### Work

- Add one selective metrics sampler, preserving exact recorded process identity. Evaluate sysinfo 0.37.2 under the current MSRV; do not choose an incompatible latest dependency or change MSRV silently.
- Feed existing native transport events into the host projector without adding competing session readers/controllers.
- Make runtime capability maps explicit for each content/event type. Implement at least two actual native stream producers before generalizing a RuntimePort extension.
- Expose scoped text/tool/provider-exposed reasoning streams with part identity, redaction, byte budgets and retention. Do not expose hidden/encrypted reasoning.
- Render dashboard as structured MCP/CLI data first, plus a compact live terminal view. A browser UI may later consume the same snapshot/delta contract, not a new store.
- Track process, transport, tool, model, child-family and Task progress separately. Include GitHub/queue/Goal/rule rows only when those sources are ready.
- Instrument observer counts, event lag, dropped presentation bytes, DB time, outstanding native requests and process handles.

### Done when

With all manager models idle, source events still update the dashboard. No monitor action prompts an agent, starts a provider CLI healthcheck or kills a supposedly stale native session. Unsupported reasoning stream reports unavailable.

## 7. O4 — GitHub/Git source synchronization

### Planned units

```text
src/github/observer.rs
src/github/webhook.rs
src/github/work_pool.rs
src/git_inspect.rs                 share with #22 if present
src/store/github.rs
```

### Work

- Add local setup mapping of project to canonical GitHub repository ID and credential reference. Keep domains/installation IDs/secrets outside Git and tool results.
- Use existing reqwest/network boundary. Signed webhook intake records raw-body digest, delivery identity and validated minimal payload before ACK.
- Reconcile missed/stale pages with ETags/conditional reads and GitHub rate-limit headers. No reliance on webhook ordering; no all-repository polling by every viewer.
- Import/update selected source-indexed Issue/PR facts through manager preview/apply; never change a running Task snapshot just because a label/comment changed.
- Reuse bounded Git inspection and one notify watcher per relevant repository scope. Verify OID/ref/worktree after hooks; identify untracked/overlap separately.
- Emit trusted `git.commit_observed`, `github.pr_head_observed`, `github.review_observed` facts and their coverage. Commit ownership derives from recorded work context, not author strings.

### Done when

Duplicate local/remote reports of one commit do not create duplicate audit execution. A missing webhook or permission-limited GitHub response yields stale/partial data, not an empty project or a falsely completed Task.

## 8. O5 — Native hook support and setup

### Existing adapters first

Extend `modules/command/mod/eliot-command.ts`; wire supported hooks in the installed Claude/Codex/Muse/OpenCode integration without replacing SDKs. Other modules declare supported hooks or gaps.

### Work

- Implement per-event capability matrix: native version, input/output schema, phase, veto ability, async/lifetime and install/readback status.
- Provide small native-compatible JSON ingress wrappers. Use existing authenticated local IPC with narrowly scoped event rights; no generic module permission expansion.
- Install via preview/apply that preserves existing user/managed hook configuration, checks current hash/revision and reports required safe restart separately.
- Observational callbacks enqueue minimal facts and exit quickly. Never launch audits or poll a model inline. Explicit pre-effect gates use local bounded policy only.
- Native async callbacks cannot claim prevention; output after failure cannot undo a tool effect.
- Mark unsupported native coverage clearly. ELIOT wrapper events cover only calls it actually owns.
- Preserve forge's controlled Git hook suppression. ELIOT before/after action events are its supported extension points.

### Done when

A callback storm cannot block compaction or hang the tool pipeline. One malformed callback is isolated. Installed capabilities are read back on the selected runtime, not asserted from a manifest alone. No host settings/service change occurs merely because a repository file requests it.

## 9. O6 — Script registry and owned runner

### Existing anchors

`src/artifacts.rs`, `src/checks/worker.rs`, `src/checks/source.rs`, `src/forge/windows.rs`, `src/platform/process_group.rs` and atlas-redact. Extract a shared runner only where behavior truly matches; do not force CheckRunner verification semantics onto arbitrary scripts or regress forge containment.

### Planned units

```text
src/scripts/manifest.rs
src/scripts/registry.rs
src/scripts/runner.rs
src/scripts/protocol.rs
src/store/scripts.rs
```

### Work

- Register immutable script bundles and schemas under author scope; include dependencies/support files, not only an entrypoint path.
- Validate, activate and run through exact revisions and execution profiles. Source edits never mutate an active invocation's bytes.
- Add a narrow script-bundle publisher because current artifact APIs do not offer generic arbitrary upload. Bound size/file count, reject traversal/symlinks escaping the bundle, retain provenance and apply read permissions. Do not turn it into unrestricted filesystem upload.
- Implement Python and PowerShell adapters with JSON stdin/protocol output, no shell interpolation, private environment and selected local interpreter.
- Separate `trusted_local` from genuinely enforced `isolated`. Show the actual OS identity/trust and refuse unsupported isolation.
- Permit agent-authoring/activation inside a standing grant; wider trust/effects need additional authority. No manual Root approval for every harmless authorized change.
- Bound processes, stdout/stderr, duration, retained output and child effects. Reconcile unknown outcome; kill only owned invocation descendants under explicit timeout policy.
- Built-in Rust notify/check/publish actions bypass scripting entirely. Optional script-promoted MCP tools need catalog schema/risk review.

### Done when

A grant-authorized agent can register a Python or PowerShell script and run it on demand without editing global settings. Revocation is enforced at dispatch. A changed script does not change an already scheduled pinned run. Exit zero with a live descendant is not success.

## 10. O7 — Rules and the first complete vertical recipe

### Planned units

```text
src/automation/rules.rs
src/automation/dispatcher.rs
src/store/automation.rs
```

### Work

- Build a typed predicate/mapping registry with immutable revisions, no arbitrary expression language.
- Add create/update/enable/disable/simulate. Simulation returns matches/planned targets and never effects.
- Atomically record trigger consideration, occurrence/action-slot dedupe and existing Operation admission.
- Track causal ancestry, same-rule descendant suppression, no-progress fingerprints and bounded error handlers.
- Support ready native actions and registered script actions through one dispatcher.
- Implement commit-to-auditor end to end: Git observation -> exact context -> deduplicated notice -> optional standing-grant review dispatch -> exact evidence reader.
- Review assignment must name a distinct authorized auditor and captured candidate; WIP notices without a candidate remain informational.

### Done when

The example works without any script. Adding a user script to transform a report uses the same invocation path. No ordinary notice wakes a model; an explicitly preauthorized review rule may start exactly the eligible review. Hook/webhook redelivery and restart do not duplicate it.

## 11. O8 — Cron and dynamic server schedules

### Existing anchors

`src/scheduler.rs`, `src/store/schedules.rs`, `src/config.rs`, `docs/schedules.md`.

### Work

- Add cron/timezone preview using a complete reviewed evaluator. Retain one-shot/interval functionality.
- Separate trigger definition, occurrence and action invocation. Store logical UTC due instant separately from actual run time.
- Preserve v1 schedule IDs/receipts and configured enabled state. Import old records idempotently into the upgraded indexed registry; only one scheduler owns each ID during migration.
- Add revisioned create/update/pause/resume/run_now, lateness and overlap policy, bounded catch-up and fair concurrency.
- Recheck permission and target freshness before action start. Unknown earlier same-target effect blocks overlapping mutation, not unrelated work.
- Record calendar engine/tzdata revision and preview DST behavior. Bound scans of pathological or impossible expressions.
- Update the explicit policy delta in Owner Decisions §4 and `schedules.md` with implementation status.

### Done when

An enabled named Python script and a Rust notification action can each run by server cron with manager/UI disconnected. Sleep/reboot follows missed-run policy without a burst. Pausing a schedule does not silently kill a running script.

## 12. O9 — Server Goal and shared reminder service

### Planned units

```text
src/goals.rs
src/store/goals.rs
shared watch/reminder implementation from #22
```

### Work

- Add `goal.*` independent of `agent.goal`, referencing an assigned work pool and explicit completion predicates.
- Reuse Task/Attempt operations for work. Never add a second mutable Task graph.
- Evaluate only changed evidence/dependencies and coalesce redundant wakes. Record decision/action linkage and standing grant.
- Support active/waiting/paused/achieved/cancelled/needs-attention with separate execution outcomes.
- Implement one continuation owner per binding. Native loop active or uncertain -> no competing server continuation.
- Goal pause/cancel controls future admission; active child cancellation is explicit and uses its owner.
- Reuse one watch store/timer heap. Fact reminders default to notifications, not model prompts.
- Update Owner Decisions §3 to describe this separately authorized server Goal while keeping existing native `agent.goal` meaning unchanged.

### Done when

A Goal can advance an authorized pool on a harness without native Goal support. It survives manager exit and resumes after reboot without repeating uncertain input. A Goal cannot complete from text saying "done" or acquire broader authority to unblock itself.

## 13. O10 — Deferred MCP, CLI and status

Follow #22's hard profile / eager surface / deferred catalog separation.

- Register every typed application method in validation, authorization, Store dispatch, CLI and the MCP catalog exactly once.
- Keep ordinary manager core small. Search loads streams/hooks/schedules/goals/scripts/roles/forge groups only when requested and authorized.
- Avoid enormous mode-switching tools and two competing names for the same capability.
- Reads/list/search expose no forbidden names or unrelated job data. Cached schemas do not bypass current authority.
- Hook/mutation/output annotations are hints; server checks remain authoritative.
- MCP subscriptions/resources use negotiated capabilities. Where notifications are unsupported, use bounded cursor reads without pretending a notification reached the model.
- Role/capability revocation rechecks every call and publishes the appropriate catalog change. Old cached clients cannot retain new rights.
- Dashboard includes next due jobs, active hooks, failed rules, Goal progress, script history, pending audits and actual streaming coverage.

No remote domain or service setup is part of this wiring slice.

## 14. O11 — Qualification matrix

These are mandatory planned cases, not results of this documentation review.

| Area | Positive case | Failure/counterexample |
|---|---|---|
| Monitoring | Native progress updates dashboard with manager asleep | Shared server sibling event misattributed; missing stream appears healthy |
| Snapshot/delta | Snapshot then exact cursor continuation | Change between read/subscription is silently lost |
| Streams | Authorized native summary/text and complete item retrieval | Hidden/encrypted state exposed, split secret leaked, slow reader blocks control |
| Hooks | Installed supported callback emits correctly scoped event | Async hook claimed as veto; observer backlog blocks compaction |
| GitHub | Valid webhook plus missed-delivery reconciliation | Invalid HMAC, duplicate/reordered delivery, truncated page treated as complete |
| Commit/audit | One OID creates one notice and authorized review | Local+remote double dispatch; WIP accepted as final; old result modifies new head |
| Scripts | Pinned Python/PowerShell bundle, valid typed result | Bundle changed, dependency escaped, bad JSON, endless output, live descendant |
| Permissions | Authorized local agent reuses standing script grant | Script self-promotes, stale grant starts work, custom role inherits every writer right |
| Scheduler | Once/interval/cron with known next occurrences | DST/clock jump, impossible expression, restart burst, duplicate ID migration |
| Goal | Evidence-driven progress through existing Task pool | Native and server continuation both active; same failure repeatedly reprompted |
| Forge | Accepted candidate publication/qualified merge | Lost response replay, stale PR base, current Git hook policy bypass |
| MCP | Searchable scoped domain without eager catalog inflation | Manual hidden-tool call, stale cached capability, disconnected observer stops job |
| Recovery | Host reboot reconciles exact prior invocations | Replacement script starts while old writer's disposition remains unknown |

Suggested staged contour: 10,000 registrations, 1,000 tracked active work contexts, 1,000 timer/watch definitions, 500 bounded viewers and a synthetic event burst. Registered identities are not active model turns. Measure RSS, handles, DB lock time, loss/gaps, control reply latency and per-source recovery. Start live model qualification with a small authorized fleet and increase only after measured resource/cost/results remain acceptable.

Do not infer safety from a clean UI or green process exit. Do not execute paid model work or owner-machine cleanup in CI.

## 15. Persistence and migration checklist

Before choosing DDL, enumerate current schema ownership and reuse existing Operations for outcomes. Add forward migrations only; never patch an old migration in place.

Minimum required durable information:

```text
versioned definitions: rules, scripts, roles/grants, schedules, goals
indexed next due/subject routing
unique occurrence/cause + definition revision + action-slot linkage
source health/cursors/delivery dedupe and bounded gap records
current projection revisions and exact source Operation references
```

This does not require a second event store. Script bundles and optional stream chunks use the existing artifact mechanism with a narrow publisher extension.

Migration runs under the existing single host/Store owner, is transactional and can resume safely. Old and new scheduler implementations must never own one schedule simultaneously. Old binaries must refuse a newer unsupported schema, not ignore it. Keep a recoverable backup and a documented rollback procedure; never promise binary downgrade will automatically undo new side effects.

## 16. Installation and privacy handoff

Local setup collects trusted interpreters, project roots, GitHub credentials/webhook ingress, role grants, service startup choice, quotas, stream retention and script trust modes. Values stay in user-restricted local configuration/secret storage, outside Git.

Setup preview lists exact files/services to change, old/new hashes, required privileges and rollback. It neither restarts an existing harness nor changes UAC/PATH/branch protection silently. Do not rewrite an executing script in place.

A separate setup qualification proves: host survives client exit; startup after reboot; native hook readback; process ownership; webhook authentication; named script execution; disabled rule/schedule stops future admissions; secret/private-path redaction.

## 17. Completion definition

The feature is not complete merely because another tool list exists. It is complete for the delivered scope when:

- managers see current work without asking models for status;
- supported native streams/hooks are observable with truthful coverage;
- server schedules and Goals work independently of harness timers;
- built-in actions and authorized user scripts share one durable execution path;
- commit-to-auditor works without duplicate model wakes or stale-candidate effects;
- custom roles and standing grants permit useful autonomy without privilege escalation;
- all new methods are accessible on demand through MCP without eager catalog overload;
- restart, overload, revoked authority and unknown effects preserve correctness;
- privacy and installed-runtime qualification evidence are recorded honestly.
