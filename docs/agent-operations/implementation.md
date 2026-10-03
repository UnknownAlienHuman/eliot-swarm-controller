# Implementation Plan — Rust Agent Operations and Reviewed Delivery

Revision 2 · 2026-10-03 · inspected main `35e499ae73b622d873c44873f6993ee3fcbea87b`.

This is an implementation contract, not a record of completed code. Read [README](README.md), then the relevant [Architecture](architecture.md), [Delivery](delivery.md), [Configuration](configuration.md) and [Donor map](donor-map.md) sections. Internal systems are Rust; optional external scripts are Python or PowerShell. No fixed library/CLI/model release is prescribed.

## 1. Shared ownership with PR #22

PR #22 owns the proposed Participant role, peer communication/cards/integration cells, shared watch service, launcher and deferred catalogue concepts. This program owns continuous collectors, server automation, configurable distribution/review/publication and their shared settings. Check what has actually landed before creating files.

Implement exactly one role/grant evaluator, watch store, dashboard, launcher and Task/Attempt lifecycle. Neither a delivery preset nor a server Goal creates another Task ledger. Existing Operations remain execution receipts and outcomes; new indexes reference them.

One manager owns one worktree and one mutable candidate. Internal writer assignments use non-overlapping files and do not become separate product Issues/submissions. Writers do not run Cargo. The manager integrates the completed Issue and runs scoped formatting/minimal warnings-denied Clippy once. Broad tests, load and installed-runtime qualification occur after the corresponding complete product path, unless the owner explicitly advances a named check.

An Issue delivery includes its producer, consumer, registration and result reader. Do not leave production modules disconnected until the final integration phase. Each slice exposes its usable narrow application path; O10 completes cross-surface parity, not the first call site for every earlier slice.

## 2. Dependency sequence

```text
O1 shared Rust authority, actions and configuration
  -> O2 event intake, source health and current projections
       -> O3 Rust native adapters, monitoring and streams
       -> O4 GitHub/Git mapping and work distribution
       -> O5 Rust hook ingress and capability setup
  -> O6 optional external-script registry/runner

O1 + O2 + O4 + actual runtime/launcher path
  -> O7 complete reviewed_delivery: submit -> audit -> repair/publication

O1 + O2
  -> O8 cron and dynamic rules/actions
       -> O9 server Goal and shared reminders

O10 complete deferred MCP/CLI/configuration parity
  -> O11 integrated qualification and installation handoff
```

The ordinary delivery workflow does not depend on Python/PowerShell scripts or O6. O3/O4/O5 may proceed in parallel only after agreeing shared source envelopes and file ownership. No worker independently edits shared Store/registry contracts.

## 3. O1 — Authority, configuration and action admission

### Existing anchors

`src/model.rs`, `src/policy.rs`, `src/config.rs`, `src/store/mod.rs`, `src/store/operations.rs`, `src/store/submissions.rs`, `src/store/acceptance.rs`, `src/store/forge.rs`, `src/mcp/profiles.rs` and the actual #22 implementation when available.

### Proposed units

```text
src/automation/actions.rs
src/automation/config.rs
src/authorization.rs              reuse a shared evaluator if already present
src/store/automation.rs
src/store/configuration.rs
src/runtime/profiles.rs
```

### Work

- Inventory positive and negative role checks, ownership checks and GM checks before adding custom roles. New roles must not inherit every right merely because they are not observers.
- Define finite capabilities, scoped standing execution grants and actor provenance. A workflow service is neither a reviewer model nor a disguised GM. It acts for its sponsor through the same application guards.
- Keep `task.request_changes`' existing current-GM/operator path and add a narrow delegated disposition path for an assigned review. Preserve exact Task/Attempt/submission/candidate and requirement guards.
- Define similarly narrow delegated acceptance/publication only when configured and granted. A manager preset alone must not unlock a GM-only effect.
- Preserve historical owner-policy editions. Current `src/policy.rs::attempt_projection` recognizes only equality with `current_edition`; replacing that edition naively would label retained Attempts unrecognized. Add an accepted-edition registry or equivalent explicit compatibility path, without rewriting historical snapshots.
- Update the relevant Owner Decisions and their code bindings in the same delivery. Do not edit a document digest to manufacture permission or weaken old Attempt checks.
- Implement one typed action admission shared by manual, preset, rule, timer and Goal causes. Invoke the internal handler, not recursive IPC while holding a Store transaction.
- Use the active definition for new admissions and retain the effective execution snapshot. The same request ID after a settings change returns the old receipt, not a newly resolved action.
- Define semantic cause/action-slot uniqueness independent of incidental rule edits. Intentional replay has an explicit authorized replay generation.
- Implement `automation.config.get/preview/apply/explain` and runtime profiles with typed configuration, revision conflict detection, editable-field feedback and no-effect preview. Use Configuration's complete candidate tuples and precedence.

### Done when

An authorized agent configures reviewed delivery and preferred writer/auditor models without editing several services. It can change future defaults inside its grant, but cannot gain protected effects, mutate an active run or invalidate historical Attempt evidence by changing a role/profile name.

## 4. O2 — Events, source health and projections

### Existing anchors and proposed units

`src/store/projection.rs`, `src/store/capacity.rs`, `src/mcp/subscriptions.rs`, Store Observation insertion and committed-change signals.

```text
src/monitoring/events.rs
src/monitoring/projection.rs
src/monitoring/pump.rs
src/store/monitoring.rs
```

### Work

- Establish the minimal event envelope: verified source identity, optional native cursor/event ID, source epoch, controller sequence, exact work/binding scope, event kind, evidence class and coverage/gaps.
- Keep control/terminal evidence durable and live text presentation separate. Never permit a native/tool event to assert controller acceptance or forge completion.
- Share source readers/projectors and scope-aware fanout. Replace redundant per-viewer scans while retaining existing public lag/resync behavior.
- Implement consistent committed snapshot plus cursor and bounded delta reads. Authorize before selection; no global roster dump followed by client-side filtering.
- Retain per-source unknown/unavailable state. A failed process/API query must not become an empty successful inventory.
- Feed trusted configuration-file changes through O1 validation, not direct mutation. Parent-directory watching handles atomic replacement; invalid changes retain the last valid revision.
- Reserve control/permission/completion capacity independently from optional telemetry. Bound CPU parsing and blocking work, not just async queue length.

### Done when

A malformed event, slow viewer or partial configuration write does not block native replies or unrelated work. A disconnected client resumes via bounded authoritative reads without a model call.

## 5. O3 — Rust native adapters, monitoring and streams

### Existing anchors

`src/runtime/owner.rs`, `src/runtime/warm_stream.rs`, `src/runtime/opencode_v2/*`, `src/platform/process_group.rs`, `src/doctor.rs`, and the observed mapping behavior of existing non-Rust modules.

### Work

- Implement ELIOT-owned native transport/control/translation in Rust using the documented protocol or a suitable whole Rust library. Treat existing Python/JS/TS mappings as migration sources, not the implementation target.
- Prefer the installed protocol/capability contract over exact software-version equality. Accept additive native fields; reject unsupported consequential operations precisely. Do not rewrite vendor model loops or copy commercial SDK internals.
- Preserve binding/generation, native input/turn identity, delivery certainty and family ownership through migration. Existing live sessions are not restarted to activate a new adapter.
- Wire at least two actual supported native event producers before expanding a universal stream trait. Other adapters report their real scope/gaps.
- Add one selective metrics sampler from a maintained compatible Rust library. No old sysinfo release requirement or permanent MSRV freeze is part of this plan; toolchain/dependency support changes are explicit code changes.
- Provide scoped text/tool/provider-exposed reasoning streams, redaction and byte/retention budgets. Do not promise hidden/encrypted reasoning access or inherit missing native replay features by declaration.
- Track process, connection, model/tool, child-family and work state independently. Dashboard reads current projections even with all manager models idle.
- Record actual requested/effective model and options, catalogue freshness, fallback and shared account-capacity group. Model-name presence is not proof of a working route.

### Done when

The monitored delivery path operates through Rust adapters without ELIOT-owned Python/Node control processes. Native work survives observer exit. Unknown parent/child disposition cannot start a competing writer. Capabilities absent on the chosen native interface are explicitly incomplete, not claimed by a manifest.

## 6. O4 — GitHub/Git intake and distribution

### Proposed units

```text
src/github/client.rs              one Rust API/rate boundary
src/github/observer.rs
src/github/webhook.rs
src/github/work_pool.rs
src/git_inspect.rs                shared with #22
src/automation/distribution.rs
src/store/github.rs
```

### Work

- Use a complete maintained Rust GitHub client where suitable, behind one narrow application port. Do not introduce `gh`/Python subprocesses as the GitHub controller.
- Resolve local repository/credential handles without exposing secrets or deployment endpoints. Inventory supported read/write/Checks/publication permissions.
- Verify raw webhook signatures and allowed source context, persist intake before ACK, deduplicate and reconcile gaps. Use conditional reads, paging and account-level rate budgets.
- Give each external Issue/PR one stable Task origin; source changes produce revisions. Preserve canonical source references and surface unclassified potentially normative comments.
- Implement manager-selected pool preview/apply. Eligibility is computed before queue page limits. Keep manager order unless an explicit routing/ranking policy says otherwise.
- Reserve Task/Attempt, manager slot and workspace ownership transactionally. No stale queue read, label or heartbeat age can assign a duplicate writer.
- Prepare compact assignment/neighbor context through the existing/planned launcher. Native dispatch remains an Operation outside the transaction.
- Use one Git watcher/inspection path per relevant scope; verify actual OID/worktree state before treating a hook as a commit observation. No per-token full status scan.

### Done when

Several eligible managers consume the assigned queue without duplicate ownership or Root relaying assignments. Missing events and unavailable routes yield scoped wait reasons. A new source revision does not create a second Task for the same Issue.

## 7. O5 — Rust hook setup and ingress

### Work

- Implement per-event native capability/schema/phase/veto/lifetime/readback metadata.
- Provide Rust `swarm hook emit` and direct typed adapter ingress. Hooks authenticate source/binding scope; no self-asserted manager ID grants access.
- Prefer native executable callbacks or actual external protocol events. Where a function is exposed only through a non-Rust custom plugin with no acceptable external interface, report the Rust integration gap instead of extending the current TS mod and claiming compliance.
- Preserve existing user/managed hooks via preview/apply. Installation and restart requirements are separate, explicit operations.
- Keep observational callbacks short and nonblocking to productive work. Heavy script/review/publication actions execute through the host, never inline.
- A pre-effect gate uses bounded local policy; async after-events are not vetoes. ELIOT wrapper events cover only ELIOT-controlled calls.
- Keep protected Git hook suppression in forge; add Rust action before/after extension events instead.

### Done when

Supported hooks produce correctly scoped events without an owned JS/Python daemon. Callback failure or optional backlog cannot wedge compaction. A missing safety capability blocks its protected action only, with an actionable explanation.

## 8. O6 — Optional external script registry and Rust runner

### Proposed units

```text
src/scripts/manifest.rs
src/scripts/registry.rs
src/scripts/runner.rs
src/scripts/protocol.rs
src/store/scripts.rs
```

### Work

- Reuse artifacts and owned-process patterns; extract common code only where semantics match existing CheckRunner/forge containment.
- Register bounded script bundles with declared support files and typed inputs/results. Reject traversal, escaping links and unrestricted upload paths.
- Named scripts follow the active definition for a new invocation; retain the admitted bundle/environment and old request receipt for recovery. Updates do not alter executing code or replay old slots.
- Resolve installed Python/PowerShell and prepared dependency environment through local configuration. No fixed interpreter release and no package installation in cron/hook hot paths.
- Pass JSON stdin/separate argv, bound output and own all descendants. JSON shape validation is not a proof of substantive success.
- Make `trusted_local` OS trust explicit; support `isolated` only with real enforcement. No silent fallback and no default ExecutionPolicy Bypass.
- Permit agent author/activate/run within standing scope. Changes to secret/network/OS/publication authority require the relevant grant, not every script edit.

### Done when

An authorized agent can add an optional script and attach it to a rule/schedule. The ordinary delivery path still works with neither interpreter installed. A script update affects the next independent invocation, not a running process or retried request.

## 9. O7 — Complete reviewed delivery and publication

### Existing anchors and proposed units

Reuse `src/store/submissions.rs`, `src/store/acceptance.rs`, `src/store/forge.rs`, `src/forge.rs`, source capture/checks and Task dispatch.

```text
src/automation/delivery.rs
src/review.rs
src/store/review.rs
src/github/projection.rs
src/github/pulls.rs
```

### Work

- Implement the `reviewed_delivery` preset before optional custom workflow complexity. Start review only after an applied valid `task.submission` fact.
- Reserve exact review slots and implement `review.assign/get/submit`. Receive authenticated structured pass/changes_requested/inconclusive verdicts with actual requirement coverage/evidence.
- Bind results to immutable submission/candidate, retain late results historically and reject self-review where independence is required.
- Aggregate `audited` from required slots and declared phase, not from labels or a model's prose. Unknown/failed collection and missing required evidence cannot pass.
- Apply actionable corrections through the O1 delegated branch of the existing feedback transition, then separately admit repair delivery to the owning manager. Mail storage alone is not model delivery.
- Prefer the original owner; switch a route only under configured fallback and after previous native/lease disposition is known. Keep corrected work eligible even when checklist text is unchanged.
- Implement auto-after-audit and manager-gate using the same candidate/effect checks. No manager round trip for each eligible repair/publication inside an existing grant.
- Reuse existing accepted non-force publication. Add candidate upload only for explicit review-branch mode, with a restricted namespace and no acceptance claim. Local-first review must work before the first push.
- Implement Rust GitHub check/summary/label projection. Do not treat neutral/skipped checks, bare labels or an App's internal reviewer count as independent successful reviews.
- Record Check Run IDs/correlation and annotation-batch delivery. Annotation retries must account for append semantics; uncertain create does not justify blind duplication.
- Implement PR publication against actual repository/API capabilities. Native GitHub merge queue is optional; a local Rust publication queue is always distinct from it. Do not require organization-only functionality for this user-owned repository.
- Preserve head-versus-base semantics and verify the integrated candidate through a qualified path. No local mutex excludes all external writers, and a manager approval is not an atomic base guard.
- For supported asynchronous merge, record request UUID/options and reconcile status. An enqueued result requires later actual PR-merge observation. The documented result lifetime is finite (currently 24 hours after its last update); after expiration read PR/ref/commit state rather than repeat merge on a 404.
- Handle merge-group checks where selected, workflow-token triggering differences and remote permissions without bypassing protections. Do not execute untrusted candidate code with privileged workflow credentials.
- Separate publication from bookkeeping so a label/comment failure cannot re-run push or return correct code for implementation. Confirm remote outcome before releasing candidate/workspace or applying closure policy.

### Done when

A manager supplies a pool once. An executor produces a submission, an auditor returns one actionable finding, the owner fixes it, the corrected candidate is audited, and the selected authorized auto/gated publication completes with readback. The path has no internal Python/PowerShell/Node dependency, no comment-triggered model loop and no perpetual version pin.

## 10. O8 — Cron and event-rule management

Extend existing scheduler/config/store and add typed rule routing. No second daemon or general workflow language.

- Use a maintained complete Rust cron evaluator and explicit timezone/DST policy with next-occurrence preview.
- Preserve existing schedule identity/receipts through idempotent migration into indexed definitions.
- Distinguish definition, due occurrence and invocation. Script/profile edits do not repeat previously considered occurrences or semantic events.
- Default to latest-only catch-up and no conflicting mutations; bounded replay and concurrency are configurable for eligible repeat-safe actions.
- Allow agent configuration inside standing grants, with no-effect preview and active-definition selection.
- Recheck grants/targets at start; pause stops future admission, not running jobs. Unknown same-target effect prevents duplicate mutation, not unrelated scheduling.
- Handle calendar-engine/timezone-data change by verifying declared semantics and recalculating future occurrences, not by silently freezing one old library forever.
- Bound rule ancestry and repeated failure; status and failure notifications cannot feed an unbounded automatic model loop.

Done when Rust native actions and optional named scripts can run on server time/events while manager clients are disconnected, without catch-up bursts or per-agent polling processes.

## 11. O9 — Server Goal and shared reminders

Use `src/goals.rs`, `src/store/goals.rs` or the established module layout; reuse #22's watch storage and timer structure.

- Reference existing assigned Tasks and exact completion evidence; do not construct a second work graph.
- Advance only eligible actions under standing grants, runtime preferences and actual capacity.
- Keep one native/server/manual continuation owner. Native Goal active/unknown or unfinished children cannot lead to a competing continuation.
- Distinguish waiting for dependency/capacity from a defect; material new evidence reactivates relevant work without repetitive identical prompts.
- Pause/cancel future admission; current child cancellation remains separate and addressed through its owner.
- Reminders are notices unless an explicit granted execution action is attached. Never replace the current assignment with the reminder text.

Done when a harness with no native Goal participates in the same server-owned delivery progression and recovery as one with native support, without competing model loops.

## 12. O10 — Complete MCP/CLI/configuration parity

- Register typed application methods once in validation, authorization, dispatch and the MCP catalogue; preserve the existing validated wire-name transformation.
- Keep normal eager cores small. Configuration, runtime profiles, review, streams, hooks, rules, scripts, schedules, Goal and forge are searchable deferred groups.
- Return field-level validation errors, editable fields and an effective settings diff. Common preset changes require one preview/apply, not manual construction of low-level records.
- Test native discovery/list-change behavior by actual client capabilities; fixed small surfaces remain a supported fallback. Pagination alone is not lazy loading.
- Recheck all cached/manual tool calls under current rights. Optional script promotion cannot expose unreviewed future capabilities.
- Include delivery phase, original-owner repair, current reviewers, effective model/profile, pending approval and remote projection lag in dashboard/inspect/explain.

Done when an authorized agent can configure and inspect the normal workflow without reading a giant tool catalogue or asking Root how to wire every stage.

## 13. O11 — Final qualification matrix

These are requirements to exercise after the complete path exists, not tests executed by this documentation PR.

| Area | Required case |
|---|---|
| Rust boundary | Ordinary startup, observation, queue, audit, repair and GitHub publication need no owned Python/Node/PowerShell control scripts |
| No software pin | A newer compatible CLI/model catalogue is accepted by capability; optional unknown fields do not fail exact-version checks |
| Preferences | Changing writer/auditor model affects next assignment; no active session restart or hidden billing fallback |
| Configuration | Concurrent edits conflict; malformed file preserves last valid settings; repository text cannot activate privileged changes |
| Task origin | Repeated source edits update one Task's revisions rather than create duplicate Tasks |
| Distribution | Concurrent dispatch admits one owner; blocked rows do not hide ready work; unknown execution does not release its workspace |
| Submission | Queued/failed submit starts no review; applied valid submission starts one required set of slots |
| Review | Required coverage passes; malformed/inconclusive output remains non-success; reviewer cannot approve own code |
| Repair | Corrected source can be re-reviewed with unchanged checklist; late A feedback cannot affect B |
| Delegation | Auto handoffs work without GM credentials; revoked grants stop queued effects without replaying uncertain ones |
| Publication | Local audit precedes first push; remote-review mode uploads only to permitted review refs; manager mode gates only its intended final step |
| GitHub | Duplicates, missed events, permission loss and token-trigger differences are visible without repeated model dispatch |
| Checks | Neutral/skipped does not fake ELIOT acceptance; label lag and append-only annotations cannot corrupt current-candidate verdict |
| Merge | Head and integrated-base freshness differ; async result expiration and enqueued status do not produce another merge |
| Hooks | Supported callback works from Rust; optional ingress backlog cannot block compaction; async hook is not a veto |
| Scheduler | DST/sleep/reboot follows policy; active-script update does not repeat completed slots |
| Goal | Native and server continuation never compete; parent end with active children is not assignment completion |
| Streams | Exact native ancestry, cumulative usage, chunk-safe redaction and bounded slow-viewer recovery |
| Process ownership | Failed OS query is not empty inventory; unconfirmed descendants preserve the scope hold |
| Migration | Old receipt/Attempt/policy evidence remains readable and old/new schedulers do not own one ID simultaneously |

Measure control latency, queue wait, audit/repair throughput, false exceptions, duplicate runs, input/schema tokens, RSS, process handles and SQLite contention. Staged simulated fleets precede small explicitly authorized live runs. Target scale numbers are qualification goals, not current performance promises. No paid provider work or owner-machine cleanup in ordinary CI.

## 14. Installation, migration and completion

Keep one Store and forward migrations. Add only required indexes/current metadata for configuration, origins, memberships, review slots/verdicts, due occurrences and effect linkage. Operations keep execution outcomes; artifacts keep immutable source/script/evidence bytes. Do not add a second event store for the delivery preset.

Local setup resolves installed executables/interpreters, source roots, credentials, optional webhook ingress, grants, runtime/model profiles and publication policy. All deployment values remain outside Git. Setup preview states exact changes and privileges; service restarts/installations remain explicit and never occur as a side effect of discovering a tool.

Retire old owned non-Rust bridges only after their bindings have a proven safe disposition or qualified handover. No destructive migration of live native stores and no assumption that binary downgrade reverses external effects. No dependency manifests, native versions or machine configuration are modified by this documentation PR.

The delivered feature is complete only when the normal manager queue -> executor -> audit -> correction/audited -> authorized publication path is callable and observed end to end, independently of manager-client lifetime. A tool list, green process exit, optimistic label or updated document is not that evidence.
