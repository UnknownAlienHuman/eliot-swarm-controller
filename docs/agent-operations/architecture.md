# Agent Operations — Rust Architecture and Execution Contracts

Revision 4 · 2026-10-03 · source baseline `504199d14135c030ad3951a3c5023a098a3d03f0`.

[Configuration](configuration.md) owns per-automation settings; [Delivery](delivery.md) owns work transitions; [Donor map](donor-map.md) separates external evidence from design. New contracts are proposed, not implemented by this document.

## 1. Rust boundary

All ELIOT-owned host, Store, authorization, configuration, native transport/adapters, process supervision, monitoring, GitHub, hooks, distribution/review, cron, Goal, MCP/gateway and script-runner logic is Rust. Python/PowerShell are optional external extensions, not internal services or mandatory wrappers for basic operations.

Vendor executables remain external products. Rust adapters use actual documented protocols or suitable maintained Rust libraries. Existing owned Python/JS/TS bridges are migration inputs. A capability reachable only through an unacceptable non-Rust owned subsystem remains an explicit integration gap; do not invent parity or copy private vendor internals.

No fixed library/CLI/model release is prescribed. Qualify protocol/capability compatibility; preserve observed versions as evidence. New adapter settings affect future bindings, not an unannounced restart of existing work.

## 2. One action path, owned by the manager

Manual management is always available. Automations are individual manager-owned definitions, disabled unless that manager enables them. There is no global mode state machine or parallel control service.

```text
explicit manager command -------------------------------+
                                                       |
verified event/time -> enabled automation -> its manager |
                                                       v
                     common action/object/resource checks
                                                       |
                             retained Operation + action slot
                                                       |
                        Rust worker performs I/O outside Store
                                                       |
                               outcome/evidence/readback
```

A manual action needs no automation object. An automation uses the same rights the owning manager currently has for the configured action and target. Enabling it is sufficient standing instruction within those rights; do not require another per-stage grant or Root approval merely because execution is automatic.

The service does not acquire the manager's unrestricted credentials. It receives a trusted internal execution context limited to that enabled definition, resolved action and scope. This context is created from Store state, never from an event's `manager_id`, a role name or a public `run_as` parameter.

### 2.1 Authorization and attribution

Keep these facts distinct:

```text
requester / technical executor
on_behalf_of_manager_id
automation_id and definition revision, when automatic
semantic trigger/cause and action slot
Task / Attempt / candidate / effect target
actual reviewer or native producer, when recording evidence
```

Resolve the manager's current role, scope and existing grants using the shared authorization evaluator. Check again immediately before a consequential effect. Automatic execution cannot do something the manager cannot do directly. Existing GM/epoch, accepted-candidate and repository checks remain authoritative.

Use one trusted execution context in the common Rust handlers. Do not change an existing Operation's `caller_id`, forge a manager `Principal`, pass a GM token around, or give the internal scheduler unrestricted writer rights. Add the on-behalf linkage without losing the actual requester and existing receipt identity. Public request parsing rejects an attempt to supply internal execution identity.

Current feedback/acceptance/forge role gates require explicit implementation changes where scoped manager rights are intended. Add those rights to the shared action check for both manual and automatic use; do not add a service-only bypass. Preserve historical policy editions and current candidate checks when updating Owner Decisions.

A manager launched by another manager is still a manager for its assigned scope. Its real authority, not its launch origin, decides which automations it can configure. Ordinary executors, auditors and script invocations are not promoted by changing a profile title.

When an automation assigns an auditor, the assignment is on behalf of its manager, but `review.submit` remains attributable to the assigned auditor. A successful script or manager-owned automation cannot fabricate independent review evidence.

### 2.2 Shared deduplication and execution identity

Retain `(caller_id, client_request_id)` receipts. Internal automatic request IDs are deterministically derived from the definition identity, semantic cause and action slot; never use a constant request ID across all owners or an event timestamp alone.

Also retain domain-level uniqueness across callers: one current Task launch reservation, one `(submission_ref, review_policy_generation, review_slot)`, and one exact candidate/target publication slot. Manual versus automatic origin is not a separate slot. An incidental rule/config edit does not permit the same effect again.

Distinct automations may legitimately observe the same project or submission. They are not mutually exclusive owners of the entire Task. Only conflicting effect/role/workspace reservations contend. Identical requests coalesce or return an existing reference; incompatible choices report the exact conflict instead of running two auditors in one slot or pushing twice.

Resolve active profiles/scripts for each new invocation and retain that invocation's effective inputs. An identical retry returns its old receipt, not newly resolved settings. A deliberate rerun needs a new explicit decision after the prior outcome is known.

### 2.3 Disable versus effect start

The enabled/revision check and transition to effect-started serialize with disable/update in the existing Store. I/O occurs afterward, outside the transaction.

If disable commits first, an unstarted automatic invocation remains held and cannot start using old admission permission. If effect-start commits first, report it as in-flight; its previously authorized I/O may finish after disable returns. Do not claim instantaneous remote rollback.

A started operation may finish its bounded action and record/reconcile its result. Each separate follow-up action consults the currently enabled definition and manager rights. Work already assigned to an executor may finish; turning off audit dispatch does not kill that auditor or revoke its ability to submit evidence.

Never clear native Goals, interrupt models, cancel remote merges, delete worktrees or release live ownership implicitly on disable. Those are separately requested supported actions. A running trusted-local script may continue real OS effects; an API flag is not a sandbox.

### 2.4 Manual action while automation is pending

Manual controls remain available. A manager's normal typed action may name an existing pending operation when choosing to handle it directly. If the automatic operation is provably never started, atomically supersede its queued attempt and link the new manual request to the same logical slot. Preserve both receipts; the old worker's start check must fail.

If work has started, is sending or has an unknown outcome, return the actual operation/readback reference instead of repeating it. Completed equivalent work returns its result. This shared handler prevents both duplicate execution and a permanently held automatic slot making manual control unusable.

## 3. Continuous monitoring

| Source | Rust path | Truthful coverage |
|---|---|---|
| Native stream | Shared reader per supported connection/scope | Verified binding/root/child/generation; shared server traffic is not every manager's traffic. |
| Native snapshot | Shared bounded read-only reconciliation | Missing/partial data is not proof of termination. |
| OS process | Recorded start/boot/ownership plus selective metrics | PID alive is not model progress; enumeration failure is not an empty inventory. |
| Git | Shared change hints plus exact object/status reads | File events and commit authors do not assign current work. |
| GitHub | Authenticated intake plus conditional paged reads | Comments, labels and assignees are source facts, not runnable instructions. |
| Store | Committed Task/Attempt/Operation/submission events | Queued admission is not successful completion. |

Observation works with no automations enabled. Configuring observation does not enable model launch, scripts or remote writes. Use events first and centrally paced read-only polling where necessary. Never ask models for status or invoke a health-check CLI that may restart a shared service.

Parent idle, wrapper exit, mtime and tunnel loss do not prove that descendants or native Goal stopped. Preserve actual lifecycle ownership and uncertainty before admitting a replacement writer.

### 3.1 Dashboard

Join source Issue/revision; Task/Attempt/assignment/submission; manager/executor/auditor; native binding/root/child/turn; workspace/branch/candidate; process, connection, execution and delivery states without conflating them.

Show the manager's enabled automations, each configured scope, last/next run, pending manual choices and in-flight/unknown effects. There is no mode gate on the dashboard. Waiting for the manager is not a code defect or reason to send repetitive prompts.

Transport activity and material progress are separate. Include tool/question/child state, CPU/RSS, shared account capacity and source gaps. Unknown native statuses remain unknown. Reads use maintained projections, not a sequence of model interrogations.

### 3.2 Cursors and fairness

Return a consistent committed snapshot plus controller high-water cursor. Preserve upstream cursors independently; do not promise a globally atomic native/GitHub/OS view. Migrate per-viewer fact polling to shared intake/projectors while preserving lag/resync.

Bound queues and serialized bytes. Reserve control, native reply and completion capacity separately from optional streams, Git scans and scripts. Slow readers receive a gap and reread authoritative state. CPU parsing and OS work use bounded blocking workers; no transaction waits on network, process or model.

## 4. Text, tools and reasoning streams

`stream.open/read/close` observes an authorized assignment/binding; it starts no model. Support cursor pages and qualified notifications.

Expose only deliberately supplied assistant text, reasoning summary/native reasoning text, tool progress/results, lifecycle and usage. Hidden/encrypted reasoning is not decoded or reconstructed. Mark unavailable, redacted and not-retained classes explicitly.

Preserve native item/part IDs and order. Final content supersedes matching deltas without double-counting; cumulative usage replaces prior cumulative data. Unknown cost basis stays unknown.

Use shared bounded rings and optional capped artifact chunks, not full Observations per token or transcript copies per viewer. Redact across chunk boundaries; use bounded logical records or omit sensitive classes until streaming redaction is qualified. Protect referenced evidence by ownership, not file mtime. Text never becomes execution authority.

## 5. Hooks and event actions

Report each supported event's native name, phase, correlation, ordering, lifetime, output/veto and installation readback. Distinguish `native_blocking`, `native_observational`, ELIOT-only `wrapper` and `external_hint` needing confirmation.

Rust `swarm hook emit` or direct adapter ingress authenticates its setup-issued source scope. The callback records bounded facts and returns; it does not run an auditor, Python, GitHub write or model inline. Only a matching enabled manager automation schedules those effects.

Mandatory action authorization is not optional automation. A genuine blocking hook may evaluate bounded local policy before its protected effect. Async after-hooks cannot undo an effect. Optional telemetry backlog must not wedge compaction or productive work.

Hook installation preserves existing user/managed entries through preview/apply and readback. No hidden service restart, global hooks-path change, PATH edit or interpreter installation. Installing the observer does not enable associated actions.

Qualify the actual installed interface: Gemini CLI support does not establish Spark support; public OpenCode plugins are not automatically this project's V2 contract. Forge suppresses uncontrolled Git hooks and exposes Rust before/after action events. Missing mandatory protection blocks its action, not the fleet.

## 6. Rules and event loops

Rules use small typed predicates and registered actions. No free-text method selection, executable templates or implicit dispatch from mentions. New rules are disabled unless the manager explicitly enables them in the save request. Simulation is read-only.

Rule/event and preset call sites use the same per-automation enabled check, not a second global stage gate. Record source cause, parent invocation and action slot. Ignore own descendants by default, reject configured cycles and contain dynamic loops with scoped dedupe/capacity. Meaningful changed evidence can permit progress; repeated identical failure produces one local diagnostic, not another agent asked to explain itself.

While disabled, retain useful source facts/current state, not one runnable command per old event. Re-enable defaults to future events; explicit current-eligible selection is available. Do not replay an entire old transcript after editing a rule.

## 7. Cron and reminders

Extend the existing Rust scheduler and Store, preserving legacy receipt identity. Indexed per-definition/due records replace expansion of the old shared schedule blob; no second scheduler database.

A schedule is a manager-owned automation with an enabled flag, calendar, action and scope. There is no extra project mode or stage flag. Use a maintained complete Rust cron evaluator plus one supported timezone implementation. Preview grammar, next occurrences, DST gaps/repetitions, overlap and catch-up honestly; no handwritten parser or frozen release.

Occurrence identity includes schedule identity/generation and due UTC instant, not actual jitter/start time. Updating active scripts/profiles does not rerun considered occurrences. Default missed-run handling is latest-only; bounded replay is deliberate and only for repeat-safe work. Future calendar/tzdata changes must not duplicate retained occurrences.

Authorized `schedule.run_now` is one manual run even when recurrence is disabled. Resume after intentional disable follows the manager's selected future/current policy; normal restart recovers saved enabled entries and their cursors without an obligatory approval round.

Reuse #22's shared watches and timer indexes. Explicit one-shot notices work without enabling a recurring automation. A recurring nudge or reminder that invokes a model is a separately chosen typed automation action, not an implicit consequence of notification delivery.

## 8. Server Goal

ELIOT `goal.*` references existing assigned Tasks and completion evidence. Native `agent.goal` remains adapter-specific. Goal state/evaluation is not another Task graph.

A tracked objective alone starts nothing. Its automatic progression is governed by its one manager-owned enabled execution definition. Goal-specific UI edits that flag through the same configuration path; do not store conflicting enabled values in two systems.

Dispatch, review, repair and continuation are limited to the manager's configured actions and rights. Achievement uses exact required evidence, not commit volume or a model saying done. Unmachine-checkable completion uses the assigned evaluator.

Exactly one continuation owner per assignment is manual, server or native. This is a lifecycle fact, not a global operating mode. Changing native continuation requires supported clear/pause and actual readback. Unknown old input/children prevents competing continuation, not unrelated work. Disabling further progression preserves current work, results and monitoring.

## 9. Optional scripts

`script.register/revise/validate/activate/run/get/list` manages named Python/PowerShell bundles through Rust. Activating content selects future runnable bytes; it does not start a script. A manager may run it manually or enable a cron/hook automation that selects it.

Resolve the active bundle, support files, installed interpreter/environment, schemas and trust for each new run. Retain the admitted snapshot. No mutable imported-file substitution halfway through a run, runtime install in a callback, or fixed interpreter/dependency release requirement.

Use JSON stdin, separate argv and installed noninteractive/no-profile PowerShell without default ExecutionPolicy Bypass. Drain bounded stdout/stderr and own descendants. These options and Job Objects/process groups provide hygiene/lifecycle containment, not filesystem/network isolation.

`trusted_local` executes with real selected OS-user rights. `isolated` requires actual supported enforcement; no silent downgrade. Run-scoped API credentials allow only that invocation's declared effects, on behalf of its owner, without exposing a manager token. They cannot edit automation ownership, expand rights or enable a new recurring workflow.

A manager may authorize bounded child actions as part of an invocation. Disabling prevents new separate automatic stages; already-started native/OS activity remains in-flight until observed complete or explicitly cancelled. Unknown remote effects or surviving writers hold the affected scope; exit zero alone does not prove all intended work succeeded.

## 10. MCP, persistence and recovery

Use #22's role/surface/catalog separation and small eager cores. Deferred groups contain automation config, runtime profiles, streams, review, hooks, scripts, schedules, Goals and forge. No `automation.control.*` mode-management group and no arbitrary execute-method tool. Names and output identify the real owner and enabled entry.

Keep existing SQLite/Store, Operations, Observations and artifacts. Add indexed automation ownership/revision/enabled, review/due/action-slot metadata only as needed. Forward migrations preserve historical policy, old receipts and evidence; no second job engine or full-history scan per viewer.

Crash recovery preserves explicitly saved enabled/disabled settings. Manager-client disconnect is not loss of management identity. Recheck owner permissions, targets and unresolved effects before continuing; never automatically pause all entries because the host restarted. Unattributable imported definitions remain disabled until a manager adopts them, which is different from recovering an existing owned entry.

Rights revocation blocks the affected new effects. Explicit handover validates the new manager and preserves old invocation attribution; a new owner cannot adopt an old epoch-fenced possibly sent publication by renaming it. Never pick Root as fallback owner.

Independent native Goals, queued remote merge/workflow requests and trusted-local processes can outlive local disable. Keep them visible and reconcile read-only; supported cancellation is a separate manager choice. A failed label update does not repeat push, and a late result does not affect a newer candidate.
