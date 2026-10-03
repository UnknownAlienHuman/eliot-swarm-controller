# Agent Operations: Rust Architecture and Service Contracts

Revision 3 · 2026-10-03 · source baseline `504199d14135c030ad3951a3c5023a098a3d03f0`.

[Configuration](configuration.md) defines manual/assisted/delegated control. [Delivery](delivery.md) defines work transitions. [Donor map](donor-map.md) distinguishes inspected evidence from design. Proposed methods are not claimed implemented.

## 1. Rust boundary and manual-first operation

All ELIOT-owned internal logic is Rust:

```text
host / Store / authorization / configuration / control
native adapters / transport readers / process supervision
monitoring / dashboard projections / streams
Git / GitHub API / webhook validation
assignment distributor / review router / publication controller
hooks / actions / cron / Goal / reminders
MCP facade / gateway / CLI / external-script runner
```

Python/PowerShell are optional external extensions. Normal startup, manual or automated delivery and recovery must work without them. No internal Node gateway, shell merge loop or Python GitHub daemon is introduced.

Vendor executables remain external products. Rust adapters use documented HTTP/SSE, WebSocket, MCP/ACP or framed stdio interfaces and suitable Rust libraries. Existing owned Python/JS/TS bridges are migration inputs. A feature accessible only through a non-Rust custom plugin is a named integration gap, not permission to hide another owned scripting subsystem or copy commercial SDK internals.

Accept newer compatible runtimes by protocol/capability evidence. No software-version pins or automatic downgrade to make an old document true. Adapter changes affect future bindings; existing work retains its lifecycle owner until a supported handover.

Manual mode is the first-run state and a complete path through the same services. It means the manager chooses assignments and workflow transitions, not that the native model asks permission for every token/tool or that safety checks are bypassed. Native permission/Goal behavior is separately configured and reported honestly.

## 2. One admission path, two execution authorities

Separate observation, deterministic decisions and effects:

```text
native/source event or deadline -> validated fact/current projection
                                  |
                    manual mode: next action is visible, not executed
                                  |
                  explicit manager command OR enabled delegated stage
                                  |
             current scope/control/grant/identity/resource checks
                                  |
          existing Operation and semantic action slot committed
                                  |
                 Rust effect worker outside Store transaction
                                  |
                 retained result and actual resource disposition
```

An invocation is an existing Operation, not a second job state machine. Metadata links cause, selected definition, control/grant revisions, targets and outcome references. Delivery phases are projections.

Permission alone is not a trigger. Automatic execution requires a manager-activated control scope, selected stage, selected trigger definition when applicable, a current standing grant and actual capacity. A saved configuration, pool import, completed review, larger queue or model request cannot enable it.

Direct authorized manual operations remain callable with automation disabled. A manual launch includes its declared finite prerequisites such as workspace preparation and selected runtime start. It does not authorize an endless pool-consumption/review/repair/publication loop. Subsequent workflow stages are independent decisions.

### 2.1 Provenance and dedupe

Origin is derived from trusted admission, not accepted as a caller-supplied `manual=true` flag. Distinguish direct commands from an authorized management session, explicitly requested notifications/watches, and autonomous rule/schedule/Goal/preset actions.

Run-scoped credentials for automatically launched models and scripts retain that causal origin. A model with a manager-like job title cannot relabel its new downstream launch/publication as a direct control-manager command. It may finish its already assigned work and report evidence, but new workflow effects remain subject to current control. Management-control credentials are not silently inherited by worker sessions.

Retain request identity and domain-level uniqueness. Examples: one launch slot for the current Task/Attempt, one review slot for `(submission_ref, policy_generation, slot)`, and one publication intent for its exact candidate/target. Manual and automatic callers use the same logical slot. Origin or incidental configuration revision must not produce duplicate work. A slot may reference serial explicitly superseded attempts; at most one may start an effect at a time.

Automatic event consideration uses `(scope, semantic cause, action slot, deliberate replay generation)`. Editing a rule does not replay past events. Explicit rerun is a new authorized decision after prior outcome is known; it cannot conceal uncertain delivery.

A new invocation resolves active definitions once and records what it used. The same request ID after a settings change returns the prior receipt. Future independent work may use updated libraries, profiles and scripts; retained identity does not freeze future software.

### 2.2 Control/effect ordering

Use the existing single Store owner and short transactions. Add the smallest indexed control/current-start linkage needed; do not introduce another coordinator or custom distributed lock service.

Before an autonomous worker crosses the effect-start boundary, it revalidates effective control, selected stage/definition, grant, expected candidate and ownership. The transition to effect-started and a pause/manual change serialize through Store. Only then may it perform the corresponding I/O outside the transaction.

If pause commits first, an unstarted automatic action remains held and cannot use its old admission permission to start. If effect-start commits first, the pause result lists that action as in-flight: a network write may reach the remote after the local pause returned. Do not claim instantaneous rollback of already-started effects.

The started action may finish its originally authorized bounded work, record its outcome and reconcile uncertainty. Its completion cannot automatically admit the next disabled stage. Each independently effectful child of an automation script/preset/model rechecks control; it cannot inherit unlimited start permission from an old parent.

A held condition is a projection over the existing queued Operation/control state, not a fabricated success or another authoritative execution state machine. Manual and automatic starts contend for the same slot. A second caller gets the retained in-progress/result reference or a precise conflict, never a second writer/auditor/push.

### 2.3 Returning control to the manager

A restrictive local control change must work without GitHub, a model, script execution or a successful configuration-file reload. It returns control revision, held unstarted actions, started actions, uncertain effects and external continuations. Native readers, result ingestion, read-only reconciliation and dashboard remain active.

Do not kill processes, clear native Goals, cancel GitHub requests, delete worktrees or release occupied leases as a side effect of setting manual/paused. Offer exact supported addressed operations separately. A trusted-local script may still perform OS effects while draining; API permission revocation alone is not a process sandbox.

Expose `draining` while previously started actions remain, and `external_continuation_unresolved` when a native/remote owner can still continue. Report quiescent manual control only after relevant outcomes/owners are observed, without inferring death from silence. Local manual mode alone cannot disable independently configured GitHub workflows or recall accepted remote auto-merge.

### 2.4 Manually execute held work without deadlock or double execution

The manager's normal typed action may name `expected_pending_operation_id` from the pending-action view. This is not a generic run-any-method tool. It requests a one-shot takeover of the same logical action slot, using the method's ordinary input, candidate guards and manager authority.

Inside one Store transaction, verify that the old automatic Operation still owns the slot and is provably never started. Cancel/supersede that unstarted attempt through the existing guarded cancellation path, append the new manual Operation and atomically link the slot to it. Retain the original request/receipt and an explicit successor reference; do not edit the original caller or claim it was manually authorized all along.

A racing old worker's start check must fail against that updated slot/control revision. Retrying either request returns its own retained lineage. This is one serial replacement, not a new slot that bypasses duplicate protection. All normal manual effect checks still run.

If the prior operation crossed effect-start, is sending or has unknown outcome, takeover is rejected with its exact operation/readback reference. If it already completed, return that fact rather than execute again. Queued replacement cannot bypass stale GM epoch or acceptance rules. No new request identity is ever used to repeat a possibly applied external effect.

Implement this once in shared admission/cancellation internals and exercise it from review and publication; merely returning a held automatic receipt forever would leave manual mode unusable.

## 3. Continuous observation in every mode

| Source | Rust path | Limit |
|---|---|---|
| Native stream | Shared adapter reader for a supported connection/scope | Exact binding/root/child/generation; shared-server traffic is not every manager's traffic. |
| Native snapshot | Shared bounded read-only reconciliation | Partial absence is not termination. |
| OS processes | Recorded identity plus selective sampler | PID alive is not model progress; query failure is not an empty process set. |
| Git | Shared hints followed by object/status reads | File event and commit author are not current assignment ownership. |
| GitHub | Authenticated intake and conditional paged reads | Assignees/labels/comments are external facts, not execution authority. |
| Store | Committed Task/Attempt/Operation/submission facts | Admission is not completion. |

Configured-source observation continues in manual mode. Source setup is still explicit; opening a local dashboard does not install a GitHub App, hook or remote service. Use events first, shared paced read-only polling when necessary. Never query a model for status or invoke a health CLI that may restart its service.

Recorded process identity includes start/boot/ownership facts. Parent idle, wrapper exit, old mtime and transport loss do not prove that children or native Goals stopped. No heuristic death grants a second writer the same workspace.

### 3.1 Dashboard

Join project/repository/Issue/source revision; Task/Attempt/assignment/submission; manager/executor/auditor; binding/native root/child/run/turn; worktree/branch/candidate; and separate process, connection, execution and delivery states.

Include current control mode/owner, selected automatic stages, manual holds, paused/draining state, pending manual decisions and external continuations. `awaiting_manager` is not a code defect, timeout-based consent or trigger to prompt that manager repeatedly. A deliberately manual scope with idle agents is not broken automation.

Expose last transport activity separately from material progress, active tools/questions/children, CPU/RSS, account-capacity basis and coverage/gaps. Preserve unknown native statuses instead of mapping them to success. Dashboard reads projections, not serial queries to all agents.

### 3.2 Snapshot, delta and fairness

Return a consistent committed cut plus controller cursor. Preserve upstream cursors independently; do not promise a globally atomic GitHub/native/OS view. Existing per-subscription committed-fact polling is migrated to a shared Rust projector while preserving lag/resync.

Live content has a separate presentation sequence. Bound queues and serialized bytes. Slow viewers receive gaps and resync; they cannot block terminal recording, native permission replies or manager control commands. CPU parsing/OS enumeration use bounded blocking workers. No Store transaction waits for network, scripts or models.

## 4. Text and reasoning streams

`stream.open/read/close` select an authorized binding/assignment and content classes. Opening a stream observes; it never starts a model. Support bounded cursor reads and negotiated notifications.

Expose only content deliberately provided by the native interface: assistant text, reasoning summaries or native reasoning text when supplied, tools, lifecycle and usage. Do not decode hidden/encrypted state or manufacture a reasoning transcript. Mark unavailable/redacted/not-retained content explicitly.

Keep native item/part identities and ordering. Final items supersede matching deltas without double counting. Cumulative usage replaces prior cumulative observations; an unknown billing basis stays unknown.

Use shared bounded rings and configured capped artifact chunks, not a full Observation per token or per viewer. Protect secrets spanning chunk boundaries; until streaming redaction is qualified, buffer bounded logical records or omit the sensitive class. Source/reasoning/tool text never becomes a rule or authority. Retention protects active/referenced evidence by ownership, not mtime.

## 5. Hooks are observation unless an action is explicitly enabled

Every installed adapter reports supported native event, phase, correlation fields, lifetime, ordering, veto/output behavior and installation readback. Distinguish `native_blocking`, `native_observational`, ELIOT-only `wrapper`, and `external_hint` requiring verification.

The Rust `swarm hook emit` helper or adapter ingress authenticates setup-issued scope and bounds input. Observational hooks enqueue facts and return; heavy review/script/publication is never inline. In manual mode a commit/tool hook updates visible state but does not dispatch an auditor or execute a user script.

Mandatory pre-effect safety checks still apply to manual actions. They are part of the action's authorization, not optional workflow automation. A supported blocking hook uses short local policy, not a model/remote script/Store-lock cycle. Async after-hooks cannot undo effects.

Install through preview/apply preserving user and managed hooks. No implicit service restart, PATH change, interpreter installation or global hooks-path replacement. Enabling hook observation does not enable any associated rule. Optional telemetry loss is a gap and must not wedge compaction.

Actual vendor capabilities remain separate: public OpenCode plugin docs do not prove the installed V2 contract; Gemini CLI is not Gemini Spark; Command's existing listener is observational. Missing optional coverage affects its source only; a missing mandatory protection blocks only the protected operation. Forge keeps controlled suppression of arbitrary Git hooks and exposes Rust action events instead.

## 6. Rules and causal loops

Rules use typed predicates/mappings and registered action kinds, not executable strings, arbitrary method selection or prompt-derived authority. Create/save defaults inactive. Simulation is effect-free. Enabling requires manager control over the named definition and its action stages; rule edits cannot unpause or expand the envelope.

Persist source/cause/parent/action-slot ancestry. Ignore self-descendants by default, reject visible cycles and bound dynamic causality/rates. No automatic explanation agent for failed notifications. New evidence may make work eligible; repeated unchanged failure affects only that item/rule and produces one incident.

While manual/paused, retain source facts and coalesced pending decisions, not one runnable command for every old event. Resume chooses `future_only` or a previewed `current_eligible` set and revalidates current state. Neither choice blindly drains an old event backlog.

## 7. Server cron and reminders

Extend the existing Rust scheduler and preserve old receipt identities. Use indexed schedule/due records instead of enlarging the old 64-entry meta blob. New definitions default disabled; stored `enabled` is necessary but not sufficient for unattended starts without applicable manager control.

Use a maintained complete Rust cron evaluator and one timezone integration. Intended calendar semantics, timezone, next occurrences, daylight-saving gaps/repetitions, overlap and misfire are explicit in preview. No software-release pin or handwritten parser. Library/tzdata changes recalculate future occurrences under declared semantics without duplicating already considered UTC slots.

Occurrence identity is schedule ID, logical generation and due UTC instant; jitter/start time are separate. Active script/profile changes affect new runs, not old receipts. The default misfire rule is latest-only; bounded backfill is deliberate and requires a repeat-safe action. Intentional suspension is not a host outage: resume uses the manager's inclusion/misfire choice, not an automatic burst.

A paused schedule can be run once via authorized `schedule.run_now`; that is a manual Operation, not an unpause. Ordinary host new-work/resource guards still apply. Pausing project automation holds unstarted automatic jobs across schedules/rules/Goals, not just the preset distributor.

Shared `coordination.watch.*` produces bounded freshness notices. Explicit direct mail, requested one-shot watches and delivery of already assigned results work in manual mode. Recurring nudges or a reminder that starts model work require separate selected automation authority. A notice does not replace the current Task prompt or restart an old session.

## 8. Server Goal

`goal.*` is ELIOT-owned and distinct from native `agent.goal`. It references existing Tasks and evidence, not another work graph. Create defaults draft/observational; an objective or enabled-looking file cannot delegate execution.

Only a manager-activated Goal definition and applicable stage/grant can advance dispatch/review/repair/continuation. Goal evaluation may update local evidence in manual mode without issuing commands. Success requires configured evidence at exact Task revisions, never commit volume or the model saying done.

Exactly one continuation owner exists per assignment: manual, server or native. Server control being manual does not prove a previously active native Goal stopped. Changing native ownership requires supported pause/clear and readback; unresolved old input/children blocks only competing work on that scope.

Pause/cancel affects future admissions. Active child cancellation is separate and addressed through the lifecycle owner. Preserve partial work, continue observation and do not infer permission to terminate from a timer or manager disconnect.

## 9. Optional external scripts

`script.register/revise/validate/activate/run/get/list` manages named Python/PowerShell bundles with a Rust registry/runner. Activating content chooses the version for a future invocation; it does not start a script or attach an enabled trigger.

Each new run resolves active bundle, support files, installed interpreter/environment, input/result schema and trust profile. Retain that execution snapshot. No mid-run imported-file mutation, software pin or package installation from a timer/hook. Environment preparation is a separate setup action.

Pass JSON stdin and fixed separate argv; never interpolate event/Issue/branch text into code. PowerShell uses installed noninteractive/no-profile file execution without default policy bypass. Those options are hygiene, not isolation.

Drain bounded stdout/stderr, validate typed result/evidence and own descendants. Short-lived invocation tokens retain the original manual/automatic lineage and permitted child effects. A manual script invocation can run its declared bounded work; it cannot activate a recurring workflow. An automatic script cannot bypass an off stage via a child MCP call.

`trusted_local` has the selected user's real filesystem/network rights. `isolated` requires actual OS/container/VM enforcement; no silent downgrade. Job Objects/process groups contain lifecycle, not permissions. A running trusted-local script may continue OS effects after local automation pause; show it as in-flight and offer exact authorized cancellation, not a fictitious rollback.

Agent authoring/content activation is permitted inside a granted project envelope. Enabling unattended triggers remains a manager decision. Unknown effect or surviving writer descendants holds only the affected mutation scope. Exit zero alone does not prove completion.

## 10. Roles and MCP

Separate principal, finite capability bundle, object scope, standing grant, effective control, MCP profile/surface, current GM designation and OS identity. Presets include manager, executor/Participant, auditor, observer and GM candidate/operator; several auditors are normal.

Only current management authority over a scope can activate/widen control. Configuration preparation can be delegated without that right. Custom role labels and a negative not-observer check are insufficient. Role/grant revocation invalidates cached authorization for new effects; it does not replay uncertain work under another identity.

Current GM/operator feedback/acceptance/publication paths remain available. Planned narrow delegated branches use the same exact candidate checks. Manual requests do not need an active automation preset; reviewers do not receive GM credentials.

Use #22's small cores and deferred groups. Add `automation.control.get/preview/apply` alongside config, runtime profiles, streams, review, hooks, schedules, Goals and scripts. Disabling automation does not remove manual tools, dashboard or streams. Search/loading affects schema presentation, not permission or activation.

Reads do not launch work or change settings. Static catalogue listing should not require every executor to be healthy. Cached/manual tool calls still pass application authorization. Output includes effective mode and exact reasons; deliberate manual waiting is not repeatedly escalated as failure.

## 11. Persistence and recovery

Reuse SQLite/Store, Operations, Observations and artifacts. Forward migrations add only needed indexed configuration/control, source, review, timer and effect-linkage data. No second broker, queue authority or state-machine copy. Do not rewrite historical policy/evidence.

Manual is the default on new or unattributable restored state. Preserve explicitly configured legacy intent only when its source/authority is established; otherwise import definitions inactive and expose a manager adoption preview. Do not destroy old schedule receipts.

Intentional delegation can survive manager-client exit. Host restart follows the manager's recorded `resume_after_restart` choice, default false. If true, recheck control/grants/targets and reconcile prior effects before new starts. If false, pause unattended execution while retaining readback and manual controls. Files/templates/plugins cannot clear that pause.

Record late results in every mode. Readback of an already sent push/merge continues after disable, but a label-update failure does not restart publication and a finished action does not trigger an off next stage. Any external auto-merge/native continuation remains visibly owned by that external runtime until confirmed complete or explicitly cancelled.

The product succeeds when small teams can operate entirely manually and larger teams can delegate only what they choose, without sacrificing observation, direct collaboration or exact work/effect identity.
