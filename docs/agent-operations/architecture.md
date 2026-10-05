# Agent Operations — Rust Architecture and Execution Contracts

Revision 13 · 2026-10-05 · source baseline `821251f5e59e88df38de9659393a50f4c0546984`.

[Configuration](configuration.md) owns editable settings; [Delivery](delivery.md) owns work transitions; [Donor map](donor-map.md) separates source evidence from proposals. These contracts are not implementation claims.

## 1. One Rust system

All owned host/Store, authorization, configuration, native transport/adapters, supervision, monitoring, GitHub, hooks, distribution/review, cron, Goal, MCP/gateway and script-runner logic is Rust. Python/PowerShell are optional external extensions. Vendor binaries and native Git remain external tools reached through typed Rust boundaries.

Reuse maintained complete Rust libraries where suitable; qualify installed protocols/capabilities rather than prescribe a fixed release. Existing owned non-Rust bridges are migration inputs. Unsupported non-Rust-only integration remains an explicit gap, not invented parity or copied private vendor internals. New adapter settings do not restart existing work implicitly.

### 1.1 Universal transactional kernel and adapters

The system is a general transactional framework: a kernel, durable event bus,
commands, actions and messages. Agent work is one application of that framework.
The kernel must not depend on a particular provider, harness, model, session or
Task workflow to persist and route an occurrence or report an action outcome.

An event records an occurrence with stable source and occurrence identity,
kind, version, time, applicable scope and causal links. A command requests an
action; its durable Operation records admission, retained input and observed
outcome. A message is scoped communication carried by the same kernel. A
notification or accepted command is not evidence that its action completed.
Task, review, hook, script and provider lifecycle events are applications of
these contracts, not separate execution engines.

Each provider/harness has an adapter that translates common commands into its
concrete API, protocol or CLI instructions and translates native observations
back into common events and outcome facts. Preserve native identity and
capability evidence where required, but keep scheduling, ownership, action
deduplication and recovery in the shared kernel. An unsupported translation
returns a bounded capability/error result through that same contract.

Acceptance, execution, script completion and host termination are distinct
occurrence phases. An accepted provider command must remain available to a
statusless event rule without acquiring completed status. A completed ScriptRun
or graceful host exit establishes only that subject's lifecycle result. Neither
establishes Task completion. Native receipt details are not event-selector
metadata; their size must not silently remove a valid occurrence from the bus.

Rejected, cancelled and uncertain Operation outcomes are kernel facts, not a list of
provider-specific failure callbacks. SQLite migration `010` captures an
Operation inserted in `rejected` or `outcome_unknown`, and each transition into
either state, in the same transaction as the state change. The normalized
projection is a closed phase/status/error-category tuple keyed by the exact
Operation and phase; it excludes request/result content and detailed error
text, and it does not backfill old rows. Adapters may alias a verified raw
outcome to that same phase and occurrence. A raw `runtime.outcome` of `unknown`
aliases `operation.outcome_unknown`; it is not a completion event. Duplicate
views of one phase collapse, while distinct phases remain separate facts.

Migration `011` captures new `cancelled` Operations and transitions into that
state by the same transaction rule. The occurrence names the cancelled target
Operation, independently of the command requesting cancellation. Its bounded
`operation_cancelled` / `cancelled` projection enters ordinary ScriptRun routing
without requiring a Task or Attempt. Installation does not backfill historical
cancellations; identical requests and repeated reconciliation preserve one
occurrence and its retained cause.

Adapters preserve bounded native failure diagnostics in the common durable
outcome. OpenCode retains the verified terminal lifecycle stage and code for
the exact root or child execution; Command Code projects a validated native
exit category into Operation details. Antigravity treats a failed input stream
as an uncertain transport effect, settles each pending send once and blocks
further writes to that stream. Raw native errors are not selector metadata.
Current authorized managers obtain retained details through ordinary readback.

These normalized observations are the framework event bus. Addressed mailbox
messages remain durable delivery to their recipient, with one raw mailbox
delivery on the public timeline; message lifecycle observations do not repeat
that delivery through `report.delta` or `message.read`. Operation-linked
observations are scoped through the existing Operation ACL before paging and
their exact link is checked again after paging. Event metadata does not expose
the retained diagnostic: an authorized current manager obtains detail through
the ordinary scoped Operation read path.

### 1.2 Transaction and load boundaries

Admission, semantic reservation, committed event identity and the associated
cursor/pending state share the relevant short Store transaction. Perform native
or network I/O outside it; then persist its confirmed or uncertain outcome with
the applicable identity and authority checks. An uncertain remote effect stays
uncertain until exact readback; restarting a host or changing managers does not
authorize a second effect or rewrite its original actor.

For committed `rejected`, `cancelled` and `outcome_unknown` state, the Store-level capture
also commits in that transaction. A speculative exception is not an observed
terminal fact, and opening the Store does not synthesize historical failure
events. The bounded event is suitable for an enabled manager rule to select;
diagnostic access still uses current scope and Operation read authorization.

Notifications wake readers of committed records. They are not delivery
authority. Bounded pages, fair dispatch, output limits and explicit backpressure
must prevent a slow adapter or consumer from blocking unrelated subjects.
Preserve exact held occurrences across restart and authorized ownership
transfer. No in-memory-only handoff, global effect lock, per-provider workflow
engine or inferred exactly-once remote execution is part of this contract.

An entry-local admission integrity failure holds that entry's exact cause and
revision in its existing durable journal while unrelated entries progress.
The current manager can read the bounded reason and pending/history through
`automation.config.explain`. A valid replacement revision can release the hold
through ordinary revalidation; changing the manager does not discard the cause.
Store-wide failures remain visible failures of the supervisor.

A restart may temporarily invalidate an adapter's current binding while its
previous confirmed observations remain valid historical facts. Reconciliation
must retain those facts, defer that entry with a readable bounded reason and
continue unrelated entries. Restoring current authority permits ordinary
readback; it does not authorize another external challenge or action. Supervisor
failure retains the fixed supervisor identity when available, including task
join failures, without broadcasting a panic payload.

Script triggers use this same bus: the manager may select any system event
kind, including future kinds. Event visibility and the action's current rights
are checked independently. A safe projection preserves occurrence/causal
identity without passing private message bodies or credentials to scripts.
Legacy and normalized views of one occurrence must not produce duplicate
actions. These are requirements; current implementation and qualification
boundaries remain in [Implementation Status](../implementation-status.md).

The native-MCP failure adapter is another ordinary ScriptRun event source. C7
readback retries and C8 tools retries/stale-launch holds commit a bounded
`controller:native-mcp` / `native.mcp.failure` fact beside the existing retry
marker or schedule transition. The normalized status is `failed`, or `unknown`
for an explicitly unknown native outcome. Its occurrence identifies the launch
Operation, failure supervisor/kind, and attempt: a later attempt is a distinct
fact, while a repeated unchanged stale hold without an established launch
identity is emitted once. The persisted `retry_wait` deadline remains the
scheduler's gate; selecting the event does not cause an immediate retry.

These observations are historical facts, not continuing authority. Before an
unstarted ScriptRun proceeds, the shared event path rechecks the exact retained
projection, selected rule, current Manager/GM, linked Operation visibility, and
current Task/Attempt scope when present. Safe event metadata can trigger the
ordinary configured `script_run`; detailed diagnostics remain behind current
Operation read rights. Failure or unknown status never establishes Task
completion.

Committed HookSource setup and revocation also use this common path. Their
adapter checks the exact operationless observation against the retained source
and client registration, then exposes only status and occurrence identity.
Current Manager/GM and project scope govern script admission. A setup fact
remains historical after revocation; selecting it does not restore the source
credential. No synthetic Operation, Task or Attempt is required for either fact.

An adapter can reject admission before its external effect. The workspace Git
adapter checks normalized metadata path lengths before `worktree add`; the
Store recognizes its exact path-limit failure as closed only for a queued,
unbound launch with a preparing lease. It persists a blocked launch, settled
Operation, stale lease and `not_attempted` evidence in one transaction, using
the ordinary launch-progress event writer. The Manager receives the exact code
and an actionable configuration correction through scoped readback. A retained
uncertain launch stays uncertain even if a later check reports that same code.

## 2. Same action handler for both callers

```text
manager's direct command -----------------------------+
                                                     |
verified fact/time -> enabled manager-owned entry ----+
                                                     v
                        common action/object/resource checks
                                                     |
                            retained Operation and semantic slot
                                                     |
                        owned Rust worker; I/O outside Store
                                                     |
                               observed result/evidence/readback
```

A manager enables selected actions once within their existing rights. There is no global operating mode, extra stage gate, automation-specific approval authority or separate workflow Task store. Direct manual actions need no automation object.

### 2.1 Trusted on-behalf context

Keep technical requester/executor, effective manager, automation/revision, semantic cause, Task/Attempt/submission/target and actual evidence producer distinct.

Construct the internal execution context from authenticated admission or retained Store records. It is not deserializable from public `run_as`, role or manager-ID fields. The context resolves the manager's current action/object rights and intersects them with the entry's configured actions/targets and ordinary host/project/resource/candidate policy. Do not mutate a Principal or rewrite an Operation's caller to impersonate the manager.

The existing internal Scheduler ownership exception in `Principal::owns` is specific to the original scheduled-check path. It must not become a blanket bypass for manager-owned launch, feedback, acceptance or forge actions. Use the shared authorization evaluator, preserving historical scheduler receipts without widening their rights.

Current GM/epoch, accepted-candidate and repository checks remain real guards. Where new scoped manager rights are intended, add them for manual and on-behalf requests together and update the accepted policy explicitly. Reaching an internal function is not permission. Historical policy/Attempt evidence remains readable.

An actually appointed AI manager has management rights for its scope regardless of launch origin. An executor, auditor or script is not promoted by a profile name. Assigning an audit on behalf of a manager does not make that manager the author of the auditor's verdict.

### 2.2 Receipts, slots and reads

Keep the existing `(caller_id, client_request_id)` receipt semantics. Technical automatic requests use a stable internal requester plus request identity derived from the owning entry, semantic cause and action. Record the on-behalf linkage in the same transaction. Preserve original/effective input and do not re-resolve them on identical retry.

Domain uniqueness also spans callers. A review semantic slot is `(manager_id, task_revision, attempt_id, submission_ref, candidate_ref, action, review_slot)`; record the policy generation and use it as a stale guard, but do not make it a way to free/reoccupy the slot. Manual versus automatic, transport, automation ID, random cause/request IDs or cosmetic policy/configuration edits cannot create another slot for the same effect. Identical choices return the retained operation; incompatible choices expose the exact reservation/conflict. Distinct non-conflicting automations can observe the same scope without one global Task lock.

Manager `operation.get/list`, dashboard, cancellation and explain paths must authorize the linked on-behalf work, not only `operation.caller_id == manager`. Filter by current object rights before paging; do not expose every service-requested Operation. Auditors read their assigned review/evidence. This linkage must be implemented with the first automatic caller, not postponed to UI work.

### 2.3 Durable dispatch, not notification delivery

There are two short atomic cuts:

1. Native/GitHub/hook intake durably records the verified fact and its source identity before acknowledging durable acceptance.
2. A bounded dispatcher transaction reads committed facts/current state, validates the enabled entry, records a slot reservation/Operation or a retained pending reason, and advances that entry's processing cursor together.

Never advance a processed cursor and merely enqueue work in an in-memory channel afterward. A crash there would lose a task. Commit first; signal afterward. If the signal is lost, startup and shared bounded reconciliation find retained eligible Operations/pending work again.

`watch` and subscriptions carry revision hints, not every event. Read all unseen committed records through a high-water cut using bounded pages. Do not jump directly to the newest event and miss intermediate submissions. A capacity or prerequisite wait retains the exact subject and wake condition before its input is marked considered. Re-evaluate on that dependency/capacity change; no per-agent poller and no rejected Operation every tick.

Operations remain the execution record. Pending indexes/cursors are rebuildable routing metadata in the same Store, not another queue authority. Volatile optional text may be dropped with a visible gap; terminal work and admission evidence must not be silently dropped. If durable intake cannot commit, report the failure so the source can retry/reconcile.

### 2.4 Settings, disable and start ordering

A retained admission keeps its resolved model, script, candidate and action parameters. Before a consequential effect starts, recheck current manager rights, enabled entry, allowed action/target, candidate and resource ownership. Configuration changes never silently reinterpret a queued Operation's payload.

The check and effect-start transition serialize with disable/narrowing in Store. If disable wins, the unstarted automatic action is held. If start wins, its authorized bounded I/O may finish after disable returns; report it as in-flight. No fictitious remote rollback. Each separate follow-up action performs a fresh eligibility/authority check.

Changing a preferred model affects new admissions, not a running or previously admitted job. Removing the relevant action/target blocks that unstarted job. A display-only revision change does not block it. Explicit replacement of a provably unstarted action supersedes the old attempt and reserves its successor in the same slot, retaining both receipts. A stale worker then fails its start check.

Sending/unknown actions require readback before replacement. Disable never kills models, clears native Goals, deletes worktrees or frees live ownership. Those are separately requested supported actions. A trusted-local script can continue OS effects after disable; an API flag is not isolation.

### 2.5 Result ingestion after control changes

Keep recording authenticated results/readback for the already admitted action even when its entry is disabled, its Task was superseded or its manager lost the right to start more work. Mark historical/non-current applicability and never apply the late result to a newer candidate.

This is not a bypass for revoked credentials: preserve trusted adapter/source evidence through its authenticated ingestion path; refuse unauthenticated submissions. A recorded outcome does not authorize a new Task transition, GitHub write or continuation. Original candidate/actor/epoch checks still govern those effects.

## 3. Monitoring without model interrogation

| Source | Shared Rust path | Coverage boundary |
|---|---|---|
| Native events | One reader per supported connection/scope | Exact binding/root/child/generation; shared-server traffic is not every line's traffic. |
| Native snapshots | Paced read-only reconciliation | Missing/partial response is not proof of termination. |
| OS processes | Recorded start/boot/ownership plus selective metrics | PID alive is not progress; failed enumeration is not an empty inventory. |
| Git | Change hints followed by bounded object/status reads | File event/author does not establish assignment ownership. |
| GitHub | Verified intake and conditional paged reads | Comments, assignees and labels are data, not commands. |
| Store | Committed work and outcome facts | Queued admission is not completion. |

Observation works with zero enabled automations. Configuring a source does not authorize scripts, model launch or remote writes. Never use a status CLI that may restart a shared service. Parent idle, wrapper exit, mtime and tunnel loss do not prove native children or Goal stopped.

### Dashboard and cursors

Join source revision, Task/Attempt/submission, manager/executor/auditor, binding/native family/turn, workspace/candidate and separate process/connection/execution/delivery states. Show enabled entries, owner, selected actions, last/next run, material progress, real capacity basis, pending choices and exact gaps. Waiting for a manager is not a source-code defect.

A snapshot returns its committed cut. Changes after that cut remain readable by cursor; subscribe-and-read/recheck must not leave a race between snapshot and subscription. Upstream cursors remain distinct. No promise of one atomic native/GitHub/OS snapshot.

Bound serialized pages, rings and blocking work. Slow viewers receive lag/resync and cannot block native permission replies, completion or manual commands. Dispatch is fair across owners/routes with account-wide capacity; display that basis without claiming estimated cost is a measured quota. Waiting for an auditor retains candidate ownership, not an unnecessary active-model slot.

## 4. Text, tools and reasoning

`stream.open/read/close` observes an authorized work/binding context without starting a model. Expose only native-supplied assistant text, reasoning summary/text where supported, tool progress, lifecycle and usage. Hidden/encrypted reasoning is unavailable.

Preserve native item/part ordering. Final content replaces matching deltas rather than counting both; cumulative usage replaces earlier cumulative observations. Unknown pricing/cost remains unknown. Use shared bounded rings and optional capped artifacts, not an Observation per token or a transcript copy per viewer.

Redact across chunk boundaries; buffer bounded logical records or omit a sensitive class until the redaction path is qualified. Retention protects active/referenced evidence by ownership, not mtime. No stream content becomes an automation definition or permission.

## 5. Hooks

Publish actual event name, phase, correlation, lifetime, ordering, veto/output semantics and installation readback. Distinguish native blocking, native observational, ELIOT-only wrapper and external hint.

Rust hook ingress authenticates setup-issued scope, records bounded facts and returns. Long review/script/GitHub/model work runs only through a matching enabled entry, never inline. Async after-hooks cannot undo an effect. Mandatory action authorization is not optional workflow automation; blocking hooks may use short local policy, not a remote model or a Store-lock/network cycle.

Install/update via preview/apply preserving existing user/managed hooks. No hidden service restart, global hooks-path/PATH change or interpreter installer. Optional telemetry backlog must not wedge compaction. Forge keeps controlled suppression of arbitrary Git hooks and emits Rust action facts.

Qualify the installed interface rather than infer it from a vendor name. Gemini CLI is not Spark; generic OpenCode plugin documentation is not proof of the V2 route. Missing mandatory protection blocks only the protected action; optional gaps do not disable the fleet.

## 6. Typed rules and legitimate progression

Rules select registered predicates/actions, not arbitrary methods or executable text from events. Create defaults disabled unless the manager explicitly enables the same save. Simulation is effect-free.

Do not blanket-drop events descended from the same automation. `review -> changes -> repair -> new applied submission -> review` is a legitimate selected workflow. Suppress a repeated semantic action on unchanged material state instead. A new random event ID, timestamp, config revision, restated checklist or progress message is not progress; changed relevant source/evidence or a new valid prerequisite can be.

Delivery steps use the transition matrix in Delivery. Provenance detects notifications triggering themselves and rule A/B echoing unchanged state. It is not a global maximum number of repairs or a reason to reject every repeated rule. A script-specific repeat-safe cron occurrence is distinct from a delivery action on an unchanged submission.

Retain one scoped pending diagnostic for an unchanged failure. Do not launch an explanation agent for a failed notification. Configuration validation rejects obvious unconditional cycles; dynamic guards use domain identity and current eligibility. Backpressure and configured budgets do not fabricate task completion.

## 7. Cron and shared reminders

Extend the existing Rust scheduler. Schedules/rules/Goal editors share the same manager-owned enabled entry, without duplicate switches. Use indexed per-definition/due records rather than expanding one shared blob; preserve legacy schedule/receipt identities.

Use a maintained complete cron evaluator and supported timezone integration. Preview grammar, calendar, next occurrences, DST gaps/repetitions, overlap and misfire. No handwritten cron parser or frozen release requirement.

Logical calendar generation changes only when calendar semantics change; toggling enabled, changing a display name/model preference or jitter does not recreate old slots. Identity includes the schedule and intended due UTC occurrence, not actual start time. Persist considered occurrences and invocation identity. Legacy interval slot keys remain recognizable.

Latest-only is the ordinary missed-run policy; bounded backfill is deliberate and only for suitable repeat-safe work. Never restart a possibly sent job from a new timer tick. `schedule.run_now` is one explicit invocation while recurrence may remain disabled. Ordinary restart restores saved enabled settings after readback. Intentional re-enable applies its future/current-work choice, not blanket backlog replay.

Reuse #22 watches/subject indexes. One-shot watch notices work without recurring automation. No per-participant Tokio timer or model loop; a small shared timer index and event-driven wake suffice. A recurring model nudge is an explicitly selected action, not a side effect of a notification.

## 8. Server Goal

`goal.*` references existing assigned Tasks and completion evidence; it is not a second Task graph. Tracking an objective starts nothing. One manager-owned enabled execution entry controls its selected progression.

Achievement requires the configured exact evidence, not commit/token volume or the model saying done. An unmachine-checkable criterion needs the assigned evaluator and remains unknown until evaluated. Do not redefine the objective through peer conversation.

One continuation owner exists per assignment: direct manager control, server Goal or supported native Goal. This is lifecycle ownership, not a global mode. Switching from a native owner needs supported clear/pause and actual readback. Unknown prior input or live children prevents competing continuation on that scope, not unrelated work. Disable preserves results and observation.

## 9. External scripts

`script.register/revise/validate/activate/run/get/list` manages named Python/PowerShell bundles through Rust. Content activation selects future runnable bytes; an authorized direct run or enabled trigger starts an invocation.

Resolve and retain bundle/support files, prepared interpreter/environment, input/result schemas and trust. No mid-run imported-file substitution, callback-time installation or fixed software release prescription. Pass JSON on stdin with separate argv; installed noninteractive/no-profile PowerShell is hygiene, not a sandbox, and does not require default ExecutionPolicy Bypass.

Bound stdout/stderr, validate result references and own descendants. Job Objects/process groups provide lifecycle containment, not filesystem/network isolation. `trusted_local` has the selected user's real rights. `isolated` requires actual supported enforcement; never silently downgrade.

Run-scoped API credentials permit declared invocation effects only. They carry manager/cause linkage, not a reusable manager token, and cannot edit ownership or enable unrelated automations. Started OS activity may finish after disable; new independent follow-ups still need current authorization. Unknown surviving writers hold their relevant mutation scope. Exit zero alone is not proof of all effects.

## 10. MCP and cross-program contracts

Use one registry and #22's profile/surface/catalog separation. Keep normal cores small; detailed config, runtime profiles, streams, reviews, hooks, scripts, schedules, Goal and forge are deferred. No arbitrary execute-method tool or new mode-management group.

The assigned auditor submits only its exact result through `review.submit`; `review.get` and linked `operation.get` may finish/read that same retained assignment after Task revision or Attempt release while authentication remains valid and unrevoked. No historical context/list, artifact or evidence reads are implied. A null preregistered review assignment remains unusable until atomic binding. `task.request_changes` applies manager disposition and is not an ordinary auditor tool. Preserve any explicitly named legacy `Reviewer` compatibility profile separately; neither tool visibility nor a role label grants new rights.

For capability receipts distinguish configured, listed by the transport, acknowledged by the harness, and successfully used. `tools/list` or `list_changed` alone cannot prove a model loaded a schema. Do not start extra model turns to fill a readiness checkbox. Use native inventory or a real harmless call in already requested work where supported; otherwise show the gap. Required missing reporting capability affects that launch, not unrelated tools/agents.

Cache/list metadata by catalogue/profile revision, not process per group. Watch results never change the tool schema set. Cached calls still check current role/object access. Result schemas return exact IDs, next action and coverage rather than verbose full histories.

## 11. Recovery and failure isolation

Keep SQLite/Store, Operations, Observations, artifacts and existing effect owners. Forward migrations add only needed indexes/records for entries, cursors, pending subjects, review slots and occurrences. These are routing/projection data over one execution authority, not an imported workflow engine.

Recovery loads saved entries and current permissions, reconciles sending/unknown work, then resumes eligible starts. An absent owner is not replaced by Root. Revocation blocks new effects while trusted observation/readback continues. Explicit transfer preserves old invocation attribution and does not adopt an old GM-fenced possibly sent publication.

A blocked input, corrupt optional projection, rate-limited account or malformed native event is scoped to its source/subject. Retry pacing follows real failure/capacity evidence, not a model-written status phrase. Do not let a library middleware, adapter and scheduler each independently retry the same uncertain write.

Remote auto-merge/workflow requests, native Goals and trusted-local processes may outlive local disable. Keep actual ownership visible; supported cancellation is a separate manager choice. A failed GitHub label projection cannot repeat publication or return valid code for reimplementation.
