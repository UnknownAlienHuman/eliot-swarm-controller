# ELIOT Swarm Launcher and Assignment Context
## Manager dashboard, compact launch packet, agent self-service, Git overlap and event-driven reminders

**Revision:** 1 — 2026-10-03  
**Source baseline:** `main` at `35e499ae73b622d873c44873f6993ee3fcbea87b`  
**Applies to:** [Communication Program](agent-communication-program.md), [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md), [MCP Tool Catalog and Deferred Loading](mcp-tool-catalog-and-loading.md), [Legacy Swarm Transition](legacy-swarm-transition.md), [Task Policy](task-policy.md)  
**Status:** normative implementation amendment. The named high-level methods are design targets until implemented and qualified.  
**Precedence:** this file governs launch preview, assignment packets, manager/participant launcher UX, queue/overlap projection and reminder/watch semantics.

## 0. Decision

The Swarm Launcher must do more than start a process. It must establish one coherent work context before the model begins:

```text
canonical Task/Attempt snapshot
+ exact runtime route and identity
+ manager-owned workspace/write lease
+ relevant peers and current contracts
+ queue/dependency reason
+ Git/scope overlap
+ small role-specific MCP surface
+ deferred authorized tool groups
+ exact stop/output/evidence contract
+ capability receipt
```

The model then uses the same controller to answer ordinary operational questions without asking Root or reconstructing the fleet from chat:

```text
What exactly is my task?
Who owns the contract I consume?
Who is changing an overlapping path/symbol?
What answer or prerequisite am I waiting for?
Has the contract/scope/operation changed?
Which tool group do I need?
```

The launcher is not another task authority, queue database, Git owner or session scheduler. It is a high-level application projection and admission path over existing ELIOT Task, Attempt, Operation, binding, mailbox, scope and runtime authority.

## 1. Product law

```text
Manager sees the fleet and assigns work.
Agent sees its relevant neighborhood and coordinates directly.
Launcher establishes identity, workspace, tools and facts.
Git corroborates work state; ELIOT owns current assignment authority.
```

The desired normal flow:

```text
manager opens dashboard
  -> selects one ranked work item
  -> launch.preview shows dependencies, owners, overlap, capacity and tool/runtime gaps
  -> manager accepts exact plan digest
  -> launcher revalidates and starts one manager-owned work context
  -> participant calls context.get
  -> reads cards / asks owner / checks overlap / syncs integration
  -> event watch supplies a compact reminder when a named fact changes
  -> result enters normal submission/review/acceptance path
```

No model receives the entire queue, global roster, all messages, all Git history or every MCP schema.

## 2. Existing foundation and current gaps

Current ELIOT already owns:

- TaskSpec objective, phase, requirements, dependencies, scope, acceptance policy, owner-policy ID and source index;
- frozen Task/Attempt policy and source-indexed brief in the Attempt snapshot;
- caller-owned request receipts and durable Operations;
- route configuration and native bindings/generations;
- exact agent open/send/reply/configure/goal/background/readback operations;
- report attention/capacity/delta projections;
- mailbox delivery and cancellation;
- source capture, checks, submission and protected acceptance;
- closed MCP authorization profiles and profile-bound client identity.

Current documented gaps include:

- no first-class durable launch manifest containing route/model/variant/effort/budget/stop conditions;
- no unified manager dashboard or ranked queue projection;
- no compact assignment-neighborhood projection for a participant;
- no controller-owned Git/worktree overlap view;
- no launch-time verification that the runtime actually received required MCP tools;
- no one-shot participant watch/reminder tied to exact durable facts;
- no high-level manager launch method that combines preview, workspace, scoped participant credential and capability receipt while preserving existing authorities.

The implementation must close those gaps without creating a parallel run database or accepting a model's self-report as truth.

## 3. High-level application methods

These are application methods, not MCP-facade macros. The Store/application layer constructs and authorizes their results. MCP, CLI and UI expose them one-to-one.

## 3.1 Common/basic methods

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.consult
coordination.inbox
coordination.watch.create
coordination.watch.list
coordination.watch.cancel
operation.get
```

Semantics:

- `swarm.context.get` returns the caller's current bounded work neighborhood.
- `coordination.send` sends typed coordination information; it starts no model.
- `coordination.consult` resolves an exact relevant owner/expert and creates one bounded question or returns ambiguity/unowned state.
- `coordination.watch.create` creates a one-shot or bounded durable fact watch; it does not create a model timer or active polling task.
- `swarm.tools.search` searches only the hard-authorized deferred catalog.

`launch` is not a participant/basic authority. It appears in the manager surface.

## 3.2 Manager methods

```text
swarm.dashboard
swarm.queue.get
swarm.assignment.preview
swarm.launch.preview
swarm.launch
swarm.agent.inspect
swarm.agent.steer
swarm.agent.stop_request
swarm.exceptions.get
swarm.capacity.get
swarm.overlap.check
swarm.review.context
```

Preferred common manager tools:

```text
swarm.dashboard
swarm.queue.get
swarm.launch.preview
swarm.launch
swarm.agent.inspect
swarm.agent.steer
swarm.exceptions.get
operation.get
swarm.tools.search
```

Lower-level task/runtime methods remain deferred.

## 3.3 Participant/agent methods

```text
swarm.context.get
swarm.assignment.get
swarm.overlap.check
coordination.send
coordination.inbox
coordination.consult
coordination.ask_owner
coordination.answer
coordination.publish_contract
coordination.sync_integration
coordination.peer_agree
coordination.watch.create/list/cancel
operation.get
swarm.tools.search
```

This surface answers the user's required agent questions:

```text
check my task                   swarm.assignment.get / swarm.context.get
send a message                 coordination.send
ask for consultation           coordination.consult / ask_owner
receive a reminder             coordination.watch.*
see who works on this seam     swarm.overlap.check
see current related workers    swarm.context.get
inspect my durable operation   operation.get
load another tool group        swarm.tools.search
```

## 4. Manager dashboard

`swarm.dashboard` returns a compact operational projection, not raw logs or all conversation bodies.

### 4.1 Result sections

```text
host and admission state
active model turns/processes by route
registered/fresh/stale participants
current Tasks/Attempts by lifecycle state
queue summary and oldest/highest-priority work
capacity and provider/runtime gaps
manager exceptions
integration hotspots and scope conflicts
pending submissions/reviews/acceptance
lost/unknown Operations
stale workspaces/leases
recent material progress
coverage and freshness
```

Suggested shape:

```json
{
  "status": "ok | partial | degraded",
  "revision": "dashboard-revision",
  "generated_at_ms": 1780000000000,
  "host": {
    "new_work": "enabled",
    "boot_id": "..."
  },
  "fleet": {
    "registered_participants": 120,
    "active_model_turns": 16,
    "material_coordination_edges": 42,
    "manager_exceptions": 3
  },
  "queue": {
    "ready": 18,
    "blocked": 7,
    "in_progress": 16,
    "review": 5
  },
  "capacity": [],
  "hotspots": [],
  "exceptions": [],
  "recent_progress": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

### 4.2 Drill-down

Dashboard items carry exact Task, Attempt, Operation, binding, scope/cell or workspace references. The manager loads only the relevant deferred group for drill-down.

The dashboard never injects:

- the full participant roster;
- full message bodies;
- full queue rows;
- raw provider logs;
- all Git diffs;
- all tool definitions.

## 5. Queue projection

`swarm.queue.get` is a read projection over existing Task/Attempt/dependency/ownership/capacity facts. It is not a second scheduler or editable issue list.

### 5.1 Row

```json
{
  "task_id": "...",
  "task_revision": 4,
  "state": "ready | blocked | claimed | running | review | accepted",
  "rank": 12,
  "priority_class": "blocker | dependency_ready | nearly_complete | normal",
  "why_ranked": ["unblocks_3_tasks", "owner_route_available"],
  "dependencies": {
    "accepted": [],
    "waiting": []
  },
  "current_attempt_id": null,
  "current_owner": null,
  "related_active_owners": [],
  "scope_conflicts": [],
  "route_candidates": [],
  "capacity": "available | constrained | unavailable | unknown",
  "source_freshness": "fresh | stale | unknown",
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

### 5.2 Ranking rules

Ranking is deterministic and explainable. Inputs may include:

```text
explicit Task priority/phase
dependency fan-in/unblocking value
ready versus blocked state
current owner/lease
known route capability/capacity
scope/workspace conflict
age and starvation prevention
number of remaining requirements when durably known
manager policy edition
```

A model does not invent the rank. The result names why it was ranked.

### 5.3 Freshness

The manager requests a fresh page immediately before launch. A queue page is a snapshot, not a reservation. `swarm.launch` revalidates the exact Task revision, Attempt state, owner, dependency receipts, workspace lease and capacity.

### 5.4 Agent view

The participant does not receive the whole queue. Its context may include:

```text
its own rank/priority reason when relevant
direct dependencies and dependents
blocking contract owners
related active assignments
```

This provides useful orientation without global distraction or information leakage.

## 6. Assignment preview

`swarm.assignment.preview` is a pure bounded read used before claim/launch. It answers:

```text
What is this Task?
Is it ready under the current revision/policy/dependencies?
Which canonical sources are selected or missing?
Which current assignments overlap?
Which contract owners/producers/consumers are relevant?
Which route/profile is suitable?
What would block launch?
```

It returns no new Attempt, workspace, credential or native process.

## 7. Launch preview

`swarm.launch.preview` resolves one exact proposed launch plan without effects.

### 7.1 Input

```json
{
  "task_id": "...",
  "expected_task_revision": 4,
  "route": "codex-writer",
  "agent_profile": "writer",
  "mcp_profile": "participant",
  "mcp_surface": "participant-core",
  "workspace_policy": "manager_owned_worktree",
  "requested_model": "exact-route-model-or-null",
  "requested_effort": "exact-route-variant-or-null",
  "budget": {
    "max_turns": null,
    "max_duration_ms": null,
    "max_cost_units": null
  },
  "stop_conditions": [],
  "purpose": "implementation"
}
```

Route/model/effort/budget fields remain plan facts until a first-class durable launch manifest is implemented. The preview must label fields the adapter cannot enforce.

### 7.2 Output

```json
{
  "plan_digest": "sha256:...",
  "task": {},
  "attempt_action": "claim_new | use_existing | forbidden",
  "workspace": {
    "repository_handle": "project-local-handle",
    "baseline_commit": "full-object-id",
    "worktree_handle": "redacted-handle",
    "branch": "planned-branch",
    "lease": "available | conflict | unknown",
    "dirty": false
  },
  "queue_context": {
    "rank": 12,
    "why_ranked": []
  },
  "peers": [],
  "contracts": [],
  "integration_cells": [],
  "overlap": [],
  "route": {
    "alias": "codex-writer",
    "configured": true,
    "live_qualified": false,
    "capability_gaps": []
  },
  "mcp": {
    "hard_profile": "participant",
    "surface": "participant-core",
    "core_tools": [],
    "deferred_groups": [],
    "catalog_revision": "sha256:..."
  },
  "warnings": [],
  "hard_blocks": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

### 7.3 Hard blocks

Examples:

```text
Task revision changed
Task/Attempt no longer claimable
required dependency not accepted
owner policy unrecognized
selected source gap is launch-blocking under policy
exclusive mutable scope owned elsewhere
manager worktree/branch has unresolved dirty state
route absent or disabled
required core MCP capability unavailable
provider/runtime capacity unavailable when launch would exceed policy
prior launch Operation outcome unknown
```

Unknown Git history or optional peer availability is not automatically a hard block. It remains an explicit gap unless policy makes it material.

## 8. Launch operation

`swarm.launch` is a high-level manager mutation. It accepts the exact preview digest and a caller-owned request ID.

### 8.1 Authority

Manager/current GM/operator according to existing application ownership rules. A Participant cannot call it.

### 8.2 Transaction and external-effect sequence

```text
1. Re-read and validate preview inputs.
2. Verify exact Task revision, policy, dependencies and current Attempt state.
3. Verify one manager/workspace/write-lease lineage.
4. Commit launch intent/Operation and stable plan identity.
5. Outside the Store transaction, prepare or verify the manager-owned worktree.
6. Claim/reuse Attempt through existing authority.
7. Open/reuse exact native binding through existing agent Operation.
8. Register a scoped Participant credential/profile for this work context.
9. Start the runtime through its existing adapter/Operation.
10. Verify actual MCP core/deferred capability receipt.
11. Record launch result/readback or OutcomeUnknown.
12. Return durable IDs; never infer success from process creation alone.
```

The implementation may split this into existing child Operations linked by one launch-plan Operation. It must not make unrecorded side effects inside a SQLite transaction.

### 8.3 Lost response

Identical retry under the caller-owned request ID returns the retained receipt. It does not create a second Attempt, worktree, credential, binding or native session.

An unknown external effect is reconciled through exact readback. Never blindly launch a replacement.

### 8.4 Workspace ownership

The launcher creates or verifies the workspace before the model starts. It does not ask the model to clone the repository or choose a directory.

Required facts:

```text
trusted repository identity
exact baseline commit
manager owner
worktree handle/path kept local
branch/ref identity
dirty/untracked state
write lease generation
allowed mutable scope
cleanup/retention policy
```

The manager owns one worktree/candidate lineage. Read-only researchers/reviewers may inspect it under scoped access. Parallel writers require independent Task scopes/worktrees or an explicit manager decision; peer communication does not grant another write lease.

## 9. Assignment packet

The model receives a compact packet or resource reference, not a copied project history.

### 9.1 Required sections

```text
identity and freshness
objective/non-goals
requirements and acceptance references
canonical source index
workspace and allowed mutation scope
queue/dependency context
relevant peers and why they are relevant
provided/required contracts and integration cells
overlap/hotspot summary
runtime route/model/effort/budget facts
MCP core/deferred capability receipt
output/submission/evidence contract
stop/escalation conditions
coverage and gaps
```

### 9.2 Suggested shape

```json
{
  "packet_version": "eliot-assignment-context-v1",
  "packet_digest": "sha256:...",
  "generated_at_ms": 1780000000000,
  "identity": {
    "task_id": "...",
    "task_revision": 4,
    "attempt_id": "...",
    "assignment_id": "...",
    "owner_policy": "owner-policy-v1",
    "manager_id": "...",
    "binding_id": "...",
    "binding_generation": 3
  },
  "work": {
    "objective": "...",
    "phase": "...",
    "non_goals": [],
    "requirements": [],
    "acceptance_refs": [],
    "source_index": []
  },
  "workspace": {
    "repository_handle": "...",
    "baseline_commit": "full-object-id",
    "worktree_handle": "...",
    "branch": "...",
    "write_lease_generation": 2,
    "allowed_paths": [],
    "allowed_symbols": [],
    "dirty": false
  },
  "queue_context": {
    "priority_class": "blocker",
    "why": ["unblocks_3_tasks"],
    "dependencies": [],
    "dependents": []
  },
  "coordination": {
    "relevant_peers": [],
    "contract_cards": [],
    "integration_cells": [],
    "pending_asks": [],
    "scope_conflicts": [],
    "overlap": []
  },
  "runtime": {
    "route": "...",
    "model": "...",
    "effort": "...",
    "delivery_semantics": "...",
    "budget": {},
    "enforcement_gaps": []
  },
  "mcp": {
    "profile": "participant",
    "surface": "participant-core",
    "catalog_revision": "sha256:...",
    "core_tools": [],
    "deferred_groups": [],
    "tool_search": "native | launcher_assisted | unsupported",
    "gaps": []
  },
  "delivery": {
    "expected_result": "one completed Task submission or explicit retained partial/failed disposition",
    "submission_method": "task.submit",
    "verification_owner": "separate reviewer",
    "acceptance_owner": "manager/current GM"
  },
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

### 9.3 Context limits

Initial model presentation should contain only:

- short objective/non-goals;
- exact current requirements;
- selected source references or compact excerpts;
- workspace/scope;
- relevant peers/contracts/overlap summary;
- five-to-nine core tools;
- exact next action.

Full source text, artifacts, messages, queue and Git evidence are pulled on demand.

## 10. Participant context

`swarm.context.get` is the first normal tool call after launch and after any compaction/reconnect.

It returns:

```text
current identity and stale/fresh state
current Task/Attempt revision
workspace/write lease summary
my work and contract cards
relevant peers and exact edge reasons
required/provided contracts
integration cells and mismatches
pending asks/replies/watches
scope/Git overlap
operation/submission state
tool capability gaps
recommended next actions
coverage/gaps
```

It does not return all agents, all tasks or all messages.

If the participant is stale, result is explicit and mutations remain rejected. It is never silently mapped to a new Attempt.

## 11. Peer consultation

`coordination.consult` is the convenient general form of `ask_owner`.

### 11.1 Input

```json
{
  "task_id": "...",
  "attempt_id": "...",
  "target": {
    "contract_key": "...",
    "path": null,
    "symbol": null,
    "capability": null,
    "actor": null
  },
  "question_kind": "contract | integration | predecessor_fact | review | diagnosis",
  "question": "...",
  "why_needed": "...",
  "blocking": false,
  "expected_answer": "one_fact | structured_contract | recommendation",
  "evidence_refs": []
}
```

### 11.2 Resolution

```text
current structured card answers it
  -> return answered_from_card, no delivery
one exact relevant owner/expert
  -> create one addressed ask
multiple candidates
  -> return candidates and reasons, send nothing
unowned
  -> return unowned; create manager exception only when marked blocking
stale/unknown index
  -> return gap; never broadcast
```

Consultation never becomes peer assignment or automatic model wake.

## 12. Watches and reminders

Agents need reminders without polling or scheduler-driven model work.

`coordination.watch.create` records one bounded predicate over existing durable facts.

### 12.1 Watch kinds

```text
ask_answered
contract_revision_changed
integration_cell_changed
scope_released_or_changed
task_revision_changed
attempt_disposition_changed
operation_terminal
submission_reviewed
owner_available
exact_deadline_reached
```

### 12.2 Shape

```json
{
  "client_request_id": "...",
  "watch_kind": "contract_revision_changed",
  "address": {
    "task_id": "...",
    "attempt_id": "...",
    "contract_key": "operation-admission-v3",
    "expected_revision": 3
  },
  "expires_at_ms": 1780003600000,
  "delivery": "mailbox_header | subscription_hint",
  "one_shot": true
}
```

### 12.3 Semantics

- the watch is durable and exact;
- it creates no dedicated Tokio task and no model turn;
- matching is evaluated when the owning fact changes or by a bounded shared deadline wheel, not per-agent polling;
- one-shot watch settles after one notification;
- expiry settles silently or with one compact expired receipt according to request;
- disconnect does not fabricate delivery;
- `stored/available/presented/consumed` remain separate;
- notification is a freshness hint; the agent rereads authoritative state;
- repeated identical watch fingerprints coalesce;
- a watch cannot start, resume or steer a native session.

### 12.4 Difference from schedules

```text
watch/reminder
  observes a named existing fact
  produces a small notification
  starts no work

schedule
  admits a typed future Operation under scheduler authority
  may create work
```

Do not implement reminders by creating arbitrary scheduled prompts.

## 13. Git and work ownership

Git integration exists to prevent overlap and show evidence, not to replace ELIOT authority.

## 13.1 `swarm.overlap.check`

Convenience read composing:

```text
current Task/Attempt/participant ownership
accepted/proposed code-scope intents
current manager worktree/branch/lease
dirty/untracked/changed paths
optional exact baseline-to-candidate changed paths
contract/path/symbol relation
recent history/blame as separately labelled provenance
coverage and gaps
```

Input:

```json
{
  "task_id": "...",
  "attempt_id": "...",
  "paths": [],
  "symbols": [],
  "contracts": [],
  "candidate_ref": null
}
```

Output distinguishes:

```text
active_owner
planned_overlap
uncommitted_overlap
changed_since_baseline
historical_provenance
unknown_coverage
```

## 13.2 Manager preview behavior

Before launch, the manager sees:

- current owners of related paths/symbols/contracts;
- active worktrees and branches through redacted handles;
- exclusive/shared/read-review scope intents;
- uncommitted overlap where observable;
- unknown Git coverage;
- recommended sequencing or integration-cell relation.

A conflict-free overlap inside a defined producer/consumer seam may be coordinated locally. Competing mutable ownership or a scope expansion is manager-required.

## 13.3 Participant behavior

Before materially expanding planned paths/symbols, the participant calls `swarm.overlap.check` or republishes its work card. It does not need manager permission merely to read the result or coordinate a compatible sequence.

It cannot infer current owner from branch name, commit author or `git blame`.

## 13.4 Git safety

All Git subprocess constraints from the communication program remain:

- trusted repository root from controller context;
- exact recorded full object IDs;
- literal repository-relative paths;
- read-only command allowlist for participant/inspection methods;
- bounded time/output/process tree;
- no network/write/credential/hook side effect;
- private paths/emails redacted remotely;
- incomplete inspection returns `partial/unknown`, never `no conflict`.

Manager write operations remain separate existing forge/workspace authority and are not hidden inside `swarm.overlap.check`.

## 14. Manager assignment presentation

When a manager selects work, the UI/CLI/MCP result should present one concise decision card:

```text
Task and revision
why this is next
objective and remaining requirements
direct dependencies/blockers
current related owners and assignments
contracts this work provides/requires
path/symbol/scope overlap
workspace/branch/lease plan
suggested route/profile/model and qualification state
core MCP tools and deferred groups
hard blocks, warnings and evidence gaps
```

Example text rendering:

```text
Task T-42 rev 4 — ready; unblocks 3 consumers
Owner/workspace: none; proposed manager M2 / worktree W17
Requires: native-input-readback-v2 (owner P7, draft rev 3)
Provides: operation-admission-v3 (consumers P9, P11)
Overlap: src/runtime.rs shared integration seam; no competing exclusive owner
Queue: blocker class, rank 2/18 ready
Runtime: codex-writer configured; live qualification stale
MCP: participant-core ready; git-read deferred; catalog abc123
Gaps: provider quota unknown; one source-index comment retained as nonblocking gap
Next: preview launch, then confirm plan digest
```

The model's prompt receives the compact equivalent. It does not receive the full dashboard.

## 15. Runtime capability receipt

After launch, the controller records the actual observed runtime/MCP surface.

```json
{
  "launch_operation_id": "...",
  "binding_id": "...",
  "generation": 3,
  "native_session_id": "...",
  "mcp_client_id": "...",
  "profile": "participant",
  "surface": "participant-core",
  "catalog_revision": "sha256:...",
  "core_tools": {
    "swarm.context.get": "available",
    "coordination.send": "available",
    "coordination.consult": "available",
    "operation.get": "available"
  },
  "deferred_tool_search": "native | launcher_assisted | unsupported",
  "list_changed": true,
  "presentation": "pull_only | safe_boundary",
  "state": "ready | ready_with_gaps | relay_only | incompatible",
  "gaps": []
}
```

The receipt is not a provider-quality or completion claim. It proves only the observed launch capability state.

If the agent cannot access a required reporting/coordination tool, the launcher fails before productive work or explicitly registers `relay_only`; it does not rely on the prompt to overcome a missing schema.

## 16. Monitoring and status

### 16.1 `swarm.agent.inspect`

Returns one exact participant/binding/work context:

```text
Task/Attempt/assignment identity
freshness and participant grant revision
runtime/binding/native session state
current operation and outcome certainty
workspace/write lease/dirty summary
work/contract cards and integration obligations
pending asks/watches/messages
last material progress facts
MCP capability receipt and gaps
resource/capacity observation
```

It does not infer “stuck” from silence alone.

### 16.2 `swarm.agent.steer`

Manager-only high-level exact steer:

- selects the current binding/generation and current native turn when supported;
- reuses `agent.send` exact-turn semantics;
- requires expected turn ID where the adapter supports it;
- returns stored/accepted/presented/readback disposition honestly;
- refuses or reports unsupported rather than silently queuing into an unrelated future turn;
- is not peer coordination mail.

### 16.3 `swarm.agent.stop_request`

A request, not proof of termination. It uses the runtime owner's existing cancellation/goal/interrupt semantics and records unknown outcome where readback cannot prove disposition.

### 16.4 Exceptions

`swarm.exceptions.get` coalesces one current item per fingerprint:

```text
unowned blocking contract/scope
manager-required agreement
stale/missing affected owner
exclusive overlap
unknown launch/runtime effect
missing required core tool
no-progress loop
security/persistence/identity/lifecycle decision
Concilium proposal
```

Successful asks, card reads and compatible local handshakes do not appear.

## 17. Role and tool matrix

| Capability | Participant | Reviewer | Manager | GM/operator |
|---|---:|---:|---:|---:|
| Own assignment/context | yes | sponsored read | yes | yes |
| Direct coordination/consult | yes | findings/consult | yes | yes |
| Watches/reminders | own context | sponsored context | yes | yes |
| Overlap/Git reads | scoped | scoped | yes | yes |
| Queue/dashboard | local context only | review queue | full manager view | full |
| Launch preview/launch | no | no | yes | yes |
| Runtime inspect/steer | no | no | owned bindings | all authorized |
| Task create/revise/claim/dispatch | no | no | existing manager rules | yes |
| Acceptance/publication | no | no | only if separately authorized; deferred | current protected authority |
| Client/host/GM admin | no | no | no | deferred/manual-only |

MCP loading cannot change this table.

## 18. Source-level implementation plan

## L1 — high-level read projections

Files:

```text
src/swarm.rs                         new pure schemas/limits
src/store/swarm.rs                   new application projections
src/store/projection.rs
src/store/mod.rs
src/model.rs                         minimal shared typed fields
src/doctor.rs
```

Implement:

```text
swarm.dashboard
swarm.queue.get
swarm.assignment.preview/get
swarm.context.get
swarm.agent.inspect
swarm.exceptions.get
swarm.capacity.get
```

No launch mutation, Git subprocess or model call.

## L2 — read-only Git/workspace overlap

Files:

```text
src/git_inspect.rs
src/store/swarm.rs
```

Implement `swarm.overlap.check` using the bounded Git inspection contract and controller ownership facts.

## L3 — watches/reminders

Implement exact bounded watch records, change-triggered evaluation, expiry and subscription/mailbox hints. No per-participant polling task and no model wake.

## L4 — launch preview and plan digest

Implement pure `swarm.launch.preview`, exact workspace/route/profile checks and deterministic plan digest. Do not start runtime work.

## L5 — launch orchestration

Implement `swarm.launch` as an application Operation linking existing claim, workspace, binding, runtime and participant-profile Operations. Preserve each existing authority and unknown-outcome reconciliation.

## L6 — MCP/CLI/UI surfaces

Expose high-level methods through the grouped/deferred catalog. Add compact rendering and role-specific eager surfaces. Low-level methods remain available by search where authorized.

## L7 — qualification

Run fixture, simulated fleet and staged live launch scenarios only after the complete production path exists.

## 19. Acceptance cases

1. Manager preview names current related owners, queue reason, overlap, route state, MCP surface and gaps without starting work.
2. Launch under stale Task revision or changed workspace lease is rejected before native effect.
3. Lost launch response under identical request ID returns one retained launch lineage.
4. OutcomeUnknown runtime start is reconciled; no blind replacement session is created.
5. Participant receives no global queue or all-agent roster.
6. Participant can find the exact owner of an overlapping contract/path/symbol without asking Root.
7. Ambiguous owner search sends no broadcast.
8. `coordination.consult` answers from a current card without a model message when possible.
9. Watch notification creates zero model turns and zero dedicated polling tasks.
10. Stored reminder/message does not claim presentation or consumption.
11. Missing required MCP core tool prevents a false-ready launch.
12. Runtime-specific missing tool becomes `relay_only` or `incompatible`, not hallucinated availability.
13. Git blame/history never becomes current ownership authority.
14. Incomplete Git scan returns an explicit gap and cannot prove no overlap.
15. One mutable candidate has one manager/write-lease lineage.
16. Manager dashboard cost depends on bounded pages/projections, not full transcripts.
17. Agent prompt packet remains bounded while full evidence is pullable.
18. Manager exceptions remain near zero as a conflict-free registered population grows.
19. No launch/packet/catalog data contains real credentials, private deployment domain or remote-visible local absolute path.
20. Acceptance/publication remains a separate protected operation after launch and implementation.

## 20. Final invariant

```text
The manager should know enough to assign correctly.
The agent should know enough to work and coordinate independently.
Neither should receive the entire fleet, queue, history or tool catalog in context.
```

The launcher establishes a trustworthy work environment once. The controller then supplies compact current facts and precise tools on demand, allowing agents to collaborate without turning Root into a message broker or MCP into an unbounded prompt dump.