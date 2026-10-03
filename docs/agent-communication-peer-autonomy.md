# ELIOT Agent Communication — Peer Autonomy and Integration Handshake

**Revision:** 1 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Applies to:** [Implementation Issues](agent-communication-implementation-issues.md), [Implementation Checklist](agent-communication-implementation-checklist.md), [Agent Communication and Concilium](agent-communication-concilium.md), [Tool Contracts](agent-communication-tool-contracts.md), [Field Evidence](agent-communication-field-evidence.md)  
**Status:** normative autonomy addendum. It changes the routing of ordinary coordination, not Task/Attempt/Acceptance authority.

## 0. Decision

The communication plane must remove the General Manager and auditor from ordinary engineering coordination.

The manager still:

- assigns and revises work;
- selects Task/Attempt ownership;
- changes canonical project policy;
- ratifies global/public/authority-sensitive contracts;
- accepts or rejects the final candidate;
- opens Concilium when a material conflict cannot be settled locally.

Peers may independently:

- discover the exact owner of a code or contract scope;
- inspect what that owner plans to provide;
- ask one precise question;
- answer with a typed fact, unknown, redirect or negotiation request;
- publish draft producer/consumer contract cards;
- compare an integration offer with a consumer requirement;
- agree on a local implementation detail that stays inside both existing assignments and canonical documentation;
- record the agreement so later agents and the auditor do not reconstruct it from chat;
- continue working without waiting for the General Manager to relay every message.

```text
manager assigns work
        │
        ▼
peer directory + current contract cards
        │
        ├── exact fact already available ───────────────► continue
        │
        ├── one precise question ──► direct answer ─────► continue
        │
        ├── producer/consumer seam ─► handshake ───────► peer-local agreement
        │
        └── authority/global conflict ──────────────────► manager escalation
                                                           │
                                                           └── Concilium only if justified
```

The common path must not wait in a General Manager queue.

## 1. Design law

```text
Assignment authority is centralized.
Engineering coordination is local and sparse.
Project-wide policy and irreversible authority remain centralized.
```

The controller must distinguish:

```text
message                     pass information
quick ask                   request one bounded fact
peer agreement              coordinate existing assignments
manager decision            change project authority or canonical contract
follow-up work               new/revised Task or Assignment
```

None of these may be inferred from ordinary prose.

## 2. Why the previous design was still too centralized

The earlier program correctly prohibited free-running chat and peer task assignment, but it made two ordinary cases unnecessarily expensive:

1. an agent that only needs “what exact type/version/error shape are you implementing?” would need to open a full coordination thread;
2. two directly affected agents could exchange proposals but any selected contract could be read as requiring manager ratification.

At fleet scale this creates a new bottleneck:

```text
100 agents
  × ordinary interface questions
  × manager/auditor mediation
  = central attention queue and repeated context reconstruction
```

The solution is not to weaken Task authority. The solution is to create a **peer-local autonomy envelope** and make the cheapest safe interaction the default.

## 3. Evidence from current systems

### 3.1 Claude Code Agent Teams and cross-session messaging

Official current behavior establishes useful product patterns:

- teammates have independent context and can message one another directly;
- a team has a shared task list, but the lead coordinates and assigns work;
- cross-session messages are text only, not the sender's full conversation or files;
- messages identify the sender as another session, not the human;
- peer messages cannot approve permissions or change configuration;
- inbound messages can be accepted, held or refused;
- the UI shows a one-line preview before expansion;
- official guidance warns that coordination and token overhead increase with team size and that shared mutable files are a poor parallelization target.

Field reports also show failure modes ELIOT must avoid:

- silent fallback from a real team to isolated subagents;
- wrong recipient names creating orphan inboxes;
- task status divergence between team and session lists;
- delayed inbox processing until a teammate finishes its current task;
- messages reported sent but never delivered;
- completion notifications flooding the context;
- duplicate/replayed assignments;
- one logical agent name mapping to multiple live writers;
- team lifecycle ending while background teammates remain active.

**Adopt:** direct addressed peer access, one-line preview, explicit inbound policy, peer messages never count as user approval.

**Do not adopt:** a second shared Task authority, auto-starting an idle model for every message, recipient strings that create inboxes, poll-driven delivery or full transcript injection.

### 3.2 OpenAI Responses Multi-agent

The official API separates two operations:

```text
send_message   queue information without starting a turn
followup_task  assign more work and start/resume a turn
```

This is the exact semantic split ELIOT needs.

**ELIOT peer tools expose the equivalent of `send_message`.** Only existing manager/Task/runtime authority can create the equivalent of `followup_task`.

### 3.3 Agency Swarm

Agency Swarm uses explicit directional `communication_flows`, and allows a message schema to carry required decisions/context fields.

**Adopt:** communication edges are explicit and typed.

**Improve:** ELIOT derives a sparse allowed graph from current assignment, contract and scope relationships instead of one global static graph. An agent cannot message every registered actor merely because they exist.

### 3.4 Gas Town and Beads

Useful patterns:

- durable work exists outside model context;
- `seance --talk` is a one-shot question to a predecessor rather than a shared room;
- information queries are explicitly listed as a case that should not escalate;
- escalation is severity/category routed rather than every issue going directly to the Mayor.

Field reports also show that a central Mayor can become reactive or idle, cross-machine nudges can be one-way, routed identity can resolve against the wrong store, and operational state stored in an issue graph can diverge from live processes.

**Adopt:** one-shot owner/predecessor query and tiered escalation.

**Do not adopt:** another work graph, another lifecycle authority or automatic stale escalation that itself creates repeated model work.

### 3.5 Overstory

Useful patterns:

- agent discovery by capability/state/parent;
- typed mail;
- isolated worktrees;
- explicit warning that integration boundaries, coordination cost, context fragmentation and merge forensics are normal swarm risks.

Its mail type list also includes `dispatch` and `assign`, and supports group broadcast.

**Adopt:** discovery and typed technical messages.

**Do not adopt:** peer-visible `assign`/`dispatch`, broadcast groups, urgent-message auto-nudge or a second mail database.

### 3.6 MCP Agent Mail and small mailbox projects

Useful patterns:

- mail rather than chat;
- agent directory;
- threaded pull views;
- advisory file reservations;
- no need for the human to relay every message.

The existing PR already records why its code/storage stack is not adopted. This addendum narrows the useful UX further: most agents should query a current contract card before sending any mail.

## 4. Coordination ladder

Every interaction begins at the cheapest level that can answer the question.

### Level 0 — read current facts

No peer message.

```text
coordination.context.get
coordination.peer.find
coordination.work_card.get/list
coordination.contract_card.get/list
code.scope.inspect
git.who_works_here
```

Use this for:

- who owns this symbol/interface/path;
- what the owner says it will produce;
- current draft version and readiness;
- expected integration point;
- existing assumptions and known gaps.

### Level 1 — quick ask

One question, one exact recipient, one bounded answer.

```text
coordination.ask
coordination.answer
coordination.ask.get/list
```

Use this for:

- “Which error enum variant will you return?”
- “Is this ID equal to the Operation ID or distinct?”
- “Will the consumer receive bytes or an artifact reference?”
- “Which function is the production caller?”
- “Has your draft moved from v2 to v3?”

The caller continues other work. It does not block a model thread or poll in a loop.

### Level 2 — integration handshake

Producer and consumer compare a typed offer and requirement.

```text
coordination.integration.offer
coordination.integration.requirement
coordination.integration.check
coordination.integration.ack
coordination.agreement.get/list
coordination.agreement.supersede
```

Use this when an interface must be stitched.

### Level 3 — bilateral negotiation thread

Use the existing typed thread only when one ask/answer and deterministic compatibility check are insufficient.

### Level 4 — manager decision

Needed when the proposed agreement crosses the autonomy envelope in §10.

### Level 5 — Concilium

Only after a material, documented disagreement survives bilateral work and the manager accepts the cost.

## 5. Assignment-start experience

The first coordination call for an agent is:

```text
coordination.context.get
```

Input:

```json
{
  "task_id": "...",
  "attempt_id": "...",
  "participation_basis": {
    "kind": "attempt_owner | producer_ref | sponsored_reviewer",
    "id": "..."
  },
  "candidate_ref": "optional exact current candidate"
}
```

Bounded result:

```json
{
  "assignment": {
    "task_revision": 4,
    "attempt_id": "...",
    "owner": "...",
    "canonical_sources": []
  },
  "my_work_card": null,
  "relevant_peers": [
    {
      "actor": {},
      "participation_basis": {},
      "why_relevant": ["producer_for_required_contract"],
      "contract_keys": ["operation-admission-v3"],
      "scope_overlap": []
    }
  ],
  "required_contracts": [],
  "provided_contracts": [],
  "integration_edges": [],
  "pending_asks": [],
  "scope_conflicts": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

This call replaces repository-wide agent discovery and avoids asking the General Manager “who should I talk to?”

## 6. Work cards

A work card is a compact current projection, not a chat status message.

### 6.1 Methods

```text
coordination.work_card.publish
coordination.work_card.get
coordination.work_card.list
coordination.work_card.withdraw
```

### 6.2 Shape

```json
{
  "task_id": "...",
  "attempt_id": "...",
  "participation_basis": {},
  "owner_actor": {},
  "state": "planning | active | waiting_input | integration_ready | review_ready | superseded",
  "summary": "Implement producer side of operation-admission-v3.",
  "planned_paths": ["src/runtime/opencode.rs"],
  "planned_symbols": ["RuntimeCommand"],
  "provides": ["operation-admission-v3"],
  "requires": ["native-input-readback-v2"],
  "assumptions": [],
  "known_gaps": [],
  "expected_integration_points": ["Store::admit_runtime_command"],
  "material_revision": 3,
  "material_digest": "sha256:...",
  "updated_at_ms": 0
}
```

### 6.3 Anti-spam rule

Ordinary progress percentages, “still working,” liveness heartbeats and tool activity do not create durable work-card revisions.

A new revision requires a material change in at least one of:

```text
provided/required contract
planned scope
assumption
known gap
integration point
state transition affecting another assignment
```

Current liveness remains a separate replaceable observation.

## 7. Contract cards

A contract card tells another agent what is being built before implementation is complete.

### 7.1 Methods

```text
coordination.contract_card.publish
coordination.contract_card.get
coordination.contract_card.list
coordination.contract_card.withdraw
```

These are draft/current projections. They are not manager ratification and do not replace the immutable proposal path for disputed/global contracts.

### 7.2 Shape

```json
{
  "contract_key": "operation-admission-v3",
  "task_id": "...",
  "attempt_id": "...",
  "owner_actor": {},
  "owner_basis": {},
  "role": "producer | consumer | carrier | observer",
  "state": "draft | peer_agreed | manager_ratified | implementation_ready | superseded",
  "canonical_sources": [],
  "interface": {
    "input_type": "RuntimeCommand",
    "output_type": "AdmissionReceipt",
    "version": "v3",
    "producer": "OpenCode adapter",
    "consumer": "Store admission",
    "carrier": "Operation result",
    "caller": "task.dispatch"
  },
  "identity": {
    "correlation_key": "controller_operation_id",
    "equalities": ["receipt.operation_id == operation.id"],
    "distinctions": ["native_input_id != operation.id"]
  },
  "data_boundary": {
    "kind": "inline | artifact_ref | native_readback",
    "readable_by": "...",
    "availability_boundary": "native admission readback"
  },
  "time_boundary": {
    "clock_owner": "controller",
    "observed_at": "after readback",
    "epoch_revision": "binding generation"
  },
  "result_owner": "Store",
  "failure": {
    "rejected": "native explicit refusal",
    "unknown": "write may have reached native service",
    "retry": "GET/readback only under unknown outcome"
  },
  "compatibility": {
    "additive_fields": true,
    "breaking_change_policy": "new version"
  },
  "affected_paths": [],
  "affected_symbols": [],
  "material_revision": 2,
  "material_digest": "sha256:..."
}
```

The fields mirror the actual integration failures seen in the swarm: type/producer, identity equality versus distinction, data versus readable reference, time/epoch observation, result owner and failure semantics.

## 8. Relevant peer graph

ELIOT does not expose “all agents” as the normal starting point.

A peer is relevant when at least one exact edge exists:

```text
Task dependency
producer/consumer/carrier relation
accepted scope overlap
shared contract key
explicit code symbol/path reference
manager-sponsored reviewer relation
prior unresolved coordination thread
```

### 8.1 Method

```text
coordination.peer.find
```

Input may name one or more exact targets:

```json
{
  "task_id": "...",
  "attempt_id": "...",
  "contract_key": "operation-admission-v3",
  "path": null,
  "symbol": "RuntimeCommand",
  "interface": null,
  "purpose": "ask_contract | integration | scope_overlap | review"
}
```

Result ranks exact current owners and reports why each matched. It never creates an actor or sends anything.

### 8.2 Ambiguity

If one exact owner exists, `ask_owner` may address it.

If several owners exist, return candidates and relation facts. Do not broadcast the question.

If no owner exists, return `unowned` and a manager attention item only when the caller marks the missing ownership as blocking.

## 9. Quick ask

### 9.1 Method

```text
coordination.ask
```

Input:

```json
{
  "client_request_id": "caller-known-id",
  "task_id": "...",
  "attempt_id": "...",
  "target": {
    "actor": null,
    "contract_key": "operation-admission-v3",
    "path": null,
    "symbol": null
  },
  "question_kind": "contract_shape | integration_point | identity | failure_semantics | status_fact | assumption_check | scope_overlap | compatibility | predecessor_fact",
  "question": "Is the native input ID equal to the controller Operation ID?",
  "why_needed": "The consumer must choose its idempotency key.",
  "expected_answer": "one_fact",
  "blocking": false,
  "reply_deadline_ms": null,
  "evidence_refs": []
}
```

The host resolves one recipient from the peer graph or returns ambiguity. The sender cannot request `@all`.

### 9.2 Answer

```text
coordination.answer
```

```json
{
  "client_request_id": "...",
  "ask_id": "...",
  "status": "answered | unknown | redirect | needs_negotiation",
  "answer": "Distinct. The controller Operation ID is the idempotency key; native input ID is read back afterward.",
  "contract_card_revision": 2,
  "redirect_target": null,
  "evidence_refs": [],
  "assumptions": []
}
```

### 9.3 Behavior

- one ask creates one addressed mailbox delivery;
- an identical unresolved ask fingerprint returns the existing ask unless the caller explicitly records changed evidence/question revision;
- the recipient may answer, redirect, state unknown or request negotiation;
- no politeness acknowledgement is required;
- the sender continues other work;
- no poll loop; a small reply-ready freshness fact is enough;
- no model is started merely because the recipient is idle;
- sender and recipient cannot use the ask to create work for one another.

### 9.4 One-shot predecessor query

`question_kind=predecessor_fact` may address a retained prior participant/session result through its current manager-visible projection. It is analogous to a one-shot seance, not a resumed full conversation. If the prior native session cannot accept messages, the query is answered only from retained facts or returns unknown.

## 10. Peer-local autonomy envelope

A deterministic classifier decides whether two peers may commit an agreement without manager ratification.

### 10.1 Peer-local agreement allowed only when all are true

- exact current Task/Attempt/participation bases are valid;
- only directly affected current assignments are parties;
- the agreement stays within their existing Task scopes;
- it refines, but does not contradict, canonical documentation or Task text;
- it does not create another Task/Assignment;
- it does not change public API compatibility promised outside these assignments;
- it does not add or alter a database migration/persistent authoritative schema;
- it does not change roles, permissions, credentials or security policy;
- it does not change identity, idempotency, fencing or lifecycle ownership required by canonical contracts;
- it does not authorize an irreversible external effect;
- it does not change provider/model/billing route;
- it does not change project build/test/acceptance policy;
- it does not expand another agent's write scope without that owner and manager-approved scope revision;
- all affected peers acknowledge the exact agreement digest;
- no unresolved objection remains from an affected current owner.

Examples:

- exact internal function name and module boundary inside two assigned files;
- whether a local carrier exposes `&[u8]` or an already required immutable reference, when both satisfy the canonical type/ownership contract;
- exact registration call site already required by documentation;
- error mapping among already permitted variants;
- integration order and branch/candidate read point inside current assignments;
- compatibility shim details when the canonical contract already requires backward compatibility and no authority boundary changes.

### 10.2 Manager ratification required if any are true

- canonical Task/documentation is ambiguous or would be changed;
- agreement changes a stable/public interface or shared schema used beyond current parties;
- database migration or authoritative persisted representation changes;
- security, role, permission, credential, network or sandbox boundary changes;
- identity, retry, idempotency, fence, clock/epoch or lifecycle owner changes;
- irreversible external effect or forge publication changes;
- provider/model/billing/usage semantics change;
- project-wide build/test/acceptance policy changes;
- scope expands to an unrepresented owner or more than the directly affected assignments;
- parties disagree after one bounded negotiation cycle;
- compatibility check is unknown on a required dimension;
- one affected current owner is absent/stale;
- manager previously marked the contract as manager-controlled.

### 10.3 Auditor

The auditor is not in the approval path for peer-local agreements.

The auditor later receives:

```text
exact agreement digest
parties and participation bases
canonical sources
compatibility result
implementation evidence
remaining assumptions/dissent
```

It verifies compliance during review. It does not relay routine messages.

## 11. Integration handshake

### 11.1 Producer offer

```text
coordination.integration.offer
```

Carries a contract card revision plus exact integration availability:

```json
{
  "contract_key": "operation-admission-v3",
  "proposal_revision": 2,
  "produced_by": {},
  "will_be_available_at": {
    "path": "src/runtime/opencode.rs",
    "symbol": "build_runtime_command"
  },
  "readiness": "draft | implementation_ready | observed",
  "candidate_ref": null,
  "assumptions": []
}
```

### 11.2 Consumer requirement

```text
coordination.integration.requirement
```

```json
{
  "contract_key": "operation-admission-v3",
  "required_by": {},
  "consumer_path": "src/store/runtime.rs",
  "consumer_symbol": "admit_runtime_command",
  "required_dimensions": {
    "input_type": "RuntimeCommand",
    "correlation_key": "controller_operation_id",
    "data_kind": "inline",
    "unknown_outcome": "readback_only",
    "version": "v3"
  },
  "must_be_ready_before": "consumer integration",
  "assumptions": []
}
```

### 11.3 Deterministic compatibility check

```text
coordination.integration.check
```

Dimensions:

```text
producer and consumer identity
input/output schema and version
IDs that must be equal
IDs that must remain distinct
ownership and lifetime
inline data vs readable reference
availability/read boundary
clock/epoch/binding generation
result/disposition owner
rejected/failed/unknown semantics
retry and reconciliation
capability/feature gate
registration and production caller
bounds and unsupported cases
```

Result:

```json
{
  "status": "compatible | compatible_with_assumptions | mismatch | unknown",
  "matches": [],
  "mismatches": [],
  "unknown_dimensions": [],
  "required_actions": [],
  "autonomy": "peer_local | manager_required",
  "autonomy_reasons": [],
  "comparison_digest": "sha256:..."
}
```

### 11.4 Acknowledgement and agreement

Both affected parties acknowledge the same comparison digest and exact terms:

```text
coordination.integration.ack
```

When `autonomy=peer_local` and all required owners acknowledge, the controller records:

```text
peer_agreed
```

When `manager_required`, acknowledgement records peer positions but the agreement remains:

```text
pending_manager
```

No agreement changes Task or canonical documentation by itself.

## 12. Peer agreement lifecycle

```text
draft
  -> peer_agreed
  -> active
  -> implemented_observed
  -> verified | refuted
  -> superseded

manager-sensitive branch:
  draft
    -> peers_ready
    -> pending_manager
    -> manager_ratified | rejected
```

Properties:

- agreement is pinned to exact Task/Attempt revisions and participants;
- later Task revision makes it historical/stale;
- manager may supersede a peer-local agreement with an exact reason and replacement;
- parties receive a small superseded notification;
- implementation does not imply verification;
- verification does not imply Task acceptance;
- a peer-local agreement does not become a project-wide standard.

## 13. Manager load model

The manager does **not** receive a copy of every:

- work-card revision;
- quick ask;
- answer;
- compatible handshake;
- peer-local agreement;
- ordinary thread message.

The manager gets a bounded digest of:

```text
unowned blocking scopes/contracts
manager_required agreements
unresolved mismatches
absent/stale required owner
scope override requests
repeated no-progress loops
security/authority/global policy questions
Concilium proposals
```

### 13.1 Method

```text
coordination.manager.digest
```

The digest groups by Task/contract/root cause and shows one current item per fingerprint. It is a read projection, not mail spam.

### 13.2 Expected operational result

The design target—not a current measured claim—is:

```text
most coordination resolved by lookup, ask or peer-local handshake
manager sees exceptions and authority changes
independent auditor reviews durable summaries/evidence, not chat transcripts
```

## 14. Inbound delivery policy

A recipient, manager policy or local profile controls inbound behavior. The sender cannot force a model turn.

```text
pull_only       durable mailbox/header; recipient reads when it chooses
safe_boundary   if an active compatible client reaches a safe tool boundary, expose one compact header
hold            record delivery but do not expose to the model until released
refuse          reject new peer coordination for that recipient/scope
```

V1 application storage supports these policy facts even if not every runtime can consume `safe_boundary` yet.

Rules:

- idle recipient is not automatically started;
- active tool call is never interrupted by ordinary coordination;
- full body/history is never auto-injected;
- one-line header includes sender, topic, question kind and deadline;
- recipient explicitly reads the full item;
- held queue is bounded; overflow reports an explicit drop/rejection, not silent loss;
- duplicate/burst control uses ask/message fingerprints;
- peer message cannot approve permissions or configuration;
- task assignment uses a separate manager action.

### 14.1 One-shot readiness notification

```text
coordination.notify_when_available
```

This registers one bounded subscription to an existing ask/agreement/contract revision. It emits one small freshness fact and expires. It does not poll with model calls and does not wake an idle model.

## 15. Agent-facing convenience tools

The common agent UX should be five small tools rather than dozens of low-level calls.

### 15.1 `coordination.context.get`

“What work/peers/contracts matter to me now?”

### 15.2 `coordination.ask_owner`

“Resolve the exact owner of this contract/path/symbol and send one question.”

It composes `peer.find + ask` atomically only after unambiguous readback. If ownership is ambiguous, it sends nothing.

### 15.3 `coordination.publish_contract`

“Publish or revise my current contract card.”

### 15.4 `coordination.check_integration`

“Compare my requirement to the producer's current offer.”

### 15.5 `coordination.peer_agree`

“Acknowledge the exact peer-local agreement/comparison digest.”

These are typed convenience application methods, not MCP-only macros and not prompt conventions.

Advanced methods remain available for diagnostics and manager control.

## 16. No-wait working pattern

An agent that sends a question should continue with independent work.

```text
ask sent
  -> continue local implementation not dependent on answer
  -> optional one-shot availability subscription
  -> inspect answer at next chosen coordination checkpoint
```

If the answer is genuinely blocking:

- mark the exact dependency in the work card;
- avoid implementing the uncertain seam;
- work on another in-scope part;
- escalate only when the deadline/impact justifies it.

The controller does not suspend a native session or consume a manager slot while waiting.

## 17. Sparse topology and scale

### 17.1 No all-to-all graph

At 100 agents, an all-to-all graph has 4,950 undirected pairs. ELIOT builds only material edges from current work facts.

Indexes:

```text
contract_key -> current producer/consumer cards
path/symbol/interface -> accepted scopes and work cards
Task/Attempt -> current participation bases
recipient -> pending ask/message headers
agreement fingerprint -> current state
manager fingerprint -> current escalation digest item
```

### 17.2 No per-agent process/task

Registered inactive agents consume no Tokio task, model turn or polling loop.

### 17.3 Current-state projections

Work cards, contract cards and manager digest are replaceable/revisioned `meta` projections backed by normal Operations/Observations. Ordinary material updates do not generate one durable message per observer.

### 17.4 Complexity target

Common operations should scale with matching active edges/results, not total registered agents:

```text
context.get              bounded current Task neighborhood
peer.find                indexed exact keys
ask_owner                one owner resolution + one delivery
integration.check        two exact revisions + deterministic compare
manager.digest           current exception fingerprints only
```

## 18. Escalation classifier

Escalation is typed and compact:

```text
information_missing
owner_missing
contract_mismatch
canonical_ambiguity
authority_change
security_boundary
persistent_schema
identity_or_replay
scope_expansion
irreversible_effect
no_progress
```

Default route:

| Class | Route |
|---|---|
| information missing with known owner | quick ask |
| local compatible interface | peer handshake |
| owner missing | manager digest |
| contract mismatch, local | bilateral negotiation |
| canonical ambiguity/global/public change | manager decision |
| security/persistence/identity/irreversible effect | manager/current GM |
| unresolved material conflict | manager may open Concilium |

No severity timer automatically starts models or repeatedly re-escalates the same unchanged item.

## 19. Auditor experience

The auditor should not read hundreds of chat messages.

Review packet:

```text
Task/Attempt revision
current work/contract cards used
peer agreement or manager ratification digest
compatibility comparison
remaining assumptions/unknown dimensions
scope/Git overlap result
candidate implementation evidence
minority objections
```

This lets the auditor test the actual seam instead of reconstructing informal negotiations.

## 20. API additions and changes

### 20.1 New reads

```text
coordination.context.get
coordination.peer.find
coordination.work_card.get
coordination.work_card.list
coordination.contract_card.get
coordination.contract_card.list
coordination.ask.get
coordination.ask.list
coordination.integration.get
coordination.agreement.get
coordination.agreement.list
coordination.manager.digest
```

### 20.2 New mutations

```text
coordination.work_card.publish
coordination.work_card.withdraw
coordination.contract_card.publish
coordination.contract_card.withdraw
coordination.ask
coordination.answer
coordination.integration.offer
coordination.integration.requirement
coordination.integration.check
coordination.integration.ack
coordination.agreement.supersede
coordination.notify_when_available
coordination.escalate
```

### 20.3 Existing methods retained

```text
coordination.thread.*
coordination.contract.*
code.scope.*
git.*
concilium.*
message.cancel
```

The convenience methods do not bypass request receipts, authorization or the Store.

## 21. Authority matrix

| Action | Peer | Attempt manager | Current GM/operator | Auditor |
|---|---:|---:|---:|---:|
| Read relevant work/contract card | yes | yes | yes | during authorized review |
| Ask exact relevant owner | yes | yes | yes | no routine need |
| Answer own ask | yes | yes | yes | no |
| Publish own draft work/contract card | yes | yes | yes | no |
| Commit peer-local agreement | affected peers | yes | yes | no |
| Change Task/Assignment | no | yes through existing API | yes through existing API | no |
| Ratify manager-required contract | no | yes if current owner policy permits | yes | no |
| Override another accepted scope | no | yes with reason | yes | no |
| Open/advance/close Concilium | propose only | yes | yes | no |
| Verify/refute implementation | no self-authority | no self-authority | acceptance path | yes |
| Accept Task | no | existing protected authority only | existing protected authority | evidence provider only |

## 22. Adjusted implementation sequence

This addendum modifies the earlier I1–I9 plan as follows.

### I1 — mailbox extraction

Unchanged.

### I2 — schemas, directory and current cards

Add before general thread mutations:

```text
coordination.context.get
coordination.peer.find
work_card publish/read
contract_card publish/read
relevant peer graph
manager digest read projection
```

This gives immediate value without model-to-model communication.

### I3 — quick ask/answer

Before full bilateral threads:

```text
ask_owner
ask
answer
one-shot availability notification
inbound policy facts
```

Reuse one-recipient mailbox delivery. No model wake.

### I4 — integration handshake and autonomy classifier

Add:

```text
offer
requirement
compatibility check
ack
peer-local agreement
manager-required classification
```

Then implement the longer generic contract proposal/ratification path for disputed/global contracts.

### I5 — advisory scopes

Unchanged, but work/contract cards feed relevant-peer discovery.

### I6 — Git inspector

Unchanged.

### I7 — Concilium

Remains last among coordination semantics. It must not be the normal interface negotiation path.

### I8 — CLI/MCP/profile UX

Expose five convenience tools first. Low-level methods remain available under manager/debug profiles.

### I9 — qualification

Add measures:

```text
percentage resolved by lookup only
percentage resolved by one ask/answer
percentage resolved by peer-local handshake
manager escalations per 100 coordination events
messages/model turns per resolved seam
full-thread reads per ask
duplicate/no-progress rate
integration mismatches found before merge
```

No target is claimed passed until measured.

## 23. Acceptance scenarios

### Directory and cards

1. Consumer finds the exact current producer without manager involvement.
2. Stale work card from an old Attempt is visible as historical, not current.
3. Materially unchanged work-card publish is idempotent/no new revision.
4. Liveness/status chatter creates no card revision or mailbox row.
5. Ambiguous ownership returns candidates and sends nothing.
6. Unowned blocking contract creates one manager digest item.

### Quick asks

7. One ask reaches one exact current owner.
8. Ask cannot create a placeholder recipient.
9. Duplicate unresolved ask coalesces.
10. Answer may be `unknown` without inventing a fact.
11. Redirect names an exact current owner/basis and does not forward automatically to many actors.
12. Ask/answer changes no Task/Assignment.
13. Idle recipient is not started.
14. Full conversation history is not attached.
15. Sender continues other work and receives one freshness fact on answer.

### Integration handshake

16. Matching producer/consumer cards return compatible.
17. Different correlation-key equality returns mismatch.
18. Missing clock/epoch boundary returns unknown rather than compatible.
19. Both peers acknowledge exact comparison digest.
20. Local agreement commits without manager when every autonomy condition holds.
21. Public API or persistent schema change is classified manager-required.
22. Stale Task/Attempt/card revision blocks acknowledgement.
23. One unresolved affected-owner objection blocks peer-local commit.
24. Peer-local agreement does not modify canonical docs or Task.
25. Manager supersession preserves the old agreement and notifies affected peers.

### Manager/auditor load

26. Manager receives no copy of ordinary compatible handshakes.
27. Ten repeated identical mismatches create one digest item.
28. Auditor reviews agreement/comparison/evidence without reading the chat.
29. Auditor refutation does not erase negotiation history.
30. Acceptance remains a separate protected Operation.

### Delivery and scale

31. Recipient `hold` prevents model exposure but preserves durable item.
32. Recipient `refuse` rejects clearly; sender does not retry automatically.
33. Safe-boundary header never interrupts an active tool.
34. Held queue overflow is explicit.
35. 10,000 inactive clients create no background tasks.
36. Relevant-peer lookup does not enumerate all registered clients.
37. Slow reader does not block ask admission or native reply/control work.
38. No peer action invokes `followup_task`-equivalent work.

## 24. UX examples

### 24.1 No message needed

```text
Agent A: coordination.context.get
ELIOT: contract operation-admission-v3 is owned by Agent B; draft card revision 2 attached
Agent A: implements against the card
```

### 24.2 One fact

```text
Agent A: coordination.ask_owner(contract=operation-admission-v3,
                                question_kind=identity,
                                question="Are the IDs equal?")
Agent B: coordination.answer(status=answered,
                             answer="No; Operation ID is durable, native input ID is read back.")
Agent A: continues
```

The General Manager sees none of this unless it becomes unresolved or authority-sensitive.

### 24.3 Stitching

```text
Agent B publishes producer offer
Agent A publishes consumer requirement
ELIOT compares the six-link chain and failure semantics
Both acknowledge the compatible digest
Agreement becomes peer_agreed for exact Task/Attempt revisions
```

### 24.4 Real escalation

```text
Compatibility check: persistent schema and idempotency owner would change
ELIOT classifies manager_required
Manager digest receives one compact item with both positions and mismatches
Manager decides or opens Concilium
```

## 25. Donor adoption update

| Source | Take now | Explicitly reject |
|---|---|---|
| Claude Code cross-session/teams | direct discoverable peer messages, text-only context, preview, inbound accept/hold/refuse, peer message not approval | shared Task authority, auto idle-turn start, silent fallback, recipient-string inbox creation, polling delivery |
| OpenAI Responses Multi-agent | hard semantic split between message and follow-up work; author/recipient tracing | letting every peer assign/start work |
| Agency Swarm | explicit directional communication edges and typed required context | static all-project flow as the only routing source; handoff conflated with ordinary message |
| Gas Town/Beads | one-shot predecessor query; escalation only after local resolution fails; durable summaries outside context | another work graph/lifecycle authority; Mayor as mandatory relay; automatic stale model escalation |
| Overstory | capability/current-state discovery; typed mail; integration risk warnings | broadcast groups, peer `assign/dispatch`, auto-nudge urgency, second mail DB |
| MCP Agent Mail | mail UX, directory, advisory reservations | code/license/storage stack and status/liveness mail |
| A2A | Message/Task distinction for future external boundary | local protocol replacement |

## 26. Non-goals

- no peer-created Task or Assignment;
- no autonomous reallocation of work;
- no general chat room;
- no all-agent broadcast;
- no requirement that every local detail reach the manager;
- no auditor approval for ordinary peer-local agreement;
- no consensus/vote authority;
- no automatic model wake;
- no hidden `followup_task` equivalent;
- no second task list or issue graph;
- no semantic search/vector database in V1;
- no contract-card claim that overrides canonical sources;
- no peer-local change to security, persistence, identity, lifecycle or external effects;
- no real domain, credential or private local path in repository examples.

## 27. Final implementation rule

```text
Make the correct peer easy to find.
Make the current contract cheaper to read than to ask about.
Make one precise question cheaper than opening a thread.
Make a compatible producer/consumer agreement cheaper than escalating.
Make authority-changing decisions impossible to hide inside peer conversation.
```

This is the required balance: agents collaborate directly and naturally on engineering details, while the manager remains the authority for work, global contracts and acceptance rather than becoming the switchboard for every sentence.