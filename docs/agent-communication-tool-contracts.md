# ELIOT Agent Communication — Tool Contracts and Rust Implementation

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Normative companion:** [Agent Communication and Concilium](agent-communication-concilium.md)  
**Evidence companion:** [Field Evidence and Donor Map](agent-communication-field-evidence.md)

## 0. Purpose

This document translates the communication program into implementable application methods, state machines, Rust file ownership, SQLite projections, Git commands, MCP surfaces and directed tests.

It intentionally stays inside the current architecture:

```text
public typed application method
  -> current authentication and role checks
  -> current request receipt / Operation
  -> current Store transaction
  -> current Observation stream
  -> bounded read projection
  -> current local IPC / CLI / MCP facade
```

No second task system, database, event store, broker, daemon or agent loop is introduced.

## 1. Existing code to extend

| Existing unit | Current responsibility | Coordination use |
|---|---|---|
| `src/model.rs` | canonical JSON/digests, IDs, actors/scopes, strict field validation, roles | shared validation helpers only; do not place the whole feature here |
| `src/store/mod.rs` | authenticated public-method dispatch, request receipts, read classification, observation/report reads | register exact coordination methods and role gates |
| `src/store/producers.rs` | Attempt assignment/native producer identity | validate Task/Attempt/assignment context; do not turn messages into Producers |
| `src/store/projection.rs` | bounded report/capacity/attention projections | project coordination attention, active threads and conflicts |
| `src/artifacts.rs` | immutable bounded artifacts/pages | store large message/proposal/evidence bodies |
| `src/mcp.rs` | one typed tool per public application method; no passthrough | expose profile-filtered coordination tools |
| `src/mcp/subscriptions.rs` | bounded freshness hints with lag/read-resync | notify only small committed coordination facts |
| `src/ipc.rs` | authenticated local transport | unchanged protocol boundary |
| `src/host.rs` | host supervision | no per-agent coordination worker; only existing revision wake |
| `src/doctor.rs` | readiness and known gaps | report feature/config/index/qualification state |
| `migrations/001_core.sql` | nine-table durable authority | no new table in v1; do not edit the historical migration in place |

New recommended Rust units:

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/git_inspect.rs
src/git_inspect_tests.rs
src/mcp/coordination_tests.rs
```

`src/coordination.rs` owns pure typed schemas, limits, canonical digests and deterministic reasonability/progress evaluation. `src/store/coordination.rs` owns authorization, transactions and projections.

## 2. Authorization model

Do not add a generic `Worker` role solely for this feature. Current roles remain:

```text
operator
manager
observer
module
```

Communication authority is the intersection of role and exact work context.

### 2.1 Manager/operator

May:

- sponsor participants;
- open/merge/close threads;
- accept/release code-scope intents;
- ratify/reject contract proposals;
- preview/open/close Concilium;
- override a soft reasonability warning with a recorded reason.

### 2.2 Assigned participant

A registered client/module may communicate only when the current Task/Attempt projection establishes its participation through one of:

- current Attempt owner;
- bound `assignment_id`/ProducerRef;
- manager-sponsored participant record in the thread plan;
- explicit reviewer participant for the exact submission/candidate.

May:

- read threads in which it participates;
- send typed messages;
- propose/counter/object/support/withdraw;
- propose a code-scope intent;
- propose Concilium.

May not:

- assign work;
- add arbitrary participants;
- ratify project contract;
- open Concilium;
- mutate Task/Attempt/Acceptance through message text.

### 2.3 Observer

Read-only access only when a named local profile permits it and the thread/evidence visibility policy includes the observer.

### 2.4 Module

A runtime module does not automatically receive peer-communication authority. It may project a native agent participant only through its exact binding/generation/assignment and the application's scoped module checks.

## 3. Identifier and retry rules

IDs:

```text
thread_id              coord-<uuid>
message_id             cmsg-<uuid>
proposal_id            cprop-<uuid>
proposal_revision_id   cprev-<uuid>
scope_intent_id        cscope-<uuid>
concilium_id           concilium-<uuid>
round_id               cround-<uuid>
```

The caller supplies and retains `client_request_id` before every mutation. Existing `(caller_id, client_request_id)` equality remains the idempotency boundary.

The server mints entity IDs inside the first committed Operation result. Repeating the same request returns the same entity ID. The caller never invents a replacement entity after a lost reply.

All timestamps are positive epoch milliseconds. Human-readable ages are presentation only.

## 4. Shared limits

Proposed v1 limits:

```text
subject UTF-8 bytes                    512
summary UTF-8 bytes                    8 KiB
inline body UTF-8 bytes                32 KiB
reasonability declaration              32 KiB
proposal canonical JSON                64 KiB
participants normal/warn/absolute      2 / 4 / 8
artifact refs per message              8
related scopes per thread              64
claims per Concilium position           64
Concilium packet inline bytes          128 KiB
thread page serialized bytes           existing projection limit
```

Exceeding a protocol/size/security limit is a hard rejection. Conversation-count and participant warnings are soft and may be overridden by the manager with a reason.

## 5. `coordination.thread.open`

### 5.1 Request

```json
{
  "client_request_id": "caller-known-id",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "assignment_id": "assignment-a",
  "topic_kind": "contract",
  "subject": "Canonical Operation correlation key",
  "participants": [
    {
      "client_id": "writer-a",
      "generation": 3,
      "reason": "producer owner"
    },
    {
      "client_id": "writer-b",
      "generation": 7,
      "reason": "consumer owner"
    }
  ],
  "reasonability": {
    "blocking_fact": "Producer and consumer selected different retry identities.",
    "decision_needed": "Choose Operation ID or native input ID.",
    "why_coordination_is_needed": "Both assignments edit opposite sides of one interface.",
    "expected_output": "One contract proposal or a precise unresolved objection.",
    "close_condition": "Manager ratifies one proposal or revises one assignment."
  },
  "related_scopes": [
    {"kind":"symbol","value":"RuntimeCommand"},
    {"kind":"path","value":"src/store/runtime.rs"}
  ],
  "body_ref": null,
  "delivery_mode": "mailbox_only"
}
```

### 5.2 Validation

- strict allowlist/deny unknown fields;
- Task and Attempt exist and revisions match current context;
- caller is manager/operator or established participant;
- every participant exists at the exact generation;
- every participant has a concrete reason;
- caller cannot create a peer Assignment;
- body artifact exists and caller can read it;
- no active exact duplicate unless `allow_linked_duplicate=true` under manager sponsorship;
- broad/pathless topic produces a soft warning;
- no model/native call occurs.

### 5.3 Deterministic reasonability result

```json
{
  "classification": "reasonable | accepted_with_warning | manager_sponsorship_required",
  "reasons": ["participant_count_above_normal"],
  "duplicate_thread_id": null,
  "loop_fingerprint": "sha256:..."
}
```

The classification is rule-based. No LLM judges whether agents are “allowed to talk.”

### 5.4 Result

```json
{
  "thread_id": "coord-...",
  "revision": 1,
  "state": "open",
  "reasonability": {},
  "created_at_ms": 0,
  "operation_id": "...",
  "model_work_started": false
}
```

## 6. `coordination.message.send`

### 6.1 Request

```json
{
  "client_request_id": "caller-known-id",
  "thread_id": "coord-...",
  "expected_thread_revision": 4,
  "speech_act": "propose",
  "subject": "Use controller Operation ID",
  "summary": "Correlate native input to the already durable controller Operation.",
  "inline_body": null,
  "body_ref": "artifact-optional",
  "reply_to_message_id": "cmsg-optional",
  "requires_reply": true,
  "reply_deadline_ms": 0,
  "evidence_refs": ["artifact-..."],
  "proposal_revision_id": "cprev-...",
  "delivery_mode": "mailbox_only"
}
```

### 6.2 Speech acts

```text
inform
query
answer
propose
counterproposal
object
support
withdraw
not_understood
resolution_summary
```

`assign`, `accept_task`, `publish`, `merge`, `change_role` and similar acts are not valid enums.

### 6.3 Admission

- caller is a current participant at the recorded generation;
- thread open and revision matches;
- exactly one of inline body/body artifact or a nonempty summary is present;
- referenced proposal/thread/evidence is visible in the same Task context;
- reply target belongs to the same thread;
- artifact URLs/paths are not dereferenced;
- message does not add recipients;
- no model call or native input is created.

### 6.4 Progress classification

The host calculates:

```json
{
  "progress_kind": "new_proposal | new_evidence | new_counterexample | narrowed_disagreement | resolution | none",
  "progress_digest": "sha256:...",
  "loop_fingerprint": "sha256:..."
}
```

For `none`, the message may still be stored, but it cannot schedule another round or model wake. Stable repeated loops coalesce into one manager attention item.

### 6.5 Result

Return exact message/delivery identity, payload digest, thread revision, recipient generations and attention outcome. Reuse existing mailbox reply/cancellation binding where practical; do not duplicate message text per recipient.

## 7. Thread reads and closure

### 7.1 `coordination.inbox`

Read-only, cursor/page bounded.

Filters:

```text
unread_only
requires_reply
Task/Attempt
speech_act
topic_kind
state
created_after_ms
```

Default response is headers only:

```json
{
  "thread_id": "coord-...",
  "latest_message_id": "cmsg-...",
  "subject": "...",
  "topic_kind": "contract",
  "requires_reply": true,
  "deadline_ms": 0,
  "unread_messages": 2,
  "participant_count": 2,
  "freshness": "fresh",
  "coverage": "complete"
}
```

### 7.2 `coordination.thread.get`

Paged by Observation/message cursor and serialized-byte budget. It returns raw typed messages plus a compact current-state projection. It never auto-summarizes with another model.

### 7.3 `coordination.thread.resolve`

Request:

```json
{
  "client_request_id": "...",
  "thread_id": "coord-...",
  "expected_thread_revision": 9,
  "outcome": "resolved | unresolved | withdrawn | superseded",
  "resolution_summary": "...",
  "selected_proposal_revision_id": "cprev-or-null",
  "remaining_objections": ["claim-id"],
  "follow_up_operation_ids": [],
  "manager_ratification_operation_id": null
}
```

Rules:

- a participant may close its own question as withdrawn/unresolved;
- a bilateral informational thread may resolve when all required participants acknowledge the same resolution revision or the manager closes it;
- a contract thread cannot become `resolved` without exact manager ratification;
- silence/timeout never means agreement;
- closure creates no follow-up work automatically.

## 8. Contract proposal methods

## 8.1 `coordination.contract.propose`

```json
{
  "client_request_id": "...",
  "thread_id": "coord-...",
  "supersedes_revision_id": null,
  "topic": "Operation correlation key",
  "affected": {
    "paths": ["src/runtime.rs", "src/store/runtime.rs"],
    "symbols": ["RuntimeCommand"],
    "schemas": ["operation-contract-v3"]
  },
  "statement": {
    "producer": "runtime adapter",
    "consumer": "Store admission/readback",
    "identity": "controller Operation ID",
    "payload": "immutable RuntimeCommand",
    "observation_boundary": "native admission",
    "failure_semantics": "unknown after possible write",
    "versioning": "additive fields only in v3"
  },
  "acceptance_conditions": [
    "same Operation ID survives retry",
    "lost response never creates a second native input"
  ],
  "claims": [],
  "open_questions": []
}
```

The server canonicalizes and stores a digest. Revisions are immutable.

## 8.2 `coordination.contract.respond`

One method with exact enum:

```text
counterproposal
object
support
withdraw
```

An objection must name at least one:

```text
violated requirement
counterexample
evidence gap
unowned effect
identity/replay ambiguity
versioning incompatibility
```

“Disagree” without a material basis is retained but classified as no progress.

## 8.3 `coordination.contract.ratify`

Manager/operator/current GM only.

Request includes:

```text
proposal revision ID and digest
Task/Attempt revisions
current affected-scope revisions
manager decision rationale
conditions/caveats
```

Before commit:

- proposal digest unchanged;
- thread/context still current;
- Task/Attempt revision unchanged;
- exact caller authority current;
- no superseding ratification exists.

Ratification is a durable Operation. It does not prove implementation or verification.

## 9. Code-scope methods

## 9.1 `code.scope.propose`

Participant proposal:

```json
{
  "client_request_id": "...",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "assignment_id": "assignment-a",
  "mode": "exclusive_edit | shared_edit | read_review",
  "paths": ["src/runtime/**", "src/model.rs"],
  "symbols": ["RuntimeCommand"],
  "interfaces": ["operation-contract-v3"],
  "baseline_candidate_ref": "source-...",
  "reason": "implement producer side",
  "suggested_expires_at_ms": 0
}
```

This does not reserve automatically. It produces a manager attention/proposal.

## 9.2 `code.scope.accept`

Manager accepts exact proposal digest and may narrow/broaden it with an explicit recorded revision. Broad `**/*` and shared central schema scopes receive a warning requiring manager confirmation.

## 9.3 `code.scope.inspect`

Read-only projection by Task/Attempt/path/symbol/interface.

Returns:

```text
active intents
stale/unknown intents
Task/Attempt/assignment identity
actor generation
mode
baseline candidate
created/updated/expiry timestamps
manager acceptance revision
overrides
coverage/gaps
```

## 9.4 `code.scope.conflicts`

Overlap rules:

- exact path equals exact path;
- directory/glob contains exact path;
- two globs overlap conservatively where determinable;
- exact symbol/interface identity matches;
- `exclusive_edit` versus edit is conflict;
- `shared_edit` reports coordination-required overlap;
- `read_review` is informational;
- unknown glob normalization or failed repository inspection yields `unknown`, not no conflict.

The v1 overlap evaluator should use a small reviewed path-normalization/glob implementation already available in the repository dependency graph if one exists. Do not add a broad glob engine until current dependencies and path semantics are audited. Exact paths/prefixes/interfaces are enough for the first slice.

## 9.5 `code.scope.release`

Manager or exact scope owner under manager policy.

Result includes previous revision and current readback:

```json
{
  "scope_intent_id": "cscope-...",
  "previous_state": "active",
  "current_state": "released",
  "current_revision": 5,
  "readback_verified": true
}
```

TTL alone never performs release.

## 10. Git inspection tools

Git tools are read-only and use an explicitly resolved native `git` executable through a bounded owned-process boundary. No shell strings, hooks, global config mutation, credential operations, fetch, pull, checkout, reset, commit or push.

### 10.1 `git.worktree.inspect`

Commands:

```text
git worktree list --porcelain -z
git status --porcelain=v2 -z --untracked-files=all
```

Returns worktree path as a locally redacted/stable handle when exposed remotely, HEAD/ref, lock/prunable facts and dirty/untracked counts. Private absolute paths are not returned to remote profiles.

### 10.2 `git.changed_paths`

Commands selected by request:

```text
git diff --name-status -z -- <bounded pathspecs>
git diff --numstat -z -- <bounded pathspecs>
git diff --name-status -z <baseline>...<candidate>
git diff --name-only -z <baseline>...<candidate>
```

No human diff parsing. Rename/copy status remains structured.

### 10.3 `git.overlap`

Inputs:

```text
exact baseline/candidate refs
one or more active scope intent IDs
include_untracked boolean
```

The tool intersects machine-readable changed paths with active accepted scope intents. It does not infer Assignment ownership from branch names.

### 10.4 `git.history`

Command shape:

```text
git log --format=<fixed NUL-safe machine fields> --name-status -z -- <pathspecs>
```

Bounded by commit count, output bytes and deadline. Historical authorship is labelled `history`, never `active_owner`.

### 10.5 `git.blame.summary`

Command:

```text
git blame --line-porcelain <revision> -- <exact file>
```

Return bounded aggregate authors/commits for exact ranges. Do not expose emails to remote agents unless local policy explicitly permits it. Blame is provenance only.

### 10.6 `git.who_works_here`

Composes, without one giant transaction:

1. ELIOT accepted active scope intents;
2. Attempt/assignment/actor generation facts;
3. bounded worktree/status inspection;
4. optional bounded recent history/blame.

Result:

```json
{
  "active": [],
  "worktrees": [],
  "uncommitted_overlaps": [],
  "recent_history": [],
  "blame_summary": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

If Git inspection fails, ELIOT ownership facts remain available and Git coverage is partial/unknown.

## 11. Concilium methods

## 11.1 `concilium.propose`

Any established participant may submit:

```json
{
  "client_request_id": "...",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "failed_thread_id": "coord-...",
  "decision_question": "Which identity survives retry?",
  "material_conflict": "Producer and consumer cannot both satisfy current contracts.",
  "participants": [
    {"client_id":"writer-a","generation":3,"reason":"producer owner"},
    {"client_id":"writer-b","generation":7,"reason":"consumer owner"}
  ],
  "proposal_revision_ids": ["cprev-a", "cprev-b"],
  "evidence_refs": [],
  "expected_output": "recommendation or irreconcilable contract report",
  "suggested_max_rounds": 2,
  "suggested_budget": {},
  "close_condition": "manager decides after positions and one cross-review"
}
```

It creates manager attention only. No participant is invoked.

## 11.2 `concilium.preview`

Manager/operator/current GM only. Deterministically resolves:

- exact participants/generations/current assignments;
- correlation metadata (runtime/provider/model/parent context);
- read/evidence scopes;
- canonical requirements/non-goals;
- proposal/evidence digests;
- participant packet byte counts;
- round plan;
- warnings;
- plan digest.

Preview performs no model call.

## 11.3 `concilium.open`

```json
{
  "client_request_id": "...",
  "proposal_operation_id": "...",
  "plan_digest": "sha256:...",
  "confirmed_reasonable": true,
  "manager_reason": "Bilateral negotiation cannot reconcile the shared interface."
}
```

Before commit, all preview inputs are revalidated. Changed participant generation, Task revision, proposal digest or scope yields stale/conflict.

## 11.4 Round state machine

```text
planned
  -> round_1_positions
  -> round_2_cross_review
  -> optional_round_3_merge
  -> completed | unresolved | cancelled | failed
```

No generic “next speaker.” The plan names every expected response.

Round 1 packet is identical in requirements/evidence but does **not** include other participants' positions.

Round 2 packet contains only bounded structured summaries/claim IDs from the other positions.

Optional round 3 requires an explicit manager continuation Operation and changed merged-proposal digest.

## 11.5 Participant response schema

```json
{
  "position": "support | oppose | alternative | insufficient_evidence",
  "proposal_revision_id": "cprev-or-null",
  "claims": [
    {
      "claim_id": "claim-...",
      "stance": "support | oppose | uncertain",
      "fact": "...",
      "evidence_refs": [],
      "counterexample": null,
      "falsifier": "...",
      "assumptions": [],
      "confidence": "low | medium | high"
    }
  ],
  "required_change": "...",
  "unresolved_questions": []
}
```

Reject/mark invalid empty, oversized or schema-breaking output. Preserve other participants.

Do not request or store hidden chain-of-thought. Require concise inspectable grounds, facts, counterexamples and evidence.

## 11.6 `concilium.close`

Manager/GM only. Result classes:

```text
recommended
minority_report
insufficient_evidence
irreconcilable_contract
cancelled
failed
```

Result contains all valid positions, correlation metadata, dissent and selected recommendation. It is `advisory_only=true`.

No automatic contract ratification, Task mutation, assignment or model follow-up.

## 12. Reasonability engine

Pure deterministic function in `src/coordination.rs`.

Inputs:

```text
Task/Attempt/assignment validity
participant count and ownership reasons
related scopes/contracts
existing duplicate threads
recent progress fingerprints
requested operation kind
expected output and close condition
current capacity/attention state
```

Outputs:

```text
reasonable
accepted_with_warning
manager_sponsorship_required
hard_reject
```

Hard rejection is limited to:

- invalid/stale identity;
- unauthorized access;
- malformed/oversized payload;
- impossible Task/Attempt context;
- attempts to invoke a forbidden peer-assignment/action method.

Warnings include:

- more than four participants;
- broad project scope;
- duplicate thread;
- unchanged proposal/evidence loop;
- missing expected output;
- assignment-like prose pattern;
- proposed Concilium;
- recipient saturation.

A manager override is another exact Operation with reason, not a hidden config switch.

## 13. Durable representation without a new table

V1 uses existing Operations/Observations/artifacts.

### 13.1 Methods as Operations

Every mutation has the existing Operation receipt:

```text
coordination.thread.open
coordination.message.send
coordination.thread.resolve
coordination.contract.propose
coordination.contract.respond
coordination.contract.ratify
code.scope.propose
code.scope.accept
code.scope.release
concilium.propose
concilium.open
concilium.close
```

### 13.2 Observation streams

Use stable source stream IDs:

```text
coordination:thread:<thread_id>
coordination:proposal:<proposal_id>
coordination:scope:<scope_intent_id>
coordination:concilium:<concilium_id>
```

Kinds:

```text
coordination.thread_opened
coordination.message
coordination.thread_closed
coordination.contract_proposed
coordination.contract_response
coordination.contract_ratified
coordination.scope_proposed
coordination.scope_accepted
coordination.scope_released
coordination.concilium_proposed
coordination.concilium_opened
coordination.concilium_position
coordination.concilium_closed
```

`source_event_key` is the immutable entity/revision identity, enabling existing dedupe.

### 13.3 Large content

Inline validated small fields live in the Observation payload. Large bodies/diffs/logs are artifacts. The Observation stores artifact ID, digest, media type and allowed semantic role; it does not dereference a URI.

### 13.4 Queries

Start with bounded prepared queries over `kind`, `observation_id`, Operation/task/attempt IDs and JSON extraction where needed. Keep compact active indexes in memory and rebuild them from bounded current projections at host start.

Do not retrofit `migrations/001_core.sql`. If profiling proves expression/partial indexes are required, add an explicit forward migration reviewed as its own slice.

Do not add a materialized-table authority. A future cache/projection table, if needed, must be rebuildable from Operations/Observations and never decide authorization.

## 14. In-process concurrency

Reuse current Tokio and Store topology.

```text
bounded mpsc     admitted optional work (Git scans/Concilium model calls)
oneshot          one request result
watch/revision   coalesced freshness signal
SQLite DB thread durable mutation/read authority
```

Rules:

- no Git/HTTP/model call inside DB transaction;
- commit intent/plan before optional external work;
- persist result after work;
- no `tokio::broadcast` for message bodies;
- subscriptions carry small IDs/cursors only;
- slow readers receive `lagged` and resync;
- control/native reply queue has priority over Git inspection/summaries;
- one slow Concilium participant cannot block direct mailbox traffic;
- inactive clients have no dedicated task.

## 15. Safe-boundary delivery to native agents

Communication state and native delivery are separate Operations.

Delivery modes:

```text
mailbox_only
notify_manager
next_safe_boundary
```

Default is `mailbox_only`.

`notify_manager` produces an attention/freshness fact; it does not automatically invoke the model.

`next_safe_boundary` requires a manager-created delivery Operation selecting an exact native binding/session/generation and the adapter's real semantics.

There is no default current-turn steer. Exact steer uses the existing runtime method and target identity, not the coordination message itself.

Never send mail to a completed old native session merely because its logical client name matches. Recheck active binding/generation and selected native target before admission.

## 16. MCP surface

After application methods exist, add one typed MCP tool per allowed method in `src/mcp.rs`. No generic coordination passthrough.

Named profiles:

| Profile | Coordination access |
|---|---|
| observer | bounded thread/contract/scope/Concilium reads where authorized |
| reviewer | observer + messages/findings in selected review thread |
| manager | thread/proposal/scope methods for owned Attempts; Concilium propose/preview/open/close according to app authority |
| gm | manager + current GM ratification/handover-sensitive operations |

Resources:

```text
eliot://coordination/inbox
eliot://coordination/thread/{id}
eliot://coordination/contracts/pending
eliot://coordination/scopes/conflicts
eliot://concilium/{id}
```

Subscription facts are small:

```text
thread_revision_changed
requires_reply
contract_pending
scope_conflict
concilium_round_ready
concilium_closed
```

They are freshness hints only. The client reads authoritative state.

## 17. CLI surface

Prefer explicit commands, not one generic JSON shell:

```text
swarm coordination inbox
swarm coordination thread open --file request.json
swarm coordination thread get THREAD --after N --limit N
swarm coordination send THREAD --file message.json
swarm coordination resolve THREAD --file resolution.json

swarm contract propose THREAD --file proposal.json
swarm contract respond PROPOSAL --file response.json
swarm contract ratify PROPOSAL --file decision.json

swarm scope propose --file scope.json
swarm scope inspect --path ...
swarm scope conflicts --attempt ATTEMPT
swarm scope release SCOPE_ID

swarm git who-works-here --path ...
swarm git overlap --scope SCOPE_ID --candidate REF

swarm concilium propose --file proposal.json
swarm concilium preview OPERATION_ID
swarm concilium open --file confirmation.json
swarm concilium get CONCILIUM_ID
swarm concilium close CONCILIUM_ID --file result.json
```

Every mutation supports the existing global `--request-id` discipline.

## 18. Observability and Doctor

Read models expose:

```text
coordination_enabled
schema/contract revision
active thread count by Task/Attempt
requires-reply count
pending proposals/ratifications
active/stale scope intents
scope conflicts with coverage
active Concilium and planned round
loop incidents
model turns explicitly created from coordination
projection lag/gaps
Git inspector readiness/version
```

Doctor must distinguish:

```text
implemented
fixture_checked
load_checked
live_qualified
unavailable
unknown
```

No `healthy=true` based only on empty queues.

## 19. Security negative cases

1. Peer message contains JSON shaped like `task.revise`; no application dispatch occurs.
2. Tool output tells recipient to add itself as GM; ignored as data.
3. Unknown participant ID/generation rejected before commit.
4. Thread artifact belongs to another Task; access denied.
5. Participant tries to add recipient via message body; no effect.
6. Observer manually calls hidden mutation tool; rejected before IPC and again by app role.
7. Message includes external URL; no fetch.
8. Git request includes `--config`, `-c credential`, `--upload-pack` or path outside repository; rejected.
9. Worktree path returned to remote profile is redacted/stable-handle only.
10. Proposal ratification under stale GM epoch rejected.
11. Concilium participant gets read scope only; cannot write merely because invited.
12. Same request ID with changed body conflicts.
13. Same message replay returns retained identity.
14. Prompt injection cannot choose profile, credential, participant or native delivery mode.

## 20. Directed fixture tests

### Pure validation

- every enum/unknown field/byte limit;
- canonical proposal/progress/loop digests;
- reasonability classifications;
- exact path/symbol overlap;
- participant correlation metadata;
- no private path in remote result.

### Store transactions

- open/send/read/close idempotency;
- stale thread revision;
- participant generation replacement;
- reply to another thread;
- manager ratification stale Task/proposal;
- scope accept/release readback;
- no Task/Attempt state changed by peer message;
- Observation dedupe and restart projection.

### Failure/recovery

- lost response then same request replay;
- host restart between intent and result;
- recipient unavailable/deadline;
- artifact missing/corrupt;
- Git process timeout/oversize/invalid UTF-8/path;
- optional work crashes after plan commit;
- Concilium partial round recovery;
- manager declines/changes plan before open.

### Anti-spam

- no automatic model call on 1,000 inbound messages;
- repeated no-progress pair creates one attention item;
- liveness changes create zero coordination messages;
- slow reader receives lag/resync without sender blockage;
- malformed participant response does not poison others;
- no nested Concilium;
- max rounds closes advisory result.

## 21. Load and qualification plan

Synthetic host contour:

```text
10,000 registered clients
1,000 active threads
10,000 small messages/hour
200 concurrent inbox readers
1,000 active scope intents
100 simultaneous Git overlap requests (bounded worker pool)
10 simultaneous Concilium rounds
```

Measure:

```text
admission/read p50/p95/p99
DB transaction time
RSS and open handles
observation growth
subscription lag
Git process count
slow-reader isolation
control/native-reply latency under optional-work load
```

No model calls are needed for the base load contour.

Live qualification:

1. producer and consumer negotiate one real Rust interface;
2. exact proposal ratified by manager;
3. both implementations integrate without a conflicting rewrite;
4. compare total model input/output against a free-form shared-chat baseline;
5. run one justified Concilium with one minority objection;
6. verify no Task/Acceptance side effect before manager actions.

## 22. Implementation slices and ownership

### C1 — schemas/read projections

Files:

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/lib.rs
```

Deliver:

- enums/limits/digests;
- thread header/message projection;
- read-only list/get/inbox;
- no mutation/model/Git yet.

### C2 — direct thread mutations

Deliver:

- open/send/resolve/withdraw;
- exact participants/generations;
- reasonability;
- loop/progress fingerprint;
- artifact refs;
- attention projection.

### C3 — contract proposals

Deliver:

- immutable revisions;
- respond/object/support/withdraw;
- manager ratification;
- stale guards;
- registry projection.

### C4 — scope intents

Deliver:

- propose/accept/inspect/conflicts/release;
- exact path/symbol/interface overlap;
- no Git subprocess yet;
- no hard lock.

### C5 — Git inspection

Files:

```text
src/git_inspect.rs
src/git_inspect_tests.rs
```

Deliver bounded native Git commands and coverage/gap projection. No writes.

### C6 — Concilium durable plan

Deliver propose/preview/open/position/close state machine without binding any native model adapter automatically. Fixture positions can prove authority/state semantics.

### C7 — adapter execution

Managers explicitly choose participants/native routes. Reuse existing runtime send/result mechanisms where possible. Do not add a generic LLM API client to the Store.

### C8 — MCP/CLI/docs

Add profile-filtered tools/resources, CLI commands, README/module/architecture/implementation updates and qualification matrix.

## 23. Definition of done

The communication feature is not complete merely because agents can send text.

It is complete for v1 only when:

- peer communication cannot mutate assignments implicitly;
- exact participant/generation and retry identities survive restart;
- no incoming message automatically invokes a model;
- contract proposal and ratification are different durable facts;
- code-scope ownership is advisory and accurately reports unknown coverage;
- Git tools separate active ownership, worktree state and history;
- Concilium requires manager sponsorship and preserves dissent;
- full history is pull/paged, not broadcast into contexts;
- overload and slow readers do not block native reply/control paths;
- directed tests cover field failures from the evidence companion;
- one real producer/consumer negotiation shows lower context/token use than free-form chat;
- no new domain, credential or private local path appears in repository artifacts.