# ELIOT Agent Communication — Implementation Checklist and Clarifications

**Revision:** 2 — 2026-10-02  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Applies to:** [Agent Communication and Concilium](agent-communication-concilium.md), [Field Evidence](agent-communication-field-evidence.md), [Tool Contracts](agent-communication-tool-contracts.md)  
**Status:** source-exact implementation handoff. No product code is added by this document.

## 0. Read this first

Implementation agents use this order:

1. product architecture, module contract and Owner Policy;
2. this checklist;
3. Tool Contracts for expanded examples;
4. the normative communication program;
5. Field Evidence for rationale and negative tests.

Where an example in an older companion differs from this file, this file governs implementation details. It does not override Task/Attempt/Operation/Acceptance authority.

Do not take a single Issue named “implement communication.” Use the independent slices in §18.

## 1. Final-review corrections

| Earlier ambiguity | Required implementation |
|---|---|
| New coordination mail could duplicate `message.send` | Extract one internal mailbox delivery primitive. Preserve legacy `message.send`; build typed coordination over the same identity, scopes, deadlines, reply binding and cancellation. |
| Every participant example had a numeric generation | Do not invent one. Current `message_actor` has `generation` only when the registration has a binding generation; current `message_scope` separately carries optional binding ID/generation. Ordinary managers have null generation. |
| `assignment_id` looked like a free writer-subtask ID | It is an existing ProducerRef/native-assignment identity. Do not create a subassignment registry as a side effect of communication. |
| Examples used semantic ID prefixes | Use existing `model::new_id()` UUIDs. Prefixes in prose are not validation rules. |
| Examples used deadline `0` | Omit an optional deadline or use JSON `null`. A supplied deadline is a positive epoch-millisecond integer. |
| Messages had recipient arrays | V1 has exactly one recipient per send. There is no reply-all/broadcast path. |
| Participants could be added but no operation existed | V1 participant set is immutable. A manager opens a linked successor thread to change participants. |
| Every message CASed one thread revision | Separate structural `state_revision` and monotonic `message_seq`; concurrent legitimate replies do not stale each other. |
| Thread had automatic `expired` | No timer closes a thread. Age/deadline creates attention only. |
| Oversized bodies automatically became artifacts | No generic artifact-upload method exists. V1 uses bounded inline content or existing readable artifact references. |
| Concilium had no position submission/round transition | Add `concilium.position.submit` and manager-only `concilium.round.advance`. |
| `concilium.open` might start agents | It only commits the plan and slots. It starts no model and sends no native input. |
| Peer selected `notify_manager`/`next_safe_boundary` | V1 message delivery is mailbox-only. Attention is derived. Any native input is a separate manager-authorized runtime Operation. |
| Eight participants appeared to prohibit communication | Two is normal; above four warns. Eight is only the V1 simultaneous Concilium model-execution cap. Larger stakeholder sets use written positions/batches. |
| Custom observation streams would require duplicate events | Select a stream/key for the existing automatic Observation. Do not store a second full body. |
| Current-state lookup required replaying the entire log | Maintain a revisioned `meta` current projection in the same transaction; Operations/Observations remain immutable history. |
| Git accepted arbitrary path/ref strings | Repository comes from trusted local Task/candidate context; revisions are exact recorded object IDs; pathspecs are canonical literal repository-relative values. |
| Scope TTL implied release | TTL marks stale/needs-review only. Release is an exact Operation with readback. |
| Target load numbers sounded qualified | They are contours, not readiness claims. |

## 2. Existing contracts to preserve exactly

### 2.1 Mutation receipt

Current identity:

```text
(caller_id, client_request_id, method, canonical original request JSON)
```

Identical retry returns the retained receipt. Same request ID with changed method/payload returns `REQUEST_ID_CONFLICT`. Coordination uses this mechanism without a second outbox.

### 2.2 Current mailbox result

Current `message.send` already records:

```text
operation_id / message_id
delivery_id
sender / recipient
source_scope / target_scope
actor
payload_digest
admission_deadline_ms
delivery_deadline_ms
reply_deadline_ms
in_reply_to / reply_to
cancellation
delivery = durable_mailbox_only
```

Current `message.cancel` is a separate immutable Operation bound to `delivery_id + payload_digest`. It never rewrites the original delivery and never changes workflow state.

### 2.3 Actor and scope are different fields

Use the existing shapes; do not merge or rename them during the first slice.

Ordinary manager:

```json
{
  "actor": {
    "client_id": "manager-a",
    "role": "manager",
    "generation": null
  },
  "scope": {
    "client_id": "manager-a",
    "role": "manager",
    "binding_id": null,
    "binding_generation": null
  }
}
```

Scoped module registration:

```json
{
  "actor": {
    "client_id": "module-client",
    "role": "module",
    "generation": 3
  },
  "scope": {
    "client_id": "module-client",
    "role": "module",
    "binding_id": "binding-id",
    "binding_generation": 3
  }
}
```

`Principal.link_id` is ephemeral authenticated transport identity and must not enter durable participant identity.

### 2.4 Current role gate

Roles remain:

```text
operator
manager
observer
module
```

Current Store routing permits a module credential only `module.outcome` and `module.observe`. Do not silently allow modules to call coordination methods in C1–C7.

V1 writable coordination participants are registered operator/manager clients with exact work-context authority. An observer may receive only authorized read projections. Native subagents are not direct mailbox clients; their later delivery path is manager-mediated and separate (§14).

### 2.5 Assignment identity

Current ProducerRef stores:

```text
attempt_id
assignment_id
native_session_id
native_run_id
observation_id/disposition
```

An `assignment_id` cannot be rebound to another native run. It is not an arbitrary writer card.

A coordination participant has a tagged basis:

```json
{
  "kind": "attempt_owner",
  "attempt_id": "uuid",
  "owner_id": "manager-a"
}
```

or, only after exact ProducerRef validation:

```json
{
  "kind": "producer_ref",
  "attempt_id": "uuid",
  "assignment_id": "existing-assignment-id"
}
```

C2 must support `attempt_owner`. `producer_ref` and native participants land only when exact routing is implemented. No communication method creates a Task assignment.

## 3. Correct V1 application surface

### 3.1 Reads

```text
coordination.inbox
coordination.thread.get
coordination.thread.list
coordination.contract.get
coordination.contract.list
code.scope.inspect
code.scope.conflicts
git.worktree.inspect
git.changed_paths
git.overlap
git.history
git.blame.summary
git.who_works_here
concilium.get
concilium.list
concilium.preview
```

### 3.2 Mutations

```text
coordination.thread.open
coordination.message.send
coordination.thread.resolve
coordination.thread.withdraw
coordination.thread.supersede
coordination.contract.propose
coordination.contract.respond
coordination.contract.ratify
coordination.contract.reject
code.scope.propose
code.scope.accept
code.scope.release
concilium.propose
concilium.open
concilium.position.submit
concilium.round.advance
concilium.close
```

### 3.3 Reused existing mutation

```text
message.cancel
```

Generalize its delivery lookup to legacy and coordination deliveries. Do not add a second cancel method.

### 3.4 Deliberately absent

```text
coordination.broadcast
coordination.participant.add/remove
chat.room.create
peer.assign / peer.dispatch
concilium.next_speaker
concilium.vote_accept
generic method passthrough
shell
```

## 4. Thread data model

### 4.1 Header

```json
{
  "thread_id": "uuid",
  "task_id": "uuid",
  "task_revision": 4,
  "attempt_id": "uuid",
  "sponsor_owner_id": "manager-a",
  "topic_kind": "contract",
  "subject": "Operation correlation identity",
  "participants": [],
  "participation_bases": [],
  "state": "open",
  "state_revision": 1,
  "next_message_seq": 1,
  "supersedes_thread_id": null,
  "reasonability": {},
  "created_at_ms": 1780000000000,
  "updated_at_ms": 1780000000000
}
```

Rules:

- Task revision and Attempt are pinned;
- participants, subject, topic and original reasonability declaration are immutable;
- a later Task revision does not rewrite this thread;
- a successor thread may link to one prior thread;
- structural changes increment `state_revision`;
- ordinary mail increments only `message_seq`.

### 4.2 States

```text
open
resolved
unresolved
withdrawn
superseded
```

No automatic `expired` state.

### 4.3 One-recipient message

```json
{
  "thread_id": "uuid",
  "message_id": "uuid",
  "message_seq": 5,
  "sender_actor": {},
  "sender_scope": {},
  "recipient_actor": {},
  "recipient_scope": {},
  "speech_act": "object",
  "subject": "Native input ID cannot be the retry authority",
  "summary": "The native ID is derived after the durable controller Operation exists.",
  "inline_body": null,
  "body_ref": null,
  "evidence_refs": [],
  "proposal_revision_id": null,
  "in_reply_to": null,
  "in_reply_to_digest": null,
  "requires_reply": true,
  "reply_deadline_ms": null,
  "payload_digest": "sha256",
  "created_at_ms": 1780000000000
}
```

### 4.4 Coordination digest basis

Legacy `message.send` digest stays unchanged.

New digest:

```text
sha256(canonical JSON {
  contract: "eliot-coordination-message-v1",
  thread_id,
  message_id,
  message_seq,
  sender_actor,
  sender_scope,
  recipient_actor,
  recipient_scope,
  speech_act,
  subject,
  summary,
  inline_body,
  body_ref,
  evidence_refs,
  proposal_revision_id,
  in_reply_to
})
```

Deadlines and delivery outcome remain delivery facts outside the payload digest, matching the existing mailbox rule.

## 5. Mailbox extraction is the first code slice

Create:

```text
src/store/mailbox.rs
```

Move from `src/store/mod.rs` without public behavior change:

- legacy `message.send` admission;
- delivery lookup;
- cancellation;
- reply party/digest validation;
- actor/scope projection.

Recommended internal boundary:

```rust
struct DeliveryRequest<'a> {
    sender: &'a Principal,
    recipient_id: &'a str,
    payload_kind: &'static str,
    payload_digest: String,
    reply_to: serde_json::Value,
    admission_deadline_ms: serde_json::Value,
    delivery_deadline_ms: serde_json::Value,
    reply_deadline_ms: serde_json::Value,
}

fn admit_delivery(
    tx: &rusqlite::Transaction<'_>,
    operation_id: &str,
    request: DeliveryRequest<'_>,
) -> Result<serde_json::Value>;
```

Requirements:

- existing mailbox tests pass without changing expected legacy fields/digest;
- coordination caller supplies a versioned payload and digest;
- one canonical body per Operation;
- one recipient per Operation;
- existing `message.cancel` finds either allowed delivery method;
- Task feedback/check observations without a delivery ID are not cancellable;
- no model/runtime work occurs.

`message.read` keeps old records and additionally includes addressed `coordination.message.send` observations with their typed kind.

## 6. Concurrency and CAS

### 6.1 Structural CAS

Require `expected_state_revision` for:

```text
thread.resolve
thread.withdraw
thread.supersede
contract.ratify when selecting a contract
concilium.round.advance
concilium.close
```

### 6.2 Message send

`coordination.message.send` does not CAS the latest message count.

Inside the existing immediate transaction:

1. load current thread projection;
2. require `state=open`;
3. verify current sender authority and exact recorded participant;
4. verify recipient is an exact participant;
5. allocate `message_seq = next_message_seq`;
6. increment `next_message_seq`;
7. admit one mailbox delivery;
8. update current projection;
9. commit Operation and Observation.

Two valid participants may send concurrently without one becoming stale merely because the other message committed first.

### 6.3 Reply

A reply names exact prior message/delivery and optional digest claim. It must:

- belong to the same thread;
- reverse exact sender/recipient parties;
- reference an existing participant;
- satisfy the existing digest validation;
- never infer a reply from subject or latest message.

## 7. Deadlines and soft limits

No timer mutates a thread, Assignment or scope.

Derived attention only:

```text
reply_overdue
thread_review_recommended
scope_stale_review
```

Soft defaults:

```text
2 participants normal
warn above 4
3 open threads per participation basis
4 messages without changed progress digest
6 bilateral alternations
2 hours without progress -> review attention
```

A justified message is still stored after a soft threshold. Hard rejection is limited to authorization/identity/schema/size/path/protocol safety.

A thread may cap its participant list at 64 actor refs for request/frame safety. Larger coordination is decomposed into addressed threads or written-position batches; this is not a ban on necessary communication.

## 8. Visibility and stale work

Default readers:

- local operator;
- current owner of the pinned Attempt;
- exact participants;
- current GM under existing GM authority;
- explicitly sponsored exact reviewer where recorded.

Role `observer` alone does not grant project-wide coordination access. MCP profile and application authorization both apply.

After Attempt release/supersession:

- history remains readable under normal authority;
- peers cannot continue ordinary messages as if the assignment were current;
- manager/operator may close/supersede the thread;
- no message is automatically reassigned to a new Attempt/client generation.

## 9. Bodies and evidence

V1:

- summary required and bounded;
- inline body optional and bounded;
- `body_ref` only to an already registered artifact readable under existing product/profile rules;
- evidence refs name exact existing Operation/Observation/artifact/source/submission facts;
- no URL fetch;
- no arbitrary path;
- no generic upload endpoint.

If no suitable existing artifact exists and inline bytes exceed the limit, return `PAYLOAD_TOO_LARGE`. A generic immutable note publisher is a separate future Issue.

## 10. Persistence without a new authority

### 10.1 History

Each mutation is an existing Operation. The existing automatic Observation is the immutable event.

Add a pure selection helper at mutation commit:

```rust
fn observation_identity(
    method: &str,
    result: &serde_json::Value,
    operation_id: &str,
) -> Result<(String, String)>;
```

Default:

```text
controller / operation_id
```

Coordination:

```text
coordination:thread:<thread_id>       / operation_id
coordination:proposal:<proposal_id>   / proposal_revision_id
coordination:scope:<scope_id>         / state_revision_id
coordination:concilium:<id>           / operation_id or position/round ID
```

Do not insert a duplicate full-body observation.

### 10.2 Current projection

Use existing `meta` namespaced keys:

```text
coordination:thread:<thread_id>
coordination:proposal:<proposal_id>
coordination:scope:<scope_id>
coordination:concilium:<concilium_id>
```

The value:

- has explicit revision and source Operation ID;
- is updated in the same transaction;
- supports exact CAS and lookup;
- is a rebuildable current projection, not another ledger/database;
- can be compared by Doctor to the newest stream event.

List queries are prefix-bounded and paged. If measured load needs indexes/tables, add a forward migration in a separate Issue. Do not edit `migrations/001_core.sql` for an existing version-1 database.

### 10.3 Operation linkage

Every Task-bound coordination mutation sets exact `operations.task_id` and `operations.attempt_id`. It does not attach a binding merely because a participant owns one.

Local coordination mutations settle synchronously with precise completion conditions:

```text
coordination_thread_committed
coordination_message_committed
coordination_proposal_committed
coordination_scope_committed
concilium_plan_committed
concilium_position_committed
```

They never claim native start/terminal or Task acceptance.

## 11. Contract proposals

- revisions immutable;
- counterproposal names exact prior revision;
- objection names requirement/counterexample/evidence gap/identity/replay/versioning conflict;
- support remains advisory;
- ratification names exact digest, Task/Attempt revision and affected scope revisions;
- manager Attempt owner, local operator or current GM may ratify under existing authority checks;
- ratified, implemented, verified and accepted remain separate facts;
- Task revision change makes pending ratification stale;
- unratified implementation may be recorded only as an explicit assumption.

## 12. Scope intents

- participant proposes;
- exact Attempt owner/operator accepts or revises;
- exact path/symbol/interface matching lands before glob sophistication;
- `exclusive_edit` overlap is conflict;
- `shared_edit` overlap is coordination-required;
- `read_review` is informational;
- no hard filesystem lock;
- no waiting on TTL;
- TTL marks stale only;
- override records manager identity/reason/revision;
- release returns previous/current revision and verified readback;
- Attempt release marks scope stale/needs-review but does not erase it.

## 13. Git inspection

### 13.1 Trusted repository and revisions

- repository root comes from trusted local project/candidate/source context, never arbitrary remote input;
- baseline/candidate are full recorded 40/64-hex object IDs;
- verify object type;
- reject ref expressions/ranges/strings beginning with `-`;
- remote projections use stable redacted handles, not private absolute paths.

### 13.2 Literal paths

- repository-relative only;
- no NUL, absolute path, `..` or `.git` component;
- normalize separators;
- caller cannot supply Git magic;
- use `--literal-pathspecs` or explicit literal magic plus `--`;
- advisory glob evaluation happens in reviewed Rust code, not as arbitrary Git pathspec input.

### 13.3 Process boundary

Reuse/extract the proven source-capture builder from `src/checks/source.rs`:

```text
--no-optional-locks
--no-replace-objects
-c core.fsmonitor=false
-C <trusted canonical repository>
stdin null
remove GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE/GIT_OBJECT_DIRECTORY/GIT_ALTERNATE_OBJECT_DIRECTORIES
Windows CREATE_NO_WINDOW
```

Add:

```text
GIT_TERMINAL_PROMPT=0
GIT_PAGER=cat
PAGER=cat
no external diff
bounded stdout/stderr
bounded deadline
owned process disposition
```

Do not reuse the current unbounded `output()` helper for large history/status reads.

Allowed subcommands only:

```text
--version
rev-parse --verify <exact-oid>^{commit|tree}
worktree list --porcelain -z
status --porcelain=v2 -z --untracked-files=all
diff --name-status -z
diff --numstat -z
diff --name-only -z
log <fixed machine format> --name-status -z
blame --line-porcelain
```

No network, config write, checkout, reset, clean, commit, hook, credential or external command.

Timeout/output cap/parse error returns `coverage=partial|unknown` and exact gaps; never `no_conflict`.

### 13.4 `git.who_works_here`

It composes three separately labelled sources:

1. accepted ELIOT scope/Attempt ownership;
2. current worktree/status overlap;
3. optional bounded history/blame provenance.

Git history never becomes current assignment authority.

## 14. Concilium corrected contract

### 14.1 Methods

```text
concilium.propose          participant -> manager attention only
concilium.preview          read-only deterministic plan/digest
concilium.open             manager commits plan and slots; no model call
concilium.position.submit  exact participant/slot response
concilium.round.advance    manager commits next packet/round
concilium.get/list         bounded read
concilium.close            manager records advisory result
```

### 14.2 State

```text
proposed
planned
round_1_open
round_1_ready
round_2_open
round_2_ready
merge_available
completed
unresolved
cancelled
failed
```

### 14.3 Slot

```json
{
  "slot_id": "uuid",
  "round": 1,
  "participant_actor": {},
  "participant_scope": {},
  "participation_basis": {},
  "packet_digest": "sha256",
  "state": "pending",
  "response_operation_id": null,
  "source_result_ref": null
}
```

One participant submits only its slot. A malformed/timeout slot does not invalidate other positions.

### 14.4 C1–C7 do not start models

- registered clients pull packets and submit positions explicitly;
- `open`/`round.advance` create no `agent.send`;
- no generic LLM API client is added to Store;
- no LLM performs reasonability or speaker selection.

A later separate native-execution slice may let the manager explicitly dispatch a slot to an exact existing binding/session through normal runtime Operations and exact result provenance. Naming an agent in a plan never wakes it.

### 14.5 Participant bound

V1 active Concilium execution invokes at most eight model participants. More stakeholders may submit written positions outside the active execution; all referenced dissent is preserved.

## 15. MCP and CLI

After each application method exists:

```text
[ ] add read/mutation to Store classification/dispatch
[ ] add strict parser or model mutation allowlist
[ ] set Operation task/attempt linkage
[ ] add exact CLI command mapping
[ ] add one typed MCP tool; no passthrough
[ ] set correct read-only/destructive annotations
[ ] update MCP completeness test/count
[ ] enforce named profile in discovery and before IPC dispatch
[ ] add bounded subscription fact (ID/cursor only)
[ ] update README public method list
[ ] update Doctor/readiness status
```

MCP subscription messages are freshness hints; clients perform authoritative reads. No subscription contains a full thread body.

## 16. Rust units

Existing units to touch:

```text
src/model.rs
src/store/mod.rs
src/store/projection.rs
src/mcp.rs
src/mcp/subscriptions.rs
src/main.rs
src/doctor.rs
README.md
```

New units:

```text
src/coordination.rs
src/store/mailbox.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/git_inspect.rs
src/git_inspect_tests.rs
src/mcp/coordination_tests.rs
```

Rules:

- one existing SQLite owner thread;
- short transactions;
- no Git/HTTP/model call in transaction;
- bounded `mpsc` only for optional admitted work;
- `oneshot` for result;
- `watch`/revision for coalesced freshness;
- no `tokio::broadcast` of bodies;
- slow reader gets lag/resync;
- control/native reply work outranks Git/Concilium optional work;
- inactive clients own no background task.

## 17. Security negative cases

```text
[ ] peer text shaped as task.revise is data only
[ ] tool output cannot choose role/profile/credential/GM
[ ] unknown/stale participant rejected
[ ] observer cannot discover or manually dispatch hidden mutation
[ ] participant cannot add recipients through body text
[ ] external URL is not fetched
[ ] artifact from another unauthorized context is not exposed
[ ] Git option/ref/path injection rejected
[ ] remote Git result has no private absolute path/email by default
[ ] stale GM/Attempt owner cannot ratify
[ ] Concilium participant gains no write authority from invitation
[ ] identical retry stable; changed payload conflicts
[ ] no native input/model call from thread/Concilium open
```

## 18. Implementation slices

### P0 — implementation Issues

Create one Issue per slice below. Every Issue names exact files, methods, negative cases and non-goals. Do not assign the entire program to one agent.

### P1 — mailbox extraction only

Files:

```text
src/store/mailbox.rs
src/store/mod.rs
src/store/mailbox_tests.rs
```

Acceptance:

- no public/result/digest behavior changes;
- existing mailbox tests pass;
- no coordination method yet.

### P2 — schemas and reads

Files:

```text
src/coordination.rs
src/store/coordination.rs
src/store/coordination_tests.rs
src/store/mod.rs
src/model.rs
src/lib.rs
```

Acceptance:

- strict validation;
- exact actor/scope/participation basis;
- no fake generation/subassignment;
- state/message revisions split;
- bounded reads;
- no mutations/Git/model.

### P3 — direct threads

Acceptance:

- open/send/resolve/withdraw/supersede;
- one recipient;
- shared mailbox primitive;
- existing cancellation works;
- immutable participants;
- no auto wake;
- stale/released Attempt behavior;
- deadline attention only.

### P4 — contract proposals

Acceptance:

- immutable revisions;
- exact responses;
- authority/digest/Task/scope CAS;
- ratified/implemented/verified distinct.

### P5 — scope intents

Acceptance:

- propose/accept/inspect/conflicts/release;
- exact matching first;
- no hard lock;
- TTL not release;
- override/release readback.

### P6 — Git inspector

Acceptance:

- trusted root/exact OIDs/literal paths;
- machine formats and untracked files;
- bounded process/output/time;
- no mutation/network/private path;
- incomplete inspection is a gap.

### P7 — Concilium durable state

Acceptance:

- propose/preview/open/position.submit/round.advance/get/list/close;
- manager-only plan transitions;
- no model call;
- blind positions and preserved dissent;
- malformed slot isolation;
- no implicit continuation or Task effect.

### P8 — MCP/CLI/profiles/docs

Add typed methods, profile gates, subscriptions, CLI, README and status. No generic tool.

### P9 — optional native participant execution

Separate Issue after P7 qualification:

- manager explicitly chooses exact binding/session/slot;
- existing runtime Operations deliver/read result;
- exact provenance and unknown-outcome handling;
- no Store-owned generic LLM client;
- no automatic fallback/model substitution.

### P10 — qualification

Run directed fixtures, restart/lost-response/generation cases, concurrent replies, loop coalescing, slow readers, bounded Git failures, Windows process behavior, one real producer/consumer negotiation and one justified Concilium. Measure context/tokens against shared-chat baseline.

## 19. Required test/check touchpoints

### Application

```text
[ ] strict request fields/enums/limits
[ ] receipt replay/conflict
[ ] exact task/attempt linkage
[ ] authorization/visibility
[ ] Observation stream/event identity
[ ] meta revision/current projection
[ ] report/attention/Doctor
```

### Mailbox

```text
[ ] old message.send/read/cancel unchanged
[ ] old records without delivery digest remain explicit legacy cases
[ ] coordination reply reverses exact parties
[ ] one recipient only
[ ] deadline absent/null/positive behavior
[ ] no auto delete/ack workflow mutation
```

### Recovery

```text
[ ] host restart/current projection consistency
[ ] no native replay/model wake
[ ] released Attempt and stale actor
[ ] partial Concilium rounds retained
[ ] Git crash/time/size gap
```

### Anti-spam

```text
[ ] 1,000 inbound messages cause zero model calls
[ ] repeated no-progress pair -> one attention incident
[ ] liveness changes -> zero coordination messages
[ ] slow reader cannot block sender/native reply
[ ] no nested Concilium
[ ] max rounds requires manager action
```

Manager runs the repository-required scoped formatting/Clippy and the tests named by each implementation Issue. Writers follow the current Owner Policy; they do not reinterpret this program as permission to run broad Cargo workflows.

## 20. Definition of done

V1 is complete only when:

- peer communication cannot assign or mutate work implicitly;
- exact identity and retries survive restart;
- incoming mail never invokes a model;
- proposal and ratification are separate facts;
- scope ownership is advisory and incomplete coverage is honest;
- Git distinguishes active ownership, worktree state and history;
- Concilium requires manager sponsorship, explicit positions/rounds and preserved dissent;
- full history is pull/paged rather than broadcast;
- optional work cannot block native reply/control paths;
- field-failure fixtures pass;
- one real negotiation demonstrates lower context/token use than free-form shared chat;
- repository artifacts contain no private deployment identifiers.

## 21. Implementation law

```text
Peers may communicate freely enough to resolve a real technical dependency.
They must state why, address exact participants and produce inspectable progress.
The controller never converts conversation into assignment, authority, model execution or acceptance by itself.
```

The smallest correct first delivery is the behavior-preserving mailbox extraction. The smallest useful feature delivery after that is one durable bilateral thread. Do not begin with Concilium, distributed transport, semantic search, broadcast or autonomous model scheduling.