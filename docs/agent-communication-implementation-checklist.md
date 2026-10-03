# ELIOT Agent Communication — Implementation Checklist and Clarifications

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Applies to:** [Agent Communication and Concilium](agent-communication-concilium.md), [Field Evidence](agent-communication-field-evidence.md), [Tool Contracts](agent-communication-tool-contracts.md)  
**Status:** normative implementation clarification. No product code is added by this document.

## 0. Precedence

This file resolves implementation ambiguities found during the final source-level review of PR #22.

Until the three longer design documents are consolidated after implementation, this file has precedence on:

- existing mailbox reuse;
- participant and assignment identity;
- exact method names;
- thread/message revisions;
- Concilium position/round methods;
- artifact limits;
- Git command safety;
- persistence/projection details;
- implementation order.

It does not override the product architecture, module contract, Owner Policy or Task/Attempt/Acceptance authority.

## 1. Corrections found in the final review

| Ambiguity in the program | Implementation decision |
|---|---|
| A new `coordination.message.send` could duplicate `message.send` delivery semantics | Extract and reuse one mailbox delivery helper. Legacy `message.send` remains compatible; coordination adds a typed payload over the same delivery identity, actor/scope, deadlines, reply binding and cancellation mechanism. |
| Examples gave every actor a numeric `generation` | Do not invent one. Use the current durable mailbox `ActorRef`: client ID, role and optional binding ID/generation. An ordinary manager has null binding generation. `link_id` is ephemeral transport identity and is never persisted as actor generation. |
| `assignment_id` appeared to be an arbitrary writer subtask ID | It is not. In current ELIOT it identifies an exact ProducerRef/native assignment and is often the dispatch Operation ID. Communication context must not invent subassignments. Use a tagged participation basis. |
| Entity IDs used semantic prefixes (`coord-*`, `cmsg-*`) | Use existing `model::new_id()` UUIDs. Prefixes in examples are explanatory only and are not validation requirements. |
| Examples used timestamp `0` | Invalid under current `model::deadline`. Omit optional deadlines or use explicit JSON `null`; supplied deadlines must be positive epoch milliseconds. |
| Envelope used an array of recipients | V1 sends one addressed delivery per call. No reply-all/broadcast array. This reuses current mailbox identity without duplicating bodies or cancellation semantics. |
| Participant expansion was allowed with sponsorship but no method existed | V1 participant set is immutable. To add participants, the manager opens a successor thread linked by `supersedes_thread_id`. No hidden participant mutation. |
| Every message required `expected_thread_revision` | Split structural `state_revision` from monotonic `message_seq`. Sending a message allocates a new sequence atomically and does not contend on unrelated concurrent replies. Structural close/supersede/ratification uses CAS. |
| Thread lifecycle included automatic `expired` | Remove automatic expiry. Reply/thread age produces `overdue` attention only. Thread state remains `open`, `resolved`, `unresolved`, `withdrawn` or `superseded`. Time alone never closes or deletes coordination. |
| Large bodies were said to become artifacts | No generic artifact-upload application method exists. V1 supports bounded inline content and references only artifacts already created and readable through existing product paths. A generic note/body publisher is a separate future Issue. |
| Concilium had no method by which participants submit positions | Add `concilium.position.submit`. |
| Concilium had no explicit round transition | Add manager-only `concilium.round.advance`. No implicit next round. |
| `concilium.open` could be read as starting model work | It only commits the plan and response slots. It never starts a model. Native/client delivery is a distinct explicit action outside C1–C6. |
| `mailbox_only / notify_manager / next_safe_boundary` appeared as peer-selected delivery modes | V1 coordination delivery is always durable mailbox-only. Attention is a projection. Native safe-boundary input is a separate manager-authorized existing runtime Operation. |
| Eight participants was described as an absolute communication limit | Two is normal and more than four warns. The hard eight applies only to simultaneously invoked Concilium model participants in V1. Any number within frame/security limits may submit addressed written positions in batches. |
| Custom observation stream IDs were specified without acknowledging generic mutation behavior | Preserve one Operation and one canonical full result. Select a coordination `source_stream_id/source_event_key` for the existing automatic Observation; do not write a duplicate full-body event. |
| Active thread lookup from the full log was underspecified | Maintain a transactionally updated namespaced `meta` current-state projection. Immutable Operations/Observations remain history; the `meta` value is a rebuildable current projection in the same Store, not another database or authority. |
| Git examples accepted arbitrary revision/path strings | Revisions come from exact existing candidate/source object IDs. Repositories come from trusted Task/project/capture context. Paths are canonical repository-relative literal pathspecs; no caller-supplied Git option/ref expression. |
| Scope TTL could be mistaken for release | TTL only marks stale/needs-review. Release requires an exact Operation and verified current-state readback. |
| Performance targets looked like readiness claims | They are qualification contours only. No production claim until the exact build passes them. |

## 2. Exact existing facts the implementation must preserve

### 2.1 Request receipts

Current mutation identity is:

```text
(caller_id, client_request_id, method, exact original request JSON)
```

The Store rejects reuse under a changed method or payload and returns the retained receipt under an identical retry. Coordination uses this path unchanged.

### 2.2 Existing mailbox

Current `message.send` already records:

```text
operation_id / message_id
delivery_id
sender / recipient
source_scope / target_scope
actor { client_id, role, generation? }
payload_digest
admission_deadline_ms
delivery_deadline_ms
reply_deadline_ms
in_reply_to / reply_to
cancellation
delivery = durable_mailbox_only
```

Current `message.cancel` is a separate immutable Operation. It addresses the original delivery ID and digest and never rewrites the original record or changes workflow state.

These semantics are reused rather than reimplemented.

### 2.3 Actor identity

Canonical participant actor shape:

```json
{
  "client_id": "manager-a",
  "role": "manager",
  "binding_id": null,
  "binding_generation": null
}
```

For a scoped module:

```json
{
  "client_id": "builtin:runtime:binding:generation",
  "role": "module",
  "binding_id": "binding-id",
  "binding_generation": 3
}
```

No durable generic generation exists for an ordinary manager. Do not persist the ephemeral authenticated `link_id` as actor identity.

### 2.4 Assignment identity

Current `attempt.bind_producer` stores:

```text
attempt_id
assignment_id
native_session_id
native_run_id
observation_id
disposition
```

The `assignment_id` cannot be rebound to another native run. It is not a free-form child assignment registry.

A coordination participant uses a tagged basis:

```json
{
  "kind": "attempt_owner",
  "attempt_id": "...",
  "owner_id": "manager-a"
}
```

or:

```json
{
  "kind": "producer_ref",
  "attempt_id": "...",
  "assignment_id": "exact-existing-assignment-id"
}
```

or a manager-sponsored exact reviewer/client basis. C1 must support `attempt_owner`; `producer_ref` is added only when the existing ProducerRef can be verified. Do not create a new assignment record as a side effect of opening a thread.

## 3. Corrected V1 public method list

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

### 3.3 Reused existing method

```text
message.cancel
```

It must be generalized to recognize both legacy `message.send` and `coordination.message.send` deliveries through one mailbox lookup helper. Do not add `coordination.message.cancel`.

### 3.4 Deliberately absent

```text
coordination.broadcast
coordination.participant.add
coordination.participant.remove
chat.room.create
peer.assign
peer.dispatch
concilium.next_speaker
concilium.vote_accept
generic JSON-RPC passthrough
shell
```

## 4. Thread and message identity

### 4.1 Thread header

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

- participants are immutable for one thread;
- Task revision and Attempt are pinned;
- a later Task revision does not mutate this thread;
- one successor thread may reference the old thread;
- subject/topic/reasonability are immutable after open;
- structural changes increment `state_revision`;
- ordinary messages increment `message_seq`, not `state_revision`.

### 4.2 Message

One recipient per call:

```json
{
  "thread_id": "uuid",
  "message_id": "uuid",
  "message_seq": 5,
  "sender": {},
  "recipient": {},
  "speech_act": "object",
  "subject": "Native input ID cannot be the retry authority",
  "summary": "The native ID is derived only after the durable controller Operation exists.",
  "inline_body": null,
  "body_ref": null,
  "evidence_refs": [],
  "proposal_revision_id": "uuid-or-null",
  "in_reply_to": "message-id-or-null",
  "in_reply_to_digest": "sha256-or-null",
  "requires_reply": true,
  "reply_deadline_ms": null,
  "payload_digest": "sha256",
  "created_at_ms": 1780000000000
}
```

### 4.3 Payload digest

Legacy `message.send` keeps its existing digest basis unchanged.

Coordination uses a versioned basis:

```text
sha256(canonical JSON {
  contract: "eliot-coordination-message-v1",
  thread_id,
  message_id,
  message_seq,
  sender actor/scope,
  recipient actor/scope,
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

## 5. Mailbox refactor before feature code

### 5.1 New internal unit

Add:

```text
src/store/mailbox.rs
```

Move from `src/store/mod.rs` without behavior change:

- legacy `message.send` admission;
- `find_delivery`;
- `cancel_message`;
- reply party/digest validation;
- actor/scope projection.

Keep the existing mailbox tests green before adding coordination.

### 5.2 Shared helper

Recommended internal shape:

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

The helper records delivery identity/scopes. The caller builds and stores its own versioned payload.

### 5.3 Backward compatibility

Legacy `message.send` result shape and payload digest must remain byte/field compatible with current tests.

`message.read` continues to return legacy mail. It additionally returns coordination deliveries addressed to the same client, with their typed kind preserved.

`message.cancel` looks up delivery by a shared allowlist of mailbox-delivery methods. It cannot cancel Task feedback/check observations that never had a matching delivery ID.

## 6. Thread concurrency

### 6.1 Structural CAS

Require `expected_state_revision` for:

```text
thread.resolve
thread.withdraw
thread.supersede
contract.ratify when it changes selected thread contract
concilium close/advance where applicable
```

### 6.2 Message sequence

`coordination.message.send` does not require a current message count. Inside the existing immediate transaction:

1. read current thread header;
2. require `state=open`;
3. verify sender/recipient participants and Task/Attempt context;
4. allocate `message_seq = next_message_seq`;
5. increment `next_message_seq`;
6. commit Operation, delivery, Observation and current projection atomically.

Two legitimate participants may send concurrently without one becoming stale merely because the other message committed first.

### 6.3 Reply binding

A reply names exact prior message/delivery and optional digest claim. The prior message must belong to the same thread and reverse the sender/recipient parties. Reuse existing mailbox reply validation; do not infer reply from subject or latest message.

## 7. Thread lifecycle and deadlines

V1 states:

```text
open
resolved
unresolved
withdrawn
superseded
```

No timer changes state.

Deadlines produce derived attention:

```text
reply_overdue
thread_review_recommended
scope_stale_review
```

They do not:

- delete a message;
- release a scope;
- mark agreement;
- wake another model;
- cancel work;
- close a thread.

## 8. Visibility

Default read authority:

- local operator;
- current Attempt owner for the pinned Attempt;
- exact thread participants;
- current GM when operating under the normal GM authority contract;
- explicitly sponsored exact reviewer where recorded.

An observer does not gain project-wide coordination visibility from the role alone. MCP profile and application authorization must both pass.

A thread participant does not automatically gain access to every Task artifact. Existing artifact/application checks and remote-profile restrictions remain in force.

## 9. Bodies and artifacts

### 9.1 V1

- summary: required and bounded;
- inline body: optional and bounded;
- `body_ref`: may reference only an already registered immutable artifact visible to the caller and usable under current product policy;
- evidence refs: exact existing artifact/Operation/Observation/source/submission references;
- no arbitrary URL fetch;
- no arbitrary filesystem path;
- no generic coordination artifact upload.

If the caller has no existing suitable artifact and the body exceeds the inline limit, reject with `PAYLOAD_TOO_LARGE`. Do not silently write an untracked file or invent a new generic upload endpoint.

### 9.2 Future

A general immutable note/body publisher, if needed, is a separate Issue with provenance, byte limits, access rules and no-overwrite publication. It is not hidden inside C2.

## 10. Durable representation

### 10.1 Authoritative history

Each mutation remains an existing durable Operation. The automatic Observation remains the event history.

Add a pure helper used by generic mutation commit:

```rust
fn observation_identity(
    method: &str,
    result: &serde_json::Value,
    operation_id: &str,
) -> Result<(String, String)>;
```

Default:

```text
source_stream_id = controller
source_event_key = operation_id
```

Coordination examples:

```text
coordination:thread:<thread_id>       message/state operation ID
coordination:proposal:<proposal_id>   proposal revision ID
coordination:scope:<scope_id>         state revision ID
coordination:concilium:<id>           round/position/state ID
```

The existing Observation payload contains the canonical Operation result. Do not insert a second full-body event.

### 10.2 Current-state projection

Use namespaced current values in existing `meta`:

```text
coordination:thread:<thread_id>
coordination:proposal:<proposal_id>
coordination:scope:<scope_id>
coordination:concilium:<concilium_id>
```

Properties:

- updated in the same transaction as the Operation/Observation;
- includes explicit revision and source Operation ID;
- current-state CAS reads this value;
- history remains Operations/Observations;
- it is not a second database or external ledger;
- Doctor can compare current projection revision with latest event;
- no startup full-log scan is required for ordinary exact lookup.

Prefix list queries are bounded and paged. If measured query cost requires an index/table, add a forward schema migration as a separate reviewed slice. Do not edit `001_core.sql` in place for an existing database.

### 10.3 Operation linkage

Every coordination mutation with Task context updates its Operation row with exact `task_id` and `attempt_id`. No native binding is attached merely because a participant has one.

Local coordination mutations settle synchronously with completion conditions such as:

```text
coordination_thread_committed
coordination_message_committed
coordination_proposal_committed
coordination_scope_committed
concilium_plan_committed
concilium_position_committed
```

They never report `native_started`, `native_terminal` or Task acceptance.

## 11. Corrected reasonability limits

### 11.1 Soft defaults

```text
2 participants normal
warn above 4
3 open threads per participation basis
4 messages without changed progress digest
6 bilateral alternations
2 hours without progress -> review attention
```

No soft threshold rejects a justified message.

### 11.2 Protocol/security bounds

Hard rejection is permitted for:

- IPC/request byte limit;
- malformed schema;
- invalid identity/authorization;
- unsafe path/ref;
- no Task/Attempt context where required;
- too many values to validate within the declared fixed protocol cap.

V1 may cap a thread participant list at 64 actor refs for frame/memory safety. This is not a social communication limit: larger coordination uses exact bilateral threads or manager-collected written positions.

### 11.3 Concilium execution bound

At most eight model participants may be actively invoked in one Concilium execution in V1. More stakeholders may submit asynchronous written positions; the manager selects a justified active subset and the final result preserves all referenced positions.

## 12. Concilium corrected state machine

### 12.1 Methods

```text
concilium.propose          participant -> manager attention only
concilium.preview          read-only deterministic plan/digest
concilium.open             manager commits plan/slots; starts no model
concilium.position.submit  exact participant/slot response
concilium.round.advance    manager commits next bounded packet/round
concilium.get              authoritative projection
concilium.list             bounded manager/read projection
concilium.close            manager records advisory result
```

### 12.2 State

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

### 12.3 Position slots

`open` creates immutable slots:

```json
{
  "slot_id": "uuid",
  "round": 1,
  "participant": {},
  "participation_basis": {},
  "packet_digest": "sha256",
  "state": "pending",
  "response_operation_id": null,
  "source_result_ref": null
}
```

One participant submits only its exact slot. Duplicate identical retry returns the retained receipt. A changed response under the same request ID conflicts. One malformed/timeout slot does not invalidate other slots.

### 12.4 No automatic model invocation in C1–C6

For the first durable implementation:

- registered clients pull their packet and submit a position explicitly;
- `concilium.open` and `round.advance` create no `agent.send`;
- no generic model API client is added to Store;
- no model is chosen by the reasonability engine.

### 12.5 Native participants in a later slice

A later C7 may let the manager dispatch a slot to an exact existing native binding/session through a separate normal runtime Operation. It must record:

```text
concilium_id / round / slot
exact binding + generation
exact target session/run semantics
source agent.send operation
source result/artifact
unknown outcome/readback
```

The manager must explicitly request each dispatch. The resulting position is admitted only from the exact retained result. A native agent is never awakened merely because it was named in the Concilium plan.

## 13. Git inspection safety

### 13.1 Repository identity

The caller never supplies an arbitrary repository path to a remote Git tool.

Resolve the repository from one of:

- trusted local project configuration;
- exact source-capture effective request associated with the Attempt/candidate;
- locally configured route/workspace identity.

Remote output uses a stable redacted repository/worktree handle, not an absolute private path.

### 13.2 Revision identity

Baseline/candidate revisions are exact full 40/64-hex object IDs already recorded in ELIOT. Do not accept caller-supplied revision expressions, ref names, ranges or strings beginning with `-`.

Verify object type before use.

### 13.3 Pathspec identity

Pathspecs are repository-relative literal paths/prefixes:

- no NUL;
- no absolute path;
- no `..`;
- no `.git` component;
- no Git magic supplied by caller;
- normalize `/` separators;
- use `--literal-pathspecs` or explicit `:(literal)` values and `--`.

Glob-like advisory scopes are evaluated by ELIOT's reviewed scope matcher. They are not passed to Git as arbitrary pathspec magic.

### 13.4 Process builder

Reuse/extract the proven source-capture command boundary from `src/checks/source.rs`:

```text
--no-optional-locks
--no-replace-objects
-c core.fsmonitor=false
-C <trusted canonical repository>
stdin null
remove GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE/GIT_OBJECT_DIRECTORY/GIT_ALTERNATE_OBJECT_DIRECTORIES
Windows CREATE_NO_WINDOW
```

Add for inspection:

```text
GIT_TERMINAL_PROMPT=0
GIT_PAGER=cat
PAGER=cat
no external diff
bounded stdout/stderr bytes
bounded deadline
owned process disposition
```

Do not blindly reuse the current unbounded `output()` helper for potentially large history/status output. Add a bounded reader and explicit truncation/gap result.

### 13.5 Command allowlist

Allowed subcommands only:

```text
--version
rev-parse --verify <exact-oid>^{commit|tree}
worktree list --porcelain -z
status --porcelain=v2 -z --untracked-files=all
diff --name-status -z
diff --numstat -z
diff --name-only -z
log <fixed format> --name-status -z
blame --line-porcelain
```

No network, config write, checkout, reset, clean, commit, hook, credential or external command.

### 13.6 Coverage

Timeout, output cap, unsupported Git version, invalid path, repository movement or parse failure returns:

```json
{
  "coverage": "partial | unknown",
  "gaps": ["git_status_output_limit"]
}
```

Never return `no_conflict` from incomplete inspection.

## 14. Exact implementation path

## P0 — create narrow implementation Issues

Do not assign one agent “implement communication.” Create independent Issues:

1. mailbox extraction and compatibility;
2. coordination schemas/read projections;
3. direct thread mutations;
4. contract proposals/ratification;
5. scope intents;
6. bounded Git inspector;
7. Concilium durable plan/positions;
8. MCP/CLI/profile projection;
9. load/live qualification.

Each Issue names exact files, methods, negative cases and non-goals from this checklist.

## P1 — mailbox extraction only

Files:

```text
src/store/mailbox.rs
src/store/mod.rs
src/store/mailbox_tests.rs
```

Acceptance:

- no public API/result change;
- existing mailbox tests unchanged/pass;
- message reply/cancel/digest behavior byte-compatible;
- no coordination method yet.

Minimal manager gate:

```text
scoped rustfmt
cargo clippy --locked -p eliot-swarm-controller --lib --bins --no-deps -- -D warnings
existing mailbox tests named by the Issue
```

## P2 — schemas and read projection

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

- strict serde/field validation;
- actor/participation basis correct;
- no fake manager generation;
- structural/message revisions separated;
- reads paged/bounded;
- no mutations/model/Git yet;
- unknown fields rejected.

## P3 — direct threads

Acceptance:

- open/send/resolve/withdraw/supersede;
- one recipient per message;
- current mailbox delivery helper reused;
- existing `message.cancel` cancels exact coordination delivery;
- no auto model wake;
- same request retry stable;
- participant set immutable;
- overdue attention only;
- no Task/Attempt mutation except Operation linkage fields.

## P4 — contract proposals

Acceptance:

- immutable revisions;
- exact counterproposal/object/support/withdraw;
- manager/current owner/GM authority rechecked;
- ratification CAS on proposal digest, Task revision and scope revisions;
- ratified/implemented/verified remain distinct.

## P5 — scope intents

Acceptance:

- propose/accept/inspect/conflicts/release;
- exact path/symbol/interface matching first;
- no hard lock;
- TTL never releases;
- release readback verified;
- override reason retained;
- no Git subprocess yet.

## P6 — bounded Git inspector

Acceptance:

- trusted repository root and exact OIDs only;
- machine formats/NUL parsing;
- untracked files included;
- path/ref injection rejected;
- no private path remotely;
- timeout/output limit -> gap;
- no repository mutation;
- active ownership, worktree state and history clearly separated.

## P7 — Concilium durable state

Acceptance:

- propose/preview/open/position.submit/round.advance/get/close;
- manager-only open/advance/close;
- no model call;
- blind first positions;
- malformed/timeout participant isolated;
- max rounds no implicit continuation;
- minority/correlation metadata preserved;
- result advisory only.

## P8 — MCP/CLI/profile integration

Update:

```text
src/mcp.rs
src/mcp/subscriptions.rs
src/mcp/*tests.rs
src/main.rs
README.md
public method lists
Doctor/readiness docs
```

Checklist:

- add each read/mutation to the correct Store read classification;
- add each mutation to `model::validate_mutation` or typed parser;
- add exact `apply` dispatch;
- add CLI mapping without generic passthrough;
- update MCP tool-table completeness count/test;
- named profiles filter discovery and pre-dispatch;
- subscriptions carry IDs/cursors only;
- no remote path/domain/credential in output.

## P9 — optional native Concilium execution

Separate Issue only after P7 is qualified.

- manager selects exact existing participants/routes;
- existing runtime methods perform delivery/result reads;
- no Store-owned generic LLM client;
- no automatic fallback/substitution;
- each participant dispatch has an Operation and unknown-outcome reconciliation;
- communication completion never means Task acceptance.

## P10 — qualification

Run:

- idempotency/lost reply/host restart;
- old participant generation;
- concurrent bilateral send;
- malformed message and malformed Concilium position;
- no-progress loop coalescing;
- slow reader/lag-resync;
- 200 concurrent readers;
- 1,000 active thread projection;
- bounded Git status/history output;
- Windows hidden process behavior;
- one real producer/consumer contract negotiation;
- one manager-sponsored Concilium;
- token/context comparison against free-form shared chat.

Record the exact commit, platform, Git version, commands, workload and gaps. Do not convert target contours into readiness claims without evidence.

## 15. Source-level touch checklist

The implementation agent must check every item; omission is a defect.

### Application method plumbing

```text
[ ] model mutation allowlist / typed parser
[ ] Store is_read classification
[ ] Store mutate/apply dispatch
[ ] Operation task_id/attempt_id linkage
[ ] observation identity
[ ] current meta projection
[ ] exact role/owner/participant authorization
[ ] retained receipt on identical retry
[ ] changed payload conflict
[ ] report/attention projection
[ ] Doctor status/gaps
```

### Existing API surfaces

```text
[ ] CLI command mapping
[ ] `swarm call` compatibility
[ ] MCP tool schema
[ ] MCP read-only annotation
[ ] MCP completeness test/count
[ ] named profile discovery gate
[ ] named profile pre-dispatch gate
[ ] subscription filter/lag-resync
[ ] README public method list
[ ] architecture/module-contract/implementation status
```

### Mailbox compatibility

```text
[ ] legacy message.send unchanged
[ ] legacy message.read unchanged for old records
[ ] legacy message.cancel unchanged
[ ] old records without delivery digest remain explicit legacy cases
[ ] coordination reply reverses exact parties
[ ] one recipient only
[ ] deadline null/positive semantics
[ ] no auto deletion/ack mutation
```

### Recovery

```text
[ ] host restart rebuild/current projection check
[ ] no native work replay
[ ] no model wake after reconnect
[ ] stale client/module generation rejected
[ ] deadline expiry remains attention only
[ ] partial Concilium rounds retained
[ ] Git scan crash leaves durable intent/read gap only
```

## 16. Final implementation law

```text
Peers may communicate freely enough to resolve a real technical dependency.
The host requires them to state why, to address exact participants, and to produce inspectable progress.
The host never converts that conversation into work, authority, model execution or acceptance on its own.
```

The smallest correct first delivery is mailbox extraction plus one durable bilateral thread. Do not begin with Concilium, distributed transport, semantic search, broadcast or autonomous model scheduling.