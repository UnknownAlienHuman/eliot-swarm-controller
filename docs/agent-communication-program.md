# Agent communication — one mailbox, direct collaboration

This is the target communication contract. It preserves autonomous agent work while
removing the current coordination platform.
[Implementation T](agent-operations/implementation.md#t--one-mailbox-not-a-coordination-platform)
owns source migration; [current audit](agent-operations/audit-current.md#findings-ar-100114)
records source evidence; [status](implementation-status.md) records landed behavior.

Governor and Manager are agent roles. A message, review request or decision addressed
to them does not imply a human relay. Operator intervention is reserved for missing
credentials/authority or owner intent not represented in current policy.

## 1. Product boundary

Eliot needs:

```text
one durable addressed mailbox
optional tiny Task/Attempt thread for grouping
exact native Reply for pending native questions/forms
ordinary Task/review/check/workspace/acceptance owners
current-state wake/resume policy
```

Eliot does not need a second collaboration operating system with participant directory,
work/contract cards, speech acts, proposals, objections, ratification, integration ACK,
council rounds, advisory code scopes and a separate watch scheduler.

Direct collaboration remains first-class. Removing the duplicate platform must not turn
routine agent work into operator relays or remove peer questions, independent research,
review, correction or automatic follow-up.

## 2. Current source diagnosis

The present direct mailbox is not one mailbox:

- `message.send` has Operation ID, generated `cmsg-*` message ID and a distinct delivery
  ID;
- reply/cancel searches settled Operation JSON through typed and legacy identity forms;
- migration 015 adds JSON expression indexes and conflict scans for those identities;
- `message.read` is the global Observation timeline filtered by recipient;
- direct mail, coordination mail, Task feedback and Check completion share that
  mailbox-mode timeline and cursor;
- GM handover derives recipient watermarks from those observations.

The useful checks are sender/recipient/scope/deadline/digest/cancel authorization. The
identity union, global timeline scan and compatibility indexes are implementation debt.

The wider coordination product currently owns additional state in:

- `store/coordination.rs` and coordination wire modules: participant registry, cards,
  consult and sparse relevance indexes;
- `store/coordination_threads.rs`: roster, Thread state, messages and contract decisions;
- `store/code_scopes.rs`: advisory scope proposals/revisions/conflicts;
- `store/integration.rs`: offer/requirement/carrier cells and ACKs;
- `store/concilium.rs`: proposals, rounds, slots, positions and manager result;
- `store/coordination_watch.rs`: active/list/notice/revisit/quarantine lifecycle;
- method policy, MCP catalog/profiles/subscriptions, CLI and fixtures.

Current launch and assigned-reviewer access still depends on Participant registration.
That scoped access must move to the retained application authority before the duplicate
registry is deleted.

## 3. Useful communication

Agents receive one current assignment with goal, requirements, selected sources, write
boundary, deliverable and verification. They ask the exact current owner when a fact is
missing and continue unrelated work where possible.

| Need | Compact mechanism |
| --- | --- |
| Understand work | TaskPacket plus on-demand exact Task/Attempt/candidate/source reads. |
| Find current owner | Current assignments, workspace leases and observed bindings/processes. Git history or newest log is not ownership. |
| Ask/answer | One bounded addressed mailbox message with optional Task/Attempt/thread and reply reference. |
| Native question/form | Exact pending request through adapter `Reply`; native permission policy first, then Governor attention. |
| Review | Candidate-specific `review.assign` / `review.submit`; not a generic message vote. |
| Resolve conflicting work | Manager/Governor revises assignments or settles a lease; mailbox itself grants no write authority. |
| Record a durable product decision | Update the owning Task/policy/source/implementation object through its normal command; message may remain evidence/reference. |

Routine implementation choices inside assigned scope need no council, ratification,
reasonability questionnaire or manager relay. A shared interface becomes authoritative
through its source/Task revision and concrete producer/consumer verification.

## 4. One durable mailbox

Normalize directed delivery into recipient-indexed rows. The admitted Operation ID is
the durable message identity.

```text
message_id = admitted Operation ID
sender_id
recipient_id
recipient_sequence
optional Task ID / Attempt ID / thread ID
kind
bounded inline body or artifact reference
optional reply_to_message_id
optional cancelled_message_id
created/admitted time
```

Suggested physical ownership:

```text
mailbox_messages(
  message_operation_id PRIMARY KEY,
  sender_id,
  recipient_id,
  recipient_sequence,
  task_id NULL,
  attempt_id NULL,
  thread_id NULL,
  kind,
  body_json_or_artifact_ref,
  reply_to_operation_id NULL,
  cancelled_operation_id NULL,
  admitted_at_ms
)
UNIQUE(recipient_id, recipient_sequence)
INDEX(recipient_id, recipient_sequence)
INDEX(reply_to_operation_id)
INDEX(cancelled_operation_id)
```

Exact SQL and bounded field types belong to Store/migrations, not this document. Do not
add a generic event table, EAV schema or mailbox service process.

### Send

`message.send` inserts the Operation and mailbox row in one transaction. Same caller and
`client_request_id` returns the original Operation. Changed input under the same request
identity conflicts. Storage proves durable admission, not model presentation or
consumption.

### Read

`message.read` pages only the authenticated recipient's rows by
`(recipient_id, recipient_sequence)`. Reading never deletes or acknowledges a message.
Same durable client identity can resume its own cursor. A lost wake is recovered by
mailbox read; no second send or model call is needed.

Task feedback, Check completion, review attention and native pending input remain in
their owning current projections. A frontend may present them beside mail, but Store
does not relabel them as messages.

### Reply

A reply names one prior message Operation ID. Recheck:

- current sender is the prior recipient;
- requested recipient is the prior sender;
- retained Task/Attempt/thread scope is compatible;
- deadline and optional referenced digest are current;
- prior message has not been cancelled where cancellation forbids reply.

The reply is a new admitted message row. It does not mutate or delete the original.
Native questions/forms do not use mailbox reply; they use exact adapter `Reply` against
the pending native request ID.

### Cancel

Cancellation names one prior message Operation ID and retains one cancellation row or
relation. Recheck sender authority, recipient/scope and prior cancellation. Cancellation
cannot prove that an already presented model turn was interrupted. It only changes the
mailbox message disposition visible to authorized readers.

Delete `cmsg-*`, random delivery IDs, legacy/typed unions, JSON expression indexes and
corruption-sentinel lookup after concrete unresolved messages move.

## 5. Attention and automatic wake

Ordinary mail does not automatically interrupt an active tool or start an unrelated
model turn. That does not make delivery manual.

- Current active agents receive a bounded notification/wake through their selected
  runtime when supported.
- Idle assigned agents are resumed or scheduled by the same current-state automation
  policy that owns their work.
- Native forms/questions use exact `Reply`, not queued `Send`.
- Unchanged attention coalesces; expiration or stale request is reported rather than
  answered by guess.
- A missing/stale recipient remains visible. Policy may resume or replace an assigned
  worker after exact disposition; mailbox never redirects silently to the newest
  process or current GM.

Known permission policy can answer automatically. A model-judgment question becomes one
addressed Governor task. Do not auto-pick the first/recommended option, deny every
request or default to a human.

Store events/subscriptions are wake hints. They carry mailbox invalidation or latest
recipient sequence; Store mailbox history is replay authority. There is no second MCP
event log.

## 6. Scoped access

Preserve base client authentication and exact Task/reviewer/participant access.
Deleting the coordination participant registry does **not** grant participants Manager
rights and does not remove assigned-worker or assigned-reviewer access.

Target grant ownership:

```text
base authenticated client
+ exact assignment/review grant
+ Task/Attempt/candidate/source scope
+ current/historical action ceiling
```

A participant can read/send only within its granted current/historical object scope.
Late authenticated result/message access remains tied to the original assignment or
review. Revocation stops new actions while preserving already admitted authorship and
results.

Native children need no extra Store principal merely to be observed. A child that calls
Eliot uses the existing scoped identity/grant path. Never pass operator or Manager bearer
credentials to a model, native child, script or gateway.

Migration order is strict:

1. inventory launch/worker/reviewer callers of `coordination.participant.register`;
2. move required grant creation/read checks to the retained authority owner;
3. prove launch, late result, assigned review and revocation paths;
4. remove duplicate registration/directory/index methods and data.

## 7. Optional tiny Task thread

Keep a thread only if a current consumer needs grouping:

```text
thread_id
Task ID / Attempt ID
creator_id
bounded participant IDs
state = open | closed
created/closed time
```

Messages remain mailbox rows. A thread grants no authority, owns no negotiation state
and has no speech-act/proposal/objection/ratification lifecycle. Closing a thread does
not accept a Task, cancel native work or erase mail.

Do not preserve current Thread fields merely for compatibility: reasonability forms,
topic workflow, speech acts, related-scope authority, selected proposal, remaining
objections, manager-ratification reference and supersession graph are deletion targets
unless a concrete retained consumer proves a grouping need.

## 8. Product decisions remain in their domains

- Consultation is an addressed message.
- Review and findings remain in the review subsystem.
- Correction is `task.request_changes` plus one exact correction message.
- Acceptance is the candidate-specific acceptance gate.
- Write conflicts are workspace leases.
- Goal continuation is native Goal and/or scheduled ordinary messages.
- Watches are current-state subscriptions or explicit automation conditions.
- Interface changes are source/Task revisions and connected verification.
- Independent research is an ordinary Task or review assignment.

A message saying “done”, peer majority, contract support or council agreement never
accepts work. Conversely, deleting those products must not remove direct peer questions,
independent research/review or automatic follow-up.

## 9. Remove the duplicate collaboration platform

| Current product | Useful fact to retain | Replacement owner | Delete together |
| --- | --- | --- | --- |
| Participant directory | Exact scoped assignment/reviewer access | Base identity + assignment/review grants | registration/disable API, directory/index metadata and duplicate projections after migration |
| Work/contract cards | Selected source/role/interface facts | Task/source/assignment projection | cards, relevance indexes, backfill and consult directory |
| Structured consult | One question and evidence references | mailbox | consult vocabulary/search workflow |
| Current Thread workflow | Optional grouping | tiny thread + mailbox | reasonability, speech acts, resolution/supersession negotiation state |
| Contract proposal/ratification | Product/interface revision and decision | Task/source + checks/review | proposal/response/objection/support/ratify/reject machinery |
| `code.scope.*` | Writer path boundary/conflict | Task path scope + workspace lease | advisory revisions, overlap indexes, override/expiry and contract coupling |
| Integration cell/ACK | Producer/consumer compatibility evidence | exact check/evidence for source/candidate | offer/requirement/carrier cells, comparisons, assumptions and ACKs |
| Concilium | Independent opinions/research | ordinary review/Task assignments | proposal/preview/round/slot/position/manager-result workflow |
| Coordination watch | Wake on current condition | subscriptions / one automation loop | active/list/notice/revisit/quarantine workflow |

For each product, remove method policy, schemas, MCP/CLI tools, profile entries,
subscription kinds, Store routes, storage/indexes/backfills, fixtures and documentation
in the same connected slice. Do not leave disabled handlers or a generic JSON facade.

## 10. GM handover and reconnect

GM handover changes current designation/epoch only. It does not rewrite Attempt owner,
message recipient, candidate author or reviewer.

Preserve current behavior:

- former GM loses GM-only actions immediately but retains its own scoped historical
  reads;
- successor reads current Task/Attempt/native attention through authoritative
  projections;
- historical mail remains with original recipient;
- recipient/report watermarks are resync hints, never acknowledgements;
- handover never starts a model, repeats native input or cancels admitted effects.

After mailbox normalization, derive recipient watermark from the mailbox recipient
sequence rather than global Observation IDs. Keep attention snapshot separate.

## 11. Finite transition

Enumerate concrete live state before deletion:

```text
unresolved direct/coordination messages needed by current work
open Threads whose grouping is still required
active assignment/reviewer grants
pending watch/Concilium/contract/integration decisions
```

For each item choose one explicit disposition:

- migrate with original sender/recipient/scope/reply/admitted time;
- finish/read back under the current owner;
- export required result/evidence;
- close/dispose with an attributable reason;
- create a new Task revision/Attempt when product intent changed.

Completed historical Operations remain readable history and need no permanent
translator. Never repeat native input or external writes to manufacture completion.

## 12. Connected implementation slices

### T0 — recipient mailbox table

- Add bounded rows and per-recipient sequence allocation.
- Dual-write only long enough to migrate concrete unresolved mail.
- Read new rows through an internal exact path before changing public `message.read`.
- Do not create another mailbox process or event store.

### T1 — direct send/read/reply/cancel

- Switch direct methods to one Operation/message ID.
- Preserve authorization and immutable relations.
- Add exact recipient keyset pagination and lost-wake recovery.
- Remove direct-message JSON identity indexes/legacy lookup after migration.

### T2 — continuity and grants

- Derive handover recipient watermarks from mailbox rows.
- Move launch/assigned-reviewer access from duplicate participant registry to retained
  grants.
- Prove reconnect, revocation, late result/message and GM handover.

### T3 — tiny thread or no thread

- Inventory actual grouping consumers.
- Keep only the minimal record if needed; otherwise messages use Task/Attempt scope.
- Migrate concrete open grouping state; remove Thread negotiation lifecycle.

### T4 — delete coordination products

- Move consultation/cards/contracts/code-scope/integration/Concilium/watch callers.
- Delete each product vertically, including public surfaces and storage.
- Keep ordinary automation connected; deletion must not introduce human steps.

### T5 — remove compatibility data paths

- Remove legacy identity translators, JSON expression indexes, backfills and old
  subscription kinds.
- Preserve completed Operation/artifact history as opaque/readable records.
- Verify no fallback silently writes old formats.

## 13. Verification

- one logical message has one durable ID and supports send/read/reply/cancel;
- same durable client reconnect resumes recipient cursor without restarting work;
- lost notification is recovered from mailbox without duplicate send/model turn;
- feedback/check attention is not stored as mail;
- exact native question is answered by `Reply`; stale reply fails;
- known permission policy works without human relay; unresolved judgment reaches
  Governor once;
- missing recipient does not silently redirect;
- participant and assigned reviewer cannot escape their Task/candidate/source scope;
- grant migration preserves launch, late result and reviewer replacement;
- GM handover preserves original recipients/authors while exposing current attention;
- review/correction/acceptance continue after coordination products are removed;
- direct peer questions and independent review/research remain available;
- no cards/contracts/integration/Concilium/code-scope/watch schemas, indexes,
  subscriptions, CLI/MCP tools or compatibility routes remain after cutover;
- damaged or unresolved transition data remains visible rather than guessed complete.
