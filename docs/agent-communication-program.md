# Agent communication — one mailbox, direct collaboration

This is the target communication contract. It preserves autonomous agent work while
removing the current coordination platform. [Implementation](agent-operations/implementation.md#t--one-mailbox-not-a-coordination-platform)
owns source migration; [status](implementation-status.md) records evidence.

Governor and Manager are agent roles. A message, review request or decision addressed
to them does not imply a human relay. Operator intervention is reserved for missing
credentials/authority or owner intent not represented in current policy.

## 1. Useful communication

Agents receive one current assignment with the necessary goal, requirements, selected
sources, write boundary, deliverable and verification. They ask the exact current
owner when a fact is missing and continue unrelated work where possible.

| Need | Compact mechanism |
| --- | --- |
| Understand work | TaskPacket plus on-demand exact Task/Attempt/candidate/source reads. |
| Find current owner | Current assignments, workspace leases and observed bindings/processes. Git history or newest log is not ownership. |
| Ask/answer | One bounded addressed mailbox message with optional Task/Attempt/thread and reply reference. |
| Native question/form | Exact pending request through adapter `Reply`; native permission policy first, then Governor attention. |
| Review | Candidate-specific `review.assign` / `review.submit`; not a generic message vote. |
| Resolve conflicting work | Manager/Governor revises assignments or settles a lease; mailbox itself grants no write authority. |
| Record a durable product decision | Update the owning Task/policy/source/implementation object through its normal command, with message as evidence/reference when useful. |

Routine implementation choices inside assigned scope need no council, ratification,
reasonability questionnaire or manager relay. A shared interface becomes authoritative
through its source/Task revision and concrete producer/consumer verification.

## 2. One durable mailbox

Normalize directed delivery into recipient-indexed rows. The admitted Operation ID is
the durable message identity. Store:

```text
message/Operation ID
sender and recipient
optional Task/Attempt/thread
kind
bounded inline body or artifact reference
reply target
cancellation reference
recipient cursor
created/admitted time
```

`message.send` inserts one row atomically with its Operation. `message.read` pages the
current recipient's rows directly; reading never deletes. Reply and cancellation
reference the message Operation ID. Same durable `client_id` resumes its cursor.

Delete multiple identities (`cmsg-*`, random delivery ID beside Operation), legacy/
typed delivery unions, global observation-timeline mailbox scans, JSON expression
indexes and corruption sentinel queries after concrete unresolved messages move.

Mailbox storage proves durable admission, not model presentation or consumption. A
lost notification is recovered by mailbox read. Do not repeat a model call to create
delivery evidence.

## 3. Attention and automatic wake

Ordinary mail does not automatically interrupt an active tool or start an unrelated
model turn. That does not make delivery manual.

- Current active agents receive a bounded notification/wake through their selected
  runtime when supported.
- Idle assigned agents are resumed or scheduled by the same current-state automation
  policy that owns their work.
- Native forms/questions use exact `Reply`, not a queued `Send`.
- Unchanged attention coalesces; expiration or stale request is reported rather than
  answered by guess.
- A missing/stale recipient remains visible. Policy may resume or replace an assigned
  worker after exact disposition; mailbox never silently redirects to the newest
  process.

Known permission policy can answer automatically. A model-judgment question becomes
one addressed Governor task. Do not auto-pick the first/recommended option, deny all
requests or default to a human.

## 4. Scoped access

Preserve base client authentication and scoped Task/reviewer/participant access.
Deleting the coordination participant registry does **not** grant participants Manager
rights and does not remove assigned-worker or assigned-reviewer access.

A participant can read/send only within its granted current/historical object scope.
Late authenticated result/message access remains tied to the original assignment or
review. Revocation stops new actions while preserving already admitted authorship and
results.

Native children need no extra Store principal merely to be observed. A child that
calls Eliot uses the existing scoped identity/grant path. Never pass operator or
Manager bearer credentials to a model, native child, script or gateway.

## 5. Optional tiny Task thread

Keep a thread only if a current consumer needs grouping:

```text
thread_id
Task/Attempt scope
creator
bounded participants
open | closed
```

Messages remain mailbox rows. A thread grants no authority, owns no negotiation state
and has no speech-act/proposal/objection/ratification lifecycle. Closing a thread does
not accept a Task, cancel native work or erase mail.

## 6. Product decisions remain in their domains

- Consultation is an addressed message.
- Review and findings remain in the review subsystem.
- Correction is `task.request_changes` plus one exact correction message.
- Acceptance is the candidate-specific acceptance gate.
- Write conflicts are workspace leases.
- Goal continuation is native Goal and/or scheduled ordinary messages.
- Watches are current-state subscriptions or explicit automation conditions.
- Interface changes are source/Task revisions and connected verification.

A message saying “done”, peer majority or council agreement never accepts work.
Conversely, removing council/contract/integration products must not remove direct peer
questions, independent research/review or automatic follow-up.

## 7. Remove the duplicate collaboration platform

After mailbox/thread consumers move, delete together:

- coordination participant-token registry and duplicate directory/indexes;
- work/contract cards;
- consultation-specific schema/workflow;
- speech-act/proposal/counterproposal/objection machinery;
- contract ratification/rejection;
- generic integration offer/requirement/ACK cells;
- Concilium rounds/slots/packets;
- coordination watches that duplicate subscriptions;
- `code.scope.*`, which duplicates workspace write authority;
- associated MCP/CLI methods, policy, storage, backfills, cursors, fixtures and docs.

Keep authentication, assigned-worker/reviewer access, mailbox, optional tiny thread,
Task revisions, reviews and leases. Do not leave deleted products disabled behind a
compatibility facade.

## 8. Finite transition

Enumerate concrete unresolved directed messages and open threads required by current
work. Migrate them to normalized mailbox/thread rows with original sender, recipient,
Task scope, reply relation and admitted time. Completed historical Operations remain
readable history and need no permanent translator.

GM handover changes current designation/epoch only. It does not rewrite Attempt owner,
message recipient, candidate author or reviewer. A successor reads current
Task/Attempt/native attention through authoritative projections; historical mail stays
with its original recipient unless a new addressed handover message is admitted.

## 9. Verification

- same durable client reconnect resumes mailbox cursor without restarting work;
- one message has one durable ID and supports send/read/reply/cancel;
- lost notification is recovered from mailbox without duplicate send/model turn;
- exact native question is answered by `Reply`; stale reply fails;
- known permission policy works without human relay; unresolved judgment reaches
  Governor once;
- missing recipient does not silently redirect;
- participant cannot read unrelated Task/message or acquire Manager rights;
- GM handover preserves original recipients/authors while exposing current attention;
- review/correction/acceptance continue after coordination products are removed;
- no cards/contracts/integration/Concilium/code-scope indexes or compatibility routes
  remain.
