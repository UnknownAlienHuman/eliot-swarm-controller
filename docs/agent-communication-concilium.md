# ELIOT Agent Communication and Concilium
## Typed coordination, contract negotiation, code-scope awareness and bounded multi-agent deliberation

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `8c9fcfe8896f14dc9341e5d81b36ec4ac109dda4`  
**Status:** documentation and implementation handoff. Nothing in this document is implemented or live-qualified merely because the document exists.

## 0. Decision

ELIOT needs an agent communication layer, but it must **not** become a second task system, a shared free-form chat, an implicit workflow engine or an automatic model-to-model wake loop.

The communication layer exists for four narrowly defined purposes:

1. discover who currently owns or is changing a relevant code/contract scope;
2. exchange bounded technical facts, questions, proposals and objections;
3. negotiate a compatible contract before independently edited parts diverge;
4. convene a manager-sponsored **Concilium** when a bilateral thread cannot resolve a material architectural conflict.

The manager remains the only actor that assigns work. Peer messages are advisory coordination facts. They do not create, revise, claim, dispatch, accept, cancel or reassign a Task.

```text
Task authority                         Coordination authority
--------------                         ----------------------
manager / current GM                   exact participants in one thread
  assigns work                           exchange facts/proposals
  selects final contract                 may object/counterpropose
  accepts/rejects output                 cannot assign peer work
  may open Concilium                     cannot ratify project policy
```

The implementation reuses existing durable Operations, Observations, mailbox identities, request receipts and immutable artifacts. It does **not** add NATS, Matrix, another SQLite database, a Git-backed second ledger or one process per agent.

## 1. Why this is needed

When a large project is split into many small assignments, agents encounter coordination failures that ordinary Task assignment cannot resolve cheaply:

- producer and consumer implement incompatible request/response shapes;
- two agents change the same shared trait, schema, feature flag or registration table;
- one agent discovers that a named caller, provider or source of truth does not exist;
- a blocker belongs to another active assignment, but the exact missing contract is unclear;
- a shared file is edited under two apparently independent Issues;
- a late design decision invalidates a parallel implementation;
- integration requires choosing one ownership, error, identity, time or replay boundary;
- agents repeat repository archaeology because they cannot ask the current owner one precise question;
- comments and status messages accidentally trigger heavyweight coding agents and cause token-amplifying ping-pong.

The legacy swarm evidence shows both sides of the problem:

- missing contract coordination caused deferred stitching, duplicated infrastructure and mutually incompatible producer/consumer work;
- broad reminders, shared comments and long-lived conversations repeatedly inflated context, woke the wrong manager or created conflicting control activity.

The design must therefore make **precise coordination cheap** and **unbounded conversation inconvenient**, without prohibiting a justified discussion.

## 2. Existing ELIOT foundation — preserve it

Current ELIOT already has the essential lower-level primitives:

- authenticated durable clients and roles;
- one `(caller_id, client_request_id)` receipt identity for every mutation;
- Operations with retained request/result/outcome;
- Observations with a monotonic cursor;
- `message.send`, directed mailbox reads, reply binding and cancellation;
- sender/recipient actor identity including generation;
- delivery and reply deadlines;
- payload digest checks for replies and cancellation;
- bounded projections and explicit gaps;
- immutable artifacts for large bodies;
- Task/Attempt/Submission/Acceptance authority separate from messages;
- GM epoch and explicit handover;
- local IPC and MCP projections over the same application methods.

The current mailbox already records that message text is **not a workflow transition**. This is the correct base. The missing part is a higher-level typed coordination contract and code-scope projection.

Do not add a new message broker until measured cross-host requirements exceed the current host/SQLite design.

## 3. Terminology

### 3.1 Assignment

Work assigned by a manager through the existing Task/Attempt path.

Only a manager or another explicitly authorized authority may create or change it.

### 3.2 Coordination thread

A bounded, addressed exchange about one technical topic. It has exact participants, Task/Attempt context, a reason for existence and a close condition.

A coordination thread does not own work and cannot change assignment state.

### 3.3 Contract proposal

An immutable proposed interface or behavior revision referenced by a coordination thread. It contains machine-readable affected scopes and acceptance conditions.

A proposal becomes the selected contract only after manager ratification or an existing authoritative document/Task revision adopts it.

### 3.4 Code-scope intent

An advisory declaration that an assignment expects to edit certain paths, symbols, schemas or interfaces. It is not a filesystem lock and does not claim authorship of historical Git lines.

### 3.5 Concilium

A manager-sponsored, bounded multi-party deliberation. It produces advisory positions, dissent and a recommendation. It cannot itself accept a Task, ratify a contract, publish code or assign work.

## 4. Research method

Donor claims must remain classified:

```text
CODE        concrete source path/behavior
DOC         official documentation
RELEASE     exact release/tag
ISSUE       concrete field report
USER        independent user experience
INFERENCE   derived design conclusion
UNVERIFIED  plausible path without required end-to-end evidence
```

A capability found on `main`, in a PR or in an Issue is not silently attributed to a stable release.

## 5. Donor findings

### 5.1 CCCC — strongest delivery protocol, not the ELIOT authority

CCCC models inter-agent communication as protocol state rather than text printed into a terminal. Its useful properties include:

- stable delivery identity;
- source/target instance and group scope;
- sender/recipient actor generation;
- message digest;
- bounded delivery/reply windows;
- explicit message modes;
- participant-scoped replies;
- cancellation bound to the original delivery and digest;
- durable pending outbox before acknowledgement;
- cursor-based Inbox consumption with a pending-read recovery record;
- explicit ambiguous/failed delivery states.

These are excellent contract patterns and largely agree with ELIOT's current mailbox direction.

Do **not** adopt the entire CCCC system:

- it brings its own group/task/runtime authority;
- field reports show expensive bridge/bootstrap behavior, large-resume failures and retry-loop risks;
- ELIOT already has one Store, one Operation ledger and direct native adapters.

Adopt the vocabulary and negative cases, not the scheduler or storage layout.

### 5.2 MCP Agent Mail — best agent-facing mail and code-reservation ergonomics

The strongest UX pattern found is **mail, not chat**:

- exact recipients and subjects;
- persistent threads;
- inbox/outbox pull views;
- acknowledgements and importance;
- no default broadcast-to-all;
- advisory file/path reservations;
- conflict reporting instead of waiting on a hard lock;
- TTL and explicit release;
- a pre-commit guard;
- messages stored outside model context and read only when relevant.

This directly supports the user's requirement: agents can coordinate without feeding every exchange into every agent's context.

However, ELIOT must not vendor this donor:

- the current license contains an OpenAI/Anthropic restriction incompatible with unrestricted reuse;
- it carries a large parallel product surface and separate Git+SQLite authority;
- field reports describe descriptor leaks, storage corruption under swarm load, slow reservation reads, unbounded reservation/agent lifecycle and expensive archive recovery.

Use it as a source-level/UX donor only.

### 5.3 Claw Council — correct Concilium boundary

Claw's important correction is that council consensus is **advisory**. Votes and responses are recorded, but downstream verification/authority decides whether work is acceptable.

Other useful properties:

- exact invited agents/personas;
- bounded rounds;
- per-agent timeout/turn limit;
- total budget;
- cancellation signal;
- compact context and final summary;
- reaching maximum rounds is a result, not a crash;
- council output does not bypass the verifier.

ELIOT Concilium follows this model, with an additional restriction: only the current manager/GM can open it.

### 5.4 Multica — the primary anti-pattern

A real Multica report measured about 18 million processed tokens on one ordinary implementation/review workflow. The dominant mechanism was not initial repository loading but:

- agent-to-agent handoff chains;
- full coding-agent invocations for simple coordination;
- repeated resume of long contexts;
- comments doubling as workflow triggers;
- retries and failures creating explanatory agent turns.

The critical design law is:

```text
human-readable communication
    != workflow transition
    != model invocation
```

ELIOT messages never automatically invoke another model. A manager explicitly converts a coordination result into a Task action when actual work is required.

### 5.5 Google A2A — useful external vocabulary

A2A separates:

- `Message`: communication that need not be a durable task;
- `Task`: stateful work lifecycle;
- `Artifact`: output produced by work;
- `contextId`: correlation across interactions.

That distinction fits ELIOT. Internal coordination messages remain messages; work remains Task/Attempt; large evidence remains Artifact.

A2A is relevant for a future cross-controller boundary, not as the local Rust bus for the current single-host product.

### 5.6 FIPA ACL — useful speech acts, not a required protocol stack

FIPA's performative vocabulary is useful because it separates communicative intent from arbitrary prose. ELIOT adopts a smaller coding-oriented subset rather than a complete FIPA platform.

### 5.7 AutoGen, Magentic-One and group-chat systems

These systems demonstrate useful termination controls, speaker selection and moderator/orchestrator roles. They also show why a default shared group transcript is inappropriate:

- every message may be broadcast;
- full history is repeatedly injected;
- weak termination can run indefinitely;
- speaker-selection loops become their own token cost.

ELIOT does not implement free-running group chat. Concilium has exact invitations, rounds and a manager-owned close condition.

### 5.8 LangGraph/OpenAI handoffs

Handoff is a transfer of control or routing decision. It is not ordinary peer communication. ELIOT keeps `gm.handover`, Task assignment and coordination threads separate.

### 5.9 Aerial, MeshTerm, Agora and other communication projects

These confirm demand for structured inter-agent communication and low-context mailboxes. They remain research sources. None justifies replacing ELIOT's Store or importing an additional authority.

### 5.10 NATS, Zenoh and Matrix

These are credible distributed transports but unnecessary for the first implementation:

- NATS JetStream adds at-least-once redelivery and distributed broker operations;
- Zenoh is valuable for geographically distributed pub/sub/query systems;
- Matrix is optimized for federated rooms and replicated event history.

ELIOT is currently a local controller with one durable SQLite owner. Tokio channels plus SQLite are simpler and more correct. A future remote deployment may introduce a transport adapter without changing the coordination contract.

## 6. Core invariants

1. **Manager assigns; peers coordinate.**
2. **A message is never a Task mutation.**
3. **No message automatically wakes a model.**
4. **No broadcast by default.**
5. **A thread has a stated work reason and close condition.**
6. **Large evidence is referenced, not copied into every message.**
7. **Active work ownership comes from ELIOT records, not `git blame`.**
8. **Git history is provenance, not current assignment authority.**
9. **Code-scope reservations are advisory and overridable with a reason.**
10. **Contract proposals are immutable; ratification is separate.**
11. **Concilium is manager-sponsored and advisory.**
12. **Silence, timeout or participant departure never fabricates agreement.**
13. **Reasonable coordination remains possible even when a soft default is exceeded.**
14. **One malformed/slow consumer cannot block native replies or unrelated work.**

## 7. Communication semantics

### 7.1 Allowed speech acts

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

Each message also declares a topic kind:

```text
contract
code_scope
integration
blocker
assumption
compatibility
review_finding
incident
status_fact
```

### 7.2 Forbidden peer semantics

Peer communication must not contain a machine-interpreted equivalent of:

```text
assign
claim_task
revise_task
dispatch_work
accept
invalidate_acceptance
publish
merge
change_role
change_gm
```

Natural-language text can mention a possible follow-up, but the host does not execute it. The manager creates the actual Task/Operation separately.

### 7.3 Typed message envelope

```json
{
  "thread_id": "coord-...",
  "message_id": "msg-...",
  "sender": {
    "client_id": "writer-a",
    "generation": 3
  },
  "recipients": [
    {"client_id": "writer-b", "generation": 7}
  ],
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "assignment_id": "assignment-a",
  "speech_act": "propose",
  "topic_kind": "contract",
  "subject": "Canonical WidgetKey serialization",
  "summary": "Use normalized UTF-8 bytes and reject aliases before persistence.",
  "body_ref": null,
  "related": {
    "contract_proposal_id": "contract-...",
    "scope_intent_ids": ["scope-..."]
  },
  "reply_to": "msg-...",
  "requires_reply": true,
  "reply_deadline_ms": 0,
  "created_at_ms": 0
}
```

The small summary is stored in the message record. Large diffs, logs, schemas or examples are immutable artifacts referenced by ID/range/digest.

## 8. Reasonability declaration

Opening a new thread requires a short machine-readable declaration:

```json
{
  "blocking_fact": "Producer and consumer selected different retry identities.",
  "decision_needed": "Choose Operation ID or native input ID as the correlation key.",
  "why_coordination_is_needed": "Both assignments are active and edit different sides of the same interface.",
  "expected_output": "One contract proposal or a precise unresolved objection.",
  "participants": [
    {"client_id": "writer-a", "reason": "producer owner"},
    {"client_id": "writer-b", "reason": "consumer owner"}
  ],
  "close_condition": "Manager ratifies one proposal or records that one assignment must be revised.",
  "related_scopes": ["src/api.rs::OperationKey", "src/store.rs::admit"]
}
```

The host performs a deterministic reasonability assessment; it does not invoke an LLM:

```text
reasonable
accepted_with_warning
manager_sponsorship_required
```

Examples requiring sponsorship:

- more than four participants;
- project-wide topic without a concrete contract/scope;
- a duplicate unresolved thread already exists;
- repeated A↔B exchanges without new proposal/evidence;
- an agent asks to convene Concilium;
- the request resembles assignment or policy change;
- expected output is unspecified.

A soft warning never blocks a genuinely necessary exchange. The sender may proceed with a concise justification; the manager is notified and may close/merge/escalate the thread.

## 9. Thread lifecycle

```text
draft
  -> open
  -> resolved | unresolved | withdrawn | superseded | expired
```

### 9.1 Open

- exact participants and generations known;
- reasonability declaration present;
- Task/Attempt context valid;
- no conflicting duplicate topic unless explicitly linked;
- no automatic model wake.

### 9.2 Resolved

A thread is resolved only by a `resolution_summary` naming:

- selected proposal/decision source;
- affected contracts/scopes;
- dissent or remaining caveats;
- manager ratification if required;
- follow-up Task/Operation IDs, if the manager created them.

### 9.3 Unresolved

Unresolved is a valid result. It reports the exact disagreement to the manager without generating another peer round automatically.

### 9.4 Merge and supersede

Duplicate threads can be merged by the manager. The old thread remains immutable evidence and points to the current one.

## 10. Anti-spam and token control

### 10.1 Direct mail by default

- normal thread: two participants;
- third/fourth participant only when each has a declared ownership reason;
- no `@all` or implicit project mailbox in the initial implementation;
- no reply-all default;
- recipient discovery is scoped to relevant active assignments and accepted code-scope intents.

### 10.2 Pull, not push, for content

A committed message produces a small freshness event. The receiving adapter/model reads the header or thread only at a safe boundary or when its manager requests it.

The system never injects all unread mail into every model turn.

Suggested views:

```text
coordination/inbox/headers
coordination/thread/{id}/summary
coordination/thread/{id}/messages?after=...
coordination/contracts/pending
coordination/scopes/conflicts
```

### 10.3 No automatic reply loop

Receiving `query`, `propose` or `object` creates an attention item. It does not invoke a model automatically.

A native manager can choose to deliver one addressed summary at the next safe boundary. Writers without a safe steer path receive it on their next manager-issued turn.

### 10.4 Soft conversation budgets

Defaults are warning thresholds, not absolute denial:

```text
participants: 2 normal, warn above 4
open threads per assignment: 3
messages without new evidence/proposal: 4
bilateral alternations: 6
thread summary bytes: 8 KiB
inline body bytes: 32 KiB
artifact references per message: 8
ordinary thread age: 2 hours
```

When a threshold is crossed:

1. no model is killed;
2. the current message can still be recorded;
3. host requests a resolution summary or manager review;
4. identical reminders are coalesced;
5. no new automatic model invocation is created.

### 10.5 Loop fingerprint

The host derives a coordination fingerprint from:

```text
thread + participant pair + speech-act sequence + proposal revision + evidence refs
```

Repeated A→B→A patterns with no changed proposal/evidence produce one incident, not another prompt.

## 11. Contract negotiation

### 11.1 Proposal object

```json
{
  "proposal_id": "contract-...",
  "topic": "Operation correlation key",
  "revision": 2,
  "supersedes": "contract-...-r1",
  "author": {"client_id": "writer-a", "generation": 3},
  "task_context": ["task-a", "task-b"],
  "affected": {
    "paths": ["src/runtime.rs", "src/store.rs"],
    "symbols": ["RuntimeCommand", "admit_input"],
    "schemas": ["operation-contract-v3"]
  },
  "statement": {
    "producer": "runtime adapter",
    "consumer": "Store admission/readback",
    "identity": "controller Operation ID",
    "payload": "immutable RuntimeCommand",
    "observation_boundary": "native input admission",
    "failure_semantics": "unknown after possible native write",
    "versioning": "additive fields only inside v3"
  },
  "acceptance_conditions": [
    "same Operation ID survives retry",
    "lost reply never creates another native input"
  ],
  "open_questions": [],
  "digest": "sha256:..."
}
```

### 11.2 Proposal rules

- revisions are immutable;
- a counterproposal names the proposal it disputes;
- objections identify a violated requirement or concrete counterexample;
- `support` is advisory evidence, not ratification;
- an agent may withdraw its own proposal but not erase history;
- only manager/current GM or an adopted canonical Task/document revision ratifies;
- ratification records exact proposal digest and affected Task revisions;
- implementation under an unratified proposal is allowed only as an explicit assumption and cannot silently redefine the peer's contract.

### 11.3 Contract registry projection

The registry is a projection over retained Operations/Observations, not a second mutable wiki.

```text
proposed
countered
objected
manager_ratified
rejected
superseded
implemented_unknown
implemented_observed
```

`implemented` and `ratified` remain separate facts.

## 12. Code-scope and Git coordination

### 12.1 Three different questions

The system must not conflate:

1. **Who is assigned to change this scope now?** — ELIOT Task/Attempt/scope intent.
2. **Which worktree/branch currently contains changes?** — live Git inspection.
3. **Who historically authored these lines?** — Git history/blame.

`git blame` cannot answer current ownership. A branch name cannot identify a submission. A path reservation cannot assign work.

### 12.2 Scope intent

```json
{
  "scope_intent_id": "scope-...",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "assignment_id": "assignment-...",
  "actor": {"client_id": "writer-a", "generation": 3},
  "paths": ["src/runtime/**", "src/model.rs"],
  "symbols": ["RuntimeCommand"],
  "interfaces": ["operation-contract-v3"],
  "mode": "exclusive_edit | shared_edit | read_review",
  "reason": "implement producer side",
  "baseline_candidate_ref": "source-...",
  "created_at_ms": 0,
  "expires_at_ms": 0
}
```

### 12.3 Advisory semantics

- scope intent reports overlap immediately;
- it does not wait for another agent and cannot deadlock storage;
- manager accepts or adjusts edit intent when assignments are created;
- agents may propose narrower changes;
- an override is possible with a reason and manager visibility;
- expiration is a hint; it does not prove the actor stopped or that work is safe to delete;
- release is explicit when work is integrated, rejected or superseded;
- read/review intents do not block edits;
- broad `**/*` scopes require manager sponsorship.

### 12.4 Git tools

Expose bounded read-only tools:

```text
code.scope.inspect
code.scope.conflicts
code.scope.propose
code.scope.release

git.worktree.inspect
git.changed_paths
git.overlap
git.history
git.who_works_here
```

`git.who_works_here` returns:

```json
{
  "active": [
    {
      "task_id": "...",
      "attempt_id": "...",
      "assignment_id": "...",
      "actor": "writer-a",
      "scope_intent_id": "...",
      "overlap": ["src/model.rs"]
    }
  ],
  "worktrees": [],
  "uncommitted_overlaps": [],
  "recent_history": [],
  "blame_summary": [],
  "coverage": "complete | partial | unknown",
  "gaps": []
}
```

### 12.5 Machine-readable Git inputs

Use native stable formats, never parse human text:

```text
git worktree list --porcelain -z
git status --porcelain=v2 -z
git diff --name-status -z
git diff --numstat -z
git log --format=<closed machine format> --name-status -z
git blame --line-porcelain
```

The tool runs through a bounded read-only process abstraction with separated argv/cwd, no shell string and no credentials in output.

### 12.6 Optional pre-commit warning

A future manager-local guard may reject or warn when the final candidate touches an active scope owned by another Attempt without a recorded coordination resolution.

This is a candidate verification rule, not a writer-side hard lock and not an automatic `git commit` hook installed globally.

## 13. Concilium

### 13.1 Sponsorship

Any agent may create `concilium.propose` with:

- exact material conflict;
- failed bilateral thread ID;
- participants and why each is necessary;
- decision question;
- evidence/proposals to compare;
- expected output;
- estimated rounds/budget;
- close condition.

Only the Task manager or current GM may execute:

```text
concilium.preview
concilium.open
concilium.close
```

`concilium.open` requires the preview plan digest and explicit `confirmed_reasonable=true`.

An ordinary agent cannot start a multi-agent chat by itself.

### 13.2 Participant selection

Default participants:

- owners of conflicting producer/consumer assignments;
- one reviewer/domain expert when materially independent;
- manager as sponsor/synthesizer, not necessarily as a model participant.

Warn above four model participants. Hard maximum for v1 is eight because larger discussion should be decomposed or handled through independent written positions.

### 13.3 Round protocol

#### Round 0 — packet

All participants receive the same compact packet:

- decision question;
- canonical requirements;
- current proposals;
- exact evidence/artifact references;
- affected scopes;
- non-goals;
- response schema;
- maximum rounds.

No full mailbox/thread/project transcript.

#### Round 1 — blind positions

Participants independently return:

```json
{
  "position": "support | oppose | alternative | insufficient_evidence",
  "proposal_id": "...",
  "reasoning_summary": "...",
  "counterexample_or_risk": "...",
  "required_change": "...",
  "confidence": "low | medium | high"
}
```

Blind first positions reduce anchoring and conversational imitation.

#### Round 2 — bounded cross-review

Each participant receives only the other position summaries and may issue one response addressing a specific disagreement.

#### Optional Round 3 — merge

Used only when the sponsor believes one small merged proposal is possible. No free-running discussion.

### 13.4 Result

```json
{
  "concilium_id": "concilium-...",
  "status": "completed | unresolved | cancelled | failed",
  "recommendation": "contract-...",
  "support": [],
  "dissent": [],
  "unresolved_questions": [],
  "evidence_refs": [],
  "rounds_used": 2,
  "budget_observed": {},
  "advisory_only": true
}
```

The manager separately ratifies a contract, revises an assignment or records no decision.

### 13.5 Concilium anti-spam rules

- no nested Concilium;
- no participant may invite another participant;
- no automatic new round from a dissenting response;
- no automatic Task or model work after completion;
- no general project chat room;
- duplicate decision question coalesces to the active Concilium;
- same proposal/evidence cannot trigger another round without sponsor justification;
- timeout yields partial advisory output, not fabricated consensus;
- consensus does not replace verification or acceptance.

## 14. Rust architecture

### 14.1 One durable owner

SQLite remains the authoritative durable owner. Tokio channels move work inside the process; they are not a second source of truth.

```text
IPC/MCP request
  -> application validation
  -> Store transaction / durable Operation
  -> Observation revision bump
  -> small watch/freshness notification
  -> recipient pulls bounded projection
```

### 14.2 No new tables in the first slice

Represent the first version through existing Operations and Observations:

```text
coordination.thread.open
coordination.message.send
coordination.thread.close
coordination.contract.propose
coordination.contract.ratify
coordination.scope.propose
coordination.scope.accept
coordination.scope.release
concilium.propose
concilium.open
concilium.close
```

Large bodies are existing immutable artifacts.

Add expression/partial indexes only after query shapes are fixed, for example:

```text
active coordination threads by Task/Attempt
messages by thread cursor
active scope intents by project/path prefix projection
pending proposals by manager
active Concilium by decision fingerprint
```

A dedicated table is justified only after profiling proves JSON projection inadequate; it must not become a second event store.

### 14.3 Observation kinds

```text
coordination.thread_opened
coordination.message
coordination.thread_closed
coordination.contract_proposed
coordination.contract_ratified
coordination.scope_changed
coordination.concilium_proposed
coordination.concilium_opened
coordination.concilium_round
coordination.concilium_closed
```

### 14.4 In-memory projections

Keep only compact active indexes:

```text
thread_id -> header/current revision
Task/Attempt -> active thread IDs
recipient -> unread header count/cursor hint
scope key -> active scope intent IDs
proposal digest -> latest revision/status
concilium fingerprint -> active ID
```

Bodies remain in SQLite/artifacts and are paged on demand.

### 14.5 Concurrency

- one existing DB thread owns SQLite;
- short transactions only;
- no Git, HTTP or model call inside a DB transaction;
- bounded `mpsc` for admitted host work;
- `oneshot` for request results;
- `watch`/revision counters for coalesced freshness;
- separate control/reply and optional projection work queues;
- fair round-robin among ready recipients/scopes;
- no Tokio `broadcast` of message bodies to all agents;
- a slow subscriber receives `lagged`/resync, not backpressure on writers;
- inactive agents consume no dedicated task/thread/process.

Complexity should scale with active threads/messages, not registered agents.

### 14.6 Payload limits

Proposed v1 defaults:

```text
inline summary: 8 KiB
inline body: 32 KiB
participants: 8 absolute
artifact refs: 8
proposal JSON: 64 KiB
thread page: existing projection byte budget
Concilium packet: 128 KiB plus references
```

These are memory/protocol limits, not limits on the complexity of the underlying Task.

## 15. Public API surface

### 15.1 Coordination

```text
coordination.thread.open
coordination.thread.get
coordination.thread.list
coordination.message.send
coordination.thread.resolve
coordination.thread.withdraw
coordination.inbox
```

### 15.2 Contracts

```text
coordination.contract.propose
coordination.contract.get
coordination.contract.list
coordination.contract.ratify
coordination.contract.reject
```

### 15.3 Code scope

```text
code.scope.propose
code.scope.accept
code.scope.inspect
code.scope.conflicts
code.scope.release
git.who_works_here
git.overlap
git.history
```

### 15.4 Concilium

```text
concilium.propose
concilium.preview
concilium.open
concilium.get
concilium.close
```

No `coordination.broadcast`, `chat.room.create`, `peer.assign` or generic `send_to_any_agent` method.

## 16. MCP and native-agent delivery

MCP exposes small tools plus read-only resources. It does not inject every message into the current model context.

Recommended resources:

```text
eliot://coordination/inbox
eliot://coordination/thread/{id}
eliot://coordination/contracts/pending
eliot://coordination/scopes/conflicts
eliot://concilium/{id}
```

Native adapters may project one attention summary at a safe boundary. Delivery mode is explicit:

```text
mailbox_only
notify_manager
next_safe_boundary
```

There is no default `steer_current_turn`. If exact current-turn steer is requested, it must use the adapter's real typed guarantee and exact target identity.

## 17. Security and authority

- only registered principals send/read their addressed coordination;
- sender generation is persisted;
- replies remain within thread participant set;
- adding a participant requires manager sponsorship or explicit participant policy;
- message/proposal identity is immutable under retry;
- same request ID with changed payload conflicts;
- large artifacts retain normal access checks;
- external URLs are data, not automatic fetch instructions;
- tool output cannot elevate role, select credentials or alter GM epoch;
- peer text is untrusted and cannot invoke application methods;
- manager ratification names exact proposal digest;
- Concilium participants receive read scopes appropriate to the question, not automatic write access;
- no real deployment domain, credential or local private path enters repository examples or messages.

## 18. Failure and recovery

### 18.1 Lost response

The caller retains `client_request_id`. Repeating the identical mutation returns the retained Operation/result. Different content conflicts.

### 18.2 Recipient unavailable

The message remains durable. Deadline expiry creates an attention/result fact; it does not fabricate rejection or wake another agent.

### 18.3 Agent generation changed

A delivery addressed to an old generation remains historical. It is not automatically reassigned to a new process with the same client name. Manager may forward/supersede it explicitly.

### 18.4 Stale contract

Ratification checks proposal digest, Task revisions and affected scope revisions. A changed proposal or Task causes stale/conflict, not silent acceptance.

### 18.5 Scope owner vanished

The scope intent becomes stale/unknown after owner disposition evidence. TTL alone does not prove safe deletion or reassignment. Manager releases/reassigns explicitly.

### 18.6 Concilium crash

Completed rounds remain durable. Recovery either continues the exact planned round under the same Concilium revision or closes as partial/unknown. It never starts a new council under the old identity automatically.

### 18.7 Overload

Optional Git scans and summaries are delayed first. Durable messages and replies remain admitted within bounded limits. Native provider replies are never blocked behind coordination telemetry.

## 19. Performance model

The first implementation should handle thousands of registered agents because registered identity has no dedicated runtime cost.

Target contour:

```text
10,000 registered clients
1,000 active coordination threads
10,000 small messages/hour
200 simultaneous inbox readers
bounded active scope intents
<100 ms p95 local admission/status under ordinary load
```

These are qualification targets, not measured claims.

Efficiency rules:

- single canonical message body, not one copied row per recipient;
- recipient delivery references/projections are small;
- prepared statements and bounded pages;
- batch recipient/header queries;
- no whole-ledger scan per bridge;
- no Git commit for every message;
- no semantic embedding/search in the first path;
- exact topic/scope indexes before full-text search;
- compact summaries generated by the participating agent only when required, not by a supervisor LLM.

## 20. Acceptance scenarios

### Authority

1. Peer proposes work; no Task/Attempt changes.
2. Peer text contains “assign this to me”; host treats it as text only.
3. Manager separately creates/revises the Task; the new Operation is linked but distinct.
4. Former GM cannot ratify after epoch change.

### Delivery and identity

5. Same request/payload returns the retained message.
6. Same request ID/different payload conflicts.
7. Reply with wrong original digest is rejected.
8. Reply from a non-participant is rejected.
9. Cancellation references exact delivery/digest and does not erase the original.
10. Old-generation recipient is not silently mapped to new generation.

### Reasonability and spam

11. Bilateral contract question opens normally.
12. Fifth participant causes sponsorship warning, not data loss.
13. Duplicate topic is linked/coalesced.
14. Repeated A↔B messages without new evidence create one manager attention item.
15. Incoming mail does not invoke a model.
16. No broadcast tool is discoverable.
17. Slow reader does not block sender/native replies.
18. Oversized body becomes artifact reference or explicit rejection before commit.

### Contracts

19. Counterproposal preserves original revision.
20. Support from all peers does not ratify automatically.
21. Manager ratifies exact digest.
22. Changed Task revision makes ratification stale.
23. Implementation status does not imply contract ratification and vice versa.
24. Unresolved objection remains visible after thread closure.

### Code scope and Git

25. Two accepted exclusive scopes overlap; both managers receive conflict evidence.
26. Shared review scope does not block edit scope.
27. Broad scope requires sponsorship.
28. Override records reason and manager identity.
29. Expired intent is stale, not proof the worktree is disposable.
30. `git blame` author differs from current scope owner; both facts are reported separately.
31. Untracked overlapping file is visible through porcelain status.
32. Partial Git inspection reports a gap rather than “no conflict”.

### Concilium

33. Ordinary agent can propose but cannot open Concilium.
34. Manager opens only with matching preview digest.
35. Round-one positions are blind/independent.
36. Maximum rounds produces advisory unresolved result, not failure or acceptance.
37. Participant timeout preserves partial positions.
38. No nested Concilium.
39. Consensus does not accept Task or ratify contract automatically.
40. Manager decision preserves dissent.
41. Concilium completion does not invoke follow-up models without explicit manager action.

## 21. Implementation program

### C0 — documentation and source pins

- merge this contract only after review;
- record exact donor commits/releases and evidence class;
- add no runtime code;
- retain the current nine-table design as the default.

### C1 — communication types and projections

- define thread/speech-act/topic/reasonability schemas;
- add read projections and exact authorization;
- keep existing `message.send` behavior compatible;
- add fixture checks for no workflow mutation.

### C2 — typed direct threads

- implement thread open/send/resolve/withdraw;
- participant/generation checks;
- deadlines, reply binding, artifact refs;
- inbox headers and bounded thread reads;
- duplicate-topic coalescing and loop incident.

### C3 — contract proposals

- immutable proposal revisions;
- counterproposal/object/support/withdraw;
- manager ratification with Task/scope guards;
- contract registry projection.

### C4 — code-scope and Git inspection

- scope propose/accept/release;
- overlap projection;
- bounded native Git read tools;
- `git.who_works_here` separating active assignment/worktree/history;
- no hard lock or automatic cleanup.

### C5 — Concilium

- propose/preview/open/get/close;
- plan digest and sponsor confirmation;
- bounded rounds and participants;
- blind first positions;
- advisory result and dissent;
- no Task/acceptance side effects.

### C6 — native/MCP integration

- MCP tools/resources under named profiles;
- safe-boundary attention delivery;
- no automatic model wake;
- remote-agent profiles cannot broaden coordination visibility.

### C7 — qualification

- directed fixture tests;
- host restart/lost reply/generation replacement;
- 200-reader and 1,000-thread contour;
- one real multi-agent producer/consumer contract negotiation;
- one manager-sponsored Concilium;
- token/context measurement against free-form chat baseline;
- Windows Git-process and path behavior.

## 22. Donor adoption register

| Donor | Adopt | Do not adopt |
|---|---|---|
| CCCC | delivery identity, generation, deadlines, participant-bound reply/cancel, cursor recovery | scheduler, group authority, full storage/runtime |
| MCP Agent Mail | mail-not-chat UX, no broadcast, advisory scopes, thread pull resources, pre-commit conflict idea | code/license, second Git+SQLite ledger, ATC, full product |
| Claw | advisory bounded council, max rounds/budget, preserved dissent, verifier separation | workflow store and automatic node replay |
| Multica | field evidence and deterministic event-vs-model separation | comment-triggered handoff chain |
| A2A | Message/Task/Artifact separation for future external boundary | local broker replacement |
| FIPA ACL | small speech-act vocabulary | complete agent platform |
| AutoGen/Magentic-One | termination/moderator lessons | broadcast/full-history group chat |
| LangGraph/OpenAI handoffs | explicit transfer-of-control distinction | treating handoff as ordinary mail |
| NATS/Zenoh/Matrix | future transport research | first local implementation |

## 23. Required documentation updates after implementation

- `docs/agent_swarm.md`: add coordination authority and Concilium boundary.
- `docs/agent_swarm.module-contract-v2.md`: add typed peer communication and safe-boundary delivery.
- `docs/agent_swarm.implementation-v6.md`: add C12 coordination slices and exact owners/files.
- `docs/documentation-program.md`: replace legacy plain-text steer/feedback loops with typed coordination.
- `README.md`: expose implemented/qualified status without claiming chat or consensus authority.
- `docs/owner-decisions.md`: record manager-only Concilium sponsorship and peer non-assignment rule.
- MCP documentation: list coordination resources/tools by profile.

## 24. Non-goals

- no general chat application;
- no always-on room;
- no automatic reply-all;
- no peer task assignment;
- no agent self-created swarm;
- no model moderator on every message;
- no automatic consensus-to-acceptance;
- no hidden continuation loop;
- no hard filesystem lock based only on TTL;
- no direct mutation of another worktree;
- no replacement of GitHub Issues/Tasks;
- no second database or event store;
- no external broker in the local first slice;
- no semantic vector search requirement;
- no real domain, credential, account or private local path in repository examples.

## 25. Source index

### ELIOT

- [Architecture](agent_swarm.md)
- [Module contract](agent_swarm.module-contract-v2.md)
- [Implementation plan](agent_swarm.implementation-v6.md)
- [Documentation Program](documentation-program.md)
- [Owner Decisions](owner-decisions.md)

### CCCC

- <https://github.com/ChesterRa/cccc/tree/v0.4.41>
- `crates/cccc-core/src/connect_delivery.rs`
- `crates/cccc-core/src/inbox.rs`

### MCP Agent Mail — research only

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust>
- README sections: mail metaphor, no broadcast, file reservations, architecture and recovery
- license rider must be reviewed before any reuse; this program authorizes no code reuse

### Claw

- <https://github.com/Enderfga/claw-orchestrator/blob/ffa595abfb6f6671ac995041f9c44f5e1a67f50f/src/kernel/nodes/council.ts>

### Multica

- <https://github.com/multica-ai/multica/issues/8753>

### Standards and frameworks

- Google A2A: <https://a2a-protocol.org/latest/specification/>
- MCP: <https://modelcontextprotocol.io/specification/>
- FIPA ACL overview: <https://www.fipa.org/repository/aclspecs.html>
- AutoGen group chat: <https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/selector-group-chat.html>
- Tokio channels: <https://tokio.rs/tokio/tutorial/channels>
- Git status porcelain: <https://git-scm.com/docs/git-status>
- Git worktree porcelain: <https://git-scm.com/docs/git-worktree>
- Git diff machine formats: <https://git-scm.com/docs/git-diff>
- Git blame porcelain: <https://git-scm.com/docs/git-blame>

## 26. Final recommendation

Implement the smallest useful vertical slice first:

```text
existing authenticated clients
  -> direct typed coordination thread
  -> immutable contract proposal
  -> advisory code-scope overlap
  -> manager resolution
```

Only after this path is measured should Concilium be enabled:

```text
failed material bilateral coordination
  -> agent proposes Concilium
  -> manager previews and confirms reasonability
  -> bounded independent positions + one cross-review
  -> advisory recommendation with dissent
  -> manager ratifies or declines
```

This gives agents enough freedom to solve real integration conflicts while preventing conversation itself from becoming an uncontrolled workflow engine or a geometric token multiplier.
