# Agent communication — compact contract

This is the active communication contract for the compact controller refactoring.
The implementation queue lives in the
[code reduction candidates](agent-operations/refactoring.md#code-reduction-candidates).
Older communication programs and numbered stages do not add completion requirements.
The changes below define the target; they do not claim that existing runtime code
has already been removed.

## 1. Useful work

The manager assigns work once. Agents read the current assignment and contact the
relevant owner directly when a fact is missing. Routine technical questions and
implementation choices inside that assignment need no manager relay, separate
ratification record or approval form. The manager resolves conflicting assignments
and changes to the requested outcome.

Keep the useful path small:

| Need | Controller behavior |
| --- | --- |
| Understand the assignment | Return Task/Attempt, expected result, relevant source references and assigned workspace/paths. Fetch detail on demand. |
| Find the right peer | Read current assignments and declared paths. Show unresolved ownership or overlap explicitly; Git history is not current ownership. |
| Ask or answer | Send one bounded message to an exact recipient in the authorized Task scope, with an optional reply reference. |
| Resolve an interface question | Discuss it directly and put the chosen interface in the source/assignment. Shared types and the actual producer/consumer establish integration. |
| Obtain independent judgment | Use ordinary review/research assignments and collect their results. The manager records the resulting decision. |

Current assignment/path views are projections of existing work, not another work
registry or lease system. An overlap is information for the people editing the
files; it does not create a second filesystem lock or transfer the assignment.

## 2. One message path

Reuse the existing authenticated IPC/MCP, mailbox and Store records. Keep exact
public tool names and schemas in the
[shared registry](../crates/swarm-contracts/src/mcp_catalog.rs) and
[MCP surfaces](mcp-canonical-surfaces-and-topologies.md); this document adds no
parallel aliases or wire format.

- Participants use their own assignment-scoped credentials. Registration and
  current Task/Attempt checks remain in
  [Store coordination](../crates/swarm-kernel-host/src/store/coordination.rs).
  A participant must not fall through to general manager/writer authority.
- Address an existing recipient. A missing or stale recipient is reported to the
  sender; do not silently redirect to a replacement process or create an agent.
- Use the existing request identity for retries and reply identity for answers.
  Sending a message needs no preview digest, agreement digest, membership ACK or
  five-field reasonability questionnaire.
- Bound bodies, recipient lists and read pages; use existing artifact references
  for large material. Keep full threads and unrelated project history out of the
  initial prompt and automatic notifications.
- Store an unanswered message for inbox reads. Supported native notifications may
  expose a short header during active work. Ordinary messages do not start an idle
  model, interrupt a tool call or schedule follow-up work.
- Mailbox storage proves storage. Claim presentation or consumption only when the
  runtime supplies that fact. Recover a lost notice through the inbox; do not
  repeat a model invocation to manufacture delivery evidence.

Read-only context and inbox operations change no work. The receiver may answer,
report unknown information or continue another part of its assignment. Unknown
facts are visible gaps, not a requirement to construct another negotiation state
machine or stop unrelated work.

## 3. Review and Goal remain product behavior

Keep the existing submission, independent review, manager disposition and
acceptance path in the [delivery contract](agent-operations/delivery.md).
Reviewers report findings for their assigned submission/candidate. A later Task
revision does not turn a late finding into evidence about the new candidate;
valid retained assignment access remains scoped to that result. Ordinary
Participant submission rights and assigned-reviewer rights remain distinct.

Peer agreement, majority opinion and a message saying “done” do not accept a Task.
Goal tracking and explicitly selected Goal continuation remain supported product
requirements. Messages and notifications do not acquire Goal ownership or turn
into an automatic continuation loop.

## 4. Remove optional workflow engines from completion scope

The following existing mechanisms are candidates for source deletion. They are
not prerequisites for the compact communication path or mandatory follow-up
implementation phases.

| Existing mechanism | Compact replacement |
| --- | --- |
| Concilium proposals, preview/open confirmation, round/slot/packet states and council-specific result records | Ordinary independent assignments/reviews followed by one manager decision. |
| Separate contract proposal/ratification/rejection lifecycle and mandatory reasonability forms for routine questions | Addressed messages plus the actual source/assignment change. |
| Generic producer-offer/consumer-requirement comparator, integration cells, membership/material digests and peer ACK ledger | Current interface/source references and verification of the concrete integration. |

Delete each selected engine with its routes, special settings and consumers;
do not leave it disabled behind a new compatibility mode. Preserve the useful
registration, direct-message and review/submission functions sharing the same
files. Keep any required inspection of actual saved data separate from the new
message path; the mere existence of an old schema is not a mandate for permanent
compatibility machinery.

## 5. Sources and implementation work

Start with the relevant source owner: [coordination](../crates/swarm-kernel-host/src/store/coordination.rs),
[threads](../crates/swarm-kernel-host/src/store/coordination_threads.rs),
[integration](../crates/swarm-kernel-host/src/store/integration.rs) or
[Concilium](../crates/swarm-kernel-host/src/store/concilium.rs).
Use [AGENTS.md](../AGENTS.md) for the common work process and
[implementation status](implementation-status.md) for what has actually changed.
There is no separate communication build, qualification or donor-adoption program.

The superseded Concilium, tool-contract, peer-autonomy and peer-implementation
specifications remain available in
[Git history](https://github.com/UnknownAlienHuman/eliot-swarm-controller/tree/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs).
Their detailed state machines and old acceptance matrices are historical design
choices, not additional instructions for the compact implementation.
