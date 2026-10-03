# ELIOT Agent Communication Program
## Start here

**Revision:** 1 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Status:** documentation/implementation handoff. No communication feature is implemented merely because these documents exist.

## 1. Product decision

ELIOT centralizes **assignment and acceptance**, not every engineering conversation.

```text
manager assigns work
        │
        ▼
participants discover owners and current contracts
        │
        ├── read card ───────────────────────► continue
        ├── one precise ask/answer ──────────► continue
        ├── producer/consumer handshake ─────► peer-local agreement
        └── authority/global conflict ───────► manager decision
                                                   │
                                                   └── Concilium only if justified
```

Peers may coordinate existing assignments. They may not assign work, change Task authority, wake another model automatically or turn consensus into acceptance.

## 2. Required reading and precedence

Implementation agents read in this order:

1. Product architecture, module contract and [Owner Decisions](owner-decisions.md).
2. [Peer Autonomy Implementation Amendment](agent-communication-peer-autonomy-implementation.md) — current implementation sequence, `Role::Participant`, scoped credentials and fast paths. **This file supersedes I2–I4 and conflicting role/ratification examples in older handoff documents.**
3. [Peer Autonomy and Integration Handshake](agent-communication-peer-autonomy.md) — normative self-service coordination and autonomy envelope.
4. [Implementation Checklist](agent-communication-implementation-checklist.md) — source-exact current Store/mailbox/Git details except where amended above.
5. [Implementation Issue Plan](agent-communication-implementation-issues.md) — execution policy and the unchanged mailbox/scope/Git/Concilium/verification slices; use the amended sequence below.
6. [Agent Communication and Concilium](agent-communication-concilium.md) — complete architecture and non-goals.
7. [Tool Contracts](agent-communication-tool-contracts.md) — expanded schemas and recovery examples.
8. [Field Evidence and Donor Map](agent-communication-field-evidence.md) — rationale and negative cases, not product authority.

When examples disagree, use the highest applicable item in this list. Do not reconcile by inventing a third mechanism.

## 3. Current implementation sequence

```text
A1  extract existing mailbox primitive without behavior change
A2  add scoped Participant role/registration, peer directory and current work/contract cards
A3  add card-field lookup and quick ask/answer without model wake
A4  add deterministic producer/consumer comparison and peer-local autonomy classifier
A5  add generic bilateral negotiation and manager-required contract path
A6  add advisory code scopes
A7  add bounded read-only Git inspection
A8  add durable manager-sponsored Concilium state without automatic model execution
A9  expose CLI/MCP convenience tools, profiles, subscriptions and status
A10 integrated tests, load contour and live qualification
```

One Issue implements one complete slice. One manager owns its worktree/candidate. Writers do not run Cargo. The manager integrates/reviews all diffs and runs the current scoped formatting/minimal warnings-denied Clippy gate once on the final candidate. Integrated tests/load/live model work belong to A10 unless the owner explicitly advances a named check.

## 4. Common agent UX

The normal participant sees five primary tools:

```text
coordination.context.get
coordination.ask_owner
coordination.publish_contract
coordination.check_integration
coordination.peer_agree
```

Supporting reads:

```text
coordination.inbox
coordination.work_card.get/list
coordination.contract_card.get/list
coordination.ask.get/list
coordination.agreement.get/list
code.scope.inspect/conflicts
git.who_works_here
```

The participant should not manually orchestrate many low-level calls for an ordinary seam.

## 5. Authority summary

### Participant may

- read the exact current Task/Attempt neighborhood relevant to its assignment;
- publish its own work/contract card;
- discover the exact current owner of a contract/path/symbol;
- ask and answer one bounded technical question;
- publish producer offer/consumer requirement;
- acknowledge a compatible peer-local agreement;
- propose advisory scope and inspect overlap;
- propose a bilateral thread or Concilium escalation.

### Participant may not

- create/revise/claim/dispatch/accept Tasks;
- bind/release Attempts;
- control native sessions through `agent.*`;
- run checks or publish/merge;
- change roles, credentials, GM epoch, security or project policy;
- ratify a manager-required/global contract;
- open/advance/close Concilium;
- assign another peer work.

## 6. Peer-local agreement boundary

Peer-local agreement is allowed only when it:

- involves all directly affected current assignments;
- stays inside their existing scopes and canonical Task/documentation;
- changes no public/global interface beyond those assignments;
- changes no persistence, security, identity, retry/fencing/lifecycle owner, external effect, provider/model/billing or project acceptance policy;
- has a complete deterministic compatibility result;
- is acknowledged against exact revisions/digests;
- has no unresolved affected-owner objection.

Otherwise it becomes one `pending_manager` digest item. The auditor is not a routine approval hop; it later verifies the durable agreement and implementation evidence.

## 7. No-spam rules

- current contract card before message;
- one exact recipient, never broadcast;
- card field answer before peer model call;
- one unresolved ask fingerprint, not repeated reminders;
- no required politeness acknowledgement;
- no automatic model wake;
- header at safe boundary, body on explicit read;
- no durable liveness/status mail;
- no all-agent directory as ordinary UX;
- no manager copy of compatible handshakes;
- no automatic escalation timer that creates model work.

## 8. Concilium boundary

Concilium is the last escalation level, not the normal way to stitch code.

- participant may propose;
- manager/current GM previews and confirms reasonability;
- `open` creates immutable slots and starts no model;
- positions are independent and claim/evidence based;
- manager explicitly advances rounds;
- dissent and correlation are retained;
- result is advisory;
- manager separately decides;
- verification/acceptance remain separate.

## 9. Storage/runtime decision

V1 reuses:

```text
existing Store/SQLite owner
existing client registrations/meta
existing Operations and Observations
existing mailbox delivery/reply/cancellation
revisioned namespaced meta projections
existing immutable artifacts
bounded Tokio mpsc/oneshot/watch
local IPC and typed MCP facade
```

It adds no second database, broker, event store, daemon, shared chat server or per-agent background polling task.

## 10. Privacy

Repository documentation and examples contain placeholders only. Real domains, infrastructure IDs, credentials, local usernames and private paths remain local installation data and never enter Git, prompts or coordination records.