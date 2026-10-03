# ELIOT Agent Communication Program
## Start here

**Revision:** 4 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Status:** documentation/implementation handoff. No communication feature is implemented merely because these documents exist.

## 1. Product decision

ELIOT centralizes **assignment, authority and acceptance**, not every engineering conversation.

```text
strict edge                         free collaborative middle                         strict edge
-----------                         -------------------------                         -----------
manager assigns Task/Attempt  ->    discover, publish, ask, compare, coordinate, ->   protected verify/merge/accept
scoped identity/workspace           volunteer, assume, negotiate and record             external effects/policy
```

The controller is a capability and coordination service, not an approval machine.

Participants may coordinate existing assignments directly. They may not turn communication into Task assignment, recursive model spawning, scope expansion, merge, acceptance, policy change or an irreversible external effect.

The desired common path is:

```text
manager assigns work once
        │
        ▼
local relevant-neighborhood context
        │
        ├── current card/fact ───────────────────────────► continue
        ├── one precise owner ask ───────────────────────► continue
        ├── integration cell + deterministic comparison ► peer-local agreement
        ├── reversible recorded assumption ─────────────► continue and revalidate later
        └── authority/global conflict ──────────────────► one manager exception
                                                             │
                                                             └── Concilium only if justified
```

Root/General Manager sees exceptions rather than relaying routine engineering mail. The auditor verifies durable agreements and implementation evidence rather than approving conversation.

This is a design target, not a measured current capability.

## 2. Fleet-scale model

Keep four quantities separate:

```text
P = registered/discoverable participants
A = concurrently active model turns/processes
E = current material coordination edges
X = unresolved exceptions requiring manager authority
```

A correct large fleet may have thousands of registered participants while active turns remain bounded by runtime/provider/host capacity. Normal coordination is sparse and local. Manager attention follows real exceptions, not population size.

Never equate:

```text
registration with a running model
stored message with a started or informed model turn
one project with one shared room
P participants with P² communication edges
participant count with manager authority
```

## 3. Required reading and precedence

Implementation agents read in this order:

1. Product architecture, module contract and [Owner Decisions](owner-decisions.md).
2. [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md) — current product law for large populations, sparse relevance, integration cells, delivery truth, backpressure, active-turn separation and qualification. **This file supersedes older count-based sponsorship, global-directory, auto-expiry and multi-party-chat examples.**
3. [Peer Autonomy Implementation Amendment](agent-communication-peer-autonomy-implementation.md) — `Role::Participant`, scoped credentials and source-exact routing. It remains authoritative except where fleet-scale methods/sequence are amended above.
4. [Peer Autonomy and Integration Handshake](agent-communication-peer-autonomy.md) — self-service coordination and the autonomy envelope.
5. [Implementation Checklist](agent-communication-implementation-checklist.md) — current Store/mailbox/Git constraints except where amended above.
6. [Implementation Issue Plan](agent-communication-implementation-issues.md) — execution policy and unchanged mailbox/scope/Git/Concilium/verification slices; use the amended sequence below.
7. [Agent Communication and Concilium](agent-communication-concilium.md) — broader architecture and Concilium. Older illustrative schemas are non-normative when corrected by items 2–5.
8. [Tool Contracts](agent-communication-tool-contracts.md) — expanded schemas and recovery examples subject to the same precedence.
9. [Fleet-Scale Source Map](agent-communication-fleet-scale-sources.md) — Agensh, Anthropic/OpenAI/Claude, topology research, donor and user field evidence. Evidence only; not product authority.
10. [Peer Autonomy Source Map](agent-communication-peer-autonomy-sources.md) — official system behavior and earlier fleet lessons.
11. [Field Evidence and Donor Map](agent-communication-field-evidence.md) — broader evidence, not product authority.

When examples disagree, use the highest applicable item. Do not reconcile them by inventing a third mechanism.

## 4. Current implementation sequence

```text
A1  extract existing mailbox primitive without behavior change
A2  add scoped Participant role/registration, verified coordination profile,
    sparse relevance indexes and current work/contract cards
A3  add exact card-field lookup, quick ask/answer, reversible assumptions,
    truthful delivery dispositions and inbound policy without model wake
A4  add producer/consumer comparator, revisioned integration cells,
    voluntary local coordination and peer-local autonomy classifier
A5  add irreducible bilateral negotiation and manager-required/global contract path
A6  add advisory code scopes
A7  add bounded read-only Git inspection
A8  add durable manager-sponsored Concilium state without automatic model execution
A9  expose CLI/MCP convenience tools, deferred low-level tools,
    local-neighborhood/fleet views, subscriptions and metrics
A10 run integrated correctness, sparse-graph scale, staged active-turn,
    recovery, cost and live qualification
```

One Issue implements one complete slice. One manager owns its worktree/candidate. Writers do not run Cargo. The manager integrates/reviews all diffs and runs the current scoped formatting/minimal warnings-denied Clippy gate once on the final candidate. Integrated tests/load/live model work belong to A10 unless the owner explicitly advances a named check.

## 5. Common agent UX

The ordinary participant sees five primary affordances:

```text
coordination.context.get
coordination.ask_owner
coordination.publish_contract
coordination.sync_integration
coordination.peer_agree
```

`sync_integration` is the high-level operation. It reads current cards, resolves exact affected owners, updates the caller's offer/requirement, updates/coalesces a revisioned integration cell, runs the pure comparator and returns the next useful action. It does not invoke another model.

Supporting tools are deferred/discoverable:

```text
coordination.peer.find
coordination.inbox
coordination.work_card.get/list
coordination.contract_card.get/list
coordination.ask.get/list
coordination.cell.open/get/list/update/ack/supersede
coordination.integration.check/get
coordination.agreement.get/list
coordination.assumption.record/get/list
code.scope.inspect/conflicts
git.who_works_here
```

The participant should not manually orchestrate many low-level calls for an ordinary seam.

The cheapest operation that can answer the question is used:

```text
current structured field
  before quick ask
quick ask
  before integration cell/thread
pure comparison + integration cell
  before manager decision
manager decision
  before Concilium
```

## 6. Authority summary

### Participant may

- read the exact current Task/Attempt neighborhood relevant to its assignment;
- publish its own material work/contract card;
- discover the exact current owner of a contract/path/symbol;
- ask, answer, redirect, abstain or report unknown;
- publish producer/consumer/carrier offers and requirements;
- join/update a relevant integration cell;
- coordinate sequencing or volunteer as an integrator/reviewer inside existing scope;
- record a reversible local assumption and continue;
- acknowledge or object to a compatible peer-local agreement;
- propose advisory scope and inspect overlap;
- propose a bilateral thread or Concilium escalation.

### Participant may not

- create/revise/claim/dispatch/accept Tasks;
- bind/release Attempts;
- spawn, wake, resume or control another model/session;
- run checks or publish/merge through communication authority;
- change roles, credentials, GM epoch, security or project policy;
- expand scope or displace another mutation owner;
- ratify a manager-required/global contract;
- open/advance/close Concilium;
- turn prose, consensus, a voluntary intent or silence into assignment/acceptance.

## 7. Peer-local agreement boundary

Peer-local agreement is allowed when it:

- involves all directly affected current owners, not every observer/member;
- stays inside existing scopes and canonical Task/documentation;
- changes no public/global interface beyond represented assignments;
- changes no persistence, security, identity, retry/fencing/lifecycle owner, external effect, provider/model/billing or project acceptance policy;
- has a complete deterministic compatibility result with no required unknown dimension;
- is acknowledged against exact revisions/digests;
- has no unresolved affected-owner objection.

Otherwise it becomes one coalesced `pending_manager` digest item.

**Participant count alone is not a manager boundary.** A large justified seam may use a revisioned integration cell. Count creates a warning/partition suggestion; authority, relevance, unknowns and affected ownership decide escalation.

Several peers agreeing does not make a claim verified. Verification/acceptance remains protected and separate.

## 8. Integration cells, not multi-party chat

A multi-party integration cell contains revisioned structured state for one exact seam:

```text
affected current owners and bases
offers and requirements
carrier constraints
comparison per dimension
unknowns/evidence gaps
assumptions and invalidation conditions
voluntary implementation/integration intents
acknowledgements and objections
peer-local agreement or pending-manager result
```

It is not a shared transcript and does not schedule speakers or turns.

Membership is derived from exact current relations and immutable for one lineage. A changed affected-owner set creates a successor cell. Only affected owners block the relevant decision; observers do not create barrier synchronization.

## 9. Delivery truth and no-spam rules

Delivery states are distinct:

```text
stored
available
presented
consumed
held
refused
cancelled
stale
```

`stored` never implies `presented`; `presented` never implies agreement or verification.

Rules:

- current card/field before message;
- one exact recipient, never default broadcast;
- local-neighborhood query instead of all-agent directory injection;
- one unresolved ask fingerprint, not repeated reminders;
- no mandatory thanks/acknowledgement turn;
- no automatic model wake or peer-recursive spawn;
- safe-boundary header, body on explicit read;
- no durable liveness/status chatter;
- no manager copy of compatible local coordination;
- no automatic escalation timer that creates model work;
- no message body interpreted as `assign`, `dispatch`, `accept`, `merge`, `publish` or role change;
- no full conversation transcript sent by default;
- no authority/security boilerplate rendered repeatedly as human chat.

Under load, unique useful facts remain durable while presentation may degrade to pull-only/digest. Duplicate asks and unchanged material cards coalesce. Pressure/truncation returns explicit dispositions/gaps; it never becomes empty success.

## 10. Assumptions and momentum

An unanswered question does not automatically block development.

A participant may record and proceed under a `local_reversible` assumption when it stays inside current authority/scope, is reversible in the candidate and is not sensitive to security, identity, persistence, lifecycle or external effects. It records affected contract/path/symbol and an invalidation condition.

A later contradictory answer marks dependent cards/cells/work stale. It does not silently rewrite or roll back code.

Authority-sensitive or irreversible assumptions remain `manager_required`.

## 11. Concilium boundary

Concilium is the last escalation level, not the normal way to stitch code.

- participant may propose;
- manager/current GM previews and confirms reasonability;
- `open` creates immutable slots and starts no model;
- positions are independent and claim/evidence based;
- manager explicitly advances rounds;
- dissent, correlation and unknowns are retained;
- result is advisory;
- manager separately decides;
- verification/acceptance remain separate.

## 12. Storage/runtime decision

V1 reuses:

```text
existing Store/SQLite owner
existing client registrations/meta
existing Operations and Observations
existing mailbox delivery/reply/cancellation
revisioned namespaced per-object current projections
existing immutable artifacts
bounded Tokio mpsc/oneshot/watch
local IPC and typed MCP facade
```

It adds no second database, broker, event store, daemon, shared chat server or per-agent background polling task.

Registered inactive participants consume no model turn and no dedicated Tokio task. Do not store the entire fleet/graph/cell set in one giant `meta` value or reconstruct current state with a full unbounded Observation scan. A reviewed table migration is evidence-triggered by measured contention/scan/hot-row cost.

## 13. Fleet qualification

A10 validates separately:

```text
large registered population and sparse indexed state
simulated active runtime identities without paid calls
staged live active turns: 4 -> 16 -> 32 -> 64 -> 128
restart/recovery, stale generations and malformed actors
useful output/cost, duplicate work, conflicts and manager attention
cards/cells versus free-form chat and manager relay
```

The storage contour includes 10,000 Participant registrations, thousands of current cards/cells and 100,000 mixed coordination mutations with zero automatic model invocations. These are qualification targets, not current capacity claims.

A later 1,024-agent live experiment requires distributed capacity, explicit cost approval and a factorizable benchmark. It is not a first-host acceptance target.

## 14. Privacy

Repository documentation and examples contain placeholders only. Real domains, infrastructure IDs, credentials, local usernames and private paths remain local installation data and never enter Git, prompts or coordination records.