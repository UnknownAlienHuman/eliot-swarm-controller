# ELIOT Agent Communication, Launcher and MCP Program
## Start here

**Revision:** 5 — 2026-10-03  
**Source baseline:** `main` at `35e499ae73b622d873c44873f6993ee3fcbea87b`  
**Status:** documentation/implementation handoff. No communication, launcher or deferred-catalog capability is implemented merely because these documents exist.

## 1. Product decision

ELIOT centralizes **assignment, authority and acceptance**, not every engineering conversation.

```text
strict edge                         free collaborative middle                         strict edge
-----------                         -------------------------                         -----------
manager assigns Task/Attempt  ->    discover, publish, ask, compare, coordinate, ->   protected verify/merge/accept
scoped identity/workspace           volunteer, assume, negotiate and record             external effects/policy
```

The controller is a capability and coordination service, not an approval machine or a chat room.

The desired common path is:

```text
manager sees dashboard + ranked queue
        │
        ▼
launch.preview resolves exact Task, workspace, peers, overlap, route and MCP surface
        │
        ▼
launcher establishes one manager-owned work context and verifies actual capabilities
        │
        ▼
participant sees only its bounded relevant neighborhood
        │
        ├── current card/fact ───────────────────────────► continue
        ├── one precise owner consultation ─────────────► continue
        ├── integration cell + pure comparison ─────────► peer-local agreement
        ├── reversible recorded assumption ─────────────► continue and revalidate later
        └── authority/global conflict ──────────────────► one manager exception
                                                             │
                                                             └── Concilium only if justified
```

Root/General Manager sees exceptions rather than relaying routine engineering mail. The auditor verifies durable agreements and implementation evidence rather than approving conversation.

## 2. Four quantities that must remain separate

```text
P = registered/discoverable participants
A = concurrently active model turns/processes
E = current material coordination edges
X = unresolved exceptions requiring manager authority
```

A correct fleet may have thousands of registered participants while active turns remain bounded by runtime/provider/host capacity. Normal coordination is sparse and local. Manager attention follows real exceptions, not population size.

Never equate:

```text
registration with a running model
stored message with a started or informed model turn
one project with one shared room
P participants with P² communication edges
participant count with manager authority
visible tool with authorized tool
configured tool with runtime-visible working tool
loaded schema with expanded application authority
```

## 3. Required reading and precedence

Implementation agents read in this order:

1. Product architecture, module contract and [Owner Decisions](owner-decisions.md).
2. [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md) — fleet law, sparse relevance, integration cells, delivery truth, backpressure and active-turn separation.
3. [MCP Tool Catalog and Deferred Loading](mcp-tool-catalog-and-loading.md) — hard profiles, small role surfaces, searchable groups, pagination and runtime capability receipts.
4. [Swarm Launcher and Assignment Context](swarm-launcher-assignment-context.md) — manager dashboard/queue, launch preview, compact assignment packet, Git overlap and event-driven reminders.
5. [Peer Autonomy Implementation Amendment](agent-communication-peer-autonomy-implementation.md) — scoped `Role::Participant`, credentials and source-exact routing.
6. [Peer Autonomy and Integration Handshake](agent-communication-peer-autonomy.md) — self-service coordination and autonomy envelope.
7. [Implementation Checklist](agent-communication-implementation-checklist.md) — current Store/mailbox/Git constraints except where amended above.
8. [Implementation Issue Plan](agent-communication-implementation-issues.md) — execution policy and older slice templates; use the current sequence in §4.
9. [Agent Communication and Concilium](agent-communication-concilium.md) — broader architecture and Concilium. Older illustrative schemas are non-normative where corrected above.
10. [Tool Contracts](agent-communication-tool-contracts.md) — expanded schemas/recovery examples subject to the same precedence.
11. [Fleet-Scale Source Map](agent-communication-fleet-scale-sources.md), [MCP/Launcher Source Map](mcp-tool-catalog-sources.md), [Peer Autonomy Source Map](agent-communication-peer-autonomy-sources.md) and [Field Evidence](agent-communication-field-evidence.md) — evidence only, not product authority.

When examples disagree, use the highest applicable item. Do not reconcile them by inventing another identity, queue, role, task store, delivery path, tool authority or model loop.

## 4. Current implementation sequence

```text
A1  extract/reuse the existing mailbox primitive without behavior change

A2  add scoped Participant role/registration, verified coordination profile,
    sparse relevance indexes and current work/contract cards

A3  add exact field lookup, quick ask/answer/consultation, reversible assumptions,
    watches/reminders, truthful delivery dispositions and inbound policy

A4  add producer/consumer comparator, revisioned integration cells,
    voluntary local coordination and peer-local autonomy classifier

A5  add irreducible bilateral negotiation and manager-required/global contract path

A6  add advisory code scopes and bounded read-only Git/worktree inspection

A7  add high-level read projections:
    swarm.dashboard / queue.get / assignment.get+preview / context.get /
    agent.inspect / exceptions.get / capacity.get / overlap.check

A8  add launch.preview and manager-owned launch orchestration using existing
    Task/Attempt/Operation/binding/workspace authorities plus capability receipt

A9  refactor MCP registry metadata; add grouped role surfaces, deterministic paged
    tools/list, deferred searchable groups and catalog revision

A10 add durable manager-sponsored Concilium state without automatic model execution

A11 expose typed CLI/MCP/UI surfaces, subscriptions, compact rendering and status

A12 run integrated correctness, sparse-graph/catalog scale, staged active-turn,
    recovery, cost, wrong-tool and live qualification
```

One Issue implements one complete slice. One manager owns its worktree/candidate. Writers do not run Cargo. The manager integrates/reviews all diffs and runs the current scoped formatting/minimal warnings-denied Clippy gate once on the final candidate. Integrated tests/load/live model work belong to A12 unless the owner explicitly advances a named check.

## 5. Three-layer MCP model

MCP configuration has three independent layers:

```text
hard profile     maximum methods the credential may ever discover/call
surface          small role-specific initial tool set
catalog/groups   authorized deferred tools searchable on demand
```

Rules:

- profile denial hides a method from list/search and rejects manual call before IPC;
- deferred loading never widens role, Task ownership, project scope or GM authority;
- the application method rechecks authorization after MCP dispatch;
- the normal role starts with approximately 5–9 high-level tools;
- low-level, rare and authority-sensitive methods remain searchable or manual-only;
- `full` remains explicit local compatibility/debug surface, not the default;
- unsupported tool search never silently falls back to the full profile.

Current `main` already provides closed `observer/reviewer/manager/gm/full` MCP profiles, profile-bound client identity, pre-dispatch filtering and caller-owned mutation IDs. The implementation extends that sound boundary; it does not replace it.

## 6. Common role surfaces

### Participant core

```text
swarm.context.get
swarm.tools.search
coordination.send
coordination.inbox
coordination.consult
coordination.sync_integration
coordination.watch.create
swarm.overlap.check
operation.get
```

### Manager core

```text
swarm.dashboard
swarm.queue.get
swarm.launch.preview
swarm.launch
swarm.agent.inspect
swarm.agent.steer
swarm.exceptions.get
operation.get
swarm.tools.search
```

### Reviewer core

```text
swarm.review.context
task.submission
artifact.read
check.get
task.request_changes
operation.get
swarm.tools.search
```

GM/operator uses the manager core. Acceptance, publication and administration remain deferred/manual-only even for a powerful identity.

## 7. Tool groups

The catalog is grouped by intent. Each model-facing group should normally contain about 4–9 methods:

```text
core
participant-coordination
assignment-read
manager-core
runtime-control
runtime-recovery
monitoring
git-read
task-management
review
acceptance-effects
administration
schedules
mailbox-raw
```

A high-level common-path method should replace several manual low-level calls. Raw methods remain available for precise diagnostics where authorized.

## 8. Launcher contract

`swarm.launch.preview` is read-only. It resolves:

```text
exact Task revision/policy/source index/dependencies
queue rank and reason
current Attempt/owner
manager-owned workspace/branch/write lease
related assignments and contract owners
scope/Git overlap and coverage gaps
route/model/effort/budget enforceability
MCP hard profile/surface/deferred groups/catalog revision
hard blocks and warnings
```

`swarm.launch` accepts the exact preview digest and a caller-owned request ID. It revalidates every mutable input, commits intent before external effects, prepares/verifies the workspace before the model starts, reuses existing Task/Attempt/Agent Operations, registers a scoped Participant, starts the exact runtime and records actual MCP capabilities.

A lost reply never creates a replacement Attempt, worktree, binding or native session. Unknown effects are reconciled by exact readback.

## 9. Assignment packet

The launched model receives a compact revisioned packet, not a copied project history:

```text
Task/Attempt/assignment identity and freshness
objective, non-goals and exact current requirements
canonical source index and gaps
workspace/baseline/allowed mutation scope/write lease
queue/dependency reason relevant to this work
related peers and why they are relevant
provided/required contracts and integration cells
scope/Git overlap summary
runtime route/model/effort/budget facts and enforcement gaps
MCP core/deferred capability receipt
output/submission/evidence/stop/escalation contract
coverage and gaps
```

Full sources, messages, queue pages, diffs, logs and artifacts remain pullable on demand.

## 10. Common agent UX

The ordinary participant should need five conceptual affordances:

```text
swarm.context.get
coordination.consult
coordination.publish_contract
coordination.sync_integration
coordination.peer_agree
```

`context.get` also shows pending messages/watches, own Task/submission state, relevant peers and overlap.

The cheapest operation that can answer the question is used:

```text
current structured field
  before consultation/message
one exact consultation
  before integration cell/thread
pure comparison + integration cell
  before manager decision
manager decision
  before Concilium
```

## 11. Watches and reminders

A reminder observes an exact existing fact and produces a small freshness hint. It does not start work.

Examples:

```text
ask answered
contract/cell revision changed
scope released/changed
Task/Attempt revision changed
Operation terminal
owner available
exact deadline reached
```

Watches are bounded, coalesced and normally one-shot. They create no per-agent polling task and no model turn. A schedule is different: it may admit a future typed Operation under scheduler authority. Arbitrary scheduled prompts are not a reminder implementation.

## 12. Git and current ownership

`swarm.overlap.check` and the deferred `git-read` group combine:

```text
current ELIOT Task/Attempt/scope ownership
manager worktree/branch/write lease
uncommitted/changed paths
baseline-to-candidate paths
contract/path/symbol relations
optional history/blame labelled as provenance only
coverage and gaps
```

ELIOT records decide current owner. Branch names, commit authors and blame do not.

One manager owns one mutable candidate/worktree/write-lease lineage. Many read-only peers may inspect, coordinate and review. Independent writers require independent authorized scopes/worktrees or a manager decision.

## 13. Peer-local authority

Participant may:

- read its exact work neighborhood;
- publish work/contract cards;
- find exact owners;
- ask, answer, redirect, abstain or report unknown;
- join/update relevant integration cells;
- volunteer as integrator/reviewer inside existing scope;
- record a reversible local assumption;
- acknowledge/object to a compatible local agreement;
- inspect scope/Git overlap;
- propose escalation or Concilium.

Participant may not:

- create/revise/claim/dispatch/accept Tasks;
- bind/release Attempts;
- spawn, wake, resume or control another model/session;
- expand scope or displace an owner;
- publish/merge/accept;
- change roles, credentials, GM epoch, security or project policy;
- turn prose, consensus, silence, a watch or tool activation into authority.

## 14. Peer-local agreement boundary

A local agreement is allowed when:

- all directly affected current owners are represented;
- it stays inside existing scopes and canonical Task/documentation;
- required compatibility dimensions are known;
- exact revisions/digests are acknowledged;
- no affected owner has an unresolved objection;
- it changes no global/public interface outside represented assignments;
- it changes no persistence, security, identity, retry/fencing/lifecycle owner, provider/billing/acceptance policy or irreversible external effect.

Otherwise it becomes one coalesced manager exception. Participant count alone is not a manager boundary.

Agreement is not verification:

```text
asserted -> peer_agreed -> manager_ratified -> implemented -> verified
```

A counterexample may refute or supersede any unverified claim.

## 15. Integration cells, not multi-party chat

A revisioned integration cell contains:

```text
affected owners and participation bases
producer offers / consumer requirements / carrier constraints
comparison per dimension
unknowns/evidence gaps
assumptions and invalidation conditions
voluntary integration intents
acknowledgements and objections
peer-local agreement or pending-manager result
```

It is not a transcript and schedules no speakers. Only affected owners block the relevant decision. Observers do not create a barrier.

## 16. Delivery truth and anti-spam

Delivery states remain distinct:

```text
stored / available / presented / consumed / held / refused / cancelled / stale
```

`stored` never implies `presented`; `presented` never implies agreement or verification.

Rules:

- one exact recipient, no default broadcast/reply-all;
- current card/field before message;
- one unresolved ask/watch fingerprint;
- no mandatory thanks/ack turn;
- no automatic model wake or peer-recursive spawn;
- safe-boundary header, body on explicit read;
- no durable liveness/status chatter;
- no manager copy of compatible local coordination;
- no automatic escalation/model work from timers;
- no full transcript or full tool catalog injected by default;
- no repeated authority boilerplate rendered as human chat;
- backpressure preserves unique facts and degrades presentation to pull/digest;
- partial/truncated data returns explicit coverage/gaps, never empty success.

## 17. Concilium boundary

Concilium is the last escalation level:

- participant may propose;
- manager/current GM previews and confirms reasonability;
- `open` creates immutable slots and starts no model;
- positions are independent and evidence based;
- manager explicitly advances rounds;
- dissent, correlation and unknowns are retained;
- result is advisory;
- manager separately decides;
- verification/acceptance remain separate.

## 18. Storage/runtime decision

V1 reuses:

```text
existing Store/SQLite owner
existing Task/Attempt/Operation authority
existing client registrations/meta
existing Operations and Observations
existing mailbox delivery/reply/cancellation
revisioned per-object current projections
existing immutable artifacts
bounded Tokio mpsc/oneshot/watch
local IPC and typed MCP facade
```

It adds no second task store, database, event log, broker, daemon, shared chat server or per-agent polling task.

Registered inactive participants consume no model turn and no dedicated Tokio task. Do not store the whole fleet/catalog/graph/cell set in one giant value or rebuild current state through a full unbounded history scan. A schema migration is evidence-triggered by measured contention/scan/hot-row cost.

## 19. Qualification

A12 validates separately:

```text
10,000 registered Participants
thousands of current cards/cells/watches
100,000 mixed coordination/catalog mutations
500 bounded readers
zero automatic model invocations
paged/searchable catalog with hidden-tool non-disclosure
prompt/schema token cost by role
search hit and wrong-tool rate
launch capability receipt and missing-core failures
simulated active runtime identities
staged live turns: 4 -> 16 -> 32 -> 64 -> 128
restart/recovery/stale generations/malformed actors
useful output per cost, duplicate work, conflicts and manager attention
cards/cells/launcher packet versus shared chat and manager relay
```

These are qualification targets, not current capacity claims. A later 1,024-agent experiment requires distributed capacity, explicit cost approval and a factorizable benchmark.

## 20. Privacy

Repository documentation and examples contain placeholders only. Real domains, infrastructure IDs, credentials, local usernames and private paths remain local installation data and never enter Git, prompts, catalog schemas, launch packets or coordination records.