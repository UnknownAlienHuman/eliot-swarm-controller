# ELIOT Agent Communication, Launcher and MCP Program
## Start here

**Revision:** 6 — 2026-10-03  
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

The controller is a capability and coordination service, not an approval queue, shared chat room or implicit model-spawning workflow.

Normal path:

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
participant sees only its relevant neighborhood
        │
        ├── current fact/card ───────────────────────────► continue
        ├── one exact consultation ─────────────────────► continue
        ├── integration cell + pure comparison ─────────► peer-local agreement
        ├── reversible recorded assumption ─────────────► continue and revalidate
        └── authority/global conflict ──────────────────► one manager exception
                                                             │
                                                             └── Concilium only if justified
```

Root/General Manager sees exceptions rather than relaying ordinary mail. The auditor verifies durable agreements and implementation evidence rather than approving conversation.

## 2. Keep four quantities separate

```text
P = registered/discoverable participants
A = concurrently active model turns/processes
E = current material coordination edges
X = unresolved exceptions requiring manager authority
```

A correct fleet may have thousands of registered participants while active turns remain bounded by runtime/provider/host capacity. Normal coordination is sparse and local.

Never equate:

```text
registration with a running model
stored message with a started or informed model turn
one project with one shared room
P participants with P² communication edges
participant count with manager authority
visible tool with authorized tool
configured tool with a runtime-visible working tool
loaded schema with expanded application authority
```

## 3. Required reading and precedence

Implementation agents read in this order:

1. Product architecture, module contract and [Owner Decisions](owner-decisions.md).
2. [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md) — fleet law, sparse relevance, integration cells, delivery truth and backpressure.
3. [Canonical MCP Surfaces and Client Topologies](mcp-canonical-surfaces-and-topologies.md) — exact public convenience names and OpenAI/Copilot/Claude/fallback layouts.
4. [MCP Tool Catalog and Deferred Loading](mcp-tool-catalog-and-loading.md) — profile/surface/catalog layers, groups, schemas, pagination and capability receipts.
5. [Swarm Launcher and Assignment Context](swarm-launcher-assignment-context.md) — dashboard/queue, launch preview, assignment packet, Git overlap and watches.
6. [Peer Autonomy Implementation Amendment](agent-communication-peer-autonomy-implementation.md) — scoped `Role::Participant`, credentials and source-exact routing.
7. [Peer Autonomy and Integration Handshake](agent-communication-peer-autonomy.md) — self-service coordination and autonomy envelope.
8. [Implementation Checklist](agent-communication-implementation-checklist.md) — Store/mailbox/Git constraints except where amended above.
9. [Implementation Issue Plan](agent-communication-implementation-issues.md) — execution policy and older Issue templates; use the current sequence in §12.
10. [Agent Communication and Concilium](agent-communication-concilium.md) and [Tool Contracts](agent-communication-tool-contracts.md) — broader architecture, schemas and recovery. Older examples are non-normative where corrected above.
11. [MCP/Launcher Source Map](mcp-tool-catalog-sources.md), fleet/peer source maps and [Field Evidence](agent-communication-field-evidence.md) — evidence only.

When examples disagree, use the highest applicable item. Do not invent another identity, role, Task store, queue, delivery path, tool authority or model loop.

## 4. MCP has three independent layers

```text
hard profile     maximum methods a credential may ever discover/call
surface          small role-specific initial tool set
catalog/groups   authorized deferred tools searchable on demand
```

Rules:

- profile denial hides a method from list/search and rejects manual call before IPC;
- deferred loading never widens role, Task ownership, repository scope or GM authority;
- the application method rechecks authorization after MCP dispatch;
- normal roles start with approximately 5–9 high-level tools;
- rare, low-level and authority-sensitive methods remain deferred/manual-only;
- unsupported search never silently falls back to the full profile;
- launch records the **actual** runtime-visible capability set rather than trusting declarations.

Current `main` already provides a sound hard layer: one typed tool per application method, no generic passthrough/shell tool, closed `observer/reviewer/manager/gm/full` profiles, profile-bound client identity, filtering on `tools/list` and `tools/call`, and caller-owned mutation IDs. The program extends this boundary; it does not replace or weaken it.

## 5. Canonical role surfaces

### Participant

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

### Manager

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

### Reviewer

```text
swarm.review.context
swarm.tools.search
task.submission
artifact.read
check.get
task.request_changes
operation.get
```

GM/operator uses the manager core. Acceptance, publication and administration remain deferred/manual-only even for a high-authority identity.

Canonical naming rules:

```text
swarm.context.get          replaces old coordination.context.get examples
coordination.consult       is the high-level path; ask_owner is a lower-level step
coordination.watch.*       replaces notify_when_available as a separate concept
coordination.send          wraps raw message.send for ordinary peer use
swarm.overlap.check        wraps scope + bounded Git ownership/overlap reads
swarm.agent.steer          wraps exact manager-owned agent.send semantics
```

Put synonyms in catalog search terms; do not expose competing eager aliases.

## 6. Tool groups and client layouts

Logical groups:

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

Keep groups near 4–9 model-facing methods where practical.

One logical catalog may be presented differently:

```text
OpenAI Responses    eager core + deferred logical MCP group views or client tool search
OpenAI Agents       role-filtered MCP + supported automatic discovery
Copilot CLI         core deferTools=never + deferred catalog/domain servers
Claude Code         verified core MCP + deferred optional domains
simple MCP client   fixed role surface; safe reconnect to change surface
```

No layout creates another database or authority. Logical group views normally share one local ELIOT host/facade and local IPC.

## 7. Launcher contract

`swarm.launch.preview` is read-only and resolves:

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

`swarm.launch` accepts the exact preview digest and a caller-owned request ID. It revalidates mutable inputs, commits intent before external effects, prepares/verifies the workspace before the model starts, reuses existing Task/Attempt/Agent Operations, registers a scoped Participant, starts the exact runtime and records an actual capability receipt.

A lost reply never creates a replacement Attempt, worktree, binding or native session. Unknown effects are reconciled by exact readback.

## 8. Compact assignment packet

The model receives:

```text
Task/Attempt/assignment identity and freshness
objective, non-goals and exact current requirements
canonical source index and gaps
workspace/baseline/allowed mutation scope/write lease
queue/dependency reason relevant to this work
related peers and exact relevance reasons
provided/required contracts and integration cells
scope/Git overlap summary
runtime route/model/effort/budget facts and enforcement gaps
MCP core/deferred capability receipt
output/submission/evidence/stop/escalation contract
coverage and gaps
```

It does not receive the full queue, roster, history, transcript, diff, logs or tool catalog. Those remain pullable on demand.

## 9. Agent self-service

Participant may:

- inspect its exact current assignment;
- find the current owner of a contract/path/symbol;
- send, answer, redirect, abstain or report unknown;
- publish work/contract cards;
- synchronize a producer/carrier/consumer integration cell;
- inspect scope/Git overlap;
- record a reversible assumption;
- acknowledge or object to a peer-local agreement;
- create a one-shot watch for an exact durable fact.

Participant may not:

- create/revise/claim/dispatch/accept Tasks;
- bind/release Attempts;
- spawn, wake, resume or control another model/session;
- expand scope or displace an owner;
- publish/merge/accept;
- change security, identity, persistence, lifecycle or project policy;
- turn prose, consensus, silence, a watch or tool activation into authority.

## 10. Watches, Git and integration

A watch observes a named existing fact and emits a small freshness hint. It creates no per-agent polling task, model turn or arbitrary scheduled prompt. Schedules remain separate typed future-Operation authority.

`swarm.overlap.check` combines:

```text
current ELIOT Task/Attempt/scope ownership
manager worktree/branch/write lease
uncommitted and changed paths
baseline-to-candidate paths
contract/path/symbol relations
optional history/blame labelled as provenance only
coverage and gaps
```

ELIOT records decide current ownership. Branch names, commit authors and blame do not.

Routine multi-party integration uses revisioned integration cells, not rooms or transcripts. Only directly affected owners block the relevant decision. Participant count alone is not a manager boundary.

## 11. Delivery and anti-spam

```text
stored / available / presented / consumed / held / refused / cancelled / stale
```

Stored never implies presented; presented never implies agreement or verification.

- one exact recipient, no default broadcast/reply-all;
- current structured field before a message;
- identical asks/watches/cards coalesce;
- no required politeness turn;
- no automatic model wake or recursive spawn;
- safe-boundary header, body on pull;
- no durable liveness/status chatter;
- no manager copy of successful local coordination;
- no full transcript or full catalog injected by default;
- backpressure preserves useful facts and degrades presentation to pull/digest;
- partial/truncated state returns explicit coverage/gaps.

## 12. Current implementation sequence

```text
A1  mailbox primitive extraction/reuse
A2  scoped Participant, relevance indexes and cards
A3  consultation, watches, assumptions and delivery truth
A4  comparator, integration cells and peer-local autonomy
A5  irreducible negotiation and manager-required contracts
A6  advisory scopes and bounded Git inspection
A7  dashboard/queue/context/overlap read projections
A8  launch preview/orchestration and capability receipt
A9  grouped/paged/deferred MCP catalog and role surfaces
A10 durable Concilium state
A11 CLI/MCP/UI wiring, compact rendering and status
A12 correctness, catalog/fleet scale, recovery, cost and live qualification
```

One Issue implements one complete slice. One manager owns its worktree/candidate. Writers do not run Cargo. The manager integrates/reviews and runs the current scoped formatting/minimal warnings-denied Clippy gate once. Integrated tests/load/live model work belong to A12 unless the owner explicitly advances a named check.

## 13. Concilium

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

## 14. Storage/runtime decision

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

It adds no second Task store, database, event log, broker, daemon, shared chat server or per-agent polling task.

## 15. Qualification

A12 measures:

```text
10,000 registered Participants
thousands of current cards/cells/watches
100,000 mixed coordination/catalog mutations
500 bounded readers
zero automatic model invocations
paged/searchable catalog with hidden-tool non-disclosure
initial schema tokens by role
search hit/miss/wrong-tool rate and first-load latency
launch capability receipt and missing-core failures
MCP process cleanup and list-change/reconnect behavior
staged live active turns: 4 -> 16 -> 32 -> 64 -> 128
useful output per cost, duplicate work, conflicts and manager attention
```

These are qualification targets, not current capacity claims. A later 1,024-agent experiment requires distributed capacity, explicit cost approval and a factorizable benchmark.

## 16. Privacy

Repository documentation and examples use placeholders only. Real domains, infrastructure IDs, credentials, local usernames and private paths remain local installation data and never enter Git, prompts, catalogs, launch packets or coordination records.