# ELIOT Agent Communication — Fleet-Scale Freedom
## Maximum useful autonomy without all-to-all chat, manager queues or uncontrolled model fan-out

**Revision:** 1 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Applies to:** [Communication Program](agent-communication-program.md), [Peer Autonomy](agent-communication-peer-autonomy.md), [Peer Autonomy Implementation](agent-communication-peer-autonomy-implementation.md), [Implementation Issues](agent-communication-implementation-issues.md), [Communication and Concilium](agent-communication-concilium.md)  
**Status:** normative fleet-scale amendment and implementation handoff. It does not claim that the runtime already implements these methods.  
**Precedence:** this document supersedes older count-based sponsorship rules, global-directory examples, auto-expiry examples and any example that treats multi-party coordination as a shared chat transcript.

## 0. Decision

ELIOT should be permissive in the middle of development and strict only at authority and effect boundaries.

```text
strict edge                         free collaborative middle                         strict edge
-----------                         -------------------------                         -----------
manager assigns Task/Attempt  ->    agents discover, ask, publish, compare,    ->     protected verify/merge/accept
scoped identity and workspace       volunteer, sequence, negotiate and record          external effects and policy
```

The controller is a capability and coordination service, not an approval machine.

A participant does not need Root, the General Manager or an auditor to:

- discover the current owner of a contract, symbol, path or integration edge;
- publish what it is building and what it needs;
- ask one exact technical question;
- answer, redirect, abstain or say that the answer is unknown;
- compare producer and consumer contracts;
- coordinate sequencing and a temporary integration owner inside existing scopes;
- record a reversible assumption and continue;
- agree on a local implementation detail already permitted by all affected assignments;
- inspect current scope and Git overlap;
- object to or supersede an earlier peer agreement with new evidence.

A participant still cannot turn communication into Task assignment, model spawning, scope expansion, merge, acceptance, policy change or an irreversible external effect.

The governing rule is:

```text
Freedom is the default inside current authority.
Evidence and protected verification guard the output boundary.
Only authority expansion needs central approval.
```

## 1. What fleet scale means

ELIOT must keep four quantities separate:

```text
P = registered/discoverable participants
A = concurrently active model turns/processes
E = current material coordination edges
X = unresolved exceptions requiring manager authority
```

A correct large fleet can have:

```text
P = thousands
A = bounded by runtime/provider/host capacity
E = sparse and local to current work
X = proportional to real exceptions, not to P
```

It must not assume:

```text
registered participant == running model
message stored == model turn started
one project == one shared room
P participants == P² communication edges
many participants == manager sponsorship required
```

Ten thousand registered participants are a storage and indexing problem. Ten thousand simultaneous local model processes are a runtime, provider, cost and operating-system problem. The communication plane must not create the second problem merely because it can represent the first.

## 2. Evidence and its limits

### 2.1 Agensh — direct evidence that self-organization can scale

The September 2026 Microsoft Research paper [Agensh: Scaling Organizational Intelligence to 1,024 Agents](https://arxiv.org/abs/2609.26781) removes the central orchestrator. Workers repeatedly gather shared context, claim work, act, share findings, verify and merge through:

- a shared workspace;
- a message interface;
- shared reusable context.

Reported results:

- five hard ProgramBench tasks, 1 → 128 agents: mean final pass rate `19.31% → 28.78%`;
- pandoc, 1 → 1,024 agents: `33.89% → 55.06%` under a six-hour, no-Internet budget;
- cooperation evolved from peer interface coordination to multi-worker integration, reusable workflows, specialization and redundant integrators.

Important limitations:

- the 1,024-agent result is one benchmark task, not a general production fleet result;
- most pandoc gain arrived by 128 agents; 128 → 1,024 added only 4.12 percentage points;
- the paper does not report a normalized token or dollar comparison against a strong hierarchical controller;
- the public code link was not resolvable during this review, so source-level behavior is unverified;
- Agensh lets workers self-assign and merge; ELIOT already has stronger Task/Attempt and acceptance authority and should not discard it.

**Adopt:** shared current state, sparse direct coordination, typed findings/claims, voluntary specialization, reusable integration protocols, redundant candidate integrators and failure takeover.

**Do not adopt:** prompt-only assignment authority, uncontrolled writes to one shared checkout, or peer merge/acceptance authority.

### 2.2 Anthropic multi-agent research — quality gains with large cost multipliers

Anthropic's [multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) reports a 90.2% improvement over a single-agent baseline on an internal research evaluation, but approximately 15× chat token usage. Its own development history includes agents spawning about 50 subagents for simple questions, excessive status updates and unbounded searches. The published practical guidance favors small concurrent groups with explicit scopes and warns that coding tasks with shared state are less parallel than broad research.

**Adopt:** explicit objectives, bounded independent work, direct artifact outputs, high-level topology observability, checkpoints and resumability.

**Do not adopt:** treating more turns as intrinsically productive or using a lead model as the relay for every subagent result.

### 2.3 Anthropic C compiler experiment — freedom works only when work is factorizable

Anthropic's [16-agent C compiler experiment](https://www.anthropic.com/engineering/building-c-compiler) used isolated containers and a shared Git repository over roughly 2,000 sessions. It demonstrated substantial autonomous production, but also frequent merge conflicts and a hard failure mode when all agents converged on one monolithic verifier/kernel problem and overwrote one another. Parallel progress resumed after the work was refactored into independently verifiable subsets.

**Adopt:** isolated writers, strong oracles, compact durable progress state, fresh sessions, specialization and the ability to collapse a hotspot to one integration owner.

**Do not adopt:** forced parallelism on one mutable seam or trust in agent self-reports without a verifier.

### 2.4 OpenAI Responses multi-agent — separate information from work

The official [Responses multi-agent guide](https://developers.openai.com/api/docs/guides/responses-multi-agent) distinguishes:

```text
send_message   information; does not start a turn
followup_task  additional work; starts/resumes a turn
```

It recommends multi-agent execution for independent bounded work, not ordered chains or heavily shared mutable state. Runtime concurrency is a separate capacity setting.

ELIOT adopts this semantic boundary exactly: peer coordination has `send_message` semantics; only manager/Task/runtime authority has `followup_task` semantics.

### 2.5 Claude Code Agent Teams and field reports

Official Agent Teams guidance recommends small teams for ordinary work and warns that coordination/token overhead rises with team size. Direct messages, independent contexts and compact previews are useful.

Field reports identify concrete hazards:

- recursive subagent spawning produced 48+ agents and about 1.5 million tokens from one research request, while the first few agents had already found the useful results;
- a roughly 70-agent review lost 26 agents after a network failure, and diagnosis required transcript forensics;
- messages to a busy teammate could be reported as sent but presented only after the stale work completed;
- missing coordination tools allowed an agent to finish work but made delivery of the result impossible;
- repeated authority envelopes and status notifications flooded human and model views.

**Adopt:** direct addressed messages, explicit availability policy, compact UI, durable topology/status and capability validation.

**Do not adopt:** recursive peer spawning, implicit team formation, idle auto-wake, a send receipt that falsely implies presentation, or full authority boilerplate repeated in the visible transcript.

### 2.6 Current ELIOT field evidence

The owner's existing swarm has already demonstrated the same scaling laws:

- identical queue snapshots made several lines perform the same work;
- stale ownership and fragmented submissions created conflicts and negative progress;
- global reminders and status traffic polluted contexts;
- full histories and repeated agent reuse consumed extreme token volume;
- a mass replay of old submissions flooded acceptance and reduced useful throughput;
- known platform limits were misreported when monitoring aggregated unrelated child sessions;
- shared mutable hotspots required one owner rather than more writers.

The fleet-scale design must prevent these failures structurally, not by adding another long prompt.

### 2.7 Evidence boundary

There is credible evidence for tens and one research demonstration for 1,024 agents. There is not yet credible independent evidence that thousands of autonomous coding agents provide reliable, economical, long-running production development on arbitrary repositories.

Therefore ELIOT should:

- support large registered populations and sparse self-organization now;
- qualify active-turn scale incrementally;
- measure useful work, cost, conflicts and recovery;
- never advertise a simultaneous-agent number merely because identities can be stored.

## 3. State before conversation

The default coordination path is:

```text
current facts/card
  -> exact field lookup
  -> one quick ask
  -> deterministic integration comparison
  -> revisioned integration cell
  -> bilateral thread
  -> manager exception
  -> Concilium
```

An agent should not open a conversation when a current structured fact answers the question.

The system should make accurate shared state easier than chat:

- bootstrap work cards from Task/Attempt/ProducerRef;
- material work-card revisions;
- producer/consumer/carrier contract cards;
- advisory scope intents;
- current integration comparisons;
- explicit assumptions and invalidation conditions;
- durable agreements and objections;
- exact coverage/gap reporting.

Free-form text remains available for unusual cases, but it is not the primary coordination database.

## 4. Sparse dynamic relevance graph

ELIOT maintains a current graph whose vertices are participants, Tasks, Attempts, contracts, scopes, paths, symbols and integration cells.

An edge exists only for an exact current reason:

```text
Task dependency
producer / consumer / carrier relation
contract key
scope overlap
path or symbol relation
manager-sponsored review
active integration cell
unresolved prior coordination
```

### 4.1 Local neighborhood, not global roster

`coordination.context.get` returns the caller's bounded relevant neighborhood. It does not return all agents.

`coordination.peer.find` performs an indexed ranked query and returns:

```text
exact current actor/basis
why it matched
which contract/path/symbol relation matched
identity freshness
availability policy
coverage and gaps
```

### 4.2 Scale invariant

Normal coordination work must be approximately proportional to the material local degree `d`, not the population `P`:

```text
normal read/send cost: O(d) or O(log P + d)
forbidden design:      O(P) prompt injection or O(P²) fan-out
```

No API may require loading the complete participant graph into a model context.

### 4.3 High fan-in hotspots

A symbol/schema/file with many active writers is an integration hotspot, not an invitation to create a large chat.

The system reports:

```text
owners
planned edits
contract mismatches
current integrator, if any
suggested partition keys
coverage gaps
```

Participants may sequence or elect a voluntary integrator inside existing authority. A manager is required only when ownership/scope/authority changes.

## 5. Integration cells: multi-party cooperation without a chat room

A bilateral thread is inefficient when several exact producers/consumers share one seam. A shared chat is worse. ELIOT therefore adds a revisioned **integration cell**.

An integration cell is structured current state for one seam, not a conversation transcript.

### 5.1 Methods

```text
coordination.cell.open
coordination.cell.get
coordination.cell.list
coordination.cell.update
coordination.cell.ack
coordination.cell.supersede
```

Convenience operation for ordinary agents:

```text
coordination.sync_integration
```

It reads current cards, resolves affected owners, updates the caller's offer/requirement, runs the pure comparator, updates/coalesces the cell and returns the next useful action.

### 5.2 Cell identity

```json
{
  "cell_key": "canonical contract/seam key",
  "task_revision_set": [],
  "member_basis_set": [],
  "purpose": "make producer/carrier/consumer compatible",
  "close_condition": "all required dimensions match or exact mismatch is escalated",
  "canonical_sources": [],
  "state_revision": 4,
  "material_digest": "sha256:..."
}
```

### 5.3 Cell content

```text
exact affected current owners and participation bases
producer offers
consumer requirements
carrier constraints
current comparison per dimension
unknowns and evidence gaps
assumptions and invalidation conditions
voluntary implementation/integration intents
acknowledgements and objections
selected peer-local agreement or pending-manager item
evidence refs and provenance states
```

### 5.4 Membership

Membership is derived from exact current relations. It is immutable for one revision lineage. A changed affected-owner set creates a linked successor cell.

Participant count alone never requires manager sponsorship.

An older rule such as “more than four participants requires sponsorship” is superseded. Count may produce a warning and a partition suggestion; authority, relevance, unknowns and affected ownership determine whether manager action is required.

### 5.5 No barrier synchronization

Only actors whose contract/scope/implementation intent is affected must acknowledge the relevant revision. Observers and unaffected members do not block progress.

The cell never waits for every member to say “ack.” Silence never means agreement, but irrelevant silence is not a barrier.

### 5.6 No transcript injection

A cell update emits a small revision/freshness fact. Models read only the relevant changed fields. Full state remains pull-based.

No model receives every update made by every member.

## 6. Ordinary agent UX

The common path should fit five primary affordances:

```text
coordination.context.get
coordination.ask_owner
coordination.publish_contract
coordination.sync_integration
coordination.peer_agree
```

Supporting tools remain discoverable/deferred rather than always occupying every model's tool context.

### 6.1 `context.get`

Returns assignment, current cards, relevant neighbors, integration cells, unanswered asks, scope conflicts and coverage gaps.

### 6.2 `ask_owner`

Answers from a current structured field when possible. Otherwise it resolves one exact owner and creates one addressed ask. Ambiguity returns candidates and sends nothing.

### 6.3 `publish_contract`

Publishes only the caller's owned producer/consumer/carrier fields and computes a material digest.

### 6.4 `sync_integration`

Combines card publication, owner discovery, integration-cell update and deterministic comparison. It does not invoke another model.

### 6.5 `peer_agree`

Acknowledges exact revisions/digests and commits a peer-local agreement only when the autonomy classifier permits it.

## 7. Peer-local autonomy

A peer-local agreement is valid when:

- every directly affected current owner is represented;
- all actors are fresh and authorized for the named Task/Attempt revisions;
- the decision stays inside existing scopes and canonical requirements;
- required compatibility dimensions are known;
- no affected owner has an unresolved objection;
- no public/global contract outside the represented assignments changes;
- no persistent authority, security, credential, identity, fencing, clock, lifecycle, provider, billing, acceptance or irreversible-effect policy changes.

Manager authority is required when any of those conditions fails.

Participant count is not an authority condition.

### 7.1 Consensus is not verification

Agreement states are explicit:

```text
asserted
peer_agreed
manager_ratified
implemented
verified
refuted
superseded
```

Several agents repeating the same claim does not promote it to `verified`.

A counterexample may revoke support and create a successor comparison/agreement revision. History is retained.

## 8. Proceeding under assumptions

Coordination must not make every unanswered question a blocker.

A participant may record an assumption and continue when the assumption is:

- inside its existing authority and scope;
- reversible in the current candidate/worktree;
- not security-, identity-, persistence-, lifecycle- or external-effect-sensitive;
- accompanied by an invalidation condition and affected contract/path/symbol;
- visible to relevant peers.

Assumption states:

```text
local_reversible
blocking_peer_fact
manager_required
invalidated
resolved
```

`local_reversible` does not wait for a reply. A later contradictory answer marks affected work/cells stale; it does not silently rewrite or roll back code.

This preserves momentum without allowing plausible invention at authority boundaries.

## 9. Voluntary claims and integrators

Peers may organize execution inside already assigned work without changing Task ownership.

Allowed coordination intents:

```text
volunteer_to_integrate
volunteer_to_review
yield_sequence
implement_named_seam_within_scope
need_peer_fact
not_relevant
```

These are advisory current intents, not assignments.

A voluntary integrator may:

- assemble current offers/requirements;
- propose one compatible seam;
- request exact evidence from affected owners;
- produce an integration candidate inside its existing scope.

It may not merge, accept, expand scope or command another agent.

### 9.1 Candidate responders

For a fact or integration role with several qualified current participants, the cell may publish a bounded candidate request as state. Interested active participants opt in.

The requester may accept the first valid response and cancel the remaining candidate intents. No peer model is automatically started, and no recipient list is broadcast into model contexts.

This preserves the useful redundancy seen at large scale without creating recursive fan-out.

## 10. Answers, abstention and social noise

Valid answer dispositions:

```text
answered
unknown
not_relevant
already_covered
redirect
needs_negotiation
superseded
refused
```

An agent is never required to generate a politeness-only acknowledgement.

“No answer yet” is not failure unless the caller declared the fact blocking. Even then, age creates attention, not fabricated agreement or automatic escalation work.

Status percentages, heartbeats and “still working” are replaceable observations, not durable conversation turns.

## 11. Message delivery semantics

A message receipt must distinguish:

```text
stored       durable record committed
available    recipient may pull it
presented    header/body was actually exposed at a safe boundary
consumed     recipient explicitly read/handled it
held         recipient policy withheld presentation
refused      recipient policy refused it
cancelled    sender cancelled the delivery record
stale        target identity/work context changed
```

`stored` never implies `presented`.

A sender therefore cannot assume a busy peer saw a correction merely because mailbox admission succeeded.

Urgent runtime steering remains a separate manager/runtime operation with exact current-turn authority. Peer communication never interrupts a tool or starts a turn.

## 12. Inbound policy and backpressure

Participant policy:

```text
pull_only
safe_boundary
hold
refuse
```

Backpressure is soft for useful durable coordination and hard only for safety/protocol limits.

### 12.1 Soft degradation

Under load the system may:

- coalesce identical unresolved asks;
- retain the latest material card revision;
- replace push presentation with `pull_only` availability;
- emit one digest/header instead of many bodies;
- collapse repeated no-progress updates into one attention fact;
- suggest splitting an oversized integration cell by contract/seam;
- delay nonblocking presentation while preserving durable state.

### 12.2 Hard rejection

Hard rejection is limited to:

- stale/disabled identity or invalid work context;
- forbidden authority transition;
- malformed/oversized payload;
- invalid recipient/path/protocol input;
- recipient that does not exist or is ambiguous for a convenience send;
- attempt to assign/spawn/wake/merge/accept through communication;
- capacity condition where the durable record itself cannot be safely admitted.

No justified exchange is rejected merely because a soft count default was exceeded.

### 12.3 No silent loss

Partial pages, queue pressure, unavailable indexes and truncated history return explicit coverage/gap/disposition facts. Empty success must never hide lost messages or unknown owners.

## 13. Capability validation

A registered participant advertises a controller-verified coordination profile:

```text
can_read_context
can_publish_cards
can_receive_header
can_read_body
can_send_direct
can_update_cell
can_ack_agreement
runtime_presentation_mode
profile_digest
```

The controller exposes only methods actually available to that client/runtime.

If a runtime cannot receive or send directly, its state is `relay_only` or `pull_only`; ELIOT does not give the model instructions to call a missing tool and then silently lose its result.

Profile changes create a new grant/profile revision. They do not silently widen an existing Participant credential.

## 14. No peer spawning or recursive delegation

The communication plane has no method equivalent to:

```text
spawn_agent
create_task
assign_peer
resume_peer_turn
wake_peer
```

A participant may publish an assistance need or volunteer intent. Only manager/Task/runtime authority converts that into a new active turn or assignment.

This prevents one broad request from recursively producing dozens of agents before the controller can observe or stop it.

Active-turn capacity, provider quotas, process limits and cost budgets belong to the runtime/control plane and remain separate from communication population size.

## 15. Manager and auditor at fleet scale

### 15.1 Manager exception digest

The manager sees one current item per fingerprint for:

```text
unowned blocking contract/scope
manager_required agreement
unresolved required mismatch
affected owner stale/missing
exclusive scope override
repeated no-progress loop
security/persistence/identity/lifecycle/external-effect decision
Concilium proposal
```

It does not receive:

```text
card reads
answered asks
ordinary cell updates
compatible peer-local agreements
status/liveness
presentation receipts
```

A conflict-free fleet may grow from 10 to 10,000 registered participants without growing the manager digest.

### 15.2 Auditor packet

The auditor receives compact durable facts for one candidate/seam:

```text
canonical requirements and Task/Attempt revisions
participants and scopes
cards/cell/agreement revisions and digests
comparison, unknowns, assumptions and dissent
changed files/Git overlap/evidence
verification status
```

The auditor verifies output and may refute a peer agreement. It is not a router for ordinary discussion.

## 16. One writer and isolated mutation remain compatible with freedom

Freedom to coordinate does not require several agents writing the same tree.

For each mutable candidate scope:

```text
one lifecycle owner
one active writer/worktree lease generation
many read-only peers/reviewers
peer-local sequencing and contract agreement
protected integration/acceptance
```

Several agents may explore, review, propose and compare in parallel. Mutation ownership remains explicit so that collaboration does not become overwrite/merge forensics.

When work is genuinely independent, many writers use separate worktrees/scopes. When it converges on one mutable hotspot, the graph should expose the hotspot and allow one integrator instead of forcing artificial parallelism.

## 17. Storage and concurrency

V1 continues to use the existing Store/SQLite owner, Operations, Observations, mailbox and namespaced current projections.

Fleet-scale constraints:

- do not put the whole participant roster, graph or all cells in one giant `meta` value;
- use per-object namespaced keys/revisions and bounded indexes;
- update one cell/card/ask and its exact indexes transactionally;
- allocate message/cell sequence independently from structural revisions;
- never run Git, HTTP or model calls in a SQLite transaction;
- reads must not replay the full Observation log;
- indexes report coverage/gaps when incomplete;
- a slow reader cannot hold the Store owner or block native runtime replies;
- in-memory channels carry IDs/freshness, not unbounded bodies;
- no per-participant polling task;
- no process exists merely because a Participant registration exists.

If measured contention, scan cost or hot-row size exceeds the namespaced `meta` design, add dedicated tables in a reviewed migration. Do not preemptively add a second database or broker.

## 18. Fleet observability

Monitor structure and useful progress, not every conversation body.

Required metrics:

```text
registered participants
fresh/stale active participation grants
concurrently active model turns (from runtime, not inferred from messages)
current material coordination edges
mean/max local graph degree and truncated-query gaps
current cards/cells/asks/agreements
card-field answer ratio
ask coalescing and duplicate suppression
stored -> presented -> consumed latency/disposition
message-created model turns (target: zero)
manager exception ratio and reasons
unknown/mismatch dimensions
no-progress loops
stale owner/card/cell counts
integration hotspots
context bytes presented to models
Store queue/transaction/page latency
Observation/index growth
```

The human UI should show:

- fleet topology and active work;
- local neighborhood for a selected Task/participant;
- integration hotspots and unresolved mismatches;
- manager exceptions;
- exact delivery disposition;
- drill-down on demand.

It should not render repeated authority envelopes or every peer status update as chat bubbles.

## 19. Qualification plan

The previous qualification contour is expanded. These are test targets, not current performance claims.

### 19.1 Storage/control-plane scale without paid model calls

```text
10,000 registered Participant identities
5,000 current work/contract cards
1,000 active Task/Attempt contexts
2,000 integration cells
100,000 mixed card/ask/cell/message mutations
500 concurrent bounded readers
high churn: release/supersede/re-register/restart
```

Required outcomes:

- zero automatic model invocations;
- no O(P²) operation or global prompt projection;
- conflict-free workload creates no manager items as population grows;
- duplicate asks/card updates coalesce deterministically;
- stale identities cannot write;
- malformed participant/cell update is isolated;
- all-to-all recipient request is rejected or converted into a bounded indexed query without sending;
- lag/truncation returns explicit gaps;
- restart reconstructs exact current projections without a full unbounded scan;
- no giant hot `meta` value.

Record p50/p95/p99 latency, DB time, queue depth, RSS, handles, WAL/DB growth and projection gap counts. Do not set a claimed capacity before measurement.

### 19.2 Simulated active fleet

Simulate at least:

```text
200 active runtime sessions/turn identities
1,000 durable direct asks
100 concurrent integration cells
mixed pull_only/safe_boundary/hold/refuse policies
recipient disconnect/reconnect and stale generation
```

No paid model call is required to validate routing and state semantics.

### 19.3 Live staged qualification

Increase active model turns in stages rather than jumping to a headline number:

```text
4 -> 16 -> 32 -> 64 -> 128
```

Advance only when the preceding stage demonstrates:

- useful output per cost does not collapse;
- duplicate work and merge conflicts remain bounded;
- manager/auditor attention does not grow linearly with routine coordination;
- message delivery/presentation semantics remain correct;
- process/resource cleanup is reliable;
- no recursive fan-out;
- quality/verification remains acceptable.

A later 1,024-agent experiment requires distributed runtime capacity, explicit cost approval and a factorizable benchmark. It is not a first-host acceptance target.

### 19.4 Comparative experiment

For the same integration workload compare:

```text
free-form shared chat
manager-relayed messages
cards + quick asks + integration cells
```

Measure:

- tokens/context bytes;
- model turns;
- time to compatible seam;
- manager interventions;
- mismatches found before code integration;
- duplicate work;
- merge conflicts;
- verified result rate.

The state-first path succeeds only if it improves useful coordination without merely hiding unresolved work.

## 20. Implementation amendments

This document amends the current A1–A10 program as follows.

### A2 — Participant role, current state and relevance graph

Add:

- per-participant verified coordination capability profile;
- sparse current relevance indexes;
- bounded local-neighborhood queries;
- no global roster projection;
- per-object namespaced current state.

### A3 — Quick asks and delivery truth

Add:

- `stored/available/presented/consumed/held/refused/cancelled/stale` disposition;
- answer/abstention enum;
- assumptions and invalidation conditions;
- no send receipt that implies presentation;
- coalescing/backpressure behavior.

### A4 — Integration cells and autonomy

Replace “only bilateral handshake” with:

- bilateral offer/requirement/comparator as the core primitive;
- multi-party revisioned integration cells;
- `coordination.sync_integration` convenience operation;
- voluntary integrator/reviewer intents;
- affected-owner acknowledgements without global barrier;
- participant count excluded from autonomy classification.

### A5 — Generic negotiation and manager-required path

Keep direct threads for irreducible prose/negotiation. They remain addressed and normally bilateral. Multi-party routine state belongs in an integration cell, not a room.

### A9 — Tools, profiles and observability

Expose the five common affordances first. Defer low-level tools. Add fleet/local-neighborhood/hotspot/delivery-disposition views and metrics without transcript injection.

### A10 — Qualification

Use the contours in §19 and include staged active-turn qualification, cost/useful-output measurements and failure/recovery tests.

## 21. Explicitly superseded rules

The following older examples are non-normative where they conflict with this document:

- participant count alone requiring sponsorship;
- a hard four-person limit for justified coordination;
- one global agent directory loaded into context;
- multi-party free-form room as the normal integration surface;
- automatic thread/cell expiry implying resolution or ownership release;
- message admission implying the recipient model saw it;
- idle recipient auto-wake;
- peer-recursive spawning/delegation;
- mandatory acknowledgement turns;
- manager copies of every successful local coordination event.

Age, count and breadth create warnings, partition suggestions or pull-only delivery. They do not fabricate authority transitions.

## 22. Non-goals

This amendment does not add:

- a second Task/Issue authority;
- peer assignment or model spawning;
- a general chat room or broadcast;
- peer merge/acceptance;
- a new database, broker or daemon;
- semantic-vector routing as a prerequisite;
- automatic consensus;
- automatic escalation/model work from timers;
- thousands of simultaneous local processes as a product claim;
- weaker verification in exchange for higher agent count.

## 23. Final invariants

1. **Assignment and acceptance stay centralized; engineering coordination is local and sparse.**
2. **Inside current scope, communication and reversible coordination are allowed by default.**
3. **A durable message never starts a model turn.**
4. **Population, active turns and communication edges are separate quantities.**
5. **Normal work is local-neighborhood work, never all-to-all.**
6. **Shared current state precedes direct questions; direct questions precede threads.**
7. **Multi-party integration uses revisioned cells, not a shared transcript.**
8. **Count alone never creates manager authority.**
9. **Only affected current owners block an agreement.**
10. **Unknown is explicit; a required unknown cannot become compatibility.**
11. **Peers may volunteer and sequence work but cannot create authority.**
12. **Stored is not presented; presented is not verified.**
13. **Backpressure preserves useful facts and degrades presentation, not correctness.**
14. **No peer-recursive fan-out.**
15. **One lifecycle owner and explicit mutation ownership remain.**
16. **Verifier/effect boundaries are strict so the collaborative middle can stay free.**
17. **Scale claims require measured useful output, cost, recovery and correctness.**