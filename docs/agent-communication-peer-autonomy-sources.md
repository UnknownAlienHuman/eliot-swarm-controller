# ELIOT Agent Communication — Peer Autonomy Source Map

**Revision:** 1 — 2026-10-03  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Normative consumers:** [Peer Autonomy](agent-communication-peer-autonomy.md), [Peer Autonomy Implementation](agent-communication-peer-autonomy-implementation.md)  
**Status:** research evidence. Source behavior and reports do not become ELIOT capability until implemented and qualified.

## 0. Evidence rule

```text
CODE        exact source behavior
DOC         official product/protocol documentation
RELEASE     exact tagged/released behavior
ISSUE       concrete user/developer report
USER        experience without full independent reproduction
PAPER       research result under its stated setup
INFERENCE   ELIOT design conclusion
UNVERIFIED  plausible but not end-to-end established
```

A current-main implementation is not silently attributed to a stable release. A reported defect is an acceptance case, not proof that every version still has it.

## 1. Claude Code Agent Teams and cross-session messaging

### Official behavior worth taking

Official documentation establishes:

- teammates run with independent contexts;
- teammates can message one another directly rather than relaying through the lead;
- the lead coordinates and assigns work through the shared team/task mechanisms;
- a cross-session message is presented as coming from another session, not from the human;
- peer messages cannot approve permissions or change configuration;
- inbound messaging supports accept/hold/refuse-style policy;
- a short preview can be surfaced before the receiver reads the full message;
- delivery is checked at tool/safe boundaries rather than interrupting an active tool;
- a one-shot availability notification can wait without repeatedly invoking the watched model;
- official guidance warns that coordination/token overhead grows with team size and recommends parallelizing genuinely independent work while avoiding shared mutable files.

Official sources:

- <https://code.claude.com/docs/en/agent-teams>
- <https://code.claude.com/docs/en/cross-session-messaging>

### Field failures to test

Public issue reports describe:

- messages reported as sent but not observed by the intended sender/receiver;
- recipient-name mismatch creating an orphan or wrong inbox;
- task/session status divergence;
- delayed inbox processing until a long current task finishes;
- duplicate or replayed assignment/notification events;
- completion notifications flooding the lead context;
- one logical teammate identity corresponding to more than one live writer;
- team teardown while background teammates remain active;
- silent fallback from a real team to isolated subagents, changing communication semantics.

Selected issue anchors:

- <https://github.com/anthropics/claude-code/issues/47930>
- <https://github.com/anthropics/claude-code/issues/88741>
- <https://github.com/anthropics/claude-code/issues/71723>

### ELIOT adoption

Take:

```text
direct peer messaging
independent context
one-line preview
recipient accept/hold/refuse policy
peer message never equals human approval
safe-boundary notification
one-shot readiness notification
```

Reject:

```text
second shared Task authority
recipient string that creates a mailbox/identity
poll-driven model wake
auto-starting an idle recipient for ordinary mail
full task transcript injected into every participant
implicit fallback that changes communication semantics
```

## 2. OpenAI Responses Multi-agent

### Official distinction

OpenAI's current multi-agent guide distinguishes:

```text
send_message
  queue information for another agent
  does not start/resume a turn

followup_task
  assign more work
  starts or resumes a turn
```

It also warns that multi-agent delegation increases token use and is unsuitable when work shares mutable state or requires strict ordering.

Official source:

- <https://developers.openai.com/api/docs/guides/responses-multi-agent>

### ELIOT adoption

This distinction is normative:

```text
peer coordination tool == send_message semantic
manager Task/runtime operation == followup_task semantic
```

No peer API may smuggle the second semantic into the first.

## 3. Agency Swarm

### Useful mechanism

Agency Swarm defines explicit directional `communication_flows` and allows a message tool to require structured fields such as key decisions and context. This is better than letting any actor contact any other actor through an untyped global chat.

Source:

- <https://github.com/VRSEN/agency-swarm>
- `examples/custom_send_message.py`

### ELIOT adoption

Take:

```text
explicit communication edges
typed required message fields
separate message and handoff semantics
```

Improve:

- derive current sparse edges from Task dependency, ProducerRef, contract key, accepted scope overlap and reviewer sponsorship;
- do not make one static all-project communication graph the sole authority;
- do not make handoff the default response to an ordinary question.

## 4. Gas Town / Beads

### Useful mechanisms

Gas Town keeps work state outside model context and provides:

- persistent identities/work tracking;
- mailboxes and handoffs;
- `seance --talk` as a one-shot question to a predecessor session;
- tiered escalation categories and routes;
- explicit guidance that ordinary information queries should not escalate to the Mayor/human.

Sources:

- <https://github.com/gastownhall/gastown>
- `docs/design/escalation.md`

### Field lessons

Public reports and project discussions show why durable work state and live runtime state must not be conflated:

- a central Mayor can become reactive or idle rather than proactively coordinating;
- cross-machine nudges can be one-way or reach the wrong identity/store;
- issue-graph state can diverge from current process/session state;
- stale escalation can become repeated control traffic if it automatically creates more model work.

### ELIOT adoption

Take:

```text
one-shot predecessor/owner query
information query before escalation
typed escalation classes
current facts outside model context
```

Reject:

```text
another work graph/task authority
Mayor as mandatory relay for every query
automatic stale re-escalation that starts models
live ownership inferred solely from issue state
```

## 5. Overstory

### Useful mechanisms

Overstory exposes:

- agent discovery by capability/state/parent;
- typed SQLite mail;
- isolated worktrees;
- task groups, monitoring and merge queues;
- explicit warnings that integration-boundary errors, token amplification, context fragmentation, merge conflicts and debugging forensics are normal swarm risks.

Sources:

- <https://github.com/Nokodoko/overstory>
- `README.md`
- `STEELMAN.md`
- `src/mail/agentic_instructions.md`

### Risky mechanisms for ELIOT

Its mail vocabulary includes task-adjacent `dispatch` and `assign`, group broadcasts and urgent/high auto-nudges.

### ELIOT adoption

Take:

```text
capability/current-state discovery
typed mail
explicit integration-risk accounting
```

Reject:

```text
broadcast groups
peer-visible assign/dispatch as ordinary mail
auto-nudge urgency that can trigger model work
second SQLite mail authority
```

## 6. MCP Agent Mail

### Useful UX

- mail rather than chat;
- exact recipients and subjects;
- directory and threaded pull views;
- advisory file/path reservations;
- no need for the human to relay every message.

Source:

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust>

### Boundaries

The existing Field Evidence document records:

- restrictive license rider;
- separate Git/SQLite authority;
- liveness spam;
- slow/fail-open reservation guards;
- placeholder identity creation;
- false release;
- direct-storage polling and storage/FD failures.

### ELIOT adoption

Take the UX and advisory-reservation concepts. Do not reuse code or storage.

## 7. CCCC

### Useful protocol semantics

At reviewed stable `v0.4.41`, useful source units are:

```text
crates/cccc-core/src/connect_delivery.rs
crates/cccc-core/src/inbox.rs
```

Take:

```text
delivery identity
actor generation
deadlines
digest-bound reply/cancel
participant membership checks
stored/claimed/accepted/failed/ambiguous vocabulary
cursor and pending-read recovery
```

Do not take:

```text
scheduler/group/task authority
append-only ledger as a second store
PTY/TUI process topology
whole bridge/runtime process model
```

Source:

- <https://github.com/ChesterRa/cccc/tree/v0.4.41>

## 8. Claw Council

### Useful boundary

At reviewed commit `ffa595abfb6f6671ac995041f9c44f5e1a67f50f`, Council provides:

- exact named participants;
- bounded rounds, turns, timeout and total budget;
- cancellation;
- compact context and final summary;
- maximum rounds as a valid result;
- advisory votes separate from downstream verification.

Source:

- <https://github.com/Enderfga/claw-orchestrator/blob/ffa595abfb6f6671ac995041f9c44f5e1a67f50f/src/kernel/nodes/council.ts>

### ELIOT adoption

Take bounded advisory deliberation. Do not take the workflow store, node replay or consensus as authority.

## 9. Multica and AutoGen field evidence

### Multica

Issue #8753 documents a Developer/Reviewer/Product Owner loop processing roughly 18 million tokens because comments doubled as execution triggers and long coding sessions were repeatedly resumed.

Source:

- <https://github.com/multica-ai/multica/issues/8753>

### AutoGen

Public issues document:

- agreement/politeness loops;
- full-history amplification;
- group broadcast concerns;
- speaker-selection failures;
- malformed-message cascades;
- goal drift;
- retry/backpressure storms.

Selected anchors:

- <https://github.com/microsoft/autogen/issues/108>
- <https://github.com/microsoft/autogen/issues/489>
- <https://github.com/microsoft/autogen/issues/907>
- <https://github.com/microsoft/autogen/issues/1006>
- <https://github.com/microsoft/autogen/issues/3679>
- <https://github.com/microsoft/autogen/issues/4623>
- <https://github.com/microsoft/autogen/issues/6479>
- <https://github.com/microsoft/autogen/issues/7321>
- <https://github.com/microsoft/autogen/issues/7409>
- <https://github.com/microsoft/autogen/issues/7487>

### ELIOT adoption

Convert reports into negative tests. Do not implement free-running GroupChat, LLM speaker selection or shared full history.

## 10. Sparse-communication research

Research lines such as AgentPrune, adaptive graph pruning, CONCAT and hierarchical/progressive debate support the design direction of sparse, progressively escalated communication. Debate research also warns that homogeneous agents can converge socially without becoming more correct and that answer agreement can hide incompatible reasoning.

These are design constraints, not direct product benchmarks for ELIOT.

Selected sources:

- <https://arxiv.org/abs/2410.02506>
- <https://arxiv.org/abs/2506.02951>
- <https://arxiv.org/abs/2605.29612>
- <https://arxiv.org/abs/2604.09679>
- <https://arxiv.org/abs/2605.00914>
- <https://arxiv.org/abs/2606.08457>

## 11. Fleet behavior → ELIOT requirement map

| Observed behavior | ELIOT requirement |
|---|---|
| General manager becomes a relay bottleneck | relevant-peer directory, cards, quick ask, peer-local handshake |
| Agents do not know who owns a seam | exact indexed owner resolution with ambiguity/no-send |
| Long questions repeat known facts | structured card-field lookup before delivery |
| Peer mail accidentally creates work | hard message vs follow-up-task separation |
| Busy recipients process mail late | durable pull/one-line safe-boundary header; no false immediate-delivery claim |
| Idle recipient starts a costly turn | ELIOT default forbids peer-triggered turn start |
| Recipient name typo creates orphan inbox | exact registered participant/generation, fail closed |
| Completion notifications flood context | replaceable current state and coalesced freshness, no durable liveness mail |
| Assignment replay/duplicate work | peer has no assignment method; manager operation remains idempotent authority |
| Central issue graph disagrees with live process | card/participant validity rechecked against current Attempt/ProducerRef/binding |
| Shared chat grows geometrically | sparse edges, one recipient, headers/cards, paged history |
| Peers socially agree on wrong contract | deterministic comparator, named dimensions, unknown prevents compatible |
| Every local detail waits for GM | peer-local autonomy envelope |
| Local agreement silently changes global policy | manager-required classifier and canonical-source pinning |
| Auditor reads all chatter | compact review packet with exact agreement/comparison/evidence |
| Hundreds of idle agents poll | no per-agent task/process; pull current state on demand |

## 12. Final source-derived rule

```text
Direct peer communication is useful when it carries one bounded fact or reconciles one explicit interface.
It becomes harmful when it also assigns work, wakes models, broadcasts history, changes authority or substitutes social agreement for verification.
```

ELIOT therefore makes direct coordination easy while keeping work assignment, global policy and acceptance explicit and separate.