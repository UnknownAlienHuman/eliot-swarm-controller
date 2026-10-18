# Agent communication — recorded peer-autonomy sources

**Research date:** 2026-10-03. This file retains the original source/document/report
observations; the sources were not revalidated during this documentation edit.
Only the [communication contract](agent-communication-program.md) defines current
scope. Donor mechanisms and reported defects are research inputs, not required
features, compatibility promises or a mandatory test list.

The historical design consumers were
[Peer Autonomy](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-peer-autonomy.md) and
[Peer Autonomy Implementation](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-peer-autonomy-implementation.md).
Their adoption instructions, agreement requirements and implementation queues
are superseded. A public report describes its recorded case, not every release.

## 1. Claude Code Agent Teams and cross-session messaging

### Official behavior recorded

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

### Reported field failures

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

## 2. OpenAI Responses Multi-agent

### Official distinction

The reviewed OpenAI multi-agent guide distinguished:

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

## 3. Agency Swarm

### Useful mechanism

Agency Swarm defines explicit directional `communication_flows` and allows a message tool to require structured fields such as key decisions and context.

Source:

- <https://github.com/VRSEN/agency-swarm>
- `examples/custom_send_message.py`

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

### Other exposed mechanisms

Its mail vocabulary includes task-adjacent `dispatch` and `assign`, group broadcasts and urgent/high auto-nudges.

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

The [recorded field evidence](agent-communication-field-evidence.md) includes:

- restrictive license rider;
- separate Git/SQLite authority;
- liveness spam;
- slow/fail-open reservation guards;
- placeholder identity creation;
- false release;
- direct-storage polling and storage/FD failures.

## 7. CCCC

### Useful protocol semantics

At reviewed stable `v0.4.41`, useful source units are:

```text
crates/cccc-core/src/connect_delivery.rs
crates/cccc-core/src/inbox.rs
```

The recorded units cover delivery identity, actor generations, deadlines,
reply/cancel checks, delivery dispositions and cursor/pending-read recovery.
The donor also owns group/task scheduling, its own ledger and a bridge/runtime
process model; these are separate from the message primitive.

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
