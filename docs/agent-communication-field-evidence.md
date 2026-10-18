# Agent communication — recorded field evidence

**Research date:** 2026-10-02. These are recorded source, issue and paper findings;
this documentation edit did not rerun the experiments or recheck current donor
releases. No donor was installed or load-tested by the original review.
The [communication contract](agent-communication-program.md) owns current scope.
This file contains evidence, not required implementations or a test campaign.

The [original review](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-field-evidence.md)
explains the now-superseded Concilium design. Its product prescriptions, required
round metadata and acceptance lists have been removed from this research copy.
`ISSUE` means a report, `PAPER` a result under its stated setup, and `FIELD` supplied
operating experience; none establishes current ELIOT behavior or failure rates.

## 1. Reported communication failures

### 1.1 Politeness and agreement loops

**ISSUE — AutoGen #108, #907, #7409.** Users report two-agent and group-chat runs continuing through blank messages, polite acknowledgements and repeated “how else can I help” turns until the hard reply/round limit. The proposed diagnosis in #7409 is an “infinite agreement loop”: no semantic progress is required, so agents can consume the budget while merely agreeing.

Relevant reports:

- <https://github.com/microsoft/autogen/issues/108>
- <https://github.com/microsoft/autogen/issues/907>
- <https://github.com/microsoft/autogen/issues/7409>
- <https://github.com/microsoft/autogen/issues/538>

### 1.2 Full-history amplification

**ISSUE — AutoGen #1006, #1070, #4623.** Users report that group-chat participants receive the full growing history and cannot reliably restrict a participant to the final relevant input. The graph-flow proposal explicitly identifies that “every agent sees every message” violates context independence.

- <https://github.com/microsoft/autogen/issues/1006>
- <https://github.com/microsoft/autogen/issues/1070>
- <https://github.com/microsoft/autogen/issues/4623>

**FIELD — ELIOT legacy swarm.** The supplied manager brief records managers sending 260–360 thousand input tokens per step, writers reaching 110–180 thousand, and one Codex thread processing roughly 1.28 billion input tokens during a long session. Repeated reminders and Issue history were part of that growth. The operational fix was fresh context per assignment, compact Task projections, narrow reading and short writer reports.

### 1.3 Broadcast creates accidental work and queue pressure

**ISSUE — AutoGen #489.** A user questions why group-chat messages are broadcast to all agents instead of only the selected speaker and notes that model servers may queue every incoming message.

- <https://github.com/microsoft/autogen/issues/489>

**ISSUE — MCP Agent Mail #264.** A liveness subsystem produced 24,132 of 26,092 mailbox rows in one deployment. Activity/status chatter became about 92% of all mail and consumed roughly one CPU core while targeting idle/dead identities.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/264>

### 1.4 Comment-driven routing causes token ping-pong

**ISSUE — Multica #8753.** A real Developer → Reviewer → Developer → Reviewer → Product Owner flow processed about 18 million tokens. Coordination comments caused full coding-agent invocations, long sessions were repeatedly resumed, and failures generated further explanatory turns.

- <https://github.com/multica-ai/multica/issues/8753>

The donor audit records the same causal distinction: ordinary status/failure comments entered the squad-leader fallback and triggered coordination-only model turns.

### 1.5 Speaker selection is an unreliable control plane

**ISSUE — AutoGen #842, #6523, #7677/#7678.** Reports include speaker-name normalization failures, graph edges not selected when tools are involved, and round-robin omissions.

- <https://github.com/microsoft/autogen/issues/842>
- <https://github.com/microsoft/autogen/issues/6523>
- <https://github.com/microsoft/autogen/issues/7678>

### 1.6 One malformed message can poison a group

**ISSUE — AutoGen #3679, #6479.** Invalid tool-call history and one empty message caused failures to cascade through group participants.

- <https://github.com/microsoft/autogen/issues/3679>
- <https://github.com/microsoft/autogen/issues/6479>

### 1.7 Goal drift is silent

**ISSUE — AutoGen #7487.** A contributor describes a structural conflict between coordinating work and preserving the original mission. Traffic management can remain apparently successful while the result drifts from the original objective.

- <https://github.com/microsoft/autogen/issues/7487>

### 1.8 Backpressure and retry storms

**ISSUE — AutoGen #7321.** The report describes cascading retries when an agent is saturated and capacity is implicit in each caller.

- <https://github.com/microsoft/autogen/issues/7321>

## 2. Debate and consensus research

### 2.1 Homogeneous debate can reduce quality while increasing cost

**PAPER — The Cost of Consensus (arXiv:2605.00914).** In the reported setup, ten homogeneous agents and three rounds showed:

- modal adoption up to 85.5%;
- contextual-fragility vulnerability up to 70%;
- oracle gap up to 32.3 percentage points;
- high conformity even with only two peer rationales;
- 2.1–3.4× token use for equal or lower accuracy than isolated self-correction.

<https://arxiv.org/abs/2605.00914>

### 2.2 Answer agreement can hide reasoning disagreement

**PAPER — The Consistency Illusion (arXiv:2606.08457).** The study reports that answer-level consensus can increase while semantic reasoning alignment decreases. Its Grounded Debate Protocol improves alignment by requiring named facts and explicit stances on claims without adding model calls.

<https://arxiv.org/abs/2606.08457>

### 2.3 Consensus itself is not a safe objective

**PAPER — Free-MAD (arXiv:2509.11035).** The paper identifies multi-round token overhead, conformity/error propagation and unfair/random majority voting, then evaluates a consensus-free trajectory-scoring design.

<https://arxiv.org/abs/2509.11035>

**PAPER — Emergence of Biased Consensus (arXiv:2608.02827).** The reported experiments model a conformity threshold beyond which collective bias emerges; heterogeneous agents reduce that effect in the tested settings.

<https://arxiv.org/abs/2608.02827>

### 2.4 Sparse communication is usually cheaper

**PAPER — AgentPrune / Cut the Crap (arXiv:2410.02506).** The paper treats communication redundancy as a graph-pruning problem and reports 28.1–72.8% token reduction in its benchmark setup.

<https://arxiv.org/abs/2410.02506>

**PAPER — Adaptive Graph Pruning (arXiv:2506.02951).** The paper adapts both agent count and topology and reports more than 90% token reduction in the tested benchmarks.

<https://arxiv.org/abs/2506.02951>

**PAPER — CONCAT (arXiv:2605.29612).** The method clusters initial answers, selects leaders and prunes predicted low-benefit interactions; the reported experiments show up to 2.02× efficiency and 50.1% latency reduction on one setup.

<https://arxiv.org/abs/2605.29612>

**PAPER — HCP-MAD (arXiv:2604.09679).** The design starts with a heterogeneous pair, stops easy cases early and escalates only unresolved cases.

<https://arxiv.org/abs/2604.09679>

## 3. Identity and state failures from MCP Agent Mail

The reviewed MCP Agent Mail reports concern recipient identity, reservations and its own storage/runtime behavior.

### 3.1 Registration is not liveness

**ISSUE #334.** `last_active_ts` was not updated after registration, so the system selected a recently registered but idle agent instead of the active owner. Deferred “touch” work could also be orphaned across pool generations.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/334>

### 3.2 Unknown recipients and placeholder identities

**ISSUE #301.** Sending to an unknown recipient silently created a placeholder identity despite documentation promising fail-fast behavior.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/301>

### 3.3 Reported false release

**ISSUE #329.** A CLI shape mismatch caused reservation release to do nothing while the agent was told that it had released the reservation.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/329>

### 3.4 Humanized timestamps are not machine contracts

**ISSUE #330.** Machine JSON exposed only humanized grant age, preventing reliable age calculations.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/330>

### 3.5 Direct-storage pollers can destroy capacity

**ISSUE #298.** Mixed lock protocols, retained zombie work and direct-storage pollers could reduce admission capacity to zero.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/298>

### 3.6 Reservation reads and fail-open guards

**ISSUE #337.** Reservation reads took 5–20 seconds under load; pre-commit guards timed out and effectively failed open. The report also identifies write amplification and stale backup companions.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/337>

### 3.7 Status reads starved by repair scans

**ISSUE #138, #274, #319.** Reports describe expensive overview queries, Doctor scans starving dispatch and circuit-breaker policy that blocked reservation reads despite being documented as write-only.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/138>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/274>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/319>

### 3.8 Storage failure reports

**ISSUE #278, #333.** Reports include WAL/storage corruption under concurrency and 2,011 leaked file descriptors to the main SQLite file in 3.5 hours.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/278>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/333>

## 4. Recorded donor source anchors

| Recorded source | Mechanism examined | Scope of the observation |
| --- | --- | --- |
| [CCCC v0.4.41](https://github.com/ChesterRa/cccc/tree/v0.4.41) | `crates/cccc-core/src/connect_delivery.rs` and `inbox.rs`: delivery/reply/cancel identity, deadlines, cursor and pending-read recovery. | The protocol exists inside a separate group/task/runtime product; a source example is not a required dependency. |
| [MCP Agent Mail source](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/tree/21a25c2bcfd20c9b31bcb109c294d17411eb5ba1) | `crates/mcp-agent-mail-tools/src/reservations.rs`, `messaging.rs`, `crates/mcp-agent-mail-db/src/queries.rs`, `crates/mcp-agent-mail-guard/src/lib.rs`: addressed mail and advisory reservations. | The review recorded a separate Git/SQLite/server/TUI product and a custom license rider. This records the inspected source; it does not authorize copying it. |
| [Claw council source](https://github.com/Enderfga/claw-orchestrator/blob/ffa595abfb6f6671ac995041f9c44f5e1a67f50f/src/kernel/nodes/council.ts) | Named participants, bounded rounds/turns/timeouts/budget, cancellation, compact summary and advisory votes. | A council pattern, not evidence that ELIOT needs a separate council engine. |
| [Multica report #8753](https://github.com/multica-ai/multica/issues/8753) | Comment-driven execution and repeated session resume. | Reported cost/failure example; not a benchmark of every version. |
| [A2A specification](https://a2a-protocol.org/latest/specification/) | Distinct Message, Task, Artifact and context identifiers. | External protocol vocabulary; no new local protocol is implied. |
| [FIPA ACL](https://www.fipa.org/repository/aclspecs.html) | Speech acts such as inform, query, propose and not-understood. | Vocabulary reference, not a conversation platform requirement. |

AutoGen and debate/topology paper references remain with their reported failures
and results above. Reported benchmark gains do not establish gains on Eliot's
coding work, models or compute budget.

## 5. Historical local-source note

A [2026-10-06 note in the old implementation checklist](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-implementation-checklist.md#146-current-source-note--2026-10-06)
recorded eight Concilium methods, Store actor/slot checks, independent first-round
positions and MCP/CLI routing. That note explicitly called its source uncompiled
and untested. It also recorded no native/model launch or Task acceptance from
those methods. This is a dated source observation, not the current completion
status or a requirement to keep the council subsystem.
