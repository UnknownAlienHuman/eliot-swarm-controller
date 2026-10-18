# Agent communication — recorded fleet-scale research

**Research date:** 2026-10-03. Findings retain their original experiment/report
scope and have not been revalidated during this documentation edit.
The [communication contract](agent-communication-program.md) defines current
requirements; the research adds no fleet-size gate, adoption checklist or
implementation sequence.

This was the evidence for the
[former Fleet-Scale Freedom design](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-fleet-scale-freedom.md).
The design's required integration cells, staged fleet qualification and amendment
program have been removed from the active documentation.

## 1. Evidence labels and rules

```text
PAPER       peer-reviewed/preprint research result
OFFICIAL    vendor documentation or engineering report
CODE        concrete inspected source behavior
ISSUE       concrete user report/reproduction
OWNER       operating experience recorded in the original review
INFERENCE   design conclusion derived from the above
UNVERIFIED  plausible claim without the required source/E2E evidence
```

Rules:

- a benchmark result is not a production guarantee;
- a feature on `main` is not silently attributed to a stable release;
- a GitHub issue records a reported failure, not independent proof or its population frequency;
- large registered population is not evidence for the same number of simultaneous model turns;
- stars, ratings and dashboard activity do not prove correctness, cleanup or economical coordination;
- research numbers are carried with their task, topology, model and budget limitations.

## 2. Microsoft Agensh — direct 1,024-agent research evidence

### Sources

- PAPER: [Agensh: Scaling Organizational Intelligence to 1,024 Agents](https://arxiv.org/abs/2609.26781), submitted 2026-09-22.
- OFFICIAL project page: [agens-harness.github.io/project](https://agens-harness.github.io/project/).
- UNVERIFIED CODE: the paper/project page references `microsoft/Agensh`, but the repository was not publicly resolvable during this review. Do not claim source-level verification.

### Reported architecture

No central orchestrator. Concurrent workers repeat:

```text
gather context
claim/self-assign a subtask
act and share findings
verify
merge progress
```

Infrastructure:

```text
shared workspace
message interface
shared reusable context
```

Shared context distinguishes observations, facts, failures, claims and patch summaries. Direct messages resolve overlaps/conflicts; shared context carries reusable state.

### Reported results

```text
five hardest ProgramBench tasks
1 agent      19.31% mean final pass rate
128 agents   28.78%

pandoc, six-hour/no-Internet budget
1 agent      33.89%
128 agents   50.94%
1,024 agents 55.06%
```

The 128 → 1,024 increase is 4.12 points. Returns are clearly diminishing even though they remain positive on this task.

### Cooperation described by the authors

```text
8 agents      interface agreement and component splitting
32 agents     multi-worker integration and withdrawn approval after counterexample
128 agents    specialization and reusable integration workflows
1,024 agents  redundant integrators and takeover after failures
```

These are qualitative trajectory observations, not a quantified general law.

## 3. Anthropic official multi-agent experience

### 3.1 Research system

- OFFICIAL: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system).

Reported:

- 90.2% improvement over a single-agent baseline on an internal research evaluation;
- approximately 15× ordinary chat token usage;
- early failures that spawned about 50 subagents for simple queries, searched indefinitely or produced excessive updates;
- better behavior with explicit objective/output/tool/source/boundary instructions;
- less natural parallelism for coding with shared state than breadth-first research;
- direct artifact output can avoid coordinator summarization loss;
- stateful agents require checkpoints/resume and compound failures.

### 3.2 Sixteen-agent C compiler

- OFFICIAL: [Building a C compiler with a team of parallel Claudes](https://www.anthropic.com/engineering/building-c-compiler).

Reported:

- 16 agents, roughly 2,000 sessions, about two billion input tokens and 140 million output tokens;
- separate containers and a shared Git repository;
- frequent merge conflicts;
- fresh sessions oriented through durable progress files;
- strong tests/oracles were essential;
- parallelism failed when all workers converged on one monolithic kernel/verifier issue;
- independently verifiable partitions restored useful parallel work;
- output still required review and had quality limitations.

### 3.3 Automated Alignment Researchers

- OFFICIAL: [Automated Alignment Researchers](https://www.anthropic.com/research/automated-alignment-researchers).

Nine agents received different starting directions, shared a forum and code storage, and self-directed experiments over 800 cumulative agent-hours. This supports bounded free collaboration and differentiated exploration, but it is a high-cost research setting, not evidence for unbounded coding swarms.

## 4. OpenAI official multi-agent semantics

- OFFICIAL: [Responses API multi-agent guide](https://developers.openai.com/api/docs/guides/responses-multi-agent).

Key boundary:

```text
send_message   communicates information; no new turn
followup_task  assigns additional work; starts/resumes a turn
```

Official guidance favors independent bounded work and warns against ordered chains, heavy shared mutable state and one slow operation split artificially.

## 5. Scaling and communication-topology research

### 5.1 Coordination does not scale monotonically

- PAPER: [Towards a Science of Scaling Agent Systems](https://arxiv.org/abs/2512.08296).

Across 180 controlled configurations, the authors report:

- tool-heavy tasks suffer disproportionately from multi-agent overhead under fixed budgets;
- coordination shows diminishing/negative returns once the single-agent baseline is already strong;
- independent-agent error propagation was much larger than centralized containment in their setup;
- all tested multi-agent variants degraded sequential-reasoning tasks by 39–70%;
- optimal architecture depends on measurable task properties.

### 5.2 Direct messaging tends toward quadratic growth

- PAPER: [When Agents Coordinate: Measuring Coordination in Multi-Agent AI Coding](https://arxiv.org/abs/2608.16801).

Across 1,902 runs, the paper reports:

- direct messages initially grow close to quadratically with team size;
- much growth is an early introduction/coordination round;
- shared files reduced output tokens by about 42% at eight agents on message-heavy work;
- shared files add overhead when the task already carries coordination naturally;
- shared-spec tasks form dense networks, while pipeline tasks form sparse local-interface networks;
- merely naming one coordinator created no reliable hub or success gain.

### 5.3 Dynamic topology can reduce cost

- PAPER: [Adaptive Graph Pruning for Multi-Agent Communication](https://journals.sagepub.com/doi/10.3233/FAIA251326).
- PAPER: [GoAgent: Group-of-Agents Communication Topology Generation](https://arxiv.org/abs/2603.19677).
- PAPER: [AgentConductor: Topology Evolution for Multi-Agent Code Generation](https://arxiv.org/abs/2602.17100).

These systems report performance/cost improvements from adapting agent count, groups and communication density to the task rather than using a complete/static graph. Their learned benchmark topologies are not directly reusable as ELIOT production policy.

### 5.4 Dense coupling can collapse diversity

- PAPER: [Diversity Collapse in Multi-Agent LLM Systems](https://aclanthology.org/2026.findings-acl.13/).

The paper reports diminishing group-size returns and faster premature convergence under dense communication topology.

### 5.5 Internal communication is a security surface

- PAPER: [AgentLeak: A Full-Stack Benchmark for Privacy Leakage in Multi-Agent LLM Systems](https://arxiv.org/abs/2602.11510).

The benchmark reports substantial leakage through internal inter-agent channels that output-only auditing misses. Exact percentages are benchmark/model specific and are not treated as ELIOT production rates.

## 6. Claude Code Agent Teams and cross-session messaging

### Official sources

- OFFICIAL: [Agent teams](https://code.claude.com/docs/en/agent-teams).
- OFFICIAL: [Cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging).

Useful behavior:

- independent teammate contexts;
- direct addressed peer messages;
- compact previews;
- explicit accept/hold/refuse policy;
- mailbox admission distinct from later presentation;
- peer messages carry no user authority.

### Field reports

#### Recursive fan-out

- ISSUE: [#68110 — recursive unbounded child-agent fan-out](https://github.com/anthropics/claude-code/issues/68110).

One request reportedly produced 48+ agents and roughly 1.5 million tokens; the useful research was complete within the first few workers.

#### Fleet failure visibility

- ISSUE: [#66686 — large workflow agents need first-class visibility](https://github.com/anthropics/claude-code/issues/66686).

A roughly 70-agent repository review reportedly lost 26 workers after a network failure, including 10 of 12 finder agents; diagnosis required raw transcript inspection.

#### Delayed presentation to busy peers

- ISSUE: [#99111 — interactive Agent Teams message withheld until turn end](https://github.com/anthropics/claude-code/issues/99111).
- ISSUE: [#98998 — running teammate sees message only after idle](https://github.com/anthropics/claude-code/issues/98998).

These reports distinguish mailbox storage from presentation to a busy model.

#### Coordination tool missing

- ISSUE: [#81185 — restricted teammate cannot SendMessage its report](https://github.com/anthropics/claude-code/issues/81185).
- ISSUE: [#68408 — agent description advertises unavailable SendMessage](https://github.com/anthropics/claude-code/issues/68408).

The reported failure is an advertised coordination capability that the active client could not use.

#### Peer-message UI/authority spam

- ISSUE: [#80454 — peer authority envelope rendered as repeated full chat bubbles](https://github.com/anthropics/claude-code/issues/80454).
- ISSUE: [#80625 — remote control shows reminder boxes instead of useful peer content](https://github.com/anthropics/claude-code/issues/80625).

The reports concern repeated transport/authority metadata displacing useful peer content in the UI.

#### Human as relay

- ISSUE/REQUEST: [#28300 — multi-agent collaboration across machines](https://github.com/anthropics/claude-code/issues/28300).

The request describes the product need accurately: independently owned services should negotiate schemas/integration points without making the human copy every decision between sessions.

## 7. Recorded donor implementations

| Source | Behavior recorded by the review |
| --- | --- |
| [Overstory](https://github.com/Nokodoko/overstory) | Agent discovery, typed mail, worktrees, fleet/cost views; also broadcast, task-like mail and urgent nudges. |
| [Gas Town](https://github.com/gastownhall/gastown) | Durable work state, one-shot predecessor queries and tiered escalation; a separate work graph/runtime system. |
| [Agency Swarm](https://github.com/VRSEN/agency-swarm) | Directional communication flows, structured context fields and handoff separate from ordinary send. |

The [peer source map](agent-communication-peer-autonomy-sources.md) and
[field evidence](agent-communication-field-evidence.md) retain CCCC, Multica,
MCP Agent Mail, Claw and AutoGen references. The
[former Concilium design](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/agent-communication-concilium.md)
records how those sources were interpreted at the time; that design is not a
completion requirement for the compact controller.

## 8. Owner ELIOT fleet evidence

The original review recorded the following owner-supplied swarm experience:

- several managers receiving the same stale queue and duplicating work;
- activity metrics overstating writers by aggregating unrelated child sessions;
- long-lived contexts/repeated full histories consuming extreme tokens;
- partial submissions producing more review/merge cost than completed work;
- mass replay of old submissions flooding acceptance and causing negative progress;
- global reminders/status messages consuming context without changing code;
- one shared file/contract becoming a hotspot needing one owner;
- agents blocked because they could not directly ask the current contract owner;
- independent components diverging because producer/consumer seams were not agreed before implementation.

## 9. What the evidence does not support

Do not claim:

- that 1,024 agents are economical at equal compute;
- that the Agensh result transfers to arbitrary repositories;
- that thousands of model processes fit one Windows host;
- that self-organization removes assignment/acceptance authority;
- that peer consensus establishes correctness;
- that a bigger team improves a monolithic/shared-state task;
- that a stored message was presented to a busy model;
- that user issue counts provide a failure rate;
- that learned topology papers are production policy;
- that current ELIOT already meets the fleet contour.
