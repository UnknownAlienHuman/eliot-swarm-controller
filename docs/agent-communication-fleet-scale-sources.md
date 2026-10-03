# ELIOT Agent Communication — Fleet-Scale Source Map
## Donor implementations, official guidance, scaling research and field reports

**Revision:** 2 — 2026-10-03  
**Companion to:** [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md)  
**Purpose:** preserve the evidence behind the product decisions. This file is not product authority and does not override the normative documents.

## 1. Evidence labels and rules

```text
PAPER       peer-reviewed/preprint research result
OFFICIAL    vendor documentation or engineering report
CODE        concrete inspected source behavior
ISSUE       concrete user report/reproduction
OWNER       current ELIOT operating evidence supplied by the owner
INFERENCE   design conclusion derived from the above
UNVERIFIED  plausible claim without the required source/E2E evidence
```

Rules:

- a benchmark result is not a production guarantee;
- a feature on `main` is not silently attributed to a stable release;
- a GitHub issue proves that a failure occurred, not its population frequency;
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

### ELIOT decision

Take:

- shared current state before chat;
- exact claims and overlap resolution;
- asynchronous sparse direct coordination;
- voluntary specialization/integrator intent;
- reusable seam protocols;
- candidate redundancy and failure takeover.

Do not take:

- prompt-only work authority;
- unrestricted self-assignment outside current ELIOT Task/Attempt scope;
- peer merge/acceptance authority;
- one shared mutable checkout;
- the implication that 1,024 active local processes are a first-host requirement.

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

ELIOT decision:

- active-turn count is a separately governed runtime resource;
- coordinator is not a message relay;
- state/artifacts bypass telephone-game summaries;
- topology/status is observable without full-transcript surveillance;
- communication never recursively spawns agents.

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

ELIOT decision:

- expose hotspots and allow one integrator instead of forcing parallel writers;
- preserve one writer/lease per mutable candidate scope;
- maximize collaboration before protected verification, not after it;
- prefer compact durable state over preserving long model context.

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

ELIOT decision:

- peer coordination always has `send_message` semantics;
- Task/runtime authority owns `followup_task` semantics;
- registered participant population does not determine active-turn concurrency;
- no peer message can silently become a runtime operation.

## 5. Scaling and communication-topology research

### 5.1 Coordination does not scale monotonically

- PAPER: [Towards a Science of Scaling Agent Systems](https://arxiv.org/abs/2512.08296).

Across 180 controlled configurations, the authors report:

- tool-heavy tasks suffer disproportionately from multi-agent overhead under fixed budgets;
- coordination shows diminishing/negative returns once the single-agent baseline is already strong;
- independent-agent error propagation was much larger than centralized containment in their setup;
- all tested multi-agent variants degraded sequential-reasoning tasks by 39–70%;
- optimal architecture depends on measurable task properties.

ELIOT implication: do not treat agent count as a universal improvement knob. Preserve central assignment/verification while allowing local peer coordination only where the work graph is actually parallel.

### 5.2 Direct messaging tends toward quadratic growth

- PAPER: [When Agents Coordinate: Measuring Coordination in Multi-Agent AI Coding](https://arxiv.org/abs/2608.16801).

Across 1,902 runs, the paper reports:

- direct messages initially grow close to quadratically with team size;
- much growth is an early introduction/coordination round;
- shared files reduced output tokens by about 42% at eight agents on message-heavy work;
- shared files add overhead when the task already carries coordination naturally;
- shared-spec tasks form dense networks, while pipeline tasks form sparse local-interface networks;
- merely naming one coordinator created no reliable hub or success gain.

ELIOT implication:

- structured cards/cells should replace repeated introductions and 1:1 restatement;
- topology must follow the task/dependency graph;
- a manager role alone does not solve coordination unless current state and authority are mechanical;
- ordinary pipeline integration should be sparse around local seams.

### 5.3 Dynamic topology can reduce cost

- PAPER: [Adaptive Graph Pruning for Multi-Agent Communication](https://journals.sagepub.com/doi/10.3233/FAIA251326).
- PAPER: [GoAgent: Group-of-Agents Communication Topology Generation](https://arxiv.org/abs/2603.19677).
- PAPER: [AgentConductor: Topology Evolution for Multi-Agent Code Generation](https://arxiv.org/abs/2602.17100).

These systems report performance/cost improvements from adapting agent count, groups and communication density to the task rather than using a complete/static graph. Their learned benchmark topologies are not directly reusable as ELIOT production policy.

ELIOT implication: build a deterministic sparse relevance graph from current Tasks/contracts/scopes first. Later learned ranking/pruning is optional and must never become authority or hide coverage gaps.

### 5.4 Dense coupling can collapse diversity

- PAPER: [Diversity Collapse in Multi-Agent LLM Systems](https://aclanthology.org/2026.findings-acl.13/).

The paper reports diminishing group-size returns and faster premature convergence under dense communication topology.

ELIOT implication:

- do not broadcast all peer reasoning to all models;
- preserve independent exploration/review contexts;
- Concilium round 1 remains blind/independent;
- agreement is revisioned and can be refuted by a later counterexample;
- shared state contains claims/evidence, not everyone's full reasoning transcript.

### 5.5 Internal communication is a security surface

- PAPER: [AgentLeak: A Full-Stack Benchmark for Privacy Leakage in Multi-Agent LLM Systems](https://arxiv.org/abs/2602.11510).

The benchmark reports substantial leakage through internal inter-agent channels that output-only auditing misses. Exact percentages are benchmark/model specific and are not treated as ELIOT production rates.

ELIOT implication:

- participant visibility stays Task/Attempt/scope constrained;
- a message/cell never widens artifact or secret access;
- cards/messages avoid credentials and unnecessary private data;
- internal coordination is included in audit/redaction policy;
- a global searchable roster is metadata-minimal and never a global readable transcript.

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

Rejected for ELIOT:

- automatic idle-session wake;
- automatic team formation;
- shared Task authority;
- full authority envelope in human-visible chat;
- assuming a mailbox write means a busy model saw the correction.

### Field reports

#### Recursive fan-out

- ISSUE: [#68110 — recursive unbounded child-agent fan-out](https://github.com/anthropics/claude-code/issues/68110).

One request reportedly produced 48+ agents and roughly 1.5 million tokens; the useful research was complete within the first few workers. This supports removing spawn/delegation from the participant communication surface.

#### Fleet failure visibility

- ISSUE: [#66686 — large workflow agents need first-class visibility](https://github.com/anthropics/claude-code/issues/66686).

A roughly 70-agent repository review reportedly lost 26 workers after a network failure, including 10 of 12 finder agents; diagnosis required raw transcript inspection. This supports durable agent/edge/status topology and explicit lost/stale outcomes.

#### Delayed presentation to busy peers

- ISSUE: [#99111 — interactive Agent Teams message withheld until turn end](https://github.com/anthropics/claude-code/issues/99111).
- ISSUE: [#98998 — running teammate sees message only after idle](https://github.com/anthropics/claude-code/issues/98998).

These distinguish `stored` from `presented`. ELIOT must never tell the sender that the recipient has seen a message merely because mailbox admission succeeded.

#### Coordination tool missing

- ISSUE: [#81185 — restricted teammate cannot SendMessage its report](https://github.com/anthropics/claude-code/issues/81185).
- ISSUE: [#68408 — agent description advertises unavailable SendMessage](https://github.com/anthropics/claude-code/issues/68408).

ELIOT verifies the actual coordination capability profile at registration and exposes only tools that the client/runtime can use.

#### Peer-message UI/authority spam

- ISSUE: [#80454 — peer authority envelope rendered as repeated full chat bubbles](https://github.com/anthropics/claude-code/issues/80454).
- ISSUE: [#80625 — remote control shows reminder boxes instead of useful peer content](https://github.com/anthropics/claude-code/issues/80625).

ELIOT keeps authority metadata machine-readable and renders one compact sender/subject/disposition row to humans.

#### Human as relay

- ISSUE/REQUEST: [#28300 — multi-agent collaboration across machines](https://github.com/anthropics/claude-code/issues/28300).

The request describes the product need accurately: independently owned services should negotiate schemas/integration points without making the human copy every decision between sessions.

## 7. Donor implementations

### 7.1 Overstory

- CODE/DOC: [Nokodoko/overstory](https://github.com/Nokodoko/overstory).

Take:

- capability/state/parent discovery;
- SQLite typed mail concepts;
- separate worktrees;
- fleet status, trace/replay and cost metrics;
- explicit recognition that merge conflicts, error compounding and cost amplification are normal risks.

Reject:

- broadcast groups as normal coordination;
- peer `assign`/`dispatch` semantics;
- urgent mail auto-nudging a model;
- a second mail database inside ELIOT.

### 7.2 Gas Town

- CODE/DOC: [gastownhall/gastown](https://github.com/gastownhall/gastown).

Take:

- durable identities/work state;
- one-shot predecessor query (`seance` pattern);
- tiered escalation where ordinary information queries should not escalate;
- explicit runtime-capacity governor.

Reject:

- another Beads/work graph as ELIOT authority;
- Mayor as relay for every peer fact;
- stale timers that automatically create model work.

### 7.3 Agency Swarm

- CODE/DOC: [VRSEN/agency-swarm](https://github.com/VRSEN/agency-swarm).

Take typed directional communication flows, custom decision/context fields and explicit handoff separated from ordinary send. ELIOT derives flows dynamically from current Task/contract/scope relations rather than one global static graph.

### 7.4 CCCC, Multica, MCP Agent Mail, Claw, AutoGen

Detailed source paths/issues are already classified in:

- [Peer Autonomy Source Map](agent-communication-peer-autonomy-sources.md);
- [Field Evidence and Donor Map](agent-communication-field-evidence.md);
- [Communication and Concilium](agent-communication-concilium.md).

Fleet conclusions:

- CCCC has strong delivery/reply/cancel/cursor semantics but current per-actor bridge/runtime scaling and resume failures make it unsuitable as ELIOT's default fleet process model.
- Multica shows why comments/mentions cannot be both communication and execution routing; one reported workflow processed about 18 million tokens through ping-pong.
- MCP Agent Mail has good mail/reservation UX but adds a separate authority/storage product and restrictive licensing concerns.
- Claw supports bounded advisory councils and protected verification separation.
- AutoGen reports reinforce termination, full-history, speaker-selection, backpressure and malformed-message failure cases.

## 8. Owner ELIOT fleet evidence

OWNER evidence from the current swarm includes:

- several managers receiving the same stale queue and duplicating work;
- activity metrics overstating writers by aggregating unrelated child sessions;
- long-lived contexts/repeated full histories consuming extreme tokens;
- partial submissions producing more review/merge cost than completed work;
- mass replay of old submissions flooding acceptance and causing negative progress;
- global reminders/status messages consuming context without changing code;
- one shared file/contract becoming a hotspot needing one owner;
- agents blocked because they could not directly ask the current contract owner;
- independent components diverging because producer/consumer seams were not agreed before implementation.

Derived requirements:

```text
fresh indexed ownership
state-first coordination
direct exact owner asks
revisioned integration cells
one mutable owner per candidate scope
no manager relay for routine facts
no peer model wake
no all-to-all prompt context
protected verification and acceptance
small-sample measurement before mass fleet action
```

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

## 10. Evidence-based balance

```text
maximum freedom:
  discover
  publish
  ask
  answer/abstain
  compare
  volunteer
  coordinate sequencing
  record reversible assumptions
  agree inside current authority

mechanical structure:
  scoped identity and visibility
  sparse relevance graph
  revisioned cards/cells
  explicit delivery disposition
  one mutable owner
  bounded presentation/backpressure
  no recursive spawn
  independent disagreement preserved

central authority only for:
  new work/active turn
  scope or canonical contract expansion
  security/identity/persistence/lifecycle
  irreversible effects
  merge/acceptance
  Concilium rounds

strict proof at the edge:
  independent verifier
  exact evidence
  protected acceptance
```

Communication is a tool agents can use whenever it helps, not a mandatory ceremony and not a second uncontrolled workflow engine.