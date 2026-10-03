# ELIOT Agent Communication — Fleet-Scale Source Map
## Donor implementations, official guidance and field reports for tens to 1,024 agents

**Revision:** 1 — 2026-10-03  
**Companion to:** [Fleet-Scale Freedom](agent-communication-fleet-scale-freedom.md)  
**Purpose:** preserve the evidence behind the product decisions. This file is not product authority and does not override the normative documents.

## 1. Evidence labels

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
- a GitHub issue is evidence that a failure occurred, not its population frequency;
- a large registered population is not evidence for the same number of simultaneous model turns;
- stars, ratings and dashboard activity do not prove correctness, cleanup or economical coordination.

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

The paper's shared context distinguishes work/findings such as observations, facts, failures, claims and patch summaries. Direct messages resolve overlaps/conflicts; shared context carries reusable state.

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

### Observed cooperation described by the authors

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

- multi-agent system outperformed a single-agent baseline by 90.2% on an internal research evaluation;
- multi-agent research consumed approximately 15× the tokens of ordinary chat;
- early failures included spawning about 50 subagents for simple queries, endless search and excessive status updates;
- practical prompts bound objectives, outputs, tools/sources and work boundaries;
- coding work with shared state is less naturally parallel than breadth-first research;
- direct artifact output can avoid coordinator summarization loss;
- stateful agents require checkpoints/resume and compound failures.

ELIOT decision:

- active-turn count is a separately governed runtime resource;
- a coordinator is not a message relay;
- state and artifacts bypass telephone-game summaries;
- topology/status is observable without full-transcript surveillance;
- communication never recursively spawns agents.

### 3.2 Sixteen-agent C compiler

- OFFICIAL: [Building a C compiler with a team of parallel Claudes](https://www.anthropic.com/engineering/building-c-compiler).

Reported:

- 16 agents, roughly 2,000 sessions, about two billion input tokens and 140 million output tokens;
- separate containers and a shared Git repository;
- frequent merge conflicts;
- fresh sessions used durable progress files;
- strong tests/oracles were essential;
- parallelism failed when every worker converged on one monolithic kernel/verifier issue;
- refactoring the oracle/problem into independently verifiable subsets restored useful parallel work;
- output still required review and had quality limitations.

ELIOT decision:

- expose hotspots and allow one integrator instead of forcing parallel writers;
- preserve one writer/lease per mutable candidate scope;
- maximize collaboration before the protected verifier, not after it;
- make compact durable state more important than preserving long model context.

### 3.3 Automated Alignment Researchers

- OFFICIAL: [Automated Alignment Researchers](https://www.anthropic.com/research/automated-alignment-researchers).

Nine agents received different starting directions, shared a forum and code storage, and self-directed experiments over 800 cumulative agent-hours. This supports a bounded form of free collaboration and differentiated exploration, but it is a research setting with explicit infrastructure and high cost, not evidence for unbounded coding swarms.

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

## 5. Claude Code Agent Teams and cross-session messaging

### Official sources

- OFFICIAL: [Agent teams](https://code.claude.com/docs/en/agent-teams).
- OFFICIAL: [Cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging).

Useful behavior:

- independent teammate contexts;
- direct addressed peer messages;
- compact message previews;
- explicit accept/hold/refuse policy;
- mailbox admission can be distinct from later presentation;
- messages from peers carry no user authority.

Rejected behavior for ELIOT:

- automatic idle-session wake;
- automatic team formation;
- shared Task authority;
- full message/authority envelope in human-visible chat;
- assuming a mailbox write means a busy model saw the correction.

### Field reports

#### Recursive fan-out

- ISSUE: [#68110 — General-purpose sub-agents recursively spawn unbounded child agents](https://github.com/anthropics/claude-code/issues/68110).

One request reportedly produced 48+ agents and roughly 1.5 million tokens; the useful research was complete within the first few workers. This is direct evidence for removing spawn/delegation from the participant communication surface.

#### Fleet failure visibility

- ISSUE: [#66686 — workflow subagents need first-class visibility](https://github.com/anthropics/claude-code/issues/66686).

A roughly 70-agent repository review reportedly lost 26 workers after a network failure, including most finder agents; diagnosis required manual transcript inspection. This supports durable agent/edge/status topology and explicit lost/stale outcomes.

#### Delayed presentation to busy peers

- ISSUE: [#99111 — interactive Agent Teams message arrives after the turn ends](https://github.com/anthropics/claude-code/issues/99111).
- ISSUE: [#98998 — running teammate sees message only after idle](https://github.com/anthropics/claude-code/issues/98998).

These distinguish `stored` from `presented`. ELIOT must never tell the sender that the recipient has seen a message merely because mailbox admission succeeded.

#### Agent cannot report because coordination tool is missing

- ISSUE: [#81185 — restricted teammate has no SendMessage/ToolSearch](https://github.com/anthropics/claude-code/issues/81185).
- ISSUE: [#68408 — Agent description advertises unavailable SendMessage](https://github.com/anthropics/claude-code/issues/68408).

ELIOT therefore verifies the actual coordination capability profile at registration and exposes only tools that the client/runtime can use.

#### Peer-message UI/authority spam

- ISSUE: [#80454 — peer security envelope rendered as full chat bubbles](https://github.com/anthropics/claude-code/issues/80454).
- ISSUE: [#80625 — remote control shows reminder boxes instead of useful peer content](https://github.com/anthropics/claude-code/issues/80625).

ELIOT keeps authority metadata machine-readable and renders one compact sender/subject/disposition row to humans.

#### Human as relay

- ISSUE/REQUEST: [#28300 — multi-agent collaboration across machines](https://github.com/anthropics/claude-code/issues/28300).

The request accurately describes the product need: independently owned services need to negotiate schemas and integration points without making the human copy every decision between sessions.

## 6. Donor implementations

### 6.1 Overstory

- CODE/DOC: [Nokodoko/overstory](https://github.com/Nokodoko/overstory).

Useful:

- capability/state/parent discovery;
- SQLite typed mail;
- separate worktrees;
- current fleet status, trace/replay and cost metrics;
- explicit warning that merge conflicts, compounding error and cost amplification are normal risks.

Rejected:

- broadcast group addresses as the normal coordination path;
- peer `assign`/`dispatch` semantics;
- urgent mail auto-nudging a model;
- a second mail database inside ELIOT.

### 6.2 Gas Town

- CODE/DOC: [gastownhall/gastown](https://github.com/gastownhall/gastown).

Useful:

- durable identities/work state;
- one-shot predecessor-session query (`seance` pattern);
- tiered escalation where ordinary information queries should not escalate;
- explicit runtime-capacity governor.

Rejected:

- another Beads/work graph as ELIOT authority;
- Mayor as relay for every peer fact;
- stale timers that automatically generate new model work.

### 6.3 Agency Swarm

- CODE/DOC: [VRSEN/agency-swarm](https://github.com/VRSEN/agency-swarm).

Useful:

- typed directional communication flows;
- custom message fields carrying exact decisions/context;
- explicit handoff separated from ordinary send.

ELIOT derives flows dynamically from current Task/contract/scope relations instead of maintaining one global static graph.

### 6.4 CCCC, Multica, MCP Agent Mail, Claw, AutoGen

Detailed source paths/issues are already classified in:

- [Peer Autonomy Source Map](agent-communication-peer-autonomy-sources.md);
- [Field Evidence and Donor Map](agent-communication-field-evidence.md);
- [Communication and Concilium](agent-communication-concilium.md).

Fleet-relevant conclusions:

- CCCC demonstrates strong delivery identity/reply/cancel/cursor semantics but current per-actor bridge/runtime scaling and resume failures make it unsuitable as ELIOT's default fleet process model.
- Multica demonstrates why comments and mentions must not be both human communication and execution routing; one reported workflow processed about 18 million tokens through ping-pong.
- MCP Agent Mail demonstrates good mail/reservation UX but adds a separate authority/storage product and has restrictive licensing concerns.
- Claw demonstrates bounded advisory councils and protected verification separation.
- AutoGen reports reinforce termination, full-history, speaker-selection, backpressure and malformed-message failure cases.

## 7. Owner ELIOT fleet evidence

OWNER evidence from the current local swarm includes:

- several managers receiving the same stale queue and duplicating work;
- activity metrics overstating actual writers by aggregating unrelated child sessions;
- long-lived contexts and repeated full histories consuming extreme tokens;
- many partial submissions producing more review/merge cost than completed work;
- mass replay of old submissions flooding acceptance and causing negative progress;
- global reminders/status messages consuming context without changing code;
- one shared file/contract becoming a hotspot that needed one owner;
- agents blocked because they could not directly ask the current contract owner;
- independent components diverging because producer/consumer seams were not agreed before implementation.

ELIOT product requirements derived from this evidence:

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

## 8. What the evidence does not support

Do not claim:

- that 1,024 agents are economical at equal compute;
- that the Agensh result transfers to arbitrary repositories;
- that thousands of model processes fit one Windows host;
- that self-organization removes the need for assignment/acceptance authority;
- that peer consensus establishes correctness;
- that a bigger team improves a monolithic/shared-state task;
- that a successfully stored message was presented to a busy model;
- that user issue counts provide a failure rate;
- that the current ELIOT implementation already meets the fleet contour.

## 9. Final evidence-based balance

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
  scoped identity
  sparse relevance graph
  revisioned cards/cells
  explicit delivery disposition
  one mutable owner
  bounded presentation/backpressure
  no recursive spawn

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

This is the target balance: communication is a tool agents can use whenever it helps, not a mandatory ceremony and not a second uncontrolled workflow engine.