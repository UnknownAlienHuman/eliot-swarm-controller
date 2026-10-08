# ELIOT donor registry: verified mechanisms, discovery pool and search taxonomy

**Snapshot: 8 October 2026. ELIOT evidence baseline: `40591a295af94b1541ec2ba30afe8e3247701a71`.**

This registry answers a narrower question than a product comparison: **which external systems contain one mechanism worth adapting to a named ELIOT invariant?** It is not a dependency proposal and it does not treat a similar README as qualification.

## 1. Count and counting rules

### Current count

| State | Count | Meaning |
|---|---:|---|
| Verified donor entries | **26** | Source inspected, license recorded, transferable mechanism and rejection boundary identified |
| Previous verified set | 18 | Recorded in `33-donor-field-reviews.md` |
| Added by this search round | **8** | Kandev, OpenHands, Kubernetes controller stack, SWE-agent family, Fabro, Cayu, systemd, Bazel Remote Execution APIs |
| Discovery candidates | not counted | Names found by broad search; no adoption claim until source review |

### One donor entry means

A project or inseparable project family is counted once only when all of the following exist:

1. an exact repository and reviewed revision;
2. a real implementation or normative protocol, not only a landing page;
3. one narrow mechanism that closes a named ELIOT seam;
4. an explicit boundary describing what must **not** be copied;
5. a license review;
6. source, tests, issue evidence or field evidence that can become an ELIOT adoption gate.

Forks, mirrors, product directories, `awesome-*` lists and multiple repositories implementing one inseparable stack are not multiplied to inflate the count. Conversely, independent libraries in one ecosystem remain separate donors when they own different mechanisms: for example `portable-pty` and `alacritty_terminal`.

### Status vocabulary

| Status | Meaning |
|---|---|
| `TAKE` | Mechanism is sufficiently understood to write an ELIOT implementation task |
| `SPIKE` | Worth a bounded implementation experiment before adoption |
| `REFERENCE` | Use semantics, fixtures or failure cases; do not add a runtime dependency |
| `DO NOT IMPORT` | Useful mainly as an anti-pattern or evidence source |

A donor can be verified while its status remains `SPIKE` or `REFERENCE`.

## 2. How these systems are named

Searching only for “multi-agent framework” misses most relevant systems and returns many libraries that build agents rather than operate coding-agent work. The same product class appears under several names.

| Search family | Useful terms |
|---|---|
| Development environment | `agentic development environment`, `ADE`, `AI-native IDE`, `coding agent workbench` |
| Control surface | `coding agent control plane`, `agent cockpit`, `agent control tower`, `mission control`, `developer control center` |
| Orchestration | `coding agent orchestrator`, `agent task orchestrator`, `worktree orchestrator`, `multi-agent coding orchestration` |
| Software production | `AI software factory`, `agentic software factory`, `dark software factory`, `autonomous software engineering platform` |
| Runtime | `agent runtime`, `long-horizon agent runtime`, `long-running agent runtime`, `agent execution fabric` |
| Harness | `agent harness`, `coding agent harness`, `agent-computer interface`, `ACI`, `evaluation harness` |
| Durable state | `durable agents`, `durable sessions`, `resumable agents`, `persistent agent runtime` |
| Fleet | `agent fleet manager`, `agent workforce management`, `worker fleet`, `agent team platform` |
| Operating system | `agent OS`, `agent operating system`, `agent infrastructure as code`, `agent kernel` |
| Communication | `agent mesh`, `agent service mesh`, `coordination fabric`, `blackboard`, `council`, `swarm runtime` |
| Product planning | `agent task board`, `agentic project management`, `AI kanban`, `issue-to-PR agents` |
| Execution environment | `remote coding workspace`, `workspace manager`, `sandbox runtime`, `remote executor` |
| Workflow | `human-in-the-loop workflow engine`, `durable workflow`, `agentic workflow engine` |
| Controller pattern | `operator`, `controller`, `reconciler`, `workqueue`, `level-based reconciliation` |
| Effects and artifacts | `remote execution API`, `action cache`, `content-addressable storage`, `CAS`, `execution provenance` |
| Process lifecycle | `service manager`, `process supervisor`, `watchdog`, `process family`, `cgroup lifecycle` |
| Tool plane | `MCP gateway`, `tool router`, `tool registry`, `policy gateway`, `agent gateway` |
| Observability | `agent observability`, `trajectory store`, `run inspector`, `agent replay` |

### Search passes used

1. direct analogues: control plane, orchestrator, task board, worktree manager;
2. alternative product language: ADE, workbench, cockpit, mission control;
3. factory language: software factory, dark factory, autonomous engineering;
4. runtime language: long-horizon runtime, durable session, leased worker, execution fabric;
5. infrastructure language: agent OS, mesh, infrastructure as code, fleet manager;
6. adjacent proven systems: controller/operator, service supervisor, remote execution/CAS;
7. negative-evidence pass: repository issues for duplicate starts, orphaned worktrees, missing watchdogs, stale cleanup, silent partial success and accounting loss.

`kyrolabs/awesome-ade` and similar catalogues were useful discovery indexes. They are **not** donors themselves and do not qualify the projects they list.

## 3. Verified donor table

The first 18 entries retain their detailed evidence in `33-donor-field-reviews.md`. Rows 19–26 are new in this round.

| ID | Donor | License | Class | Status | Exact useful mechanism | Do not copy |
|---:|---|---|---|---|---|---|
| D01 | `agentclientprotocol/rust-sdk` | Apache-2.0 | agent protocol | `TAKE` | Typed ACP roles, request/notification distinction, capability negotiation, session wire types | SDK child ownership, unbounded framing assumptions, HTTP transport as ELIOT authority |
| D02 | `slawlor/ractor` | MIT | actor supervision | `REFERENCE` | Priority control channels and explicit child stop-and-wait semantics | Actor runtime and implicit parent/child authority |
| D03 | `mongodb/kingfisher` | Apache-2.0 | secret scanner | `SPIKE` | Compiled rule engine and corpus as a bounded evidence producer | Scanner verdict as authorization or network-send permission |
| D04 | `agentgateway/agentgateway` | Apache-2.0 | policy gateway | `TAKE` | Phase-typed request/response/tool policy and deny-on-policy-construction-failure | Whole gateway or expression failure that silently removes policy |
| D05 | `smart-mcp-proxy/mcpproxy-go` | MIT | tool catalogue | `SPIKE` | Retrieve-then-describe and approved schema snapshot/quarantine | Ranking as authorization; automatic schema acceptance |
| D06 | `langfuse/langfuse` | MIT core | observability | `REFERENCE` | Optional trace/cost sink | Required correctness store or unbounded synchronous exporter |
| D07 | `open-telemetry/opentelemetry-rust` | Apache-2.0 | telemetry | `TAKE` | Bounded asynchronous export contracts and standard trace context | Telemetry as durable controller fact |
| D08 | `dbos-inc/dbos-transact` | MIT | durable execution | `REFERENCE` | Stable step identity before effect and durable wait patterns | Second workflow/database authority |
| D09 | `restatedev/restate` / Rust SDK | MIT | durable execution | `REFERENCE` | Explicit terminal-vs-retry classification, durable correlations and awakeable identity | Restate server, infinite retry default, closure replay |
| D10 | `wezterm/portable-pty` | MIT | terminal transport | `SPIKE` | Cross-platform PTY/ConPTY transport | Process ownership or family-departure proof |
| D11 | `alacritty/alacritty_terminal` | Apache-2.0 | VT parser | `SPIKE` | Headless terminal-state parser | Full terminal application or authority model |
| D12 | `landlock-lsm/rust-landlock` | MIT/Apache-2.0 | Linux confinement | `SPIKE` | Optional filesystem confinement with explicit compatibility/degraded state | Claim of cross-platform sandboxing |
| D13 | `rust-vmm/seccompiler` | Apache-2.0 | syscall confinement | `SPIKE` | Optional seccomp policy compiler | Universal policy or hidden degradation |
| D14 | `snyk/agent-scan` | Apache-2.0 | agent security testing | `REFERENCE` | Threat taxonomy and adversarial fixtures for tool admission | Scanner as authorization authority |
| D15 | `ChesterRa/cccc` | repository license reviewed in R33 | coordination | `TAKE` | Newest-first cursor-local inbox summary and explicit unread/read/replied facts | Filesystem ledger and two-file cursor protocol over SQLite |
| D16 | `getpaseo/paseo` | repository license reviewed in R33 | subscriptions | `TAKE` | Source-owned registration identity, detach cleanup and demand-driven observation | HTTP connection owning Task cancellation |
| D17 | `aaif-goose/goose` | repository license reviewed in R33 | scheduler/input | `REFERENCE` | Validated bytes plus base-directory DTO; scheduler failures as anti-pattern fixtures | Goose scheduler/model loop |
| D18 | `Enderfga/claw-orchestrator` | repository license reviewed in R33 | council/review | `TAKE` | Separate recommendation, dissent and downstream verification | Majority vote as acceptance authority |
| D19 | `kdlbs/kandev` @ `6c21e0ce22406c39bd74ccb7123083ca273a6ec4` | AGPL-3.0 | coding-agent control plane | `REFERENCE` | Exact Task/session/worktree ownership, per-repository worktrees, resumable ACP sessions, cleanup separated from logical retirement | Source copying into ELIOT; multiple start owners; losing path deleting another path's task/worktree |
| D20 | `OpenHands/OpenHands` @ `5fea36ab7feaa33420dd8e848a4044e24ba2e939` plus Agent Server stack | MIT | ADE/control plane | `TAKE` | Separate control surface, canonical agent-server runtime/API and automation dispatcher; per-conversation runtime with persisted workspace/history | Full Canvas stack; direct host mode as sandbox; frontend session lifetime owning accepted work |
| D21 | `kubernetes-sigs/controller-runtime` @ `117d60f3fa28bc265cc2db33bdd4c896935bdcc2` plus `client-go/workqueue` | Apache-2.0 | controller runtime | `TAKE` | Level-based reconcile from authoritative state, deduplicated keys, explicit terminal error, error backoff, scheduled requeue, `Forget` after progress | Kubernetes API/cache stack; event payload as authority; generic retry of uncertain effects |
| D22 | `SWE-agent/mini-swe-agent` @ `04d809ceab9df28f9adaed044884180159172930` and SWE-agent family | MIT | agent harness/evaluation | `REFERENCE` | Minimal linear trajectory, independent action processes, inspectable trajectory and a baseline for measuring scaffold value | Shell-only authority model; benchmark result as product qualification; task text trusted as instructions |
| D23 | `fabro-sh/fabro` @ `36f8b61b60ba9157973c37e7d359cfbe5bd0e63e` | MIT | dark software factory | `REFERENCE` | Versioned deterministic graph, human gates, durable events/checkpoints, stage Git provenance and explicit retry/fresh-run distinction | Whole graph engine/DSL; text-substring failure classifier; assuming admitted watchdog/policy is wired |
| D24 | `cayu-dev/cayu` @ `1891a6ad43251e9ad853a1753dda44a871c663b6` | Apache-2.0 | long-horizon agent runtime | `SPIKE` | Durable sessions/events/resume/fork, typed effects, approvals, idempotency, explicit environments, completion verification and recovery contracts | Python runtime/control plane as second ELIOT engine; native tools as sandbox; emerging claims without independent field qualification |
| D25 | `systemd/systemd` @ `bde980af1e4ce17ced36ecea426c1d32f85d4fee` | LGPL-2.1+ | service supervision | `REFERENCE` | Readiness notification, watchdog, bounded stop, control-group lifecycle and refusal to restart while descendants remain | Linux service-manager dependency; PID-only ownership; platform-specific policy as universal contract |
| D26 | `bazelbuild/remote-apis` @ `6def1c5d27a527c400875c24ae8b1a160145d7e1` | Apache-2.0 | remote execution/CAS | `TAKE` | Action/Command/InputRoot digest identity, CAS, exact ActionCache reuse and ordered live log streams | Remote execution service, generic worker protocol or cache hit without ELIOT authority/candidate checks |

## 4. New donor evidence and ELIOT use

### D19. Kandev: resource ownership, not its UI

Kandev is a direct analogue: a coding-agent control plane with parallel tasks, per-repository worktrees, agent sessions, subtasks, local/Docker/SSH/Kubernetes/cloud executors and ACP integrations.

Useful mechanisms:

- Task, task-session and worktree are separate identities;
- session state retains native agent/session IDs for resume;
- worktrees are intended to survive backend restart;
- logical retirement, physical cleanup and force-removal are separate operations;
- multi-repository tasks preserve one worktree/branch/PR identity per repository.

High-value negative evidence:

- issue #4323 reports two independent start paths for one automation; the losing path deleted the Task while the winner was running;
- issue #3962 describes a dead standalone helper that left an orphaned worktree and silently trapped commits;
- issue #3957 proposes force-removal only after exact quiescence, with logical quarantine before destructive cleanup.

ELIOT use:

- R01/R35: one start owner, exact process/worktree owner and cleanup readback;
- workspace leases: never let a loser delete another path's resource;
- partial cleanup: quarantine logical identity first, preserve physical evidence until ownership is proven.

License boundary: AGPL means this is a semantics-and-fixtures donor unless an explicit compatible reuse decision is made. Do not copy implementation code into ELIOT by default.

### D20. OpenHands: separate control center, runtime and automation

The current OpenHands system explicitly separates:

```text
Agent Canvas       — user-facing control center and backend selection
Agent Server/SDK   — conversations, workspaces, tools, events and canonical API
Automation Server  — schedules/webhooks, run history and dispatch
```

Its per-conversation Docker runtime persists workspace files and conversation history across container replacement. The same control surface can switch between local, remote and cloud backends.

ELIOT use:

- keep frontend/control surface ownership separate from runtime identity;
- define one canonical runtime API rather than allowing each UI/backend to reinterpret state;
- persistence survives executor replacement, while executor lifetime does not own the conversation;
- a local/direct backend must state plainly that it gives full host filesystem access.

Do not copy the complete product stack or treat a container merely launched by the control center as a proven hostile-code sandbox.

### D21. Kubernetes controller-runtime/client-go: the missing reconcile discipline

The reviewed controller contract is level-based: a key identifies the subject; the reconciler reads current authoritative state and converges it. It does not treat the triggering event payload as the current truth.

The stack separates:

- nonterminal error → rate-limited retry;
- terminal error → no retry;
- `RequeueAfter` → known future prerequisite/poll time;
- deduplicated work key;
- successful synchronization → forget prior retry history;
- startup/full resync → recover changes missed while the controller was down.

ELIOT use:

- R34 dispositions: `Pending` must not be inferred from arbitrary error text;
- R12 pacing: no-progress/error backoff differs from a known due time;
- successful cursor/state progress clears earned backoff;
- retained source identity wakes a level-based readback, not blind replay of the prior effect.

Do not import Kubernetes, informer caches or eventual-consistency assumptions into the local SQLite Store.

### D22. SWE-agent family: use minimality as a control experiment

SWE-agent itself now recommends mini-SWE-agent for most new work. The smaller implementation keeps a linear trajectory and executes each action independently rather than retaining a stateful shell session.

ELIOT use:

- maintain a minimal reference harness to measure whether controller machinery improves completion, recovery or evidence;
- preserve a complete inspectable trajectory for adapter fixtures and evaluation;
- prefer independent finite actions when a stateful terminal adds no required capability;
- use SWE-bench-style evaluation as one behavioral signal, not as acceptance of ELIOT lifecycle guarantees.

Security boundary:

- mini-SWE-agent deliberately exposes bash and treats the task as model input;
- issue #953 notes that arbitrary issue text can become prompt injection against shell, GitHub tokens and API keys;
- therefore its simplicity is a baseline, not an authorization/sandbox model.

### D23. Fabro: deterministic graph plus unusually useful failure reports

Useful mechanisms:

- version-controlled deterministic workflow graph;
- explicit human gates;
- durable event/checkpoint stream;
- stage Git commits and run provenance;
- clear difference between resume and a fresh retry.

High-value negative evidence:

- #938: `stall_timeout` was accepted but the watchdog was not installed, so a declared guarantee did nothing;
- #933: live subagent cost was lost when the completed projection replaced totals with parent-only usage;
- #936: read-only inspection restarted a stopped sandbox and failed to restore its prior state;
- #876: substring matching `500/502/503/504`, `timeout` or `budget` against compiler output misclassified deterministic failures as transient infrastructure;
- #818: parallel branches were silently killed before any model call while the fan-out still reported partial success;
- #915: workspace cleanup required deletion of the complete run and its evidence.

ELIOT use:

- admitted policy must have a connected production owner and public-path fixture;
- failure classes come from typed source fields, never arbitrary output text;
- live and terminal accounting use one event contract and must preserve descendants;
- temporary read access needs acquisition ownership and compensating cleanup;
- cleanup of heavy execution resources must be independent from retention of run evidence.

Do not import the complete graph engine or add another workflow store.

### D24. Cayu: narrow runtime primitives instead of one mandatory engine

Cayu explicitly distinguishes Agent, Environment, Session and ToolContext. It provides sessions, task dispatch, leases, resumable steps, approvals, typed effects, idempotency, recovery, budgets and evaluations while allowing the application to own its UI and business workflow.

ELIOT use as a bounded spike:

- compare its typed effect and ambiguous-outcome recovery contracts against ELIOT Operation/readback;
- compare explicit environment selection against ELIOT route/binding/workspace identity;
- reuse completion-verification and trajectory-evaluation fixture ideas;
- inspect process-isolated finite tools for R35 and exact artifact settlement.

Boundary: Cayu is young and much of the production evidence is project-authored. No Python runtime, server, dashboard or cloud component is adopted merely because the concepts align.

### D25. systemd: process-family completion semantics

Useful semantics:

- readiness is an explicit notification, not “process exists”;
- service watchdog and stop timeout are separate clocks;
- `KillMode=control-group` treats descendants as the owned unit;
- a service configured without forced cleanup is not restarted while processes remain.

ELIOT use:

- R01: `Running` requires exact readiness and owner identity;
- R35: direct child exit, family departure and capture completion remain separate;
- replacement is forbidden until exact prior family departure or explicit kill escalation is proven.

This is a semantics/test donor. ELIOT must preserve Windows Job ownership and other platforms rather than depend on systemd.

### D26. Bazel Remote Execution APIs: immutable execution identity

The protocol family defines content-addressed inputs, exact action identity, cached result lookup, distributed execution and ordered live stdout/stderr streams.

ELIOT use:

- CheckRun identity should be a digest of command, environment policy, input root and platform properties;
- artifact and candidate bytes live under content identity, not a mutable friendly name;
- cached passing result is reusable only for an exact action/input identity and compatible policy;
- live log transport and terminal ActionResult are separate facts;
- human-readable source/repository references may resolve to a digest, but never replace it as execution evidence.

Do not build a distributed execution cluster or adopt the generic worker protocol for the current local Store.

## 5. Discovery pool: found, not yet counted

These names are recorded so later searches do not restart from zero. Inclusion below is **not** endorsement.

### Direct ADE/control-plane candidates

- CodeLayer / the current HumanLayer rebuild;
- Vibe Kanban;
- Emdash;
- Aperant;
- Superset;
- Crystal;
- mux;
- Termic;
- Tempest;
- Pane;
- OpenADE;
- Overdeck;
- dmux;
- amux;
- herdr;
- Claude Squad;
- ccmanager;
- agent-deck;
- ccswarm;
- AWS CLI Agent Orchestrator;
- Orca;
- gastown;
- Open SWE;
- ruflo;
- LoopTroop.

### Software-factory candidates

- AgentFactory;
- Fusion;
- Agentic Software Factory;
- Machinist;
- Jiffy;
- Wallfacer;
- FORGE;
- Noriq;
- Optio;
- Vigla;
- Mission Control variants.

### Runtime/durable-session candidates

- `aramase/agentsessions`;
- `shenjianan97/persistent-agent-runtime`;
- Clear Ideas Agent Runtime;
- Persistent Execution Fabric;
- AgentRouter;
- OpenFang;
- OpenOS;
- SmythOS SRE;
- SwarmClaw;
- Flux;
- Conductor;
- Temporal agent/workflow patterns.

### Mesh/gateway/infrastructure candidates

- Orloj;
- agent-mesh implementations;
- OpenForge AI infrastructure-as-code;
- GAIA “Kubernetes for agents” implementations;
- fleet CLI / agent fleet managers;
- MCP/A2A control-plane routers.

### Fabro split-stack follow-up

Fabro now delegates major responsibilities to separate projects. Review them independently rather than attributing every mechanism to the product shell:

- `lithoscomputer/petri` — workflow execution, watchdog and retained policy;
- `lithoscomputer/pebble` — agent/subagent runtime and usage;
- `lithoscomputer/sandbox-driver` — sandbox lifecycle protocol.

## 6. Not promoted and why

| Candidate | Decision |
|---|---|
| `humanlayer/humanlayer` public repository | Not promoted: its README says the code is deprecated and points to a rebuild outside that source tree |
| Fabro mirrors/forks returned by repository search | Not separate donors: same lineage or unverified forks |
| `awesome-ade`, `awesome-cli-agents` and similar lists | Discovery indexes only; no runtime guarantee |
| “Kubernetes for agents” repositories with only a README/demo | Candidate until source, storage and failure paths are reviewed |
| Agent frameworks that only implement a prompt/tool loop | Not control-plane donors unless they add a distinct verified mechanism |
| Marketing-only mission-control dashboards | Not donors without authoritative runtime/ownership semantics |
| Whole Temporal/Restate/DBOS servers | Do not import: ELIOT already owns its Store; use narrow mechanisms only |

## 7. Mapping donors to current ELIOT work

| ELIOT block | Primary donors | Required deletion/simplification |
|---|---|---|
| R01 module lifecycle | systemd, Ractor, ACP, Kandev | Remove manual identity reconstruction and stop paths that consume the only child owner before departure proof |
| R08 delivery/subscriptions | CCCC, Paseo, OpenHands | Remove timestamp/UUID ordering and bootstrap-from-zero live subscription path |
| R12 scheduler fairness | Kubernetes controller stack, Restate, Goose | Remove no-delay retry loops and process-global pacing state |
| R18 Command ACP | ACP, Kandev, OpenHands | Keep one session/process owner; do not add a parallel bespoke protocol |
| R34 poison isolation | Kubernetes controller stack, Restate, Fabro | Remove cross-domain transaction coupling and string-prefix retry classification |
| R35 finite processes | systemd, Bazel log/result separation, Ractor | Remove unbounded waits, fabricated zero capture and the legacy duplicate ScriptRun executor |
| CheckRunner/artifacts | Bazel Remote Execution APIs, SWE-agent evaluation harness | Replace self-referential result identity and mutable-name cache assumptions with exact digest-bound evidence |
| Agent/runtime simplification | mini-SWE-agent, Cayu, OpenHands | Require measured benefit for each extra controller state machine and keep runtime/control-surface boundaries explicit |
| Tool admission | Kingfisher, AgentGateway, MCPProxy, Agent Scan | Remove ambiguous fail-open policy paths and dynamic schema acceptance |
| Observability | OpenTelemetry, Langfuse, Fabro issue fixtures | Keep telemetry optional/bounded; never use it as the only durable fact |

## 8. Adoption gate for every future donor

A donor-driven implementation is accepted only when its PR states:

```text
source repository + exact revision + license
one mechanism being adapted
one ELIOT invariant it establishes
one public production caller in the same PR
one fixture derived from source or field failure evidence
one current ELIOT responsibility/code path removed
what is explicitly not imported
scoped formatting and warnings-denied Clippy result
```

No new dependency follows from this registry. A design similarity is not evidence that the mechanism is wired, that its owner is correct or that its failure behavior matches ELIOT.

## 9. Next donor-review queue

Highest-value next source reviews:

1. `lithoscomputer/petri`, `pebble`, `sandbox-driver` as the actual Fabro execution stack;
2. `aramase/agentsessions` for a neutral durable-session contract;
3. Kandev force-removal/start ownership implementation, not only its issue reports;
4. OpenHands `software-agent-sdk` canonical conversation/event/workspace API;
5. one mature worktree-oriented ADE from the discovery pool, selected by real issue density rather than stars;
6. one AgentRouter/policy-control-plane implementation;
7. one remote-execution server implementation only if ELIOT moves beyond local CheckRun execution.
