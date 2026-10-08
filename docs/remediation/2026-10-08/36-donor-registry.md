# ELIOT donor registry: verified mechanisms, discovery pool and search taxonomy

**Snapshot: 8 October 2026. ELIOT evidence baseline: `40591a295af94b1541ec2ba30afe8e3247701a71`.**

This registry answers a narrower question than a product comparison: **which external systems contain one mechanism worth adapting to a named ELIOT invariant?** It is not a dependency proposal and it does not treat a similar README as qualification.

## 1. Count and counting rules

### Current count

| State | Count | Meaning |
|---|---:|---|
| Verified donor entries | **30** | Source inspected, license recorded, transferable mechanism and rejection boundary identified |
| Previous verified set | 18 | Recorded in `33-donor-field-reviews.md` before this search campaign |
| Added by this search campaign | **12** | Kandev, OpenHands, Kubernetes controller stack, SWE-agent family, Fabro, Cayu, systemd, Bazel REAPI, Petri, Pebble, sandbox-driver and agentsessions |
| Discovery candidates | not counted | Names found by broad search; no adoption claim until source review |

### One donor entry means

A project or inseparable project family is counted once only when all of the following exist:

1. an exact repository and reviewed revision;
2. a real implementation or normative protocol, not only a landing page;
3. one narrow mechanism that closes a named ELIOT seam;
4. an explicit boundary describing what must **not** be copied;
5. a license review;
6. source, tests, issue evidence or field evidence that can become an ELIOT adoption gate.

Forks, mirrors, product directories, `awesome-*` lists and several repositories implementing one inseparable product shell are not multiplied to inflate the count. Independent projects in one ecosystem remain separate donors when they own different mechanisms and publish independent contracts. Petri, Pebble and sandbox-driver therefore count separately; Fabro merely composes them.

### Status vocabulary

| Status | Meaning |
|---|---|
| `TAKE` | Mechanism is sufficiently understood to write an ELIOT implementation task |
| `SPIKE` | Worth a bounded implementation experiment before adoption |
| `REFERENCE` | Use semantics, fixtures or failure cases; do not add a runtime dependency |
| `DO NOT IMPORT` | Useful mainly as an anti-pattern or evidence source |

A donor can be verified while its status remains `SPIKE` or `REFERENCE`.

## 2. How these systems are named

Searching only for `multi-agent framework` misses most relevant systems and returns many libraries that build agents rather than operate coding-agent work. The same product class appears under several names.

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
| Session contract | `durable agent sessions`, `bring your own harness`, `session narrow waist`, `replay conformance`, `session fork` |

### Search passes used

1. direct analogues: control plane, orchestrator, task board, worktree manager;
2. alternative product language: ADE, workbench, cockpit, mission control;
3. factory language: software factory, dark factory, autonomous engineering;
4. runtime language: long-horizon runtime, durable session, leased worker, execution fabric;
5. infrastructure language: agent OS, mesh, infrastructure as code, fleet manager;
6. adjacent proven systems: controller/operator, service supervisor, remote execution/CAS;
7. negative-evidence pass: repository issues for duplicate starts, orphaned worktrees, missing watchdogs, stale cleanup, silent partial success and accounting loss;
8. responsibility-split pass: inspect the actual execution, agent-loop and sandbox projects underneath a product shell;
9. neutral-contract pass: search for vendor-independent session/harness/runtime interfaces and replay conformance.

`kyrolabs/awesome-ade` and similar catalogues were useful discovery indexes. They are **not** donors themselves and do not qualify the projects they list.

## 3. Verified donor table

The first 18 entries retain additional field evidence in `33-donor-field-reviews.md`. Rows 19–30 were verified during this search campaign.

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
| D15 | `ChesterRa/cccc` | license reviewed in R33 | coordination | `TAKE` | Newest-first cursor-local inbox summary and explicit unread/read/replied facts | Filesystem ledger and two-file cursor protocol over SQLite |
| D16 | `getpaseo/paseo` | license reviewed in R33 | subscriptions | `TAKE` | Source-owned registration identity, detach cleanup and demand-driven observation | HTTP connection owning Task cancellation |
| D17 | `aaif-goose/goose` | license reviewed in R33 | scheduler/input | `REFERENCE` | Validated bytes plus base-directory DTO; scheduler failures as anti-pattern fixtures | Goose scheduler/model loop |
| D18 | `Enderfga/claw-orchestrator` | license reviewed in R33 | council/review | `TAKE` | Separate recommendation, dissent and downstream verification | Majority vote as acceptance authority |
| D19 | `kdlbs/kandev` @ `6c21e0ce22406c39bd74ccb7123083ca273a6ec4` | AGPL-3.0 | coding-agent control plane | `REFERENCE` | Exact Task/session/worktree ownership, per-repository worktrees, resumable ACP sessions, cleanup separated from logical retirement | Source copying into ELIOT; multiple start owners; losing path deleting another path's task/worktree |
| D20 | `OpenHands/OpenHands` @ `5fea36ab7feaa33420dd8e848a4044e24ba2e939` plus Agent Server stack | MIT | ADE/control plane | `TAKE` | Separate control surface, canonical agent-server runtime/API and automation dispatcher; per-conversation runtime with persisted workspace/history | Full Canvas stack; direct host mode as sandbox; frontend session lifetime owning accepted work |
| D21 | `kubernetes-sigs/controller-runtime` @ `117d60f3fa28bc265cc2db33bdd4c896935bdcc2` plus `client-go/workqueue` | Apache-2.0 | controller runtime | `TAKE` | Level-based reconcile from authoritative state, deduplicated keys, terminal error, error backoff, scheduled requeue, `Forget` after progress | Kubernetes API/cache stack; event payload as authority; generic retry of uncertain effects |
| D22 | `SWE-agent/mini-swe-agent` @ `04d809ceab9df28f9adaed044884180159172930` and SWE-agent family | MIT | agent harness/evaluation | `REFERENCE` | Minimal linear trajectory, independent action processes, inspectable trajectory and a baseline for measuring scaffold value | Shell-only authority model; benchmark result as product qualification; task text trusted as instructions |
| D23 | `fabro-sh/fabro` @ `36f8b61b60ba9157973c37e7d359cfbe5bd0e63e` | MIT | dark software factory | `REFERENCE` | Versioned deterministic graph, human gates, durable events/checkpoints, stage Git provenance and explicit retry/fresh-run distinction | Treating the product shell as owner of execution, agent-loop or sandbox semantics; text-substring failure classification |
| D24 | `cayu-dev/cayu` @ `1891a6ad43251e9ad853a1753dda44a871c663b6` | Apache-2.0 | long-horizon agent runtime | `SPIKE` | Durable sessions/events/resume/fork, typed effects, approvals, idempotency, explicit environments, completion verification and recovery contracts | Python runtime/control plane as second ELIOT engine; native tools as sandbox; emerging claims without independent field qualification |
| D25 | `systemd/systemd` @ `bde980af1e4ce17ced36ecea426c1d32f85d4fee` | LGPL-2.1+ | service supervision | `REFERENCE` | Readiness notification, watchdog, bounded stop, control-group lifecycle and refusal to restart while descendants remain | Linux service-manager dependency; PID-only ownership; platform-specific policy as universal contract |
| D26 | `bazelbuild/remote-apis` @ `6def1c5d27a527c400875c24ae8b1a160145d7e1` | Apache-2.0 | remote execution/CAS | `TAKE` | Action/Command/InputRoot digest identity, CAS, exact ActionCache reuse and ordered live log streams | Remote execution service, generic worker protocol or cache hit without ELIOT authority/candidate checks |
| D27 | `lithoscomputer/petri` @ `2548c104e1b0b5ac710abc59b25724d6b2c4d66d` | MIT | replayable workflow/execution core | `REFERENCE` | Sans-I/O `apply(state,event)->commands`, total-order event log, deterministic replay canary, writer fencing, explicit scopes/leases and two-tier cancel/kill | Whole workflow DSL/runtime, eventually-consistent provider list as absence proof, test-future drop as a real crash model |
| D28 | `lithoscomputer/pebble` @ `e90513f4204ff6afed9204de88c4a5a92b897579` | MIT | provider-neutral agent loop | `TAKE` | Durable coding events, continuation without replaying tools, route failover preserving queued input, committed snapshot+event cursor, explicit steering bus | Storage/process isolation, global shared “always allow” permission escalation, parent-only accounting presented as full tree cost |
| D29 | `lithoscomputer/sandbox-driver` @ `90b0d825767105d4b42e59fdbd116c4d66b87d83` | MIT | sandbox provider protocol | `SPIKE` | One provider trait for in-process and JSON-RPC plugins, capability/conformance tests, output-loss facts, exact workspace ownership, recovery fences and checksum-pinned executables | Host provider as isolation, eventual list result as definitive absence, provider-specific identity leaking into ELIOT contracts |
| D30 | `aramase/agentsessions` @ `e680ea10d3b420aee0453cf6b54edeaf0c73f5c8` | Apache-2.0 | durable-session narrow waist | `REFERENCE` | Neutral Session/Harness/Runtime contracts, single-writer CAS/fence log, hash-chain provenance, zero-model-call replay conformance, stateless-vs-memory-snapshot capability matching | Pre-1.0 wire as stable ELIOT API, missing auth/authz/TLS, a managed control plane or another authoritative session store |

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

ELIOT use: one start owner, exact process/worktree owner, cleanup readback and logical quarantine before destructive cleanup. Kandev is AGPL, so semantics and fixtures are the default reuse boundary.

### D20. OpenHands: separate control center, runtime and automation

The current OpenHands system separates Agent Canvas, Agent Server/SDK and Automation Server. Its per-conversation runtime persists workspace files and history across executor replacement.

ELIOT use:

- frontend/control-surface ownership remains separate from runtime identity;
- one canonical runtime API prevents each UI/backend from inventing state semantics;
- executor lifetime does not own the conversation;
- a local/direct backend must disclose full host filesystem access.

Do not copy the complete product stack or treat a container merely launched by a control center as a proven hostile-code sandbox.

### D21. Kubernetes controller-runtime/client-go: reconcile discipline

The controller contract is level-based: a key identifies the subject; reconciliation reads current authoritative state rather than trusting the triggering payload.

The stack separates:

- nonterminal error → rate-limited retry;
- terminal error → no retry;
- `RequeueAfter` → a known future prerequisite/poll time;
- deduplicated work key;
- successful synchronization → forget prior retry history;
- startup/full resync → recover missed changes.

ELIOT use: R34 disposition typing, R12 pacing and clearing backoff only after proven progress. Do not import Kubernetes caches or blindly retry uncertain external effects.

### D22. SWE-agent family: minimality as a control experiment

SWE-agent now recommends mini-SWE-agent for most new work. The smaller implementation retains a linear trajectory and starts each action independently instead of preserving a mutable shell process.

ELIOT use:

- measure whether each extra controller state machine improves completion, recovery or evidence over a minimal baseline;
- preserve an inspectable trajectory for fixtures/evaluation;
- prefer finite independent actions when stateful terminal continuity is unnecessary.

Its shell access and task-text trust are not an authorization model. Issue #953 documents prompt-injection exposure against tokens and shell authority.

### D23. Fabro: product workflow and negative evidence

Fabro contributes product-level semantics: deterministic version-controlled workflows, human gates, stage provenance and explicit retry/fresh-run distinction. Its issue history supplies adoption gates:

- #938: configured stall watchdog accepted but not installed;
- #933: descendant usage disappeared from the completed projection;
- #936: read-only inspection acquired a sandbox and failed to restore state;
- #876: output substring matching misclassified deterministic failure as transient infrastructure;
- #818: parallel branches were killed before model calls while the fan-out reported partial success;
- #915: execution-resource cleanup required deletion of retained run evidence.

Execution semantics now belong to Petri, the agent loop to Pebble and sandbox lifecycle to sandbox-driver. Do not assign those mechanisms to Fabro or import another workflow store.

### D24. Cayu: narrow runtime primitives

Cayu distinguishes Agent, Environment, Session and ToolContext and exposes sessions, leased workers, resumable steps, approvals, typed effects, recovery, budgets and evaluations.

Use it as a bounded comparison for ELIOT Operation/readback, environment identity and completion verification. It is young and mostly supported by project-authored evidence; no Python runtime, server, dashboard or cloud component is adopted automatically.

### D25. systemd: process-family completion semantics

Useful semantics:

- readiness is an explicit notification, not “process exists”;
- watchdog and stop timeout are separate clocks;
- control-group mode owns descendants as one unit;
- replacement is refused while descendants remain when cleanup has not been forced.

ELIOT use: R01 exact readiness/owner identity and R35 separation of direct-child exit, family departure and capture completion. Windows Job ownership remains mandatory.

### D26. Bazel Remote Execution APIs: immutable execution identity

REAPI defines content-addressed inputs, Action/Command/InputRoot identity, exact cached result lookup and separate ordered log streams.

ELIOT use:

- CheckRun identity binds command, environment policy, input root and platform properties;
- artifact and candidate bytes use content identity, not mutable names;
- cache reuse requires exact action/input/policy identity;
- live log transport and terminal result are separate facts.

Do not build a remote execution cluster for the current local Store.

### D27. Petri: a pure engine and an executable replay contract

Petri separates a sans-I/O engine from driver, executor, persistence and frontends:

```text
apply(state, event) -> (state, commands)
```

The implementation has one total-order driver channel, versioned event logs, deterministic replay checks, resource scopes and leases, explicit cancel/kill ladders, simulated clocks and random crash/resume property tests.

Useful ELIOT mechanisms:

- reducers and decisions can be tested without SQLite/process effects;
- replay verifies the persisted decision stream rather than rerunning model effects;
- outstanding commands after a log cut are explicit;
- resource acquire/release and writer fencing are part of the execution contract;
- cancellation, hard kill and cleanup grace are separate states.

Negative evidence:

- issue #87 shows why dropping a future is not equivalent to a real process crash: leftover tasks can write after “recovery”; superseded writers require fencing;
- issue #39 shows that an eventually consistent provider list is not proof of absence and can cause duplicate or leaked sandboxes.

Do not import the workflow DSL/runtime. Take reducer/replay/fencing patterns and exact tests.

### D28. Pebble: separate the agent loop from its host

Pebble states its boundary explicitly: it owns model turns, tool calls, history, compaction, steering and lifecycle events, but **not** transport, storage, credentials or process isolation.

High-value mechanisms:

- `continue_prompt` resumes unfinished history without new input and does not repeat a completed tool effect;
- route failover exports the record, requeues steering/follow-ups and continues on a new provider/model;
- `observe` returns a snapshot and receiver at one committed event cursor; consumers apply the snapshot then only later sequence numbers;
- `SteeringBus` buffers bounded input before attachment and reports what was delivered/buffered/dropped;
- exported records advance past the close event before reuse;
- lifecycle events distinguish compaction completed, failed and cancelled.

ELIOT use: adapter SDK/session state, OpenCode/Codex/Command continuation and exact subscription cutoffs. Do not copy its terminal `always` approval semantics blindly: the current CLI raises a permission level for every tool allowed by that level and can share it across sessions. Prompt-level reports also exclude some descendant/tool-owned model calls, so accounting must remain explicit.

### D29. sandbox-driver: provider contract and conformance

The same `SandboxProvider`/`Sandbox` traits are exposed in-process and through a versioned JSON-RPC plugin protocol. Host, Docker and Daytona implementations must pass a black-box conformance suite.

Useful mechanisms:

- construction path is separated from the sandbox contract;
- capabilities and unsupported operations are explicit;
- output retention, output loss, sanitization and streaming are distinct facts;
- plugin executables are checksum-pinned outside development mode;
- host recovery uses a caller-owned registry and never signals saved PIDs;
- named workspace ownership remains external unless `Managed` is explicitly selected;
- managed workspaces are deleted only after work stops;
- the outer sandbox owns nested containers and sidecars.

ELIOT use: one sandbox/executor interface for CheckRun, ScriptRun and remote workspaces, plus conformance fixtures. The Host provider explicitly provides no isolation. Eventual provider lists must remain sweep hints, never authoritative absence proof.

### D30. agentsessions: durable-session narrow waist

`agentsessions` publishes three separate contracts:

```text
Session  — create/list/exec/suspend/resume/fork/replay
Harness  — out-of-process typed event stream
Runtime  — compute/snapshot/restore/stop SPI
```

Its single-writer log uses CAS, fencing and a hash chain. Replay conformance demonstrates a replacement process reconstructing the session byte-identically with zero model calls. Capability matching distinguishes stateless replay from runtimes requiring memory snapshots; unsupported placement degrades explicitly. Sensitive tools may be controller-mediated with crash-mid-tool at-most-once re-drive.

ELIOT use:

- keep durable session identity independent of producer UI and compute backend;
- define replay/fork/suspend/resume as separate lifecycle operations;
- require capability matching before recovery;
- fence stale writers across replacement;
- test replay without contacting the model/provider;
- keep harness and runtime adapters outside the neutral contract.

Boundaries: the project is pre-1.0, explicitly has no backward-compatibility promise, and does not yet provide authentication, authorization or transport security. It is a contract/reference donor, not a replacement control plane or Store.

## 5. Discovery pool: found, not yet counted

These names are retained so later searches do not restart from zero. Inclusion below is **not** endorsement.

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

### Runtime and infrastructure candidates

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
- Temporal agent/workflow patterns;
- Orloj;
- agent-mesh implementations;
- OpenForge AI infrastructure-as-code;
- GAIA “Kubernetes for agents” implementations;
- fleet CLIs and agent fleet managers;
- MCP/A2A control-plane routers.

`aramase/agentsessions`, Petri, Pebble and sandbox-driver moved out of this pool after source, license and boundary review.

## 6. Not promoted and why

| Candidate | Decision |
|---|---|
| `humanlayer/humanlayer` public repository | Not promoted: its README says the published code is deprecated and points to a rebuild outside that source tree |
| Fabro mirrors/forks returned by repository search | Not separate donors: same lineage or unverified forks |
| `awesome-ade`, `awesome-cli-agents` and similar lists | Discovery indexes only; no runtime guarantee |
| “Kubernetes for agents” repositories with only a README/demo | Candidate until source, storage and failure paths are reviewed |
| Agent frameworks that only implement a prompt/tool loop | Not control-plane donors unless they add a distinct verified mechanism |
| Marketing-only mission-control dashboards | Not donors without authoritative runtime/ownership semantics |
| Whole Temporal/Restate/DBOS servers | Do not import: ELIOT already owns its Store; use narrow mechanisms only |

## 7. Mapping donors to current ELIOT work

| ELIOT block | Primary donors | Required deletion/simplification |
|---|---|---|
| R01 module lifecycle | systemd, Ractor, ACP, Kandev, Petri fencing | Remove manual identity reconstruction and stop paths that consume the only child owner before departure proof |
| R08 delivery/subscriptions | CCCC, Paseo, OpenHands, Pebble snapshot/cursor | Remove timestamp/UUID ordering and bootstrap-from-zero live subscription path |
| R12 scheduler fairness | Kubernetes controller stack, Restate, Goose, Petri watchdog | Remove no-delay retry loops and process-global pacing state |
| R18 Command ACP | ACP, Kandev, OpenHands, Pebble | Keep one session/process owner; do not add a parallel bespoke protocol |
| R34 poison isolation | Kubernetes controller stack, Restate, Fabro/Petri evidence | Remove cross-domain transaction coupling and string-prefix retry classification |
| R35 finite processes | systemd, Bazel log/result separation, Ractor, sandbox-driver | Remove unbounded waits, fabricated zero capture and the legacy duplicate ScriptRun executor |
| CheckRunner/artifacts | Bazel REAPI, Petri replay tests, sandbox-driver conformance, SWE-agent evaluation harness | Replace self-referential result identity and mutable-name cache assumptions with exact digest-bound evidence |
| Adapter/session SDK | Pebble, agentsessions, ACP | Consolidate duplicated adapter journals, session cursors, steering and recovery contracts |
| Agent/runtime simplification | mini-SWE-agent, Cayu, OpenHands | Require measured benefit for each extra controller state machine and keep runtime/control-surface boundaries explicit |
| Tool admission | Kingfisher, AgentGateway, MCPProxy, Agent Scan | Remove ambiguous fail-open policy paths and dynamic schema acceptance |
| Observability/accounting | OpenTelemetry, Langfuse, Pebble projections, Fabro/Petri issue fixtures | Keep telemetry optional/bounded; never use it as the only durable fact; never lose child usage on terminal projection |

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

1. one mature worktree-oriented ADE from the discovery pool, selected by issue quality rather than stars;
2. one AgentRouter/policy-control-plane implementation;
3. one cross-platform local process/sandbox layer not tied to Docker or Linux;
4. a mature content-addressed remote-execution implementation only if ELIOT moves beyond local CheckRun execution;
5. one durable workflow implementation only for cancellation/fencing fixtures, never as a second Store;
6. one agent fleet/control-tower system with real operator issue history rather than a dashboard demo.
