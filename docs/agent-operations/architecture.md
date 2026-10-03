# Agent Operations: Rust Architecture and Service Contracts

Revision 2 · 2026-10-03 · design against main `35e499ae73b622d873c44873f6993ee3fcbea87b`.

Read [Delivery](delivery.md) for queue/audit/publication transitions and [Configuration](configuration.md) for configuration semantics. Names here are proposed unless explicitly identified as existing. [Donor map](donor-map.md) separates external evidence from ELIOT design.

## 1. Implementation boundary

All ELIOT-owned internal logic is Rust:

```text
host / Store / authorization / configuration
runtime adapters / native transport readers / process supervision
source intake / monitoring / dashboard projections / streams
Git inspection / GitHub API client / webhook validation
assignment distributor / review router / publication controller
hooks registry / hook ingress executable / action handlers
cron / scheduler / Goal / shared reminder service
MCP facade / local gateway / CLI / script runner
```

Python and PowerShell execute only optional external extension scripts. Normal startup, task handoff, audit, recovery and push must work without either interpreter. No internal JS/TS control daemon, Node gateway, shell merge loop or Python GitHub worker is introduced. Rust-produced protocol/configuration data is not an executable script.

Native vendors still own their tools, model loops and services. Connect from Rust to documented HTTP/SSE, WebSocket, MCP/ACP or framed stdio interfaces that the selected runtime actually supports. A Rust SDK may be used whole where suitable. Do not manufacture a Rust SDK by translating commercial SDK internals or copying private protocol assumptions.

Existing Python/JS bridges and the Command TS mod are historical implementation sources. Port their necessary ELIOT-owned behavior into Rust, retain source attribution and qualify the new path. A vendor surface available only through a non-Rust custom plugin is a named integration gap, not permission to hide another owned JS/TS subsystem. Use a supported native executable callback or externally supplied documented protocol when available; otherwise expose only the capabilities actually reachable from Rust. Do not claim old and new bindings are interchangeable without recovery evidence.

Adapter upgrades affect future bindings. Existing bindings keep their recorded lifecycle owner until safe handover; missing compatibility never authorizes a replacement session while prior work may still run.

## 2. One admission path

Observation, deterministic decision and execution are separate:

```text
native/source event or deadline
  -> validated fact / current projection
  -> policy chooses a typed action
  -> scope, lifecycle and concurrency checks
  -> existing durable Operation + cause linkage committed
  -> Rust worker performs effect outside Store transaction
  -> result and actual resource disposition retained
```

The same action descriptor is used from manual MCP, a delivery preset, event rule, schedule or Goal. Initial action families are notification, source/check capture, assigned work dispatch, candidate review, repair handoff, registered script execution and explicitly authorized forge effects. Each maps to a typed application handler; no `call_any_method`, shell-command string, arbitrary remote URL or serialized closure is an action.

An invocation's execution state is the existing Operation, not a second job state machine. Additional records identify definition, cause, resolved settings, grant, target and result references. Delivery phases are projections over those facts.

Admission reads the currently active definition, resolves its settings once, and records that execution snapshot. An existing request ID always returns its original receipt even after configuration changes. Future independent work uses the new active definition. Do not bind every future occurrence forever to an old script/profile release.

Uniqueness is `(project, semantic cause, action slot, deliberate replay generation)`. Record the definition revision used; editing a rule alone does not produce another execution for the same cause. Same request identity with different input conflicts. An explicit authorized re-run is a new logical request, not an attempt to conceal uncertain delivery.

The workflow service executes under a scoped standing grant and retains its sponsor. It does not impersonate a model manager or hold a reusable GM credential. Recheck grants at admission and immediately before consequential effects. A revoked grant stops future starts; an already possibly-sent effect is reconciled, not reissued.

## 3. Continuous observation

### 3.1 Source contracts

| Source | Rust path | Coverage limit |
|---|---|---|
| Native stream | One adapter reader per supported connection/scope | An event belongs only to its verified binding, root/child and generation |
| Native snapshot | Shared bounded read-only reconciliation | Partial absence is not termination |
| OS processes | Recorded process ownership plus one selective sampler | PID alive is not model progress; failed enumeration is not an empty process set |
| Git | Shared file/ref hints followed by exact object/status reads | File event or commit author does not assign work |
| GitHub | Authenticated events plus paged conditional reads | Assignees, labels and comments are external facts, not controller authority |
| Store | Committed Task/Attempt/Operation/submission facts | Admission is not completed work |

Use events first. Source-specific read-only polling is an acceptable fallback when a native event stream is missing or unhealthy. It is paced centrally and shared across viewers. Never ask a model for status, launch a CLI with implicit service recovery to observe health, or start a poller per registered participant.

Process identity includes recorded start/boot/ownership facts, not just PID or executable name. A wrapper exit, quiet parent, closed tunnel or old mtime is not evidence that its children or native Goal stopped. No heuristic death detection grants a new writer access to the same worktree.

### 3.2 Dashboard row

Join these facts without merging their meanings:

```text
project / repository / Issue / source revision
Task / Task revision / Attempt / assignment / submission
manager / executor / assigned auditor(s)
binding / generation / native root / child / run / turn when supplied
worktree handle / branch / observed commit / candidate
process state / connection state / execution state / delivery phase
active tool / pending native question / child-family coverage
last transport event / last material progress / source freshness
CPU / RSS / process count / provider usage and capacity basis
pending rule, script, Goal, audit and publication Operations
```

`queued`, `model_running`, `tool_running`, `waiting_for_input`, `waiting_for_children`, `paused`, `terminal` and `unknown` describe execution, not acceptance. Preserve unfamiliar native status as raw data with unknown mapping rather than guessing success.

Dashboard, queue and inspect are maintained projections. Their reads do not serially interrogate every agent. A suspected stall is an explanation with evidence, not an automatic kill instruction. Supervisor failures, delayed audit and blocked publication remain visible separately from writer defects.

### 3.3 Snapshot, delta and fairness

Return a consistent committed snapshot plus its controller high-water cursor. Subscribe after that cursor. Preserve each upstream cursor independently; never promise a globally atomic GitHub/native/OS view. Source cursors remain opaque unless their protocol defines order.

The existing MCP subscription implementation polls committed facts per subscription. Preserve its lag/resync contract while moving shared intake/projection into the Rust host. Fanout must not repeat full scans for every viewer. Live content has a separate presentation sequence and is not the durable control stream.

Bound queues and bytes. Slow viewers receive one lag/gap indicator and resynchronize; they cannot backpressure native permission replies or terminal recording. Permission/reply and completion traffic have capacity reserved independently from text, Git scans and optional scripts. CPU-heavy parsing and OS enumeration use bounded blocking workers. No Store transaction waits on a network, process, callback or model.

## 4. Text and reasoning streams

`stream.open/read/close` select an authorized assignment/binding and content classes. Opening a stream starts observation, not a model. Support cursor reads as well as negotiated notifications; a client with no notification support still reads a bounded page, not a global transcript.

Classes are `assistant_text`, `reasoning_summary`, `native_reasoning_text`, `tool_progress`, `tool_result`, `lifecycle` and `usage`. Expose only content intentionally supplied by the native API. Hidden or encrypted reasoning is not decoded, reconstructed or claimed available. Return `unavailable`, `redacted`, `not_retained` or an explicit gap.

Retain item/part/native IDs and source order. Final content supersedes matching deltas without counting it twice. Cumulative usage replaces the preceding cumulative observation; it is not another charge. Unknown billing basis remains unknown.

Use shared bounded live rings and, when configured, capped artifact chunks. No Observation per token or transcript copy per viewer. Redaction must handle sensitive strings split across chunks; until a streaming redactor's guarantee is established, buffer bounded logical records or omit that sensitive content class. Tool output and reasoning are data, never rule definitions or authority.

Retain active/referenced evidence by ownership and references, not by filesystem mtime. Under disk pressure shed optional presentation history, not unresolved-effect evidence.

## 5. Hooks

### 5.1 Per-event capability report

Every installed adapter reports native event name, before/after phase, input/correlation fields, callback lifetime, ordering, output/veto rules and installation readback. Distinguish:

- `native_blocking`: actual supported pre-effect decision;
- `native_observational`: notification only;
- `wrapper`: only operations passing through ELIOT;
- `external_hint`: a fact requiring confirmation.

Wire supported lifecycle, tool, permission, child, compaction and material-source events. Unknown optional events do not break the adapter. Missing a required protected pre-effect hook prevents that action, not the entire fleet.

The public OpenCode plugin API is not automatically this repository's OpenCode V2 contract. Gemini CLI callbacks do not establish Gemini Spark or Antigravity capabilities. The existing Command ModApi listener is observational. A callback declaration does not prove installation or use.

### 5.2 Rust ingress and fast path

`swarm hook emit` is a Rust executable using authenticated local IPC; Rust in-process adapters may call the same typed ingress. Scope is setup-issued, not a JSON field an agent invents. Bound input fields, bytes and callback lifetime; preserve real native IDs or explicitly source-local epoch/sequence.

The callback does not run an audit, Python, GitHub write or model inline. Observational events enqueue compact facts and return according to native semantics. Optional telemetry loss becomes a gap; it cannot wedge compaction or productive work.

A genuine blocking gate evaluates local bounded policy, without waiting on a script/model/remote API or a Store lock held by its caller. It may deny only the relevant protected effect when permission is unknown. Async callbacks cannot undo an action already performed.

### 5.3 Configuration

`hook.install.preview/apply` preserves unrelated user and managed hooks, checks the source configuration, stages the change and records readback. No hidden global hooks-path replacement, service restart, PATH change or new interpreter. An authorized project-local installation grant can permit repeat changes without Root involvement; new global privileges still need the grantor.

Protected forge actions continue to suppress uncontrolled Git hooks. Rust before/after action events provide their integration points. Hooks for native tools outside ELIOT need real native support; wrapper coverage is never advertised as universal.

## 6. Rules and causal loop control

Rules match a small typed predicate language over event kind, project, assignment, lifecycle and known fields. No shell interpolation, unrestricted template evaluation or method selection from prose. Configuration preview/simulation returns planned actions with no effects.

Persist root cause, parent invocation, semantic event key, action slot and ancestry. Default rules ignore their own descendants. Static cycles are rejected; dynamic ancestry and granted work budgets contain non-obvious cycles. Policy changes do not replay old comments or all retained events.

Automatic error handlers are subject to the same dedupe and authority. A failed notification must not spawn an agent to explain the notification failure. A meaningful new fact can re-enable work; unchanged failure parks only the affected rule/work item with an explanation. No universal arbitrary number of repair rounds becomes a product-completion limit.

## 7. Server cron and reminder service

Extend `src/scheduler.rs` and `src/store/schedules.rs`. There remains one scheduler and one durability authority. Preserve v1 configured CheckRun IDs/receipts; migrate definitions and due indexes without two owners for one schedule. Do not scale the old 64-entry JSON meta blob into a fleet-wide hot row.

A schedule names `once`, anchored `interval` or `cron`, enabled state, timezone/calendar policy, lateness/catch-up, overlap, action selector and grant. Configuration stores the intended calendar semantics, not a prescribed cron-library release. Use a complete maintained Rust expression evaluator and one timezone library; no handwritten cron parser.

Cron preview states the grammar, next occurrences, timezone, spring-gap and repeated-time behavior. A timezone is explicit; host locale does not silently decide it. Any unsupported requested calendar behavior is a validation error. Detect semantic changes when the evaluator/timezone data is updated, keep already admitted occurrences intact, and recalculate future occurrences under the declared policy. Do not require the old binary indefinitely or silently duplicate a previously considered UTC occurrence.

Occurrence identity is schedule ID, logical schedule generation and due UTC instant. Actual start time/jitter is separate. Content/profile updates affect future admissions but do not re-run an already considered slot. Explicit `run_now` has its own request ID. Default catch-up is latest-only; bounded replay is opt-in and requires a repeat-safe action.

Pause disables new starts, not active work. `new_work=disabled` is respected for execution. Read-only observation remains available. An uncertain earlier same-target effect prevents competing mutation, not unrelated work. Monotonic waits plus wall-clock rechecks handle sleep/reboot and clock jumps.

Reminders reuse #22's `coordination.watch.*`, mailbox and subject indexes. One shared timer structure serves all watches; ordinary watches send notices and never start models. A separately configured action can dispatch authorized work on the same event. The distinction remains visible in the receipt.

## 8. Server Goal

`goal.*` is ELIOT-owned; `agent.goal` remains a native adapter capability. A server Goal references the manager's existing Task pool and completion predicates, not another Task graph. It stores desired outcome, standing execution grant, route preferences, budgets, evidence, waiting reason and current continuation owner.

State is `draft`, `active`, `waiting`, `paused`, `achieved`, `cancelled` or `needs_attention`. Operation outcomes remain separate. A goal can advance assignments, review and repair automatically within its grant. It cannot enlarge its own work pool or change acceptance policy from generated text.

Achievement checks the configured required evidence for exact Task revisions. Commit volume, tokens, running-agent count and an agent's statement that it is done are not completion. Unmachine-checkable objectives require the configured authorized evaluator.

Exactly one continuation owner is active for an assignment: server, native or manual. Switching from native Goal requires supported pause/clear and actual readback. Unknown input or active children prevents a competing continuation. Never re-prompt just because a wrapper ended or a manager went silent.

Pause/cancel changes future admission. Cancelling already running children is a separate addressed operation through their owner. Persist useful partial results; no timer invents permission to kill.

## 9. Optional external scripts

`script.register/revise/validate/activate/run/get/list` manage named Python or PowerShell extension bundles. The registry, validation, runner, result ingestion and scheduling are Rust. A script is not a mandatory wrapper for notification, push, merge, audit or queue operations.

The active definition is the normal selector for future runs. At admission resolve the bundle, installed interpreter, dependency environment, input schema, working scope and trust profile and retain that run's evidence. New edits apply to later invocations. An old request retry returns its prior result; it does not resolve the newly active script again.

Capture declared support files with the entrypoint so a running script cannot change halfway through from an edited import. No package installation in a hook or timer callback. Environment preparation is a separate explicitly authorized setup action. This is execution integrity, not a requirement to use one fixed Python/PowerShell/dependency version forever.

Pass JSON stdin and separate argv. Do not interpolate Issue bodies, event text or branch names into source code. Use the installed `pwsh` with noninteractive/no-profile file execution, or the selected Python environment; no default ExecutionPolicy Bypass. These switches are hygiene, not sandboxing.

A result has typed status, evidence references and bounded optional progress. Drain stdout/stderr without blocking control. Script tokens are invocation-scoped, short-lived and unable to grant roles or accept their own work. Each authorized child effect has an action-slot identity.

`trusted_local` has the real permissions of the selected OS user; cwd, a manifest or an MCP allowlist do not prevent arbitrary local code accessing that user's files/network. `isolated` requires actual OS/container/VM enforcement; missing support never downgrades silently. Job Objects/process groups provide lifecycle containment, not permission isolation.

Scripts can be authored, activated and run by agents inside a standing local project grant. Untrusted repository changes cannot silently become trusted-local code or widen secrets/network/host access. Timeouts affect only the invocation's owned descendants. Unconfirmed surviving writers hold that mutation scope; exit zero alone is not sufficient. Unknown external effects are reconciled, not blindly retried.

## 10. Roles, authority and MCP

Separate principal, role capability bundle, object scope, standing execution grant, MCP profile/surface, current GM designation and OS identity. Presets include manager, executor/Participant, auditor, observer and GM candidate/operator. Several auditors are normal. A role named `gm` does not perform leadership handover.

Custom roles contain finite registered capabilities and scope constraints, not arbitrary policy code or a wildcard covering future methods. Grantors delegate only rights they can delegate. Role/grant revocation invalidates cached authorization immediately for new calls/actions.

Current `task.request_changes` requires GM/operator. The new scoped workflow service must be allowed through an explicit audited-disposition authorization branch, retaining the existing candidate guards. Do not solve this by leaking GM credentials to reviewers or by adding a second review state machine. The same explicit principle applies to delegated acceptance/publication; see Delivery.

Preserve #22's small core. Configuration, runtime profiles, streams, review/delivery, hooks, rules, schedules, goals, scripts and roles are deferred groups. No generic passthrough and no script-per-version tool explosion. `script.run` accepts a named active script and typed input; optional promotion requires normal catalog metadata and authorization.

Reads are side-effect-free with respect to work. Refresh may perform authorized read-only source I/O but cannot start a model or modify settings. `tools/list` pagination alone is not lazy model loading. Native search or verified surface refresh is used where supported, otherwise a bounded fixed role surface. Every call rechecks permission, including cached or manually addressed tools.

## 11. Persistence and recovery

Keep SQLite/Store, existing Operations, Observations and artifacts. Add only needed indexed metadata: active configuration, origins, work-pool membership, review assignments/results, due occurrences, grants and action/effect linkage. Use forward migrations, not edits of old schema history.

Current indexes and bounded pages drive scheduling. No full event-history replay per viewer or per agent. Large script bundles and optional stream content use artifacts. Durable intake plus pending effect records form an outbox in the same Store; no second broker is needed.

A model/client exit does not stop server policy. Reboot reconciles queued/possibly-sent operations and recorded process ownership before new dispatch. Lost GitHub responses are read back. Ambiguous source data is retained as unknown. Service restart, host cleanup and repository-settings changes remain explicit operator actions, not automatic repairs for a failed observation.

Useful autonomy is the goal: expensive effects are precisely authorized, ordinary eligible work is automatic, and one bad source, unavailable model or publication policy does not freeze unrelated tasks.
