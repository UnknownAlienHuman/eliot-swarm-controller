# Agent Operations: Architecture and Contracts

Revision 1 · 2026-10-03 · proposed against `35e499ae73b622d873c44873f6993ee3fcbea87b`.

This is the normative design for the extension described in [README](README.md). Refer to the [donor map](donor-map.md) for external evidence and the [implementation plan](implementation.md) for delivery order. Method names below are proposed unless explicitly labelled existing.

## 1. One host, three different kinds of work

Separate these paths:

1. **Observation:** consume native events, sample owned processes, reconcile Git/GitHub and maintain projections. No model prompt.
2. **Decision:** evaluate a registered rule, deadline, Goal condition or capability grant using typed facts. No model is needed for ordinary routing.
3. **Execution:** admit an authorized action as a normal durable Operation, then execute outside the Store transaction. A model runs only if the selected action explicitly starts/continues a permitted assignment.

An observer disconnect does not stop native work. A hook callback is not another controller. A Python script is not a second scheduler. A model's text is not an accepted Task, a commit receipt or a permission grant.

### 1.1 Shared action boundary

Manual MCP calls, cron, hooks, Goal transitions and reminders use the same action admission path:

```text
validated cause + pinned definition + execution grant
    -> deduplicate
    -> resolve exact target
    -> recheck scope, revision, lifecycle, concurrency and permission
    -> commit existing Operation and cause linkage
    -> execute through native Rust handler / owned script / RuntimePort
    -> retain outcome, evidence and actual resource disposition
```

A small closed `ActionSpec` enum is extended deliberately. Initial kinds:

| Kind | Target implementation | Default effect |
|---|---|---|
| `notify` | Existing mailbox/attention with exact recipient or authorized role resolver | Durable notice, no model turn |
| `run_script` | Versioned script registry and owned runner | Explicit script effects within selected trust/grant |
| `run_check` | Existing CheckRunner admission | Exact candidate/profile check |
| `dispatch_task` | Existing Task/Attempt dispatch, compatible with #22 launcher | New work within manager-assigned pool |
| `continue_assignment` | Exact existing assignment via supported RuntimePort semantics | Explicit authorized continuation, never uncertain-input retry |
| `publish_ref` | Existing `forge.publish_ref` | Existing protected publication semantics |
| `merge_pr` | New qualified forge merge implementation | Protected merge; not a script shortcut |

There is no `call_any_method`, string shell command, arbitrary HTTP destination or serialized closure action. Small request/result/context transformations use reviewed typed mappings or a registered script, not an embedded unrestricted template language.

## 2. Continuous monitoring, not model interrogation

### 2.1 Sources and ownership

| Source | How to observe | What it does not prove |
|---|---|---|
| Native SDK/HTTP stream | Reuse the adapter's existing reader; route by exact native root/child and binding generation | Text activity is not Task progress or success |
| Native snapshot API | Bounded shared reconciliation after a gap, reconnect or freshness deadline | Absence from one partial list is not death |
| Owned process group | Existing recorded OS identity plus shared metrics sampler | PID alive is not a running model turn |
| Git repository | File/ref hook hints, then bounded exact ref/status/object reads | Commit author is not current assignment owner |
| GitHub | Signed webhooks plus conditional reconciliation reads | Assignee/label/PR merge is not ELIOT acceptance |
| Store | Committed Task/Attempt/Operation/submission/check facts | A queued request is not a completed external effect |

One reader per native connection/required stream scope, not one reader per dashboard viewer. Multiplex roots only where the native protocol permits it. A shared app-server event is assigned by its exact thread ancestry; never count all traffic in every manager's stream.

The OS sampler is one long-lived collector with selective refresh and bounded work. PID reuse is checked using recorded start/boot identity and process ownership, not executable-name matching. Enumerate unknown external processes for diagnostics only; do not kill or adopt them automatically.

Where event APIs are absent, controlled read-only polling is acceptable. It is performed by the server, paced by source health/capacity, shared among observers and labelled as polling. Do not spawn a CLI health check whose implementation may restart the native service. Never send a model a status question merely to populate the dashboard.

### 2.2 Work row

A dashboard row joins exact identities and retains separate state dimensions:

```text
project/repository + Issue + selected source revision
Task + Task revision + Attempt + assignment/ProducerRef
manager/executor/auditor + grant revision
binding + generation + native root/child/run/turn when available
manager worktree handle + current ref/OID + candidate/submission
process state / transport state / execution state / work state
current tool and pending native question
last transport event / last native progress / last material project progress
child-family coverage, source cursor, freshness and gaps
CPU/RSS/owned descendant counts as observations
quota/capacity and usage semantics (per-turn/cumulative/unknown)
Goal/schedule/script/hook invocation references
```

Useful execution values include `queued`, `starting`, `model_running`, `tool_running`, `waiting_for_input`, `waiting_for_children`, `paused`, `terminal` and `unknown`. Preserve `native_status_raw` beside the normalized status.

A state such as `tool_running` for a foreground child must not be displayed as a dead manager. Unknown source health does not turn a previously active child into completed. A diagnostic `suspected_stall` is advisory, with evidence and confidence; it never triggers a kill by itself.

### 2.3 Snapshot and delta

`swarm.dashboard`, `swarm.agent.inspect` and `swarm.queue.get` retain the convenience names proposed in #22. They read host-maintained projections, not a series of agent prompts.

A snapshot returns a controller high-water cursor and separate source watermarks. The client subscribes after that controller cursor and receives bounded deltas. Snapshot construction and cursor selection must use one consistent committed cut. Live presentation has its own sequence and is explicitly non-authoritative; do not claim a globally atomic native/GitHub snapshot.

On overflow/disconnect: return `lagged` plus the missing range/resync address. Recovery is a bounded read from retained facts. On restart, rebuild only required indexed current state and resume cursors. A source that cannot replay reports a gap.

Current `src/mcp/subscriptions.rs` polls per subscription every 250 ms and carries only committed facts. Preserve its public lag/resync semantics, but share the pump/projector by source/filter and avoid multiplying scans with viewer count. Do not claim that it already streams provider text.

### 2.4 Streaming text, tools and reasoning

`stream.open`, `stream.read`, `stream.close` use an exact authorized binding/assignment and content-class filter. `open` creates a read subscription, not a model execution. MCP clients without native notifications use bounded cursor reads; capable clients receive notifications or resources supported by their negotiated protocol.

Classes: `assistant_text`, `reasoning_summary`, `native_reasoning_text`, `tool_progress`, `tool_result`, `lifecycle`, `usage`. Only expose fields deliberately provided by the native API and permitted by local policy. Do not extract hidden chain-of-thought, decode opaque/encrypted reasoning state, synthesize a fake reasoning transcript, or ask another model to reconstruct it. Report `unavailable`, `redacted` or `not_retained` where appropriate.

Every part carries native message/item/part identity, sequence/cursor, visibility and redaction status. Complete result objects supersede presentation deltas without double-counting bytes/tokens. Tool outputs remain untrusted data, not hook rules or instructions.

Use bounded shared rings for live deltas and capped spool chunks only when retention is enabled. Keep control/terminal facts durable; do not store one full Observation per token. Evict slow viewers rather than delaying the native reader. A large tool output is a paged artifact/reference with explicit byte coverage. Stateful redaction must cover secrets split across chunk boundaries; opaque authentication/reasoning fields are excluded rather than logged for debugging. Existing atlas-redact remains the redaction boundary; its current guarantees must be verified for streaming before claiming coverage.

Retention protects active sessions and referenced evidence. A stale filesystem mtime is not permission to delete a live transcript.

## 3. GitHub and Git integration

### 3.1 Work pool and source truth

The manager selects repositories and a pool of Issues/PRs through `github.work_pool.preview` and `github.work_pool.apply`. A pool is a versioned selection/mapping to existing ELIOT Tasks, not another task ledger. Imports are idempotent by immutable repository ID, external item ID and source revision. Existing Task source-index and owner-policy rules decide what becomes specification.

Observe Issues/comments/PR heads/reviews/check results through a GitHub App or another reviewed credential source. Credentials, webhook URLs and local roots are setup-only secrets/references. App installations request only required repository permissions. Repository settings/branch protection are never silently changed.

Webhook intake verifies HMAC on the raw request body, event/action and installation/repository allowlist before committing. Deduplicate by installation/repository/delivery identity and payload digest. Acknowledge only after durable intake, then process asynchronously. GitHub redelivery can retain the same delivery ID. Do not assume ordered or complete delivery; use paged conditional reads and bounded reconciliation after gaps, rate limits and reconnect.

Provider `updated_at` timestamps are not unique CAS versions. Record external IDs, ETag/version where supplied, observed content digest and collection coverage. A missing item in an incomplete page is not deleted. GitHub assignees and labels are external facts; ELIOT's accepted Task/Attempt ownership remains separate.

An Issue edit marks the selected specification stale and creates one relevant exception. It cannot rewrite a running assignment's frozen requirements or grant new script privileges automatically.

### 3.2 Local commit observation

Managed local Git hooks and filesystem events are hints. Verify the observed ref/OID in the exact manager worktree and registered repository before emitting `git.commit_observed`. Resolve linked worktree metadata and common Git directory through Git, not by assuming `.git` is a directory.

Do not use a regex matching `git commit` in model output as a commit receipt. A `push` webhook proves remote ref information, not which local agent typed the command. When ownership cannot be established, label the event unassigned and do not notify an arbitrary auditor.

One shared Git observer services interested Tasks. Debounce path/ref changes and perform bounded read-only scans; never run full `git status` for every token/tool or per subscriber. File-watch overflow becomes a dirty scope requiring reconciliation, not silent completeness.

### 3.3 Commit -> auditor

Built-in recipe, configurable without writing a script:

```text
verified commit in a managed workspace
  -> resolve exact Task/Attempt/worktree and new OID
  -> persist one audit notice keyed by repository + work context + OID + rule revision
  -> route to authorized auditor pool
  -> optional pre-authorized review dispatch
  -> findings tied to exact candidate/submission
```

Notice delivery never invokes a model by itself. The manager may grant automatic review dispatch once, with allowed projects, reviewer roles, cost/concurrency and candidate freshness rules. This enables useful unattended automation without turning every commit into a Root approval request.

WIP commits can send a cheap informational notice; a review requires captured immutable candidate evidence. Coalesce queued superseded WIP notices, but never erase an accepted review, objection or submitted candidate. Local hook and remote webhook for the same material OID must not launch duplicate audits. A late result can annotate its old candidate, not invalidate a newer submission by branch name alone.

Multiple auditors may work independently. Scheduling chooses among authorized available auditors and records assignments; disagreement is preserved. Audit opinion is not automatic Task acceptance.

## 4. Hooks as adapters, not a universal fake guarantee

### 4.1 Capability matrix

Every installed module/plugin declares and reports per event:

```text
native event name + installed native version
phase: before / after / lifecycle / stream
class: native_blocking / native_observational / wrapper / external_hint
available input fields and correlation IDs
allowed outputs and veto/rewrite capability
sync/async semantics and failure policy
installation/readback revision
status: documented / installed / observed / unsupported / unknown
```

Use all required supported events for lifecycle, tools, subagents, permissions, compaction and material file/commit activity. Missing optional events produce a named gap. A required pre-effect policy on an unsupported runtime blocks only that protected operation/route, not the whole fleet.

Different native versions/products remain different adapters. Public OpenCode plugin documentation is not automatically the contract for the repository's OpenCode V2 service. Gemini CLI hooks are not proof that Gemini Spark or Antigravity exposes the same callbacks. Existing Command ModApi listeners observe; they do not veto a tool. SDK package presence does not prove ELIOT wires its hook options.

### 4.2 Hook ingress

A native hook delegates to a small `swarm hook emit` ingress helper or in-process adapter callback. Scope comes from a setup-issued binding/session grant, not a self-asserted JSON manager ID. Allowlist event types and bounded fields. Preserve native event IDs when supplied; otherwise generate a local source epoch/sequence and explicitly avoid claiming native replay identity.

The hook does not run Python, a full audit or network publication inline. It validates/adopts a fact or returns a native policy decision within a bounded deadline. Long actions are admitted to ELIOT's server queue.

Observational telemetry failure must not stop productive work or compaction. Return native success when permitted, record an ingress gap/loss counter and retry only transport delivery with the same event identity. A source without durable replay may lose telemetry; do not claim otherwise.

A blocking safety hook is different: its short local evaluator may deny the specific protected tool when policy cannot be established. That evaluator must not wait on a model, script, GitHub API, Store lock held by the caller or its own callback. Never label an async hook as prevention after the action already executed.

### 4.3 Installation

`hook.install.preview/apply` operates on exact local configuration revisions. Preserve unrelated user/managed hooks, native ordering and entrypoint semantics. Managed restrictions are respected. Apply staged changes with readback and rollback plan. No silent service restart, global `core.hooksPath` replacement or edits of running scripts.

MCP wrapper before/after events cover ELIOT calls only. They do not cover tools executed wholly inside a native harness. For batch adapters without hooks, expose process-start/output/exit and the coverage gap instead of invented tool events.

Protected forge publication currently skips arbitrary Git hooks. Keep that guarantee. Native ELIOT action before/after events provide extension points without executing uncontrolled repository Git hooks in a privileged push.

### 4.4 Rules and loop prevention

A rule selects typed event fields, scope and phase, and pins an ActionSpec plus mapping revision. It cannot select a method from arbitrary event text. Replay/simulation performs no effects.

Cause linkage includes root cause, parent invocation, rule revision, semantic event key, action slot and bounded ancestry. One trigger event/slot creates one retained Operation even when delivered twice. Rule edits do not reprocess old history unless explicit replay is requested.

By default a rule does not react to its own descendants. Cycles are rejected where statically visible and stopped by ancestry/hop/rate bounds otherwise. Repeated no-progress or failures pause that rule and create one incident; they do not spawn explanatory agents. Bound automatic error/recovery actions too.

## 5. Server scheduler, cron and reminders

### 5.1 Extend the existing scheduler

Keep one host scheduler and the existing Store transaction/Operation semantics. The v1 registry of at most 64 configured CheckRuns remains readable with unchanged IDs and receipts. New definitions use versioned records with due-time indexes; do not enlarge one global JSON blob into a hot row for thousands of timers.

Definition fields:

```text
schedule_id, revision, enabled, owner/grant reference
trigger: once | interval | cron
once: due_at_ms
interval: anchor_ms, period_ms
cron: expression, dialect, timezone, calendar_engine_revision
start/end bounds, optional deterministic jitter
misfire: skip | coalesce_latest | bounded_replay
catchup_window_ms, overlap: skip | buffer_latest | allow_bounded
max_concurrency, action revision, input, next_due_at
```

`once` and legacy interval retain their existing timestamp rules; an old anchor of zero remains valid. Optional message deadlines are a separate type.

Cron uses a selected complete parser/evaluator, initially the reviewed Croner 3.0.1 API plus timezone support; no handwritten parser. Declare the accepted dialect explicitly. Preview shows the next occurrences, effective timezone and daylight-saving behavior. Do not infer timezone from whichever machine runs the host.

The initial calendar behavior follows the selected Croner contract: fixed-time schedules advance past a spring gap and use the first repeated fixed time; wildcard schedules skip missing matches and can match both repeated occurrences. Exact timestamp identity is UTC. Record the engine/tzdata revision and show the behavior in preview. For effects requiring uniform intervals, recommend UTC/interval rather than silently changing local calendar semantics. A later engine update must not silently reinterpret an existing schedule.

### 5.2 Occurrences and overlap

Occurrence identity derives from schedule ID/revision and logical due UTC instant; actual launch time and jitter are separate. Persist consideration and admitted Operation linkage atomically. Manual `run_now` has its own request identity and does not impersonate a missed scheduled occurrence.

Default is `coalesce_latest` with one pending latest run and no overlapping mutation of the same target. Bound catch-up windows and replay counts. Replay requires a repeat-safe action; never backfill an uncertain push or prompt. One-shot missed work is eligible only within its configured lateness policy.

Use monotonic waits and periodic wall-clock rechecks. Handle sleep, restart and clock jumps. Disable/pause prevents new starts; it does not kill an active job. Resume considers policy-eligible work, not a burst of every old tick. A new definition revision does not rewrite an in-flight invocation; queued old work is cancelled/deferred or explicitly retained by revision policy.

### 5.3 Model-independent operation

Cron evaluation, due-time handling, notification and script dispatch run inside the long-lived ELIOT host even if no manager model is awake. No harness `CronCreate`, session loop or desktop timer is the durable authority.

A scheduled model action requires an active standing execution grant and its exact target/pool. If no capacity exists, it waits or is skipped according to policy. Unknown quota is not infinite capacity. Repeated provider limits use provider-specific reset evidence/backoff; no universal hardcoded 429 interpretation.

### 5.4 Reminder/watch service

Use the `coordination.watch.*` service planned in #22, not another reminder table with unrelated semantics. A watch observes a named fact or deadline and produces a notice. One-shot is the default; recurrence is explicit and coalesced. One shared timer heap and indexed event matching serve all participants.

Delivery states distinguish stored, available, presented and consumed. A reminder cannot replace the current Task prompt or wake an old session. A manager can deliberately attach an authorized `continue_assignment` action to a reminder rule; ordinary reminder delivery still starts no model.

## 6. Server Goal

### 6.1 Independent authority

`goal.*` is an ELIOT object; `agent.goal` remains a native adapter operation. A server Goal works on runtimes with no native Goal feature.

A Goal contains:

```text
goal_id, revision, owner and role/grant revision
project/work-pool reference and allowed Task revisions
objective and completion contract
allowed action kinds and target-selection policy
capacity, cost/turn budgets and optional deadline
progress evidence and unresolved conditions
continuation owner: server | native | manual
state and last admitted action references
```

Goal is not a second Task. Its work is ordinary Tasks/Attempts and its effects are ordinary Operations. No hidden recursive decomposition: expanding the work pool or creating new Task definitions needs a manager grant explicitly permitting that operation.

### 6.2 States and progression

```text
draft -> active -> waiting | paused | achieved | cancelled
                      \-> needs_attention
```

`waiting` includes native work, dependency, capacity, budget reset and authority renewal. A distinct execution ledger contains attempts/outcomes; lifecycle state never doubles as proof of task success.

For a development Goal, achievement is the declared set of required Task acceptance/evidence predicates at exact revisions. A textual objective without a machine-checkable completion predicate needs explicit authorized evaluation; it never becomes achieved from the model saying "done". Commit count, tokens and running-agent count are not completion criteria.

The planner evaluates changed committed facts, selects at most the authorized next action and persists it. A standing manager grant can authorize continued work from an assigned pool, bounded repairs or review dispatch without a fresh manager approval each time. It cannot extend its own permissions, buy capacity, change provider/model or merge work unless those capabilities were explicitly delegated.

No-progress detection compares material work/evidence revisions, not elapsed silence. Repeated unchanged failure yields waiting/needs_attention and a compact reason. It never creates unlimited re-prompt loops.

### 6.3 One continuation owner

When native Goal continuation is active for a binding, ELIOT observes it and does not run a second server continuation loop on the same assignment. Switching owner requires reconciliation and supported native pause/clear readback. Unknown prior continuation prevents a competing start, not unrelated Task work.

Server continuation starts only after the preceding input outcome and relevant native execution/family disposition are established. A transport ACK or parent turn end with active children is insufficient. Never retry a possibly delivered input with a new ID.

Pause stops future admissions; current work continues unless separately cancelled through its owner. Cancel records the Goal decision; cancelling child Operations is a distinct scope-explicit action. Native limitations are returned honestly.

## 7. Named PowerShell and Python scripts

### 7.1 Authoring and invocation

Scripts are first-class versioned extension artifacts, not arbitrary command strings embedded in schedules.

```text
script.register/revise -> immutable content + manifest revision
script.validate       -> schema/interpreter/dependency/grant checks, no execution
script.activate       -> choose a version under existing authority
script.run            -> existing durable Operation + owned process
script.get/list       -> metadata, inputs, state, receipts
```

Users and agents with `scripts.author` may create/revise their own scripts. A grant may permit activation and runs within the current project/workspace without Root approval on every edit. It must explicitly state trust mode, allowed runtimes, resources and API effects. Widening that envelope requires authority; changing only implementation bytes creates a new revision rather than modifying a running file.

### 7.2 Manifest

```json
{
  "contract": "eliot-script-v1",
  "script_id": "project-a/report-changes",
  "runtime": "python",
  "entrypoint": "main.py",
  "content_ref": "registered-immutable-bundle",
  "input_schema_ref": "registered-schema",
  "result_schema_ref": "registered-schema",
  "execution_profile": "project-a-tools",
  "working_directory": "assigned-workspace",
  "effect_class": "workspace_mutation",
  "retry": "never_after_possible_start",
  "timeout_ms": 120000,
  "output_limit_bytes": 1048576
}
```

The profile resolves an installed interpreter, dependency environment, private secret references, process limits and trust mode. No implicit `pip install`, npm download or online dependency resolution at hook time. A script bundle includes declared support files; execution uses a read-only captured version rather than rereading a changing worktree file. Source paths alone are not executable identity.

### 7.3 Runtime contract

PowerShell: selected `pwsh` executable, `-NoProfile -NonInteractive -File`, arguments as separated data. Respect OS policy; do not default to ExecutionPolicy Bypass. Python: selected interpreter/venv with configured isolation flags and entrypoint; flags and NoProfile are hygiene, not sandboxes.

Pass bounded JSON on stdin and deterministic fixed argv, never interpolate event/branch/Issue text into shell code. UTF-8 output is a typed protocol: bounded progress records, final result, stderr diagnostics. Unknown fields/types fail validation; a line containing "success" is not a receipt. Large output is chunked/spooled under retention policy, not put in the manager prompt.

A script receives only its minimal environment and optional invocation-scoped ELIOT token. The token permits specific child actions/targets and cannot administer clients, read unrelated secrets or accept its own work. Child action IDs derive from invocation plus declared action slot; scripts do not mint new retry identities to evade dedupe.

### 7.4 OS trust is explicit

`trusted_local` executes code with the selected local OS identity. Capability manifests, cwd, argv and API scopes do not prevent that code from opening other files or network connections allowed to that OS identity. Do not call this sandboxed.

`isolated` requires a separately supported OS/container/VM runner with demonstrated filesystem/network/credential isolation. If the selected runner cannot enforce it, reject that mode; do not silently fall back to trusted local. A Job Object/process group contains lifecycle, not permissions.

Untrusted remote or repository-supplied scripts may be authored/staged but cannot auto-activate as trusted local merely because an agent created a manifest. An operator can preauthorize a trusted local project for convenient agent-authored scripts after accepting that OS trust boundary.

### 7.5 Lifecycle and retries

Reuse existing owned-process patterns. On Windows, establish process ownership at creation; on Unix preserve the declared child-group owner. Concurrent stdout/stderr draining is bounded. Timeouts cancel only that script invocation's owned descendants, never a shared native server or unrelated manager.

Success needs valid result, required evidence and confirmed owned-process disposition. A surviving/unknown descendant holds the relevant workspace/effect concurrency key. No automatic replacement writer starts there. Exit zero alone is insufficient.

Read-only retry is allowed only by an explicit action contract. Arbitrary local mutation and external effects remain unknown after a possible start until reconciliation or operator decision. A script's self-declared `idempotent=true` is not enough.

## 8. Native actions and protected forge

Keep ordinary notification, state projections, cron evaluation, review-queue admission and existing CheckRunner/publication in Rust. Scripts are for additional behavior, not mandatory wrappers around every built-in action.

`forge.publish_ref` remains the current exact accepted-candidate operation. Its preflight is not an atomic expected-old CAS; the current documentation explicitly records that race. This program does not upgrade the guarantee by renaming the operation.

A new `forge.merge_pr` must pin repository, PR, head, merge strategy, accepted candidate/tree and required policy. GitHub's expected head SHA guards the head, not an arbitrary expected base. For merge/squash, verification must cover the actual integrated candidate under a qualified merge-queue or equivalent serialization/check mechanism. If that guarantee is unavailable, report the merge mode unsupported or require the documented operator-controlled path; never bypass branch protection or claim the PR head alone is a verified merged tree.

A merge result lost in transport is reconciled by exact PR/ref/commit readback. Script output, a webhook or a GitHub comment cannot grant merge or acceptance authority. Accepted code, published code and merged code remain three facts.

## 9. Roles and delegated automation

### 9.1 Separation

Keep distinct:

```text
principal identity (human, model participant, module, script invocation)
role definition (finite capability set)
object scope (project, Task, Attempt, workspace, repository)
execution grant (who may start which actions, where and how often)
MCP profile/surface (permission ceiling and loading presentation)
current GM designation/epoch (one leadership owner)
OS execution identity/trust mode
```

Built-in presets: executor, manager, auditor, general-manager candidate, operator and read-only observer. The executor builds on the scoped Participant design in #22. Auditor is a capability preset, not a privileged synonym for operator; several independent auditors can be assigned concurrently. Holding the GM preset does not make a principal the currently designated GM.

### 9.2 Custom roles

`role.define/update/get/list` stores immutable revisions of finite registered capabilities with scope constraints. No executable policy script and no wildcard granting future methods by default. Explicit custom role bindings attach to principals; readable labels never decide permission.

Example: `build-observer` may read its project's status/streams and invoke one named diagnostic script, but cannot alter Task assignment, hooks, schedules or credentials.

The grantor may delegate only its delegable capabilities within its own scope. Separate script author, script runner, rule author, rule activator, schedule editor, Goal editor, model-dispatcher and forge publisher rights. No script/agent self-promotion. Role/grant changes invalidate cached decisions; queued actions are checked again before effects. Already uncertain effects are reconciled without retrying them under a new identity.

Before adding roles, audit every existing `require_writer`/role match/profile gate. A generic check that merely rejects observer must not accidentally authorize new roles. Keep module credentials restricted to their binding protocol; hook intake and script effect requests get separate narrowly scoped ingress contracts.

### 9.3 Delegation without a Root queue

A manager can approve once: "For this project and assigned work pool, run these script profiles, dispatch at most N reviewers, and notify this auditor pool on submitted candidates." Every invocation is still checked, but there is no model/human approval round for each eligible event.

Escalate only a genuine scope/trust/policy change, missing required owner, unresolved effect or budget decision. Ordinary monitoring, peer questions, compatible local agreements, notices and already granted scripts continue independently.

## 10. MCP and agent UX

Use the small eager role cores from #22; do not add every operation below to the initial prompt. New domains are metadata in the same hard-authorized catalog:

| Deferred group | Representative methods |
|---|---|
| Monitoring | `swarm.dashboard`, `swarm.agent.inspect`, `swarm.queue.get`, `swarm.exceptions.get` |
| Streams | `stream.open`, `stream.read`, `stream.close` |
| GitHub work pool | `github.work_pool.preview/apply`, `github.sync.status`, `github.item.get` |
| Hook setup | `hook.capabilities`, `hook.install.preview/apply`, `hook.status` |
| Event rules | `automation.rule.create/update/list/get/enable/disable`, `automation.simulate` |
| Scheduler | `schedule.create/update/list/get/preview/pause/resume/run_now` |
| Server goals | `goal.create/update/get/list/pause/resume/cancel/evaluate` |
| Scripts | `script.register/revise/validate/activate/run/get/list` |
| Roles/grants | `role.define/update/get/list`, `grant.create/revoke/get/list` |
| Reminders | existing planned `coordination.watch.create/list/cancel` |
| Forge | existing `forge.publish_ref`, new qualified `forge.merge_pr` |

This table is a method inventory, not permission. Resolve exact signatures against the public registry before implementation; slash lists denote separate typed methods, not a string-dispatch API.

All mutations require caller-owned request IDs. Reads do not trigger hidden scripts, refresh native work by prompting a model or perform network writes. Deferred schema activation changes presentation only, never roles or effect authority. Tool pagination alone does not guarantee model-side deferral; verify each client's native search/list-change behavior and use the fixed bounded surface fallback from #22.

A script is not automatically a new MCP tool. `script.run` uses a named revision and typed input. Optional promotion of a script to a discoverable tool requires reviewed schema/risk metadata and the same permission gates, preventing script revisions from exploding or silently widening the catalog.

## 11. Runtime, storage and fairness

Keep one Store and SQLite durability authority. Permit forward migrations for indexed automation definitions, due occurrences and invocation linkage; do not force thousands of records into the old 64-schedule `meta` blob. Existing Operations remain execution outcomes; linkage tables do not duplicate that state machine.

Suggested logical indexes: enabled definitions by kind/scope; next due time; source event plus rule revision/action slot unique key; invocation Operation ID; scope/concurrency key; outstanding watches by subject; Goal by affected Task. Records are paged, retention-aware and rebuilt only when necessary. Large streams/script bodies are not unbounded JSON columns.

Use existing Tokio runtime, bounded channels and a small worker pool per workload class. Prioritize native permission/reply and control completion over optional Git scans, user scripts and observer rendering. Separate CPU-heavy parsing/process enumeration onto bounded blocking workers. Never hold Store transactions while waiting for Git, a script, a native callback or GitHub.

Configuration declares per-host/project/definition concurrency and fairness. Raising a soft tuning value is possible within granted capacity; protocol byte caps, owner exclusion and external-effect safety remain hard. A slow or malformed source degrades its projection, not the entire swarm.

## 12. Required recovery behavior

| Failure | Required behavior |
|---|---|
| Manager/UI exits | Host monitoring, timers and authorized automation continue |
| Native stream disconnect | Mark source stale; reconcile read-only; no service restart or model duplicate |
| Reboot with queued job | Recheck grant/target; start once if still eligible |
| Reboot after possible effect | Unknown + readback; no blind re-execution |
| Webhook duplicate/out of order | Deduplicate; reconcile exact source object; no repeated audit |
| Hook queue overload | Bounded gap/loss report; observational hook does not wedge native work |
| Script malformed result/timeout | Retain partial evidence and owned-process disposition; isolate this invocation |
| Role revoked or Task superseded | Stop new admissions under stale authority; preserve historical results |
| Rule feedback loop | Suppress duplicate descendant cause, pause affected rule, emit one incident |
| Goal has no progress | Wait/attention with exact missing evidence; no repeated identical prompts |
| Viewer too slow | Lag/resync, not native backpressure |
| Disk pressure | Reduce optional stream retention; never delete active/referenced evidence to fake success |

The system is successful when it makes current work observable and useful automation cheap, without turning hooks, scripts, roles or cron into a second uncontrolled swarm.
