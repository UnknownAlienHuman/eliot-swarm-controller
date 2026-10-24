# Refactoring — source-backed findings and deletion map

Current runtime findings were reconciled through
`c92120ed70ca7e625440a8a19517c481e73b13c9`; active target documents through
`4c052141792b9e12818b9bed9b666b086242b9cc` on 2026-10-22.
[Architecture](architecture.md) owns product law;
[implementation](implementation.md) owns the only work order;
[status](../implementation-status.md) records landed evidence.

This is not an exhaustive audit claim. It records confirmed ownership/code-growth
mechanisms, retained behavior and concrete deletion boundaries. A simplification is
valid only when its automatic positive path remains connected.

## 1. Retained behavior

| Required behavior | Retained owner/mechanism | Obsolete machinery may be removed |
| --- | --- | --- |
| Same Manager reconnect never restarts work | durable client, Task/Attempt/Operation and native readback | chat/link workflow state and marker files |
| GM handover preserves owners/authors/reviewers/candidates/recipients | GM designation/epoch + exact grants | rewriting or adopting historical objects |
| Native-manager delegation never receives a second controller initial prompt | `Attempt.start_owner`, `START_OWNED_BY_MANAGER`, producer correlation | repeated dispatch receipts/proofs |
| Lost response never authorizes prompt/external-write replay | retained Operation/native identity + exact readback | transport retry guesses |
| Harness Goal and Eliot Goal coexist | adapter-native Goal + bounded ordinary scheduled/event Send | vendor-branched Goal workflow/proof packets |
| Agents execute the routine work cycle | one current-state policy loop using ordinary commands | WorkDispatch/review-repair/publication engines |
| Native questions/permissions do not hang or default to deny/first choice | native policy + exact Reply + addressed Governor decision | queued Send as answer and manual relay |
| Corrections return all findings; missing reviewer can be replaced | review result/replacement classes + one request-changes transition | ReviewDisposition/RepairDispatch state |
| Acceptance proves frozen requirements for exact candidate | requirement evidence plan + exact checks/reviews/dependencies | prose “done”, proof copies, legacy policy branch |
| Governor observes native/process/worktree/provider facts locally | concrete source collectors + one joined Store view | optional logs as truth and newest-log/PID heuristics |
| One manager owns mutable Git/worktree; leaf writers are disjoint | Task path scope + workspace lease + manager Git ownership | `code.scope.*` proposal/override system |
| Results survive link/process failure without input replay | exact StoreDelivery + origin Operation/boot readback | memory-only pending receipts/universal journals |
| External writes happen at most once and settle by readback | one effect owner per domain | duplicate GitHub/Forge writers/projection workflow |
| Direct peer work remains available | normalized mailbox + scoped grants + optional tiny Task thread | cards/contracts/integration/Concilium |
| Remote gateway exposes one fixed identity/profile | one loopback gateway + private credential loader | identity broker/proxy/database |

Fixed-age rotation, newest-session ownership, automatic “recommended” answers, silent
model fallback and independent lane daemons are rejected heuristics.

## 2. Landed source reduction and verification truth

- `25a8e3b`: duplicate host CLI/MCP launcher removed; recorded net −1,206 Rust lines.
- `cba98b7`: active Antigravity coordinate aligned on v6.
- `eeea355`: terminal receipt no longer requires Store observation ID or duplicate
  `local_execution_results`; recorded net −265 production Rust lines for that commit.
- `c92120e`: one dispatch producer is updated across SUCCESS/ERROR/CANCELED. Run
  `39579583956`, job `119153571930`, passed the connected terminal regression and
  minimal Store Clippy before publishing it.
- `3c7a84f`: A1b removes the Antigravity refresh/session-proof admission gate and its
  repeated assignment/lease/session digests; run `40131522836`, job `120984044559`,
  passed affected rustfmt and warnings-denied Clippy. Recorded net change is −305
  production Rust lines, −20 qualification-glue lines and +3 test lines.

Still open: crash-unsafe in-memory delivery and extra command/profile surface.
Installed harness/crash/scale behavior is unqualified.

## 3. Repeated causes of growth

### One fact becomes several products

The same decision becomes domain state, JSON receipt, proof packet, workflow state,
cursor family, compatibility profile and fixture. Select one owner and delete the
others rather than moving them.

### Metadata becomes behavior and permission

Artifact version/build, schema inventories, document editions, source checkout,
compiler/cache path and hashes select runtime behavior/authorization. Target behavior
uses authenticated actor/object scope, current revision/generation/boot and semantic
capability. Verify package bytes at install/launch and artifact bytes at storage.

### Receipts echo Store-owned identity

Task/Attempt/revision, request/source/prompt hashes and nested package/protocol receipts
are repeated through dispatch/outcome/result. Keep stale generation/boot guard, native
identity/disposition and integrity of actual immutable bytes.

### Adapter persistence becomes universal journals

Claude, Codex, Command, Muse and OpenCode persistence mixes native intent, Eliot
delivery, results, observations, callbacks, startup inventory, tombstones and
compaction. Use actual lifetimes only:

```text
SessionRecord
NativeEffectRecord
StoreDelivery
ObservationCheckpoint
persisted/readable result when required
```

Do not extract another shared event-sourcing framework.

### Parallel executors survive every migration

| Runtime | Current duplication | Selected target |
| --- | --- | --- |
| Antigravity | Rust + retained Node profiles | one Rust route after A1b–A3 |
| OpenCode | built-in host + standalone Rust | one exact live-qualified standalone route |
| Codex | Rust + retired Python/profile history | one Rust app-server route |
| Muse | active Node + disabled custom Rust | simplify Node `.13`; retire custom Rust |
| Claude | Rust/SDK + retained Node generations | one Rust adapter with selected official SDK driver |
| Command | Batch profile ladder + ACP generations | one Batch + one stable ACP v1 |

Completed history does not keep an executor or translator alive.

<a id="code-reduction-candidates"></a>
## 4. Confirmed workflow/authority duplication

### WorkDispatch copies Task lifecycle

It retains global/per-entry cursors, pending subjects, activation/catch-up state,
rechecks, semantic slots, links and quarantine. Its bounded pending list can hide later
eligible work. Replace with ranked current query and atomic `dispatch_once`; reuse
suitable sessions according to explicit continuation policy.

### Automation registry hides missing ordinary actions

`AutomationStep` lists ten workflow steps and `has_consumer()` returns true for all,
so `capability_gap()` cannot expose missing shared action semantics.
`supported_action_for()` maps only some steps; ReviewDisposition, RepairDispatch and
Acceptance depend on separate consumers rather than ordinary domain actions.

Target actions:

```text
dispatch | review | return_changes | accept | check | goal_message | script
optional source_import | publish
```

Each queries current state and calls the ordinary handler. Remove old enum value,
consumer, policy/config/schema, worker demand, cursor/slot/link and fixture together.

### Review/correction repeats one candidate relation

`automation_repair.rs` duplicates review/candidate/manager identity in slots, operation
links, entry links, finding packages and digests after review disposition/feedback.
Use one `return_changes`: select all current actionable findings, apply exact
`task.request_changes` and admit one correction Send when owner is live. Preserve
`ReviewReplacementClass::{Unanswered, Inconclusive, ReturnedForCorrection}`.

### Goal progression is both under-specified and over-built

Current `GoalProgressionSettings` contains only `goal_id`. The consumer admits a
continuation for each exact terminal event and retains its own global/per-entry cursor,
activation/catch-up state, pending source gaps, terminal slots, objective hash and
Goal-specific admission/terminal proof types.

It has no selected message/turn/elapsed/cost budget, no candidate/evidence progress
criterion and no safe session-rotation policy. Therefore it can continue indefinitely
while still carrying substantial workflow machinery.

Target:

- native Harness Goal stays adapter-native;
- Eliot `goal_message` is an ordinary Send occurrence;
- policy selects cadence/cooldown and optional occurrence/turn/elapsed/cost/no-progress
  budgets;
- meaningful progress is current candidate/result/check/finding/native work, not a new
  ID or old timestamp;
- exhausted/no-progress policy pauses new input and creates one Governor decision;
- selected rotation drains Goal/children/questions/uncertain effects, retains
  checkpoint/results, then opens/resumes a replacement under a new Operation.

Delete Goal cursor/slot/proof workflow after ordinary occurrence identity and current
source facts replace it. Do not impose fixed-age rotation or a universal turn cap.

### Dependency acceptance is hard-coded into claim

`task.claim` currently calls `acceptance::resolve_dependency` for every Task dependency
before inserting a new Attempt. This protects acceptance truth but also prevents
research/preparatory implementation when the dependency map is stale or merely an
integration dependency.

Selected target separates start policy from acceptance truth:

- default automatic `hold` keeps unresolved dependencies pending;
- explicit policy `allow_preparatory` permits claim/work and exposes unresolved
  dependencies in TaskPacket;
- acceptance/publication always revalidates exact required dependency revision/phase;
- no separate blocked-state workflow or copied dependency ledger is introduced.

This is a target product decision, not a claim that current gate is a coding error.

### `code.scope.*` duplicates workspace authority

It models paths/symbols/interfaces, proposals, modes, override and expiry but does not
own the worktree. Task path prefixes and Attempt lease are the controller write
admission/conflict guard. Preserve role/object grants and manager Git ownership; delete
the advisory product.

### Capacity copies Operation lifecycle

Capacity phases/evidence/refusal rows/meta indexes mirror Operations and count read/
control methods. Reserve unresolved root opens/unreleased root bindings in launch;
provider availability is source observation; capacity report is projection.

### Scheduling is triplicated

Host scheduler, standalone scheduler service and automation loops overlap. One
in-process Store-change/nearest-due/safety loop admits ordinary Operations. Separate
workers remain only for external I/O/custody.

## 5. Task, prompt, review and acceptance findings

### TaskPrompt spreads controller proof through every runtime

`TaskPromptEnvelopeV1` is consumed by Store, batch/zed paths and all major adapters.
Model input includes identity JSON/hash structure while adapters echo it through
contexts/receipts.

Target TaskPacket renders goal, requirements, current initial/continue/correction,
selected sources, dependencies, scope, workspace, route, deliverable and verification.
Store retains identity. Migrate route by route; no flag-day format.

### Verification criteria are not bureaucracy

V2 acceptance freezes `requirement_evidence_plan`; every requirement needs evidence.
Preserve the semantic plan while removing duplicate envelopes/hashes. Render meaningful
criteria for the agent. Do not reduce acceptance to prose confidence or differential
cleanliness alone.

### Manual and automatic acceptance must converge

`store/acceptance.rs::{reserve_with_actor,evidence}` and their asynchronous verification
already share the candidate, frozen-plan, check, dependency and byte gate. Preserve
that working path. Automatic acceptance copies retained assigned-auditor evidence;
the earlier instruction to remove fabricated automatic review arrays was too broad.

Reviewer provenance has not converged: direct acceptance supplies caller-authored
requirement reviews, while assigned-review/empty-findings checks live in automatic
preflight. The common feedback cursor checks freshness, not finding resolution.
Resolve required reviews and finding dispositions in the common gate; Manager/Governor
supplies decision/reason, not another auditor result. Candidate B needs B's evidence.
The [current trace](audit-register.md#trace-2026-10-23-review-acceptance) records these
preserved checks and the distinct missing automatic replacement/progress paths.

## 6. API, Store and frontend findings

### Public API ownership is fragmented

Method registry, MCP catalog/profile/schema, request validation, Store routing, role
checks and visibility SQL each encode the surface. A retained domain owns typed parse,
exact role/object/revision authorization and handler. Public, internal-worker and
adapter protocols separate. Removing a product removes all routes/policy/storage.

An active profile doc still claimed an already-fixed operator-only Task defect; it was
removed in `3e4bf9c`. Stale defect text causes agents to add unnecessary grants.

### Store evolution protects SQL text

Base SQL SHA, extension digests, reference DB and DDL-token checks protect historical
source. Use application ID, numeric `user_version`, ordered transactional migrations
and bounded postconditions. Migrate concrete retained data only.

### MCP underuses multiplexing and duplicates wakes

`swarm-client::Client` correlates concurrent requests while MCP retains a connection
mutex across calls. Clone current client and release mutex before RPC. Replace
per-subscription poller/forwarder/queue stacks with one coalesced wake pump; Store
history is replay authority.

### Hot reads perform broad work

`tasks/get` pages global attention for exact pending input. Mailbox scans global
observation timeline. Native in-memory ring operations run through Store writer. Add
exact addressed reads and move ring work outside writer after short authorization.

## 7. Governor observation findings

Current inventory begins at ready bindings. Unmatched sessions/processes and surviving
shared services can disappear. Process metrics is a separate selected Windows CLI and
repeats executable verification. Workspace lease shows launch-time facts, not current
branch/HEAD/dirty. Remote readback is attached to Forge. Usage/conditions are binding/
artifact oriented.

Target uses concrete provider/process/worktree/remote sources with epoch/cursor/
freshness/coverage, current facts and assignment edges in one WAL projection. Ordinary
reads invoke no provider/model/filesystem/network call. Do not build a generic entity
graph or telemetry warehouse.

## 8. Mailbox and coordination findings

Messages live in settled Operation JSON; `message.read` scans observations. One message
may have request ID, Operation ID, `cmsg-*` ID and random delivery ID. Reply/cancel uses
JSON indexes and corruption sentinels.

Normalize recipient rows keyed by Operation/message ID. Preserve sender/recipient,
Task/Attempt/thread, reply/cancel and cursor. Same client reconnects; GM handover never
rewrites historical recipients.

Delete duplicate participant directory, cards, consult vocabulary, speech-act thread
workflow, watches, contracts/ratification, integration ACK, Concilium and code scope
after callers move. Preserve base authentication, scoped worker/reviewer access,
direct peer mail, review and optional tiny grouping thread. Define automatic wake/
resume so simplification does not become human relay.

## 9. External integration and gateway findings

### GitHub

Current poll wraps GET in Operation/lease/recovery, pages mutable `updated desc` and in
one transaction stores facts, mutates Tasks and updates separate work pool. API writes
have direct-label and Forge-PR owners; automatic projection adds workflow state.

Separate collector, policy import, one GitHub API effect owner, Forge remote Git and
independent remote-ref observation. Enabled policy may import/publish automatically;
removing projection cannot create manual publication work.

### Gateway

Keep loopback-only, exact path, one credential/profile/bearer, Host/Origin/body/header/
request/connection bounds and timeout-as-unknown. Replace plain secret reads with
private bounded loader, cap sessions/idle lifetime and remove compatibility launcher.
One process is one principal; no identity broker/proxy/database.

## 10. Packaging, modularity and documentation findings

Active docs previously contradicted target: full descriptor/schema echo, strict
receipt identity, hashed TaskPrompt, old automation action family, undecided Muse
executor, “manual qualification” and stale Task planner defect. `3e4bf9c` and
`4c05214` corrected the active guidance and added the positive-path invariant.

Package existence does not justify a process or permanent crate. A process is for
native SDK isolation, external I/O/custody or real fault containment. Conditional
donors remain conditional until a connected path uses them and deletes current code.

## 11. Audit gaps

Before final-complete claim:

1. trace every retained public action caller→parse→authorization→Store→effect/readback
   →consumer;
2. include cancellation, restart, late result, transfer and partial-write branches;
3. map every retained historical requirement to current owner/connected behavior;
4. recheck findings after each runtime slice and remove contradictory active docs;
5. execute privilege and retained-format/migration/corruption matrices;
6. qualify installed providers, Windows custody, IPC/Store fairness, mixed routes,
   gateway, autonomous end-to-end cycle and scale.

Do not create another audit framework. Close the matching audit path while implementing
existing vertical slices and keep status honest.
