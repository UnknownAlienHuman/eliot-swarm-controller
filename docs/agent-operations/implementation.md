# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source, active tooling, old Claude Swarm
scripts and current donor sources were rechecked at
`9c6192a70382bb12a842f5b960cab904f95908a7` on 2026-10-19.
[Findings](refactoring.md) preserve evidence; [status](../implementation-status.md)
records completed work. This document says what to change, where to change it and
what old mechanism must disappear.

Do not start another repository-wide framework migration. Implement one complete
caller-to-result slice, delete the replaced path, then use a second real route to
prove any shared abstraction.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Work as vertical slices

Every coding slice states:

- observable behavior;
- exact source owners and symbols;
- unsupported and uncertain cases;
- old callers, formats, fixtures and configuration to delete;
- one relevant donor mechanism or native API;
- minimal format/Clippy/build command;
- connected completion proof and net owned-code change.

The manager owns shared contracts, Store transitions and integration. Leaf workers
receive compact non-overlapping tasks. A type, schema, fixture or adapter stub is not
progress unless a current caller uses it and the superseded path is removed.

Migration is route-by-route. An unmigrated route may temporarily remain on the
current protocol, but receives no new compatibility profile. A migrated route admits
no new work through its old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Shared semantic adapter commands and bounded records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` |
| Adapter process membership and private durable files | `crates/swarm-process::module_owner`, `module_membership`, `permissions` |
| Native transport, session state, provider readers and uncertain native effects | selected `swarm-adapter-*` or `modules/*-rust` implementation |
| Workspace registration, worktree creation and write authority | `crates/swarm-kernel-host/src/store/workspace.rs`; `crate::workspace` performs filesystem/Git effects |
| Queue, route decision and launch admission | current Store launcher owner; no second queue/capacity service |
| Time-based admission | one in-process host control loop; Store owns durable due/occurrence state |
| External effect execution | existing checks, scripts, Forge and adapter owners |
| Process/worktree/Git collection | current host lifecycle/process owners |
| Public CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling Store handlers |

No adapter accepts a Task, changes ownership or interprets review. No common shell
owns a vendor session, native retry policy, scheduler or transcript store.

## 3. First executable route: Antigravity Rust

Antigravity remains the first adapter route because it has one serialized native
input window and a concrete lost-delivery defect. Do not start the common shell with
Command, Codex or Muse: their profile ladders and native state machines would define
the abstraction around the hardest legacy code.

### A0 — synchronize the active Antigravity coordinate

Current source and descriptor use artifact version `6`, while active route/tooling
selectors still name `5`.

Update together:

- `modules/antigravity-rust/route.example.toml`;
- `tools/ci/module-package-policy.json`;
- `tools/qualification/New-NativeQualification.ps1::Get-AdapterContract`;
- `tools/modules/Install-ModuleArtifact.ps1` Antigravity validator;
- active catalog/qualification fixtures selecting new work.

Do not rewrite historical v5 receipts under `docs/remediation`. New installation,
selection and qualification use v6 only. Remove active `"5" | "6"` behavior after
concrete unsettled v5 bindings receive a final disposition.

Completion: descriptor, Rust constant, package policy, installer and qualification
select one active version; no fallback silently chooses v5.

### A1 — remove observation/proof coupling

Current Antigravity terminal outcome is not self-contained. The adapter first emits
`module.observe`, receives a Store `observation_id`, rewrites
`local_execution_ref.observation_id`, then sends `module.outcome`. Store compares the
outcome against a duplicate `local_execution_results` copy in the latest observation.
A durable outbox built on this order would need dependency edges and mutable outcome
revisions.

Make terminal outcome final at the native settle point. It contains only:

- Operation ID;
- binding generation and adapter boot;
- native conversation identity;
- monotonically increasing result ordinal within that conversation/boot;
- terminal native status and Eliot disposition;
- response-content hash and result reference where bytes exist.

Store validates these fields against its retained Operation, binding and boot. The
same terminal result does not need to be duplicated in observation state.

Delete in the same change:

- `local_execution_ref.observation_id`;
- duplicated `local_execution_results` receipts in observations;
- outcome-to-current-observation matching in `runtime/warm_stream.rs`;
- `AntigravityDispatchSessionProof` from `swarm-contracts::runtime`;
- proof production in `modules/antigravity-rust/src/controller.rs`;
- `launcher_dispatch/antigravity_capability.rs` proof lookup/digests;
- refresh-before-dispatch qualification and proof fixtures.

`agent.refresh` remains safe readback. Model, stream phase, counters, gaps and native
readiness remain `agent.state` observations; they do not authorize dispatch.

Completion: a ready current binding receives first input without fresh refresh;
stale Task/Attempt/binding/workspace facts still fail; terminal outcome is immutable
before Store delivery; no proof DTO or observation dependency survives.

### A2 — make Store delivery durable without replaying native input

After A1, implement a route-local delivery spool using existing private durable file
primitives in `swarm-process::permissions`. Do not add a database, append-only
journal, tombstones, compaction or retention graph.

Terminal outcomes and result pages use one private file per already-formed Store
request:

```text
sequence
method = module.outcome | module.result
operation_id
binding generation + adapter boot
exact bounded params
```

Persist before the in-memory owner can forget it. Reconnect may send the same params.
Delete only after exact Store ACK. A replacement boot does not silently adopt or
delete predecessor files. The file never contains a native prompt or process launch.

Observations use at most one replaceable pending checkpoint per source:

```text
source_id + source_epoch
source cursor/sequence
coverage or gap
latest normalized state
```

A newer unsent checkpoint replaces an older one. Store creates the durable
observation journal after accepting it. Do not create one local file per native
frame.

Edit:

- `modules/antigravity-rust/src/controller.rs`;
- `modules/antigravity-rust/src/main.rs`;
- one small route-local spool module;
- focused Antigravity tests;
- only required `swarm-process` exports.

Completion:

- native input is written once;
- Eliot disconnect after terminal settlement leaves one durable exact envelope;
- same boot reconnect sends identical params and ACK removes them;
- restart never converts the envelope into native execution;
- predecessor-boot records remain visible for explicit disposition;
- result pages remain attached to the original Operation;
- observation bursts remain bounded by one checkpoint per source.

### A3 — give Antigravity only its real semantic actions

The retained route supports:

```text
Open
Dispatch
Send(next_turn)
Refresh
Result
Reconcile
Shutdown/release of an explicitly owned process or session
```

Do not advertise or implement placeholder branches for `Reply`, `Configure`, `Goal`,
`Background`, `Recover` or live steer. Unsupported actions fail at Store routing
before `module.next`.

Replace string method switching with the typed semantic command for this route.
Collapse per-outcome receipt identity to facts Store cannot derive:

- Operation ID;
- binding generation/adapter boot;
- native conversation/input/result identity when observed;
- disposition and bounded action details.

Keep hashes on immutable result/artifact bytes. Remove artifact version, protocol,
request hash and repeated Task/source hashes from every outcome.

Completion: adding/removing an optional action changes one capability value and one
adapter match, not artifact profiles, schema families or Store vendor branches.

## 4. Queue, route policy and current-state admission

<a id="work-dispatch"></a>

The current `store/automation_work_dispatch.rs` is another workflow engine. It tails
Task observations, owns global/per-entry cursors, copies blocked Tasks into a bounded
pending list, periodically rechecks them, creates semantic slots, retains operation
links and quarantines source/config records. `automation/work_dispatch.rs` duplicates
launch route/profile/model/effort/budget/stop-condition planning.

The target is a current-state query, one admission transaction and no copied pending
state.

### B0 — one queue projection

Keep `swarm.queue.get` as a read projection in the existing launcher/Store owner. It
reads current:

- Task revision/state and current Attempt;
- review/change-request state;
- dependencies and acceptance state;
- active workspace leases/write conflicts;
- selected route capability, provider state and root capacity;
- current binding/session availability.

Return deterministic rank and `why_ranked`:

1. current candidate returned for correction;
2. current unfinished Attempt needing continuation/relaunch;
3. ready unclaimed Task;
4. explicit manager/task phase ordering;
5. oldest eligible Task, then stable Task ID.

Do not infer “small” from prompt length or code heuristics. A queue page is not a
reservation. Every launch rechecks current facts.

### B1 — route policy is explicit configuration, not a lane entity

Replace the single duplicated WorkDispatch launch bundle with a small manager policy:

```text
automation entry
  enabled
  project/task selector
  ordered route aliases
  optional agent profile
```

A route alias owns exact harness, native options, model/effort defaults and root
limit. Participant/MCP profile is derived from the admitted role; the workspace
policy is the single manager-owned worktree policy. Task requirements own the work,
not another copied budget/stop-condition packet.

The ordered route list is explicit. Automatic dispatch chooses the first currently
eligible alias and stores that exact alias in the launch Operation. It never changes
provider, model, effort or permissions by inference. Manual launch names one alias.

A “lane” is only the view of:

```text
manager + project + automation entry
+ ordered routes
+ current queue
+ provider/capacity state
+ active bindings/workspaces
```

Do not add a lane table, process, cursor, inbox, state machine or scheduler.
Disabling the entry stops future admissions and does not cancel active work.

### B2 — simplify provider state

Replace `NativeUsageSnapshot`/`ProviderConditionFact` proof matrices with two
separate normalized records:

```text
ProviderUsage {
  source_key, source_epoch, source_sequence
  observed_at, freshness, coverage
  scope = account | service | connection
  native-labelled buckets/windows/credits
}

ProviderAvailability {
  same source coordinates and scope
  available | rate_limited | quota_exhausted | overloaded
  | auth_required | data_policy_required | model_gone | unknown
  optional retry_at/reset_at
  optional native_class
}
```

The adapter owns native method names, rolling-patch folding and provider-specific
fields. Store validates authenticated source, bounds and monotonic source sequence.
Do not repeat route, binding, generation, model and auth hashes in every row.

Source scope rules:

- account scope only when the provider supplies stable account identity;
- service scope only when the service identity is explicit;
- otherwise connection scope; never merge across connections;
- a productive binding may report the fact but is not the durable identity of an
  account/service condition.

Decision rules:

| Fact | Automatic admission |
| --- | --- |
| No fact ever observed | permit one bounded probe under root capacity |
| Fresh `available` | eligible |
| `rate_limited` / `overloaded` before retry | hold until retry time |
| `quota_exhausted` before reset | hold until reset |
| Transient fact after retry/reset | permit one probe; its admitted root prevents a stampede |
| `auth_required` / `data_policy_required` / `model_gone` | unavailable until source/config changes or explicit configured fallback |
| Fresh `unknown` | hold automatic dispatch; manager may explicitly probe |
| Stale/gapped source with no applicable hard negative | hold automatic dispatch; manager may explicitly probe |

A retry/reset timestamp is not proof of recovery. It only opens one probe. A dead
source binding must not turn an old transient condition into a permanent hold.

Use current Codex semantics honestly: `ordinary_usage_allowed = null` is unavailable,
not recovered. Muse usage is point-in-time “as of” data, not live truth.

Delete:

- `UsageService::{Codex,Muse}` as a shared allowlist;
- native method allowlists from shared DTOs;
- `auth_context_ref` and `details_digest` as admission proofs;
- artifact-version source allowlists;
- Codex-specific permission fields in the universal DTO;
- repeated route/binding/model columns;
- per-binding copies of one account/service snapshot.

### B3 — capacity is a transactional root count, not a second lifecycle ledger

The current capacity ledger copies each Operation/producer into reserved/active/
released phases, event references, unknown timestamps and `meta` indexes. It even
counts `refresh`, `result` and `reconcile`, which perform no model execution.

For the compact product, keep only the configured route root limit.

A root claim is derived from current Store facts:

- one unresolved `agent.open`/launch that may create a root;
- one unreleased binding/native root;
- never both for the same parent launch/binding;
- rejected/cancelled opens and released bindings do not count.

Count and reserve in the same Store writer transaction that admits the launch/open.
The single writer makes the limit race-free. A recheck excludes its own exact
Operation/binding.

`task.dispatch`, `agent.send`, `reply`, `configure`, `goal`, `refresh`, `result`,
`reconcile` and `recover` do not create another root claim. Adapter-native
serialization and provider-native limits govern work inside an existing root.

Delete after callers switch:

- `CapacityLedger`, `CapacityEntry`, `ResourceEvidence` and phase merge code;
- `capacity:*`, `capacity-entry:*` and refusal meta records;
- v1/v2 capacity storage migration and encoded key helpers;
- per-operation execution identity/event proof rows;
- sync hooks from every Operation lifecycle transition.

`report.capacity` becomes a bounded view over routes, current root claims, provider
state and host resource facts.

### B4 — `dispatch_once`, not observation replay

Replace WorkDispatch reconciliation with:

```text
dispatch_once(automation_entry)
  -> query ranked current Tasks
  -> test a small bounded page
  -> choose first eligible configured route
  -> atomically admit one launch
  -> return Operation or current blocker reasons
```

The admission transaction:

1. validates manager/project/Task revision;
2. creates or reuses current Attempt;
3. reserves workspace/write lease;
4. validates selected route/provider/root capacity;
5. creates launch Operation with deterministic request identity.

External filesystem, Git and native I/O occur after commit through existing owners.

Delete:

- `GlobalWorkDispatchCursor`;
- `WorkDispatchState`, `PendingSubject` and pending rechecks;
- activation/catch-up cursors and `MAX_PENDING_SUBJECTS`;
- semantic launch-slot and duplicate operation-link records;
- WorkDispatch quarantine for ordinary blockers;
- preview-digest binding in automated launch;
- duplicated launch settings replaced by route aliases/current policy.

Wake notifications merely cause another query; a safety wake recovers lost/coalesced
notifications. A blocked Task never stalls a cursor behind it.

Completion: 128 blocked Tasks do not hide a later eligible Task; Task revision needs
no pending-copy repair; same Attempt cannot create two launches; fallback is explicit;
disabling dispatch admits nothing new and cancels nothing active.

## 5. Collapse advisory code scopes into workspace write leases

Current `code.scope.*` is a separate product with proposal digests, revisions,
overrides, expiry, paths, symbols, interfaces, indexes and overlap pages. Its own
source states that it does not lock files, inspect Git or start work. Actual launch
already reserves a workspace lease and calls `reject_scope_conflicts`.

### C0 — simple scope semantics

- `TaskSpec.scope.initial_paths` → repository-relative write prefixes;
- `TaskSpec.scope.forbidden_paths` → hard negative prefixes;
- absent/empty initial paths → repository root;
- review-only work → no write lease.

Normalize separators and reject absolute paths, traversal, empty segments and `.git`.
Use path-segment prefix comparison. `src/a` overlaps `src/a/file.rs`; it does not
overlap `src/ab`.

Symbols/interfaces/globs are guidance, not concurrency authority. Remove
`allowed_symbols` from the lease after callers migrate.

### C1 — one launch-time decision

`reserve_lease_for_launch` checks overlap in the same transaction as Attempt/launch
admission. Another active lease in the project with overlapping path prefixes blocks
automatic launch and names the conflicting Task/Attempt/paths.

Do not add a generic override. Resolve intentional overlap by revising scope,
settling/releasing one Attempt or assigning a non-overlapping slice.

The lease remains while the candidate can change and releases when the Attempt is
accepted, abandoned or superseded, or after an explicit settled stop.

Delete:

- `coordination/code_scope.rs`;
- `store/code_scopes.rs`;
- `code.scope.*` CLI/MCP/method-policy/catalog entries;
- proposal digest, broad-scope acknowledgement, override and expiry;
- code-scope references in removal-target contract/ratification engines.

## 6. Simplify process ownership and recovery

Current `swarm-process::module_owner` already uses owner-first Job/process-group
custody. Keep it. CCCC's suspended-child primitive is useful only for a future proven
child-first race; do not import it for this path.

### D0 — one process owner record

Persist only:

- binding/generation and adapter boot;
- state directory and credential reference;
- selected executable;
- PID plus OS birth/handle identity;
- Job/process-group identity;
- parent/shared-service relation.

Artifact version, build ID, protocol and full module-contract JSON remain
installation/inventory facts. Verify bytes once at install/launch; authenticate
ModuleLink; use binding/generation/boot for runtime ownership.

Simplify `ModuleOwnerPlan` and `ProtectedResolverMap`. Keep path containment,
no-link traversal, private permissions and exact credential scope.

### D1 — conservative restart reconciliation

- dead process → clear live record;
- PID exists but birth/image/owner differs → clear record, kill nothing;
- process cannot be inspected → retain `unknown`, retry later;
- exact Eliot-owned leftover → route policy chooses recover/observe/terminate;
- shared provider service → never terminate because one session disappeared.

Do not sweep by executable name, cwd, title, newest log or age. Do not hash the
executable on every metric sample.

## 7. One scheduling/admission loop, not scheduler products

<a id="automation-and-delivery"></a>

The repository currently contains:

- `scheduler.rs`, an in-process timer handling static CheckRun schedules, automation
  cron and Goal due state;
- `store/automation_scheduler.rs` plus `swarm-automation-worker`, a second process
  that pages/digests/admit the same due sources through its own credential, owner,
  generation and process lifecycle;
- `supervise_automation`, another two-second loop for automation actions/watches;
- separate two-second launcher/native-MCP loops and an eleven-slot legacy worker
  coordinator.

### E0 — one current schedule model

Use manager-owned automation entries as the retained schedule model. A schedule is:

```text
entry_id
enabled
trigger = event | interval | cron
typed retained action/settings
last admitted occurrence
next due time
```

Keep typed actions; do not create a generic Store-method workflow language. Convert
the operator `Config.schedules` CheckRun form into equivalent automation entries, then
delete `ScheduleConfig` and `schedule_registry:v1`.

Eliot Goal reminders use the same interval/cron occurrence mechanism but still emit
ordinary `Send` Operations. Harness Goal remains native and independent.

### E1 — one in-process control loop

Create one host-owned admission loop in existing host/scheduler code:

```text
wait for Store-change notification or nearest due time
  -> reconcile due schedule occurrences
  -> dispatch_once for enabled work policies
  -> reconcile review/repair/acceptance and watches
  -> compute next due time
```

All passes are bounded and commit ordinary Operations. They perform no native,
filesystem, Git or network I/O. A slow safety wake (for example 60 seconds) recovers a
lost/coalesced notification; no domain needs its own two-second poll.

Failure in one malformed subject is returned as that subject's visible gap/quarantine
where already supported. SQLite/kernel failure stops the loop. Do not invent a
generic supervisor DSL or per-domain cursor framework.

### E2 — delete the standalone scheduler service

Delete after occurrence parity:

- `crates/swarm-automation` worker executable/config/credential logic;
- `store/automation_scheduler.rs` page/admit service;
- declared automation-scheduler service scope and registration;
- scheduler owner/generation/config SHA/process group/readiness protocol;
- `AutomationScheduler` slot and demand flag;
- R48 scheduler qualification and service-specific remediation fixtures;
- duplicate due snapshots, source digests and scheduler quarantine used only by that
  service.

Keep one in-process timer owner. Store retains due/occurrence rows and idempotent
Operation admission.

### E3 — shrink optional workers to actual effect owners

After route migrations and scheduler consolidation:

- remove built-in OpenCode/Zed workers when their standalone/current routes own work;
- remove native-MCP workers when MCP setup becomes adapter-internal;
- remove Scheduler/AutomationScheduler/Automation slots replaced by the control loop;
- keep checks, scripts, Forge, launcher and adapters only while they perform real
  external effects or recover such effects.

A worker lifecycle is justified by isolated external I/O/custody, not by a timer or
a Store query.

## 8. Extract shared adapter plumbing only after proof

<a id="processes-and-frontends"></a>

Use Antigravity plus one second small current Rust route to prove common code. Claude
`OwnedBootstrap` is a bootstrap candidate; it is not automatically a delivery-spool
candidate.

Extend existing owners rather than creating an adapter-framework crate:

- `swarm-process::module_owner` returns verified binding/generation/boot, state dir,
  client/credential and selected protocol/capabilities;
- `swarm-client::ModuleLink` owns Eliot connect/hello, command pull and exact
  redelivery of already retained Store messages.

The shell does not own native session state, native command queue, result association,
uncertain-effect readback, Tasks, timers or scheduling.

Extract only when two routes adopt it in the same change and delete their old env
parsing, hello equality checks and outer delivery loops.

## 9. Typed semantic adapter protocol

Target closed commands:

```text
Open
Dispatch
Send
Reply
Configure
Goal
Background
Cancel
Recover
Refresh
Result
Reconcile
Shutdown
```

A route declares supported actions and scopes. Unsupported actions fail before native
I/O. Vendor-only operations remain private.

Remove public/common internal names:

- `native.opencode.loop_step` → `Send { steer }`;
- `native.command.cancel_turn` → `Cancel { turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter cleanup;
- `native.mcp.*` → OpenCode adapter setup/observation.

`Recover` remains explicit for known-session restoration without input replay.
`Reconcile` remains readback for an uncertain effect.

Simplify hello to one current protocol major plus capability subset. Delete artifact
version/schema profile ladders and full descriptor echo/comparison.

Simplify outcomes to Operation ID, generation/boot, disposition, native IDs and
bounded details. Do not repeat package/protocol/request hashes. Keep content hashes on
actual immutable bytes.

## 10. Retain one executor per harness

- OpenCode: standalone Rust route; remove built-in host executor, legacy demand and
  public MCP phases.
- Codex: current Rust app-server route; remove Python executor/SDK/fixtures after
  concrete old work is disposed.
- Muse: choose Node or Rust by complete behavior and total maintained Rust+JS+glue;
  delete the other.
- Claude: one Rust adapter with thin supported SDK driver; delete retained Node
  generations.
- Command: one current Batch profile plus one current ACP generation; remove profile
  ladders.
- Antigravity: one current warm-stream implementation after A0–A3.

Historical records do not keep an executor alive.

## 11. Preserve Harness Goal and Eliot Goal

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

Harness Goal maps through adapter `Goal` and is default where supported. Native
set/resume may start a turn; associate it with the initiating Operation.

Eliot Goal becomes a small automation action:

```text
due/event occurrence -> ordinary Send message -> one Operation
```

Remove provider branches, OpenCode pseudo-Goal continuation, objective approval
hashes and Goal-specific continuation proof packets. Replaying one occurrence returns
the same Operation; a later occurrence may intentionally repeat text. Disabling the
plugin, pausing native Goal, cancelling a turn and clearing queued input are
independent.

## 12. Passive observation and quotas

Adapters fold native events and read provider-local JSON/JSONL/SQLite. Host adds OS
process and Git facts. Store joins Task/Attempt/Operation, binding, native session,
process, worktree, branch/HEAD, remote ref, source cursor/gaps and provider usage.

Configured readers run without a Task/binding. Unmatched sessions remain inventory,
not assigned work. `agent.list/state/family/usage` read Store without provider calls.

Use selective `sysinfo` refresh and exact OS birth identity. Remove per-sample
executable hashing and small diagnostic fleet limits. Shared services and account
quota appear once, not once per session.

Provider readers are byte/record/time bounded. JSONL cursors stop at complete
records; SQLite uses short read-only WAL-visible connections. Source failure records a
gap, not an empty fleet.

Fix startup connection reset for every unreleased binding. Preserve source identity
in follow events. A late source sequence may enter history without replacing newer
current state.

## 13. Launch, results, review and Store reduction

Make launch preview optional. Store resolves one launch plan under the Operation;
remove caller-supplied plan hashes. First dispatch requires current assignment,
workspace lease, binding and usable session—not MCP readiness.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Move independent review/check evidence into the common acceptance gate for the exact
candidate. Candidate B cannot reuse A's review.

Keep one Store writer. Move pure bounded reads to existing WAL readers. Do not hold a
transaction across external I/O or pagination.

Replace base-SQL text SHA with application ID, schema version and explicit migrations.
Move retained indexed identity/state/due fields out of `meta`; bounded payload JSON
may remain in its owning row. Do not add an ORM/EAV store.

Give automation an immutable `entry_id`; transfer updates owner/revision in one
transaction. Delete successor copies, journal relocation, transfer chains and cycle
logic.

Remove from compact-core completion:

- Concilium/council state;
- generic integration ratification/ACK;
- automatic publication/Issue-label projection;
- document-edition rights and legacy acceptance behavior;
- retired installers/provenance equality/duplicate validators;
- host dependencies on worker implementation crates after contracts move.

<a id="store-and-current-data"></a>
<a id="identity-and-hashes"></a>

## 14. Current format and existing data

<a id="current-format-and-existing-data"></a>

Backward compatibility is not a product feature. Before deleting a protocol,
executor or storage path, enumerate concrete unsettled Operations and required
results. Finish/read back with the current owner, export the result, or create a new
Task revision/Attempt. Then remove producer, reader, schema branch, config, fixture
and package together.

Do not write generic converters for hypothetical old data. Completed history may
remain opaque when current projections already hold required facts; it does not keep
runtime code alive.

## 15. Verification

<a id="verification"></a>

Run scoped checks for the changed boundary.

| Boundary | Required proof |
| --- | --- |
| Antigravity A0–A3 | v6 is the sole active coordinate; first dispatch needs no proof refresh; terminal outcome survives Eliot disconnect; no native replay; removed proof/handlers have no callers. |
| Queue/route | deterministic `why_ranked`; blocked Tasks do not stall later work; fallback uses only configured aliases; no silent model/provider change. |
| Provider state | first unobserved probe works; transient hold opens one probe after time; hard negatives persist until source/config change; stale source is visible, not permanent hidden denial. |
| Capacity | concurrent root admissions cannot exceed limit; existing root rechecks exclude self; readback/result/refresh create no claims; old ledger/meta keys are gone. |
| Workspace scope | overlapping writers cannot launch; disjoint prefixes can; review-only has no write lease; `code.scope.*` is absent. |
| Scheduling | one timer/admission loop; restart/lost wake recomputes due work; same occurrence admits once; standalone scheduler process/credential/projection is absent. |
| Adapter protocol | two current adapters use typed commands/common bootstrap; rights still reject unauthorized calls; optional action changes no Core artifact profile. |
| Results/review | exact input/result attribution; stream gap stays unknown; manual/automatic acceptance use same candidate-specific evidence. |
| Deletion | no config, package, CI, docs or fallback wrapper for removed paths; report net owned Rust+JS+glue separately from tests. |

Documentation changes run content/link checks. Runtime slices start with formatting
and minimal affected warnings-denied Clippy/build checks. Focused connected tests run
when the slice closes; broad qualification follows the complete product path.
