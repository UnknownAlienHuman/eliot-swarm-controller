# Eliot Swarm Controller — Implementation

This is the only implementation program for the
[architecture](architecture.md). Runtime source was rechecked at
`e6b03b72ccec03b378c70b883f51ba2f34fde8d2`; audit/status documentation was
updated through `aeb9c2a4c7f00d609cd7b1e3c176362088d42371` on 2026-10-19.
[Findings](refactoring.md) retain source evidence; [status](../implementation-status.md)
records what actually landed.

Do not start another repository-wide framework migration. Complete one vertical
caller-to-result slice, delete the replaced path, and use a second real consumer to
prove any shared implementation.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Working rules

Every coding slice states:

- observable behavior;
- exact source owners and symbols;
- unsupported and uncertain cases;
- old callers, records, config, fixtures and branches to delete;
- one relevant donor mechanism or native API;
- minimal affected format/Clippy/build command;
- connected completion proof and net owned-code change.

The manager owns shared contracts, Store transitions and integration. Leaf workers
receive compact non-overlapping tasks. A type, schema, fixture or adapter stub is not
progress unless a current caller uses it and the superseded path disappears.

Migration is route-by-route and domain-by-domain. An unmigrated route may temporarily
stay on its current protocol, but receives no new compatibility profile. A migrated
route admits no new work through its old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, review, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Current semantic adapter commands and bounded records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` |
| Adapter membership, exact process custody and private durable files | `crates/swarm-process::{module_owner,module_membership,permissions}` |
| Native transport, session state, provider readers and uncertain native effects | selected `swarm-adapter-*` or `modules/*-rust` implementation |
| Workspace registration, worktree effects and write authority | `store/workspace.rs` plus `crate::workspace` |
| Queue, route choice and launch admission | existing Store launcher owner |
| Time-based admission | one in-process host loop; Store owns due/occurrence state |
| Checks, scripts, Git/Forge and GitHub effects | existing dedicated effect owners |
| Public CLI/MCP | `swarm-cli` and `swarm-mcp`, calling Store handlers |

No adapter accepts a Task, changes ownership or interprets review. No common shell
owns a provider session, native retry policy, scheduler or transcript store.

## 3. First executable route: Antigravity Rust

Antigravity remains first because it has one serialized native input window and a
proven lost-delivery boundary. Do not define shared plumbing from Command, Codex or
Muse first: their historical profiles and native state machines would infect the
abstraction.

### A0 — one active Antigravity coordinate

Current Rust and descriptor use artifact version `6`; active route/tooling selectors
still contain `5`.

Update together:

- `modules/antigravity-rust/route.example.toml`;
- `tools/ci/module-package-policy.json`;
- `tools/qualification/New-NativeQualification.ps1::Get-AdapterContract`;
- `tools/modules/Install-ModuleArtifact.ps1` Antigravity validator;
- active catalog/qualification fixtures used for new work.

Do not rewrite historical v5 receipts. Give concrete unsettled v5 bindings a final
disposition, then remove active `"5" | "6"` behavior.

Completion: source, descriptor, route, installer, package policy and qualification
select v6 only.

### A1 — terminal outcome independent of observation/proof

Today Antigravity first copies a terminal result into `module.observe`, receives an
`observation_id`, rewrites the pending outcome and then sends `module.outcome`. Store
compares the outcome against the duplicated observation result. A durable queue built
on this order would require mutable records and dependency edges.

Make the terminal outcome final at native settlement. Retain only:

- Operation ID;
- binding generation and adapter boot;
- native conversation and result ordinal;
- native terminal status and Eliot disposition;
- result reference and content hash when bytes exist.

Store validates those facts against its retained Operation/binding/boot.

Delete in the same slice:

- `local_execution_ref.observation_id`;
- duplicated terminal results in observations;
- outcome-to-latest-observation matching in `runtime/warm_stream.rs`;
- `AntigravityDispatchSessionProof` and its constants;
- proof production in `modules/antigravity-rust/src/controller.rs`;
- `launcher_dispatch/antigravity_capability.rs` proof validation/digests;
- refresh-before-dispatch qualification and proof fixtures.

`agent.refresh` remains readback. Model, stream, counters and gaps remain ordinary
`agent.state` observations, not dispatch authority.

### A2 — durable Store delivery, never native replay

After A1, add a route-local spool using existing private atomic file primitives in
`swarm-process::permissions`. Do not add a database, append-only journal, tombstones,
compaction or retention graph.

Terminal outcomes/result pages use one private file per already-formed Store request:

```text
sequence
method = module.outcome | module.result
operation_id
binding generation + adapter boot
exact bounded params
```

Persist before the in-memory owner can forget it. The same boot may resend identical
params after Eliot IPC reconnect. Delete only after exact Store ACK. A replacement
boot does not silently adopt predecessor records. The file never contains native
prompt/process intent.

Observations use one replaceable pending checkpoint per source:

```text
source_id + source_epoch
source cursor/sequence
coverage or gap
latest normalized state
```

A newer unsent checkpoint replaces an older one. Store creates durable observation
history after accepting it.

### A3 — only real Antigravity actions

Retain:

```text
Open
Dispatch
Send(next_turn)
Refresh
Result
Reconcile
Shutdown/release of an explicitly owned resource
```

Do not advertise placeholder `Reply`, `Configure`, `Goal`, `Background`, `Recover`
or live steer. Unsupported actions fail before `module.next`.

Replace string switching with the typed semantic command. Collapse outcome identity
to Operation, generation/boot, observed native IDs, disposition and bounded details.
Remove package/protocol/request and repeated Task/source hashes from outcomes.

## 4. Queue, route policy and current-state admission

<a id="work-dispatch"></a>

`store/automation_work_dispatch.rs` is a second workflow engine: global/per-entry
cursors, activation cuts, copied blocked Tasks, recheck rotation, semantic slots,
operation links, quarantine and recent history. The target is a current-state query
and one admission transaction.

### B0 — one queue projection

`swarm.queue.get` reads current:

- Task revision/state and current Attempt;
- correction/review state;
- dependencies and accepted prerequisites;
- workspace leases/write conflicts;
- route capabilities, provider availability and root capacity;
- binding/session availability.

Return deterministic rank plus `why_ranked`:

1. current candidate returned for correction;
2. unfinished Attempt needing continuation/relaunch;
3. ready unclaimed Task;
4. explicit manager/task phase order;
5. oldest eligible Task, then stable Task ID.

A page is not a reservation. Every launch rechecks current facts.

### B1 — explicit route policy; lane remains a view

One manager policy contains:

```text
automation entry
enabled
project/task selector
ordered route aliases
optional agent profile
```

A route alias owns exact harness, native options, model/effort defaults and root
limit. Automatic dispatch chooses the first eligible configured alias and stores it
in the launch Operation. It never infers another provider/model/effort/permission.

A lane is only a view of manager + project + policy + ordered routes + current queue +
provider/capacity state + active bindings/workspaces. Do not add a lane table,
process, cursor, inbox or scheduler.

### B2 — source-scoped provider state

Replace provider proof matrices with:

```text
ProviderUsage {
  source_key, source_epoch, source_sequence,
  observed_at, freshness, coverage,
  scope = account | service | connection,
  native_labelled_buckets
}

ProviderAvailability {
  same source coordinates and scope,
  available | rate_limited | quota_exhausted | overloaded |
  auth_required | data_policy_required | model_gone | unknown,
  optional retry_at/reset_at/native_class
}
```

Adapters own native method names and rolling-patch folding. Store checks authenticated
source, bounds and monotonic source sequence.

Rules:

- account scope requires stable native account identity;
- service scope requires explicit service identity;
- otherwise retain connection scope and never merge across connections;
- no fact permits one bounded probe under root capacity;
- transient negative holds until retry/reset, then permits one probe—not recovery;
- hard negative requires source/config change or explicit fallback;
- unknown/stale/gapped holds automation but remains manually probeable.

Delete `UsageService::{Codex,Muse}`, native-method/artifact allowlists,
`auth_context_ref`, `details_digest`, Codex-only universal fields and per-binding
copies of account/service facts.

### B3 — capacity from actual roots

Capacity currently mirrors every Operation/producer into reserved/active/released
records and counts read/control methods. Keep only configured route root limits.

A claim is:

- unresolved launch/open that may create a root; or
- unreleased binding/native root;
- never both for the same parent.

Count and reserve in the same Store writer transaction that admits launch/open.
Dispatch, send, reply, configure, Goal, refresh, result, reconcile and recover create
no root claim.

Delete `CapacityLedger`, `CapacityEntry`, `ResourceEvidence`, capacity meta keys,
v1/v2 migration/key helpers, refusal rows and lifecycle sync hooks. `report.capacity`
becomes a projection.

### B4 — `dispatch_once`

```text
dispatch_once(policy)
  -> query a bounded ranked page
  -> test candidates against current facts
  -> select first eligible configured route
  -> atomically admit one launch
  -> return Operation or blocker reasons
```

The transaction validates manager/project/Task revision, creates or reuses Attempt,
reserves the write lease, validates route/provider/root capacity and creates the
launch Operation. External I/O follows after commit.

Delete WorkDispatch cursors, `PendingSubject`, catch-up state,
`MAX_PENDING_SUBJECTS`, semantic launch slots, duplicate links, ordinary-blocker
quarantine, mandatory preview digest and copied launch settings.

## 5. One write authority

### C0 — simple Task scope

- `TaskSpec.scope.initial_paths` → repository-relative write prefixes;
- `forbidden_paths` → hard negative prefixes;
- empty initial paths → repository root;
- review-only work → no write lease.

Normalize separators; reject absolute paths, traversal, empty segments and `.git`.
Use path-segment prefix comparison. Symbols/interfaces/globs remain guidance only.

### C1 — workspace lease is authority

`reserve_lease_for_launch` checks overlap in the same transaction as Attempt/launch
admission. Another active overlapping project lease blocks automatic launch and names
the conflicting Task/Attempt/paths.

No generic override. Resolve overlap by revising scope, settling/releasing an Attempt
or assigning a disjoint slice. Keep the lease while the candidate can change.

Delete `coordination/code_scope.rs`, `store/code_scopes.rs`, `code.scope.*` public
methods/catalog/schema, proposal digest, broad-scope acknowledgement, override,
expiry, symbol authority and references from removal-target contract engines.

## 6. Minimal process custody

### D0 — one owner record

Persist only:

- binding/generation and adapter boot;
- state directory and credential reference;
- selected executable;
- PID plus OS birth/handle identity;
- Job/process-group identity;
- parent/shared-service relation.

Package version/build/protocol/full module contract remain install/inventory facts.
Verify bytes once at install/launch; authenticate ModuleLink; use
binding/generation/boot for runtime ownership. Simplify `ModuleOwnerPlan` and
`ProtectedResolverMap` while retaining path containment, no-link traversal and
private permissions.

### D1 — conservative restart reconciliation

- dead process → clear live record;
- PID exists but birth/image/owner differs → clear record, kill nothing;
- uninspectable process → retain `unknown`, retry later;
- exact owned leftover → route policy chooses recover/observe/terminate;
- shared provider service → never kill because one session disappeared.

Do not sweep by executable name, cwd, title, latest log or age. Do not hash executable
bytes on every metric sample.

## 7. One scheduling/admission loop

<a id="automation-and-delivery"></a>

Current source has an in-process scheduler, a standalone scheduler service and another
short-interval automation loop, plus optional-worker lifecycle slots.

### E0 — one schedule model

Use manager-owned automation entries:

```text
entry_id
enabled
trigger = event | interval | cron
typed action/settings
last admitted occurrence
next due time
```

Convert static `Config.schedules` checks into equivalent entries; delete
`ScheduleConfig` and `schedule_registry:v1`. Eliot Goal reminders use the same
occurrence mechanism but emit ordinary `Send`; Harness Goal stays native.

### E1 — one in-process control loop

```text
wait for Store change or nearest due time
  -> reconcile due occurrences
  -> dispatch_once enabled policies
  -> review/return_changes/accept/check/watch passes
  -> compute next due time
```

Passes are bounded and commit Operations; they perform no external I/O. A slow safety
wake recovers lost/coalesced notifications.

### E2 — delete standalone scheduler service

Delete `swarm-automation` worker executable/config/credential lifecycle,
`store/automation_scheduler.rs` page/admit service, declared scheduler scope,
owner/generation/config digest/process group/readiness, worker slot/demand, service
qualification and duplicate due snapshots/quarantine.

### E3 — keep workers only for external effects

Remove built-in executor/MCP/timer workers as their routes migrate. Retain checks,
scripts, Forge, launcher and adapters only while they perform or recover real external
I/O/custody.

## 8. Compact model-visible work

### F0 — one `TaskPacket`

Edit:

- `store/task_prompt.rs`;
- `swarm-contracts/src/task_prompt.rs`;
- `store/launcher_dispatch.rs`;
- dispatch decoders in retained adapters.

Render only current actionable context:

```text
Goal
Requirements and review/check criteria
Current phase: initial | continue | correct
Selected source references/excerpts
Write and forbidden paths
Worktree, branch and baseline
Selected route/model/effort
Deliverable and completion checks
Current correction or resume facts when applicable
```

Do not inject canonical Task JSON, launch manifest, binding generation, proof digests,
full issue history or every MCP schema into model text.

### F1 — Store identity stays in Store

A dispatch carries retained Operation ID plus rendered body. Store resolves Task,
Attempt, revision, binding and workspace through the Operation. Delete repeated
`task_snapshot_sha256`, `source_text_sha256`, `source_text_bytes`, prompt hashes and
nested contexts where they only echo Store-owned data.

Keep a content hash only where exact persisted bytes require integrity. Adapters may
verify the body they actually submit; they do not re-prove every Store relation.

### F2 — one renderer for initial, correction and resume

Initial assignment, correction and session recovery use the same renderer with a
small phase-specific section. A correction contains the complete current actionable
findings set. Resume contains only facts changed since the retained Attempt/candidate.
Do not append historical packets or addendum chains.

## 9. Review, correction and acceptance without workflow copies

### G0 — `review_once`

Query current candidates missing required independent review and admit at most one
`review.assign` per exact candidate/auditor slot. Do not copy the submission into a
review workflow state machine.

### G1 — one `task.request_changes` transition

`review.submit` retains verdict and all findings for one candidate.
`task.request_changes` verifies the exact current candidate/review, sets the Attempt
to `needs_correction`, stores one cause/findings set and optionally admits one child
`agent.send` to a live binding.

If no binding is live, the Task state remains sufficient; the next dispatch/resume
renders the same correction. No pending-repair queue is needed.

### G2 — bounded correction rounds

Candidate B requires review B. Identical candidate identity after a correction is
`no_progress`, not a fresh successful round. Count correction rounds in Attempt
lifecycle and escalate to manager attention after the configured bound.

### G3 — `accept_once`

Automation queries candidates whose exact retained review/check evidence satisfies
policy and invokes the same Store acceptance gate as manual `task.accept`. It does
not construct caller-authored evidence arrays or a second acceptance semantics.

### G4 — reduce automation steps

Retain thin policy toggles only:

```text
dispatch
review
return_changes
accept
check
goal_message
script
```

Delete `ReviewDisposition`, `RepairDispatch`, publication and GitHub-projection
workflow states, cursors, semantic slots, duplicate links, findings packages and
ordinary blocker quarantine. Explicit accepted-candidate publication remains an
integration, not a lifecycle stage.

## 10. One public API route and one authorization owner

Current method handling is duplicated across `METHOD_REGISTRY`, MCP catalog/profile,
`model::validate_mutation`, `Store::call`, `Store::apply`, participant special cases,
`mcp_authorization` and `OPERATION_VISIBILITY_SQL`.

### H0 — split three protocols

Keep distinct:

1. **Application API** — manager/participant reads and mutations.
2. **Internal host protocol** — module/supervisor/bus/service calls using typed
   internal enums, never MCP/public method registry.
3. **Adapter protocol** — typed semantic `AdapterCommand` and adapter deliveries.

Remove internal and vendor-native names from the public registry. Remove optional
products (`code.scope`, Concilium, contract ratification/ACK, managed-label
projection) before designing the final public table.

### H1 — domain-owned typed handlers

For each retained public domain, define one current request enum/parser and one
handler that performs:

```text
parse -> authenticate role -> authorize exact object/revision -> mutate/read
```

The central router dispatches to the domain and does not repeat field schemas or
object rules. `METHOD_REGISTRY` may retain only small public classification needed by
CLI/MCP discovery; delete coarse `ParticipantAccess` once domain handlers own exact
rights.

Do not create a generic policy DSL or generated method framework.

### H2 — Operation scope, not method-name visibility SQL

At Operation admission retain normalized scope needed for reads:

```text
project_id
task_id / attempt_id / candidate when applicable
owner/caller
binding/generation when applicable
integration subject when applicable
```

Generic Operation visibility uses these facts plus current grants. Delete method-name
`LIKE`/`IN` branches, automation meta joins and historical product exceptions from
`OPERATION_VISIBILITY_SQL` as their producers disappear. Domain-specific sensitive
reads still use their domain authorizer.

### H3 — remove a surface completely

Every removed method disappears together from:

- Store router/apply/read;
- method policy and participant projection;
- MCP schema/catalog/profile;
- CLI/help/examples;
- Operations/capacity/automation consumers;
- tests and compatibility fixtures.

Completion: adding one retained public method edits one domain handler and its actual
frontend schema, not five independent allowlists.

## 11. Numeric migrations, not source-text compatibility

Current Store binds database identity to exact `001_core.sql` SHA, keeps
`user_version=1`, digest-pins extensions, and implements special Rust verifiers for
009/010/011. The script migration builds a reference database and contains a custom
SQL lexer; failure/cancel migrations duplicate trigger-verification modules.

SQLite already provides application-owned `application_id` and `user_version`; use
them as intended. Run missing changes in one `IMMEDIATE` transaction.

### I0 — one migration ledger

Add a small current table:

```sql
CREATE TABLE schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at_ms INTEGER NOT NULL
);
```

`application_id` identifies Eliot. `user_version` and the highest migration row name
the current schema version. New databases apply the ordered migration list from zero;
existing databases apply only missing versions.

Keep migration SQL immutable after release, but do not compare the current binary's
base SQL text with the database on every open.

### I1 — finite bridge from existing markers

For one release, inspect existing `application_id`, `user_version`, known tables,
columns and old `schema_extension:*` markers. If they match one supported current
state, record the corresponding numeric migrations transactionally. Unknown or
inconsistent state fails with an explicit diagnostic.

This bridge supports only concrete known deployed states. After those databases move,
delete old digest-marker readers and bridge fixtures. Do not build a generic old-SQL
canonicalizer.

### I2 — delete exact-DDL machinery

Delete:

- `SchemaIdentity::digest`, `schema_digest` write/read/verification and
  `SchemaDigestMismatch`;
- exact base-schema verification in status reader;
- generic extension SQL-digest comparison;
- `script_event_schema.rs` reference DB, DDL tokenizer and shape comparator;
- separate failure/cancel trigger verifier modules and duplicate tests;
- per-migration source-SHA markers once bridged.

A normal migration may perform explicit precondition queries and postcondition checks
for the columns/indexes it changes. Optional startup health can use bounded
`foreign_key_check`/`quick_check`; it must not reconstruct a second schema compiler.

### I3 — reader compatibility

Read-only Store connections validate:

- application ID;
- supported current schema version;
- query-only/WAL-visible connection settings.

They do not require exact bundled base SQL bytes. Writer startup applies migrations
before readers open.

Completion: formatting or equivalent DDL changes cannot brick a compatible database;
partial migration rolls back; unsupported versions fail before normal work; no custom
SQL lexer/reference DB runs on every startup.

## 12. Extract adapter plumbing only after proof

<a id="processes-and-frontends"></a>

Use Antigravity plus one second small current Rust route. Claude `OwnedBootstrap` is a
bootstrap candidate, not automatically a spool candidate.

Extend existing owners:

- `swarm-process::module_owner` returns verified binding/generation/boot, state dir,
  client/credential and selected protocol/capabilities;
- `swarm-client::ModuleLink` owns Eliot connect/hello, command pull and exact
  re-delivery of already retained Store messages.

The shell does not own provider session, native command queue, result attribution,
uncertain-effect readback, Tasks, timers or scheduling. Extract only when two routes
adopt it and delete both old outer loops in the same slice.

## 13. Typed semantic adapter protocol

Target commands:

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

Unsupported actions fail before native I/O. Vendor-only operations stay private.

Remove public/common internals:

- `native.opencode.loop_step` → `Send { steer }`;
- Command cancel/close → `Cancel`/`Shutdown`;
- release-evidence → adapter cleanup;
- `native.mcp.*` → OpenCode adapter setup/observation.

`Recover` restores a known session without input replay. `Reconcile` reads an
uncertain effect. Hello becomes protocol major plus capability subset; delete package
version/schema ladders and full descriptor echo. Outcomes retain Operation,
generation/boot, native IDs, disposition and bounded details—not package/protocol/
request hashes.

## 14. One executor per harness

- OpenCode: standalone Rust; remove built-in executor, legacy demand and public MCP
  phases.
- Codex: Rust app-server; remove Python executor/SDK/fixtures after concrete old work
  is disposed.
- Muse: select Node or Rust by complete behavior and total maintained code; delete the
  other.
- Claude: one Rust adapter with thin current SDK driver; delete old Node generations.
- Command: one current Batch and one current ACP generation; remove profile ladders.
- Antigravity: one current warm-stream route after A0–A3.

Historical records do not keep an executor alive.

## 15. Goals and passive observation

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

Harness Goal maps through adapter `Goal` and is default where supported. Eliot Goal is
an independent occurrence that sends an ordinary message Operation. Remove provider
branches, pseudo-Goal continuation and Goal proof packets. Disabling Eliot Goal,
pausing native Goal, cancelling a turn and clearing queued input remain independent.

Adapters read native events and provider-local JSON/JSONL/SQLite. Host adds process
and Git facts. Store joins Task/Attempt/Operation, binding, native session, process,
worktree/branch/HEAD, remote ref, source cursor/gaps and provider usage.

Configured readers run without Task/binding; unmatched sessions remain inventory.
Use selective `sysinfo` refresh and exact OS birth identity. Remove per-sample
executable hashing. Provider reads are byte/record/time bounded; source failure is a
gap, not an empty fleet. Fix restart connection reset for every unreleased binding and
preserve source identity in follow events.

## 16. Remaining Store and product reduction

<a id="store-and-current-data"></a>
<a id="identity-and-hashes"></a>

Make launch preview optional. Store resolves one launch plan under the Operation;
remove caller-supplied plan hashes. First dispatch requires current assignment,
workspace lease, binding and usable session—not MCP readiness.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Keep one Store writer; move pure bounded reads to WAL readers. Never hold Store
transactions across external I/O or client pagination.

Give automation immutable `entry_id`; transfer updates owner/revision in one
transaction. Delete successor copies, journal relocation, chains and cycle logic.

Remove from compact-core completion:

- Concilium/council;
- generic contract/integration ratification and ACK;
- automatic publication/Issue-label projection;
- document-edition rights and legacy acceptance behavior;
- retired installers/provenance equality/duplicate validators;
- host dependencies on worker implementation crates after contracts move.

## 17. Finite transition

<a id="current-format-and-existing-data"></a>

Backward compatibility is not a product feature. Before deleting a protocol,
executor, method or storage path, enumerate concrete unsettled Operations and required
results. Finish/read back with the current owner, export the result, or create a new
Task revision/Attempt. Then remove producer, reader, schema branch, config, fixture
and package together.

Do not write converters for hypothetical history. Completed history may stay opaque
when current projections retain required facts; it does not keep runtime code alive.

## 18. Verification

<a id="verification"></a>

| Boundary | Required proof |
| --- | --- |
| Antigravity A0–A3 | v6 only; first dispatch needs no proof refresh; terminal outcome survives Eliot disconnect; no native replay; removed proof/handlers have no callers. |
| Queue/route/provider/capacity | deterministic reasons; blockers do not stall later Tasks; fallback is explicit; provider freshness is honest; concurrent root admission respects limit; old ledgers are gone. |
| Workspace/process | overlapping writers cannot launch; disjoint prefixes can; review-only has no lease; PID reuse cannot kill foreign process; uninspectable owner remains unknown. |
| Scheduling | one due/admission loop; lost wake recomputes current state; same occurrence admits once; standalone scheduler service is absent. |
| TaskPacket | model receives only actionable bounded work; initial/correction/resume share renderer; Store identity and repeated hashes are absent from prompt/adapters. |
| Review/correction | all findings return together; B cannot reuse A review; identical corrected candidate becomes no-progress; automation uses common Store transitions. |
| Public API | each retained method has one domain parser/authorizer/handler; internal/adapter protocols are separate; removed surfaces have no MCP/CLI/policy/visibility remnants. |
| Migrations | new and bridged DBs reach one numeric version transactionally; equivalent SQL text does not block open; partial migration rolls back; readers reject unsupported versions. |
| Adapter protocol/executors | two current adapters prove shared plumbing; rights still reject unauthorized calls; one executor per retained harness. |
| Observation/results | unbound sessions and source gaps remain visible; exact input/result attribution; uncertain stream stays unknown; complete result path is singular. |
| Deletion | no config, package, CI, docs or fallback wrapper remains; report net owned Rust+JS+glue separately from tests. |

Documentation changes run content/link checks. Runtime slices start with formatting and
the minimal affected warnings-denied Clippy/build. Focused connected cases run when
the slice closes; broad qualification follows the complete product path.
