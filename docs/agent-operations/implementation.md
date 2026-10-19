# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source, active tooling, old Claude Swarm
scripts and current donor sources were rechecked at
`4cfbbf84dfde7efc60c5dd6e17f1f5bc48377800` on 2026-10-18.
[Findings](refactoring.md) preserve evidence; [status](../implementation-status.md)
records completed work. This document says what to change, where to change it and
what old mechanism must disappear.

Do not start another repository-wide framework migration. Implement one complete
caller-to-result slice, delete the replaced path, then use the second real route to
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
| Adapter process membership and private durable files | `crates/swarm-process::module_owner`, `module_membership` and `permissions` |
| Native transport, session state, provider readers and uncertain native effects | selected `swarm-adapter-*` or `modules/*-rust` implementation |
| Workspace registration, worktree creation and write authority | `crates/swarm-kernel-host/src/store/workspace.rs` plus `crate::workspace` for filesystem/Git effects |
| Queue/dashboard projection and launch admission | current Store launcher owner; no second queue service |
| Process/worktree/Git collection | current host lifecycle/process owners |
| Enabled helper scheduling | one `swarm-automation` service with Store-owned due state |
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
- `tools/modules/Install-ModuleArtifact.ps1` narrow Antigravity validator;
- active catalog/qualification fixtures selecting new work.

Do not rewrite historical v5 receipts under `docs/remediation`. New installation,
selection and qualification use v6 only. Remove the active `"5" | "6"` behavior
branch after concrete unsettled v5 bindings receive a final disposition.

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
- duplicated `local_execution_results` terminal receipts in observations;
- outcome-to-current-observation matching in `runtime/warm_stream.rs`;
- `AntigravityDispatchSessionProof` and constants from `swarm-contracts::runtime`;
- proof production in `modules/antigravity-rust/src/controller.rs`;
- `launcher_dispatch/antigravity_capability.rs` proof lookup/validation/digests;
- refresh-before-dispatch qualification and proof fixtures.

`agent.refresh` remains safe readback. Model, stream phase, counters, gaps and native
readiness remain useful `agent.state` observations; they do not authorize dispatch.

Completion: a ready current binding receives its first assigned input without a fresh
refresh; stale Task/Attempt/binding/workspace facts still fail; terminal outcome is
immutable before Store delivery; no proof DTO or observation dependency survives.

### A2 — make Store delivery durable without replaying native input

After A1, implement a route-local delivery spool using existing private durable file
primitives in `swarm-process::permissions`. Do not add a database, append-only
journal, tombstones, compaction or retention graph.

Two record classes are intentionally different.

#### Terminal outcomes and result pages

One private file per already-formed Store request:

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

#### Observations

Maintain at most one replaceable pending checkpoint per observation source:

```text
source_id + source_epoch
source cursor/sequence
coverage or gap
latest normalized state
```

A newer unsent checkpoint replaces an older one. Store creates the durable
observation journal after accepting it. Do not create one local file for every native
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

## 4. Replace WorkDispatch with current-state admission

The current `store/automation_work_dispatch.rs` is another workflow engine. It tails
Task observations, owns global/per-entry cursors, copies blocked Tasks into a bounded
pending list, periodically rechecks them, creates semantic slots, retains operation
links and quarantines source/config records. `automation/work_dispatch.rs` duplicates
launch route/profile/model/effort/budget/stop-condition planning.

The target is a query, one admission transaction and no copied pending state.

### B0 — one queue projection

Keep `swarm.queue.get` as a read projection in the existing launcher/Store owner. It
reads current:

- Task revision/state and current Attempt;
- review/change-request state;
- dependencies and acceptance state;
- active workspace leases/write conflicts;
- selected route capabilities, provider condition and capacity;
- current binding/session availability.

Return deterministic rank and `why_ranked`. The default rank tuple is deliberately
small:

1. current candidate returned for correction;
2. current unfinished Attempt needing continuation/relaunch;
3. ready unclaimed Task;
4. explicit manager/task phase ordering where configured;
5. oldest eligible Task, then stable Task ID.

Do not infer “small” from prompt length or code heuristics. Dependency and missing
source facts remain visible. Automatic dispatch skips a blocked dependency; a manager
may explicitly start investigative work, while acceptance still enforces the Task's
frozen dependency requirements.

A queue page is not a reservation. Every launch rechecks current facts.

### B1 — a lane is configuration, not a new entity

A lane name may be shown in UI/reports, but its durable inputs already exist:

- manager/project automation entry;
- one primary route and an optional explicit ordered fallback list;
- current route configuration/model/effort/profile;
- Task selector using current project/phase/state;
- `RouteAdmissionPolicy.max_concurrent_roots`;
- enabled/disabled state.

Do not add a lane table, process, cursor, inbox, status machine or per-lane scheduler.
Use the automation entry ID as the display/policy identity. Disabling the entry stops
future admissions; active Attempts continue.

Provider conditions decide route use:

- `rate_limited`: hold until documented retry time;
- `quota_exhausted`, `model_gone`, `auth_required`, `data_policy_required`: route
  unavailable until a new fact or explicit configured fallback;
- `overloaded`: temporary hold;
- `unknown` or stale coverage: automatic dispatch holds; manual launch may proceed
  only with an explicit manager decision recorded in the launch Operation.

Fallback selects only from configured aliases and revalidates the target route. It
never silently changes provider/model/effort.

### B2 — `dispatch_once`, not observation replay

Replace the WorkDispatch reconciler with one bounded operation, invoked by existing
scheduler/event wakeups:

```text
dispatch_once(automation_entry)
  -> query ranked current Tasks
  -> for each candidate within a small bound:
       validate current Task/Attempt and route
       try one atomic launch admission
       return admitted Operation or continue past an ordinary blocker
  -> return why nothing was admitted
```

The admission transaction:

1. validates manager/project/Task revision;
2. creates or reuses the current Attempt;
3. reserves the workspace/write lease;
4. checks route/provider/root capacity;
5. creates the launch Operation with one deterministic request identity.

External filesystem, Git and native I/O occur after commit through existing launcher
owners.

Delete:

- `GlobalWorkDispatchCursor`;
- `WorkDispatchState`, `PendingSubject` and pending recheck rules;
- activation/catch-up cursors and `MAX_PENDING_SUBJECTS` behavior;
- semantic launch-slot records and duplicate operation-link records where Operation
  identity already provides idempotency;
- WorkDispatch quarantine records for ordinary Task/config blockers;
- preview-digest binding in automated launch;
- duplicated route/model/profile/stop-condition envelopes that launcher can read
  directly from current policy.

Wake notifications merely cause another query; losing/coalescing a wake is harmless
because periodic reconciliation reads current state. A blocked Task never stalls a
cursor behind it.

Completion: 128 blocked Tasks do not prevent a later eligible Task from launching;
no pending copy must be repaired after a Task revision; the same current Attempt does
not produce two launch Operations; disabling dispatch admits nothing new and cancels
nothing active.

## 5. Collapse advisory code scopes into workspace write leases

Current `code.scope.propose/accept/inspect/conflicts/release` is a separate product
with proposal digests, revisions, overrides, expiry, paths, symbols, interfaces,
indexes and conservative overlap pages. Its own source states that it does not lock
files, inspect Git or start work. Actual launch already reserves a workspace lease,
stores allowed paths/symbols and calls `reject_scope_conflicts`.

Use the existing workspace lease as the only write authority.

### C0 — simple scope semantics

Source:

- `TaskSpec.scope.initial_paths` → repository-relative write prefixes;
- `TaskSpec.scope.forbidden_paths` → hard negative prefixes;
- absent/empty initial paths → repository root;
- review-only work → no write lease.

Normalize separators and reject absolute paths, traversal, empty segments and `.git`.
Use path-segment prefix comparison: `src/a` overlaps `src/a/file.rs`, but `src/a`
does not overlap `src/ab`.

Do not use symbols/interfaces/globs as concurrency authority. Remove
`allowed_symbols` from the workspace lease after consumers migrate. Keep source
symbols in the task brief only as guidance.

### C1 — one launch-time decision

`reserve_lease_for_launch` performs the overlap check in the same transaction as
Attempt/launch admission. Another active lease in the same project whose path prefix
overlaps blocks automatic launch. The response names the conflicting Task/Attempt and
paths.

No generic override in the first compact implementation. The manager resolves an
intentional overlap by revising scope, finishing/releasing one Attempt or assigning a
different non-overlapping slice. This keeps actual ownership obvious.

The lease remains through repair/review while the candidate can still change. It is
released automatically when the Attempt is accepted, abandoned or superseded, and
explicitly on a manager stop that has settled native uncertainty.

Delete after callers switch:

- `coordination/code_scope.rs`;
- `store/code_scopes.rs` records/indexes/conflict engine;
- `code.scope.*` CLI/MCP/method-policy/catalog entries;
- proposal digest, broad-scope acknowledgement, override and expiry logic;
- code-scope references in contract/ratification engines that are themselves removal
  targets.

Completion: two overlapping writers cannot launch; two disjoint path-prefix leases
can; review-only work launches without a write lease; Task revision invalidates the
old intended scope without preserving a second scope history product.

## 6. Simplify process ownership and recovery

Current `swarm-process::module_owner` already uses owner-first Job/process-group
custody. Keep that topology. CCCC's suspended-child primitive is useful only if a
future child-first launch cannot be assigned before user code runs; do not import it,
its scheduler or ledger for the current path.

### D0 — one process owner record

Persist only facts required to identify and control a launched adapter process:

- binding ID/generation and adapter boot ID;
- state directory and module client/credential reference;
- executable selected by the installed package;
- PID plus OS process-birth/handle identity;
- process group/Job ownership identity;
- parent/shared-service relation where applicable.

Package/artifact version, build ID, protocol and the full module-contract JSON remain
installation/inventory facts. The resolver does not compare the complete coordinate
matrix again. Verify executable/package bytes once at install/launch, authenticate the
ModuleLink, then use binding/generation/boot for runtime ownership.

Simplify `ModuleOwnerPlan` and `ProtectedResolverMap` accordingly. The resolver maps
opaque protected references to private files; it does not become another descriptor
validator. Keep path containment, no-link traversal, private permissions and exact
credential scope.

### D1 — restart reconciliation

Use the conservative owner-led rule:

- record says process is dead → clear live record;
- PID exists but OS birth/image/owner identity differs → clear record, kill nothing;
- process cannot be inspected → retain `unknown`, retry later;
- exact Eliot-owned leftover → route policy decides recover, continue observation or
  terminate;
- shared provider service → never terminate merely because one session/adapter link
  disappeared.

Do not sweep by executable name, cwd, title, newest log or age. Do not hash the
executable on every metric sample.

Completion: PID reuse cannot kill an unrelated process; an uninspectable owner stays
visible; exact leftover cleanup works; process records no longer carry a duplicated
runtime compatibility contract.

## 7. Extract shared adapter plumbing only after proof

### Bootstrap threshold

Antigravity and Claude's contained
`crates/swarm-adapter-claude/src/module_runtime.rs::OwnedBootstrap` are the first two
candidates for one reader in `swarm-process::module_owner`.

The shared reader may return only:

- verified owner/worker membership;
- binding ID/generation and boot;
- module client/credential identity;
- private state directory;
- current protocol major and required semantic actions;
- typed host connection configuration.

It does not parse vendor options, launch native processes or compare artifact/schema
matrices. Convert both adapters and delete their duplicate env/path/protocol/claim
parsers in the same change.

### Eliot link threshold

`swarm-client::ModuleLink` already owns authenticated hello, `module.next`, outcome,
observation and result calls. Replace Antigravity's private `ManagerLink` only after
A1/A2 and the reduced hello contract.

A common loop may own connect/hello, Eliot-link reconnect, typed command pull, exact
delivery of route-owned retained Store records and bounded link shutdown. It may not
own native session state, provider retries, result association, Task state, timers or
scheduling.

Do not generalize the current Command/Claude/OpenCode journals. A shared exact
terminal-delivery spool moves to `swarm-client` only after a second migrated adapter
uses the same immutable-file semantics and both previous delivery loops disappear.

<a id="p1--replace-the-module-compatibility-matrix-with-protocol-2"></a>

## 8. Semantic adapter protocol, route by route

The target internal command set is closed and vendor-neutral:

```text
Open          open/resume/attach a selected native session
Dispatch      deliver the first immutable TaskPrompt
Send          next-turn input or exact active-turn steering
Reply         answer one observed pending request
Configure     apply supported model/mode/options
Goal          set/edit/pause/resume/clear Harness Goal
Background    move an eligible native run/tool to background
Cancel        cancel one exact turn or queued input
Recover       restore one known session after confirmed owner/control loss, no replay
Refresh       safe native readback
Result        retrieve output for one admitted input
Reconcile     read back one uncertain admitted effect
Shutdown      release/stop an explicitly owned resource
```

A route declares supported actions and scopes. Capabilities route commands; Store
rights remain the permission boundary. Vendor RPC names/payloads stay in the adapter.

Remove internal methods as their route migrates:

- `native.opencode.loop_step` → `Send { delivery: steer }`;
- `native.command.cancel_turn` → `Cancel { target: turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → internal cleanup;
- `native.mcp.*` → OpenCode adapter lifecycle/observation.

Each migrated route uses one current protocol major and compact capabilities. Package
version/build and schema inventories are installation/diagnostic facts, not behavior
branches. Hello verifies authenticated binding, protocol major and required
capabilities; it does not echo/compare the complete descriptor on both sides.

<a id="processes-and-frontends"></a>

## 9. One executor per retained runtime

| Runtime | Retained direction | Delete after finite disposition |
| --- | --- | --- |
| Antigravity | Rust serialized warm stream | Node bridge and proof/observation coupling |
| OpenCode | standalone Rust adapter | built-in host executor, legacy demand and public native MCP phases |
| Codex | current Rust app-server adapter | Python executor/SDK, artifact-version templates and fixtures |
| Muse | choose Node or Rust by complete behavior and total Rust+JS+IPC code | rejected executor and duplicate usage normalizer |
| Claude | one Rust adapter with thin current SDK driver | retained Node generations and duplicate bootstrap/transport code |
| Command | one current batch profile plus one current ACP implementation only if both are product requirements | historical Batch/ACP profile ladders and native method aliases |

Historical completed records do not keep an executor alive. Enumerate concrete
unsettled Operations, finish/read back/export them or create a new Task
revision/Attempt, then delete producer, package, fixture, configuration and reader.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

## 10. Keep both Goal mechanisms simple

Harness Goal is vendor-native and default where supported. The adapter maps
set/edit/pause/resume/clear and associates native turns with the initiating Operation
and Task/Attempt.

Eliot Goal is a Store plugin that emits an ordinary `Send` Operation on a selected
event or cadence. Configuration needs only message/template, trigger/cadence and
delivery mode. Replaying one occurrence returns the same Operation; a later
occurrence may intentionally repeat the text.

Remove provider branches, OpenCode pseudo-Goal, objective approval hashes and
Goal-specific continuation proof/receipt families. Plugin disable, native Goal pause,
turn cancellation and queued-input removal are independent.

<a id="passive-agent-observation"></a>

## 11. Passive observation and quotas

One bounded host collection lifecycle runs independently of automation/diagnostics.
Adapters fold native events and provider-local JSON/JSONL/SQLite; host adds OS/Git;
Store joins Task/Attempt/Operation, binding, native session/turn/child, process,
worktree/branch/HEAD/upstream/push target, remote ref, source cursor/gaps and quota.

Configured sources run without Task/binding. Unmatched sessions remain inventory.
`agent.list`, `agent.state`, `agent.family` and `agent.usage` read Store without
provider calls.

Keep assignment, native activity, pending wait, connection, process liveness, source
freshness, Git state, Task completion and quota separate. Never kill/restart/reassign
from age, low CPU, old provider status, unchanged branch, PID alone, newest file or
title/path regex.

Provider/source cursor and Store observation cursor remain separate. For a replay +
live source: subscribe before/coupled with replay, read a bounded page after the
source cursor, apply ID-addressed upserts, reach a watermark, then commit normalized
facts + cursor + coverage in one Store transaction. Expired/foreign cursor, truncated
JSONL, WAL loss or source replacement produce a gap and bounded rescan.

Replace the current quota proof system with a generic adapter-produced snapshot:

```text
source key + source epoch + source-local sequence
observed_at + freshness + coverage
scope = account | service | connection
provider identities only when actually available
availability condition + optional retry/reset
native-labelled buckets/windows/credits
```

Store validates authenticated source, bounds and monotonic source sequence. Remove
service/artifact/method allowlists, `auth_context_ref` proof SHA, repeated
route/binding/model provenance and `details_digest`. A connection-scoped snapshot is
not merged across routes without stable account identity.

Fix startup stale connection health and preserve source identity in follow/timeline.
Malformed optional usage cannot discard valid activity/result observations. Missing
quota is unavailable, not zero.

<a id="launch-and-results"></a>

## 12. Launch, results, review and acceptance

Launch preview is optional. Store admits explicit settings/current revisions,
resolves one launch plan under the Operation and references it from workspace and
dispatch. Remove caller-supplied plan hashes and MCP-readiness gates from first input.

The agent receives one compact TaskPrompt: objective, requirements/evidence criteria,
current scope, selected sources, exact workspace and relevant commands. Raw Store
snapshots, proof digests and the global queue do not enter model context.

Keep one result path:

```text
Result -> adapter page -> module.result -> immutable artifact
       -> artifact.read / artifact.assemble -> submission
```

Associate output with explicit native parent/input identity or one serialized admitted
turn window. Changed generation, second independent input or unclassified stream gap
requires readback; never choose the newest assistant message by guess. Exact terminal
pages remain in the delivery spool until Store ACK. `artifact.assemble` is the only
multi-page assembler.

Manual and automatic acceptance use the same candidate-specific retained review/check
gate. Remove caller-authored duplicate proof arrays. Candidate B cannot reuse A's
review; checks-only policies remain valid.

<a id="store-and-data-model"></a>
<a id="store-and-automation-implementation-details"></a>

## 13. Store, automation, tooling and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool; authorization, page data and journal cut use one short snapshot. Never hold a
transaction across native/file/Git/network I/O or client pagination.

Replace base-SQL text SHA equality with application identity, schema version and
explicit bounded migrations. Move indexed identity/state/due fields out of `meta`;
retain bounded payload JSON where useful. Remove old key codecs/readers after
consumers switch. No ORM, EAV store or second migration runner.

Give retained automation immutable `entry_id` independent of owner/name. Delete
successor copies, relocation paths, transfer chains/cycle logic and parallel
schedulers. One `swarm-automation` service evaluates due work; Store commits due
identity and action disposition. WorkDispatch itself becomes `dispatch_once`, not a
cursor-driven domain.

Simplify packaging/qualification to selected executable/resources, one byte check at
install/launch, path containment/private credentials, protocol major/capabilities and
observable runtime behavior. Delete checkout/compiler/cache-path equality,
artifact-specific PowerShell validators, duplicate hashes, legacy frontend inventory
and incident-specific workflow modes after permanent checks move to focused tests.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- advisory code-scope product after workspace-lease migration;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- retired installers and duplicate package validators;
- host dependencies on worker implementations after required contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual optional integrations.

<a id="current-format-and-existing-data"></a>

## 14. Finite protocol transition

Migration is route/responsibility-specific, not a repository-wide flag day:

- an unmigrated current path may temporarily remain, but receives no new historical
  profile or feature;
- once migrated, new work uses the new path and the old producer is disabled;
- concrete unsettled old Operations are finished/read back/exported or replaced by a
  new Task revision/Attempt;
- then old producer, reader, descriptor, fixture, config and package are deleted
  together.

Do not branch on artifact version inside a protocol major or build converters for
hypothetical old data. Completed historical records may remain opaque history when
current projections contain needed facts; they do not keep executable code alive.

<a id="verification"></a>

## 15. Verification

During construction run formatting and minimal affected warnings-denied Clippy/build
or syntax checks. Do not create a broad matrix around an incomplete path. When a
connected slice is complete, verify:

| Boundary | Required proof |
| --- | --- |
| Antigravity A0–A3 | v6 is the only active selector; terminal outcome needs no observation/proof; lost Store ACK survives without native replay; unsupported actions never reach adapter. |
| Queue/dispatch | 128 blocked Tasks do not block a later eligible Task; rank is deterministic/explained; same current Attempt does not get two launch Operations; disabling policy stops only new admissions. |
| Write lease | Overlapping path prefixes block concurrent writers; disjoint paths work; review-only needs no write lease; Attempt lifecycle releases authority; `code.scope.*` is gone. |
| Process ownership | Dead/mismatched/uninspectable/exact-owned processes get distinct outcomes; PID reuse kills nothing; shared service survives session loss. |
| Typed port/hello | Store rights reject unauthorized calls; optional action changes capability, not artifact/version logic. |
| Store delivery | Same-boot reconnect sends identical terminal params; ACK removes file; replacement boot cannot silently drain it; observations coalesce by source. |
| Native effects | Native idempotency/readback is harness-specific; unknown ACP/Claude/CLI submission is not retried. |
| Goals | Native and Eliot Goal work alone or together; disabling/pausing one affects only that mechanism. |
| Observation | Unbound session, shared service, surviving process, changed worktree and stale remote ref remain distinct without provider calls. |
| Quota | Node/Rust producer versions do not affect admission; connection-scoped facts do not impersonate account-wide quota. |
| Results/review | Correct Operation owns output; stream gap stays unresolved; candidate B cannot reuse A's review. |
| Deletion | Removed path has no config, package, CI, docs, fixture or fallback wrapper; net owned Rust+JS+glue is reported separately from tests. |

Documentation changes require content/link checks. Every completed source slice updates
[status](../implementation-status.md) with behavior, evidence and net owned-code
change.
