# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source, historical Claude Swarm scripts,
current package/tooling and donor sources were reviewed through
`0e8c7377a0902b9fc92eb9f1f6db794496a2e261` on 2026-10-20.
[Findings](refactoring.md) retain the evidence;
[status](../implementation-status.md) separates implemented source from plans.

Do not start another repository-wide framework migration. Complete one
caller-to-result path, delete the replaced path, then use a second current caller to
prove any shared implementation.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. How agents implement this program

Every coding slice states:

- observable behavior;
- exact current source owners and symbols;
- unsupported and uncertain cases;
- old callers, formats, fixtures, configuration and packages to delete;
- one donor mechanism or native API when relevant;
- minimal affected format/Clippy/build command;
- connected completion proof and net owned-code change.

One manager owns shared contracts, Store transitions and integration in one worktree.
Leaf agents receive narrow non-overlapping tasks. A type, fixture, adapter stub,
compatibility reader or new crate is not progress unless a current caller uses it and
the superseded mechanism is removed in the same slice.

Migration is route-by-route. An unmigrated route can temporarily retain its current
protocol but receives no new profile/version family. Once a route migrates, no new
work enters its old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Semantic adapter commands and bounded Store records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` / `Client` |
| Adapter process membership and private durable files | `crates/swarm-process::{module_owner,module_membership,permissions}` |
| Native transport/session/readback | selected adapter only |
| Workspace registration, worktree creation and write authority | `store/workspace.rs` plus `crate::workspace` for Git/filesystem effects |
| Queue, route decision and launch admission | existing Store launcher owner |
| Time admission | one in-process host loop; Store owns durable due state |
| Checks, scripts and remote Git effects | their existing domain workers |
| Public CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling Store |

No adapter accepts Tasks or candidates. No frontend interprets vendor events. No
shared shell owns provider session state, native replay, Task state, scheduling or
transcript storage.

<a id="identity-and-hashes"></a>

## 3. Common semantic adapter boundary

The target internal command is a closed typed enum:

```text
Open
Dispatch
Send(next_turn | steer)
Reply
Configure
Goal(set | edit | pause | resume | clear)
Background
Cancel(turn | queued_input)
Recover
Refresh
Result
Reconcile
Shutdown
```

Each adapter advertises only the actions and scopes it actually supports. Unsupported
actions fail before native I/O. Capabilities route commands; Store authorization
grants rights.

Remove vendor/internal methods from the common/public protocol:

- `native.opencode.loop_step` → `Send { steer }`;
- `native.command.cancel_turn` → `Cancel { turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation.

Store retains the admitted Operation and authenticated binding. A normal outcome
needs only Operation ID, stale-generation/boot guard, native IDs, disposition and
bounded action details. Do not repeat package version, protocol, request hash,
Task/Attempt identity and source-text hashes through every receipt.

Keep hashes for credentials/verifiers, selected installed package bytes and immutable
result/artifact bytes. Readiness, model, activity, quota and stream state are
observations, not cryptographic admission tokens.

## 4. First executable route — Antigravity Rust

Antigravity remains the first runtime slice because it has one serialized native input
window and a concrete lost-delivery defect.

### A0 — one active v6 coordinate

Synchronize source, descriptor, route example, package policy, installer,
qualification and active catalog on artifact version 6. Historical v5 receipts remain
historical. Finish/export concrete unsettled v5 work, then delete active `5 | 6`
branches.

### A1 — self-contained terminal outcome

Current terminal settlement depends on a Store observation ID and duplicate
`local_execution_results`, while dispatch depends on
`AntigravityDispatchSessionProof`.

Make the terminal outcome final at native settlement:

```text
operation
binding generation + adapter boot
native conversation
result ordinal
native terminal + Eliot disposition
result/content reference
```

Delete observation-result duplication, proof DTO/producer/validator/digests and
refresh-before-dispatch gates. `agent.refresh` stays safe readback; its facts do not
authorize work.

### A2 — minimal Store-delivery spool

Persist already-formed terminal `module.outcome` and `module.result` parameters in
private atomic files before memory can forget them. Same boot reconnect resends the
same parameters and deletes only after exact Store ACK. The spool never contains
native prompt/process intent.

Observations use one replaceable pending checkpoint per source, not one file per
native frame.

### A3 — real typed actions only

Retain only:

```text
Open
Dispatch
Send(next_turn)
Refresh
Result
Reconcile
Shutdown/release of an explicitly owned resource
```

Delete placeholder Reply/Configure/Goal/Background/Recover/steer handlers and repeated
receipt identity.

## 5. Queue, routes, provider state and admission

<a id="work-dispatch"></a>

### B0 — one current-state queue

`swarm.queue.get` reads current Task/Attempt, corrections, dependencies, workspace
leases, route/provider/root capacity and binding state. It returns deterministic rank
plus `why_ranked`:

1. current correction;
2. unfinished Attempt needing continuation/relaunch;
3. ready unclaimed Task;
4. explicit phase order;
5. age and stable Task ID.

A queue page is not a reservation. Launch always rereads current facts.

### B1 — lane is a view, routes are explicit

A manager policy stores:

```text
enabled
project/task selector
ordered route aliases
optional agent profile
```

Each route alias owns exact harness/native settings/model/effort/root limit.
Automatic fallback selects only from that explicit ordered list and records the
selected alias. Do not create a lane table, daemon, cursor, inbox or scheduler.

### B2 — source-scoped provider facts

Normalize provider data as:

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
  retry_at/reset_at?
}
```

Adapters own native method names and rolling-patch folding. Store checks source
identity, bounds and monotonic sequence. Remove provider-method/artifact allowlists,
auth-context proof hashes and per-binding copies of account/service facts.

No fact permits one bounded probe. Transient retry/reset opens one probe, not an
assumption of recovery. Hard negatives require a new source/configuration fact or an
explicit configured fallback.

### B3 — root count, not a second lifecycle ledger

Count and reserve only unresolved root-opening Operations and unreleased bindings in
the same writer transaction as launch/open. Do not count send/reply/configure/goal/
refresh/result/reconcile/recover as new roots.

Delete capacity v1/v2 blobs, key codecs, phase/evidence rows, refusal records and
lifecycle sync hooks. `report.capacity` becomes a read projection.

### B4 — `dispatch_once`

Existing wake/timer paths call:

```text
dispatch_once(entry)
  -> query a bounded ranked page
  -> choose first eligible configured route
  -> atomically create/reuse Attempt, reserve workspace/root and create launch Operation
  -> return Operation or current blocker reasons
```

Delete WorkDispatch global/per-entry cursors, copied pending subjects, catch-up
watermarks, semantic slots, duplicate operation links and ordinary-blocker quarantine.
Lost/coalesced wakeups are harmless because a recovery wake rereads current state.

## 6. Workspace and process custody

### C0/C1 — one write authority

`TaskSpec.scope.initial_paths` supplies repository-relative path prefixes;
`forbidden_paths` is a hard negative boundary. Empty scope means repository root.
Normalize separators and reject absolute paths, traversal, empty segments and `.git`.
Use path-segment prefix overlap.

The Attempt workspace lease is the only write authority and is reserved in launch
admission. Read-only review needs no lease. Delete `code.scope.*`, proposal digests,
symbols/interfaces as authority, overrides, expiry, indexes and contract links.

### D0/D1 — minimal process owner

Persist only binding/generation/boot, private state/credential reference, selected
executable, PID plus OS birth identity, Job/process-group identity and shared-service
relation.

At restart:

- dead process → clear live record;
- PID identity mismatch → clear record, kill nothing;
- uninspectable → keep unknown;
- exact Eliot-owned leftover → route policy chooses recover/observe/terminate;
- shared provider service → never kill because one session disappeared.

Package/protocol/contract matrices remain installation facts, not repeated runtime
custody checks. Do not hash executables on every metric sample.

## 7. One scheduler and thin automation

<a id="automation-and-delivery"></a>

### E0–E3 — current-state admission loop

Keep one automation-entry schedule model and one in-process loop that waits for Store
change or nearest due time, plus a slow safety wake. Store atomically claims an
occurrence and admits an ordinary Operation.

Delete the static schedule registry, standalone scheduler process/credential/page
protocol, overlapping automation loops and optional workers that only perform Store
bookkeeping. Keep separate workers only for real external I/O/custody.

Automation steps become thin policy toggles:

```text
dispatch
review
return_changes
accept
check
goal_message
script
```

They reread current state and attempt one authorized action. They do not own a second
workflow state machine.

## 8. Compact model context and ordinary review lifecycle

### F0–F2 — one `TaskPacket`

Replace raw identity JSON, canonical brief JSON and launch packet in the model prompt
with:

```text
Goal
Requirements
Current work: initial | continue | correction
Selected sources/files/symbols
Write and forbidden paths
Worktree/branch/baseline
Selected route/model/effort
Deliverable
Verification
```

Store retains Task/Attempt/Operation/binding identity. The adapter receives one
Operation reference plus rendered body; it does not re-prove Task/source hashes.
One renderer handles initial assignment, correction and resume.

### G0–G4 — current review/correction state

```text
review_once
  -> assign one missing review for current candidate

review.submit
  -> retain verdict and all findings

task.request_changes
  -> exact current candidate/review
  -> one Attempt transition to needs_correction
  -> optional child agent.send with the full findings set

accept_once
  -> call the same candidate-specific acceptance gate as task.accept
```

Delete ReviewDisposition and RepairDispatch workflow state, per-finding delivery,
semantic slots, duplicate findings packages and automation-authored evidence arrays.
Candidate B needs review B. Same candidate after correction is `no_progress`; bounded
rounds escalate to manager attention.

## 9. Separate public, internal and adapter protocols

### H0 — freeze the current inventory

Classify every method in `method_policy`, MCP catalogs and Store routing as:

- retained public Core/integration;
- internal host/worker;
- adapter semantic;
- removal target.

No new aliases during the cut.

### H1 — domain-owned parse/authorize/execute

A retained domain owns its typed request, exact role/object/revision checks and Store
handler. The shared registry only classifies/exposes names; it cannot become another
authorization DSL.

### H2 — split protocols

Public methods never contain `module.*`, `native.*` or scheduler service RPC.
Internal service calls are not MCP tools. Adapter commands are typed semantic actions,
not arbitrary public method strings.

### H3 — delete whole products

Removing code scope, Concilium, contract/ratification, automatic GitHub projection,
old scheduler or vendor-native methods removes their policy rows, schemas, CLI/MCP
tools, handlers, fixtures and storage together.

## 10. Store evolution and hot reads

### I0–I3 — numeric migrations

Use SQLite `application_id`, application-owned `user_version`, one numeric migration
ledger and `BEGIN IMMEDIATE`. Each migration has explicit pre/postconditions and
bounded integrity checks. Remove base-SQL SHA equality, extension digests, reference
database construction and custom DDL-token validation.

Migrate only retained data. Do not build converters for hypothetical abandoned
formats. Historical completed rows may remain opaque when the current projection
already contains required facts.

Move pure bounded query families to existing WAL readers. Keep authorization, page
cut and returned data in one short snapshot. Do not hold Store transactions across
native/file/Git/network I/O.

## 11. IPC, MCP and high-volume views

### J0 — use existing multiplex correctly

`swarm-client::Client` already multiplexes bounded requests. Frontends must clone the
current `Arc<Client>` and release their connection mutex before awaiting RPC. A
transport failure retires only the exact connection and never replays the request.

### J1 — one MCP wake pump

One pump per MCP session follows Store cursor changes and coalesces notifications.
`monitor.follow`/`report.delta` remains authoritative replay. Do not start a poller,
forwarder and queue per subscription.

### J2 — exact pending-input reads

MCP task projection reads the addressed Operation/binding/pending request directly.
Do not scan every `report.attention` page for `tasks/get`.

### J3 — native stream ring outside writer

Store authorizes/open-closes the stream and records identity. Ephemeral ring
read/write occurs outside the single writer, with bounded memory and backpressure.

## 12. Checks, scripts and Forge

### K0/K1 — one Check executor

Use standalone `swarm-checks::execute_with_plan` for new CheckRuns, including bounded
source/tool/Git-hook probe modes. Keep historical host receipts readable through a
finite transition, then delete the hidden host check worker.

### K2 — one Script wire

Keep `swarm-script-worker` as executor and `swarm-scripts` as the only shared
Work/plan/start/completion types. Delete duplicated compatible host/worker structs and
old hidden entrypoints.

### K3/K4 — one remote Git effect owner

Keep standalone Forge for remote push/publication with exact repository/ref
serialization and remote-ref readback. Local Git inspection/preparation stays bounded
in host. Reuse `swarm-process` Group/capture/private files and verify worker package
bytes at install/launch, not per Operation.

## 13. Adapter-specific reductions

### L — shared shell only after proof

Extract common bootstrap/Eliot-delivery code only after two current routes use the
same behavior and delete their old outer loops in the same change. Extend existing
`swarm-process` and `swarm-client`; do not create an adapter-framework crate.

The shell may own authenticated Eliot IPC, hello, typed command pull and exact
redelivery of already-formed Store records. It must not own native session state,
provider recovery, result association, timers or Task state.

### M — Claude

Retain one Rust adapter with a thin official SDK driver. Split live session/native
effect, exact Store delivery, latest observation and persisted terminal result.
Delete the generic journal, separate callback retention engine, tombstones, reference
graph and old Node generations after finite transition. Live JS resolver loss becomes
unknown; it is not durable defer/resume.

### N — Codex

Retain the current Rust app-server adapter. Split one connection owner, SessionRecord,
per-effect records, exact Store delivery and latest observation. Use thread,
`clientUserMessageId`, turn and `expectedTurnId`. Map native Goal directly. Delete
full-snapshot checkpoints/tombstones, Goal proof packets, retired Python executor and
profile ladders.

### O — Command Batch/ACP

Keep one current Batch profile and one stable ACP v1 route. `AcpV2` is an Eliot
artifact generation, not ACP protocol v2; do not add the draft upstream v2 now.

Preserve ACP `session/load` no-fresh-fallback recovery and option readback. Replace
Batch/ACP duplicate journals with minimal SessionRecord, NativeEffectRecord and exact
StoreDelivery. Live permission resolver loss becomes unknown. Module-owner
Job/process group owns descendants. Remove `native.command.*`, release-evidence,
profile/schema ladders and duplicate package proofs.

### P — Muse: keep Node now, retire custom Rust

The default Node `muse-sdk-1.3.0-bridge.13` is the current complete executor. It uses
the official low-level Connection and persists one Eliot-derived command ID before
native I/O. Keep it for new work during this refactor.

The disabled custom Rust `muse-rust-controller.1@0.1.2` duplicates MSP wire/schema,
transport, fold, journal, process custody, observations and results and has only a
narrow recorded result qualification. It is a retirement target, not the basis of a
shared shell.

The official `muse-code-sdk` Rust crate is a future replacement candidate, not an
additional route. Its high-level `send_user_turn` currently mints command IDs
internally; using it unchanged would hide Eliot's Operation→native-command boundary.
Its low-level constructor with a custom minter skips the normal handshake. Re-evaluate
only when the SDK exposes the required production seam, or when a bounded adapter can
use public low-level Connection/generated types while deleting more code than it adds.

Muse slices:

```text
P0  freeze new work on Node bridge.13; inventory concrete Rust 0.1.2 work
P1  split Node checkpoint into SessionRecord, NativeEffectRecord,
    exact StoreDelivery and latest Observation
P2  keep explicit Operation-derived command IDs and targeted reconciliation
P3  keep native Goal/turn/reply/config/result/usage through the same connection
P4  use module-owner custody and bounded close; remove duplicate process owner logic
P5  finish/export Rust work, then delete crates/swarm-adapter-muse, Cargo member,
    disabled descriptor, custom wire/schema and Rust-only fixtures
P6  upgrade Node SDK/host only as a separate exact-version, live-qualified transition
```

Do not translate Node and Rust private journals or relabel historical receipts.

### Q — OpenCode

Retain standalone Rust as the only new-work executor after the current state-machine
audit. Use durable inbox/message identity, process-global per-session owner and local
SQLite/log readback. Remove built-in host executor, `legacy_worker_demand`, public
loop-step/MCP phases and duplicate journals only after exact result-parent and
service-restart behavior is settled.

## 14. Goals, observation and product deletion

<a id="passive-agent-observation"></a>

Harness Goal is native and default where supported. Eliot Goal is an independent
Store plugin that creates ordinary scheduled/event `Send` Operations. Either or both
may be active. Remove vendor branches and Goal-specific continuation proof protocols.

Passive observation is core. Configured provider readers run without Task/binding;
Host adds OS/Git/GitHub facts; Store joins assigned and unassigned sessions. Reads
never contact the provider. Process liveness, native activity, connection, Task state,
Git and quota remain separate with source times and gaps.

Remove from compact-core completion:

- Concilium/council;
- generic contract/integration ratification/ACK;
- automatic Issue-label/publication projection;
- document-edition rights and Legacy acceptance branches;
- retired installers/provenance matrices;
- duplicate executor generations and worker implementation dependencies.

Retain ordinary messages, independent review, explicit accepted-candidate publication,
hooks/scripts as optional integrations and the local Governor view.

<a id="launch-and-results"></a>

## 15. Launch and results

Launch preview is optional. Store admits explicit settings/revisions, resolves one
launch plan under the Operation and references it from workspace/dispatch. Delete
caller-supplied preview/plan hashes.

A prepared workspace, current assignment/binding and usable native session permit
first input. MCP discovery is asynchronous observation.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

The adapter retains an unacknowledged page until Store ACK. A file without committed
receipt is not a result. Do not add another result reader or assembler.

<a id="current-format-and-existing-data"></a>

## 16. Finite transition

Backward compatibility is not a product feature. Before deleting a current executor
or format, enumerate concrete unsettled Operations and required retained results. For
each: finish/read back with its current owner, export the result, or create a new Task
revision/Attempt. Then delete the producer, reader, config, fixture, package and CI
branch together.

Do not write a universal converter. Historical completed records do not keep an
executable alive.

<a id="11-build-install-ci-and-qualification-reduction"></a>

## 17. Build, install and qualification

One current package contains one manifest, executable, descriptor and required
resources. Verify selected bytes once at install/launch. Runtime hello verifies
authenticated binding, protocol major and required semantic capability subset.

Delete checkout/compiler/cache-path equality, per-artifact PowerShell validators,
repeated local hashes and proof-packet qualification. Qualification calls public APIs
and verifies observable behavior for the selected current route.

<a id="verification"></a>

## 18. Verification

Run formatting and minimal warnings-denied Clippy/build for changed production
packages during construction. Run focused connected cases when the vertical slice
closes; broad product/live qualification follows the finished product.

Minimum connected proofs:

- exact same logical request is idempotent; changed request conflicts;
- uncertain native input is never resent by transport guess;
- stale binding/boot outcome fails; exact Store delivery redelivers safely;
- native and Eliot Goals work independently and together;
- unbound sessions and source gaps remain visible without provider calls;
- queue/admission cannot duplicate Attempt/launch and blockers cannot starve later work;
- overlapping write leases block, disjoint scopes proceed;
- review B cannot reuse review A; all findings return together; no-progress stops;
- PID reuse cannot kill unrelated process;
- removed paths have no config, schema, policy, fixtures, package or fallback wrapper.

Report net owned Rust + JS + glue separately from tests.