# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source and active tooling were rechecked at
`513f9698c268753dc6f2a6c0cf4d9a53f7f5e472` on 2026-10-18.
[Findings](refactoring.md) preserve the evidence; [status](../implementation-status.md)
records completed work. This document describes what to change, where to change it
and what old mechanism must disappear.

Do not start another repository-wide framework migration. Implement one complete
caller-to-result slice, delete the replaced route, then use the second real route to
prove any shared abstraction.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Work as vertical slices

Every coding slice must state:

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

Migration is route-by-route. An unmigrated route may temporarily remain on the current
protocol, but receives no new compatibility profile. A migrated route admits no new
work through its old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Shared semantic adapter commands and bounded records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` |
| Adapter process membership and private durable files | `crates/swarm-process::module_owner` and `permissions` |
| Native transport, session state, local provider readers and uncertain native effects | selected `swarm-adapter-*` or `modules/*-rust` implementation |
| Process/worktree/Git collection | current host lifecycle/process owners |
| Enabled helper scheduling | one `swarm-automation` service with Store-owned due state |
| Public CLI and MCP | `swarm-cli` and `swarm-mcp`, both calling Store handlers |

No adapter accepts a Task, changes ownership or interprets review. No common shell
owns a vendor session, native retry policy, scheduler or transcript store.

## 3. First executable route: Antigravity Rust

Antigravity is the first route because it has one serialized native input window and
a concrete lost-delivery defect. Do not start the common shell with Command, Codex or
Muse: their profile ladders and native state machines would define the abstraction
around the hardest legacy code.

### A0 — Synchronize the active Antigravity coordinate

Current source and descriptor use artifact version `6`, while active route/tooling
selectors still name `5`.

Update together:

- `modules/antigravity-rust/route.example.toml`;
- `tools/ci/module-package-policy.json`;
- `tools/qualification/New-NativeQualification.ps1::Get-AdapterContract`;
- `tools/modules/Install-ModuleArtifact.ps1` narrow Antigravity validator;
- active catalog/qualification fixtures that select new work.

Do not rewrite historical v5 receipts under `docs/remediation`. Store may read them as
history during the finite transition, but new installation, selection and
qualification use v6 only. Remove the `"5" | "6"` active-behavior branch when no
unsettled v5 binding remains.

Completion: the shipped descriptor, Rust constant, package policy, installer and
qualification select one active version; no fallback silently chooses v5.

### A1 — Make Store delivery durable without replaying native input

Current Antigravity `Controller::pending_receipts` is an in-memory queue. A terminal
native result can be observed, the worker can exit before `module.outcome` ACK, and
Store remains `outcome_unknown`; restart cannot reconcile because the receipt died
with the process.

Implement a route-local `DeliveryOutbox` first. Use existing private durable file
primitives in `swarm-process::permissions`; do not add a database or a generic event
journal.

Scope:

```text
binding_id + generation + adapter_boot_id
```

Record only already-produced Eliot deliveries:

```text
delivery_id
lane: one Operation or one observation source
kind: outcome | observation | result
exact bounded serialized envelope
created sequence
```

Rules:

1. Persist the exact envelope before the in-memory owner can forget it.
2. Retry only the byte-identical `module.outcome`, `module.observe` or
   `module.result` request after Eliot-link reconnect.
3. Delete only after an exact Store ACK.
4. Preserve order within one Operation/result-page lane or observation source; do
   not impose a global order on unrelated lanes.
5. Stop pulling new commands before capacity is exhausted and reserve space for the
   terminal record of admitted work.
6. A new adapter boot does not automatically drain another boot's files. It exposes
   them for explicit recovery/readback or finite disposition.
7. Never store or replay the native prompt, process launch or session command in
   this outbox.

Edit:

- `modules/antigravity-rust/src/controller.rs`;
- `modules/antigravity-rust/src/main.rs`;
- a small route-local outbox module;
- `modules/antigravity-rust/tests`;
- only the `swarm-process` exports needed to reuse existing private-file primitives.

Keep the outbox route-local until a second current adapter demonstrates identical
Store-delivery ownership. Do not move it into `swarm-client` in this slice.

Completion cases:

- native prompt is written once;
- Eliot disconnect after terminal observation leaves one durable envelope;
- same boot reconnect sends the identical bytes and ACK removes them;
- process restart does not convert the record into a new native execution;
- replacement boot cannot silently adopt or delete predecessor records;
- result pages survive lost ACK and remain associated with the original Operation;
- net owned code includes deletion of the old in-memory-only delivery path.

### A2 — Remove the Antigravity proof packet

The current dispatch path requires `agent.refresh` to generate
`AntigravityDispatchSessionProof`, hashes the native scope, repeats model/stream/event
state, stores it in an Operation, revalidates it against the binding, then hashes the
proof again inside a dispatch packet. This is observation data turned into a second
authorization system.

Replace it with ordinary current facts:

- current assigned Task/Attempt and binding generation;
- binding is ready and unreleased;
- adapter boot owns the binding;
- native session identity is known;
- one serialized input window is free;
- expected Task/workspace revisions still match.

Move model, stream phase, event/gap counters and native readiness to `agent.state`.
They remain useful diagnostics but do not authorize dispatch.

Delete in one connected change:

- `AntigravityDispatchSessionProof` and constants from `swarm-contracts::runtime`;
- proof generation from `modules/antigravity-rust/src/controller.rs`;
- `launcher_dispatch/antigravity_capability.rs` proof lookup, validation and digest
  packet;
- refresh-before-dispatch qualification logic and proof fixtures;
- hashes/digests whose only purpose was binding the proof packet.

`agent.refresh` remains an explicit safe observation/readback action. It no longer
mints permission to dispatch.

Completion: a ready current binding can receive its first assigned input without a
fresh refresh; stale assignment/generation/workspace still fails; diagnostics remain
visible; no proof DTO, Store validator or qualification gate survives.

### A3 — Give Antigravity only its real semantic actions

The route supports:

```text
Open
Dispatch
Send(next_turn)
Refresh
Result
Reconcile
Shutdown/release of the owned adapter/native process when explicitly requested
```

Do not advertise or implement placeholder branches for `Reply`, `Configure`, `Goal`,
`Background`, `Recover` or live steer. Unsupported actions fail at Store routing from
capabilities before `module.next`; the adapter does not carry rejection handlers for
features it never declared.

Replace string method switching with the current typed semantic command for this
route. Collapse `OperationIdentity` and per-outcome `ModuleReceiptIdentity` to facts
Store cannot already derive:

- Operation ID;
- binding generation and adapter boot needed to reject stale reports;
- native conversation/input/result identity when observed;
- disposition and bounded action details.

Keep hashes on immutable result/artifact bytes. Remove artifact version, protocol,
request hash and duplicated Task/source hashes from every outcome.

Completion: adding or removing one optional action changes the Antigravity capability
value and adapter match only; no artifact profile, command/event schema family or
Store vendor branch is added.

## 4. Extract shared bootstrap and delivery only after proof

### Bootstrap threshold

Antigravity and Claude's contained
`crates/swarm-adapter-claude/src/module_runtime.rs::OwnedBootstrap` are the first two
candidates for one reader in `swarm-process::module_owner`.

The shared reader may return only:

- verified owner/worker membership;
- binding ID and generation;
- module client/credential identity;
- private state directory;
- selected protocol major and required semantic actions;
- typed host connection configuration.

It does not parse vendor options, launch native processes or compare artifact/schema
matrices. Convert both adapters and delete their duplicate env/path/protocol/claim
parsers in the same change.

### Eliot link threshold

`swarm-client::ModuleLink` already owns authenticated hello, `module.next`, outcome,
observation and result calls. Replace Antigravity's private `ManagerLink` with it only
when the route-local outbox is working and the hello contract has been reduced.

A common adapter loop may later own:

- connect and one hello per physical Eliot link;
- link reconnect;
- typed command pull;
- exact delivery of a route-owned durable envelope;
- bounded link shutdown.

It may not own native session state, provider retries, result association, Task state,
timers or scheduling.

A common `DeliveryOutbox` moves into `swarm-client` only when a second migrated route
uses the same record and deletion semantics and both previous loops disappear. Until
then Antigravity's outbox remains local.

### Why Command is not the second proof

Command currently contains Batch v3/v4/v5, ACP v1/v2, alternative capability/schema
sets, native cancel/close method aliases, a large journal and ACP queue/session state.
First retire its historical profiles and choose the retained batch/ACP product. Do
not let that legacy matrix define shared bootstrap or outbox abstractions.

<a id="p1--replace-the-module-compatibility-matrix-with-protocol-2"></a>

## 5. Semantic adapter protocol, route by route

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
rights remain the permission boundary. Vendor RPC names and payloads stay inside the
adapter.

Remove public/common internal methods as their route migrates:

- `native.opencode.loop_step` → `Send { delivery: steer }`;
- `native.command.cancel_turn` → `Cancel { target: turn }`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → internal cleanup;
- `native.mcp.*` → OpenCode adapter lifecycle/observation.

`Recover` remains a real semantic action where supported. It restores a known
session/binding after confirmed loss and never resends Task input. `Reconcile` only
reads an uncertain effect; `Open` selects/attaches a session.

Each migrated route uses one current protocol major and a compact capability subset.
Package version/build and schema inventories are installation/diagnostic facts, not
behavior branches. Hello verifies authenticated binding, protocol major and required
capability subset; it does not echo/compare the complete descriptor on both sides.

<a id="processes-and-frontends"></a>

## 6. One executor per retained runtime

| Runtime | Retained direction | Delete after finite disposition |
| --- | --- | --- |
| Antigravity | Rust serialized warm stream | Node bridge and proof packet after consumers/package paths switch |
| OpenCode | standalone Rust adapter | built-in host executor, legacy demand and public native MCP phases |
| Codex | current Rust app-server adapter | Python executor/SDK, artifact-version templates and fixtures |
| Muse | choose Node or Rust by complete behavior and total Rust+JS+IPC code | rejected executor and duplicate usage normalizer |
| Claude | one Rust adapter with thin current SDK driver | retained Node generations and duplicate bootstrap/transport code |
| Command | one current batch profile plus one current ACP implementation only if both are product requirements | Batch/ACP historical profile ladders and method aliases |

Historical completed records do not keep an executor alive. Enumerate concrete
unsettled Operations, finish/read back/export them or create a new Task
revision/Attempt, then delete producer, package, fixture, configuration and reader.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

## 7. Keep both Goal mechanisms simple

Harness Goal is the vendor-native mechanism and default where supported. The adapter
maps set/edit/pause/resume/clear and associates native turns with the initiating
Operation and Task/Attempt.

Eliot Goal remains a Store plugin that emits an ordinary `Send` Operation on a chosen
event or cadence. Configuration needs only message/template, trigger/cadence and
delivery mode. Replaying one occurrence returns the same Operation; a later
occurrence may intentionally repeat the same text.

Remove provider branches, OpenCode pseudo-Goal, objective approval hashes and
Goal-specific continuation proof/receipt families. Plugin disable, native Goal pause,
turn cancellation and queued-input removal are independent.

<a id="passive-agent-observation"></a>

## 8. Passive observation and quotas

One bounded host collection lifecycle runs independently of automation and optional
diagnostics. Adapters fold native events and provider-local JSON/JSONL/SQLite; host
adds OS process and Git facts; Store joins:

- Task/Attempt/Operation and binding when known;
- native session/turn/child identity;
- adapter and shared provider-service process identity;
- assigned and actual worktree/branch/HEAD/upstream/push target;
- independently observed GitHub repository/ref/head;
- local source/cursor/gaps;
- run usage and account quota snapshots.

Configured sources run without a Task/binding. Unmatched sessions remain inventory.
`agent.list`, `agent.state`, `agent.family` and `agent.usage` read Store without
provider calls.

Keep assignment, native activity, pending wait, connection, process liveness, source
freshness, Git state, Task completion and quota separate. Never kill/restart/reassign
from age, low CPU, old provider status, unchanged branch, PID alone, newest file or
title/path regex.

Bound bytes, records and elapsed work. Preserve complete-record cursors and a bounded
unfinished suffix. Failed discovery is a visible coverage gap, not an empty fleet.
SQLite readers use short read-only WAL-visible connections. Remove per-sample
executable hashing and share one service's process/quota facts across its sessions.

Fix startup so every unreleased binding loses stale connection health; preserve
source identity in timeline/follow; validate usage by the selected adapter/source,
not artifact versions. Malformed optional usage cannot discard valid activity or
result observations.

<a id="launch-and-results"></a>

## 9. Launch, results, review and acceptance

Launch preview is optional. Store admits explicit settings and expected revisions,
resolves one launch plan under the Operation and references it from workspace and
dispatch. Remove caller-supplied plan hashes and MCP-readiness gates from first input.

Keep one result path:

```text
Result -> adapter page -> module.result -> immutable artifact
       -> artifact.read / artifact.assemble -> submission
```

Associate output with explicit native parent/input identity or one serialized admitted
turn window. Changed generation, a second independent input or an unclassified stream
gap requires readback; never choose the newest assistant message by guess. The exact
page remains in delivery outbox until Store ACK. `artifact.assemble` is the only
multi-page assembler.

Manual and automatic acceptance use the same candidate-specific retained review/check
gate. Remove caller-authored duplicate proof arrays. Candidate B cannot reuse A's
review; checks-only policies remain valid.

<a id="store-and-data-model"></a>
<a id="store-and-automation-implementation-details"></a>

## 10. Store, automation and product deletion

Keep one Store writer. Move pure bounded query families to the existing WAL reader
pool. Authorization, page data and journal cut use one short snapshot. Never hold a
transaction across native/file/Git/network I/O or client pagination.

Replace base-SQL text SHA equality with application identity, current schema version
and explicit bounded migrations. Move indexed identity/state/due fields out of
`meta`; bounded payload JSON may remain in its owning row. No ORM, EAV store or second
migration runner.

Give automation an immutable `entry_id` independent of owner/name. Update owner and
revision in one transaction; journals/occurrences retain the ID. Delete successor
copies, relocation paths, transfer chains/cycle logic and parallel schedulers. One
`swarm-automation` service evaluates due work; Store commits due identity and action
disposition.

Remove from compact-core completion:

- Concilium/council state machine;
- generic integration negotiation/ratification/ACK engine;
- automatic publication/Issue-label projection;
- document-edition rights and `LegacyV1` acceptance branches;
- host dependencies on worker implementation crates after needed contracts move.

Retain explicit accepted-candidate publication, ordinary messages, independent
review, participant grants and manual optional integrations.

## 11. Build, install, CI and qualification reduction

Current tooling duplicates product contracts in PowerShell, JSON policies, workflow
matrices and Rust. It also treats checkout, compiler and build-cache identity as
runtime compatibility.

Target one package path:

1. Cargo builds one selected binary and required resources.
2. A compact package manifest names package coordinate, protocol major, semantic
   actions, executable and required resource paths. Build commit/toolchain may be
   recorded as diagnostics.
3. Installer checks path containment/reparse safety, protected credential references
   and selected package bytes once, then atomically publishes one package directory.
4. Supervisor registers the installed descriptor.
5. Runtime hello checks authenticated binding, protocol major and required action
   subset.
6. Qualification exercises behavior through the public API and records observable
   facts. It does not re-prove source checkout, compiler, Cargo.lock or target-cache
   equality.

Delete as each migrated route/package no longer needs them:

- per-artifact PowerShell validators and exact capability/schema arrays;
- host/module same commit/tree/toolchain/target-directory requirements;
- repeated build-manifest and receipt hashes whose only purpose is comparing the
  same local files again;
- retired frontend `node_modules` inventory and
  `Test-InstallFrontendLegacyCompat.ps1` from normal CI;
- duplicate standalone installers;
- qualification logic that blocks dispatch on preview hashes, refreshed proof packets
  or provider catalog body hashes.

Keep real safety: private credentials, path containment, no reparse traversal where
used, bounded regular files, atomic publication, one install/launch byte verification
and honest uncertain outcomes.

Reduce ordinary CI to:

- changed Markdown/link/whitespace validation;
- rustfmt for changed Rust files;
- `cargo check`/warnings-denied Clippy for affected packages and real local reverse
  dependencies derived from `cargo metadata`;
- syntax check for changed JS;
- focused connected test targets once a product slice closes.

Full native/product qualification is an explicit release/manual gate, not a growing
list of incident-specific workflow modes. Remove a focused mode when its permanent
behavioral test lands.

<a id="current-format-and-existing-data"></a>
<a id="store-and-current-data"></a>

## 12. Finite transition

Backward compatibility is not a product feature.

For each migrated route or format:

1. enumerate concrete unsettled Operations and required results;
2. finish/read back/export with the current owner or create a new Task
   revision/Attempt;
3. stop admitting new work through the old producer;
4. switch active configuration/package selection;
5. delete old producer, reader, schema branch, fixture, package and fallback;
6. keep completed historical records only as opaque history when current projection
   already contains the needed facts.

Do not build generic converters for hypothetical old data and do not branch on
artifact version inside one protocol major.

<a id="verification"></a>

## 13. Verification

During construction run formatting and minimal affected warnings-denied Clippy/build
or syntax checks. Do not create a broad matrix around an incomplete route. When a
connected slice closes, prove:

| Boundary | Required proof |
| --- | --- |
| Antigravity A0 | all active selectors use v6; historical v5 receipts are unchanged; no active fallback silently selects v5 |
| Antigravity outbox | prompt written once; terminal envelope survives disconnect; same boot resends exact bytes; ACK deletes; replacement boot cannot silently adopt |
| Proof removal | current ready binding dispatches without refresh proof; stale assignment/generation/workspace still fails; proof DTO/validator/fixtures are gone |
| Typed actions | route exposes only implemented actions; removed methods have no Store policy, capacity, descriptor, adapter or docs path |
| Bootstrap/link | Antigravity and Claude use one owner reader; old env/claim parsers are deleted; shared link owns no native policy |
| Route migration | one route works admission→native action→result/readback; old producer is disabled for new work before the next route starts |
| Goals | native and Eliot Goal work alone or together; pausing/disabling one affects only that mechanism |
| Observation | unbound session, shared service, surviving process, changed worktree and stale remote ref remain distinct without provider calls |
| Results/review | result binds to the exact Operation; stream gap stays unresolved; candidate B cannot reuse A's review |
| Store/automation | restart/lost wakeup retains admitted work; WAL reads are bounded/authorized; transfer preserves one entry; old schedulers/keys are gone |
| Tooling | installed package runs with different checkout/toolchain/cache provenance; qualification tests behavior rather than source equality |
| Deletion | removed path has no config, package, CI, docs, fixture, compatibility alias or fallback wrapper |

Every completed source slice updates [status](../implementation-status.md) with
behavior, evidence, removed files/branches and net owned Rust + JS + glue separately
from tests.
