# Refactoring — distilled source findings

Runtime source and the first Antigravity implementation slices were rechecked through
`eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` on 2026-10-20.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the work order;
[status](../implementation-status.md) records completed source changes.

The central defect is not line count alone. Eliot repeatedly represents one authority,
state or recovery decision as several durable products: a Store row, a JSON receipt, a
proof packet, a workflow state machine, a cursor family, a compatibility profile and a
qualification fixture. Refactoring must remove the duplicate responsibility, not only
move files or rename types.

## Implemented reductions

### Duplicate host CLI

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines.

### Antigravity A0

Commit `cba98b755ff8d93405f91751a00d6dd6d91bbb60` synchronized active Rust
Antigravity package/tooling selection on artifact version 6. Historical v5 receipts
remain history; they are not a current compatibility branch.

### Antigravity A1a

Commit `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` removed the terminal receipt's
dependency on Store observation ID and duplicate `local_execution_results` evidence.
The production Rust diff is 106 additions and 371 deletions, net −265 lines.

A1 is not finished. Dispatch still requires a refresh-generated proof packet, and the
adapter still retains unacknowledged outcomes/results only in memory.

## Active Antigravity findings

### Terminal identity is now self-contained

The current validator binds a terminal receipt to the exact Operation, binding
generation, adapter boot, native conversation/scope, result ordinal, response digest
and terminal status. Observation is current-state data rather than terminal evidence.

This is the correct boundary and must remain.

### Dispatch proof is still a second authorization system

`AntigravityDispatchSessionProof` remains in shared contracts. `agent.refresh`
constructs it, launch admission selects the latest settled refresh, validates the
proof against binding/opening/snapshot state, hashes assignment/lease/proof packets and
blocks dispatch when the packet is unavailable or stale.

Store already owns assignment, Attempt, binding/generation, workspace lease, current
adapter boot and Operation. Model/readiness/stream counters are observations. A1b must
delete the DTO, producer, validator, packet digests and refresh-before-dispatch gate.

### Store delivery is still crash-unsafe

The Rust adapter's `pending_receipts` and completed-turn mapping are bounded
same-process caches. Once a terminal outcome is formed it is valid without an
observation, but process exit before Store ACK still loses that outcome/result.

A2 must persist only the exact already-formed `module.outcome`/`module.result`
parameters under binding + generation + adapter boot. It must never persist or replay
the native prompt/process intent. A new boot cannot silently inherit an old boot's
unsent records.

### Historical warm bridges remain in the validator

The shared warm-route validator still recognizes retained Node bridge artifacts in
addition to Rust. This is a finite-transition concession, not the final runtime
boundary. Concrete unsettled work must be disposed, then old producers and active
selection branches removed.

## Adapter and runtime causes

### Vendor methods leaked into common/public protocols

OpenCode loop-step/MCP phases and Command cancel/close/release-evidence appear in
contracts, policy, schemas, capacity and fixtures. They are semantic Send/Cancel/
Shutdown actions or adapter internals. Every leaked method multiplies the whole stack.

### Package metadata became behavior

Store and adapters compare artifact versions/builds, protocol and command/event schema
inventories at runtime. Optional behavior creates another profile and compatibility
branch. Package identity belongs to installation/inventory; authorization belongs to
Store objects and semantic capabilities.

### Bootstrap and Store delivery are duplicated

Codex, Claude, OpenCode, Muse, Command and Antigravity repeat owner/env/credential
parsing, hello/claim equality, command pulling, reconnect and Store-delivery retry.
Extract shared plumbing only after two current routes use the same behavior and delete
their old loops in the same change.

### Receipts echo Store-owned identity

Package/protocol, binding/generation, request hashes, Task/Attempt/revision,
source/prompt hashes and nested receipts recur through dispatch/outcome/result. Store
created the Operation and authenticated the binding. Keep stale boot/generation guards,
native identity/disposition and hashes of actual immutable bytes.

### Parallel executor generations dominate code size

| Runtime | Current duplication | Target |
| --- | --- | --- |
| OpenCode | built-in host + standalone Rust | standalone Rust only |
| Muse | active Node + disabled custom Rust | simplify Node, then deliberate future replacement |
| Codex | current Rust + retired Python/profile history | one Rust app-server route |
| Claude | Rust/SDK + Node generations | one Rust adapter |
| Antigravity | retained Node generations + Rust | one Rust route after transition |
| Command | Batch v3/v4/v5 + ACP artifact generations | one Batch + one stable ACP v1 route |

Historical completed records do not keep an executor alive.

### Runtime-specific persistence is universalized

Claude, Codex, Command, Muse and OpenCode journals/checkpoints combine native intent,
Store delivery, result pages, observations, callbacks, startup inventory, references,
tombstones, retention and compaction. Do not create a shared universal journal.
Separate only the real lifetimes:

```text
SessionRecord
NativeEffectRecord
StoreDelivery
ObservationCheckpoint
persisted/readable result when required
```

## Workflow and authority duplication

### WorkDispatch is another workflow engine

It copies Task blockers into global/per-entry cursors, pending subjects, recheck state,
semantic slots, links and quarantine. A bounded current-state queue plus one atomic
`dispatch_once` is sufficient.

### Advisory code scope duplicates workspace authority

`code.scope.*` has proposals, modes, symbols, digests, override, expiry, indexes and
conflict pages but does not lock files or launch work. Task path prefixes plus one
Attempt workspace lease already provide the write authority and overlap guard.

### Capacity copies Operation lifecycle

Capacity materializes Operations/producers into phases, evidence and meta indexes and
counts read/control methods as execution. Count unresolved root opens and unreleased
root bindings transactionally; `report.capacity` is a projection.

### Provider state is a proof matrix

Shared DTO/Store code hard-codes Codex/Muse methods, artifact families, auth hashes and
repeated route/binding/model coordinates. Stale source bindings can impose sticky
holds. Use source-scoped usage/availability with explicit freshness and coverage.

### Scheduling is triplicated

In-process scheduler, standalone scheduler service and automation loop overlap; legacy
worker slots add another lifecycle. One in-process Store-change/nearest-due loop admits
ordinary Operations. Workers remain only for external I/O/custody.

### Review/correction is another workflow family

ReviewDisposition, RepairDispatch, semantic slots, links and separate Acceptance
repeat one candidate/review/manager relation. Keep `review.submit`, one
`task.request_changes` transition and the common candidate-specific acceptance gate.

### Automatic GitHub projection is another workflow engine

Activation watermark, global/per-entry cursor, catch-up, quarantine/recent state and
effect-drain cursor project acceptance into labels. Remove it. An explicit configured
integration action can follow acceptance without owning another lifecycle.

## Store, API and frontend causes

### Public API ownership is fragmented

Method policy, MCP catalog/profiles/schemas, mutation validation, Store role routing,
apply/read dispatch, participant exceptions and visibility SQL each encode part of the
surface. Each retained domain needs one typed parser/authorizer/handler. Public,
internal worker and adapter protocols must be separate.

### Store migration validates SQL text

Base-schema SHA, extension digests, reference DB/DDL lexer and duplicate trigger
verification protect historical source text rather than compatible state. Use
application ID, numeric `user_version`, ordered transactional migrations and bounded
postconditions. Convert only concrete retained data once.

### MCP serializes a multiplexed client

`swarm-client::Client` already supports correlated concurrent RPC, while MCP keeps a
mutex around its client through awaited calls. Clone the current client and release the
connection mutex before RPC. One slow read must not block unrelated tools.

### Subscriptions multiply pollers

MCP can create a poller, forwarder and queue per subscription, repeatedly paging the
same durable history. Use one coalesced wake pump; Store cursor/history remains replay
authority.

### `tasks/get` scans global attention

The frontend pages fleet-wide `report.attention` and filters locally for one addressed
Operation. Add an exact pending-input read.

### Ephemeral native streams occupy the Store writer

In-memory ring operations run through `Store::run` despite changing no business rows.
Authorize in a short snapshot, then operate the bounded ring outside the writer.

### TaskPrompt sends controller internals to the model

Identity JSON, canonical brief, launch packet and hashes are injected into the model
and echoed through adapters/results. The model needs current goal, requirements,
sources, scope, workspace, deliverable and verification. Store retains identity.

## Observation causes

### Governor inventory is binding-rooted

`agent.list/state/family` and runtime catalog start from ready live bindings. Native
sessions/processes without current Task/Attempt/binding disappear. Discovery must
start from stable local/provider sources; assignment is an edge.

### Process and workspace status are not live joined facts

Process metrics is a receipt-driven Windows CLI for at most eight selected identities,
with no Store join and repeated image verification. Supervisor public state omits
PID/birth/group. Workspace lease exposes preparation-time baseline/clean evidence;
lifecycle reconciliation performs no filesystem/Git read.

Use one process collector and one worktree collector with source time, freshness and
gaps. Package bytes are verified at install/launch, not every metric sample.

### Remote state exists only inside publication

Forge has exact remote-ref readback for uncertain pushes, but no independent bounded
remote-ref cache. Governor cannot show remote head before publication.

### Provider usage is binding/artifact-oriented

Usage is stored inside one binding observation and provider conditions retain source
binding/artifact evidence. Shared account/service state is copied or remains
ambiguous. Source-scoped provider records must exist independently of assignment.

### Diagnostics and reports are parallel partial views

Optional observer, agent views, reports, runtime catalog and process metrics each
assemble a different partial projection. They need one joined current-state Store
projection and addressed cursor invalidations; frontends do not reduce native events.

## External integration causes

### GitHub source polling is a workflow and has an unsafe cursor

`github.source.poll` is a GET represented as queued/sending/outcome_unknown Operation,
source lease, cancellation and recovery. It pages Issues sorted by mutable
`updated desc` and persists page number across polls. Items can move between pages,
creating cross-poll gaps.

Collection should start from page one, advance to a stable high-water/source cursor,
retain explicit partial coverage and periodically reconcile.

### GitHub collection mutates Tasks

One transaction commits source facts, creates/revises Tasks, records mapping conflicts,
updates a work pool and settles the poll. Source observation and Task import are
separate responsibilities. Commit source facts first; import selected items explicitly.

### GitHub work pool duplicates Eliot queue

Selection rows and order form another queue. Imported Tasks enter the ordinary Eliot
queue/lane.

### GitHub writes have two effect engines

Managed labels use a direct `gh` child. PR description uses `swarm-forge-worker` and a
second private job/custody protocol. Preserve the correct semantic rule—durable sending,
one write attempt, exact readback after ambiguity—but keep one small GitHub API effect
owner. Forge remains remote Git push only.

### Mailbox is stored inside Operation JSON

Mailbox lookup searches settled `operations.result_json` across several methods and
needs JSON expression indexes plus corruption sentinels. Normalize bounded message
rows by recipient and monotonic cursor.

### One message has too many identities

Legacy message uses Operation ID; typed coordination adds `cmsg-*` and a random
delivery ID while client request ID remains idempotency. Use the admitted Operation ID
as the sole durable message ID; replies/cancel reference it.

### Coordination is a second collaboration platform

Legacy/typed messages, consult, threads, participant registry, work/contract cards,
contracts, integration ACK, watches, Concilium and code scope overlap. Keep one mailbox
and, only if proven necessary, a minimal Task-scoped grouping record. Consultation is
a message; review is review; write conflict is a workspace lease; watch is a
subscription/automation condition.

### Gateway claims exceed secret-file enforcement

Gateway correctly uses loopback, fixed profile, bearer auth, Host/Origin checks and
bounds, but ordinary file reads do not enforce regular file, private mode/ACL or
symlink/reparse rejection. Use the shared private bounded file primitive.

### Gateway tenancy is one identity

One Eliot credential/profile/bearer is shared by many connections. Those sessions are
one Eliot principal and request namespace. One process/listener must equal one remote
identity/profile, with bounded active sessions and idle lifetime. Edge TLS/auth remains
outside.

## Selected decisions

- Keep one current executor per harness; no historical runtime compatibility product.
- Keep native Goal and Eliot Goal independent.
- Keep exact result-page retention until Store ACK.
- Keep one remote Git effect owner and one GitHub API effect owner; no universal
  EffectRunner.
- Keep one normalized mailbox; delete the coordination workflow platform.
- Keep passive Governor observation independent of provider/model calls.
- Keep the gateway small and single-tenant.
- Do not add a Lane entity, ORM, broker, universal journal, permanent converter or
  adapter-framework crate.
- Octocrab is optional; duplicate effect ownership is the defect, not `gh` itself.

## Next connected source changes

```text
A1b  delete Antigravity proof DTO/producer/validator/digests/refresh gate
A2   add exact crash-safe Store-delivery spool
A3   remove unsupported Antigravity commands and retained active profiles
B0   build one current queue projection
```

Every slice must delete its replaced schemas, policy entries, fixtures, configuration
and package/tooling branches in the same change and report net production Rust, JS and
glue separately from tests.
