# Refactoring — Source Findings

Runtime source was rechecked through
`85b40e63a9225db05a480c027cca49f4913e4540` on 2026-10-20.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the work order;
[status](../implementation-status.md) records completed source changes.

Source presence is not a requirement to preserve a subsystem. The dominant defect is
not raw line count by itself: the same authority, state and recovery decision is
reimplemented as multiple durable products.

## Concentration

| Owned Rust scope | Physical lines after CLI deletion | Explicit test-only estimate |
| --- | ---: | ---: |
| All owned Rust, excluding `vendor/` | 533,720 | 172,511 |
| `crates/swarm-kernel-host` | 365,726 | 128,996 |

Counts include blanks/comments. Commit `25a8e3b` removed the duplicate host CLI/MCP
launcher: `main.rs` 1,949 → 743 lines, net −1,206 Rust lines. Later commits in the
current audit series change documentation only unless stated otherwise.

## Confirmed structural causes

### 1. Vendor/internal methods leaked into common/public protocols

OpenCode loop-step/MCP phases and Command cancel/close/release-evidence appear in
contracts, Store policy, schemas, capacity and fixtures. They are semantic
Send/Cancel/Shutdown or adapter internals. Every leaked method multiplies the entire
surface.

### 2. Package metadata became runtime behavior

Store/adapters compare artifact version/build, protocol and command/event schema
inventories at runtime. Optional behavior therefore creates new package profiles and
compatibility branches. Package identity belongs to installation/inventory; rights
belong to Store objects.

### 3. Adapter bootstrap and Store delivery are repeatedly implemented

Codex, Claude, OpenCode, Muse, Command and Antigravity duplicate owner/env/credential
parsing, hello/claim equality, command pull, reconnect and Store-delivery retry.
`swarm-process` and `swarm-client` already own the useful low-level boundary. Extract
only after two current routes delete their old loops.

### 4. Receipts echo Store-owned identity

Package/protocol, binding/generation, Operation/request hashes, Task/Attempt/revision,
source/prompt hashes and nested receipts recur through dispatch/outcome/result. Store
created the Operation and authenticated the binding. Retain only stale boot/generation
guards, native identity/disposition and hashes of actual immutable bytes.

### 5. Proof packets became a second authorization system

Antigravity session proof, Goal continuation proof and OpenCode MCP protected packets
are observations or adapter internals. They cannot grant dispatch rights.

### 6. Antigravity can lose a completed result

Terminal native state is kept in memory and the outgoing outcome depends on a later
Store observation ID. Worker exit can leave Store `outcome_unknown` after native
completion. First make outcome self-contained; then persist exact already-formed
Store params. Never persist/replay native prompt in that spool.

### 7. Parallel executor generations dominate deletion potential

| Runtime | Current duplication | Target |
| --- | --- | --- |
| OpenCode | built-in host + standalone Rust | standalone Rust |
| Muse | active Node + disabled custom Rust | active Node, then deliberate future replacement |
| Codex | current Rust + retired Python/profile history | one Rust app-server route |
| Claude | Rust/SDK + Node generations | one Rust adapter |
| Antigravity | Node generations + Rust | one Rust route |
| Command | Batch v3/v4/v5 + ACP v1/v2 artifacts | one Batch + one stable ACP v1 route |

Historical records do not justify keeping an executable.

### 8. WorkDispatch is another workflow engine

It copies Task blockers into global/per-entry cursors, pending subjects, recheck state,
semantic slots, links and quarantine. The fixed pending list can hide later eligible
work. Current Task/Attempt state plus bounded `dispatch_once` is sufficient.

### 9. Advisory code scopes duplicate the real write authority

`code.scope.*` has proposals, acceptance, modes, symbols, digests, overrides, expiry,
indexes and conflict pages but does not lock files or start work. Workspace lease
already owns Task/Attempt/worktree/path overlap before effects.

### 10. Process custody repeats package compatibility

Owner/resolver records repeat package ID/version/build, protocol and full contract in
addition to binding and process identity. Keep PID/birth/group/Job, boot, executable
selection and private references; remove runtime package negotiation.

### 11. Capacity copies Operation lifecycle

Capacity materializes Operations/producers into phases, evidence, unknown timestamps
and meta indexes and counts non-root controls. Root capacity is a transactional count
of unresolved root opens and unreleased root bindings.

### 12. Provider state is a vendor proof matrix

Shared DTO/Store code hard-codes Codex/Muse methods, artifact families, auth hashes and
repeated route/binding/model coordinates. Stale source bindings can impose sticky
holds. Use source-scoped usage/availability and honest freshness.

### 13. Three scheduling/control products overlap

An in-process scheduler, standalone scheduler service and automation loop handle the
same due/current state; legacy worker slots add another lifecycle. One in-process
Store-change/nearest-due loop is enough.

### 14. TaskPrompt sends controller internals to the model

Model text contains identity JSON, canonical brief and launch packet while envelopes,
admission and results repeat Task/Attempt/source hashes. The model needs current
work, sources, scope, deliverable and verification. Store keeps identity.

### 15. Review/correction is another workflow family

ReviewDisposition, RepairDispatch, semantic slots, links and separate Acceptance
repeat one candidate/review/manager relation. Keep `review.submit`, one
`task.request_changes` transition and the common acceptance gate.

### 16. Public API ownership is fragmented

Method policy, MCP catalog/profiles/schemas, mutation-field validation, Store role
routing, apply/read dispatch, participant exceptions and visibility SQL each encode
part of the public surface. No bypass was proven in this pass; drift/removal cost is
confirmed. Each retained domain needs one typed parser/authorizer/handler.

### 17. Store migration verifies SQL text, not compatible state

Base-schema SHA, extension digests, reference DB/DDL lexer and duplicate trigger
verifiers protect historical source text. Use application ID, numeric user version and
ordered transactional migrations. Bridge only concrete retained states once.

### 18. MCP serializes an already multiplexed client

`swarm-client::Client` supports concurrent correlated RPC. MCP holds a mutex around
`Option<Client>` through awaited calls, so one slow read blocks unrelated tools in the
same session.

Subscriptions create a poller, forwarder and queue per subscription and poll durable
history repeatedly. Use one coalesced wake pump; Store cursor/history remains replay
authority.

### 19. `tasks/get` scans global attention

MCP pages fleet-wide `report.attention` and filters locally to answer one Task/
Operation. Add an exact addressed pending-input projection.

### 20. Ephemeral native streams occupy the Store writer

In-memory stream publish/open/read/close runs through `Store::run` despite writing no
business rows. Authorize in a short Store snapshot and operate the bounded ring
outside the writer.

### 21. Checks still have two execution owners

Standalone `swarm-checks` and hidden host check-worker share the same lower-level
executor but duplicate bootstrap, pins, custody, packages and recovery. Require
standalone execution for new work; move bounded probes there; retain old receipts only
through finite readback.

### 22. Scripts have one executor but duplicate wire types

Host and worker independently define intentionally compatible Work/plan/start/result
shapes. Make `swarm-scripts` the one DTO owner and remove the hidden compatibility
entrypoint/readers after concrete old work is settled.

### 23. Forge mixes valid effects with duplicate custody/hashing

Local workspace Git and remote push are distinct legitimate effects. The Store owner
contains blocking Git closure custody plus native worker custody, while the worker
implements another capture/termination loop. Worker and Git binaries are rehashed per
Operation. Reuse `swarm-process`; verify packages at install/launch, not each effect.

### 24. Claude has two persistence engines around one SDK query

`lib.rs`, `journal.rs` and `interactions.rs` exceed 536 KB before other driver/state
files. Generic operation and interaction journals combine intent, Store outbox,
results, callbacks, startup inventory, tombstones, compaction and reference graphs.

Lost Store ACK for `module.result` is treated as unreplayable even though redelivering
identical Store params is safe. Terminal body is memory-only. Target is one live
session, minimal NativeEffectRecord, exact StoreDelivery, latest observation/pending
interaction and persisted/readable terminal result.

### 25. Codex rewrites one giant checkpoint per mutation

A 354-KB `lib.rs` serializes package/binding/session, all operations, effects, Goal
proofs, result pages, observations, up to 1 MiB native events, usage and retirement as
one fsynced snapshot plus SHA sidecar and tombstone tree. Events are represented in
memory, checkpoint and outgoing observation.

Split connection, session, per-effect, StoreDelivery and latest observation. Map
native Goal directly. Delete full snapshots, event mirrors, tombstones, Goal proofs,
profile ladders and retired Python execution.

### 26. Command contains two large persistence products and five profiles

Batch v3/v4/v5 and ACP v1/v2 encode compatibility into behavior. Preserve one Batch,
one stable ACP v1, `session/load` no-fresh fallback and exact option readback. Delete
profile/schema ladders, duplicate journals and `native.command.*` surface.

### 27. Muse custom Rust duplicates the official Node route

The active Node bridge already provides explicit Eliot-derived command IDs, targeted
readback, Goal, result and usage. Disabled custom Rust duplicates MSP wire/schema,
transport, fold, journal, process custody and results at far greater size. Keep and
simplify Node; disposition Rust work and delete it. Do not add the official Rust SDK
as a third route before it provides the required command-ID seam.

### 28. OpenCode has two executors and a universal journal

Built-in host runtime and standalone adapter overlap. Standalone `journal.rs` combines
intent, execution, outcome, result pages, Store ACKs, references, checkpoints,
tombstones and retention. Current upstream supplies durable inbox, per-session owner,
execution claim/restart terminal and assistant `parentID`; select and live-qualify one
exact installed contract, then keep standalone only.

### 29. Governor inventory is binding-rooted

`agent.list/state/family` and runtime catalog start from ready live bindings. Provider
sessions/processes without a current Task/Attempt/binding disappear. Assignment must
be an edge over independently discovered source subjects, not the discovery root.

### 30. Process and workspace status are not live joined facts

Process metrics is an explicit receipt-driven Windows CLI, max eight selected
identities, with no Store join and repeated image verification. Supervisor public
state omits PID/birth/group. Workspace lease exposes preparation-time baseline/clean
evidence; lifecycle reconciliation explicitly performs no filesystem/Git read.

The target needs one source collector for PID/birth/image/group/shared service and one
for actual worktree repository/branch/HEAD/dirty/upstream/push target.

### 31. Remote repository state exists only inside publication

Forge has correct exact remote-ref readback for uncertain pushes, but no independent
bounded remote-ref cache. Governor cannot show remote head/source time/error before a
publication Operation.

### 32. GitHub source polling is overbuilt and cursor-unsafe

`github.source.poll` is a GET collector represented as a durable Operation with
queued/sending/outcome_unknown, source lease, cancellation, recovery and observations.
Safe GET collection does not need an effect workflow.

The poll requests Issues sorted by mutable `updated desc`, persists a page number
between passes and resets only after reaching the end. Items can move across pages
between polls; same-window duplicate checks do not close cross-poll gaps. Start from
page one and advance to a stable high-water/source cursor with explicit partial
coverage and periodic reconciliation.

### 33. GitHub collection mutates Task lifecycle in one writer transaction

One bounded network snapshot is committed together with issue fact history, current
items, Task creation/revision, mapping conflicts, work-pool rows, coverage and
Operation settlement. Source observation and Task import are different responsibilities.
Commit source facts first; import selected items separately.

### 34. GitHub work pool duplicates Eliot queue/lane

`github_work_pool_members`, preview/apply and selection order form another queue.
Imported Tasks should enter the ordinary queue and explicit route/lane policy.

### 35. GitHub writes have two effect engines

Managed labels execute a direct `gh` child; PR description executes through
`swarm-forge-worker` with private job/authorization/result/custody. Both implement the
same durable sending → write once → exact readback rule using different process
owners. Keep one small GitHub API effect owner; keep remote Git push in Forge.

The semantic rule is sound and must survive: after an ambiguous write, read exact
repository/item state and never resend by guess.

### 36. Automatic GitHub projection is another workflow engine

Activation watermark, global/per-entry cursors, catch-up state, quarantine/recent
records and effect-drain cursor project acceptance into labels. Remove the engine.
An explicitly configured integration action may follow acceptance without owning a
second lifecycle.

### 37. Mailbox is encoded inside arbitrary Operation JSON

`find_delivery` searches settled `operations.result_json` across legacy and typed
message methods. It needs JSON expression indexes and a corruption-sentinel query to
reproduce historical malformed-row behavior. Mailbox deserves a normalized bounded
row keyed by recipient/cursor.

### 38. One message has too many identities

Legacy send uses Operation ID as message ID; typed coordination creates `cmsg-*` plus
a separately allocated delivery ID, while client request ID remains idempotency.
Use Operation ID as the sole durable message ID; replies/cancellation reference it.

### 39. `message.read` scans the generic observation timeline

Mailbox read is a filtered generic timeline scan with per-row method/object
authorization and JSON projection. It competes with `report.delta` semantics instead
of reading recipient-indexed message rows by a monotonic mailbox cursor.

### 40. Coordination is a second collaboration platform

Parallel surfaces include legacy message, typed send, consult, thread open/send/
resolve/withdraw/supersede, participant registration, work/contract cards, contracts,
integration ACK, watches, Concilium and code scope.

Thread wire includes speech acts, reasonability, related scopes, proposals,
objections, ratification and evidence/deadline state. Store adds meta-JSON thread
projections, task/actor/active/fingerprint/event indexes, legacy backfill and custom
scan cursors while message bodies still live in Operations.

Keep one mailbox and, only if a real consumer needs it, a minimal Task-scoped thread
group. Consultation is a message; review is review; write conflict is workspace lease;
watches are subscriptions or automation conditions. Delete the rest.

### 41. Gateway secret-file claims exceed source enforcement

Gateway correctly requires loopback bind, fixed profile, bearer auth, Origin/Host
checks and body/connection/request limits. But credential loading uses ordinary file
reads and bearer loading only caps bytes; source does not enforce regular file,
symlink/reparse rejection or private owner/ACL/mode despite documentation requiring
user-only files. Use the shared private bounded file primitive and enforce token upper
bounds.

### 42. Gateway is single-identity but not explicit about tenancy

One fixed Eliot credential/profile and one bearer are shared by up to 32 connections.
Those clients are the same Eliot principal/request namespace. Make one gateway
process/listener one remote agent identity/profile, bound MCP sessions and idle time,
and keep edge TLS/auth outside. Remove the kernel-host compatibility wrapper.

## Code-reduction candidates

- retired sibling installers/profile selectors;
- checkout/compiler/cache-path qualification;
- host dependencies on worker implementation crates;
- pure reads still routed through the writer;
- caller-supplied launch preview hashes and MCP readiness gates;
- vendor option/package IDs hard-coded in route validation;
- old completed data readers that keep no current work alive.

## Claims explicitly rejected

- One MCP exposure catalog is not automatically duplicate authorization; parallel
  routing/authorization ownership is the defect.
- Check, Script, Forge and GitHub do not need a universal EffectRunner because their
  domain readback differs; they should share OS custody primitives.
- Result-page retention before ACK is necessary.
- Transcript, alive PID, low CPU or mtime does not settle an uncertain native write.
- A Lane entity, ORM, broker, permanent migration framework or universal journal is
  unnecessary.
- Octocrab is optional; the problem is duplicate effect ownership, not `gh` itself.
- Session persistence does not make a timed-out input/reply safe to resend.
- A gateway timeout does not prove the Store request failed.

## Implemented source reduction

Commit `25a8e3b` removed the host's duplicate application CLI/MCP launcher:
`main.rs` fell from 1,949 to 743 physical lines, net −1,206 Rust lines. Later audit and
documentation commits are not runtime deletion.
