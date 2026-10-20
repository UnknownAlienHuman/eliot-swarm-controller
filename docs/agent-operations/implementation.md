# Eliot Swarm Controller — Implementation

This is the only implementation program for the
[architecture](architecture.md). Runtime source was audited through
`85b40e63a9225db05a480c027cca49f4913e4540` on 2026-10-20. The
[findings](refactoring.md) explain why code is removed; [status](../implementation-status.md)
separates completed runtime work from plans.

Do not start another repository-wide framework migration. Complete one vertical
caller-to-result path, delete its replaced path, then use a second current caller to
prove any shared implementation.

## 1. Working rules

Every coding slice must name:

- observable behavior;
- exact current source owners/callers;
- unsupported and uncertain cases;
- old code, formats, fixtures, configuration and packages deleted in the same slice;
- one donor/native mechanism when relevant;
- minimal affected formatting and warnings-denied Clippy/build command;
- connected completion evidence and net owned-code change.

One manager owns shared contracts, Store transitions and integration in one worktree.
Leaf agents receive narrow non-overlapping tasks. A new type, fixture, stub, crate,
compatibility reader or proof packet is not progress unless a current caller uses it
and the superseded mechanism disappears.

Migration is route/domain-by-route/domain. An unmigrated route may temporarily retain
its current format but receives no new profile/version family. Once switched, no new
work enters the old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance, durable messages and observations | `crates/swarm-kernel-host/src/store` |
| Semantic adapter commands and bounded Store records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `swarm-client::ModuleLink` / `Client` |
| Process groups, birth identity and private atomic files | `swarm-process` |
| Native transport/session/readback | selected adapter only |
| Workspace registration/worktree/write authority | Store workspace + host Git/filesystem effect |
| Queue, route decision and launch admission | Store launcher |
| Time admission | one in-process host loop; Store owns due state |
| Checks, scripts, remote Git and GitHub writes | one selected worker per domain |
| Public CLI/MCP | `swarm-cli` / `swarm-mcp`, both calling Store |
| Local source collection | selected adapter plus host process/Git/remote collectors |

No adapter accepts Tasks or candidates. No frontend reduces vendor events. No shared
shell owns provider session state, native replay, Task state, scheduling or transcript
storage.

## 3. Semantic adapter boundary

Use one closed internal command enum:

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

Each adapter exposes only actions/scopes it really supports. Unsupported actions fail
before native I/O. Remove vendor/internal public methods:

- `native.opencode.loop_step` → `Send(steer)`;
- `native.command.cancel_turn` → `Cancel(turn)`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation.

A normal outcome needs Operation ID, stale binding/boot guard, native identity,
disposition and bounded action details. Do not repeat package versions, protocol,
request/source/prompt hashes or Task/Attempt identity already owned by Store.

Keep hashes for credentials/verifiers, selected package bytes and immutable artifact
bytes. Readiness, model, activity, quota and stream state are observations.

## 4. Runtime implementation order

```text
A  Antigravity vertical result/delivery slice
B  queue, explicit routes, provider facts, root admission
C  one workspace write authority
D  minimal process custody and restart
E  one scheduler/current-state automation loop
F  compact TaskPacket
G  ordinary review/correction/acceptance
H  separate public/internal/adapter protocols
I  numeric Store migrations and read conversion
J  concurrent IPC/MCP and high-volume reads
K  one Check, Script, remote-Git and GitHub effect owner
L  shared adapter shell only after two current users
M  Claude reduction
N  Codex reduction
O  Command Batch/ACP reduction
P  Muse Node consolidation and custom-Rust retirement
Q  OpenCode standalone consolidation
R  joined local Governor observation
S  optional-product and tooling deletion
T  one mailbox; delete coordination workflow products
U  GitHub source/import/effect separation
V  small single-tenant remote gateway
```

### A — Antigravity first slice

**A0 — one active v6 coordinate.** Synchronize source, descriptor, route, installer,
package policy, qualification and catalog on v6. Historical v5 receipts remain
historical. Disposition concrete unsettled v5 work, then delete active `5 | 6` paths.

**A1 — self-contained terminal outcome.** Make terminal settlement final at native
completion:

```text
operation + binding generation + adapter boot
native conversation + result ordinal
native terminal + Eliot disposition
result/content reference
```

Delete `local_execution_results`, observation-result coupling,
`AntigravityDispatchSessionProof`, its producer/validator/digests and mandatory
refresh-before-dispatch. Refresh remains safe readback, not authority.

**A2 — exact Store-delivery spool.** Persist already-formed `module.outcome` and
`module.result` parameters in private atomic files before memory can forget them.
Same-boot reconnect resends identical parameters and deletes only after exact Store
ACK. Never persist or replay native prompt/process intent. Observations use one
replaceable pending checkpoint per source.

**A3 — real actions only.** Retain Open, Dispatch, Send(next_turn), Refresh, Result,
Reconcile and explicit release of a truly owned resource. Delete placeholder
Reply/Configure/Goal/Background/Recover/steer handlers and repeated receipt identity.

### B — queue, routes, provider state and admission

**B0 — one queue projection.** `swarm.queue.get` reads current Task/Attempt,
corrections, dependencies, workspace lease, provider/root capacity and binding state.
Rank deterministically: correction; unfinished Attempt; ready unclaimed Task; explicit
phase policy; age + Task ID. A page reserves nothing; launch rereads current facts.

**B1 — lane is a view.** A manager policy stores enabled selector, ordered route
aliases and optional agent profile. Each route owns exact harness/model/effort/root
limit. No lane table, daemon, cursor, inbox or scheduler.

**B2 — source-scoped provider facts.** Normalize usage/availability by stable source,
epoch, sequence, observed time, freshness, coverage and actual account/service/
connection scope. Adapters fold native methods. Remove provider-method/artifact
allowlists, auth-context proof hashes and per-binding copies of shared account facts.

**B3 — root count.** Count/reserve only unresolved root-open Operations and unreleased
root bindings in the launch transaction. Delete capacity phase/evidence ledgers,
refusal records, key codecs and lifecycle synchronization. `report.capacity` is a
projection.

**B4 — `dispatch_once`.** On wake, query a bounded ranked page, select the first
eligible configured route and atomically create/reuse Attempt, reserve workspace/root
and create launch Operation. Delete WorkDispatch cursors, copied pending subjects,
catch-up watermarks, semantic slots, duplicate links and ordinary-blocker quarantine.

### C — one workspace write authority

`TaskSpec.scope.initial_paths` supplies normalized repository-relative path prefixes;
`forbidden_paths` is hard negative scope. Empty allowed scope means repository root.
Reject absolute paths, traversal, empty segments and `.git`; compare by path segments.

The Attempt workspace lease is the only write authority and overlap guard. Review
needs no write lease. Delete `code.scope.*`, symbol/interface authority, proposals,
digests, overrides, expiry, indexes and contract links.

### D — minimal process custody

Persist binding/generation/boot, private state/credential reference, selected
executable, PID + OS birth identity, Job/process-group identity and shared-service
relation. Package/protocol matrices stay install facts.

Restart rules:

- process dead → clear live record;
- PID/image/birth mismatch → clear record, kill nothing;
- uninspectable → retain unknown;
- exact Eliot-owned leftover → route policy chooses recover/observe/terminate;
- shared service → never kill because one session disappeared.

### E — one scheduler and thin automation

Keep one automation-entry schedule and one in-process loop waiting for Store change or
nearest due time, plus a slow safety wake. Store atomically claims occurrence and
admits an ordinary Operation.

Delete standalone scheduler service/credential/protocol, overlapping loops and
workers that only perform Store bookkeeping. Keep workers only for real external
I/O/custody.

Automation toggles are small current-state attempts:

```text
dispatch | review | return_changes | accept | check | goal_message | script
```

They do not own another workflow lifecycle.

### F — compact model-visible TaskPacket

Replace identity JSON, canonical brief and launch packet with:

```text
Goal
Requirements
Current work: initial | continue | correction
Selected sources/files/symbols
Write/forbidden paths
Worktree/branch/baseline
Selected route/model/effort
Deliverable
Verification
```

Store retains Task/Attempt/Operation/binding identity. Adapter receives Operation
reference plus rendered body. One renderer handles initial assignment, correction and
resume. Delete source/prompt hash echo chains.

### G — ordinary review/correction/acceptance

```text
review_once     -> assign one missing review for current candidate
review.submit   -> retain verdict + all findings
task.request_changes -> exact candidate/review, one Attempt transition,
                        optional child Send with all findings
accept_once     -> common candidate-specific task.accept gate
```

Delete ReviewDisposition/RepairDispatch workflow state, per-finding delivery,
semantic slots, duplicate findings packages and automation-authored evidence arrays.
Candidate B needs review B. Same candidate after correction is `no_progress`; bounded
rounds escalate to manager attention.

### H — separate protocols and domain authorizers

Freeze every current method as retained public Core/integration, internal host/worker,
adapter semantic or removal target. No aliases during the cut.

A retained domain owns typed parse, exact role/object/revision authorization and its
handler. The shared registry exposes/classifies methods only. Public MCP never contains
`module.*`, `native.*` or scheduler-service RPC. Removing a product removes policy,
schema, CLI/MCP tool, handlers, fixtures and storage together.

### I — numeric Store migrations

Use SQLite `application_id`, application-owned `user_version`, ordered numeric
migrations and `BEGIN IMMEDIATE`. Each migration has explicit pre/postconditions and
bounded integrity checks. Remove base-SQL SHA equality, extension digests, reference
DB construction and custom DDL token validation.

Migrate only concrete retained data. Historical completed rows may remain opaque when
current projection retains required facts. Move pure bounded query families to WAL
readers. Never hold Store transactions across native/file/Git/network I/O.

### J — IPC, MCP and high-volume views

- Clone current `Arc<Client>` and release frontend connection mutex before awaiting
  multiplexed RPC; transport failure retires only that connection and never replays.
- One wake pump per MCP session coalesces Store cursor changes. `monitor.follow` /
  `report.delta` remains replay authority.
- `tasks/get` reads the exact Operation/binding/pending input; no global attention scan.
- Store authorizes native stream open/close; bounded ephemeral ring read/write runs
  outside the single writer.

### K — selected effect executors

**Checks.** Require standalone `swarm-checks` for new CheckRuns, including bounded
source/tool/Git-hook probes. Keep historical host receipts only through finite
readback; delete hidden host check worker.

**Scripts.** Keep `swarm-script-worker`; `swarm-scripts` owns the only Work/plan/start/
completion wire. Delete duplicate host DTOs and compatibility entrypoint.

**Remote Git.** Keep Forge for remote push/publication with exact target serialization
and remote-ref readback. Local Git inspection/preparation stays bounded in host. Use
`swarm-process` custody/capture/private files. Verify package bytes at install/launch,
not per Operation.

**GitHub API.** Add one small GitHub effect owner for explicit retained API writes.
Preserve durable sending boundary, one write attempt and exact readback. Remove PR
description execution from Forge worker and direct unmanaged `gh` child ownership.

### L — shared adapter shell only after proof

Extract authenticated Eliot IPC, hello, typed command pull and redelivery of already
formed Store records only after two current adapters use identical behavior and delete
their old loops in the same change. Extend `swarm-process`/`swarm-client`; do not add an
adapter-framework crate. Native session state, provider recovery, result association,
timers and Task state remain adapter-specific.

### M — Claude

Retain one Rust adapter with thin official SDK driver. Split live session,
NativeEffectRecord, exact StoreDelivery, latest Observation and persisted terminal
result. Delete generic journal, callback retention engine, tombstones/reference graph
and old Node generations after finite disposition. Lost JS resolver remains unknown;
never replay reply. Persist terminal body or read it through supported history.

### N — Codex

Retain Rust app-server adapter and externally owned app-server. Use one connection
owner, SessionRecord, per-effect records, exact StoreDelivery and latest observation.
Use thread, `clientUserMessageId`, turn and `expectedTurnId`; map native Goal directly.
Delete full-snapshot checkpoint, raw event mirror, tombstones, Goal proof packets,
retired Python executor and profile ladders.

### O — Command Batch/ACP

Keep one current Batch profile and one stable ACP v1 route. Eliot `AcpV2` is an
artifact generation, not protocol v2. Preserve `session/load` no-fresh-fallback and
option readback. Replace Batch/ACP journals with minimal SessionRecord,
NativeEffectRecord and StoreDelivery. Remove `native.command.*`, release-evidence,
profile/schema ladders and package proof duplication.

### P — Muse

Keep `muse-sdk-1.3.0-bridge.13` as the only new-work executor during refactor. Split
its checkpoint into SessionRecord, NativeEffectRecord, exact StoreDelivery and latest
Observation; preserve explicit Operation-derived command IDs, targeted reconciliation,
native Goal/result/usage and module-owner custody.

Inventory/disposition concrete custom-Rust work, then delete
`crates/swarm-adapter-muse`, Cargo member, disabled descriptor, custom MSP schema/wire
and Rust-only fixtures. The official Rust SDK is only a future replacement candidate
when it exposes the required command-ID seam and deletes more code than it adds.

### Q — OpenCode

Select/live-qualify one exact installed native contract with durable inbox,
process-global Session owner, execution claim/restart terminal and assistant
`parentID`. Converge on `crates/swarm-adapter-opencode` as sole new-work executor.

Bind result by exact assistant ID + parent input ID + selected part IDs. Map exact
steer/reply/cancel/background/restart readback; unsupported controls fail before I/O.
Internalize MCP setup/readiness. Switch route/package to standalone, freeze built-in
new work, disposition old work, then delete host `runtime/opencode_v2`, legacy demand/
worker slot, native MCP/owned-service launch paths and built-in fixtures.

Shrink standalone persistence to SessionRecord, NativeEffectRecord, StoreDelivery and
ObservationCheckpoint. No universal journal/reference graph/tombstone system.

### R — joined local Governor observation

Implement [observability](observability.md) R0–R6:

1. stable SourceRecord + collector epoch/cursor/freshness/coverage;
2. configured provider-local readers independent of Task/binding;
3. one OS process collector with PID/birth/image/group/shared-service facts;
4. actual worktree Git collector and independent bounded remote-ref cache;
5. current native/process/worktree subjects plus assignment edges;
6. one WAL projection and one cursor invalidation/subscription model;
7. delete binding-only catalogs, receipt-driven metrics CLI, static lease-as-live view
   and parallel report reducers after consumers move.

Unassigned subjects remain visible and grant no Task/transcript rights. Ordinary reads
make no provider/model/filesystem/network call.

### S — delete non-core products and tooling bureaucracy

Delete Concilium, generic contract/integration ratification, automatic GitHub label/
publication projection, document-edition rights, legacy acceptance branches, retired
installers/profile matrices and duplicate worker implementation dependencies.

One current package has one manifest, executable, descriptor and required resources.
Verify selected bytes at install/launch. Runtime hello checks authenticated binding,
protocol major and required semantic capability subset. Delete checkout/compiler/
cache-path equality, per-artifact PowerShell validators and proof-packet qualification.

### T — one mailbox, not a coordination platform

**T0 — normalized mailbox.** Add a bounded message row keyed by one durable message ID
(the admitted Operation ID), sender, recipient, optional Task/Attempt/thread, kind,
body/artifact reference, reply target and monotonic mailbox cursor. Stop storing the
mailbox as JSON fields inside arbitrary `operations.result_json`.

**T1 — one public surface.** Retain `message.send`, `message.read`, optional
`message.cancel`; replies reference the original message ID. Delete separate random
delivery IDs, `cmsg-*` identity and legacy/typed lookup unions.

**T2 — minimal optional thread.** If a consumer still requires grouping, keep only
thread ID, Task scope, creator, bounded participants and open/closed state. Messages
remain mailbox rows. No speech-act taxonomy, reasonability packet, proposal,
objection, supersede or ratification lifecycle.

**T3 — derive rights.** Current Task/Attempt/role/object grants authorize messages.
Delete participant token-hash registration, generation/basis records, work/contract
cards and duplicate actor directories.

**T4 — remove workflow products.** Consultation becomes an ordinary message. Review
stays review. Write conflict stays workspace lease. Watches become Store cursor
subscriptions or explicit automation conditions. Delete `coordination.consult`,
thread negotiation methods, contracts, integration ACK, Concilium, cards, watches and
their indexes/backfills/custom cursors.

**T5 — finite transition.** Migrate only unresolved/open messages needed by concrete
work. Completed historical Operations remain readable as history; no permanent
translator keeps old coordination products alive.

### U — GitHub source, import and effects

**U0 — collector, not workflow.** Replace `github.source.poll` Operation states,
poll lease and cancellation/recovery machine with one configured bounded source
collector. GET collection retries safely under source epoch/cursor; failures update
coverage, not Task workflow.

Do not persist page number across independently changing `sort=updated` snapshots.
Each pass begins from page 1, advances to a retained stable high-water
`(updated_at, external_id)` or explicit source cursor, and periodically performs a
bounded reconciliation scan. Cross-page movement remains coverage, never deletion.

**U1 — source facts first.** Collector commits current Issue/PR facts, source revision,
last-seen time and coverage in short batches. It does not create/revise Tasks in the
same writer transaction.

**U2 — explicit import.** Manager/current policy selects source items and constructs a
compact Task spec. External title/body is source material, not owner of the complete
Task spec. Local Task edits do not become source mapping corruption.

**U3 — delete GitHub work pool.** Imported Tasks enter the ordinary Eliot queue/lane.
Delete `github.work_pool.preview/apply`, selection rows and duplicate ordering.

**U4 — one GitHub API effect owner.** Keep explicit managed-label/PR-description writes
only when required. One retained Operation writes at most once after durable sending,
then exact repository/item readback settles or leaves unknown. Use shared process
custody. Keep remote Git push in Forge; remove GitHub PATCH from Forge worker.

**U5 — delete automatic projection engine.** Remove GitHub projection activation
watermarks, global/per-entry cursors, quarantine/recent state and effect-drain cursor.
Acceptance may trigger an explicit configured integration action, but no second
workflow lifecycle.

**U6 — remote observation.** Feed repository/ref/head/read time/error into R3's
independent remote-ref cache. Passive Governor status does not depend on publication.

### V — small single-tenant gateway

**V0 — private bounded secret loading.** Load Eliot credential and bearer through one
shared private-file helper: absolute/canonical path, no symlink/reparse, regular file,
owner/ACL/mode check and strict byte bound. Enforce upper and lower token lengths.

**V1 — explicit single tenancy.** One gateway process/listener maps to one Eliot client
identity and fixed MCP profile. Bound active MCP sessions and idle lifetime, not only
TCP connections. A shared bearer does not create distinct Eliot principals.

**V2 — standalone only.** Keep loopback bind, Host/Origin checks, body/connection/
time limits and external TLS/edge auth. Remove the kernel-host compatibility `gateway`
wrapper after finite CLI transition. Do not add an identity broker, TLS platform,
general reverse proxy or gateway database.

A timeout returns uncertainty: the Store request may have committed. It never triggers
a blind replay.

## 5. Launch, results and finite transitions

Launch preview is optional. Store resolves one plan under the admitted Operation and
references it from workspace/dispatch. Delete caller-supplied preview/plan hashes.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Adapter retains an unacknowledged page until Store ACK. A file without committed
receipt is not a result.

Before deleting a current executor/format/product, enumerate concrete unsettled
Operations and required retained results. Finish/read back with current owner, export
result or create a new Task revision/Attempt. Then delete producer, reader, config,
fixture, package and CI branch together. Do not write a universal converter.

## 6. Verification

During construction run formatting and minimal warnings-denied Clippy/build for
changed production packages. Focused connected cases run when the vertical slice
closes; broad/live qualification follows the completed product.

Minimum connected proofs:

- exact logical request is idempotent; changed request conflicts;
- uncertain native or external write is never resent by transport guess;
- stale binding/boot outcome fails; exact Store delivery redelivers safely;
- native and Eliot Goals work independently and together;
- queue/admission cannot duplicate Attempt/launch or starve later eligible work;
- overlapping write leases block; disjoint scopes proceed;
- review B cannot reuse review A; all findings return together; no-progress stops;
- unmatched sessions/processes remain visible without provider calls;
- one shared provider process/account is counted once;
- assigned Git facts and observed branch/HEAD/dirty may differ and both display;
- one message identity supports send/read/reply/cancel without JSON scans;
- GitHub source collection cannot silently miss moved pages or mutate Tasks inside the
  source transaction;
- GitHub/Forge unknown writes settle only by exact readback;
- gateway rejects unsafe secret files and remains one fixed identity/profile;
- PID reuse cannot kill unrelated process;
- removed paths have no config, policy, schemas, fixtures, package or fallback wrapper.

Report net owned Rust, JS and glue separately from tests.
