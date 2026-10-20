# Eliot Swarm Controller — Implementation

This is the only implementation program for the
[architecture](architecture.md). Runtime source and landed Antigravity slices were
reviewed through `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` on 2026-10-20.
[Findings](refactoring.md) explain the deletions; [status](../implementation-status.md)
records completed runtime work.

Do not start another horizontal framework migration. Complete one vertical
caller-to-result path, delete its replaced path, then use a second current caller to
prove any shared implementation.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>
<a id="identity-and-hashes"></a>

## 1. Working rules

Every coding slice states:

- observable behavior and exact current owners/callers;
- unsupported and uncertain cases;
- old code, schemas, policy, fixtures, configuration and packages deleted with it;
- one donor/native mechanism when relevant;
- minimal formatting and warnings-denied Clippy/build command;
- connected completion evidence and net production Rust/JS/glue change.

One manager owns shared contracts, Store transitions and integration in one worktree.
Leaf agents receive narrow non-overlapping tasks. A new type, fixture, compatibility
reader, proof packet or crate is not progress unless a current caller uses it and the
superseded mechanism disappears in the same slice.

Migration is route/domain by route/domain. An unmigrated route may temporarily retain
its format but receives no new profile/version family. Once switched, no new work
enters the old producer.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance, durable messages and observations | `crates/swarm-kernel-host/src/store` |
| Semantic adapter commands and bounded Store records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `swarm-client::ModuleLink` / `Client` |
| Process groups, birth identity and private atomic files | `swarm-process` |
| Native transport/session/readback | selected adapter only |
| Workspace/worktree/write authority | Store workspace + bounded host Git/filesystem effect |
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

Each adapter exposes only actions and scopes it actually supports. Unsupported actions
fail before native I/O. Store authorization grants rights; capabilities only route.

Remove vendor/internal public methods:

- `native.opencode.loop_step` → `Send(steer)`;
- `native.command.cancel_turn` → `Cancel(turn)`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation.

A normal outcome carries Operation ID, stale binding/boot guard, native identity,
disposition and bounded action details. Do not repeat package version, protocol,
request/source/prompt hashes or Task/Attempt identity already retained by Store.

Keep hashes for credentials/verifiers, selected installed package bytes and immutable
result/artifact bytes. Readiness, model, activity, quota and stream state are
observations, not permissions.

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
J  concurrent IPC/MCP and exact hot reads
K  one Check, Script, remote-Git and GitHub API effect owner
L  shared adapter shell only after two current users
M  Claude reduction
N  Codex reduction
O  Command Batch/ACP reduction
P  Muse Node consolidation and custom-Rust retirement
Q  OpenCode standalone consolidation
R  joined local Governor observation
S  optional-product and tooling deletion
T  one normalized mailbox; delete coordination workflow products
U  GitHub source/import/effect separation
V  small single-tenant remote gateway
```

### A — Antigravity first slice

**A0 — DONE: one active v6 coordinate.** Commit `cba98b7` synchronized the Rust
source/descriptor, route, installer, package policy and qualification on v6. Do not
reintroduce active v5 compatibility; historical receipts remain historical.

**A1a — DONE: self-contained terminal outcome.** Commit `eeea355` made terminal
settlement depend directly on exact Operation, binding generation, adapter boot,
native conversation/scope, result ordinal, response digest and terminal status. The
Store no longer requires `observation_id` or duplicate `local_execution_results`.

**A1b — NEXT: remove dispatch proof.** Delete
`AntigravityDispatchSessionProof`, its shared DTO, refresh producer, latest-refresh
lookup, binding/snapshot validator, assignment/lease/session digests and mandatory
refresh-before-dispatch. Store already owns assignment, Attempt, binding/boot,
workspace lease and Operation. Refresh remains safe readback only.

**A2 — exact Store-delivery spool.** Persist already-formed `module.outcome` and
`module.result` parameters in private atomic files before memory can forget them.
Scope by binding + generation + adapter boot. Same-boot reconnect resends identical
parameters and deletes only after exact Store ACK. Never persist or replay native
prompt/process intent. Observations use one replaceable pending checkpoint per source.

**A3 — real actions only.** Retain Open, Dispatch, Send(next_turn), Refresh, Result,
Reconcile and explicit release of a truly owned resource. Delete placeholder
Reply/Configure/Goal/Background/Recover/steer handlers, retained active Node profiles
and repeated receipt identity after concrete old work is disposed.

<a id="work-dispatch"></a>

### B — queue, routes, provider state and admission

**B0 — one queue projection.** `swarm.queue.get` reads current Task/Attempt,
corrections, dependencies, workspace lease, provider/root capacity and binding state.
Rank: current correction; unfinished Attempt; ready unclaimed Task; explicit phase;
age + Task ID. A page reserves nothing; launch rereads current facts.

**B1 — lane is a view.** Manager policy stores enabled selector, ordered route aliases
and optional agent profile. Each route owns exact harness/model/effort/root limit. No
lane table, daemon, cursor, inbox or scheduler.

**B2 — source-scoped provider facts.** Normalize usage/availability by stable source,
epoch, sequence, observed time, freshness, coverage and real account/service/
connection scope. Adapters fold native methods. Remove method/artifact allowlists,
auth-context proof hashes and per-binding copies of shared account facts.

**B3 — root count.** Count/reserve only unresolved root-opening Operations and
unreleased root bindings in the launch transaction. Delete capacity phase/evidence
ledgers, refusal rows, key codecs and lifecycle synchronization. `report.capacity` is
a projection.

**B4 — `dispatch_once`.** On wake, query a bounded ranked page, select the first
eligible configured route and atomically create/reuse Attempt, reserve workspace/root
and create launch Operation. Delete WorkDispatch cursors, copied pending subjects,
catch-up watermarks, semantic slots, duplicate links and ordinary-blocker quarantine.

### C — one workspace write authority

`TaskSpec.scope.initial_paths` supplies normalized repository-relative path prefixes;
`forbidden_paths` is a hard negative boundary. Empty allowed scope means repository
root. Reject absolute paths, traversal, empty segments and `.git`; compare by path
segments.

The Attempt workspace lease is the only write authority and overlap guard. Review
needs no write lease. Delete `code.scope.*`, symbol/interface authority, proposals,
digests, override, expiry, indexes and contract links.

### D — minimal process custody

Persist binding/generation/boot, private state/credential reference, selected
executable, PID + OS birth identity, Job/process-group identity and shared-service
relation. Package/protocol matrices stay installation facts.

Restart rules:

- dead process → clear live record;
- PID/image/birth mismatch → clear record, kill nothing;
- uninspectable → retain unknown;
- exact Eliot-owned leftover → route policy chooses recover/observe/terminate;
- shared service → never kill because one session disappeared.

<a id="automation-and-delivery"></a>

### E — one scheduler and thin automation

Keep one automation-entry schedule and one in-process loop waiting for Store change or
nearest due time, plus a slow safety wake. Store atomically claims an occurrence and
admits an ordinary Operation.

Delete standalone scheduler service/credential/protocol, overlapping loops and
workers that perform only Store bookkeeping. Keep workers for real external I/O or
custody.

Automation toggles are bounded current-state attempts:

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
review_once          -> assign one missing review for current candidate
review.submit        -> retain verdict + all findings
task.request_changes -> exact candidate/review, one Attempt transition,
                        optional child Send with all findings
accept_once          -> common candidate-specific task.accept gate
```

Delete ReviewDisposition/RepairDispatch workflow state, per-finding delivery,
semantic slots, duplicate findings packages and automation-authored evidence arrays.
Candidate B needs review B. Same candidate after correction is `no_progress`; bounded
rounds escalate to manager attention.

### H — separate protocols and domain authorizers

Freeze every method as retained public Core/integration, internal host/worker, adapter
semantic or removal target. No aliases during the cut.

A retained domain owns typed parse, exact role/object/revision authorization and its
handler. Shared registry exposes/classifies names only. Public MCP never contains
`module.*`, `native.*` or scheduler-service RPC. Removing a product removes policy,
schema, CLI/MCP tool, handlers, fixtures and storage together.

### I — numeric Store migrations

Use SQLite `application_id`, application-owned `user_version`, ordered numeric
migrations and `BEGIN IMMEDIATE`. Each migration has explicit pre/postconditions and
bounded integrity checks. Remove base-SQL SHA equality, extension digests, reference
DB construction and custom DDL-token validation.

Migrate only concrete retained data. Historical completed rows may remain opaque when
the current projection retains required facts. Move pure bounded reads to WAL readers.
Never hold Store transactions across native/file/Git/network I/O.

### J — IPC, MCP and high-volume views

- Clone current `Arc<Client>` and release frontend connection mutex before awaited RPC.
- One wake pump per MCP session coalesces Store cursor changes; Store history remains
  replay authority.
- `tasks/get` reads exact Operation/binding/pending input; no global attention scan.
- Store authorizes native stream open/close; bounded ring I/O runs outside writer.

### K — selected effect executors

**Checks:** standalone `swarm-checks` for all new CheckRuns; finite historical readback,
then delete hidden host check worker.

**Scripts:** `swarm-script-worker` executes; `swarm-scripts` owns the one wire. Delete
duplicate host DTOs and compatibility entrypoint.

**Remote Git:** Forge owns remote push/publication with exact target serialization and
readback. Local Git stays bounded in host. Reuse `swarm-process` custody and verify
worker bytes at install/launch, not per Operation.

**GitHub API:** one small effect owner performs explicit retained writes with durable
sending, one write attempt and exact readback. Remove PR PATCH execution from Forge
worker and unmanaged direct child ownership.

### L — shared adapter shell only after proof

Extract authenticated Eliot IPC, hello, typed command pull and redelivery of already
formed Store records only after two current adapters use identical behavior and delete
their old loops in the same change. Extend `swarm-process`/`swarm-client`; do not add
an adapter-framework crate. Native session state, recovery, result association, timers
and Task state remain adapter-specific.

### M — Claude

Retain one Rust adapter with thin official SDK driver. Split live session,
NativeEffectRecord, exact StoreDelivery, latest Observation and persisted terminal
result. Delete generic journal, callback retention engine, tombstones/reference graph
and old Node generations after finite disposition. Lost callback resolver remains
unknown; never replay reply.

### N — Codex

Retain Rust app-server adapter and externally owned app-server. Use one connection
owner, SessionRecord, per-effect records, exact StoreDelivery and latest observation.
Use thread, `clientUserMessageId`, turn and `expectedTurnId`; map native Goal directly.
Delete full checkpoint, raw event mirror, tombstones, Goal proofs, Python executor and
profile ladders.

### O — Command Batch/ACP

Keep one current Batch and one stable ACP v1. Eliot `AcpV2` is an artifact generation,
not protocol v2. Preserve `session/load` no-fresh-fallback and option readback. Replace
journals with SessionRecord, NativeEffectRecord and StoreDelivery. Remove
`native.command.*`, release-evidence and profile/schema ladders.

### P — Muse

Keep Node bridge `.13` during refactor. Split its checkpoint into SessionRecord,
NativeEffectRecord, exact StoreDelivery and latest Observation; preserve explicit
Operation-derived command IDs, targeted readback, native Goal/result/usage and
module-owner custody.

Disposition custom-Rust work, then delete `crates/swarm-adapter-muse`, Cargo member,
disabled descriptor, custom MSP schema/wire and Rust-only fixtures. Official Rust SDK
is a future candidate only when it exposes the required command-ID seam and deletes
more code than it adds.

### Q — OpenCode

Select and live-qualify one exact installed native contract with durable inbox,
process-global Session owner, execution claim/restart terminal and assistant
`parentID`. Converge on standalone Rust as sole new-work executor.

Bind result by exact assistant ID + parent input ID + selected part IDs. Map exact
steer/reply/cancel/background/restart readback; unsupported controls fail before I/O.
Internalize MCP setup/readiness. Switch route/package, freeze built-in new work,
disposition old work, then delete host `runtime/opencode_v2`, legacy demand/worker
slot, native MCP/owned-service paths and built-in fixtures.

Shrink persistence to SessionRecord, NativeEffectRecord, StoreDelivery and
ObservationCheckpoint. No universal journal/reference graph/tombstone system.

<a id="passive-agent-observation"></a>

### R — joined local Governor observation

Implement [observability](observability.md) R0–R6:

1. stable sources with collector epoch/cursor/freshness/coverage;
2. provider-local readers independent of Task/binding;
3. one OS process collector with PID/birth/image/group/shared-service facts;
4. worktree Git collector and independent bounded remote-ref cache;
5. current subjects plus assignment edges;
6. one WAL projection and cursor invalidation/subscription model;
7. delete binding-only catalogs, receipt metrics CLI, static lease-as-live view and
   parallel report reducers after consumers move.

Unassigned subjects remain visible and grant no Task/transcript rights. Ordinary reads
make no provider/model/filesystem/network call.

### S — delete non-core products and tooling bureaucracy

Delete Concilium, generic contract/integration ratification, automatic GitHub
projection, document-edition rights, legacy acceptance branches, retired installers/
profile matrices and duplicate worker implementation dependencies.

One current package has one manifest, executable, descriptor and required resources.
Verify bytes at install/launch. Runtime hello checks authenticated binding, protocol
major and required semantic capability subset. Delete checkout/compiler/cache-path
equality, per-artifact validators and proof-packet qualification.

### T — one mailbox, not a coordination platform

**T0:** add bounded recipient-indexed message rows keyed by the admitted Operation ID,
with sender, optional Task/Attempt/thread, kind, body/artifact ref, reply target and
monotonic mailbox cursor.

**T1:** retain `message.send`, `message.read`, optional `message.cancel`; replies use
message ID. Delete random delivery IDs, `cmsg-*` and legacy/typed lookup unions.

**T2:** if a real consumer needs grouping, keep only thread ID, Task scope, creator,
bounded participants and open/closed state. Messages remain mailbox rows.

**T3/T4:** derive rights from current Task/Attempt/role grants. Delete participant
token registry, cards, consult/watch/contracts/integration/Concilium/code-scope
products and custom indexes/backfills/cursors.

**T5:** migrate only unresolved messages needed by concrete work; historical completed
Operations remain history without permanent translators.

### U — GitHub source, import and effects

**U0:** replace `github.source.poll` Operation workflow, poll lease and recovery machine
with a bounded source collector. Start from page one, advance to a stable high-water or
explicit cursor and retain partial coverage; do not persist page number across mutable
`updated desc` snapshots.

**U1:** commit current Issue/PR source facts in short batches; do not create/revise
Tasks in the same transaction.

**U2/U3:** explicit manager/policy import constructs compact Task specs; delete GitHub
work-pool selection/order. Imported Tasks enter ordinary queue/lane.

**U4:** keep explicit label/PR-description writes only when required. One retained
Operation writes at most once, then exact readback settles or remains unknown. Git push
stays Forge; GitHub API writes use the one GitHub effect owner.

**U5/U6:** delete automatic projection watermarks/cursors/quarantine/drain and feed
remote repository/ref/head/time/error into R3 independent of publication.

### V — small single-tenant gateway

**V0:** load Eliot credential and bearer through the shared private-file helper:
absolute/canonical path, no symlink/reparse, regular file, private owner/ACL/mode and
strict byte/token bounds.

**V1:** one gateway process/listener maps to one Eliot identity and fixed MCP profile.
Bound active MCP sessions and idle lifetime; shared bearer does not create principals.

**V2:** retain loopback, Host/Origin, body/connection/time bounds and external TLS/edge
auth. Delete kernel-host compatibility launcher after callers move. Do not add an
identity broker, proxy platform or gateway database.

A timeout is uncertain: Store may have committed. Never replay by guess.

<a id="launch-and-results"></a>

## 5. Launch, results and finite transition

Launch preview is optional. Store resolves one plan under the admitted Operation and
references it from workspace/dispatch. Delete caller-supplied preview/plan hashes.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Adapter retains an unacknowledged page until Store ACK. A file without committed
receipt is not a result.

<a id="current-format-and-existing-data"></a>

Before deleting a current executor/format/product, enumerate concrete unsettled
Operations and required results. Finish/read back with current owner, export result or
create a new Task revision/Attempt. Then delete producer, reader, config, fixture,
package and CI branch together. Do not write a universal converter.

<a id="11-build-install-ci-and-qualification-reduction"></a>

## 6. Build, install and qualification

One current package contains one manifest, executable, descriptor and required
resources. Verify selected bytes once at install/launch. Runtime hello validates
binding, protocol major and required semantic action subset.

Delete checkout/compiler/cache-path equality, per-artifact PowerShell validators,
repeated local hashes and proof-packet qualification. Qualification calls public APIs
and verifies observable behavior for the selected current route.

<a id="verification"></a>

## 7. Verification

During construction run formatting and minimal warnings-denied Clippy/build for
changed production packages. Focused connected cases run when a vertical slice closes;
broad/live qualification follows the completed product.

Minimum connected proofs:

- exact logical request is idempotent; changed request conflicts;
- uncertain native or external write is never resent by transport guess;
- stale binding/boot outcome fails; exact Store delivery redelivers safely;
- native and Eliot Goals work independently and together;
- queue/admission cannot duplicate Attempt/launch or starve eligible work;
- overlapping write leases block; disjoint scopes proceed;
- review B cannot reuse review A; all findings return together; no-progress stops;
- unmatched sessions/processes remain visible without provider calls;
- shared provider process/account is counted once;
- assigned and observed Git facts may differ and both display;
- one message identity supports send/read/reply/cancel without JSON scans;
- GitHub collection cannot silently miss moved pages or mutate Tasks in collection;
- GitHub/Forge unknown writes settle only by exact readback;
- gateway rejects unsafe secret files and stays one fixed identity/profile;
- PID reuse cannot kill unrelated process;
- removed paths have no config, policy, schemas, fixtures, package or fallback wrapper.

Report net production Rust, JS and glue separately from tests.
