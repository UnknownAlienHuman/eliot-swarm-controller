# Refactoring — source-backed findings

Runtime source, retained historical requirements and the current documentation were
rechecked through `6e470726c633209f98fea2bfd7bd12d039f285f1` on 2026-10-21.
[Architecture](architecture.md) owns the product boundary;
[implementation](implementation.md) owns the work order;
[status](../implementation-status.md) separates landed code from plans.

This is not a claim that every line or live failure mode has been qualified. It is the
current evidence/deletion map. A proposed simplification is valid only when the
retained behavior below remains connected and the replaced implementation disappears.

## 1. Behavior that must survive simplification

The old specifications, owner decisions and Claude scripts contain useful behavior as
well as obsolete machinery. The following requirements are retained.

| Required behavior | Retained authority | What may be deleted |
| --- | --- | --- |
| Reconnect the same manager identity after chat/link loss without restarting work | durable `client_id`, current Task/Attempt/Operation and exact native readback | link-specific state, marker files and chat-owned workflow state |
| Appoint a successor GM while preserving original owners, authors, reviewers and candidates | GM designation/epoch plus current object grants | copying Attempts, rewriting historical actors or adopting old unknown effects |
| Historical directed mail stays with its original recipient; successor reads current Task/native attention separately | normalized mailbox + Task/Attempt/Operation projections | mailbox reassignment, opaque inherited live stream and coordination participant registry |
| Lost reply or timeout never authorizes replay of a prompt or external write | retained request/Operation identity plus domain readback | transport-level retry guesses and generic recovery loops |
| Harness Goal and Eliot Goal remain independent and may be combined | adapter-native Goal + ordinary scheduled/event `Send` | vendor-branched continuation workflow and Goal proof packets |
| Governor can inspect native/process/worktree/provider facts locally without model/provider calls | stable local source collectors and joined Store projection | optional diagnostic recorder as product state, newest-log/PID/age heuristics |
| One manager owns one worktree; concurrent writers use non-overlapping path scopes | Task path scope + Attempt workspace lease | advisory symbol/interface locks and generic override workflow |
| Review/acceptance address the exact candidate | submission/review/check/acceptance records | review disposition/repair slot workflow and caller-authored proof arrays |
| External writes happen at most once and settle by exact readback after ambiguity | one effect owner per write domain | duplicate GitHub/Forge writers and automatic projection workflow |
| Remote gateway exposes one fixed Eliot identity/profile behind loopback and external edge security | one gateway process/configured credential/profile | multi-tenant identity broker, proxy platform and gateway database |

Old defaults such as fixed-age restart, newest-session guessing, automatic form answers,
forced model rotation and independent lane daemons are historical operating policy, not
product requirements.

## 2. Implemented reductions and current verification truth

### Duplicate host CLI

Commit `25a8e3b` removed the duplicate host application CLI/MCP launcher:

- host `main.rs`: 1,949 → 743 physical lines;
- net owned Rust reduction: 1,206 lines.

### Antigravity A0

Commit `cba98b755ff8d93405f91751a00d6dd6d91bbb60` synchronized active Rust
Antigravity selection on artifact version 6. Historical v5 receipts remain history,
not a current selection branch.

### Antigravity A1a

Commit `eeea3558d66a1525c9f5c2d9f3b4f75c62714bf8` removed the terminal receipt's
required Store observation ID and duplicate `local_execution_results` dependency. Its
production Rust diff was net −265 lines.

This slice is not verified complete. The later repair run tested a temporary patched
tree and failed because a rejected terminal producer projected a null
`completion_condition`. The main Rust workflow for the latest head skipped changed
Rust compilation/Clippy. The producer contract, test expectation and final intended
rejected/cancelled semantics still require one connected correction.

A1b, A2 and A3 remain unimplemented: dispatch proof removal, durable Store delivery
and real-action-only Antigravity surface.

## 3. Recurring causes of code growth

### One fact becomes several products

Eliot repeatedly represents one authority/state/recovery decision as a Store row, JSON
receipt, proof packet, workflow state machine, cursor family, compatibility profile and
qualification fixture. Refactoring must remove duplicate ownership rather than move it.

### Package metadata drives runtime behavior

Store/adapters compare artifact versions, builds and exact schema inventories. Optional
behavior creates another profile and branch. Package identity belongs to install/
inventory; Store authorization and semantic capabilities govern behavior.

### Receipts echo Store-owned identity

Package/protocol, Task/Attempt/revision, request/source/prompt hashes and nested receipts
are repeated through dispatch/outcome/result. Keep only stale binding/boot guards,
native identity/disposition and hashes of actual immutable bytes.

### Adapter persistence became universal journals

Claude, Codex, Command, Muse and OpenCode persistence mixes native intent, Store
delivery, results, observations, callbacks, startup inventory, tombstones and
compaction. Separate only real lifetimes:

```text
SessionRecord
NativeEffectRecord
StoreDelivery
ObservationCheckpoint
persisted/readable result when required
```

Do not build a shared event-sourcing framework.

### Parallel executor generations remain

| Runtime | Current duplication | Target |
| --- | --- | --- |
| OpenCode | built-in host + standalone Rust | one exact live-qualified standalone route |
| Muse | active Node + disabled custom Rust | simplify Node, delete custom Rust after disposition |
| Codex | current Rust + retired Python/profile history | one Rust app-server route |
| Claude | Rust/SDK + retained Node generations | one Rust adapter |
| Antigravity | Rust + retained warm bridge profiles | one Rust route after transition |
| Command | Batch profile ladder + ACP generations | one Batch + one stable ACP v1 |

Historical completed records do not keep executors alive.

## 4. Workflow and authority duplication

### WorkDispatch copies Task lifecycle

It stores global/per-entry cursors, pending subjects, rechecks, slots, links and
quarantine. A bounded current queue plus one atomic `dispatch_once` is sufficient.

### `code.scope.*` duplicates workspace authority

It models proposals, symbols, digests, modes, override and expiry but does not own the
worktree. Task path prefixes plus the Attempt workspace lease are the actual write
authority and conflict guard.

### Capacity copies Operation lifecycle

Capacity phases/evidence/meta indexes count read/control methods as execution. Count
unresolved root opens and unreleased root bindings transactionally; report capacity as
a projection.

### Scheduling is triplicated

In-process scheduler, standalone scheduler service and automation loop overlap. One
in-process Store-change/nearest-due loop admits ordinary Operations. Separate workers
remain only for real external effects/custody.

### Review/correction is another workflow family

ReviewDisposition, RepairDispatch, slots and separate Acceptance repeat one candidate/
review/manager relation. Keep `review.submit`, one `task.request_changes` transition
and the common candidate-specific acceptance gate.

## 5. Store, API and frontend findings

### Public API ownership is fragmented

Method policy, MCP catalog/profiles/schemas, mutation validation, Store dispatch and
visibility SQL each encode part of the surface. A retained domain needs one typed
parser/authorizer/handler. Public, internal-worker and adapter protocols must separate.

### Store migration protects SQL text

Base-schema SHA, extension digests, reference database and DDL-token validation protect
historical source text. Use application ID, numeric `user_version`, ordered
transactional migrations and bounded postconditions. Convert only concrete retained
data once.

### MCP underuses its multiplexed client

`swarm-client::Client` can correlate concurrent requests while MCP holds a connection
mutex through awaited calls. Clone the current client and release the mutex before RPC.
One coalesced wake pump should replace per-subscription poller/forwarder/queue stacks.

### Hot views perform broad scans

`tasks/get` pages global attention to find one pending input. Mailbox reads scan the
shared observation timeline. Native in-memory stream operations run through the Store
writer. Add exact addressed reads and move ephemeral ring work outside the writer.

### TaskPrompt sends controller internals to the model

Identity JSON, canonical brief, launch packet and hash chains enter the prompt and are
then echoed through adapters/results. Model-visible input needs only goal,
requirements, sources, scope, workspace, deliverable and verification; Store retains
identity.

## 6. Governor observation findings

Current inventory starts from ready bindings. Unbound native sessions/processes and a
surviving shared service can disappear. Process metrics is a separate Windows CLI for
at most eight selected identities and repeats executable verification. Workspace lease
projects preparation-time baseline/clean evidence, not current branch/HEAD/dirty.
Remote-ref readback exists only inside Forge publication. Provider usage/conditions are
binding/artifact-oriented.

The replacement uses stable source records, provider-local readers independent of
Task/binding, one process collector, one worktree collector, independent remote-ref
cache, current subjects plus assignment edges and one joined WAL projection. Ordinary
reads make no provider/model/filesystem/network call.

## 7. Mailbox and coordination audit

### Current mailbox mechanics

`message.send` and typed coordination sends settle as Operations whose message body and
identities live in `operations.result_json`. `message.read` is not a recipient-indexed
mailbox table: it scans the common observation timeline, filters candidate rows by
recipient/role, resolves object grants and then loads each payload. The scan is bounded
and disclosure checks are careful, but every mailbox page traverses the global event
surface.

One logical message can have four identities:

- caller `client_request_id` for idempotent admission;
- Operation ID;
- typed coordination `cmsg-*` message ID;
- randomly allocated delivery ID.

Reply/cancel lookup searches settled Operation JSON across legacy and typed methods,
using JSON expression indexes and corruption sentinels. This is valid historical
behavior, not a compact storage model.

### Continuity semantics that must remain

Current `gm.handover` correctly changes only GM designation/epoch. It does not rewrite
Attempt owner, restart native work or reassign historical mail. Its report/mailbox
watermarks are explicitly not acknowledgements; the successor resynchronizes
`report.delta`, `message.read` and current attention. Preserve this behavior.

A reconnect with the same durable `client_id` retains its mailbox cursor and existing
work. A different successor gains current Task/Attempt control through exact GM scope,
not through ownership rewrite. Historical recipients/authors/reviewers remain intact.

### Coordination is a second collaboration platform

Beyond mailbox delivery, the current coordination surface adds participant token
registrations, work/contract cards, consult vocabulary, threads with speech acts and
proposal revisions, watches, contracts/ratification, integration ACK, Concilium and
code scopes. These overlap existing identities:

- consultation is an addressed message;
- review is the review subsystem;
- write conflict is the workspace lease;
- watch is a subscription or explicit automation condition;
- agreement does not grant Store authority;
- GM continuity comes from durable identities and current object grants.

### Target

Add one normalized recipient-indexed mailbox table keyed by the admitted Operation ID
as the durable message ID. Store sender, recipient, optional Task/Attempt/thread, kind,
bounded body or artifact ref, reply target, cancellation reference and monotonic
mailbox cursor. Reading is non-destructive and cursor-based. Historical messages remain
addressed to the original recipient.

Keep a minimal Task-scoped thread only if a current consumer needs grouping. Delete the
participant registry, cards, consult/watch/contracts/integration/Concilium/code-scope
products and their policy, indexes, backfills, cursors and fixtures after callers move.
Migrate only unresolved messages required by concrete work; completed Operations remain
historical records without a permanent translator.

## 8. GitHub integration audit

### Source polling is modeled as an effect workflow

`github.source.poll` is a read-only GET represented as queued/sending/
outcome_unknown Operation plus a poll lease, cancellation/recovery and source state.
Reads page Issues sorted by mutable `updated desc` and persist the next page between
polls. Items can move across pages, so a later poll can miss a changed item even though
coverage is reported honestly as partial.

### Collection, Task import and queue selection are coupled

The poll commit stores source facts and, in the same transaction, creates/revises
Tasks, records mapping conflicts, updates work-pool members and settles the poll.
`github.work_pool.apply` then stores another selected/order queue beside Eliot's Task
queue. Source observation, Task import and work selection are separate responsibilities.

### Writes have two owners

Managed Issue labels use the direct `GhCli` path with durable `sending`, at-most-one
write and exact Issue readback. PR description updates use `swarm-forge-worker`, its
private job/custody/result protocol and then exact PR readback. The semantic safety
rule is correct; the duplicate execution/custody path is not.

Automatic GitHub projection adds activation/catch-up watermarks, global/per-entry
cursors, quarantine/recent state and an effect-drain cursor to turn acceptance events
into labels. This is another workflow engine and is not part of Task acceptance.

### Target

1. A bounded source collector reads current Issue/PR facts from page one to a stable
   high-water/cursor, retains source epoch/freshness/partial coverage and periodically
   reconciles. A GET does not create an Operation lifecycle.
2. Explicit manager/policy import turns selected source facts into compact Task specs in
   a separate transaction. Imported Tasks enter the ordinary Eliot queue; delete the
   GitHub work pool.
3. One GitHub API effect owner performs explicit label/PR metadata writes: durable
   sending, one write attempt and exact readback. Forge remains remote Git push only.
4. Delete automatic projection watermarks/cursors/quarantine/drain. Remote
   repository/ref/head/time/error feeds the independent Governor remote collector.

Preserve immutable repository ID checks, bounded response/text limits, no credential
storage in Store, unknown-after-write semantics and exact readback before any retry.

## 9. Remote gateway audit

The gateway already has the right narrow product boundary:

- loopback-only listener;
- exact `/mcp` path and no query;
- one configured Eliot credential and restricted MCP profile;
- bearer auth with duplicate-header rejection and constant-time comparison;
- Host/Origin validation delegated to RMCP;
- body/header/request-time/connection limits;
- timeout explicitly reports unknown outcome rather than safe retry;
- stopping the HTTP route does not cancel admitted Eliot work.

Remaining defects:

- credential and bearer files are read with ordinary `std::fs` calls: no shared
  regular-file/private owner/ACL/mode/symlink-reparse enforcement;
- one credential/profile/bearer is shared by all connections and negotiated sessions;
  this is one principal, so one process/listener must be treated as one tenant;
- connections are bounded, but negotiated session count/idle lifetime needs an explicit
  bound rather than relying on transport disconnect behavior;
- the kernel-host compatibility launcher remains an extra entry path after callers move.

Use the existing bounded private-file primitive, bind one gateway process to one
identity/profile, cap active sessions and idle lifetime, retain loopback/Host/Origin/
body/time controls and leave TLS/edge identity outside. Do not add an identity broker,
reverse-proxy product or gateway database.

## 10. Findings that remain unproved

The audit is still incomplete. Remaining source work includes:

- reconcile every retained requirement from old specs/scripts against current callers;
- finish public method → role → exact object → Store/effect/result tracing;
- cover cancellation, late result, restart and partial-write branches for retained
  paths;
- reconcile findings after intervening runtime commits;
- execute migration/corruption/partial-upgrade and privilege matrices after the reduced
  surface exists;
- live-qualify Windows custody, provider restarts, mixed-provider routes, gateway and
  scale on the owner machine.

Do not turn these gaps into another issue hierarchy or framework. Record evidence in
this file, implement the existing vertical slices and keep status honest.
