# Eliot Swarm Controller — Implementation

This is the only implementation program for the
[architecture](architecture.md). Runtime source was rechecked through
`96a0d5a6632cfbd541e804d7969d3e76e716bf6a` on 2026-10-19.
[Findings](refactoring.md) retain source evidence; [status](../implementation-status.md)
records what actually landed.

Do not start another repository-wide migration. Complete one vertical
caller-to-result slice, delete the replaced path, then use a second real consumer to
prove any shared implementation.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Working rules and owners

Every coding slice states observable behavior, exact source owners, unsupported and
uncertain cases, old code/config/fixtures to delete, one relevant native API or donor
mechanism, the minimal affected format/Clippy/build command, and net owned-code change.

The manager owns shared contracts, Store transitions and integration. Leaf workers
receive non-overlapping local changes. A schema, fixture, wrapper or new crate is not
progress unless a current caller uses it and the superseded path disappears.

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, review, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Current semantic adapter commands and bounded records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` / `swarm-client::Client` |
| Process membership, custody and private durable files | `crates/swarm-process` |
| Native transport, session state, local provider readers and uncertain native effects | selected adapter |
| Workspace/write authority | `store/workspace.rs` plus `crate::workspace` |
| Queue, route choice and launch admission | existing Store launcher owner |
| Time-based admission | one in-process host loop; Store owns due/occurrence state |
| Checks, scripts, Git/Forge and GitHub effects | existing dedicated effect owners |
| Public CLI and MCP | `swarm-cli` and `swarm-mcp`, calling Store handlers |

No adapter accepts a Task or interprets review. No common shell owns a provider
session, native retry policy, scheduler or transcript store.

## 2. Executable order

```text
A0-A3  Antigravity: one active route, self-contained result, durable Store delivery,
       real typed actions only
B0-B4  current-state queue, explicit routes, provider state, root capacity,
       dispatch_once
C0-C1  Task path scope and one workspace write lease
D0-D1  minimal process owner record and conservative restart reconciliation
E0-E3  one schedule model and one in-process admission loop
F0-F2  compact TaskPacket; Store identity stays outside model text
G0-G4  review/correction/acceptance through ordinary Store transitions
H0-H3  split public/internal/adapter protocols; one domain authorizer
I0-I3  numeric Store migrations; delete SQL-text compatibility
J0-J3  concurrent MCP IPC, one notification pump, exact task-attention read,
       ephemeral native streams off the Store writer
K       shared adapter shell only after two current routes delete duplication
L       one executor per harness, Goals, passive observation and remaining deletion
```

A later section does not block an earlier vertical slice unless the earlier slice
would otherwise preserve the mechanism being deleted.

## 3. A — first executable route: Antigravity Rust

Antigravity is first because it has one serialized native input window and a proven
lost-delivery boundary. Command, Codex and Muse are poor first abstractions because
their historical profiles and native state machines would infect the common layer.

### A0 — one active coordinate

Source and descriptor use artifact version `6`; active route/tooling still contains
`5`. Update route example, package policy, qualification, installer validator and
active fixtures together. Historical v5 receipts remain historical. Give concrete
unsettled v5 bindings a final disposition, then remove active `"5" | "6"` behavior.

### A1 — outcome independent of observation/proof

Make terminal outcome final at native settlement. Retain Operation ID,
binding generation/adapter boot, native conversation/result ordinal, terminal status,
Eliot disposition, result reference and content hash where bytes exist.

Delete `local_execution_ref.observation_id`, duplicated terminal results in
observations, outcome-to-observation matching, `AntigravityDispatchSessionProof`, its
producer/validator/digests and refresh-before-dispatch fixtures. `agent.refresh`
remains readback; model/stream/counter facts remain observations, not dispatch rights.

### A2 — durable Store delivery, never native replay

Use existing private atomic file primitives. One file retains one already-formed
`module.outcome` or `module.result` request scoped to Operation, generation and boot.
Persist before memory can forget it; resend identical params after Eliot reconnect;
delete only after exact Store ACK. A new boot never adopts predecessor files.

Observations use one replaceable pending checkpoint per source. Do not add a database,
append-only journal, tombstones, compaction or native intent to this spool.

### A3 — only real actions

Retain `Open`, `Dispatch`, `Send(next_turn)`, `Refresh`, `Result`, `Reconcile` and
release/shutdown of an owned resource. Do not advertise placeholder Reply, Configure,
Goal, Background, Recover or live steer. Replace string switching with a typed command
and remove package/protocol/request and repeated Task/source hashes from outcomes.

## 4. B — queue, routes, provider state and capacity

<a id="work-dispatch"></a>

### B0 — one queue projection

`swarm.queue.get` reads current Task/Attempt, correction/review, dependencies,
workspace conflicts, route/provider/root capacity and binding availability. Ranking
is deterministic: correction, unfinished Attempt, ready Task, explicit phase order,
then age and stable Task ID. The response explains `why_ranked`; a page is never a
reservation.

### B1 — explicit route policy; lane remains a view

One manager policy stores enabled state, project/task selector, ordered route aliases
and optional agent profile. A route alias owns exact harness/options/model/effort/root
limit. Automatic fallback selects only the next configured alias and records it in the
launch Operation. A lane is a UI/report projection, not a table, process, inbox,
cursor or scheduler.

### B2 — source-scoped provider facts

Normalize separate `ProviderUsage` and `ProviderAvailability` records with source
key/epoch/sequence, observation time, freshness/coverage and honest account/service/
connection scope. Adapters own native method names and rolling patches. Remove
provider method/artifact allowlists, auth hashes, vendor-only universal fields and
per-binding copies of account/service facts.

No fact permits one bounded probe. Transient negative facts hold until retry/reset,
then permit one probe rather than claim recovery. Hard negative needs source/config
change or explicit fallback. Unknown/stale/gapped holds automation but remains
manually probeable.

### B3 — capacity from actual roots

Count unresolved root-opening Operations and unreleased roots in the same writer
transaction that admits launch/open. Dispatch, send, reply, configure, Goal, refresh,
result, reconcile and recover create no root claim.

Delete `CapacityLedger`, `CapacityEntry`, `ResourceEvidence`, capacity meta keys,
v1/v2 codecs/migration, refusal rows and lifecycle sync hooks. `report.capacity`
becomes a current projection.

### B4 — `dispatch_once`

A bounded pass queries ranked Tasks, selects the first eligible configured route and
atomically validates manager/project/Task revision, creates or reuses Attempt,
reserves write lease, checks provider/root capacity and creates launch Operation.
External I/O follows commit.

Delete WorkDispatch cursors, copied pending subjects, catch-up/recheck state,
semantic slots, duplicate links, ordinary-blocker quarantine, mandatory preview
digest and copied launch settings. Wakeups are hints; the next pass reads current
state.

## 5. C-D — one write authority and minimal process custody

### C0-C1 — workspace lease

`TaskSpec.scope.initial_paths` supplies repository-relative write prefixes;
`forbidden_paths` is a hard negative; empty initial scope means repository root;
review-only work has no write lease. Normalize separators and reject absolute paths,
traversal, empty segments and `.git`. Symbols/interfaces/globs are guidance only.

`reserve_lease_for_launch` checks path-prefix overlap in the same admission
transaction. No generic override. Delete `code.scope.*`, its records, digests,
override/expiry/symbol authority and frontend/catalog/tests after callers switch.

### D0-D1 — process ownership

Persist binding/generation/boot, state directory, credential reference, selected
executable, PID + OS birth/handle identity, Job/process-group identity and
parent/shared-service relation. Package version/build/protocol remain installation
facts.

On restart: dead clears record; PID identity mismatch clears record and kills nothing;
uninspectable stays unknown; only an exact Eliot-owned leftover may be recovered or
terminated; shared service is not killed for one lost session. Do not sweep by name,
cwd, latest log or age, and do not hash the executable on every metric sample.

## 6. E — one scheduling/admission loop

<a id="automation-and-delivery"></a>

Use one automation-entry schedule model: immutable entry ID, enabled state, event/
interval/cron trigger, typed action/settings, last admitted occurrence and next due.
Convert static schedules and delete their registry.

One in-process loop waits for Store changes or nearest due time, runs bounded
current-state dispatch/review/return-changes/accept/check/watch passes and computes the
next due time. A slow safety wake recovers lost/coalesced notifications.

Delete the standalone automation scheduler service, credential/config digest,
page/admit RPC, process custody, duplicate due snapshots/quarantine and timer-worker
slots. Retain separate workers only while they perform real external I/O/custody.

## 7. F-G — compact TaskPacket and ordinary review transitions

### F0-F2 — model-visible work

One renderer serves initial assignment, correction and resume. The `TaskPacket`
contains goal, current requirements/check criteria, phase, selected sources, write and
forbidden paths, worktree/branch/baseline, selected route/model/effort, deliverable and
current correction/resume facts.

Store identity stays in Store and the retained Operation. Delete canonical Task JSON,
launch packet, binding generation, proof digests, full history and repeated
`task_snapshot_sha256`, `source_text_sha256`, size/prompt hashes where they only echo
Store-owned data. Keep content hashes only on exact persisted bytes.

### G0-G4 — review/correction/acceptance

`review_once` assigns missing review for an exact candidate. `review.submit` stores
verdict and the complete findings set. `task.request_changes` verifies the current
candidate/review, sets Attempt to `needs_correction`, stores one cause/findings set and
optionally admits one child `agent.send`; without a live binding the Task state is
sufficient for the next dispatch/resume.

Candidate B requires review B. Identical candidate after correction is no-progress;
correction rounds live in Attempt lifecycle and escalate after a bound. `accept_once`
calls the same candidate-specific review/check gate as manual acceptance.

Retain only thin policy toggles: dispatch, review, return_changes, accept, check,
goal_message and script. Delete ReviewDisposition/RepairDispatch workflow state,
cursors, slots, duplicate links/evidence packages and automatic publication/GitHub
projection stages.

## 8. H-I — one API owner and numeric migrations

### H0-H3 — public, internal and adapter protocols

Split application API, internal host protocol and typed adapter protocol. Internal and
vendor-native methods never enter the public registry. Remove optional products before
freezing the final table.

Each retained public domain owns one typed parser/authorizer/handler:
`parse -> role -> exact object/revision -> read/mutate`. Central routing does not
repeat field schemas or object rules. Operation visibility uses normalized retained
scope, not method-name `LIKE`/`IN` branches and product-specific joins.

A removed method disappears from Store routing, method policy, MCP, CLI, Operations,
automation/capacity consumers, tests and fixtures together. Do not replace the current
parallel maps with a generic policy DSL.

### I0-I3 — migration ledger

Use SQLite `application_id`, monotonic `user_version` and a small
`schema_migrations(version, applied_at_ms)` table. Writer applies missing ordered SQL
inside one `BEGIN IMMEDIATE` transaction before readers start.

For one finite release, map concrete known legacy digest/marker states to numeric
versions after checking actual tables/columns. Unknown/inconsistent state fails.
Then delete base SQL digest, generic extension SHA comparison, custom DDL lexer/
reference DB, duplicate trigger verifiers and per-migration source hashes.

Readers validate application ID and supported numeric version, not exact bundled SQL
bytes. Equivalent DDL text cannot brick a compatible DB; partial migration rolls back.

## 9. J — bounded IPC, one MCP wake pump and reader-owned live views

### J0 — use the multiplex that already exists

`swarm-client::Client` already correlates concurrent requests, bounds in-flight work
and marks post-write cancellation/transport loss `OUTCOME_UNKNOWN` without replay.
`swarm-mcp::request_on` currently holds `Mutex<Option<Client>>` across the whole RPC,
serializing every tool call again.

Store `Option<Arc<Client>>`. Hold the mutex only to connect/install or compare-and-
clear a failed current client. Clone the Arc, release the lock, then call
`request_shared`. On transport failure clear the slot only if it still points to that
same client. Never retry the request automatically.

### J1 — one session notification pump

Current MCP permits 16 subscriptions; each owns a poller, forwarder, record queue and
1 MiB byte budget, polling `report.delta` every 250 ms and scanning extra pages under
lag. Replace this with one pump per MCP session:

- one last Store cursor and one bounded reconnect/backoff state;
- one category/filter set for the session, or one subscription ID if API continuity is
  temporarily required;
- one coalesced `changed` notification containing latest known Store head/cut;
- `monitor.follow` / `report.delta` remains the authoritative resync;
- a slow client replaces the pending hint with a newer head instead of mirroring every
  committed fact;
- reconnect starts from the client's last acknowledged Store cursor; there is no MCP
  session replay continuity.

Delete per-subscription pollers/forwarders/queues, exact dropped-item scan-to-cut,
per-category replay machinery and Concilium/contract categories when those products
are removed. A notification is a wake hint, never history or authority.

### J2 — exact pending-input projection

`tasks/get` currently pages the entire `report.attention` inventory and filters one
Operation in MCP. Add an exact bounded Store read keyed by Operation/binding, or include
its current pending-input summary in `operation.get`. Preserve the native request's
own shape and exact stale-request guard. One Task read must not be O(fleet attention).

### J3 — native content stream off the writer

`module.stream.publish`, `stream.open`, `stream.read` and `stream.close` mutate/read an
ephemeral in-memory ring but currently execute through `Store::run`, occupying the
single writer while doing authorization and JSON/ring work.

Use the existing WAL/status-reader transaction to resolve current principal, object
rights, binding/generation/boot and source identity. After that bounded snapshot,
mutate/read the ring outside the DB writer. Revalidate on every read; revoke viewers
for a disconnected link instead of waiting only for TTL. Keep this path distinct from
durable observations: it is bounded live content, not another event journal.

Do not add a broker. Keep host IPC's existing bounded concurrent server, frame limits,
write timeout and “admitted work drains after disconnect” semantics.

## 10. K-L — adapter consolidation, Goals, observation and remaining deletion

<a id="processes-and-frontends"></a>

Extract shared adapter bootstrap/delivery only after two current Rust routes adopt it
and delete both old loops. Extend existing `swarm-process::module_owner` and
`swarm-client::ModuleLink`; the common shell owns Eliot connect/hello, command pull and
exact re-delivery of already retained Store messages—not vendor session, native queue,
result attribution, retry policy, Task state or scheduling.

Target semantic commands are `Open`, `Dispatch`, `Send`, `Reply`, `Configure`, `Goal`,
`Background`, `Cancel`, `Recover`, `Refresh`, `Result`, `Reconcile`, `Shutdown`.
Unsupported actions fail before native I/O. Remove vendor/internal method names,
package/schema ladders and proof-heavy receipts.

Retain one executor per harness: standalone Rust OpenCode, Rust app-server Codex, one
selected Muse, one Claude, one current Batch and ACP Command generation, one
Antigravity warm stream. Concrete unsettled work receives finite disposition;
historical records do not keep executors alive.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

Harness Goal maps through native `Goal`; Eliot Goal independently admits ordinary
message Operations. Their controls do not affect each other. Remove provider branches,
pseudo-Goal continuation and Goal proof packets.

Adapters read native events and local JSON/JSONL/SQLite; host adds process and Git;
Store joins Task/Attempt/Operation, binding, session, process, worktree/ref, source
cursor/gaps and provider usage. Configured readers run without Task/binding and expose
unmatched sessions. Remove per-sample executable hashing and provider calls from
viewer reads.

<a id="store-and-current-data"></a>
<a id="identity-and-hashes"></a>

Make launch preview optional; first dispatch needs current assignment, workspace
lease, binding and usable session—not MCP readiness or a caller proof hash. Keep one
result path: `Result -> module.result -> immutable artifact -> read/assemble ->
submission`.

Move pure bounded reads to WAL readers; never hold a Store transaction across
external I/O or client pagination. Give automation immutable entry ID and update
owner/revision in one transaction; delete successor copies, relocated journals and
chains.

Remove Concilium, generic contract/integration ratification/ACK, automatic
publication/Issue-label projection, document-edition rights, legacy acceptance,
retired installers/provenance equality and worker implementation dependencies outside
the compact core.

## 11. Finite transition

<a id="current-format-and-existing-data"></a>

Backward compatibility is not a product feature. Before deleting a protocol,
executor, method or storage path, enumerate concrete unsettled Operations and required
results. Finish/read back with the current owner, export the result, or create a new
Task revision/Attempt. Then remove producer, reader, schema branch, config, fixture and
package together.

Do not write converters for hypothetical history. Completed records may stay opaque
when current projections contain the needed facts; they do not keep runtime code alive.

## 12. Verification

<a id="verification"></a>

| Boundary | Required proof |
| --- | --- |
| Antigravity | v6 only; no proof refresh gate; terminal Store delivery survives reconnect without native replay; removed handlers have no callers. |
| Queue/routes/provider/capacity | deterministic reasons; blockers do not hide later Tasks; fallback explicit; source freshness honest; concurrent root admission obeys limit; old ledgers absent. |
| Workspace/process | overlapping writers cannot launch; disjoint prefixes can; PID reuse cannot kill foreign process; uninspectable owner stays unknown. |
| Scheduling | one due/admission loop; lost wake recomputes current state; same occurrence admits once; standalone scheduler absent. |
| TaskPacket/review | model receives bounded actionable work; initial/correction/resume share renderer; all findings return together; B cannot reuse A review; no-progress stops. |
| Public API/migrations | one domain authorizer per retained method; internal/adapter protocols separate; removed surface absent everywhere; numeric migration is atomic and readers reject unsupported version. |
| IPC/MCP | slow tool call does not block unrelated call on the same session; lost mutation reply remains unknown and is never replayed; one wake pump is bounded/backed off; Task pending-input read is exact; native stream traffic does not occupy the writer. |
| Adapter/executor/observation | two adapters prove shared shell; one executor per harness; unbound sessions/gaps visible; exact input/result attribution; uncertain stream stays unknown. |
| Deletion | no config, package, CI, docs or fallback wrapper remains; report net owned Rust+JS+glue separately from tests. |

Documentation changes run content/link checks. Runtime slices start with formatting and
the minimal affected warnings-denied Clippy/build. Focused connected cases run when
the slice closes; broad qualification follows the complete product path.
