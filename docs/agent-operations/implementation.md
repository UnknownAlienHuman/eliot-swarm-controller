# Eliot Swarm Controller — Implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source, active tooling, old Claude Swarm
scripts and current donor sources were rechecked through
`8355bdf0fbe561b5163bd3ab1ba6db24c0f8ddbc` on 2026-10-19.
[Findings](refactoring.md) preserve source evidence; [status](../implementation-status.md)
records completed work.

Do not start another repository-wide framework migration. Implement one connected
caller-to-result slice, delete the replaced path, then use a second real consumer to
prove any shared abstraction.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>

## 1. Work as vertical slices

Every coding slice states:

- observable behavior;
- exact source owners and symbols;
- unsupported and uncertain cases;
- old callers, formats, fixtures and configuration to delete;
- one relevant donor mechanism/native API;
- minimal format/Clippy/build command;
- connected completion proof and net owned-code change.

The manager owns shared contracts, Store transitions and integration. Leaf workers
receive compact non-overlapping tasks. A type, schema, fixture or adapter stub is not
progress unless a current caller uses it and the superseded path disappears.

Before adding durable state, answer:

1. Which authoritative facts are insufficient?
2. Why can the answer not be derived by one bounded query?
3. Who owns mutation and recovery?
4. Which old state disappears in the same change?

If those answers are weak, do not add the state.

## 2. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, rights, acceptance and durable observations | `crates/swarm-kernel-host/src/store` |
| Shared semantic adapter commands and bounded records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `crates/swarm-client::ModuleLink` |
| Adapter process membership/private files | `crates/swarm-process::{module_owner,module_membership,permissions}` |
| Native transport, session state, provider readers and uncertain native effects | selected `swarm-adapter-*` or `modules/*-rust` route |
| Workspace/worktree/write authority | `store/workspace.rs`; `crate::workspace` performs filesystem/Git effects |
| Queue, route decision and launch admission | current Store launcher owner; no second queue/capacity service |
| Time-based admission | one in-process host control loop; Store owns due/occurrence state |
| Check, Script and Forge effects | their existing domain owners |
| Process/worktree/Git collection | current host lifecycle/process owners |
| Public CLI/MCP | `swarm-cli` and `swarm-mcp`, both calling Store handlers |

No adapter accepts a Task, changes ownership or interprets review. No common shell
owns a vendor session, native retry policy, scheduler or transcript store.

## 3. A — first executable route: Antigravity Rust

Antigravity is first because it has one serialized native input window and a concrete
lost-delivery defect. Do not define the common shell around Command, Codex or Muse;
their historical profiles and native state machines would leak into the abstraction.

### A0 — one active v6 coordinate

Synchronize source, descriptor, route example, package policy, installer,
qualification and active fixtures on Antigravity v6. Historical v5 receipts remain
historical; concrete unsettled v5 bindings receive final disposition. Remove active
`5 | 6` behavior and never introduce v7 merely to mark the cleanup.

### A1 — outcome independent of observation/proof

Make terminal outcome final at native settlement. Retain Operation, generation/boot,
native conversation/result ordinal, terminal status, disposition, result reference
and content hash where bytes exist.

Delete observation ID from outcome, duplicate terminal result in observations,
outcome-to-observation matching, `AntigravityDispatchSessionProof`, its producer,
validator/digests and refresh-before-dispatch fixtures. Refresh remains readback;
model/stream/counter facts remain observations, not rights.

### A2 — exact Store delivery, never native replay

Use existing private atomic files. One file retains one already formed
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

<a id="work-dispatch"></a>
<a id="automation-and-delivery"></a>

## 4. B — queue, routes, provider state and capacity

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

Normalize separate `ProviderUsage` and `ProviderAvailability` with source
key/epoch/sequence, observed time, freshness/coverage and honest account/service/
connection scope. Adapters own native method names and rolling patches. Remove method/
artifact allowlists, auth proof hashes, vendor-only universal fields and per-binding
copies of account/service facts.

No fact permits one bounded probe. A transient negative holds until retry/reset, then
permits one probe rather than claiming recovery. A hard negative requires source/
config change or explicit fallback. Unknown/stale/gapped holds automation but remains
explicitly probeable by a manager.

### B3 — transactional root count

Derive capacity from unresolved root-opening Operations and unreleased bindings;
never count both for one root. Reserve/count in the launch/open transaction. Send,
reply, configure, Goal, refresh, result, reconcile and recover consume no new root.

Delete `CapacityLedger`, phase/event evidence, `capacity:*` meta rows, v1/v2 key
migration and lifecycle sync hooks. `report.capacity` becomes a bounded projection of
routes, roots, provider and host facts.

### B4 — `dispatch_once`, not copied pending work

```text
dispatch_once(policy)
  -> read bounded ranked current Tasks
  -> select first eligible configured route
  -> atomically validate Task/Attempt, reserve workspace/root and create launch
  -> return Operation or current blocker reasons
```

External filesystem/Git/native I/O follows commit. Delete WorkDispatch global/per-
entry cursors, pending subjects/rechecks, activation/catch-up state, semantic slots,
duplicate links, blocker quarantine, preview-digest binding and copied launch config.
Wakeups cause another query; a safety wake recovers lost/coalesced notification.

## 5. C — one workspace/write authority

### C0 — path-prefix scope

`TaskSpec.scope.initial_paths` supplies repository-relative write prefixes;
`forbidden_paths` is a hard negative boundary; empty initial paths means repository
root; review-only work gets no write lease. Reject absolute paths, traversal, empty
segments and `.git`. Compare by path segment, not string prefix. Symbols/interfaces/
globs guide work but grant no write authority.

### C1 — one admission decision

`reserve_lease_for_launch` checks overlap in the same transaction as Attempt/launch
admission and names the conflicting Task/Attempt/paths. Do not add a generic override:
revise scope, finish/release one Attempt or split the work.

Delete `code.scope.*` coordination/Store code, CLI/MCP/policy/catalog entries,
proposal digests, override/expiry and references from ratification products.

## 6. D — process ownership and restart

### D0 — minimal owner record

Persist binding/generation/boot, private state/credential reference, selected
executable, PID + OS birth identity, group/Job identity and parent/shared-service
relation. Package/build/protocol/full contract remain installation facts. Simplify
`ModuleOwnerPlan`/resolver while retaining containment, no-link traversal, private
permissions and exact credential scope.

### D1 — conservative reconciliation

- dead process → clear live record;
- PID exists but birth/image/owner differs → clear record, kill nothing;
- uninspectable → retain `unknown` and retry later;
- exact Eliot-owned leftover → recover/observe/terminate by route policy;
- shared provider service → never kill because one session link disappeared.

Do not sweep by executable name, cwd, title, newest log or age, and do not rehash an
executable on every metric sample.

<a id="store-and-automation"></a>

## 7. E — one schedule/admission loop

### E0 — one retained schedule shape

```text
entry_id
trigger/calendar
next_due_at
last_occurrence_id
catch_up = latest_only
```

Static schedule config, manager cron and standalone scheduler pages cannot remain
three representations of the same due work.

### E1 — one in-process wake/timer loop

One host loop waits for Store change or nearest due time, queries bounded due entries
and admits actions through the same Store gates as manual commands. A slow safety wake
recovers lost notification. The timer owns no credential/process identity.

### E2 — delete the standalone scheduler service

Remove `automation.scheduler.page/admit`, service credential/config digest, worker
process/group, duplicate cursors and status projections after the host loop serves all
retained schedule types.

### E3 — workers only for real effects

Checks, Scripts, Forge and adapters may remain separate because they perform external
I/O or own native processes. Pure policy/query/reconciliation helpers remain in host.
Delete optional worker slots that only call Store and wait.

<a id="identity-and-hashes"></a>

## 8. F — one compact TaskPacket

### F0 — model-visible packet

`store/task_prompt.rs` is the only renderer. Produce bounded Markdown with Goal,
requirements, initial/correction/resume state, selected source references, write/
forbidden paths, workspace/branch/baseline, deliverable and verification.

Do not send raw Task/Attempt JSON, snapshot/plan/source hashes, package coordinates,
launch manifest, proof packet, global queue, participant roster, full Issue thread or
all MCP schemas. Default to 64 KiB; larger inputs remain artifacts/references.

### F1 — Store identity stays outside model text

Replace `TaskPromptEnvelopeV1` + `TaskDispatchContext` duplication with:

```text
operation_id
binding generation + adapter boot
task_packet text
optional native selectors
```

Store resolves Task/Attempt from Operation. Adapter validates authenticated binding
and associates native input/output with Operation. A content hash is allowed only for
actually retained immutable packet bytes; it is not authorization.

### F2 — one renderer across lifecycle

Initial assignment renders current work; correction adds all findings for the exact
candidate; resume adds remaining requirements/current candidate and unresolved
attention, not transcript history. Delete old prompt schemas/JS validators/fixtures as
each route migrates.

## 9. G — ordinary review, correction and acceptance

### G0 — `review_once`

Query current submissions requiring review and lacking current assignment for that
candidate/slot; call the existing `review.assign` gate with deterministic identity.
Do not copy submissions into pending lists or keep a dispatch cursor.

### G1 — one `task.request_changes`

Request names the exact current review result and expected Task/Attempt/candidate.
Store loads all ordered findings; caller does not submit a second package. In one
transaction Store verifies authority/current candidate, records disposition, marks
Attempt `needs_correction`, retains review as cause and, if a live binding exists,
admits one child `agent.send` with correction TaskPacket.

Delete ReviewDisposition, RepairDispatch, repair slots/links, duplicate findings
packages and per-finding delivery.

### G2 — candidate-specific bounded rounds

Candidate B never reuses A's review. Return all findings in one round. Derive round
count and identical-candidate no-progress from retained submissions/reviews. On no
progress or configured limit, create manager attention and stop automatic correction.

### G3 — `accept_once` uses the manual gate

Query current candidate and call the same `task.accept` gate. Store resolves exact
candidate review/check evidence; automation does not rebuild evidence arrays.

### G4 — thin policy switches

Retain `dispatch`, `review`, `return_changes`, `accept`, `check`, `goal_message` and
`script`. Publication/Issue labels are explicit integrations. Current Task/review/
acceptance state is queried directly.

## 10. H — shrink and split the API

The current catalog has 164 hand-written MCP tools while method policy, Store routers,
role gates, CLI mapping and tests repeat the surface.

### H0 — separate three protocols

- public semantic methods for authenticated clients;
- closed internal methods for module/supervisor/workers;
- typed adapter commands.

Internal or vendor methods never enter the public MCP catalog.

### H1 — delete excluded products end-to-end

For Concilium, generic contract/integration ratification, advisory code scopes,
automatic publication/labels and retired vendor-internal methods, remove together:
method policy, MCP schema/profile, CLI, Store router, model validation, auth
projection, migrations/config/fixtures/docs. Keep no compatibility aliases for
abandoned internal APIs.

### H2 — one domain authorizer/handler

Each retained public domain owns `parse -> coarse role -> exact object/revision ->
read/mutate`. A small shared descriptor may own name/class/frontend exposure, but it
must not replace object authorization or become a generated protocol family.

### H3 — merge only truly identical aliases

Trace callers before merging message/coordination methods. Shared storage does not
prove identical scope semantics. Remove alias and consumers in one change.

## 11. I — numeric Store migrations

### I0 — current numeric schema authority

Use SQLite application ID and integer version plus one ordered migration list owned by
Store startup. Each migration runs transactionally, checks semantic preconditions and
records version/receipt. Readers verify version and required shape, never SQL source
bytes.

### I1 — finite bridge for concrete active data

Enumerate only unsettled Operations, current Tasks/Attempts/candidates/reviews/checks,
active bindings/workspace leases, required results, enabled automation and non-
reissuable credential/config references.

Prefer a fresh current database and bounded import; archive old DB read-only. If one
in-place transition is required, support exactly one known from-version → to-version
migration and delete it after supported installs cross.

### I2 — delete source-text compatibility

Remove base schema SHA, `schema_extension:*` digests, reference-DB replay, custom DDL
lexer/normalized `sqlite_master` equality and duplicate trigger verifiers once the
numeric migration path owns startup.

### I3 — relational admission scope

At Operation admission persist small indexed scope facts Store already knows:
caller/effective manager, project, Task/revision, Attempt, binding/generation, parent
Operation and closed visibility class. Migrate one method family at a time from giant
method-specific JSON/link visibility SQL, preserve exact object rights, then delete
legacy branches/meta links.

## 12. J — MCP/IPC and native live streams

### J0 — concurrent RPC over the existing multiplexed client

Do not hold the MCP session mutex across awaited Store RPC. Clone the current
`Arc<Client>` under the lock and release it before `call`. Bound per-session in-flight
requests and preserve request identity; lost mutation response remains unknown and is
never retried automatically.

### J1 — one coalesced subscription pump

Replace per-subscription pollers/forwarders/queues with one session pump over the
Store cursor. Fan out latest notifications to subscribers with bounded queues; drop
and report a gap rather than rescanning history to count every filtered drop. Store
cursor remains authority.

### J2 — exact pending-input projection

`tasks/get` must not page the entire attention inventory. Add exact bounded Store read
by Operation/binding or include current pending-input summary in `operation.get`, with
native request shape and stale-request guard.

### J3 — native content ring off the writer

Resolve current principal/object/binding/source rights in a short WAL reader snapshot,
then operate the bounded ephemeral ring outside Store writer. Revalidate every read
and revoke link-owned viewers on disconnect. This remains distinct from durable
observation history; do not add a broker.

<a id="build-install-ci-and-qualification-reduction"></a>

## 13. K — one physical owner per external-effect domain

### K0 — one CheckRun executor

Make standalone `swarm-checks` mandatory for every new CheckRun. Keep old hidden-host
receipts readback-only. Move bounded source/tool/Git-hook probe modes to the selected
check executable first, then remove the root-host `check-worker`, duplicate custody,
packaging and reconciliation branches. Store keeps Check admission/profile/coverage/
verdict.

### K1 — one ScriptRun wire protocol

New work already requires `swarm-script-worker`; `None` is historical only. Move
cross-process Work/plan/start/completion DTOs to existing `swarm-scripts`, use them
from host/worker, then delete hidden compatibility entrypoint and legacy Go/Deny
readers after concrete old runs are settled.

### K2 — one Forge remote-effect owner

Keep accepted-candidate push and exact remote-ref readback in
`swarm-forge-worker`; keep bounded local workspace Git in host. Preserve one
repository/ref serialization owner and never replay unknown push. Delete duplicate
installer, parallel JSON validators and automatic label/publication projection.

### K3 — verify packages once

Installer verifies selected package bytes and publishes immutable installed identity.
Launch checks containment/current identity; OS birth/image protects custody. Stop
hashing worker/Git executables per Check/Script/Forge Operation and repeating those
hashes in domain receipts. Keep hashes for credentials, package publication and actual
result/artifact bytes.

### K4 — share only physical OS primitives

Use `swarm-process` Group/Job, birth/image identity, bounded capture and private atomic
files. Do not create a universal EffectRunner/journal/completion DTO; Check, Script and
Git retain distinct semantics.

<a id="module-contract-and-adapter-shell"></a>
<a id="processes-and-frontends"></a>

## 14. L — semantic adapter protocol and shared shell after proof

Target commands: `Open`, `Dispatch`, `Send`, `Reply`, `Configure`, `Goal`,
`Background`, `Cancel`, `Recover`, `Refresh`, `Result`, `Reconcile`, `Shutdown`.
Unsupported actions fail before native I/O.

Remove vendor/internal methods, package/schema ladders and proof-heavy receipts.
Simplify hello to authenticated binding + current protocol major + capability subset.

Extract common bootstrap/delivery only after two current Rust routes use identical
behavior and delete both old loops. Extend existing `swarm-process::module_owner` and
`swarm-client::ModuleLink`; common code owns Eliot IPC and exact already formed Store
delivery, never vendor session, native queue/recovery, Task state or scheduling.

## 15. M — Claude route reduction

Current useful core: Rust writes intent before SDK input, assigns a user-message UUID,
correlates SDK frames to Operation, and never guesses after transport loss. Preserve
that. The official SDK already supplies `startup()`, streaming `query()`, permissions/
hooks, session identity and session-history readers.

### M0 — one live session owner

Keep one ordered owner for startup/query/input queue/frames. Remove full descriptor
echo and repeated Task/source hash validation as H/F land. The Node process remains a
thin SDK driver; Rust owns Operation correlation.

### M1 — split native effect from Store delivery

Replace the 183-KB mixed `OperationJournal` with:

```text
NativeEffectRecord
  operation_id, action, boot/session, native input/request ID
  exact bounded native payload identity
  disposition/readback cursor

StoreDelivery
  exact module.outcome or module.result params
  generation + boot + sequence
```

Native record lives only until native disposition is known. Store delivery lives only
until exact ACK. Neither owns retention graphs, tombstones, cleanup queues, generic
references or Task state.

### M2 — capture terminal result before memory loss

At SDK result frame, persist bounded immutable body + session/input/frame identity
before it can disappear from `NativeControl`, then enqueue exact `module.result`.
For larger/unavailable bodies, use supported session-history reading with explicit
coverage gap; do not maintain a transcript mirror.

A lost Store ACK may resend identical already formed result bytes. It must not replay
the native prompt. Delete `MODULE_RESULT_REPLAY_PREVENTED` behavior after exact
Store-delivery idempotency/readback owns this boundary.

### M3 — simplify live permissions/questions

Expose current pending callback through observation. Before resolving it, admit one
exact `agent.reply` Operation with request/session/boot/reply identity. ACK settles;
bridge loss becomes Unknown/Retired. A replacement query never receives the old
reply.

Delete the 119-KB interaction retention engine: retention state/items, pending index,
tombstones and cleanup cursor. Store Operations/observations retain the audit trail;
a JavaScript resolver cannot be durably reconstructed anyway.

### M4 — connected deletion proof

Qualify open → dispatch → callback/reply → result; Eliot disconnect after terminal;
bridge loss before/after callback delivery; result retrieval after adapter restart;
exact session/Operation attribution. Report net Rust + JS deletion, not only moved
files.

<a id="harness-goals-and-the-eliot-goal-plugin"></a>
<a id="passive-agent-observation"></a>
<a id="store-and-current-data"></a>

## 16. N — remaining harnesses, Goals, observation and product deletion

Retain one new-work executor per harness: standalone Rust OpenCode, Rust Codex app-
server, one Muse, one Claude, current Command Batch + one ACP, one Antigravity warm
stream. Concrete unsettled work receives finite disposition; history does not keep an
executor alive.

Harness Goal maps through native Goal and is default when supported. Eliot Goal
independently admits ordinary message Operations. Remove provider branches, pseudo-
Goal continuation and Goal proof packets.

Adapters read native events and provider-local JSON/JSONL/SQLite; host adds process/
Git; Store joins Task/Attempt/Operation, session, process, worktree/ref, source gaps
and provider usage. Configured readers run without Task/binding and expose unmatched
sessions. Viewer reads make no provider call.

Move pure bounded reads to WAL readers; never hold Store transaction across external
I/O/pagination. Give automation immutable entry ID and delete successor copies,
relocated journals/chains. Remove Concilium, generic ratification/ACK, automatic
publication/labels, document-edition rights, legacy acceptance, retired installers/
provenance equality and worker implementation dependencies outside compact core.

<a id="current-format-and-existing-data"></a>

## 17. Finite transition

Before deleting a protocol, executor, method or format, enumerate concrete unsettled
Operations and required results. Finish/read back with current owner, export result,
or create a new Task revision/Attempt. Then remove producer, reader, schema branch,
config, fixture and package together.

Do not write converters for hypothetical history. Completed records may stay opaque
or in an archived old DB; they do not keep runtime code alive.

<a id="verification"></a>

## 18. Verification

| Boundary | Required proof |
| --- | --- |
| Antigravity | v6 only; no refresh/proof gate; terminal Store delivery survives reconnect without native replay |
| Queue/routes/provider/capacity | deterministic reasons; blockers do not hide later work; fallback explicit; root limit race-free; old ledgers absent |
| Workspace/process | overlap blocks, disjoint runs; PID reuse kills nothing; uninspectable stays unknown |
| Scheduling | one due loop; lost wake recomputes; same occurrence once; scheduler service absent |
| TaskPacket/review | bounded actionable context; shared initial/correction/resume renderer; all findings together; B gets B review; no-progress stops |
| Public API/migrations | domain authorizer per retained method; internal/adapter protocols separate; removed methods absent everywhere; numeric migration atomic |
| IPC/MCP | slow call does not block unrelated call; one wake pump; exact pending-input read; live stream avoids writer |
| External effects | one Check executor, one Script DTO/protocol, one Forge remote owner; no per-Operation executable hashes; unknown effects never replay |
| Claude | exact Store result page retries after lost ACK; native input never repeats; callback cannot leak to replacement boot; result survives adapter restart |
| Remaining adapters/observation | two consumers prove shared shell; one executor per harness; unbound sessions/gaps visible; exact input/result attribution |
| Deletion | no config/package/CI/docs/fallback wrapper remains; net owned Rust+JS+glue reported separately from tests |

Documentation changes run content/link checks. Runtime slices start with formatting and
minimal affected warnings-denied Clippy/build. Focused connected cases run when the
slice closes; broad qualification follows the complete product path.
