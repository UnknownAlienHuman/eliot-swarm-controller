# Eliot Swarm Controller — implementation

This is the single implementation program for the
[architecture](architecture.md). Runtime source, historical owner requirements and
Claude Swarm scripts were rechecked through
`6e470726c633209f98fea2bfd7bd12d039f285f1` on 2026-10-21.
[Findings](refactoring.md) explain the current excess;
[status](../implementation-status.md) separates landed code from plans.

Do not start another horizontal framework migration. Complete one vertical
caller-to-result path, delete its replaced path, then use a second current caller to
prove any shared implementation.

<a id="implementation-order"></a>
<a id="adapter-bus-first-migration"></a>
<a id="identity-and-hashes"></a>

## 1. Rules for coding agents

Every slice names:

- observable behavior;
- exact current callers, owners and symbols;
- unsupported and uncertain cases;
- old code, policy, schemas, fixtures, config and packages deleted with it;
- one donor/native mechanism when relevant;
- minimal affected formatting and warnings-denied Clippy/build command;
- connected evidence and net production Rust/JS/glue change.

One manager owns shared contracts, Store transitions and integration in one worktree.
Leaf agents receive narrow non-overlapping work. A new type, proof, compatibility
reader, fixture, crate or wrapper is not progress unless a current caller uses it and
the replaced mechanism disappears in the same slice.

Migration is route/domain by route/domain. An unmigrated path may temporarily retain
its current format but gets no new version/profile family. Once switched, no new work
enters the old producer.

## 2. Non-negotiable retained behavior

Deletion is allowed only if these behaviors remain connected:

1. Reconnecting the same durable manager `client_id` never restarts current work.
2. `gm.handover` changes current authority but does not rewrite Attempt owners,
   candidates, authors, reviewers or historical mailbox recipients.
3. Historical directed mail remains addressed to its original recipient; successor GM
   reads current Task/Attempt/native attention through authoritative projections.
4. Unknown native or external writes are never resent because a transport response was
   lost.
5. Harness Goal and Eliot Goal remain independent and may run together.
6. Governor observation remains local/passive and shows unmatched sessions/processes.
7. One manager owns one worktree; overlapping writer scopes cannot launch.
8. Review/acceptance address the exact candidate and cannot reuse stale evidence.
9. External writes have one domain owner and exact readback after ambiguity.
10. One remote gateway process maps to one fixed Eliot identity/profile.

The historical implementation machinery is not itself a requirement.

## 3. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, authority, acceptance, durable messages/observations | `crates/swarm-kernel-host/src/store` |
| Semantic adapter commands and bounded Store records | `crates/swarm-contracts` |
| Authenticated Eliot IPC | `swarm-client::Client` / `ModuleLink` |
| Process groups, birth identity and private atomic files | `swarm-process` |
| Native transport/session/readback | selected adapter only |
| Workspace/worktree/write authority | Store workspace + bounded host Git/filesystem effect |
| Queue, route choice and launch admission | Store launcher |
| Time admission | one in-process host loop; Store owns due state |
| Checks, scripts, remote Git and GitHub API writes | one selected worker/owner per domain |
| Public CLI/MCP | `swarm-cli` / `swarm-mcp`, both calling Store |
| Local source collection | selected adapter plus host process/Git/remote collectors |

No adapter accepts Tasks/candidates. No frontend reduces vendor events. No shared shell
owns provider session state, native replay, Task state, scheduling or transcripts.

## 4. Semantic adapter boundary

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

Each adapter advertises only supported actions/scopes. Unsupported actions fail before
native I/O. Capabilities route; Store authorization grants rights.

Remove vendor/internal public methods:

- `native.opencode.loop_step` → `Send(steer)`;
- `native.command.cancel_turn` → `Cancel(turn)`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation.

A normal outcome carries Operation ID, stale binding/boot guard, native identity,
disposition and bounded action details. Do not repeat package version, protocol,
request/source/prompt hashes or Store-owned Task/Attempt identity.

Keep hashes for credentials/verifiers, selected package bytes and immutable result/
artifact bytes. Readiness, model, activity, quota and stream state are observations,
not permissions.

## 5. Runtime order

```text
A  Antigravity result/delivery
B  queue, routes, provider facts and root admission
C  one workspace write authority
D  minimal process custody/restart
E  one scheduler/current-state automation loop
F  compact TaskPacket
G  ordinary review/correction/acceptance
H  separate public/internal/adapter protocols
I  numeric Store migrations and WAL reads
J  concurrent IPC/MCP and exact hot reads
K  one Check, Script, remote-Git and GitHub API effect owner
L  shared adapter shell only after two current users
M  Claude reduction
N  Codex reduction
O  Command Batch/ACP reduction
P  Muse Node consolidation and custom-Rust retirement
Q  OpenCode standalone consolidation
R  joined local Governor observation
S  optional-product/tooling deletion
T  normalized mailbox; delete coordination platform
U  GitHub source/import/effect separation
V  small single-tenant remote gateway
```

### A — Antigravity

**A0 — landed:** active v6 coordinate in source, route, installer, package policy and
qualification. Historical v5 receipts stay historical.

**A1a — landed, verification open:** terminal receipt no longer requires Store
`observation_id` or duplicate `local_execution_results`. The later temporary repair
run exposed an unresolved rejected/cancelled producer projection: do not mark A1a
complete until the producer contract and connected regression agree.

**A1b:** delete `AntigravityDispatchSessionProof`, refresh producer, latest-refresh
lookup, binding/snapshot proof validator, assignment/lease/session digests and
mandatory refresh-before-dispatch. Store already owns assignment, Attempt,
binding/boot, workspace lease and Operation.

**A2:** persist already-formed `module.outcome`/`module.result` parameters in private
atomic files before memory can forget them. Scope by binding + generation + boot;
same-boot reconnect resends exact parameters and deletes only after Store ACK. Never
persist/replay native prompt or process intent. Observations use one replaceable
checkpoint per source.

**A3:** keep only real Open, Dispatch, Send(next_turn), Refresh, Result, Reconcile and
explicit release of an owned resource. Delete placeholder Reply/Configure/Goal/
Background/Recover/steer, retained active Node profiles and repeated receipt identity
after concrete old work is disposed.

<a id="work-dispatch"></a>

### B — queue, routes, provider state and admission

**B0:** `swarm.queue.get` reads current Task/Attempt, correction, dependencies,
workspace lease, provider/root capacity and binding state. Rank current correction,
unfinished Attempt, ready unclaimed Task, explicit phase, age + Task ID. Page reserves
nothing; launch rereads current facts.

**B1:** lane stays a view. Manager policy stores enabled selector, ordered route aliases
and optional agent profile. Each route owns exact harness/model/effort/root limit. No
lane table, daemon, cursor, inbox or scheduler.

**B2:** normalize provider usage/availability by stable source, epoch, sequence,
observed time, freshness, coverage and real account/service/connection scope. Adapters
fold native methods. Remove method/artifact allowlists, auth-context proof hashes and
per-binding copies of shared account facts.

**B3:** count/reserve only unresolved root-opening Operations and unreleased root
bindings in the launch transaction. Delete capacity phase/evidence ledgers, refusal
rows, codecs and lifecycle synchronization. `report.capacity` becomes a projection.

**B4:** `dispatch_once` queries a bounded ranked page, chooses the first eligible
explicit route and atomically creates/reuses Attempt, reserves workspace/root and
creates launch Operation. Delete WorkDispatch cursors, copied pending subjects,
watermarks, slots, links and ordinary-blocker quarantine.

### C — workspace authority

`TaskSpec.scope.initial_paths` supplies normalized repository-relative path prefixes;
`forbidden_paths` is hard negative. Empty allowed scope means repository root. Reject
absolute paths, traversal, empty segments and `.git`; compare path segments.

Attempt workspace lease is the only write authority/overlap guard. Review needs no
write lease. Delete `code.scope.*`, symbol/interface authority, proposals, digests,
override, expiry, indexes and contract links.

### D — process custody

Persist binding/generation/boot, private state/credential ref, selected executable,
PID + OS birth identity, Job/process-group identity and shared-service relation.
Package/protocol matrices stay installation facts.

Restart:

- dead → clear live record;
- PID/image/birth mismatch → clear, kill nothing;
- uninspectable → unknown;
- exact Eliot-owned leftover → route policy recover/observe/terminate;
- shared service → never kill because one session disappeared.

<a id="automation-and-delivery"></a>

### E — scheduler and automation

Keep one automation-entry schedule and one in-process loop waiting for Store change or
nearest due time plus slow safety wake. Store atomically claims an occurrence and
admits an ordinary Operation.

Delete standalone scheduler service/credential/protocol, overlapping loops and workers
that do only Store bookkeeping. Keep workers for real external I/O/custody.

Automation toggles are current-state attempts:

```text
dispatch | review | return_changes | accept | check | goal_message | script
```

They do not own another workflow lifecycle.

### F — compact TaskPacket

Replace identity JSON, canonical brief and launch packet in the model context with:

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

Store retains Task/Attempt/Operation/binding identity. Adapter receives Operation ref
plus rendered body. One renderer handles initial, correction and resume. Delete
source/prompt hash echo chains.

### G — review/correction/acceptance

```text
review_once          -> assign missing review for current candidate
review.submit        -> retain verdict + all findings
task.request_changes -> exact candidate/review; one Attempt transition;
                        optional child Send with all findings
accept_once          -> common candidate-specific task.accept gate
```

Delete ReviewDisposition/RepairDispatch workflow state, per-finding delivery, slots,
duplicate findings and automation-authored evidence arrays. Candidate B needs review
B. Same candidate after correction is `no_progress`; bounded rounds escalate.

### H — separate protocols and authorizers

Classify every method as retained public Core/integration, internal host/worker,
adapter semantic or removal target. No aliases during cut.

A retained domain owns typed parse, exact role/object/revision authorization and
handler. Shared registry exposes names only. Public MCP never contains `module.*`,
`native.*` or scheduler-service RPC. Removing a product removes policy, schemas,
CLI/MCP, handlers, fixtures and storage together.

### I — Store evolution

Use SQLite `application_id`, application `user_version`, ordered numeric migrations and
`BEGIN IMMEDIATE`. Each migration has explicit pre/postconditions and bounded integrity
checks. Remove base-SQL SHA, extension digests, reference DB and DDL-token validation.

Migrate concrete retained data only. Move pure bounded reads to WAL readers. Never hold
Store transactions across native/file/Git/network I/O.

### J — IPC, MCP and hot views

- Clone current `Arc<Client>` and release frontend connection mutex before RPC.
- One wake pump per MCP session coalesces Store cursor changes; Store history is replay
  authority.
- `tasks/get` reads exact Operation/binding/pending input; no global attention scan.
- Store authorizes native stream open/close; bounded ring I/O runs outside writer.

### K — selected effect owners

**Checks:** standalone `swarm-checks` for new CheckRuns; finite historical readback,
then delete hidden host check worker.

**Scripts:** `swarm-script-worker` executes; `swarm-scripts` owns one wire. Delete
host DTO/compatibility duplication.

**Remote Git:** Forge owns remote push/publication with exact target serialization and
readback. Local Git stays bounded in host. Reuse `swarm-process` custody; verify worker
bytes at install/launch, not per Operation.

**GitHub API:** one small owner performs explicit retained writes with durable sending,
one write attempt and exact readback. Remove PR PATCH execution from Forge worker and
unmanaged direct child ownership.

### L — shared adapter shell

Extract authenticated Eliot IPC, hello, typed command pull and redelivery of already
formed Store records only after two current adapters use identical behavior and delete
their old loops together. Extend `swarm-process`/`swarm-client`; no adapter-framework
crate. Native session state, recovery, results, timers and Task state stay adapter-side.

### M — Claude

Retain one Rust adapter with thin official SDK driver. Split live session,
NativeEffectRecord, exact StoreDelivery, latest Observation and persisted terminal
result. Delete generic journal, callback retention, tombstones/reference graph and old
Node generations after finite disposition. Lost callback resolver remains unknown.

### N — Codex

Retain Rust app-server adapter and external app-server. Use one connection owner,
SessionRecord, per-effect records, exact StoreDelivery and latest observation. Use
thread, `clientUserMessageId`, turn and `expectedTurnId`; map native Goal directly.
Delete full checkpoint, raw event mirror, tombstones, Goal proofs, Python executor and
profile ladders.

### O — Command Batch/ACP

Keep one current Batch and one stable ACP v1. Eliot `AcpV2` is an artifact generation,
not protocol v2. Preserve `session/load` no-fresh-fallback and option readback. Replace
journals with SessionRecord, NativeEffectRecord and StoreDelivery. Remove
`native.command.*`, release-evidence and profile/schema ladders.

### P — Muse

Keep Node bridge `.13` during refactor. Split checkpoint into SessionRecord,
NativeEffectRecord, exact StoreDelivery and latest Observation; preserve explicit
Operation-derived command IDs, targeted readback, native Goal/result/usage and
module-owner custody.

Disposition custom-Rust work, then delete `crates/swarm-adapter-muse`, Cargo member,
disabled descriptor, custom MSP wire/schema and Rust-only fixtures. Official Rust SDK
is future only when its command-ID seam allows net deletion.

### Q — OpenCode

Select/live-qualify one exact installed native contract with durable inbox,
process-global Session owner, execution claim/restart terminal and assistant
`parentID`. Converge on standalone Rust only.

Bind result by exact assistant ID + parent input ID + selected parts. Map exact steer/
reply/cancel/background/restart readback; unsupported controls fail before I/O.
Internalize MCP setup/readiness. Switch route/package, freeze built-in new work,
disposition old work, then delete host `runtime/opencode_v2`, legacy demand/worker
slot, native MCP/owned-service paths and built-in fixtures.

Shrink persistence to SessionRecord, NativeEffectRecord, StoreDelivery and
ObservationCheckpoint. No universal journal/reference graph/tombstones.

<a id="passive-agent-observation"></a>

### R — local Governor observation

Implement [observability](observability.md) R0–R6:

1. stable sources with collector epoch/cursor/freshness/coverage;
2. provider-local readers independent of Task/binding;
3. one OS process collector with PID/birth/image/group/shared-service facts;
4. worktree Git collector and independent remote-ref cache;
5. current subjects plus assignment edges;
6. one WAL projection and cursor invalidation/subscription model;
7. delete binding-only catalogs, receipt metrics CLI, static lease-as-live view and
   parallel report reducers after consumers move.

Unassigned subjects remain visible and grant no Task/transcript rights. Ordinary reads
perform no provider/model/filesystem/network call.

### S — non-core/tooling deletion

Delete Concilium, generic contract/integration ratification, automatic GitHub
projection, document-edition rights, legacy acceptance branches, retired installers/
profiles and duplicate worker dependencies.

One current package has one manifest/executable/descriptor/resources. Verify bytes at
install/launch. Runtime hello checks binding, protocol major and semantic capability
subset. Delete checkout/compiler/cache equality, per-artifact validators and proof
qualification.

### T — one mailbox, not a coordination platform

Current owners:

- `store/mailbox.rs` — legacy/typed delivery, reply/cancel lookup and random delivery ID;
- `store/object_reads.rs::timeline` — `message.read` over global observations;
- `store/gm.rs` — handover/resync semantics;
- `coordination/{mod,thread,watch,contract,integration,concilium,code_scope}.rs` and
  matching Store/API surfaces — the removable collaboration platform.

**T0 — normalized mailbox table.** Add bounded recipient-indexed rows keyed by the
admitted Operation ID as the durable message ID. Store sender, recipient, optional
Task/Attempt/thread, kind, bounded body or artifact ref, reply target, cancellation
reference and monotonic recipient cursor.

**T1 — direct send/read/reply/cancel.** `message.send` inserts one row atomically with
its Operation. `message.read` pages recipient rows directly; reading never deletes.
Reply/cancel references message Operation ID. Remove `cmsg-*`, random delivery IDs,
legacy/typed unions, JSON expression indexes and corruption-sentinel scans after
concrete unresolved messages move.

**T2 — preserve continuity.** Same durable `client_id` resumes its own cursor. GM
handover changes designation/epoch only, does not reassign historical mail and exposes
resync watermarks as hints, not ACKs. Current Task/Attempt/native attention remains a
separate projection for the successor.

**T3 — optional tiny thread.** Only if a current consumer requires grouping, keep
thread ID, Task/Attempt scope, creator, bounded participant IDs and open/closed state.
Messages remain mailbox rows; thread never grants authority or negotiates policy.

**T4 — delete coordination products.** Consultation becomes message. Review stays
review. Write conflicts stay workspace leases. Watches become subscriptions/explicit
automation conditions. Delete participant token registry, cards, speech-act/proposal
workflow, contracts/ratification, integration ACK, Concilium, code scope and custom
indexes/backfills/cursors, with policy/MCP/CLI/fixtures/storage.

**T5 — finite transition.** Migrate only unresolved messages required by concrete
work. Completed Operations remain readable history; do not keep permanent translators.

### U — GitHub sources, import and effects

Current owners:

- `github/client.rs`, `github/observer.rs`, `github/pulls.rs` — bounded remote reads;
- `store/github.rs` — source poll Operation/lease, source facts, Task mutation and work pool;
- `store/github_effects.rs` — direct managed-label effect;
- `store/github_pr_effects.rs` + Forge worker — PR description effect;
- `store/automation_github_projection.rs` — automatic label workflow.

**U0 — source collector, not poll workflow.** Replace `github.source.poll` Operation,
lease/cancellation/recovery machine and persisted mutable page number. One configured
collector starts at page one, advances to stable high-water/cursor, records source
epoch/freshness/partial coverage and periodically reconciles. GET failure leaves a
gap, not an empty source.

**U1 — source facts first.** Commit Issue/PR facts in short batches without creating or
revising Tasks in that transaction. Preserve immutable repository ID, bounded fields,
source revision and no credential storage in Store.

**U2 — explicit import.** Manager/policy selects source facts and constructs compact
Task specs in a separate mutation. Preserve origin identity and conflict detection.
Delete GitHub work-pool selected/order rows; imported Tasks enter ordinary queue/lane.

**U3 — one GitHub API effect owner.** Retain explicit label/PR metadata writes only
when required. Operation moves to sending before I/O, writes at most once, then exact
readback settles or remains unknown. Move PR PATCH out of Forge worker. Forge remains
remote Git push only.

**U4 — remove automatic projection.** Delete activation/catch-up watermarks,
global/per-entry cursors, quarantine/recent state and effect-drain cursor. Acceptance
does not implicitly mutate GitHub. Optional integration action is explicit.

**U5 — independent remote view.** Feed repository/ref/head/time/error into R3 remote
collector independent of publication. Local tracking refs are not fresh remote facts.

### V — small single-tenant gateway

Current owners: `crates/swarm-gateway/src/{main,gateway,config}.rs`, the existing MCP
facade and the temporary kernel-host compatibility launcher.

**V0 — private secret loading.** Load Eliot credential and bearer through the shared
bounded private-file primitive: absolute canonical path, regular file, no symlink/
reparse, private owner/ACL/mode and strict byte/token bounds. Delete plain `std::fs`
credential/bearer reads.

**V1 — one tenant.** One gateway process/listener maps to one Eliot `client_id`, one
fixed restricted profile and one bearer. HTTP/MCP sessions are transport sessions, not
new principals or request namespaces. Bound active sessions and idle lifetime.

**V2 — retain narrow network boundary.** Keep loopback-only bind, exact `/mcp`, no
query, duplicate Authorization rejection, constant-time bearer comparison, Host/Origin
validation, body/header/request/connection bounds and timeout-as-unknown semantics.
Stopping gateway never cancels admitted Store work.

**V3 — delete extra launcher.** Remove kernel-host compatibility launcher after callers
use standalone gateway. Leave TLS, tunnel and edge identity outside. Do not add OAuth,
identity broker, reverse-proxy platform or gateway database.

<a id="launch-and-results"></a>

## 6. Launch, results and finite transition

Launch preview is optional. Store resolves one plan under the admitted Operation and
references it from workspace/dispatch. Delete caller-supplied plan hashes.

Keep one result path:

```text
Result -> module.result -> immutable artifact -> artifact.read/assemble -> submission
```

Adapter retains unacknowledged page until Store ACK. File without committed receipt is
not a result.

<a id="current-format-and-existing-data"></a>

Before deleting an executor/format/product, enumerate concrete unsettled Operations and
required results. Finish/read back with current owner, export result or create new Task
revision/Attempt. Then delete producer, reader, config, fixture, package and CI branch
together. No universal converter.

<a id="11-build-install-ci-and-qualification-reduction"></a>

## 7. Build, install and qualification

One current package contains one manifest, executable, descriptor and resources.
Verify bytes once at install/launch. Runtime hello validates binding, protocol major
and semantic action subset.

Delete checkout/compiler/cache equality, per-artifact PowerShell validators, repeated
local hashes and proof-packet qualification. Qualification calls public APIs and checks
observable behavior for the selected current route.

<a id="verification"></a>

## 8. Verification

During construction run formatting and minimal affected warnings-denied Clippy/build.
Focused connected cases run when a vertical slice closes; broad/live qualification
follows the completed product.

Minimum connected proofs:

- exact logical request is idempotent; changed request conflicts;
- uncertain native/external write is never resent by transport guess;
- stale binding/boot outcome fails; exact Store delivery redelivers safely;
- native and Eliot Goals work independently and together;
- queue/admission cannot duplicate Attempt/launch or starve eligible work;
- overlapping write leases block; disjoint scopes proceed;
- review B cannot reuse review A; all findings return together; no-progress stops;
- unmatched sessions/processes remain visible without provider calls;
- assigned and observed Git facts may differ and both display;
- GM reconnect/handover preserves current work and historical identities;
- historical mail remains with original recipient while successor receives current
  Task/native attention;
- one mailbox message ID supports send/read/reply/cancel without global JSON scan;
- GitHub collection cannot silently miss moved pages or mutate Tasks during collection;
- GitHub/Forge unknown writes settle only by exact readback;
- gateway rejects unsafe secret files and remains one fixed identity/profile;
- PID reuse never kills unrelated process;
- removed paths leave no config, policy, schemas, fixtures, package or fallback wrapper.

Report net production Rust, JS and glue separately from tests.
