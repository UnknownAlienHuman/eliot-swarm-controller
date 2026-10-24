# Eliot Swarm Controller — implementation

This is the single implementation program for the
[architecture](architecture.md). The 2026-10-21 preservation pass compared
`c92120ed70ca7e625440a8a19517c481e73b13c9` with the pre-refactor specification,
October 6 audit, master audit v26 and retained Claude-script review. This is a
focused requirements/contract review, not an exhaustive reread of every source file.
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

The historical implementation machinery is not itself a requirement. Manager/Governor
is an agent role, not a mandatory human. An enabled policy must run the whole selected
work cycle without per-step human confirmation. `explicit` means an attributable
Store command; the authorized Governor or helper can issue it.

Preserve `Attempt.start_owner` and single initial dispatch: native delegation claims
`native_manager` before spawn, then binds the observed producer; controller launch
claims `controller`. `task.dispatch` must not start a native-managed Attempt again.
This guard exists in `store/operations.rs` and `migrations/001_core.sql`; removing
receipt duplication must not remove it. Follow-up input is a separate `Send`.

[Delivery](delivery.md) defines the autonomous handoffs and historical requirement
checks. It does not add a second implementation queue.

## 3. Current owners

| Responsibility | Owner |
| --- | --- |
| Tasks, Attempts, Operations, authority, acceptance, durable messages/observations | `crates/swarm-kernel-host/src/store` |
| Semantic adapter commands and bounded Store records | `swarm-contracts` |
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

- `native.opencode.loop_step` → remove the public method; map retained callers only
  to supported `Send` timing, not automatically to exact-turn steering;
- `native.command.cancel_turn` → `Cancel(turn)`;
- `native.command.close_session` → `Shutdown`;
- `native.command.release_evidence` → adapter-internal cleanup;
- `native.mcp.*` → OpenCode adapter-internal setup/observation.

Native inbox/loop-boundary steering is not atomic exact-turn steering. Current
`runtime/opencode_v2/effects.rs` explicitly rejects exact-turn steering without a
native expected-turn guard. Removing a vendor method must preserve the requested
timing or report the unsupported scope, not rename a weaker action as exact.

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

**A1a — landed, scoped check passed:** terminal receipt no longer requires Store
`observation_id` or duplicate `local_execution_results`. Commit `c92120e` updates one
existing dispatch producer across terminal SUCCESS/ERROR/CANCELED. Repair run
[39579583956](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/39579583956)
passed the connected terminal regression and Store Clippy before publishing that
commit. This is not installed-harness or crash-recovery qualification.

**A1b — landed, scoped production Clippy passed:** commit
[`7408e77`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/7408e77e958aa4e8c7244c7c058eec0935bfa72c)
deletes `AntigravityDispatchSessionProof`, latest-refresh SQL, the adapter proof
producer, copied assignment/lease/session proof digests and mandatory
refresh-before-dispatch qualification. Dispatch capability is recomputed from current
Store Task/Attempt/binding scope plus binding generation, connected/recovery state,
opening/current boot, root/scope, native stream health, model readback and
`turn_pending=false`. Semantic Refresh remains available for explicit readback.

Run
[`40126583064`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/40126583064)
passed retired-symbol scanning, `cargo fmt --all` and warnings-denied production Clippy
for `swarm-contracts`, `swarm-antigravity-adapter` and `swarm-kernel-host`. Net change:
−271 production Rust, −37 qualification PowerShell and +6 Rust test lines. No test suite,
installed harness, disconnect/crash or delivery qualification is claimed.

**A2:** retain complete terminal bytes and final outcome/result parameters with
Operation, original binding/generation and origin boot in private atomic files before
acknowledging local retention. Same-boot IPC reconnect resends unchanged parameters;
delete only after Store ACK. Observations use one replaceable checkpoint per source.

A process restart is not an IPC reconnect. After existing custody checks fence the
previous delivery owner, the authorized successor uses the existing reconciliation
path to import retained deliveries for the original Operations. Store checks original
assignment/source identity and deduplicates receipt/page identity. A superseded assignment receives historical
evidence, not a new completion. Preserve origin boot; never relabel old facts as current activity or a new Attempt. This path starts
no native process and resends no prompt. Implement this caller-to-Store path in A2,
not a manual export instruction or a second recovery service. Missing/partial files
require native readback or visible unknown; a crash before durable capture remains an
explicit limit. Never put native intents into this delivery spool.

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

Keep a live claim's budget scope stable across model, artifact, option or limit edits
when the underlying provider/account/service capacity identity is unchanged. Current
`capacity::compare_root_route_identity` splits model/artifact revisions and
`launch_route_match` drops pending launches when the whole route digest changes;
[AR-049](audit-register.md#ar-049) records the exact limitation. Use the admitted
launch/binding and real service scope, not a full config hash, to count work. A change
to actual resource identity keeps a separate scope. An alias edit must retain unsettled
claims or be refused until their disposition; do not infer shared budget from matching
display text.

**B4:** `dispatch_once` queries a bounded ranked page, chooses the first eligible
explicit route and atomically creates/reuses Attempt, reserves workspace/root and
creates launch Operation. Delete WorkDispatch cursors, copied pending subjects,
watermarks, slots, links and ordinary-blocker quarantine. Query eligible work before
applying the admission limit; use bounded keyset scanning where eligibility needs
inspection, so blocked first-page items cannot starve later work.

Preserve the existing two-stage transaction boundary: `launcher::launch_for_actor`
records the pending launch reservation; after workspace I/O, `launch_after_workspace_held`
rechecks current facts and commits claim/Open/binding association together. Direct and
WorkDispatch already converge there ([AR-050](audit-register.md#ar-050)). Simplifying
the manifest must retain both reservations and must not move Git/native I/O inside
SQLite or introduce an unreserved gap between stages.

Reuse a compatible idle binding under the route's continuation policy rather than
unconditionally opening a session per Task. Before reuse, inspect pending inputs,
native Goal/children and unresolved effects. Capacity is not released merely because
a parent reports idle. A configured rotation drains only its affected scope and
starts a new Attempt/session only after actual disposition; source age is not a kill
condition.

### C — workspace authority

`TaskSpec.scope.initial_paths` supplies normalized repository-relative path prefixes;
`forbidden_paths` is hard negative. Empty allowed scope means repository root. Reject
absolute paths, traversal, empty segments and `.git`; compare path segments.

Attempt workspace lease is the controller's write admission/overlap guard, not an OS
sandbox for an unrestricted native shell. One manager owns the mutable worktree and
serializes index/branch/commit changes; leaf writers get non-overlapping edits, not
concurrent Git-index ownership. Review reads an immutable candidate or isolated
read-only snapshot, never a concurrently changing writer checkout. It needs no write
lease. Delete `code.scope.*`, symbol/interface authority, proposals, digests, override,
expiry, indexes and contract links; preserve current Store role/object grants.

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

**Connect retirement before calling lifecycle complete.** Existing production writers
close only rootless pre-native openings; there is no finalizer for an attached binding
([AR-048](audit-register.md#ar-048)). In the existing runtime/Store disposition owner,
fence new input, join exact native/process closure and required output/readback facts,
then retire that binding generation once its obligations are resolved. Recompute after
late outcomes and restart. The existing `workspace_lifecycle` sweep can then release an
eligible lease. Preserve stored result bytes and existing reader authorization; verify
post-release artifact/result reads separately, since the source trace did not audit the
complete grant matrix. Do not hold a live root merely because historical bytes exist.
Attempt release, service-helper departure and socket close alone cannot prove that a
native session or its children stopped.

<a id="automation-and-delivery"></a>

<a id="harness-goals-and-the-eliot-goal-plugin"></a>

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

They do not own another workflow lifecycle. Use the same admitted command for a
Governor call or enabled helper; record represented manager, action and subject in
that Operation and recheck current grants at admission. Human setup of budgets or
permissions is not a per-action approval gate.

On wake, query current subjects and admit at most the configured work budget. Persist
the action's domain identity with its Operation, not another copied subject/cursor.
Review assignment, correction delivery and publication keys include the current
candidate/review or accepted-candidate/target; scheduled messages use occurrence ID.
A new request ID cannot bypass these domain checks. Lost wakes trigger a bounded
rescan; capacity/429/reset changes wake the same loop. Keep unrelated work running.

The [Goal/schedule source trace](audit-register.md#trace-2026-10-23-goal-policy)
names the current connected failures and preserved boundaries. In
`automation_goal_progression::{prepare,require_current_for_dispatch,reconcile_entry}`,
`goals::progression_target` and `runtime::{user_command_with_actor,next_internal}`,
replace vendor-specific Goal continuation with the ordinary attributed Send path.
Do not register the technical requester as a client to repair OpenCode's actor error.
Admission must reach command selection, native outcome and the same original
source-Operation result capture; a queued-Operation fixture is insufficient.

Implement [Goal policy](configuration.md#goal-message-and-native-goal) in that path:
phase-gate generic messages during submitted/review/correction work, reserve finite
occurrence/no-progress budgets, and coalesce one Governor decision on exhaustion.
Preserve capture/check/review/accept and the one correction Send. An entry disable,
transfer or settings edit alone does not invalidate admitted input; actual revoked
rights or a stale subject still fences an unsent effect. Retry unclaimed ownership
blockers from current facts after transfer without rewinding history.

Keep exact source-event deduplication and atomic occurrence/Operation admission.
Preserve `scheduler::calendar::{generation_digest,occurrence_id,latest_due}` semantics
through the shared loop: action-only edits, re-enable, transfer and restart do not
replay a consumed tick; catch-up remains latest-only. Delete old Goal cursors, global
slot/proof copies, native-goal predecessor coupling, transfer relocators and duplicate
loop only when their retained identity/behavior is carried by the ordinary owners.
The current calendar path is CheckRun-only; it is not general Goal scheduling.

For a native question/approval, first apply configured vendor permissions/rules, then
route the unresolved addressed request to the Governor agent. Known policy may answer
automatically; a choice requiring model judgment is one Governor task. Do not deny
everything, pick the first option or require a human by default. Coalesce unchanged
attention. `Send` is not `Reply`; recheck the exact pending request before answering.
Operator input is reserved for missing authority/credentials or policy decisions the
Governor cannot resolve. Use existing attention and mailbox records, not an incident
platform or permission DSL.

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
review_once          -> assign missing or replace eligible current review
review.submit        -> retain verdict + all findings
task.request_changes -> exact candidate/review; one Attempt transition;
                        optional child Send with all findings
accept_once          -> common candidate-specific task.accept gate
```

Use `store/reviews.rs`, `store/submissions.rs` and `store/acceptance.rs`. The
[connected source trace](audit-register.md#trace-2026-10-23-review-acceptance)
separates the useful existing path from missing automation and review checks.

**Review:** preserve `ReviewReplacementClass::{Unanswered,Inconclusive,ReturnedForCorrection}`
and immutable result replay/conflict handling. Direct Store replacement already works;
current `automation_dispatch::attempt_review_assignment` only assigns from a submission
and its on-behalf actor forbids replacement. Connect `review_once` to the same ordinary
replacement handler under exact represented rights. Query current missing/inconclusive
slots and unanswered assignments past a configured response deadline; compare-and-swap
the expected assignment and choose a distinct eligible reviewer. Recheck any result
that arrived concurrently. Bound replacements and coalesce one Governor decision if
policy cannot continue. Preserve narrow non-revoked historical result access; late
output never fills the replacement's slot. No new reviewer service or copied backlog.

**Correction:** current disposition already packages all committed actionable findings;
keep that behavior. Move its positive path into one `task.request_changes` transaction:
record exact candidate/review feedback, set the same Attempt to `needs_correction`, and
admit at most one child Send if the owner binding is eligible. Native I/O follows commit.
Otherwise current-state resume renders the retained package. Mailbox feedback is not
native delivery. Keep the admitted child's request/actor across policy disable/edit;
actual rights, current candidate, phase and binding still fence unsent input. Preserve
same-Attempt resubmission and historical retention of a stale publication (AR-011/052).

Reserve a finite correction-round allowance when admitting a new correction, and assess
no-progress once the round's required output/evidence/disposition is available. Waiting,
readback retry and replay of the same receipt do not reserve another round. Compare
outstanding findings and relevant source/check evidence: unchanged bytes can gain real
verification; fresh IDs or unrelated edits cannot renew a budget. Retain consumption
across resubmission, policy edits, transfer and restart. On exhaustion admit one
Governor-agent decision with concrete revise/replace/renew/stop choices. Unknown native
effects keep exact readback and never become permission for another Send.

**Acceptance:** preserve `acceptance::{reserve_with_actor,evidence,begin,finish}` and
their on-behalf variants: frozen V2 plan, exact candidate/check/dependency receipts,
off-DB artifact verification and revalidation before Task transition. These owners
already share the substantive evidence gate. The automatic caller copies the assigned
auditor's retained requirement evidence; it does not invent it. The remaining gap is
review provenance: direct acceptance accepts caller-authored review arrays, and the
common feedback cursor is only a freshness fence. Move required assigned-review and
unresolved-finding validation into that common gate. Resolve immutable review facts
there for direct and automatic decisions; the accepting agent supplies decision/reason,
not replacement auditor evidence. Reject unsupported reviewer counts before work.

Late exact CheckRuns can unlock the same candidate without rewriting a review. A passed
review is immutable; if a new review is needed, submit the same artifact under the same
Attempt with the expected prior submission, producing a new slot. That identity change
alone is not progress. A revoked frozen dependency receipt requires an addressed
replan/new-Attempt decision; a later prerequisite receipt cannot silently replace it.
Classify that subject locally and keep unrelated review and queued acceptance work
running. An already-admitted failed/unknown acceptance keeps its receipt/readback;
changed evidence requires a fresh attributable decision, not replay of the old effect.

Delete ReviewDisposition/RepairDispatch workflow state, copied pending lists, duplicate
findings and redundant correction/evidence copies only as these paths connect. Preserve
the ordinary review slot and Operation identity. The current 64-pending-result cap can
hide later ready work for that entry; replace event-backlog eligibility with bounded
fair current-state queries, keeping unresolved effects in their existing owners.

Required requirement-linked checks must pass. A known unrelated baseline failure is
reported separately under selected policy, not turned into a demand that a worker fix
the whole repository. Differential checks alone do not prove a new requirement.
Run minimal affected Clippy during construction; broad tests remain product-stage.

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
host DTO/compatibility duplication. Current `schema.rs::ScriptControllerEffect`
contains only TaskOwnerMessage, ManagerNotification and TaskCreate: that does not
replace legacy reminder/form/route controls. Use existing helpers for common actions;
where a retained script is the caller, add only its needed typed Send/Reply/Goal or
Configure action to that enum and the existing Store admission path. No arbitrary
method passthrough or second permissions registry. A bounded returned action list
uses `(ScriptRun, action index)` identity and per-item current manager/object grants;
readback returns child Operations without re-running already admitted script effects.

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
Node generations after finite disposition. A lost live callback is not itself durable
recovery. Current official SDK docs also expose hook `defer`/resume; evaluate that
native seam only for the selected SDK version before writing callback persistence.
Project `modules/claude-rust/README.md` still names SDK 0.3.287; current docs are not
proof that this pin supports newer semantics. Advertise the verified capability and
use ordinary Governor Reply; never simulate an unavailable native feature.

### N — Codex

Retain Rust app-server adapter and external app-server. Use one connection owner,
SessionRecord, per-effect records, exact StoreDelivery and latest observation. Use
thread, `clientUserMessageId`, turn and `expectedTurnId`; map native Goal directly.
The current Rust adapter advertises `agent.goal` unsupported. Use the pinned
[native Goal source](donor-map.md#native-goal-and-eliot-goal) to implement
`thread/goal/set|get|clear`, mapping pause/resume through status only when the selected
installed server's feature/provider capabilities support it. Keep native continuation
inside Codex; Eliot messages remain independently bounded ordinary Send. A native
Goal ACK/status does not complete an Eliot Task.
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

Keep native Goal's five verbs and distinguish admission ACK from execution; an
optional native turn ID is not a delivery promise. The newer pinned upstream Goal
contract in the donor map does not qualify the locked SDK 1.3.0 or installed runtime.
Select/qualify the actual capability before relying on new wake/continuation behavior.

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

The built-in `eliot.goal` instruction/prompt wrapper is controller behavior, not a
native Goal API. Eliot Goal messages use ordinary Send; preserve admitted old wrapper
effects/readback through the finite cutover without recreating a fake native Goal.

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
perform no provider/model/filesystem/network call. Source/subject/edge names describe
facts, not a requirement for a generic entity graph, one table per word or a new
registry framework. Reuse existing IDs and the smallest indexed Store representation
needed by actual inventory/state consumers.

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
automation conditions. Delete the duplicate coordination participant-token registry,
not base client authentication or Task/reviewer grants. Native children need no extra
Store principal merely to be observed. Children that call Eliot use the existing
scoped identity/grant path. Then remove cards, speech-act/proposal workflow,
contracts/ratification, integration ACK, Concilium, code scope and their custom
indexes/backfills/cursors and redundant policy/MCP/CLI/fixtures/storage.

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

**U4 — remove the projection engine, preserve automation.** Delete activation/catch-up
watermarks, per-entry copied lifecycle, quarantine and effect-drain cursor. An enabled
manager integration policy queries accepted candidates/importable source changes and
admits the same explicit publication/metadata/import Operation as a direct agent call.
Acceptance and publication remain separate facts. Idempotency uses the accepted
candidate + remote target + requested effect, so disabling/re-enabling does not
repeat a write. Deleting the engine must not require a human to push, import or apply
routine metadata. Use the existing loop, never another publication scheduler.

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
- stale live reports fail; same-boot delivery and authorized cross-boot reconciliation
  preserve original Operation/source identity without native replay;
- native and Eliot Goals work independently and together;
- native-manager delegation cannot receive a second initial controller dispatch;
- an enabled Governor policy completes submit/review/correct/accept/optional publish
  without an operator; exact approval questions reach the authorized resolver;
- queue/admission cannot duplicate Attempt/launch or starve eligible work;
- overlapping write leases block; disjoint scopes proceed;
- review B cannot reuse review A; an eligible unanswered assignment can be replaced
  after its configured deadline; all findings return together; evidence-only progress
  is not misclassified;
- direct and automatic acceptance resolve the same required assigned reviews and
  outstanding findings; a fresh feedback cursor cannot bypass a requested correction;
- a revoked dependency pin blocks only its subject; unrelated reviews and already
  queued acceptance verifications continue; late exact checks unblock their candidate;
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
