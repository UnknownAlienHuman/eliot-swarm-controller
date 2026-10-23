# Audit register

Durable source-audit resume point: inspected revision, exact paths, findings, preserved
behavior and untraced boundaries. Not a runtime registry or second implementation
queue. [Implementation](implementation.md) owns changes;
[status](../implementation-status.md) records landed behavior.
[Audit index](audit-index.md) provides direct evidence checkpoints and source/target links.

## Resume here

- Checkpoint: **2026-10-23 / Goal policy through native selection and terminal consumer;
  disable, transfer, candidate phase, finite continuation and calendar identity**.
- Inspected repository head: **`73ba7009bbc5c5515fe4c5f85743066e944106ef`**.
  That checkpoint contains AR-048–050 and direct links to earlier evidence; the
  [index](audit-index.md#evidence-checkpoints) retains every inspected revision.
- Last connected pass: `automation::apply` → Goal activation/reconciliation → ordinary
  mutation/on-behalf link → `runtime::next_internal` → Codex Send / built-in OpenCode
  selection → terminal evidence consumer. Checked current native Goal mappings and
  upstream contracts separately; traced calendar CheckRun occurrence continuity.
  New evidence is [AR-051–058](#trace-2026-10-23-goal-policy).
- Runtime baseline: **`c92120ed70ca7e625440a8a19517c481e73b13c9`**, 2026-10-21.
  The compared interval through the inspected head contains **18 documentation-only
  commits**. This establishes unchanged source, not exhaustive source coverage.
- Overall audit: **incomplete**. There are **58 entries**, including defects, preserved
  mechanisms and partial boundaries; no measured completeness percentage.
- OpenCode automatic continuation is rejected before native dispatch by the actor
  guard. Codex's policy revision/enable check can reject admitted input. Generic Goal
  continuation lacks candidate-phase and finite total/no-progress gates; transfer
  carries an ownership-conflict cursor past an unclaimed event. Preserve terminal
  identity/atomic deduplication and calendar continuity. No runtime repair is claimed.
- **Next exact chain:** `submissions::finish` / current candidate →
  `reviews::{reserve_assign,replacement_class,reserve_submit}` → automatic disposition
  → `task.request_changes` / correction Send → corrected submission → common
  acceptance gate. Check frozen per-requirement evidence, all findings, stale candidate,
  inconclusive/missing reviewer, same-Attempt retry and bounded no-progress. Include
  disabled policy, transfer and restart at each admitted boundary; do not resume the
  old review/repair workflow as the target design.
- Source/target entry points: [index](audit-index.md#source-and-target-map),
  [configuration](configuration.md), [delivery](delivery.md), implementation E/G and
  the named source owners.
- Still unqualified: installed Goal/native turns, two real drivers sharing a session,
  live disable/transfer/restart interleavings, crash/power-loss and complete automatic
  result capture/review/acceptance. Earlier retirement/concurrency gaps remain open.
- Documentation only. No runtime change, Clippy, tests, benchmark or installed-harness
  qualification ran. Exact official Codex/Muse Goal sources were read on this date;
  [donor map](donor-map.md) separates those reads from locked/installed versions.

On resume compare changed source with the verified head, then reopen only affected
callers/consumers. Preserve IDs, evidence and qualifications. A source read or docs
commit is neither executed qualification nor a runtime repair.

## Evidence labels

**Source-confirmed** means a named path supports the consequence, not live reproduction.
**Preserve** identifies useful behavior. **Partial** names an untraced boundary. Earlier
rows remain inherited unless explicitly reread. Open findings close only with connected
runtime evidence. Current dispositions below override older broader claims; the
[index](audit-index.md#evidence-checkpoints) links the detailed evidence directly.

## Findings and coverage

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-001"></a>AR-001 | Antigravity reporting versus execution | **Open.** Report retries block native reducer; failure closes stdin and drained facts can remain memory-only. | Independent delivery and continuous native draining. |
| <a id="ar-002"></a>AR-002 | Complete result retention | **Open.** The 32-response cache can evict the only uncollected body. | Durable source lifetime and admission pressure. |
| <a id="ar-003"></a>AR-003 | Exact result identity | **Preserve.** Operation/conversation/generation and explicit unavailable oversized output. | Keep causality, delete package-proof copies. |
| <a id="ar-004"></a>AR-004 | Adapter restart | **Open.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery, no prompt replay. |
| <a id="ar-005"></a>AR-005 | Admitted page completion | **Preserve.** Sealed normalized origin permits narrow late completion and replay ACK. | First historical admission and partial storage. |
| <a id="ar-006"></a>AR-006 | Continuation output | **Open, cross-route.** Generic origin/adapters remain Dispatch-centric; Muse item addressing does not fix Store admission. | Same source-Operation/page/candidate path for attributable Send. |
| <a id="ar-007"></a>AR-007 | Assembly and byte reads | **Preserve.** Publication/registration, contiguous coverage, digests and object grants. | Crash/power-loss and grant/cache races. |
| <a id="ar-008"></a>AR-008 | Authentication versus completion | **Preserve boundary.** Disabled credentials fail new handshake; authenticated carrier can finish a narrow prior page. | Revocation and successor delivery. |
| <a id="ar-009"></a>AR-009 | Send attribution | **Mixed.** Selected Manager/Repair/Goal paths retain scope; session input can be taskless. | Explicit task-linked versus taskless authority. |
| <a id="ar-010"></a>AR-010 | Repair delivery classification | **Open.** Requires provider-specific ID/condition and treats every Rejected/Cancelled as no effect. | Common admission/terminal facts; executed failure is not non-delivery. |
| <a id="ar-011"></a>AR-011 | Same-Attempt correction | **Preserve.** `needs_correction` can resubmit against revision/prior submission; stale publication stays historical. | Continuation origin through candidate validation. |
| <a id="ar-012"></a>AR-012 | Revision fences input | **Open.** Old Attempt survives revision; direct Send can become taskless or dispatch without revision recheck. | Supersede unsent old work; check exact subject before effect. |
| <a id="ar-013"></a>AR-013 | GM cleanup of old work | **Open.** GM can revise beneath another owner but cannot release that superseded Attempt. | Scoped historical cleanup without rewriting owner. |
| <a id="ar-014"></a>AR-014 | Release preserves capture | **Open.** Release cancels queued Result together with non-check Operations. | Cancellation by effect semantics. |
| <a id="ar-015"></a>AR-015 | Restart credential | **Preserve.** Stable binding credential; no implicit rotation. | Live restart/revocation. |
| <a id="ar-016"></a>AR-016 | Old-boot first delivery | **Open.** Unseen old-boot terminal fails current-boot validation; committed identical outcome may replay-ACK. | Execution origin separate from delivery actor. |
| <a id="ar-017"></a>AR-017 | Recovery-fenced capture | **Open.** Recovery filters Result before completion-only exception. | Exact completion/readback without native readiness. |
| <a id="ar-018"></a>AR-018 | Supervisor recovery | **Scope correction.** Intake/process recovery is not native-byte import. | Import stays with adapter/Store. |
| <a id="ar-019"></a>AR-019 | Inconclusive lookup completion | **Open.** Antigravity labels completed inconclusive Reconcile itself Unknown. | Applied lookup `resolved:false`; target stays unknown. |
| <a id="ar-020"></a>AR-020 | Terminal versus release | **Boundary.** Late terminal may update unreleased old Attempt; terminal does not prove capture. | Preserve output obligation and producer fences. |
| <a id="ar-021"></a>AR-021 | First historical Result | **Open.** New normalized admission requires current open unreleased Task/Attempt. | Source-scoped completion admission. |
| <a id="ar-022"></a>AR-022 | Automatic capture/readback | **Open.** Automatic consumers do not create Result/Reconcile; Doctor recommends only. | Bounded helpers using ordinary commands. |
| <a id="ar-023"></a>AR-023 | Supervisor readback | **Scope correction.** Unresolved IDs/status do not settle targets. | Semantic recovery in Store/adapter. |
| <a id="ar-024"></a>AR-024 | Page ACK/source release | **Open; target corrected.** Boolean ACK insufficient; local ACK-range ledger duplicates Store. | Store-authoritative complete/next-missing coverage. |
| <a id="ar-025"></a>AR-025 | Heterogeneous sources | **Boundary.** Local bytes/files and provider locators have different lifetimes. | Share ready-record delivery, not source journal. |
| <a id="ar-026"></a>AR-026 | Claude ready-page loss | **Open.** Intent persists, ready page does not; ambiguous ACK becomes Unknown and replay is refused. | Persist exact page before IPC; never repeat SDK input. |
| <a id="ar-027"></a>AR-027 | Codex delivery | **Preserve plus gaps.** Exact page survives restart; reads depend on Dispatch/current root. | Keep delivery, remove checkpoint/tombstones; see AR-032/033. |
| <a id="ar-028"></a>AR-028 | OpenCode retention | **Preserve plus gaps.** Exact replay/assistant parent; permanent `result_source` and Dispatch-only target. | Coverage-driven release without retention graph. |
| <a id="ar-029"></a>AR-029 | Command release | **Preserve plus excess.** Saved pages work; source cleanup uses release-proof Operations/tombstones. | Store-complete coverage then local cleanup. |
| <a id="ar-030"></a>AR-030 | Muse source read | **Preserve plus gap.** Exact item/revision/ref and native ranges; ready page checkpoint. | AR-034 bounds discovery. |
| <a id="ar-031"></a>AR-031 | Coverage owner | **Target correction.** Store owns pages; per-page offset is not aggregate completeness. | Exact coverage ACK/replay, no local union. |
| <a id="ar-032"></a>AR-032 | Historical source versus active root | **Boundary.** Root/family guards protect scope; deleting them cannot recover missing bytes. | Source-authorized reader and actual transition cases. |
| <a id="ar-033"></a>AR-033 | Codex read amplification | **Open.** Each new page rereads thread/history/turn/full body, then slices 24 KiB. Saved replay bypasses this. | Native ranges or one selected-result materialization. |
| <a id="ar-034"></a>AR-034 | Muse discovery budget | **Open.** Repeated `readItem` walk detects cycles but has no aggregate page/byte/time budget. | Resumable discovery and locator reuse. |
| <a id="ar-035"></a>AR-035 | Capture/disclosure grants | **Preserve.** Receipt, Bytes and Assemble differ; historical reviewer gets no new bytes. | Scoped first capture, no invented Operator or wider grants. |
| <a id="ar-036"></a>AR-036 | History-dependent capacity | **Open.** Global historical census/ledger verification precedes current root count. | Indexed current claims, no full-history admission scan. |
| <a id="ar-037"></a>AR-037 | Native-default Antigravity | **Open.** Root identity requires optional `modelId`; root policy can block an empty route. | Budget identity independent of optional model. |
| <a id="ar-038"></a>AR-038 | WorkDispatch saturation | **Open.** 128 pending subjects stop intake of later eligible Task facts. | Fair current-state scan, delete copied pending list. |
| <a id="ar-039"></a>AR-039 | Stale provider precedence | **Open.** Stale matching row outranks fresh Available. Eliot link loss retains rows; accepted native account/closed snapshot invalidates its own source. | Same-source supersession, independent quota constraints and remaining lifecycle cases. |
| <a id="ar-040"></a>AR-040 | Root dedup/admission/scan | **Preserve.** Parent/Open/Dispatch/service share a claim; transaction handoff and stable scan exist. | Route edits and common transaction traced in AR-049/050; live concurrency remains unqualified. |
| <a id="ar-041"></a>AR-041 | Reads/control under quota hold | **Preserve plus earlier gates.** Reads/strict Reconcile and stop/reply controls bypass starts-only quota. | Recovery/ready/scope barriers; AR-017 and AR-045. |
| <a id="ar-042"></a>AR-042 | Cancel held unbound launch | **Open.** Cancelled parent fails workspace sweep's Settled no-effect contract; verified-held unbound lease can stay fenced. | Stage-aware cancellation and in-flight workspace readback. |
| <a id="ar-043"></a>AR-043 | Pre-send Open rejection | **Open.** Guard rejects unsent Open without retiring rootless opening binding; cancel refuses non-Queued target. | Known-no-effect disposition through existing resource owners. |
| <a id="ar-044"></a>AR-044 | Lowered root limit/reuse | **Open policy mismatch.** `other_claims >= limit` can hold zero-root-growth continuation after limit reduction. | Separate root delta from input/quota/drain policy; live reload unqualified. |
| <a id="ar-045"></a>AR-045 | Shutdown when execution is not Ready | **Source-confirmed open barrier.** Admission and pre-effect validator require Ready; recovery queue independently excludes Shutdown. | Authorize exact resource cleanup by custody/scope, not new-work readiness. |
| <a id="ar-046"></a>AR-046 | Explicit detach versus adapter demand | **Source-confirmed open integration gap.** Applied transport shutdown leaves native identity retained; demand still anchors attachment to settled Open and holds its lease. | Separate desired attachment from native occupancy and output obligations; no respawn-loop claim. |
| <a id="ar-047"></a>AR-047 | Departure and binding/workspace disposal | **Preserve plus open handoff.** Historical service departure reader survives stale Task/owner; its writer changes service row only. AR-048 completes the ordinary finalizer search. | Connect retirement after all relevant facts; neither auto-release on socket close nor delete guards. |
| <a id="ar-048"></a>AR-048 | Attached binding never reaches ordinary retirement | **Source-confirmed open.** Only rootless pre-native Open cancellation writes binding release. Attempt/service completion and periodic sweep do not retire an attached binding. | Existing Store disposition must retire the exact resolved binding, then let the sweep release an eligible lease; preserve results and unresolved native work. |
| <a id="ar-049"></a>AR-049 | Budget continuity across route edits | **Source-confirmed target mismatch; bounded claim.** Model/artifact changes split root identity; whole-route digest changes exclude old pending launch claims. Limit-only edits preserve bound-root identity. | Stable admitted budget scope. Stale-plan/current-capacity revalidation already fences launch effects; no oversubscription demonstrated. |
| <a id="ar-050"></a>AR-050 | Direct/automatic claim and Open transaction | **Preserve.** Both actors converge on the same launcher; after external workspace preparation, an Immediate transaction rechecks facts and commits claim/Open/binding association. | Preserve reservation continuity when removing proof manifests. Actual concurrent/restart/native execution remains unqualified. |
| <a id="ar-051"></a>AR-051 | OpenCode automatic Goal actor | **Source-confirmed open.** Generated `agent.goal` uses the technical requester, but the runtime actor branch handles only `agent.send`; missing client registration causes committed rejection before native command. | Ordinary attributed Send across capable adapters; do not create a synthetic Manager credential. Admission-only fixture does not cover dispatch. |
| <a id="ar-052"></a>AR-052 | Disable/edit after Goal admission | **Source-confirmed target mismatch.** Codex's pre-send guard requires enabled policy at its original revision; disable/edit/transfer can reject an already-admitted queued Send. | Separate future-admission policy from immutable admitted intent; retain actual rights, Task/Attempt/phase and binding fences. |
| <a id="ar-053"></a>AR-053 | Goal input during review/correction | **Source-confirmed eligibility gap.** Open/unaccepted Task and unreleased Attempt suffice even when Attempt is `submitted` or `needs_correction`. | Pause generic Goal Sends at those phases; preserve capture/check/review/accept and the one findings-bearing correction Send. No live race or candidate mutation proved. |
| <a id="ar-054"></a>AR-054 | Finite Goal continuation | **Source-confirmed target gap.** Settings contain only `goal_id`; per-pass caps and one-use EventRef do not bound successive valid terminal events. | Finite occurrence/no-progress budget, honest usage and one Governor decision; no fresh-ID or transfer reset. No observed infinite native run claimed. |
| <a id="ar-055"></a>AR-055 | Unclaimed Goal event after transfer | **Source-confirmed open.** Ownership conflict leaves no global slot but advances the entry cursor; relocation carries it unchanged to the authorized successor. | Re-evaluate current unclaimed subject after rights/owner change without global history rewind or replay of claimed work. |
| <a id="ar-056"></a>AR-056 | Terminal source and continuation deduplication | **Preserve.** Exact native source evidence, original Task/Attempt/binding and single-use EventRef at Store admission; ordinary continuation/link/slot/cursor commit together. | Admission does not guarantee native dispatch success. Keep atomic identity while deleting proof/cursor copies; result capture/acceptance gaps AR-006/022 remain open. |
| <a id="ar-057"></a>AR-057 | Calendar occurrence across edits/restart | **Preserve within CheckRun scope.** Generation excludes action/owner/revision; considered occurrence survives re-enable, action edit, transfer and Store reload; catch-up is latest-only. | Carry these semantics to one loop; existing separate scheduler/automation loops do not implement general Goal messages. Source review, not executed restart. |
| <a id="ar-058"></a>AR-058 | Native Goal capability versus controller wrapper | **Capability boundary.** Current Codex Rust lacks `agent.goal`; Muse Node maps native verbs; built-in OpenCode wraps instructions/prompt. Newer native upstream sources are not installed support. | Capability-based mapping, independent ordinary Eliot messages, native admission distinct from completion; pinned donor contracts and runtime qualification. |

## Prior connected source trace: AR-045–AR-047

Retained from the pass inspected at `c2b998b` and published at `3c4c859`. The indexing
checkpoint has not rerun it. Earlier details are directly linked in the
[audit index](audit-index.md#evidence-checkpoints).

### AR-045 — cleanup is gated by execution readiness

`store/runtime.rs::validate_agent_shutdown_request` checks the exact binding/generation
and closed resource scope, then rejects every state except `ready`. Both
`user_command_with_actor` and the `next_internal` guard call it. Before that guard,
`next_internal` selects only Recover/Reconcile when `recovery_required` is true.
Thus an admitted Shutdown can also become undispatchable after a state transition.
Its control priority and quota bypass do not override these earlier fences.

Use current actor/object rights and exact resource custody for cleanup. Do not mark a
failed session Ready or replay Open merely to release its transport. Conversely, a
non-ready state does not authorize an unknown process kill. Keep the native scope's
real supported stop/detach semantics and preserve uncertain effects for readback.

### AR-046 — a saved root is not a request to reconnect forever

`swarm-contracts::module_command` limits Shutdown to `external_attach` or
`owned_sdk_transport`. Neither claims native turn, Task or process-family completion.
Codex `shutdown_operation` persists intent, closes its own WebSocket, sets
`native_retired` and returns `owned_resource_released` only after its close condition.
Unknown close also retires its local transport; it is not replayed as a fresh effect.

The Store outcome path settles that Operation but has no shutdown-specific binding
disposition. `runtime::disconnected` retains root/scope and moves a ready/opening
binding to Reconciling. Preserve that unexpected-disconnect behavior; it is not a
complete implementation of intentional retirement.

`module_demand::operation_candidates` selects settled/unknown original Open for every
unreleased binding with native root/scope, even without unresolved input.
`scope_readback` reports retained native identity whenever either native coordinate is
present, irrespective of proven module-family departure. In the consumer,
`StandaloneSupervisor::reconcile_demands` keeps seen demand; it drops a stale held lease
only when native identity and pending Operations are both absent. Explicit transport
Shutdown does not remove attachment demand in this chain.

This does not prove duplicate input, repeated spawn or that the external root stopped.
Native Goal/children may still work. Separate desired attachment from observed native
occupancy using existing binding/Operation ownership. After selected detach, old Open
alone cannot request productive reattachment; saved-output/readback and passive
observation keep their bounded obligations. New attachment requires authorized policy,
not erased history or another supervisor.

### AR-047 — retain historical cleanup; connect the last disposition

`launcher_owned_service::departure_source` reconstructs exact admitted service,
parent/Open, original Attempt and lease from retained provenance. It deliberately does
not require current Task, current manager or still-held lease. Preserve this historical
cleanup mechanism rather than inventing another authority registry.

`Store::reconcile_owned_opencode_departures_once` builds read-only departure evidence,
CASes `persist_departure`, increments a count and returns. That CAS changes only
`owned_service_starts` to `service_departed`; it does not close binding or settle native
inputs. The native reader expects exact process/clean-stop evidence. Process exit,
transport close and successful model work remain different facts.

`workspace_lifecycle::workspace_resource_use` independently holds exact unreleased
binding, unresolved Operations, CheckRun resources, producers and owned-service rows.
Service departure removes the service hold, not the binding hold. Root count likewise
counts unreleased bindings independently. Do not release workspace unconditionally:
a shared service, another writer, unknown input or uncaptured local output may remain.

Join these facts in the existing disposition path. When actual obligations resolve,
retire binding claim and let workspace lifecycle release its authority, preserving
files/results. Never invent native outcomes from OS exit. The ordinary attached-binding
finalizer search is completed by AR-048 below. Crash without clean-stop receipts remains
open; not every resource is unreleasable.

## Inspected source boundaries for that trace

| Source | Read boundary |
| --- | --- |
| `store/runtime.rs` | Shutdown validator/callers; recovery selection; Open/Dispatch/Recover outcome branches, generic settlement and exact-link disconnect. |
| `swarm-contracts/src/module_command.rs` | Shutdown scopes/non-guarantees; command/demand vocabulary. |
| `swarm-adapter-codex/src/lib.rs` | `shutdown_operation`, close condition, saved intent and intent-only unknown recovery. |
| `store/module_demand.rs` | pending/native-root candidate queries, scope readback, family-departure predicate and pending projection. |
| `swarm-supervisor/src/standalone.rs` | demand/readback consumer, held-lease removal, registry obligations and outer-loop exit. |
| `store/launcher_owned_service.rs` | start/readback ownership, departure loop, historical source, proof validation and CAS. |
| `store/workspace_lifecycle.rs` | release transitions, closed-launch boundary and `workspace_resource_use` holds. |
| `store/operations.rs` | owned-service pre-start fence, linked cancellation and rootless binding closure. |
| `runtime/opencode_v2/owned_service.rs` | helper custody, graceful close and read-only retained handle; recovered handle does not gain original stop authority. |

Selected paths, not whole-file audit. External SDK behavior, live OS trees and native
Goals were not executed. That checkpoint additionally sampled public Operation
projection/readback diagnostics in `operations.rs`; those reads did not close the
post-binding cancellation chain. The later trace below does.

<a id="trace-2026-10-23-retirement-admission"></a>

## Source trace at b2519aa: AR-048–AR-050

Inspected on 2026-10-23 at `b2519aa1b70706eeb8a77f76df01e7c4de65c787`.
These are source conclusions, not executed failure reproductions.

### AR-048 — the attached-binding finalizer is missing

`operations::cancel` permits queued work; sent work returns `NOT_QUEUED`. For owned
service starts, `fail_reserved_owned_service_start` permits only the exact reserved,
rootless opening scope. Preserve this fence: late cancellation cannot erase a possible
effect. Production binding-release SQL was enumerated across Store. The only writers
are rootless Open cancellation in `operations::cancel` and
`cancel_owned_service_linked_operation`; both require no native root/scope. No ordinary
attached-binding release handler was found.

Late Open outcome branches in `runtime::outcome` retain root/scope and ready/reconciling
state, including a non-Applied result that reports native identity. `tasks::release`
updates Attempt state after `prepare_owned_service_attempt_release`; it does not retire
that binding. `launcher_owned_service::persist_departure` updates only the service row.

The consumer is connected: `host::supervise_launcher` periodically calls
`Store::reconcile_workspace_lifecycle_once`, which reconciles service departures and
then sweeps leases. `workspace_lifecycle::workspace_resource_use` treats the exact
binding with `released_at_ms IS NULL` as an independent hold. Consequently an attached
binding still fences the lease even after Attempt release and service departure, when
those transitions otherwise succeed. This is a missing final disposition, not a missing
timer and not a reason to remove conservative guards.

Use the existing Store/runtime owner: stop new input, collect exact native/process
closure and required output/readback facts, retire the resolved binding generation,
then let the existing sweep release eligible authority. Keep unresolved children/effects
held. Preserve stored result bytes and their existing reader authorization across
retirement, and verify post-release read paths separately; this source trace did not
audit the complete artifact/grant matrix. Historical result access should not require a
live native binding by default. Do not create a cleanup daemon or equate helper-process
exit with native session completion.

### AR-049 — route version is not a resource budget

`capacity::compare_root_route_identity` compares alias, runtime, artifact and optional
provider/model. For the no-explicit-service branch, a model/artifact change therefore
excludes the old binding from the new target identity. Admission-limit-only changes do
not change this bound-root identity. Separately, `launch_route_match` returns OtherRoute
when retained `selected_route_sha256` differs from the whole current route digest, so
pending unbound launches can disappear from that target's census after an options or
limit edit.

The limiting evidence matters: `current_launch_admission` recounts current facts;
`launch_workspace_plan_inner` and `launch_after_workspace_held` reject changed plan
identity before claim/Open. Configuration is loaded at host start and cloned into Store,
not hot-reloaded by these paths. This trace does not demonstrate concurrent native
over-admission or replay after restart. Preserve those effect barriers.

The current target already requires route edits not to hide live claims. Preserve the
admitted budget scope across model/artifact/options/limit revisions when the underlying
provider/account/service capacity identity is unchanged, using existing launch/binding
facts; keep current route settings for new-work policy. A change to actual resource
identity keeps a separate scope. Alias changes must retain unsettled claims or be
refused. Do not infer account/service identity from matching display labels or merge
genuinely independent resources.

### AR-050 — preserve the working admission transaction

Direct `Store::mutate` starts an Immediate transaction and routes `swarm.launch` to
`launcher::launch` / `launch_for_actor`. WorkDispatch's Immediate reconciliation calls
`admit_work_dispatch`, creates the same ordinary launch, then calls `launch_for_actor`.
Both retain the pending launch before returning; WorkDispatch additionally retains its
semantic slot/link. An unbound admitted launch already represents a root reservation.

After workspace preparation outside SQLite, `reconcile_launches_once` enters another
Immediate transaction. `launch_after_workspace_held` rechecks current admission and
plan, uses `mutate_launch_child_in_transaction` for `task.claim`, pins the lease to the
exact Attempt, admits Open through `open_for_launch_for_actor` / `reserve_open_route`,
and CAS-links the binding back to the Attempt. The admission projection passes through
this transaction to avoid recounting its parent as a second root. Native execution
follows commit.

This is a useful existing boundary. Simplifying WorkDispatch/manifests must preserve
queued-launch reservation, shared direct/automatic authorization, and the later atomic
claim/Open handoff. It must not hold SQLite across external workspace/native work.

| Inspected owner | Exact read boundary |
| --- | --- |
| `store/operations.rs` | `cancel`, linked Open cancellation, owned-service Attempt release, `open_for_launch_for_actor`, `reserve_open_route`. |
| `store/tasks.rs`, `store/runtime.rs` | Attempt release, late Open outcome and ordinary Operation settlement. |
| `store/launcher_owned_service.rs`, `store/workspace_lifecycle.rs` | Departure CAS, exact binding/resource holds and lease release sweep. |
| `host.rs`, `store/mod.rs` | Periodic sweep caller, config loading, direct/automation Immediate transactions, workspace preparation and nested launch mutations. |
| `store/capacity.rs` | Root route comparison, whole-route launch matching, census/deduplication. |
| `store/launcher.rs`, `store/automation_work_dispatch.rs` | Direct/automatic launch, current admission, workspace-plan revalidation and claim/Open continuation. |

<a id="trace-2026-10-23-goal-policy"></a>

## Source trace at 73ba700: AR-051–AR-058

Inspected on 2026-10-23 at `73ba7009bbc5c5515fe4c5f85743066e944106ef`.
Source conclusions below were independently cross-checked; no native failure case,
restart, fixture, Clippy or test was executed. The target changes live in
[configuration](configuration.md#goal-message-and-native-goal), implementation E/N/P/Q
and [Module API](module-api.md#harness-goal-and-eliot-goal).

### AR-051 — OpenCode continuation loses its represented actor before dispatch

`automation_goal_progression::prepare` chooses `agent.send` for the selected current
Codex contract and `agent.goal` with `action:"continue"` for built-in OpenCode.
`admit_goal_progression_operation` / `MutationAuthority::GoalProgression` admits the
ordinary Operation with caller `eliot-internal-automation-v1` and retains its exact
on-behalf link. That technical ID deliberately has no client registration or credential.

The running OpenCode driver calls `runtime::next_with_config` → `next_internal`.
Its technical-actor branch is gated on `method == "agent.send"`; generated
`agent.goal` does not reconstruct Goal authority. The ordinary caller lookup then
fails with `UNAUTHORIZED` / `original caller no longer registered`. This lookup is
inside the guard closure: the guard handler commits `queued` → `rejected` and returns
`command:null`. It does **not** leave that Operation queued or enter the driver's
selector-error retry branch. No native command is selected on this path.

The retained O9 transfer fixture checks queued admission, caller and on-behalf link,
not this dispatch boundary. The correction belongs in the common ordinary Send path
with represented Manager authority; registering a privileged fake client would hide
the integration error and create a second authority. Existing direct OpenCode Goal
wrapper behavior is separate from this automatic failure.

### AR-052 — future-admission policy is checked again as admitted intent

For Codex continuation, `next_internal` calls
`automation_goal_progression::require_current_for_dispatch`, which reconstructs the
original admission and calls `require_current_admission` at Dispatch stage. It requires
the old owner's entry to remain enabled/Goal-ready at the exact admitted automation
revision. A pure disable or settings edit between admission and native selection
therefore rejects queued input; transfer also leaves the former entry disabled.
This contradicts the target's disable/transfer semantics even when the represented
Manager still has the exact Task/Attempt rights.

That function also checks real current authority, Task/Goal revision, objective,
Attempt/binding and native/source identity. Retain those effect fences. The Dispatch
stage already preserves an admitted Codex artifact selection when route selection
changes; apply the same separation to immutable intent versus future policy. This is
not permission to send after actual revocation or stale Task/candidate scope. Sent
effects remain ordinary readback/result obligations; the finding concerns queued input.

### AR-053 — unaccepted does not mean ready for another generic prompt

`submissions::finish` sets a successful current Attempt to `submitted`, retaining the
open Task and unreleased Attempt. `request_changes_core` sets `needs_correction`.
`goals::readback_value` correctly reports completion only after exact Task acceptance.
However, `progression_target` treats any current open/unreleased bound assignment with
pending completion as active, without checking Attempt phase. `target_for`,
`require_current_admission` and `runtime::user_command_with_actor`'s GoalProgression
branch do not add that phase check. The repair branch separately requires
`needs_correction`.

A valid terminal observation can therefore be consumed after submission or a
correction decision and remain eligible for generic continuation. Codex's adapter
checks native idle/input identity, not Store candidate phase. This establishes an
eligibility gap; it does not prove a live race, mutation of an immutable candidate or
duplicate correction execution. Pause generic Goal input at review/correction phases
and retain the positive path through capture/check/review/accept or one exact
findings-bearing correction Send. Do not block all commands on a submitted Attempt.

### AR-054 — a bounded pass is not a bounded Goal

`GoalProgressionSettings` contains only `goal_id`. The progression `State` stores
cursor/activation/source-gap/recent fields, without total occurrence, turn, elapsed,
cost or no-progress accounting. `event_slot_id` changes for each new native input and
terminal EventRef. Thus successive valid completed turns can each admit a new Codex
continuation while the shared Goal remains pending. Entry/event/page bounds only cap
one pump pass. The separate one-shot reminder's enabled state is intentionally absent
from progression eligibility.

Real existing stops include disabled new-work execution, policy/authority changes,
stale scope and unavailable binding. They do not bound a repeated valid continuation
with unchanged assignment and no acceptance. No infinite run was executed or claimed.
Implement the finite occurrence/no-progress contract in the existing entry/Goal and
linked Operations; progress needs relevant candidate/evidence/finding facts. A new
event/request ID, transfer or host restart is not budget renewal. Native Goal's own
limits remain native and separate from Eliot message consumption.

### AR-055 — transfer preserves a cursor that skipped its still-unclaimed subject

In `reconcile_entry`, an ownership/scope conflict deliberately does not write the
global terminal slot: another authorized entry may still claim that event. Both the
prepare-error and admission-conflict paths nevertheless advance the per-entry cursor
past the observation. `relocate_state` copies that cursor to the successor, changing
only owner/revision/time; `automation_transfer::transfer` calls it without reactivation.
`event_page` then selects only observations after the retained cursor.

For an entry that encountered `GOAL_MANAGER_SCOPE_CONFLICT` and is then transferred
to a now-authorized Manager, the same entry does not revisit that unclaimed event.
Another authorized entry or explicit reactivation may recover it; this is not global
event destruction. Target current-state reconsideration must preserve that distinction:
retry the still-current unclaimed subject after rights/owner changes, keep claimed
Operation provenance immutable, and avoid a full historical rewind or new retry queue.

### AR-056 — preserve source identity and atomic admission; keep completion distinct

`runtime::codex_goal_terminal_evidence` requires an Applied completed turn, verified
input/turn identity and matching retained EventRef, Operation, Task/Attempt and binding.
It accepts exact Dispatch or Goal-Send admission; arbitrary Send is not currently
eligible. `outcome_with_artifacts` stores the receipt/native references and matching
`goal.terminal.evidence` observation in one transaction, rejecting conflicting replay.
Built-in OpenCode's `execution_reads::{terminal_evidence,record}` binds its observation
to synchronized durable input-execution proof and the same original work identity.

`verified_terminal` requires a settled eligible source Operation, validates retained
native proof and treats unresolved source evidence separately from rejected/cancelled
input. `event_slot_id` includes binding/generation, native session/input and terminal
EventRef ID/sequence/digest. Existing slot suppresses a duplicate even under another
entry/owner. Successful ordinary mutation, on-behalf link, slot and cursor commit in
the same reconciliation transaction. Preserve these source/atomicity properties when
deleting duplicate Goal proof and cursor machinery.

Single-use means at most one continuation Operation admitted for that EventRef,
regardless of its later native dispatch outcome. In AR-051 the slot remains claimed
after the Operation is rejected; another entry cannot silently retry that same event.
It is not an exactly-once native-execution or successful-continuation guarantee.

This proves only the selected source code's native-terminal/deduplication boundary.
Codex explicitly reports `task_completion:"unknown"`; neither producer captures the
assistant result or accepts the Task. Shared Goal completion reads exact Store
acceptance. The Dispatch-only normalized result and missing automatic capture/readback
gaps remain [AR-006](#ar-006) / [AR-022](#ar-022), including after attributable Send.

### AR-057 — preserve the existing calendar's stable occurrence semantics

`scheduler::calendar::generation_digest` uses expression, timezone and anchor.
`occurrence_id` uses logical origin/project/automation, that generation and intended
UTC due time, excluding current owner, action and revision. `latest_due` coalesces to
the newest unconsidered occurrence. `automation_cron::configure_activation` can change
the activation cut but keeps the generation's considered cursor; `include_existing`
does not replay it. Disabling removes the due index, not admitted Operations; changing
back to a prior calendar preserves its cursor. Transfer preserves logical origin and
cursor while updating the due-index executor. These records are Store-backed.

The operator interval path similarly selects its latest slot, and global new-work
disable holds due work rather than replaying a backlog on resume. The scheduler has
startup/change/safety wakes; `host::supervise_automation` is still a separate loop.
Preserve these semantics in the target single host loop. Current actions are
CheckRun-only, not arbitrary Goal/Send scheduling. Existing transfer/restart fixtures
were read as source, not executed; real clock, DST and crash behavior remain separately
qualified for their actual library/runtime versions.

### AR-058 — use the native contract without advertising unimplemented support

Current Codex Rust `Checkpoint::next_observation` marks `agent.goal` unsupported;
the controller's GoalProgression uses ordinary Codex Send. The inspected upstream
Codex source at `970111f6cb62cc02fe36a82ea7d71b37a2889c97` exposes native
`thread/goal/set|get|clear`, feature/provider gates and native idle continuation.
Mapping that API is target implementation, not a feature established by Eliot's route.

Muse Node `settings.mjs::goalCommand` / `bridge.mjs` maps native set/edit/pause/resume/clear
with Operation-derived command identity and admission settlement. Newer SDK source at
`09d67eca616b7b02a2d060271123c1fedbdb8fb7` states admission-only ACK and limits the
meaning of a returned turn ID. It does not qualify Eliot's SDK 1.3.0 lock or the native
binary. Built-in OpenCode `runtime/opencode_v2/goal.rs` owns an `eliot.goal` instruction
entry and prompt activation, explicitly without a native Goal API. Its old wrapper
must not become the common meaning of native Goal.

Exact upstream links and version limits live in the
[donor map](donor-map.md#native-goal-and-eliot-goal) and existing donor TOML records.
Reuse native continuation where the selected installed capability supports it, while
keeping independent bounded Eliot messages, Task acceptance and original attribution.
Do not add another executor, proof wrapper or synthetic native capability.

| Inspected owner | Exact read boundary |
| --- | --- |
| `store/automation.rs`, `automation/config.rs` | Policy apply/activation calls, Goal settings and enabled-action predicates. |
| `store/automation_goal_progression.rs` | Activation, source verification, target/prepare, admission/effect revalidation, slot identity, cursor/conflict handling and transfer relocation. |
| `store/mod.rs`, `automation/authorization.rs` | Immediate reconciliation/admission, technical requester, represented actor and Operation link; production client-registration inventory. |
| `store/runtime.rs`, `store/opencode.rs` | Queue selection/guard error commit, Goal/repair actor checks, native driver entry, Codex terminal production and outcome transaction. |
| `store/opencode/execution_reads.rs`, `runtime/opencode_v2/goal.rs` | Durable terminal proof/observation and direct controller instruction/prompt wrapper. |
| `store/goals.rs`, `store/submissions.rs` | Exact acceptance readback, current progression target, submitted and needs-correction transitions; not the full review/acceptance trace. |
| `store/automation_transfer.rs`, `store/automation_cron.rs`, `scheduler/calendar.rs`, `scheduler.rs`, `host.rs` | Cursor relocation, calendar generation/occurrence/catch-up, due-index activation and current distinct loops. |
| Codex `src/lib.rs`, Muse `settings.mjs` / `bridge.mjs`; pinned official Goal files | Current advertised/native Goal mapping and selected upstream contract; no native installation/upgrade. |

## Next work and retained evidence

Resume the candidate/review/correction/acceptance chain named above. Goal selection,
terminal evidence and calendar identity now have a connected source trace; their open
defects are implementation work, not reasons to repeat the same source pass. The full
independent native/Eliot Goal combination, automatic capture and installed restart
remain unqualified. Existing AR-048/049 and earlier runtime findings remain open.

[Coverage still open](audit-index.md#coverage-still-open) names the other areas;
[evidence checkpoints](audit-index.md#evidence-checkpoints) links prior detailed traces.
Keep IDs and refuted/narrowed claims stable. A documentation commit repairs no runtime.
