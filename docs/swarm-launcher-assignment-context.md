# Eliot Swarm Launcher, Queue and Assignment Context

Target contract subordinate to [architecture](agent-operations/architecture.md) and
[implementation B](agent-operations/implementation.md#b--queue-routes-provider-state-and-admission).
The [audit register](agent-operations/audit-register.md) owns evidence and gaps.
Launcher connects existing Task/Attempt/Operation/workspace owners; it is not another
scheduler, task database or compatibility product.

## 1. Product law

```text
Governor enables work policy
-> Store ranks current work and explains eligibility
-> one transaction authorizes and reserves one launch
-> existing owners perform workspace/native effects after commit
-> ordinary capture, review, correction and acceptance continue
```

Governor and Manager are agents. Routine admission, recovery, cleanup and route choice
run under configured authority, not human clicks. Preview is optional and reserves
nothing. Launch rereads facts; callers supply no plan digest or proof packet.

## 2. Queue is a projection

`swarm.queue.get` joins current Tasks/Attempts, review/correction, accepted dependencies,
workspace leases, selected routes, provider facts and resource claims. Rows expose
subject, rank/reasons, eligibility/blockers, route candidates, write conflicts and gaps.

Priority is correction, unfinished work, then ready unclaimed Tasks, refined by explicit
project/phase order and stable age/ID ties. Parked, delivered and review-waiting work
waits for relevant change. Automatic work respects dependencies; explicitly authorized
investigation does not bypass later acceptance requirements.

Filter cheap eligibility before LIMIT; use bounded keyset continuation for remaining
checks. Blocked first-page Tasks cannot hide later eligible work. Preserve the existing
launch scan's immutable `(created_at_ms, operation_id)` order and fixed cycle ceiling,
not mutable `updated_at_ms`. Fairness applies between policies and their candidates.
Keep the host loop's scan position, not per-Task copied pending subjects/event catch-up.
Restart recomputes facts. Raising the old 128-subject cap is not a repair.

## 3. Lane, routes and existing sessions

A lane is Manager/project/policy plus ordered routes, current queue, resource/provider
facts and bindings/workspaces. No lane table, daemon, inbox or heartbeat.

Disabling work policy stops future admissions, not admitted execution or required
capture/readback. Fallback is explicit and revalidated; no hidden model, effort,
account, permission or workspace substitution. Compatible session reuse considers
pending inputs, native Goal/children and unknown effects; parent idle is insufficient.

Keep the original binding's native endpoint/account/workspace identity. Current config
may change enablement/budgets, but cannot silently move existing work elsewhere. Route
edits also cannot hide live claims behind a new alias/model/digest.

Carry the admitted budget scope on the existing launch/binding facts. Preserve it when
editing a logical route if the underlying provider/account/service capacity identity is
unchanged; keep a separate scope when actual resource identity changes. Refuse an alias
change that would orphan unsettled claims. Count proven shared-service roots once. Do
not merge unrelated services from matching model/alias text. Current configuration is
loaded at host start, so file edits take effect after restart, not by an implemented
hot-reload contract.

## 4. Admission and effect boundaries

Direct launch and `dispatch_once` use the same Store transaction:

1. check current actor/project/Task/revision and exact Attempt, when present;
2. recheck route, provider, resource budgets and write conflicts;
3. retain the ordinary launch and attributable root/workspace reservation;
4. return admission only after commit.

A later caller sees the reservation before native-ready observation. Parent launch,
binding and Open child represent one reservation, not three. Preserve the existing
transaction-local handoff; delete repeated manifests/hashes, not atomic check/reserve.

Git/filesystem/native I/O runs outside Store. Revalidate before each actual effect.

Current direct and WorkDispatch callers already share `launch_for_actor`, then
`launch_after_workspace_held` after workspace preparation. The latter commits exact
Attempt claim, Open/binding reservation and lease association in one Immediate
transaction. Preserve that handoff while removing manifest/proof duplication; never
combine the two stages by keeping a transaction open across workspace I/O.

A parent launch can remain Queued while its workspace effect is already in flight;
its generic Operation state is not a complete description of all child resources.

### Cancellation and failure must finish resource disposition

Use existing `operations::cancel`, launch, workspace and process owners for a common
terminal disposition, rather than just changing the parent Operation's state.

| Exact current facts | Automatic disposition |
| --- | --- |
| No workspace/native/service effect admitted | Cancel/reject unsent work and release its own reservations atomically. |
| Verified held workspace, no attached Attempt/binding or admitted child/resource effect | Release that lease authority and root reservation with cancellation; preserve prepared files and evidence. |
| Workspace preparation in flight or outcome unknown | Stop later admissions; retain the workspace hold and complete/join exact readback. Do not repeat Git preparation blindly. |
| Rootless Open rejected before sending, no independent service/process effect | Retire the exact opening binding/lane claim and dispose its unused reservation through the same no-effect path. |
| Open/input may have executed, or native process/service is observed/unknown | Keep attributable ownership; bounded native/process readback or supported exact cancellation, never infer no effect from Rejected. |
| Exact owned resource departure established | Dispose only that resource claim; other native/workspace/output obligations are checked separately. |

Cancel versus send/start is a transaction/CAS race. A winner before the effect boundary
prevents that effect. A late cancellation cannot retroactively erase it. Preserve the
owned-service reserved -> failed-no-effect versus start-permit fence; do not replace it
with a timestamp or process-name guess.

A required cleanup must not depend on the spelling Settled versus Cancelled or on an
unrelated success-shaped receipt. Record attributable disposition with the existing
Operation/resource rows. Retrying that disposition is idempotent; unknown effects stay
held. Never require routine manual SQL, a new cleanup daemon or a second proof ledger.

### Scoped shutdown and retirement

`Shutdown` names a supported owned resource. Current `external_attach` closes only the
adapter's connection; `owned_sdk_transport` releases only its SDK transport. Neither
means Task accepted, native Goal stopped, children finished or process family departed.
External shared services are never adopted or killed to make local cleanup succeed.

Authorize cleanup from current actor/object scope and exact retained custody, including
an eligible Draining/Reconciling binding. New-work Ready/quota gates are not cleanup
permissions. Do not set Ready, replay Open or create a session to make a stop/read pass.
An unknown or unsupported resource scope remains an addressed decision for Governor,
not a blanket kill or a routine human task.

Keep desired control attachment separate from observed native occupancy in the existing
binding/Operation owner. On explicit successful detach, the historical Open alone must
not request productive reattachment. Preserve native identity, active Goal/children,
unknown effects, result delivery and passive observation. Bounded completion/readback
may still require an adapter; that is not new execution. Later productive attachment
requires an authorized policy action, not automatic resurrection from retained history.

Enabled retirement policy follows the same current-state loop:

```text
selected drain/detach/stop intent -> exact adapter/resource action
-> record its real disposition -> inspect remaining native/process/output obligations
-> retire the resolved binding claim -> release eligible workspace authority
```

Use `runtime` settlement, `module_demand`, existing process/service observations and
`workspace_lifecycle` as owners. A service-departed row or module-family exit is one
input, not a universal release receipt. Recompute disposition after relevant facts and
restart; short Store transitions converge idempotently. Unknown work retains its exact
hold while unrelated work proceeds. No new teardown service, workflow or proof family.

Reuse the historical cleanup pattern in `launcher_owned_service::departure_source`:
resolve the admitted resource from original Operation/Attempt/lease, not current Task
revision or changed route. Current authority to act and immutable execution provenance
remain separate. Never rewrite original ownership or fabricate terminal model results
from OS exit. Physical departure, required output capture and Task acceptance differ.

Workspace release removes only its authority, not the worktree or unpublished changes.
When native activity really remains after transport detach, retain its resource hold;
when all relevant obligations are resolved, the ordinary disposition must retire the
binding rather than leave its mere existence as a permanent workspace/root blocker.

The source trace at `b2519aa` found no ordinary attached-binding finalizer:
[AR-048](agent-operations/audit-register.md#ar-048). Only rootless pre-native Open
cancellation sets binding release today. Add the final disposition in the existing
Store owner; keep the already-running workspace sweep. Preserve retained Store artifacts
and existing reader authorization across retirement, then verify post-release read paths
separately; the source trace did not audit the complete artifact/grant matrix. Historical
bytes do not themselves reserve a live root.

## 5. Root budgets count current allocations

| Resource representation | Count |
| --- | --- |
| Admitted unbound launch | One launch reservation |
| Bound opening without native identity | The same claim under binding/generation |
| Native root observed | The same claim under exact service/root identity |
| Open, Dispatch and service records for that root | No additional slots |
| Unresolved native ownership | Claim retained |
| Definitively disposed reservation/resource | No current claim; history retained |

Readback, Result, Refresh and control do not allocate another root. Distinguish the
root-count delta of the proposed action: new root +1, already-reserved same root +0.
A reduced root limit blocks additional allocations; it does not implicitly pause all
existing roots' next turns. New-input mode, provider quota and explicit drain policy
still apply to continuation. An explicit pause policy is not hidden in root arithmetic.
Do not kill roots or erase claims to make a lowered limit fit.

Count indexed current launch/binding/resource facts in the admission transaction.
Closed bindings and completed historical Operations cannot consume a current budget.
No full-history ledger comparison, global damage from unrelated diagnostic limits,
periodic counter repair or mandatory history deletion. Ambiguous active ownership is a
scoped gap, not an assumed free slot.

Budget identity is independent of optional model selection. A supported native-default
route is valid without a fabricated provider/model. Model-specific quota and selected
model facts remain separate. Delete capacity ledger/sync/refusal/compatibility code when
this projection serves every admission/report consumer; do not create another ledger.

## 6. Provider conditions and non-work commands

Quota, root slots, active turns, transport and host resources are distinct. Normalize
facts by actual service/account/bucket/window, source epoch/order/time and coverage.
Alias or model text does not prove account identity.

Resolve current evidence for the same source before combining independent applicable
constraints. An obsolete duplicate cannot veto its verified successor. A fresh window
does not clear another window's quota; global newest timestamp is not sufficient.

Distinguish native account/connection invalidation from loss of Eliot reporting IPC.
The latter is a coverage/connection event, not evidence of quota recovery or native
death. Keep provenance, reconcile source state independently and supersede only copies
whose source relation is established. Do not keep an obsolete binding's stale permission
as a permanent veto after the same account is freshly observed elsewhere.

| Fact | Enabled behavior |
| --- | --- |
| Available | Admit subject to relevant resource/write checks |
| Rate limit / overload / exhausted quota | Hold affected new work; schedule bounded refresh or explicitly permitted probe at due time |
| Model gone | Selected compatible fallback or one addressed Governor decision |
| Authentication/data-policy requirement | Authorized remediation; human only for missing access/authority/undecided intent |
| Required source stale/ambiguous | Targeted bounded readback; unrelated sources continue |
| Optional meter never observed | Report unknown and apply policy; do not invent a mandatory meter |

Reset/retry time is not recovery proof. Coalesce holds and wake on relevant source
change/due time. No probe daemon or prompt flood.

Keep the current `starts_native_work` distinction: Result, Refresh and strict read-only
Reconcile bypass model/root admission. Replies and exact stop controls retain their
own permissions. Recovery fencing must allow authorized saved-output completion and
safe readback without claiming the session is ready. It must not grant new input or
permit an arbitrary historical read. Native resume can start work and remains distinct
from reading saved bytes. Control priority must not starve result delivery indefinitely.

## 7. Workspace and assignment

Freeze allowed/forbidden repository-relative prefixes into the Attempt. Empty allowed
scope means repository root. Reject traversal, absolute paths, empty segments and
`.git`; compare path segments, not raw string prefixes.

Lease authority coordinates Eliot writers, not unrestricted native shells. One Manager
owns the mutable worktree and Git index/branch/commit. Leaf writers edit disjoint scope;
review uses an immutable candidate/isolated snapshot without a writer lease. Remove
advisory `code.scope.*`, not base authentication or Task/reviewer grants.

TaskPacket carries goal/phase, requirement-linked verification, selected sources,
current findings/dependencies, write boundary, worktree/branch/baseline, selected route
and deliverable. One renderer handles initial/continue/correction. Keep actionable
criteria; omit Issue/audit history, raw Store JSON and controller proof digests.

## 8. Observation and public surface

Governor reads local Task/Attempt/Operation/binding, native inputs/children, provider
source, PID/birth identity, worktree/Git/remote ref and quota/activity/connection facts.
Unmatched subjects and gaps stay visible. No provider call on ordinary view reads.

Keep queue, launch/optional preview, agent state/family/usage, semantic controls,
Operation readback, results and review/acceptance. No council, ratification, integration
ACK, code-scope approval, mandatory MCP readiness or label-publication workflow gate.

## 9. Connected completion cases

- Corrections/unfinished work rank first; blocked first pages and 128 old blockers do not hide eligible Tasks.
- Concurrent admissions respect one root budget; parent/Open/service represent one allocation.
- Large terminal history cannot change an otherwise identical current admission decision.
- Native-default Antigravity works with a root limit and no model override.
- Lowering a root limit prevents growth while zero-growth continuation follows explicit input/quota policy.
- Cancellation of verified-held unbound launch releases authority without removing files; in-flight Git remains fenced until readback.
- Pre-send Open rejection retires only a proven unused binding; post-spawn failure retains ownership.
- Fresh same-source facts supersede stale copies without clearing independent quota windows.
- Held/disabled new work does not block authorized results, readback and exact stop/reply controls, including eligible non-Ready cleanup.
- Explicit detach removes productive attachment demand, not native Goal/child facts or incomplete output obligations.
- Exact resource departure converges through binding retirement to eligible workspace release; an active shared service or unresolved writer is never released by association.
- Overlapping writers are blocked before effects; review reads a frozen candidate.
- Direct and automatic launch share handlers without mandatory preview/proof packets or routine human repair.

These are target cases, not executed results. The post-binding finalizer gap, route-edit
accounting and common admission transactions are source-traced in AR-048–050. Actual
restart/concurrency, revocation and crash-only departure remain unqualified.
