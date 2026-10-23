# Eliot Swarm Launcher, Queue and Assignment Context

Target contract subordinate to [architecture](agent-operations/architecture.md) and
[implementation B](agent-operations/implementation.md#b--queue-routes-provider-state-and-admission).
The [audit register](agent-operations/audit-register.md) owns source evidence and gaps.
This replaces the old parallel Launcher program, not Task/Attempt/Operation authority.

## 1. Product law

```text
Governor selects or enables work policy
-> Store ranks current work and explains eligibility
-> one transaction authorizes and reserves one launch
-> existing owners perform workspace/native effects after commit
-> ordinary capture, review, correction and acceptance continue
```

Governor and Manager are agent roles. Routine admission, recovery and route selection
run under configured policy, not a human click. Preview is optional and reserves
nothing. Launch rereads current facts; callers supply no plan digest or proof packet.

## 2. Queue is a read projection

`swarm.queue.get` joins current Task/revision/Attempt, feedback/review, accepted
dependencies, workspace leases, selected routes, provider facts and root availability.
A row carries subject IDs, rank/reasons, eligibility/blockers, actual route candidates,
write conflicts and coverage/gaps. It is not editable queue storage.

Default priority is correction, unfinished work, then ready unclaimed Tasks, refined
by explicit project/phase policy and stable age/Task-ID tie-breaking. Do not infer
size or value from prompt length. Parked, delivered or review-waiting work is not
redispatched until a relevant fact changes. Unsatisfied dependencies block automatic
work; explicitly authorized investigation may proceed, but acceptance still enforces
its frozen requirements and dependencies.

Eligibility is evaluated before limiting admitted work. Cheap filters belong in the
query; expensive checks use bounded keyset continuation. A first page of blocked
Tasks must not prevent a later eligible Task from being examined.

Use one host loop's stable scan position and cycle boundary. The existing launcher
`pending_launches` pattern orders by `(created_at_ms, operation_id)`, not mutable
`updated_at_ms`; preserve that property. Fairness between enabled policies and between
candidates within a policy both matter. A restarted scan recomputes current facts.
Do not retain a second pending-subject list, event catch-up queue or eligibility cursor
per Task. Raising the old 128-subject cap does not fix starvation.

## 3. Lane and routes

Lane is an operational view of Manager/project/policy, primary and explicitly ordered
fallback routes, current queue, capacity/provider state and active bindings/workspaces.
No lane table, daemon, inbox or heartbeat.

Disabling policy stops future admissions, not admitted execution or required result
capture. Fallback revalidates the selected route and never silently changes model,
effort, account, permissions or agent profile. A compatible idle binding may be reused
under continuation policy, after considering pending inputs, native Goal/children and
unknown effects. Parent idle alone is not availability or release.

## 4. Atomic admission, not atomic external I/O

Direct `swarm.launch` and automatic `dispatch_once` use the same Store boundary:

1. verify current actor/project/Task/revision and exact existing Attempt, if any;
2. recheck route, provider conditions, root budget and write-scope conflicts;
3. atomically retain the ordinary launch, its root claim and workspace reservation;
4. return admission only after commit.

A later caller sees the earlier reservation. Native-ready observation is not required
before a pending root counts. A preview page or cached capacity report cannot authorize
this transaction. Actual filesystem/Git/native work occurs outside it through the
existing owners and is revalidated at its relevant effect boundary.

Claiming the Attempt, attaching its binding and admitting the Open child must transfer
one reserved launch identity, not count parent and child separately. The existing
transaction-local Open handoff is a useful mechanism; its duplicated manifests and
hashes are not a requirement. Do not add a reservation service or per-root workflow.

An uncertain external effect retains its exact Operation and resource claim for
readback. Definitively unsent cancellation may dispose its reservation; generic
Rejected, timeout, client exit or Task acceptance is not proof of native departure.
Release of workspace, root and retained result source has separate conditions.

## 5. Count current root claims, not historical paperwork

A configured concurrent-root budget counts unique live/reserved roots:

| State | Counted identity |
| --- | --- |
| Admitted launch before binding exists | Existing launch Operation reservation |
| Bound opening before native identity is known | Exact binding/generation |
| Native root observed | Exact service scope + native root, linked to its binding |
| Several Open/Dispatch/service records for that root | The same claim, not additional slots |
| Root outcome unknown or resource departure unresolved | Claim remains held |
| Definitively disposed reservation/root | No current claim; retain history separately |

Readback, Result, Refresh and ordinary control do not acquire another model/root slot.
A same-root recheck excludes only its own claim; it does not erase other active work.
Root count is not the number of active model turns, native children or RPCs.

Derive this projection from existing current launch/binding/resource facts in the
writer transaction. Keep budget keys and active predicates indexed. Closed bindings,
completed Operations and historical evidence-ledger size cannot exhaust current root
capacity. Do not load all history, compare a second ledger or fail the whole fleet
because unrelated old records exceed a diagnostic page budget.

The configured route/budget identity must be independent of optional native model
selection. A supported default model remains a valid route without inventing its
provider/model. Model-specific quota uses observed or configured model facts separately.
Route edits cannot hide still-live claims by changing a display alias, model or digest;
explicit budget changes affect future admission without deleting existing reservations.

Delete the capacity phase/evidence ledger and its sync/refusal/compatibility machinery
only when the indexed current projection serves all admission/report callers. Do not
replace it with another counter database, periodic repair engine or mandatory pruning.
Genuinely ambiguous active ownership remains a scoped gap, not an assumed free slot.

## 6. Provider conditions are source facts

Quota, concurrency, transport health, host resources and root slots are different.
Normalize availability by real source/service/account/bucket/window with epoch,
ordering, observed time and coverage. Alias or model text is not account identity.
Multiple bindings observing one account do not create independent account limits.

First select the valid current observation for the same actual source/bucket; then
combine independent applicable constraints. An obsolete copy from a replaced source
must not permanently veto newer verified availability. Conversely, a fresh observation
for one window does not clear another window's quota, and a global newest timestamp
cannot resolve uncertain account attribution. Preserve original provenance for diagnosis.

| Current fact | Enabled action |
| --- | --- |
| Available | Admit subject to current root/write/resource checks |
| Rate limited / overloaded / quota exhausted | Hold new affected work; make bounded refresh or permitted probe due at retry/reset time |
| Model gone | Use only an explicitly selected compatible fallback or addressed Governor decision |
| Authentication/data-policy requirement | Governor uses authorized remediation; operator only for missing credentials/authority/intent |
| Stale or ambiguous required source | Reconcile exact source under bounded policy; unrelated resources proceed |
| Optional meter never observed | Report unknown; apply configured policy rather than invent a hard meter requirement |

Retry/reset time is not evidence of recovery. Probes use the existing source reader or
an explicitly authorized single native attempt, not a prompt flood or new probe daemon.
Coalesce unchanged holds and wake on relevant source change/due time. Current results,
replies, cancellation and reconciliation remain available when new workload is held,
subject to their actual native permissions.

## 7. Workspace and compact assignment

Freeze repository-relative `initial_paths` and hard `forbidden_paths` into the Attempt.
Empty allowed scope means repository root. Reject absolute paths, traversal, empty
segments and `.git`; overlap uses path segments (`src/a` is not `src/ab`).

The workspace lease is Eliot's write-admission/overlap authority, not an OS sandbox or
replacement for caller permissions. One Manager owns one mutable worktree and its Git
index/branch/commit operations. Leaf writers may edit disjoint scopes, not compete for
Git-index ownership. Review reads an immutable candidate or isolated snapshot and
needs no writer lease. Remove advisory `code.scope.*`, not base authentication/grants.

One TaskPacket contains goal/phase, requirement-linked verification, selected sources,
current findings/dependencies, write boundary, worktree/branch/baseline, selected route
and deliverable. Render initial/continue/correction through the existing owner. Keep
required criteria; omit full Issue/audit history, global queue, raw Store JSON and proof
digests. Missing sources remain explicit gaps, not permission to drop requirements.

## 8. Observation and small public surface

The Governor's local view joins Task/Attempt/Operation/binding with native session,
input/turn/children, provider source cursor, PID + birth identity, actual worktree/Git,
last remote-ref observation and quota/activity/connection facts. Unmatched subjects and
source gaps remain visible. A live PID, recent output or unchanged branch proves none
of the other axes. Ordinary reads do not call a provider harness.

Keep existing small surfaces: queue, launch/optional preview, agent state/family/usage,
semantic controls, Operation readback, result capture and review/acceptance. Launcher
does not require council, contract ratification, integration ACK, code-scope approval,
mandatory MCP readiness or automatic Issue-label publication.

## 9. Connected completion cases

- Corrections/unfinished work rank before new work without redispatching review-waiting work.
- 128 blocked Tasks do not hide later eligible Tasks; held launches keep stable scan position.
- Concurrent admissible launches cannot exceed the same root budget; parent/Open/service records count once.
- Large closed/completed history does not change an otherwise identical current admission decision.
- A valid native-default Antigravity route works with a configured root limit and no model override.
- Fresh same-source quota evidence supersedes its stale duplicate without clearing independent constraints.
- Policy disable admits no new work and cancels neither active work nor capture/readback.
- Overlapping writers are rejected before effects; disjoint writers retain one Manager's Git ownership.
- Direct launch works without mandatory preview/proof packets; automatic work uses the same handlers.
- Unknown effects receive bounded readback and an addressed Governor decision, not routine human repair.

These are target cases, not executed results. Current findings and untraced disposal,
concurrency and route-change boundaries remain in the audit register.
