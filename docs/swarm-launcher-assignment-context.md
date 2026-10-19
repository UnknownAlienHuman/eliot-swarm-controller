# Eliot Swarm Launcher, Queue and Assignment Context

**Current authority:**
[architecture](agent-operations/architecture.md) and
[implementation](agent-operations/implementation.md). This file keeps the useful
launcher/queue requirements from the 2026-10-03 design without preserving its former
parallel product program.

The launcher is one application path over existing Task, Attempt, Operation, route,
workspace lease and observation authority. It is not another task database, scheduler,
coordination system or compatibility layer.

## 1. Product law

```text
Manager selects or enables current work policy.
Store ranks current Tasks from durable facts.
Launch revalidates one Task and reserves one Attempt/workspace atomically.
Adapter receives one compact TaskPrompt and one semantic command.
Governor observes local state without provider calls.
Result returns through the ordinary submission/review/acceptance path.
```

Preview is optional. A manager may inspect the same facts before launch, but launch
does not require a caller-supplied plan digest or a capability/proof packet.

## 2. Queue projection

`swarm.queue.get` is a bounded read projection over:

- Task revision/state and current Attempt;
- correction/review state;
- dependencies and accepted prerequisites;
- active workspace/write leases;
- selected routes, provider conditions and root capacity;
- current binding/session availability;
- source coverage/freshness.

It is not editable queue storage. A page is not a reservation.

### Row

```json
{
  "task_id": "task-id",
  "task_revision": 4,
  "attempt_id": null,
  "state": "ready",
  "rank": 12,
  "why_ranked": ["ready_unclaimed", "route_available", "oldest_eligible"],
  "blockers": [],
  "route_candidates": [
    {"alias": "codex-writer", "state": "available"}
  ],
  "write_scope": ["crates/example"],
  "write_conflicts": [],
  "coverage": "complete",
  "gaps": []
}
```

### Default ordering

1. candidate returned for correction;
2. current unfinished Attempt needing continuation/relaunch;
3. ready unclaimed Task;
4. explicit manager/task phase order when configured;
5. oldest eligible Task and stable Task ID.

The rank is deterministic and names its reasons. Do not infer task size from prompt
length or model judgment. Missing/partial data remains a blocker or gap, not a guessed
rank.

Dependencies remain visible. Automatic dispatch skips an unmet dependency. A manager
may explicitly start bounded investigative work, but acceptance still enforces the
frozen Task requirements/dependencies.

## 3. Lane is a view

A lane is the operational name for:

```text
manager + project + automation entry
+ primary route and explicit fallback aliases
+ current queue projection
+ route/provider/host capacity
+ active bindings and workspace leases
```

Do not create a lane table, daemon, cursor, inbox, heartbeat or independent queue.
The automation entry is the policy identity. Disabling it stops future admissions;
active work continues.

A fallback route is used only when explicitly configured. It is revalidated and never
silently changes provider, model, effort or agent profile.

## 4. Direct admission

Manual `swarm.launch` and automatic `dispatch_once` use the same Store transaction:

1. authorize manager/project;
2. reread current Task revision and Attempt;
3. validate selected route/provider/capacity;
4. reserve the workspace/write lease;
5. create the launch Operation with deterministic request identity.

Filesystem, Git and native I/O occur after commit through existing owners. If the
external workspace effect is uncertain, retain the Operation/lease and read it back;
do not create another launch.

Automatic dispatch queries a bounded ranked page and tries candidates until one is
admitted or the page is exhausted. Ordinary blockers are returned in the queue
projection. They are not copied into a pending-subject list and do not stall a cursor.

## 5. Workspace and write scope

Task scope is frozen into the Attempt:

```text
initial_paths   repository-relative write prefixes
forbidden_paths hard negative prefixes
```

Empty `initial_paths` means the repository root. Normalize separators and reject
absolute paths, traversal, empty segments and `.git`. Prefix overlap uses path
segments: `src/a` overlaps `src/a/file.rs`, not `src/ab`.

The workspace lease is the only write authority. It is bound to manager, Task,
revision, Attempt, Operation, worktree and branch. Two active overlapping write
leases in one project cannot launch automatically. Review-only work needs no write
lease.

The old `code.scope.propose/accept/override/expiry` product is not part of the target.
Paths/symbols/interfaces in coordination cards may remain descriptive until that
product is deleted, but they do not grant write authority.

## 6. Compact TaskPrompt

The agent receives only current actionable context:

- objective and phase;
- requirements with review/check criteria;
- selected source excerpts/references;
- current write/forbidden paths;
- exact worktree, branch and baseline;
- route/model/effort actually selected;
- manager identity and result/submission contract;
- directly relevant pending request or dependency.

Do not inject the full Issue thread, audit archive, global queue, all participants,
raw Store JSON, proof digests, complete Git history or every MCP schema.

## 7. Route conditions

Route/provider conditions are independent of Task state:

| Condition | Automatic dispatch |
| --- | --- |
| available | may admit subject to capacity/write scope |
| rate limited | hold until documented retry time |
| overloaded | temporary hold |
| quota exhausted | route unavailable until reset/new fact or configured fallback |
| model gone | route unavailable; never silently choose another model |
| auth/data policy required | route unavailable pending explicit repair |
| stale/unknown coverage | hold; manual launch requires explicit recorded decision |

Host resource limits, route root limits and account quota are separate facts. One
shared provider service/account fact is not multiplied per hosted session.

## 8. Observation after launch

The Governor view correlates:

```text
Task/Attempt/Operation/binding
native session/turn/child and provider source cursor
adapter/shared-service PID + OS birth identity
worktree, actual branch/HEAD/dirty state, upstream/push target
last independently observed GitHub ref
activity, pending attention, connection, process and quota facts
```

A live PID does not prove active model work. Recent native activity does not prove a
healthy control connection. An unchanged branch does not prove idleness. Unknown
association stays visible.

## 9. Methods kept small

Required high-level surfaces are existing or direct projections:

```text
swarm.queue.get
swarm.launch                 # direct admission
swarm.launch.preview         # optional read only
agent.list / agent.state / agent.family / agent.usage
agent.send / reply / configure / goal / cancel / recover / result / reconcile
operation.get
review.assign / review.submit / task.request_changes / task.accept
```

A compact mailbox/consult action may remain where it has current users. The launcher
does not require council, contract ratification, integration ACK, advisory code-scope
approval, durable watches per question or automatic Issue-label publication.

## 10. Completion cases

- a returned correction ranks before unrelated new work;
- 128 blocked Tasks do not hide a later eligible Task;
- two dispatch wakes cannot create two launch Operations for one current Attempt;
- disabling a lane/policy admits no new work and cancels none;
- overlapping write scopes block before workspace/native effects;
- disjoint scopes can run concurrently;
- route quota/model/auth states are reflected without changing Task state;
- direct launch works without preview/proof/MCP-readiness gate;
- the TaskPrompt remains compact;
- local Governor observation works without provider requests.
