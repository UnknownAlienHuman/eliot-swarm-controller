# Architecture

This is the map. Exact contracts live in `docs/spec/`; work order is the
[roadmap](roadmap.md); rationale is [decisions](decisions.md).

```text
owner / agents / scoped automation
                │ local JSON-RPC
                ▼
┌──────────────────────────── Eliot daemon ─────────────────────────────┐
│ API → Router → readiness lanes → Plugin host → native plugins/services│
│  │       │                         ▲                                   │
│  │       └─ commands/actions ──────┤ commit-ack event/process requests │
│  ▼                                 │                                   │
│ one SQLite WAL Store ↔ ordered persisted bus + bounded live fan-out  │
│  ▲          ▲             ▲                 ▲                         │
│ custody   Git/fs      Coordination        Automation                   │
│ guardian  monitor     mail/vars/who       cursors/timers/goals/scripts│
└───────────────────────────────────────────────────────────────────────┘
```

## Boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | request syntax/output/current identity forwarding | business state |
| Router | principal/capability/state, expected/target generation, turn/request binding, durable admission | vendor translation |
| Store | ids, canonical native subject, commands/events/turn/edit projections, cursors/timers/actions | native sessions |
| Plugin host | instances, deadlines, commit acknowledgements, custody and attach | harness semantics |
| Plugin | one harness protocol, native ids/readback/source normalization | Store writes or unknown retries |
| Monitors | source-owned process/Git/fs/transcript facts and workspace preparation | agent workflow |
| Coordination | durable mail/vars/claims/current edit view | locks or hidden model turns |
| Automation | scoped principals, event-time matching, trigger cursors, frozen actions and goal rounds | Task/review engine |

## Core invariants

1. One semantic language; unsupported semantics fail before native I/O.
2. Every effect has a stable Eliot id and exact generation plus turn/request where
   relevant.
3. Unknown is never replayed automatically.
4. One canonical Agent row represents one native subject.
5. Persisted plugin facts are commit-acknowledged and Store-deduplicated by stable
   native event id.
6. One source owns each lifecycle family; stale/duplicate/wrong-turn input cannot fire
   ordinary automation.
7. Session start and optional first prompt are separate linked command outcomes.
8. Rules match the event-time subject snapshot and run under global/project/goal
   principals; delayed actions freeze concrete targets.
9. Event cursors, timers, goals, unread mail, workspace/launch queues and provider
   holds reconcile from durable current state after restart.
10. Custody claims only what the OS backend proves; degraded Unix never advertises
    exact force/owner-death cleanup.
11. Environment is minimal by default; account/route/secrets are explicit.
12. Views are queries over projections/events, not parallel state machines.

## Start flow

```text
eliot launch --worktree feat/x --profile codex "do X"
  → transaction:
      agent a12 generation 0, workspace_state=preparing
      launch command L targets generation 1
      linked send S targets generation 1
  → journal/read back worktree + setup; workspace_state=ready
  → atomically reserve profile/account/proxy capacity and dispatch L
  → transaction: a12 generation 1, new token hash, state starting
  → plugin establishes native subject
  → plugin event.put(session.ready) commits and is acknowledged
  → L applied; a12 idle; S dispatches separately
  → turn start/completion update events + compact turns projection
```

If the harness requires input to expose a native subject, `agent.start` may carry S,
but session and input status remain separate. Ready never proves prompt admission.
Resume expects terminal generation g, targets g+1, retains the same canonical subject,
and moves the row/rotates token only when dispatch commits. Old queued work is stale;
goals pause until explicitly rebound.

## Command/event flow

A current-turn command captures generation and exact turn/request in one admission
transaction. Ready native calls serialize; queued future sends do not block current
controls/answers. Exact reply/readback settles applied/failed/unknown.

For persisted native facts, plugin sends `event.put` and advances its native cursor or
unacknowledged spool only after Store commit reply. Lost acknowledgement resends the
same native id and Store returns duplicate. Event transaction updates reducer and
turn/edit/current projections before bus fan-out. Unbridgeable source loss becomes a
gap, never invented completion.

## Process/service topology

- Windows exact: per-agent no-breakaway Job, kill-on-close.
- Linux exact: delegated cgroup/equivalent plus monitored owner-death guardian.
- Linux/other Unix fallback: degraded process group; no exact force guarantee.
- Shared control: only with per-session environment/tool attribution, canonical
  identity, attach/readback and isolated stop.
- Attached owner services are never killed; managed services have separate custody.

## Store/recovery

One writer commits bounded batches. Read connections are query-only, paginated and
released between pages. WAL/readers are monitored; hard pressure rejects new long reads
rather than deleting evidence or blind-truncating.

Restart recovery is level-based:

- dispatched effects remain unknown until exact readback;
- plugin/session/source ownership reattaches by canonical subject;
- event rules scan from durable cursors over stored event-time subjects;
- timers/goals/mail/workspace/launch/provider queues reconcile rows;
- Git facts rebuild from Git while edit/turn projections survive retention;
- missing custody becomes lost only after authoritative-source drain rules.

Project rules/scripts use project principal and cannot escape to another project,
global variables, plugin/service configuration or unallowlisted webhooks. Global rules
are explicit owner configuration.

## Scale

Idle/historical agents are indexed rows, not tasks. Fake and qualified shared services
can multiplex many sessions; process-owning harnesses use one small plugin/custody unit
per live agent. Lists/replays are paginated, subscriptions bounded and hot paths use
indexes/due queues rather than O(all agents) scans.

One host realistically runs tens to a few hundred native model sessions; 10,000 fake
agents test controller bookkeeping. Multi-host is a later owner decision.

## Target layout

```text
crates/
  eliot-core/      pure ids/reducer/commands
  eliot-store/     SQLite allocation/migrations/projections/queries
  eliot-daemon/    router, bus, custody, monitors, automation, coordination
  eliot-plugin/    protocol + recording/replay SDK
  eliot/           CLI/daemon/plugin/guardian/hook/MCP entry points
plugins/
  fake/ codex/ claude/ opencode/ ...
docs/
```

Add a crate/trait/service only after two current consumers require the boundary.
