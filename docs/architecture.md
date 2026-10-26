# Architecture

This is the map. Exact contracts live in `docs/spec/`; work order is the
[roadmap](roadmap.md); rationale is [decisions](decisions.md).

```text
owner / agents / scoped automation
                │ local JSON-RPC + live peer-custody classification
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
| CLI/API | request syntax/output/current identity forwarding and peer classification | business state |
| Router | principal/capability/state, expected/target generation, turn/request binding, durable admission | vendor translation |
| Store | ids, canonical native subject, commands/events/receipts/turn/edit projections, cursors/timers/actions | native sessions |
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
5. A controlled live generation has one routing-token hash; generation 0, terminal and
   observe-only/detached rows have none. Revocation never rewrites generation history.
6. Persisted plugin facts are commit-acknowledged. Stable native-event receipts survive
   event-row retention for the current generation's complete replay lifetime.
7. One source owns each lifecycle family; stale/duplicate/wrong-turn input cannot fire
   ordinary automation.
8. Session start and optional first prompt are separate linked command outcomes.
9. Rules match the stored post-transition event-time subject and run under
   global/project/goal principals; delayed actions freeze concrete targets.
10. A deterministic render/authority error creates one failed firing with no actions
    and advances its cursor. Only Store failure retries the same event.
11. Event cursors, timers, goals, unread mail, workspace/launch queues and provider
    holds reconcile from durable current state after restart.
12. No-token owner fallback requires a birth-pinned live OS check proving the peer is
    outside every current Eliot custody; missing projection or ambiguity rejects.
13. Custody claims only what the OS backend proves; degraded Unix never advertises
    exact force/owner-death cleanup.
14. Environment is minimal by default. Project script references pass both the global
    script allowlist and project allowlist before any value is read.
15. Views are queries over projections/events, not parallel state machines.

## Start flow

```text
eliot launch --worktree feat/x --profile codex "do X"
  → transaction:
      agent a12 generation 0, token_hash NULL, workspace_state=preparing
      launch command L targets generation 1
      linked send S targets generation 1
  → journal/read back worktree + setup; workspace_state=ready
  → atomically reserve profile/account/proxy capacity and dispatch L
  → transaction: a12 generation 1, new token hash, state starting
  → plugin establishes native subject
  → plugin event.put(session.ready) commits receipt + event/projection and is acknowledged
  → L applied; a12 idle; S dispatches separately
  → turn start/completion update events + compact input/turn projections
```

If the harness requires input to expose a native subject, `agent.start` may carry S,
but session and input status remain separate. Ready never proves prompt admission.
Resume expects terminal generation g, targets g+1, retains the same canonical subject,
and moves the row/rotates token only when dispatch commits. Old queued work is stale;
goals pause until explicitly rebound. Terminal or detach transition clears the current
token hash without resetting g.

## Command and event flow

A current-turn command captures generation and exact turn/request in one admission
transaction. Ready native calls serialize; queued future sends do not block current
controls/answers. Exact reply/readback settles applied/failed/unknown.

For persisted native facts, the plugin sends `event.put` and advances its native cursor
or unacknowledged spool only after Store commit reply. The Store commits a compact
native-event receipt atomically with the event/projections. A lost acknowledgement
resends the same native id and receives the original receipt even after the event row is
pruned. Receipt reclamation waits until the agent has advanced beyond that generation
and every old source/spool is closed. An unbridgeable source interval becomes a gap,
never invented completion.

Event rules scan persisted events by durable cursor. No match advances. A valid match
admits one immutable run/actions and advances. A deterministic render, secret or
authority error admits one failed run without actions and advances. A Store failure
rolls back the cursor and retries only after Store recovery.

## Caller identity and process topology

Before granting no-token owner identity, local IPC pins peer PID/birth and checks live
membership against every current Job/cgroup/process-group custody. The persisted process
projection is only a fast path; a projection miss is not proof of independence.
Ambiguous or unreadable membership returns `caller_identity_unknown`.

- Windows exact: per-owner no-breakaway Job; daemon-owned groups use kill-on-close.
- Linux exact: delegated cgroup/equivalent plus monitored owner-death guardian.
- Linux/other Unix fallback: degraded process group; no exact force guarantee.
- Shared control: only with per-session environment/tool attribution, canonical
  identity, attach/readback and isolated stop.
- Attached owner services are never killed; managed services have separate custody.

## Store and recovery

One writer commits bounded batches. Read connections are query-only, paginated and
released between pages. WAL/readers are monitored; hard pressure rejects new long reads
rather than deleting evidence or blind-truncating.

Restart recovery is level-based:

- dispatched effects remain unknown until exact readback;
- plugin/session/source ownership reattaches by canonical subject;
- event rules scan from durable cursors over stored event-time subjects;
- timers/goals/mail/workspace/launch/provider queues reconcile rows;
- Git facts rebuild from Git while edit/input/turn/native-receipt projections survive
  event retention;
- missing custody becomes lost only after authoritative-source drain rules.

Project rules/scripts use a project principal and cannot escape to another project,
global variables, plugin/service configuration, unallowlisted webhooks or environment
references outside global-script∩project allowlists. Global rules are explicit owner
configuration.

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
