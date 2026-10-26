# Architecture

This is the map. Exact contracts live in `docs/spec/`; implementation order is the
[roadmap](roadmap.md); rationale is [decisions](decisions.md).

```text
owner / agents / scripts
        │ local JSON-RPC
        ▼
┌──────────────────────────── Eliot daemon ─────────────────────────────┐
│ API → Router → readiness lanes → Plugin host → native plugins/services│
│  │       │                         │                                   │
│  │       └── commands/actions ─────┘ normalized exact events           │
│  ▼                                                                  │
│ one SQLite WAL Store ↔ ordered persisted bus + bounded live fan-out  │
│  ▲          ▲             ▲               ▲                           │
│ process   Git/fs      Coordination      Automation                     │
│ custody   monitor     mail/vars/who     rules/timers/goals/scripts     │
└───────────────────────────────────────────────────────────────────────┘
```

## Boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | request syntax, output, current token forwarding | business logic |
| Router | authority/capability/state, exact expected/target generation, turn/request binding, durable admission | vendor translation |
| Store | ids, canonical native subject, commands/events/projections, cursors/timers/action state | native sessions |
| Plugin host | plugin instances, deadlines, exact/degraded custody and restart attach | harness semantics |
| Plugin | one harness protocol, native ids/readback/event normalization | Store writes or unknown retries |
| Monitors | source-owned process/Git/fs/transcript facts | agent control |
| Coordination | durable mail/vars/edit attribution/claims | locks or hidden model turns |
| Automation | durable trigger cursors, frozen actions and bounded goal rounds | Task/review workflow engine |

## Core invariants

1. One semantic command language; unsupported semantics fail before native I/O.
2. Every effect has a stable Eliot id and exact native scope: generation plus turn or
   request where relevant.
3. Unknown is never replayed automatically.
4. One canonical Agent row represents one native subject; observers subscribe to it.
5. One source owns each lifecycle family; duplicate/stale/wrong-turn input cannot fire
   normal rules.
6. Session start and optional first prompt are separate linked command outcomes.
7. Durable reactions are level-reconciled after restart: event cursors, timers, goals,
   unread mail, launch queues and provider holds.
8. Process custody claims only what the OS backend proves. Degraded Unix fallback does
   not advertise exact force/owner-death containment.
9. Environment is minimal by default; routes/accounts/secrets are explicit.
10. Views are queries over current projections/events, not parallel state machines.

## Logical identity and start flow

```text
eliot launch --profile codex "do X"
  → transaction:
      agent a12 generation 0, state created
      launch command L targets generation 1
      linked send S targets generation 1
  → capacity admits L
  → transaction: a12 generation 1, new token hash, state starting
  → per-agent plugin/native session start
  → session.ready(g1) commits; L = applied; a12 = idle
  → S dispatches and owns its own applied/unknown/turn result
```

If the harness requires input to reveal the session id, `agent.start` may carry S, but
plugin reports session and S separately. Ready never proves prompt admission.

Resume validates terminal generation `g`, reserves command target `g+1`, and moves the
row/rotates token only when dispatch commits. Old queued work becomes stale. Goals are
paused until explicitly rebound.

## Normal command flow

```text
steer a12
  → admission captures expected_generation=4, target_generation=4, turn=t9
  → ready lane serializes the native call
  → plugin validates g4/t9 before native I/O
  → exact reply/readback settles applied/failed/unknown
```

Queued sends are FIFO future-turn work but do not block exact control/answers for the
current turn. Selector ids use fixed derived ids and expand transactionally; durable
automation already stores concrete targets.

## Event flow

```text
native event
  → authoritative plugin/source
  → validate source owner, generation, active turn/request, unique native event id
  → transaction: event + projection/reducer + derived state event
  → bus → clients and durable rule-cursor reconciliation
```

Late/duplicate events may be diagnostic, never normal lifecycle. Process exit waits for
authoritative-source drain/readback; a fixed silence timer is not terminal evidence.

## Process topology

- Windows process-owning agents: per-agent Job, no breakaway, kill-on-close.
- Linux exact mode: per-agent cgroup/equivalent plus owner-death guardian.
- Linux/other Unix process-group fallback: degraded; no exact force guarantee.
- Shared controlling service: only with per-session environment, identity, attach and
  isolated stop. Otherwise run per-agent or observe-only.
- Attached owner services are never killed. Managed services have separate custody.

## Store and recovery

One writer commits bounded batches; read connections are query-only and paginated. WAL
size/checkpoints are monitored so a stuck reader cannot silently grow disk without
limit. Commit failure blocks new writes and retains the same pending result; external
causes are not replayed.

Restart recovery reads durable current state:

- dispatched effects → unknown until exact readback;
- event rules resume from cursor;
- timers/goals/mail/launch/provider queues reconcile current rows;
- plugin/service sessions re-attach by canonical native subject;
- Git state rebuilds from Git and durable edit projection;
- missing exact custody becomes lost only after source drain rules.

## Scale

Historical/idle agents are indexed rows, not tasks. Fake and qualified shared services
can multiplex many sessions; process-owning harnesses spend one small plugin/custody
unit per live agent. Lists/replays are paginated and subscriptions bounded. Hot paths
use due queues/indexes, not O(all agents) scans.

One host realistically runs tens to a few hundred native model sessions; 10,000 fake
agents test controller bookkeeping. Multi-host belongs to a later owner decision.

## Target layout

```text
crates/
  eliot-core/      pure ids/reducer/commands
  eliot-store/     SQLite allocation/migrations/queries
  eliot-daemon/    router, bus, custody, monitors, automation, coordination
  eliot-plugin/    protocol + recording/replay SDK
  eliot/           CLI/daemon/plugin/guardian/hook/MCP entry points
plugins/
  fake/ codex/ claude/ opencode/ ...
docs/
```

Add a crate/trait/service only after two current consumers require the boundary.
