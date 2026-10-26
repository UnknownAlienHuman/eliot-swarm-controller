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
│  │       └─ commands/actions ──────┤ commit-ack source requests        │
│  ▼                                 │                                   │
│ one SQLite WAL Store ↔ ordered persisted bus + bounded live fan-out  │
│  ▲          ▲             ▲                 ▲                         │
│ custody   Git/fs      Coordination        Automation                   │
│ guardian  monitor     mail/vars/who       activations/timers/goals    │
└───────────────────────────────────────────────────────────────────────┘
```

## Boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | request syntax/output, external WriteId, identity forwarding, peer classification | business state |
| Router | principal/capability/state, exact bindings, immutable admission, selector fanout | vendor translation |
| Store | ids/receipts, canonical native subject, commands/events/source ownership, input/turn/edit projections, activation/action state | native sessions |
| Plugin host | instances, deadlines, source commit acknowledgements, custody and attach | harness semantics |
| Plugin | one harness protocol, native ids/readback/source normalization | Store writes, source keys or unknown retry |
| OS/Git/fs monitors | process lifecycle, workspaces, Git facts and source health | workflow |
| Coordination | mail/vars/claims/current-work view | locks or hidden model turns |
| Automation | scoped principals, persisted rule definitions/activations, cursors, frozen actions and goals | Task/review engine |

## Core invariants

1. One semantic command language; unsupported semantics fail before native I/O.
2. Every external mutation has ULID receipt; internal linked/selector/automation effects
   have bounded derived ids and no fake receipt.
3. Unknown is never replayed automatically.
4. One canonical Agent row represents one native subject.
5. Session start and optional first input are separate linked outcomes; async inbox
   acceptance is not send applied, and send applied is not turn completion.
6. Generation/turn/request bindings fence replacement work. Unknown turn blocks more
   turn work until exact readback or constrained owner resolution.
7. Current routing token is re-derivable from private routing key + token version while
   Store keeps only hash; terminal/detach revokes without rewriting generation.
8. Persisted plugin facts use stable `source.open`, one current owner per family and
   contiguous commit-ack sequence. Lost acknowledgement is idempotent after event
   retention.
9. Shared service PIDs belong to service custody, never an Agent. OS monitor alone owns
   process exit/stats.
10. Rules use stored event-time subject. Each install/reload has fresh activation id and
    persisted compiled definition; A→B→A is three activations.
11. Event edits hand off at captured high-water. Deterministic firing error advances
    cursor as failed run; only Store failure retries.
12. Selector fanout is one bounded atomic snapshot, paginated afterward; no partial
    broadcast.
13. Worktree base ref resolves to immutable local OID before admission.
14. Restart is level-reconciled from Store: sources, rules, timers, goals, mail,
    workspaces, launch queues and provider holds.
15. No-token owner fallback needs live birth-pinned proof peer is outside every custody.
16. Custody claims only OS-proven capability; degraded Unix never advertises exact
    force/owner-death cleanup.
17. Minimal environment/default project allowlists; secrets resolve only after authority
    validation and never persist in semantic payloads.
18. Receipt retention is a minimum window; referenced receipts live as long as retained
    domain rows require them.
19. Views query projections/events; they are not another state machine.

## Launch flow

```text
eliot launch --worktree feat/x --profile codex "do X"
  → one transaction:
      external receipt + launch command L
      agent a12 generation 0, no token
      derived linked send S
      frozen profile binding + base_oid workspace journal
  → create/read back worktree at base_oid; run setup once
  → atomically reserve profile/account/proxy capacity and dispatch L
  → a12 generation 1, increment token_version, hash derived token, starting
  → plugin establishes native subject
  → source.open(open_id, families) commits source key/family owners
  → commit-ack session.ready applies L and makes a12 idle
  → S dispatches separately; input association and terminal turn settle separately
```

A `start_requires_input` harness may carry S inside start but still reports session/input
outcomes independently. Resume expects g/targets g+1, keeps canonical subject, moves row
only at dispatch and pauses old goals. Detach preserves independently owned session,
revokes token and releases claims.

## Command fanout and event flow

Concrete command captures expected/target generation plus exact turn/request in one
transaction. Queued future sends do not block current control/answers.

Selector command is a Store-only external parent. Transaction sorts concrete AgentIds,
checks hard limit, creates derived children and records count/digest. Parent applied
means expansion committed, not child effects. Child list/outcomes are paginated; retry
same WriteId returns same fanout.

For native facts:

```text
plugin source.open(stable open_id)
  → daemon source_key + current watermark
plugin event.put/process.put(source_key, next source_seq, envelope)
  → source/family/binding validation
  → one transaction: watermark + event/projection/reducer
  → commit acknowledgement
```

One source owns each family. Vendor duplicate may advance source sequence but cannot
fire lifecycle twice. Gap/hole is explicit. Per-agent `process.put` registers verified
owned descendant; service processes are registered under service custody, and OS
monitor publishes exits/stats.

## Automation flow

Rule file produces canonical definition digest and fresh immutable activation id. Store
persists compiled definition for active/retiring/replay intervals. Enabled edit captures
H: old activation drains through H; new starts after H. Reverting bytes creates another
activation, never resets old cursor.

A firing creates generic run identity `(principal,kind,owner activation,trigger)`,
freezes concrete targets and derived commands/actions. Deterministic rendering/authority
failure is one failed run with cursor advance. Rate limits derive from durable rows, so
restart does not reset them.

## Caller identity and process topology

Local IPC pins peer birth and checks live membership against every Job/cgroup/process
group before no-token owner fallback. Process table is fast path only.

- Windows exact: no-breakaway Job per owner; daemon-owned Jobs kill on close.
- Linux exact: delegated cgroup/equivalent + monitored owner-death guardian.
- Unix fallback: degraded process group; no exact force/complete descendant proof.
- Shared control: per-session environment/identity/readback/isolated stop required.
- Attached owner services never killed; managed services separate custody.

## Store and recovery

One writer commits bounded batches. Reads are query-only, paginated and release
transactions. WAL/readers monitored; hard pressure rejects new work/long reads while
preserving stop/request/result commits.

Restart:

- dispatched effects remain unknown until exact readback;
- sessions/sources reattach by canonical subject/open id/watermark;
- active/retiring/replay definitions and cursors resume from Store;
- timers/goals/mail/workspace/launch/provider queues reconcile;
- Git facts rebuild while edit/input/turn projections survive event pruning;
- missing custody settles only under source-drain/session rules;
- routing-key mismatch blocks new token issue, not owner readback/stop.

## Scale

Idle/historical agents are indexed rows, not tasks. Qualified shared services multiplex;
process-owning harnesses use one small plugin/custody unit per live agent. Lists,
fanouts, histories and replays are paginated/bounded; hot paths use indexes/due queues.

One host realistically runs tens to a few hundred native sessions. 10,000 fake agents
test bookkeeping. Multi-host is later decision.

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

Add crate/trait/service only after two current consumers require boundary.
