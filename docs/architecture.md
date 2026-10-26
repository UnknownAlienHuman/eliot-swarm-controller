# Architecture

This is the map. Exact contracts live in `docs/spec/`; work order is the
[roadmap](roadmap.md); rationale is [decisions](decisions.md).

```text
owner / Agents / scoped automation
                │ explicit principal credential over local JSON-RPC
                ▼
┌──────────────────────────── Eliot daemon ─────────────────────────────┐
│ API → Router → readiness lanes → Plugin host → native plugins/services│
│  │       │                         ▲                                   │
│  │       └─ commands/actions ──────┤ commit-ack source requests        │
│  ▼                                 │                                   │
│ one SQLite WAL Store ↔ ordered persisted bus + bounded live fan-out  │
│  ▲          ▲             ▲                 ▲                         │
│ custody   Git/fs      Coordination        Automation                   │
│ guardian  monitor     mail/vars/who       activations/timers/Goals    │
└───────────────────────────────────────────────────────────────────────┘
```

## Boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | syntax/output, external WriteId and principal credential forwarding | business state or process-based authority inference |
| Router | authority/capability/state, exact generation/binding/turn/request, durable admission, selector fanout | vendor translation |
| Store | ids/receipts, canonical native subject, immutable binding revisions/current pointer, commands/events/source ownership and projections | native sessions |
| Plugin host | instances, deadlines, source commit acknowledgements, custody and attach | harness semantics or Store mutation |
| Plugin | one harness protocol, native ids/readback/source normalization | unknown retry or binding-pointer movement |
| OS/Git/fs monitors | process lifecycle, workspaces, Git facts and source health | API principal selection or workflow |
| Coordination | mail/vars/claims/current-work view | locks or hidden model turns |
| Automation | scoped principals, persisted activations/cursors/actions and Goals | Task/review engine |

## Core invariants

1. One semantic command language; unsupported semantics fail before native I/O.
2. Every connected caller presents one explicit principal credential. Missing credentials
   reject; owner authority is never inferred from a missing token or PID.
3. Every external mutation has a ULID receipt; internal linked/selector/automation
   effects have derived ids and no fake receipt.
4. Unknown is never replayed automatically.
5. One canonical Agent row represents one native subject.
6. Native generation and Eliot binding revision are distinct. Generation identifies the
   vendor incarnation; immutable binding revisions record observe/control/profile lease
   changes within it. One current pointer selects the active revision.
7. Every native command is scoped to expected and target
   `(agent,generation,binding_revision)`. Pointer movement stales queued old-revision
   commands but never rewrites historical readback.
8. Native control and direct routing are separate: parent-mediated child control may
   exist while `routing_enabled=false` and no child token exists.
9. Session start, first input and turn completion are separate outcomes. Inbox
   acceptance is not send applied; send applied is not completion.
10. Unknown turn blocks more work until exact readback/constrained resolution.
11. Sources open before the first native start/input effect. Durable facts use contiguous
    commit-ack sequence and vendor replay protection that survives event pruning.
12. Source replacement is one predecessor-aware atomic handoff or explicit gap.
13. Shared service PIDs belong to service custody, never an Agent; OS monitor owns
    process lifecycle.
14. Rules evaluate stored event-time subject. Each semantic install has a fresh
    activation and persisted compiled definition; A→B→A is three activations.
15. Rule edits hand off at captured high-water; deterministic firing error advances as a
    failed run, only Store failure retries.
16. Goal is bound to generation **and binding revision** and uses durable eligibility key
    or cadence slot. Binding change pauses it until explicit rebind.
17. Selector fanout and plugin discovery/history are bounded/paginated below frame caps.
18. Worktree base resolves to immutable local OID before admission.
19. Restart is level-reconciled from Store: sources, bindings, rules, timers, Goals,
    mail, workspaces, launch queues and holds.
20. Custody claims only OS-proven capability; degraded Unix never claims exact force or
    all descendants dead.
21. Project overlays only narrow owner-defined profiles; secrets resolve after authority
    validation and never persist in semantic data.
22. Receipt window is a minimum; referenced receipts outlive domain rows that need them.
23. Large immutable inputs use ArtifactRefs, not oversized JSON frames.
24. Views query projections/events; they are not another state machine.

## Launch flow

```text
eliot --owner launch --worktree feat/x --profile codex "do X"
  → transaction:
      external receipt + launch command L
      Agent a12 generation 0
      immutable target binding (g1,r1) + current pointer for g1
      derived linked send S bound to (g1,r1)
      frozen workspace base_oid/setup artifacts
  → create/read back worktree at base_oid; run setup once
  → reserve profile/account/proxy capacity and dispatch L
  → a12 generation 1; optional routing token; state starting
  → plugin opens source epochs before native session/input
  → native subject/effective settings established
  → commit-ack ready applies L and makes a12 idle
  → S dispatches; input association and terminal turn settle separately
```

A `start_requires_input` harness opens provisional sources first, then performs one
linked start+input operation. Session and input outcomes remain independent. Resume
expects old generation/current binding and targets `(g+1,r1)`.

## Attach and binding transitions

First observe attach creates an external Agent and observe binding revision 1. Pointer
moves to it only after exact read attach/readback.

Observe→control creates immutable candidate revision n+1 and an attach command that
expects current n. Until exact success:

- current pointer and Agent profile/routing remain observe;
- candidate capacity is reserved;
- failed/unknown native attach cannot partially upgrade the row.

Exact success atomically applies command, moves pointer, updates Agent projection,
installs/rotates routing token when qualified, converts capacity reservation and pauses
or rebinds Goals as specified. Detach similarly creates a candidate observe revision;
failed/unknown detach leaves control pointer unchanged. Recovery of the same current
controlled lease reuses its revision; it is not another revision.

All queued old-revision native commands become stale on pointer movement. Already
applied/dispatched/unknown commands and turns remain historical and are read back using
their stored revision.

An already-created native child is always recorded. Controller guards block new
Eliot-created fanout but do not erase native reality. Guard-exceeding child receives no
project authority/direct routing/automatic direct wake; safe parent-mediated native
verbs remain possible.

## Command fanout and event flow

Concrete command captures expected/target generation and binding revision plus exact
turn/request. Queued future sends do not block current controls in the same revision.

A selector command is Store-only external parent. Transaction sorts concrete AgentIds,
checks hard limit, captures each target's current generation/revision, creates derived
children and stores count/digest. Parent applied means expansion committed, not child
native effects. Children/results are paginated.

Native facts:

```text
plugin source.open(stable open_id, family set)      # before native effect
  → daemon source_key + watermark
plugin event.put/process.put(source_key, next source_seq, envelope)
  → source/family/generation/turn/request/native-id validation
  → transaction: watermark + native receipt + event/projection/reducer
  → commit acknowledgement
```

Lost acknowledgement resends same sequence. Vendor replay with same native identity
returns original Store seq even after event pruning. Sources without stable cursor/id
close with a visible gap.

Planned replacement atomically verifies predecessor watermark/cursor, closes/gaps old
source, creates new source and transfers family ownership.

## Automation flow

A rule file produces definition digest and fresh immutable activation id. Store keeps
compiled definition/artifacts for active/retiring/replay intervals. Enabled edit captures
H: old drains through H; new starts after H. Reverting bytes creates another activation.

Firing creates generic run identity, freezes target generation/binding/turn/request and
writes derived commands/actions. Deterministic render/authority failure is one failed run
with cursor advance. Rate limits derive from durable rows.

For Goal, `on_idle` trigger is latest committed eligibility sequence and includes current
binding pointer; cadence uses exact slot. Pointer movement pauses Goal and changes
eligibility. Same state has one round; a real generation/binding/turn/Goal change can
create the next.

## Principal and process topology

Private owner token is attribution inside one-OS-user trust boundary, not multi-user
security. `eliot --owner` reads it explicitly; Agent/automation never falls back to owner.

- Windows exact: no-breakaway Job per process owner; daemon-owned Jobs kill on close.
- Linux exact: delegated cgroup/equivalent + monitored owner-death guardian.
- Unix fallback: degraded process group; no exact force/complete descendant proof.
- Shared control: per-session environment/identity/readback/isolated stop required.
- Attached owner services are never killed; managed services have separate custody.

## Store and recovery

One writer commits bounded batches. Reads are query-only/paginated and release
transactions. WAL/readers are monitored; pressure rejects new work/long reads while
preserving stop/request/result commits.

Restart:

- dispatched effects remain unknown until exact readback;
- sessions/sources reattach by canonical subject/open id/watermark;
- current binding pointer and candidate attach commands determine control lease;
- active/retiring/replay definitions/cursors resume from Store;
- timers/Goals/mail/workspace/launch/provider queues reconcile;
- Git facts rebuild while edit/input/turn projections survive event pruning;
- missing custody settles only under source/session rules;
- routing-key mismatch blocks new token issue, not owner readback/stop.

## Scale

Idle/historical Agents are indexed rows, not tasks. Qualified shared services multiplex;
process-owning harnesses use one plugin/custody unit per live Agent. Lists, discovery,
fanouts, histories and replay are paginated; hot paths use indexes/due queues.

One host realistically runs tens to a few hundred native sessions. 10,000 fake Agents
test bookkeeping. Multi-host is a later decision.

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
