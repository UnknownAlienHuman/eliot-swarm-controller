# Architecture

This is the map. Exact contracts live in `docs/spec/`; work order is the
[roadmap](roadmap.md); rationale is [decisions](decisions.md).

```text
owner / agents / scoped automation
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
│ guardian  monitor     mail/vars/who       activations/timers/goals    │
└───────────────────────────────────────────────────────────────────────┘
```

## Boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | request syntax/output, external WriteId and principal credential forwarding | business state or process-based authority inference |
| Router | principal/capability/state, exact bindings, immutable admission, selector fanout | vendor translation |
| Store | ids/receipts, canonical native subject, commands/events/source ownership, input/turn/edit projections, activation/action state | native sessions |
| Plugin host | instances, deadlines, source commit acknowledgements, custody and attach | harness semantics |
| Plugin | one harness protocol, native ids/readback/source normalization | Store writes, source keys or unknown retry |
| OS/Git/fs monitors | process lifecycle, workspaces, Git facts and source health | API principal selection or workflow |
| Coordination | mail/vars/claims/current-work view | locks or hidden model turns |
| Automation | scoped principals, persisted definitions/activations, cursors, frozen actions and goals | Task/review engine |

## Core invariants

1. One semantic command language; unsupported semantics fail before native I/O.
2. Every connected caller presents one explicit principal credential. Owner mode reads a
   private owner token; agents and automation use scoped tokens. Missing credentials
   reject—owner authority is never inferred from a missing token or PID lookup.
3. Every external mutation has a ULID receipt; internal linked/selector/automation
   effects have bounded derived ids and no fake receipt.
4. Unknown is never replayed automatically.
5. One canonical Agent row represents one native subject.
6. Native control and direct routing are separate: a child may be controllable through
   its parent plugin while having no independently injectable routing token.
7. Session start and optional first input are separate linked outcomes. Async inbox
   acceptance is not send applied; send applied is not turn completion.
8. Generation/turn/request bindings fence replacement work. Unknown turn blocks more
   turn work until exact readback or constrained owner resolution.
9. Authoritative source epochs open before the first native start/input effect.
   Persisted facts use contiguous commit-ack sequence, one family owner and compact
   native-event receipts that survive event pruning.
10. Source replacement is one atomic handoff naming predecessor and exact cursor/gap
    boundary; two sources never concurrently own one family.
11. Shared service PIDs belong to service custody, never an Agent. OS monitor alone owns
    process exit/stats.
12. Rules use stored event-time subject. Every semantic install has a fresh activation
    id and persisted compiled definition; A→B→A is three activations.
13. Event edits hand off at captured high-water. Deterministic firing error advances
    cursor as failed run; only Store failure retries.
14. Goal continuation uses a durable eligibility key or cadence slot; restart cannot
    duplicate the same round or suppress a genuinely new eligible state.
15. Selector fanout is one bounded atomic snapshot, paginated afterward; plugin
    discovery/snapshots are also paginated below the frame limit.
16. Worktree base ref resolves to immutable local OID before admission.
17. Restart is level-reconciled from Store: sources, rules, timers, goals, mail,
    workspaces, launch queues and provider holds.
18. Custody claims only OS-proven capability; degraded Unix never advertises exact
    force/owner-death cleanup.
19. Minimal environment and project overlays only narrow owner-defined global profiles;
    secrets resolve only after authority validation and never persist in semantic data.
20. Receipt retention is a minimum window; referenced receipts live as long as retained
    domain rows require them.
21. Protocol and Store payloads have explicit bounds. Large immutable inputs travel by
    artifact reference, not an oversized JSON frame.
22. Views query projections/events; they are not another state machine.

## Launch flow

```text
eliot --owner launch --worktree feat/x --profile codex "do X"
  → one transaction:
      external receipt + launch command L
      agent a12 generation 0, no routing token
      derived linked send S
      frozen profile binding + base_oid workspace journal
  → create/read back worktree at base_oid; run setup once
  → atomically reserve profile/account/proxy capacity and dispatch L
  → a12 generation 1, increment token_version; enable/hash routing only when
    this session can receive its own environment
  → start per-agent plugin / bind shared plugin session
  → plugin source.open(open_id, families) before native start/input
  → plugin establishes native subject and effective settings
  → commit-ack session.ready applies L and makes a12 idle
  → S dispatches separately; input association and terminal turn settle separately
```

A `start_requires_input` harness opens provisional source epochs first, then performs one
linked start+input native operation. It still reports session and input outcomes
independently. Resume expects g/targets g+1, keeps canonical subject, moves the row only
at dispatch and pauses old goals.

## Attach and native children

Observe-only attach uses a synthetic `observe:<harness>` profile, no routing token and
no launch capacity claim unless the owner supplies an account observation. Control
attach requires an allowed frozen profile, exact native attach/readback, environment
binding where tools need identity, and a capacity reservation representing the already
live session. Existing usage may make the scope overcommitted; it is counted and blocks
new starts rather than being ignored.

An already-created native child is always recorded when its canonical subject is known.
Controller launch/depth/tree limits prevent new Eliot-created fanout; they do not erase
native reality. A child beyond a guard remains observable/native-controllable where
supported but receives no project authority, automatic wake or direct routing token.

## Command fanout and event flow

A concrete command captures expected/target generation plus exact turn/request in one
transaction. Queued future sends do not block current control/answers.

A selector command is a Store-only external parent. The transaction sorts concrete
AgentIds, checks the hard limit, creates derived children and records count/digest.
Parent applied means expansion committed, not child effects. Child list/outcomes are
paginated; retrying the same WriteId returns the same fanout.

For native facts:

```text
plugin source.open(stable open_id, family set)     # before native effect
  → daemon source_key + watermark
plugin event.put/process.put(source_key, next source_seq, envelope)
  → source/family/binding/native-id validation
  → one transaction: source watermark + native receipt + event/projection/reducer
  → commit acknowledgement
```

A lost acknowledgement resends the same sequence. A vendor replay with the same native
id returns the original Store sequence even after the event row was pruned. Sources
without stable replay identity cannot claim lossless recovery: they close with a visible
gap and publish only facts after a proven new boundary.

Planned source replacement is atomic: the new open request names the old source,
expected watermark and exact cursor/gap boundary; the transaction closes/gaps the old
source and transfers family ownership to the new source.

## Automation flow

A rule file produces canonical definition digest and fresh immutable activation id.
Store persists compiled definition/artifacts for active/retiring/replay intervals.
Enabled edit captures H: old drains through H; new starts after H. Reverting bytes
creates another activation, never resets an old cursor.

A firing creates generic run identity `(principal,kind,owner activation,trigger)`,
freezes concrete targets and derived commands/actions. Deterministic rendering/authority
failure is one failed run with cursor advance. Rate limits derive from durable rows.

For Eliot Goal, `on_idle` trigger is the latest committed eligibility sequence; cadence
uses the exact anchored slot. Eligibility changes when generation/state, pending
request, input/turn gate, native-goal state or goal revision changes. The same eligible
state therefore has one round, while a real change can create the next.

## Principal and process topology

The private owner token is an attribution credential inside the one-OS-user trust
boundary, not multi-user security. `eliot --owner` reads it explicitly; a scoped agent or
automation context never falls back to owner when its credential is missing.

- Windows exact: no-breakaway Job per process owner; daemon-owned Jobs kill on close.
- Linux exact: delegated cgroup/equivalent plus monitored owner-death guardian.
- Unix fallback: degraded process group; no exact force/complete descendant proof.
- Shared control: per-session environment/identity/readback/isolated stop required.
- Attached owner services are never killed; managed services have separate custody.

## Store and recovery

One writer commits bounded batches. Reads are query-only, paginated and release
transactions. WAL/readers are monitored; hard pressure rejects new work/long reads while
preserving stop/request/result commits.

Restart:

- dispatched effects remain unknown until exact readback;
- sessions/sources reattach by canonical subject/open id/watermark;
- active/retiring/replay definitions and cursors resume from Store;
- timers/goals/mail/workspace/launch/provider queues reconcile;
- Git facts rebuild while edit/input/turn projections survive event pruning;
- missing custody settles only under source-drain/session rules;
- routing-key mismatch blocks new agent-token issue, not owner readback/stop.

## Scale

Idle/historical agents are indexed rows, not tasks. Qualified shared services multiplex;
process-owning harnesses use one small plugin/custody unit per live agent. Lists,
discovery, fanouts, histories and replay are paginated/bounded; hot paths use indexes and
due queues.

One host realistically runs tens to a few hundred native sessions. 10,000 fake agents
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
