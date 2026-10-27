# Architecture

This page is the map. Exact contracts live in [spec/](spec/), build order in the
[roadmap](roadmap.md), donor provenance in [reference/donors](reference/donors.md).

## Picture

```text
 owner / manager Agent / scoped scripts                    vendor harnesses
 ──────────────────────────────────────                    ────────────────
  eliot CLI ──┐                                         ┌─► Codex app-server
  eliot MCP ──┼─ current-user IPC ────────┐             │   Claude SDK / stream-json
  hook shim ──┘                            │             │   OpenCode / ACP / ...
                                           ▼             │
 ┌──────────────────────────── Eliot daemon ─────────────┼────────────────┐
 │ API → Router → readiness lanes → Plugin host ───────► plugins/native  │
 │  │       │                              ▲             │                │
 │  │       ▼                              │ commit-ack  │                │
 │  │  commands + narrow gates             │ events      │                │
 │  ▼                                      │             │                │
 │ one SQLite WAL Store ↔ persisted bus + bounded live fan-out          │
 │  ▲           ▲              ▲                    ▲                    │
 │ custody    Git/fs      Coordination           Automation               │
 │ monitor    monitor     mail/vars/who          rules/timers/Goals      │
 └───────────────────────────────────────────────────────────────────────┘
              managed or adopted provider/native services
```

## Donor-backed boundaries

| Boundary | Eliot shape | Proven pattern |
| --- | --- | --- |
| logical vs live identity | stable Agent/native subject + one monotonic `generation` | Agent Sessions fence token; sandbox-driver plugin generation |
| turn lifecycle | queue/admission, actual turn start and terminal result are separate | ACPX shared turns/watch |
| event history | one family owner; live stream immediate, cursor history authoritative; gaps explicit | Paseo timeline; ACPX watch; Waku OpenCode stream |
| state | one persisted event + pure reducer + compact projections | Petri; Agent Sessions event log |
| restart | deduplicated reconciliation keys reread durable/native current state | controller-runtime |
| mail | stable delivery id, sender generation, exact recipients/digest, separate ack | CCCC; MCP Agent Mail |
| plugin compatibility | one semantic interface and one black-box conformance suite | sandbox-driver; Agent Sessions |

These are the only borrowed abstractions. Eliot does not import donor workflow graphs,
UIs, ledgers or proof systems.

## Components

| Component | Owns | Does not own |
| --- | --- | --- |
| CLI/API | syntax/output, write id and principal forwarding | product state or PID-based authority |
| Router | authority/capability/state, generation/turn/request binding, durable admission, readiness lanes | vendor translation |
| Store/bus | ordered events, commands, Agent/turn/source projections, subscriptions | native sessions |
| Plugin host | plugin generations, deadlines, commit acknowledgement and custody wiring | harness semantics |
| Plugin | one harness protocol, native ids/readback/normalization | SQLite or retrying unknown effects |
| Monitors | process, Git/fs, source freshness/gaps, workspace/provider projections | commanding Agents |
| Coordination | mail, vars, `who`, claims | locks or hidden model turns |
| Automation | rule cursors/handoff, timers, Goals; actions are commands | Task/review/workflow engine |

## Core invariants

1. **One semantic command language.** Unsupported semantics fail before native I/O.
2. **One stable Agent, one current generation.** Every Agent-scoped effect/fact carries
   generation; old-generation output cannot mutate replacement work.
3. **Start ≠ input ≠ turn result.** Each has its own command/fact/outcome.
4. **Unknown stays unknown.** No possible native/external effect is automatically
   replayed. Its narrow lane remains blocked until exact readback/resolution.
5. **One source owns each lifecycle family.** Replacement is one compare-and-swap
   handoff with a cursor boundary or an explicit stable gap; stream/transcript/hook never
   publish the same completion independently.
6. **Persist before publish/ack.** One native-record batch, daemon-computed digest,
   source state/cursor, reducer projection and derived events commit in one transaction;
   only then does plugin/client receive success.
7. **Events describe what happened; projections describe current state.** Views query
   them and are not another state machine.
8. **Automation uses ordinary commands.** A deterministic rule/timer/Goal key creates
   the same commands on repeated reconciliation.
9. **Routes and authority are explicit.** No silent provider/model/account/permission
   fallback; missing Agent/run identity never becomes owner.
10. **Custody claims only what the OS proves.** Windows Job exact; Unix process group
    degraded; adopted service never killed.

## Identity and lifetime

One `(native_scope,native_ref)` maps to one Agent row. `generation` fences who currently
controls/observes it:

- first launch/attach targets generation 1;
- resume, detach and a new/uncertain controller takeover target current generation + 1;
- the generation-changing command stores expected and target generation;
- its **dispatch transaction** advances the fence before native I/O, revokes the old
  controller/source/token, releases old claims, pauses Goals, stales old queued commands
  and commits the target source plan/state;
- if the target can receive its own Eliot environment, the same transaction creates the
  one random target-generation token that the plugin/native process will receive. It is
  never installed only after tools may already have started;
- exact success installs the requested control/observe/profile projection;
- definite failure never restores the revoked controller or decrements the fence:
  first start/resume becomes `failed`; takeover/detach becomes `lost` from Eliot's
  perspective, while the native subject may remain externally discoverable;
- unknown transition leaves the target generation current, preserves any token that may
  have reached the target environment, leaves the previous observation diagnostic-only
  and blocks ordinary work until exact readback/resolution;
- exact readback may clear the target token only when it proves no process/session could
  have received that environment;
- exact reconnect to the same still-current writer/source ownership may preserve the
  generation;
- exact turn/request ids bind work inside a generation.

This intentionally copies a generation fence, not a lease/binding hierarchy. A command,
source or process from an older generation can remain in history but cannot mutate the
current projection or fire ordinary automation. Recovery after failed/unknown takeover
is an explicit later attach/resume targeting another generation; there is no rollback.

Per-Agent plugins own one generation and custody unit. A shared plugin is allowed only
when a native service already isolates session identity/events/readback/stop. Its server
belongs to service custody; stopping one Agent never signals it.

A plugin crash or daemon restart never rebinds an uncertain call to a replacement
plugin generation. Recovery attaches/snapshots the exact native subject; absent proof
means unknown/lost, not replay.

## Command flow

```text
eliot steer a12 "use v2" --generation 4
 → admission transaction:
      validate principal, capability, Agent generation=4 and active turn=t9
      insert command accepted {generation:4, turn_id:t9}
 → readiness lane dispatches to plugin generation 4
 → native exact-turn mechanism
 → plugin result/event commits command outcome
```

A queued future send does not block current-turn steer/interrupt/request reply. But an
input that is dispatched/unknown without exact turn association blocks later send,
configure and Eliot Goal continuation.

Launch with a prompt creates `launch L` plus linked input `L/send`. Both are stored as
one command parent/child set. `session.ready` applies L only; exact `turn.started`
associates/applies `L/send`; terminal result settles the turn separately. Selector
fanout uses the same parent/derived-child representation and pages the fixed child set.

## Event/source flow

```text
native record R
 → the family owner normalizes one bounded atomic event batch
 → events.put {agent:a12, generation:4, source:stream, events, cursor?}
 → daemon validates source ownership + generation + turn/request/native ids
 → daemon computes canonical event digests
 → one transaction:
      events + source state/cursor + reducer/projections + derived state
 → commit acknowledgement
 → bus → clients/rules/Goals
```

Every durable event that may be resent after a lost acknowledgement has a stable
normalized native id. A cursor-backed source may derive ids from cursor + record-local
ordinal; a source with neither stable ids nor cursor records an idempotent gap instead of
retrying a lost acknowledgement. One native-record batch is all-new or an exact duplicate
as a whole; a mixed retry is integrity failure.

A replayable source resumes strictly after its opaque committed cursor. A source with no
exclusive cursor reconnects at the live edge, records `source.gap` with a stable gap id,
snapshots current state and never republishes old history. `native_id` removes only
bounded overlap; the same id with different daemon-computed payload/cursor is an
integrity error.

Dynamic family replacement is one `source.handoff` compare-and-swap transaction: verify
predecessor, record final cursor or gap, install successor. The successor publishes only
after commit. A lost response is settled by read-only `source.state`, not by blindly
issuing another handoff. There are no source epochs, open ids or source sequence protocol.

## Restart reconciliation

Restart/callbacks enqueue stable keys (`agent:<id>`, `workspace:<id>`, `service:<name>`,
`provider:<scope>`, rule/timer/Goal/mail/Git keys). A deduplicated worker rereads Store
and native current state; one key is never reconciled concurrently with itself.

- dispatched effects become unknown until verb-specific readback;
- no reconcile pass replays an uncertain effect;
- source owners/state determine recovery or gap;
- workspace/provider state prevents repeated setup/probe;
- rules drain old/new definitions at their stored high-water boundary;
- queued work runs only if its captured generation is still current;
- an open unknown generation transition blocks another takeover until exact readback.

## Process and service model

- one process-owning plugin + native descendants per Agent generation;
- Windows Job contains/terminates/verifies exact tree;
- Unix process group is degraded and reports possible escaped descendants;
- owner-run/adopted service registration is read-only and never stopped by Eliot;
- managed service has separate custody and lifecycle;
- process exit is followed by authoritative-source drain to EOF/deadline before loss is
  concluded.

## Scale

Idle/historical Agents are indexed rows, not tasks. Real live sessions cost what their
vendor harness costs; one host realistically runs tens/hundreds. Shared isolated fake/
service plugins can exercise 10,000 logical Agents. Lists, histories, discovery and
fanout are paginated; trees use parent/root indexes; no hot path scans every Agent.

## Target repository layout

```text
Cargo.toml
crates/
  eliot-core/       pure ids/events/verbs/reducer
  eliot-store/      SQLite migrations/writer/queries
  eliot-daemon/     router/bus/plugin host/custody/monitors/automation/coordination
  eliot-plugin/     typed plugin SDK + recording/replay
  eliot/            CLI/daemon/plugin/hook/MCP entries
plugins/
  fake/
  codex/
  claude/ opencode/ ...
docs/
```

Create another crate/trait/service only after two current callers require the boundary.
