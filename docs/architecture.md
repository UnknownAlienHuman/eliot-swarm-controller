# Architecture

This page is the map. Exact formats live in [spec/](spec/); build order lives in the
[roadmap](roadmap.md). Rationale lives in [decisions](decisions.md).

## Picture

```text
 owner / manager agent / scripts                         vendor harnesses
 ───────────────────────────────                         ────────────────
  eliot CLI ──┐                                      ┌─► codex app-server
  eliot mcp ──┼─ local IPC (JSON-RPC) ─┐             │   claude SDK / stream-json
  hook shim ──┘                        │             │   opencode serve
                                       ▼             │   agy / muse / acp ...
 ┌───────────────────────── eliot daemon ────────────┼───────────────────┐
 │                                                   │                   │
 │ API ─► Router ─► Plugin host ──stdio JSON-RPC──► plugin processes ───┘
 │          │             ▲              per-agent when they own native processes;
 │          ▼             │ events       shared only for isolated native services
 │   ┌──────────── Bus (persisted log + live fan-out) ────────────────┐
 │   └──▲────────▲────────▲──────────▲────────────▲──────────▲──────┘
 │      │        │        │          │            │          │
 │   process    Git    transcripts  hooks      Automation  Coordination
 │   monitor   monitor  (when owner) intake    rules/timers mail,vars,
 │                                              goal loop   who,claims
 │
 │ Store: one SQLite file (WAL), one writer task
 └─────────────────────────────────────────────────────────────────────┘
          proxies / shared native servers = supervised or attached services
```

## Components

| Component | Responsibility | Not its job |
| --- | --- | --- |
| `eliot` CLI | Thin client: parse args, call daemon, print text or `--json`. Same binary also runs daemon, built-in plugins, hook shim and MCP. | Business logic. |
| Daemon | Own state, bus, plugin instances, custody, monitors, automation and coordination. One per data dir. | Model loops or harness internals. |
| Router | Validate authority/capability/state and exact generation/turn/request binding; persist commands; schedule ready commands; record outcomes. | Translating vendor calls. |
| Plugin host | Start/restart per-agent or shared plugin processes, enforce deadlines and process custody, route exact-generation messages. | Knowing a harness protocol. |
| Plugins | Translate semantic verbs, normalize native facts, retain native mapping needed for readback, report processes and effective settings. | Writing Eliot's Store or retrying unknown effects. |
| Bus | Ordered persisted events plus bounded live ephemeral fan-out. Subscribers replay from `seq` and then follow live. | Deciding workflow. |
| Monitors | Process, Git, filesystem, transcript, hook and source health. Each owns a distinct fact family. | Commanding agents or duplicating plugin lifecycle events. |
| Automation | Rules, timers, scripts, approval policy and Eliot Goal loop. It admits ordinary commands/actions and keeps only a small crash-safe run/action journal. | A Task/review/acceptance engine. |
| Coordination | Mailbox, variables, `who`, advisory claims. | Locks or sandboxing. |
| Store | SQLite schema/migrations, one writer with group commit, read pool. | Hashing documents or proving policy. |

## Core rules

1. **One semantic language.** Callers use [verbs](spec/core.md#verbs). Plugins
   translate. Unsupported verbs are rejected before native I/O.
2. **Events say what happened; tables say what is.** State and its source event commit
   together. Views are queries, not parallel state machines.
3. **No uncertain effect is replayed.** `unknown` is settled by exact readback or an
   explicit owner resolution. This includes `stop` and `interrupt`.
4. **Unknown is not zero.** Silence may mean stalled; a source gap is a gap; a process
   disappearance may mean lost. None means idle or complete.
5. **Every native effect is scoped.** Agent generation, active turn and pending request
   ids are first-class. Old tokens/events/commands cannot act on a replacement
   session.
6. **One owner per event family.** A stream, transcript or hook may own lifecycle for a
   generation, but two of them never publish the same normalized event. Hand-off uses
   a cursor/watermark and exposes gaps.
7. **Native first.** Use native goals, hooks, subagents, permissions and resume where
   their semantics match. Eliot fills missing capabilities explicitly.
8. **Automation uses the same router.** People, rules, timers and goals submit the same
   commands. External actions additionally use the automation action journal so a
   crash never implies a blind repeat.
9. **Plugins own vendor details; core owns logical identity/history.** Core never
   parses vendor payloads; plugins never mutate Store directly.
10. **Routes are explicit.** Eliot does not silently change provider, model, effort,
    permissions or billing path. Fallback is an owner-written rule.

## Process and lifetime model

Two plugin topologies are intentional:

- **Per-agent plugin.** Used when the plugin launches a native process whose tools
  inherit process environment, or when force-stop must kill one exact tree. The daemon
  puts the plugin and descendants in an agent-owned Windows Job / Unix process group.
  Codex stdio, Claude CLI/SDK and Antigravity start here.
- **Shared plugin.** Used when a native server already isolates sessions or for the
  fake harness. It may multiplex many agents, but stopping one agent must be an exact
  native session operation. The daemon never kills the shared plugin/service for one
  agent.

Consequences:

- `eliot daemon` starts plugin instances on demand. Per-agent instances are cheap
  controller processes; native harness processes dominate memory.
- A per-agent plugin exit causes its owned process group to be terminated and verified
  before the generation becomes `lost`. A shared-plugin crash re-attaches exact live
  sessions where possible; absence becomes `lost`.
- Native stdio sessions are daemon-owned in early layers. `daemon stop --drain`
  rejects new work, keeps approvals/answers/results available, waits for active turns,
  then closes their groups. `--force` terminates them.
- Service mode (L7.6) runs supported native servers outside kill-on-close custody so a
  daemon upgrade can re-attach without interrupting turns. Attached owner-run services
  are never stopped by Eliot.
- Resume is explicit, increments generation and rotates the routing token. It never
  resends the old prompt.
- Upgrades install into versioned paths; Windows executables/scripts in use are never
  overwritten. External plugin upgrade restarts only instances of that plugin.
- Managed services are refcounted; attached external services are observed, not owned.

## Command flow

```text
eliot steer a12 "use the v2 API"
 → command.submit {id, target:a12, verb:steer, args}
 → router reads a12 generation 4 and active turn t9; validates control + capability
 → transaction: command.accepted {generation:4, turn_id:t9}
 → scheduler dispatches when its lane is ready
 → plugin: agent.command {a12, generation:4, command_id, expected_turn_id:t9, steer}
 → native: turn/steer {threadId, expectedTurnId:t9, input}
 → plugin reply applied
 → transaction: command.applied + event
 → bus → subscribers/rules
```

A mismatched generation/turn is rejected before native I/O. Timeout or a lost reply is
`unknown`; no automatic retry. A later snapshot may settle only that exact binding.

Queued sends are future-turn requests. They remain FIFO within their generation but do
not block steering/answering/interrupting the turn already running or a configuration
that can apply immediately. Resume fails old queued sends as `stale_generation`.

## Event flow

```text
native turn/completed(t9)
 → authoritative plugin stream normalizes
   {agent:a12, generation:4, native_id:t9, kind:turn.completed, data}
 → daemon validates source owner + generation + active turn
 → transaction: persist event, busy→idle, clear active turn, derived agent.idle
 → bus → rules / goal loop / clients
```

A duplicate source, old generation or wrong turn never mutates current state or fires
normal rules. A source that cannot bridge a reconnect records a gap instead of
replaying history or fabricating completion.

## Store failure boundary

No external effect starts before command/action admission commits. If a later result
cannot commit (disk full, locked file), the writer retains the same pending result,
blocks new writes and exposes in-memory health. It does not repeat the native effect
and cannot falsely claim that the failing database persisted its own failure event.
After recovery it commits the pending result and one bounded outage/recovery record.

## Scale model

Scale comes from bounded records and shared services where semantics permit them:

- An idle agent is a row plus a small indexed in-memory record; no task/thread per idle
  agent.
- Fake and service-backed shared plugins multiplex thousands of agents. Process-owned
  live harnesses use one small plugin process per agent so custody remains exact.
- Writes use bounded group commit. Token deltas and CPU samples are ephemeral.
- Lists are paginated; subscriptions are filtered server-side with bounded buffers.
- Parent/root indexes answer tree operations without O(all agents) scans.
- No timer, rule or liveness loop scans every agent; due queues and indexed queries
  drive work.

A native coding harness commonly consumes far more memory than Eliot, so one machine
runs tens to a few hundred live model sessions. The same controller can retain and
simulate far more logical agents. Multi-host scale belongs to L9, not a premature
cluster layer.

## Repository layout (target)

```text
Cargo.toml
crates/
  eliot-core/       ids, events, verbs, command bindings, pure reducer
  eliot-store/      SQLite schema, migrations, writer task, queries
  eliot-daemon/     router, bus, plugin host, custody, monitors, automation, coordination
  eliot-plugin/     Rust plugin SDK, protocol, record/replay
  eliot/            CLI + daemon + built-in plugin/hook/MCP entry points
plugins/
  fake/             deterministic shared simulated harness
  codex/            first real harness; per-agent plugin process
  claude/ opencode/ ...
docs/
```

Crates are split only at real boundaries. Do not add crates, queues or services for
future use.
