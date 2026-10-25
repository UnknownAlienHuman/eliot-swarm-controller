# Architecture

This page is the map. Exact formats live in [spec/](spec/); build order lives in the
[roadmap](roadmap.md). Rationale for each choice is in [decisions](decisions.md).

## Picture

```text
 owner / manager agent / scripts                      vendor harnesses
 ───────────────────────────────                      ────────────────
  eliot CLI ──┐                                   ┌─► codex app-server
  eliot mcp ──┼─ local IPC (JSON-RPC) ─┐          │   claude (SDK / stream-json)
  hook shim ──┘                        │          │   opencode serve
                                       ▼          │   agy / gemini / acp ...
 ┌───────────────────────── eliot daemon ─────────┼──────────────────────┐
 │                                                │                      │
 │  API ──► Router ──► Plugin host ──stdio JSON-RPC──► plugin processes ─┘
 │            │             ▲  (one process per harness kind, many agents)
 │            ▼             │ events                                     │
 │   ┌────────────── Bus (in-memory fan-out + persisted log) ──────────┐ │
 │   └──▲────────▲────────▲──────────▲─────────────▲──────────▲───────┘ │
 │      │        │        │          │             │          │         │
 │  Monitors:  process   git    transcripts     Automation  Coordination │
 │  (sysinfo) (git CLI) (file tail)  hooks      rules,timers  mail,vars, │
 │                                               goal loop    who,claims │
 │                                                                       │
 │  Store: one SQLite file (WAL), one writer task                        │
 └───────────────────────────────────────────────────────────────────────┘
          proxies (OpenCodex, ...) = supervised or attached services
```

## Components

| Component | Responsibility | Not its job |
| --- | --- | --- |
| `eliot` CLI | Thin client: parse args, call the daemon, print text or `--json`. Same binary also runs `daemon`, built-in plugins (`eliot plugin <name>`) and the hook shim (`eliot hook`). | Business logic. |
| Daemon | Owns state, the bus, plugin processes, monitors, automation. One per data dir (lock file). | Model loops, harness internals. |
| Router | Validates a command (target exists, verb supported, caller allowed, state allows it), persists it, hands it to the plugin, records the outcome. | Translating verbs — plugins do that. |
| Plugin host | Starts/restarts plugin processes, speaks the [plugin protocol](spec/plugin-protocol.md), routes per-agent messages. | Knowing any harness. |
| Plugins | One per harness kind. Translate semantic verbs to native calls, normalize native events, report native PIDs and session ids. Any language while prototyping. | Persisting Eliot state, retries of unknown effects. |
| Bus | Single ordered event stream: persisted events get a `seq`; ephemeral events are fan-out only. Subscribers replay from a `seq` then follow live. | Business decisions. |
| Monitors | Process, Git, filesystem, transcript tail, hook intake, source health. Each emits events. | Commanding agents. |
| Automation | Rules (event → actions), timers (every/cron/at), scripts, approval policy, Eliot Goal loop. Emits ordinary commands. | A second state machine. |
| Coordination | Mailbox, shared variables, area queries (`who`), advisory claims. | Locks or sandboxing. |
| Store | SQLite schema, migrations by number, one writer task with group commit, read connections for queries. | Hashing documents or proving anything. |

## Core rules

1. **One language.** Callers use [semantic verbs](spec/core.md#verbs). Plugins
   translate. A plugin declares supported verbs; anything else is rejected **before**
   any native I/O with `unsupported`.
2. **Events are the truth of what happened; tables are the truth of what is.** State
   tables (`agents`, `commands`, ...) are updated in the same transaction as the event
   that changed them. Views (`ls`, `top`, `who`) are queries, never new state machines.
3. **Never replay an uncertain effect.** If a `send`/`steer`/`launch`/`git push` may
   have happened, the command becomes `unknown` and is resolved by readback, never by
   resending.
4. **Unknown is not zero.** A silent source is `stale`, a vanished process is `lost`,
   a busy agent with no output is `stalled`. None of these means idle or done.
5. **Native first.** Use the harness's own goal, hooks, subagents, permissions and
   resume. Eliot adds only what the harness lacks.
6. **Automation uses the same commands as people.** A rule, a timer, the Goal loop and
   the owner all submit the same commands through the same router.
7. **Plugins own native details; the core owns identity and history.** The core never
   parses vendor payloads; plugins never write to the Store.

## Process and lifetime model

- `eliot daemon` starts plugin processes on demand (first agent of that harness).
- A plugin process may launch native processes (e.g. `codex app-server`) or attach to
  external services (e.g. a running `opencode serve`). It reports every native PID it
  owns so the process monitor can watch it.
- Native processes launched by Eliot belong to the daemon's process tree (Job object
  on Windows, process group on Unix). Stopping the daemon stops them. After a daemon
  restart, agents are re-attached through the plugin (`agent.attach`) or marked
  `lost`; continuing a lost session is an explicit `resume` — the last prompt is never
  resent.
- Services (proxies, shared servers) are refcounted: the daemon starts a managed
  service before the first agent that needs it and never kills an attached external
  service.

## Data flow: a command

```text
eliot steer a12 "use the v2 API"
 → CLI → IPC request command.submit {id, target: a12, verb: steer, args}
 → Router: a12 exists; plugin codex supports steer; caller may command a12; a12 is busy
 → Store: insert command (state=accepted) + event command.accepted       (one tx)
 → Plugin host → plugin: agent.command {agent_id, command_id, verb, args}
 → plugin → native: turn/steer {threadId, expectedTurnId, input}
 → plugin reply: {status: applied}  → Store: command applied + event      (one tx)
 → Bus fan-out → `eliot watch`, rules, ...
```

Timeout or plugin crash after hand-off → `unknown`; the plugin resolves it on restart
with `agent.snapshot` when it can.

## Data flow: an event

```text
native notification (turn/completed)
 → plugin normalizes → event {agent_id, kind: turn.completed, data}
 → Daemon: reduce agent state (busy → idle), persist event + agent row      (one tx)
 → Bus fan-out → automation (rule "push after writer turn" fires → command git.push)
               → goal loop (agent idle with active goal → check → continue / done)
               → subscribers
```

## Scale model

Scale comes from cheap bookkeeping, not from many processes:

- An agent is a row plus, while live, a small in-memory record (target ≤ 2 KB). No
  thread or task per idle agent.
- One plugin process multiplexes many agents; the daemon can run several instances of
  the same plugin (`instances = N`) and shard agents across them.
- Writes are batched (group commit). Ephemeral events (token deltas, CPU samples) are
  never written.
- Every list API is paginated; every subscription is filtered on the daemon side.
- Trees use `parent_id` and `root_id` indexes; tree operations never scan all agents.

Honest limit: a live native harness costs 100 MB–1 GB of RAM, so one machine runs
tens to a few hundred **live** agents. Millions are supported as registered/historical
agents and, later, across several hosts ([roadmap](roadmap.md) L9). The design never
needs an O(all agents) pass on a hot path.

## Repository layout (target)

```text
Cargo.toml                 workspace
crates/
  eliot-core/              ids, agent model, events, verbs, commands, state reducer (no I/O)
  eliot-store/             SQLite schema, migrations, writer task, queries
  eliot-daemon/            router, bus, plugin host, monitors, automation, coordination
  eliot-plugin/            small SDK for Rust plugins (stdio JSON-RPC loop, typed messages)
  eliot/                   the binary: CLI, `daemon`, `plugin <builtin>`, `hook`, `mcp`
plugins/
  fake/                    Rust crate, linked into `eliot`: deterministic simulated harness
  codex/                   Rust crate, linked into `eliot`: first real harness
  claude/ opencode/ ...    later; Rust crates (linked) or any-language programs started via `cmd`
docs/                      this documentation
```

Crates are split only where a boundary is real (pure types, storage, runtime, SDK,
binary). Do not add crates for future use.
