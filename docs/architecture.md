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
 │            │             ▲  (one process per agent; one per harness if it isolates sessions)
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
| Plugins | One per harness kind, run as one process per agent (or one for many agents when the harness isolates sessions). Translate semantic verbs to native calls, normalize native events, report native PIDs and session ids. Any language while prototyping. | Persisting Eliot state, retries of unknown effects. |
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
3. **Never replay an uncertain effect.** If a `send`/`steer`/`interrupt`/`stop`/
   `launch`/`git push` may have happened, the command becomes `unknown` and is resolved
   by readback, never by resending. Starting a session and giving it its first input
   are separate commands, so a lost start reply can never duplicate the prompt.
4. **Unknown is not zero.** A silent source is `stale`, a vanished process is `lost`,
   a busy agent with no output is `stalled`. None of these means idle or done.
5. **Native first.** Use the harness's own goal, hooks, subagents, permissions and
   resume. Eliot adds only what the harness lacks.
6. **Automation uses the same commands as people.** A rule, a timer, the Goal loop and
   the owner all submit the same commands through the same router.
7. **Plugins own native details; the core owns identity and history.** The core never
   parses vendor payloads; plugins never write to the Store.
8. **Routes are explicit.** Eliot never changes an agent's provider, model, effort,
   permissions or billing route (subscription vs API key vs proxy) on its own.
   Fallbacks exist only as rules the owner wrote.

## Process and lifetime model

- `eliot daemon` starts one plugin process per agent when the agent starts (one per
  harness for `multi_session` plugins, on first use). Each agent's plugin, harness and
  tools live in one custody unit — a Job object on Windows (exact), a process group on
  Unix (degraded: see [monitoring](spec/monitoring.md#process-monitor)) — and get that
  agent's own environment and token.
- A plugin process may launch native processes (e.g. `codex app-server`) or attach to
  external services (e.g. a running `opencode serve`). It reports every native PID it
  owns so the process monitor can watch it.
- Native processes launched by Eliot over stdio belong to the daemon's process tree:
  stopping the daemon ends those sessions (use `eliot daemon stop --drain` before an
  upgrade). After a restart the daemon rebuilds its work from the Store — live agents
  are re-attached (`agent.attach`, the rest become `lost`), `dispatched` commands are
  `unknown`, queued sends, rule cursors, timers, goals and unread mail continue — and
  continuing a lost session is an explicit `resume`; the last prompt is never resent.
- Harnesses with a server mode (Codex `app-server --listen`, `opencode serve`) can run
  their server as an Eliot **service** that lives outside the daemon (L7.6): a daemon
  restart then re-attaches without interrupting running turns.
- A plugin crash ends the stdio sessions it owned — with one process per agent, only
  that agent's (it becomes `lost`; `resume` continues it).
- Upgrades: builds install into a versioned directory and the daemon never overwrites
  its own executable (Windows cannot replace a running `.exe`). Upgrade = `eliot daemon
  stop --drain`, then start the new build. External plugins (started via `cmd`) are
  upgraded by restarting just that plugin.
- Services (proxies, shared servers) are refcounted: the daemon starts a managed
  service before the first agent that needs it and never kills an attached external
  service.

## Data flow: a command

```text
eliot steer a12 "use the v2 API"
 → CLI → IPC request command.submit {id, target: a12, verb: steer, args}
 → Router: a12 exists; plugin codex supports steer; caller may command a12; a12 is busy
   (a failed check is stored as state=rejected with its reason and returned)
 → Store: insert command (state=accepted) + event command.accepted       (one tx)
 → Plugin host → plugin: agent.command {agent_id, command_id, verb, args, turn_id}   (state=dispatched)
 → plugin → native: turn/steer {threadId, expectedTurnId, input}
 → plugin reply: {status: applied}  → Store: command applied + event      (one tx)
 → Bus fan-out → `eliot watch`, rules, ...
```

Timeout or plugin crash after hand-off → `unknown`; the plugin resolves it on restart
with `agent.snapshot` when it can.

## Data flow: a launch

```text
eliot launch --profile codex "add the /v2 routes"
 → Store: agent a12 (created) + command L (launch) + linked send L/send (queued)    (one tx)
 → free slot (profile, account, proxy) → a12 starting → per-agent plugin + Job
 → plugin: agent.start → codex app-server, thread/start → event session.ready
 → Store: L applied, a12 idle                                                      (one tx)
 → router: a12 idle → dispatch L/send → turn/start → turn.started names L/send → applied
```

A harness that can only create a session together with its first input
(`start_requires_input`) gets the text of `L/send` inside `agent.start`; the two
commands are still settled separately.

## Data flow: an event

```text
native notification (turn/completed)
 → plugin normalizes → event request {agent_id, kind: turn.completed, native_id?, cursor?, data}
 → Daemon: reduce agent state (busy → idle), persist event + agent row + turn row  (one tx)
 → reply {seq} to the plugin (only now is the fact stored)
 → Bus fan-out → automation (e.g. a rule on turn.completed fires → command notify)
               → goal loop (agent idle with active goal → check → continue / done)
               → subscribers
```

## Scale model

Scale comes from cheap bookkeeping, not from many processes:

- An agent is a row plus, while live, a small in-memory record (target ≤ 2 KB). No
  thread or task per idle agent.
- A live real agent costs one plugin process next to its harness (a few MB beside
  100 MB–1 GB); the fake plugin and isolating servers serve many agents from one
  process (`features.multi_session`), so 10,000 fake agents need one plugin process.
- Writes are batched (group commit). Ephemeral events (token deltas, CPU samples) are
  never written.
- Every list API is paginated; every subscription is filtered on the daemon side.
- Trees use the `parent` and `root` indexes; tree operations never scan all agents.

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
