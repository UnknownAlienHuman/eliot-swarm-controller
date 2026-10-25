# Client API and CLI

## Transport

- JSON-RPC 2.0, one JSON object per line (NDJSON), UTF-8.
- Windows: named pipe `\\.\pipe\eliot-<hash(data_dir)>`, ACL limited to the current
  user. Unix: socket `<data_dir>/eliot.sock`, mode `0600`.
- The daemon writes the endpoint into `<data_dir>/daemon.json` (`pid`, `endpoint`,
  `version`, `started_at`). Clients read it; no discovery service.
- Caller identity: requests without a token are the **owner**. Agents get
  `ELIOT_AGENT_TOKEN` at launch and the CLI sends it automatically; the daemon maps it
  to the agent id. This is attribution and routing, not a security boundary
  ([decisions](../decisions.md) D-07).
- Max frame 4 MiB. Requests are multiplexed by `id`; a slow call never blocks others.

## Methods

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | — | version, uptime, plugins, sources health, counts by state |
| `daemon.shutdown` | `drain?` | stops accepting new launches; with `drain`, waits for running turns |
| `agent.list` | `filter{state?, harness?, project?, label?, tree?, parent?}` (`tree` = the agent and all descendants), `after?`, `limit≤1000` | page of agents + `next` |
| `agent.get` | `agent` | agent + pending requests + goal + last events |
| `command.submit` | `id?`, `target?` (id/name/selector; omitted for `launch`), `verb`, `args`, `wait_ms?` (default 10000) | `{command_id, agent?, state, outcome?}` — waits up to `wait_ms` for the plugin reply, then returns the current state; `agent` is the new id for `launch` |
| `command.get` | `id` | command |
| `events.read` | `after_seq`, `filter`, `limit` | persisted events page |
| `events.subscribe` | `after_seq?`, `filter` | stream of `events.item` notifications: replays persisted events after `after_seq`, then live events; sends `events.lagged {resume_seq}` if the client falls behind |
| `hook.event` | `agent`, `harness`, `hook`, `payload` | decision JSON for the hook shim ([plugin protocol](plugin-protocol.md#hooks)) |
| `agent.discover` | `harness?` | native sessions found by plugins `discover` |
| `agent.attach` | `harness`, `native_ref`, `observe_only` | new `origin=external` agent |
| `mail.send` | `to`, `text`, `mode?`, `reply_to?` | message ids ([coordination](coordination.md#mailbox)) |
| `mail.inbox` | `wait_s?`, `all?` | caller's messages; marks them delivered |
| `mail.read` | `id` | one message; marks it read |
| `var.get` / `var.set` / `var.list` / `var.watch` | `scope/key`, `value`, `expect_version?`, `note?` / prefix | value + version; `watch` streams `var.changed` ([coordination](coordination.md#shared-variables)) |
| `who` | `paths: [pathspec]`, `history?` | rows per agent ([coordination](coordination.md#who-works-on-this)) |
| `claim.add` / `claim.release` | `glob`, `note?`, `ttl?` | claim / conflicts ([coordination](coordination.md#claims)) |
| `rule.list` / `rule.get` / `rule.enable` / `rule.disable` / `rule.test` | `id`, `event?` | rules; `test` returns the actions it would run ([automation](automation.md#rules)) |
| `rules.pause` / `rules.resume` | — | global automation switch |
| `timer.list` / `timer.run` | `id` | time-triggered rules with next fire time |

Goals are commands: `goal.set|pause|resume|clear` go through `command.submit`;
`agent.get` returns the current goal.

`filter` = `{kinds?: [glob], agents?: [id], tree?: id, sources?: [glob], ephemeral?: bool}`.

Errors use JSON-RPC error objects with a stable `data.code`: `not_found`,
`unsupported`, `not_allowed`, `bad_state`, `invalid`, `busy`, `unavailable`, `internal`.

Script callers (rules' `run` actions) and any CLI call without `ELIOT_AGENT_TOKEN` act
as the owner.

## CLI

All commands accept `--json` (machine output on stdout, logs on stderr) and
`--data-dir` (default `%LOCALAPPDATA%\Eliot` / `~/.local/share/eliot`, or `ELIOT_HOME`).

### Exit codes

| Code | Meaning | Error codes |
| --- | --- | --- |
| 0 | ok (command `applied`, query answered) | — |
| 1 | error | `not_found`, `invalid`, `busy`, `unavailable`, `internal`, command `failed` |
| 2 | CLI usage error (bad flags) | — |
| 3 | rejected — nothing happened | `unsupported`, `not_allowed`, `bad_state` |
| 4 | outcome unknown | command `unknown` |

```text
eliot daemon [--foreground]                 start the daemon (default: background, returns when ready)
eliot daemon stop [--drain]                 stop it (drain: wait for running turns, admit nothing new)
eliot status                                daemon, plugins, sources
eliot launch [--profile P] [--harness H] [--name N] [--parent A] [--label k=v]...
             [--project NAME] [--control self|tree|project]
             [--workspace PATH | --worktree BRANCH [--base REF]] [PROMPT]
eliot ls [--state S] [--tree A] [--label k=v] [--all]
eliot show A                                details, pending requests, goal, recent events
eliot watch [A|SELECTOR] [--kind GLOB]... [--since SEQ]    live event stream
eliot top                                   live table: agents, state, turn time, tokens, cpu/ram, git
eliot send|steer|inject A TEXT [--or-send]
eliot interrupt A (alias: abort) | stop A [--force] [--tree] | resume A [PROMPT]
eliot approve|deny A [--request R] [--remember]   default: the only pending request
eliot answer A TEXT [--request R] [--choice N]
eliot configure A [--model M] [--effort E] [--permissions P]
eliot goal set A TEXT [--check CMD|marker:TEXT] [--max-turns N] [--max-cost USD] [--no-progress N]
                                            (the three limits form `budget`)
eliot goal pause|resume|clear|show A
eliot cmd show ID                           command state/outcome
eliot events [--since SEQ] [--kind GLOB] [--agent A]
eliot msg send TO TEXT [--mode queue|inject|send|steer] [--reply-to M]
eliot inbox [--wait SECS] [--all]           for agents: unread messages
eliot var get|set|list|watch ...
eliot who PATHSPEC... [--history N]
eliot claim GLOB [--note T] [--ttl DUR] | eliot release [GLOB]
eliot rule list|show|enable|disable|test ID [--event FILE] | eliot rules pause|resume
eliot timer list|run ID
eliot discover [--harness H]                list native sessions on this machine not launched by Eliot
eliot attach SESSION [--observe-only]       start watching (and if supported, controlling) one of them
eliot plugin NAME                           (internal) run a built-in plugin on stdio
eliot plugin-test CMD...                    run the plugin conformance suite against any plugin command
eliot hook --harness H --event E            (internal) hook shim: stdin JSON → daemon → stdout decision
eliot mcp                                   MCP server on stdio exposing the same API to agents (L6)
```

Selectors: see [core](core.md#commands).

`eliot` inside an agent: the daemon injects `ELIOT_HOME`, `ELIOT_AGENT_ID`,
`ELIOT_AGENT_TOKEN` and puts `eliot` on `PATH`, so every harness that can run shell
commands can use the CLI with no extra setup.
