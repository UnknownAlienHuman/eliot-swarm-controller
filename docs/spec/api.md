# Client API and CLI

## Transport

- JSON-RPC 2.0, one JSON object per line (NDJSON), UTF-8.
- Windows: named pipe `\\.\pipe\eliot-<hash(data_dir)>`, ACL limited to the current
  user. Unix: socket `<data_dir>/eliot.sock`, mode `0600`.
- The daemon writes the endpoint into `<data_dir>/daemon.json` (`pid`, `endpoint`,
  `version`, `started_at`). Clients read it; no discovery service. A client or a
  second daemon treats it as live only if that PID exists with the recorded process
  start time (PIDs are reused); a stale `daemon.lock` is taken over only under the
  same check.
- Caller identity: requests without a token are the **owner**. Agents get
  `ELIOT_AGENT_TOKEN` at launch and the CLI sends it automatically; the daemon maps it
  to the agent id. This is attribution and routing, not a security boundary
  ([decisions](../decisions.md) D-07).
- Max frame 4 MiB. Requests are multiplexed by `id`; a slow call never blocks others.

## Methods

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | — | version, uptime, plugins, sources health, counts by state |
| `daemon.shutdown` | `drain?`, `force?` | with `drain`: no new launches, rules or timer firings; running turns finish, approvals, answers and results keep working; then stop. Without `drain` it refuses (`busy`) while any stdio session has a running turn, unless `force` — those turns would be lost. |
| `agent.list` | `filter{state?, harness?, project?, label?, tree?, parent?}` (`tree` = the agent and all descendants), `after?`, `limit≤1000` | page of agents + `next` |
| `agent.get` | `agent` | agent + pending requests + goal + last events |
| `agent.wait` | `targets` (ids/names/selector), `for: turn|idle|stopped|goal`, `after_command?`, `timeout_ms` | per agent: state reached or not, state, last `final_text`. `turn` = the running or next turn ends — with `after_command`, the turn started by that command's dispatch, so a late end of an earlier turn never satisfies it (`send --wait` always passes it); `idle` = state is `idle` (immediate if already); `stopped` = `stopped`/`failed`/`lost`; `goal` = goal `done`/`blocked`. |
| `agent.result` | `agent`, `turns?` (default 1) | `final_text`, status and usage of the last N completed turns |
| `command.submit` | `id?`, `target?` (id/name/selector; omitted for `launch`), `verb`, `args`, `wait_ms?` (default 10000) | `{command_id, agent?, state, outcome?}` — waits up to `wait_ms` for the plugin reply, then returns the current state; `agent` is the new id for `launch` |
| `command.get` | `id` | command |
| `events.read` | `after_seq`, `filter`, `limit` | persisted events page |
| `events.subscribe` | `after_seq?`, `filter` | stream of `events.item` notifications: replays persisted events after `after_seq`, then live events. Each subscriber has a bounded buffer (items and bytes); on overflow it gets `events.lagged {dropped, from_seq, through_seq}` before newer events and can re-read persisted ones with `events.read` |
| `hook.event` | `agent`, `harness`, `hook`, `payload` | decision JSON for the hook shim ([plugin protocol](plugin-protocol.md#hooks)) |
| `agent.discover` | `harness?` | native sessions found by plugins `discover` |
| `agent.attach` | `harness`, `native_scope`, `native_ref`, `observe_only` | new `origin=external` agent (`not_allowed` if another live agent controls that session) |
| `mail.send` | `to`, `text`, `mode?`, `reply_to?` | message ids ([coordination](coordination.md#mailbox)) |
| `mail.inbox` | `wait_s?`, `all?` | caller's messages; marks them read |
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

Every write method (`command.submit`, `mail.send`, `var.set`, `claim.add`, ...) takes
an optional `id`: repeating a call with the same `id` and payload returns the first
result; the same `id` with a different payload fails `invalid`. Use it when retrying
after a lost reply.

Errors use JSON-RPC error objects with a stable `data.code`: `not_found`,
`unsupported`, `not_allowed`, `bad_state`, `invalid`, `busy`, `unavailable`, `internal`.

Script callers (rules' `run` actions) and any CLI call without `ELIOT_AGENT_TOKEN` act
as the owner.

## CLI

All commands accept `--json` (machine output on stdout, logs on stderr) and
`--data-dir` (default `%LOCALAPPDATA%\Eliot` / `~/.local/share/eliot`, or `ELIOT_HOME`).
Every command that changes something accepts `--id ID`; without it the CLI generates
one and prints it, and a retry after an unknown outcome must pass the same `--id`.

`eliot daemon` (without `--foreground`) starts itself again with `--foreground`,
detached from the calling terminal so it survives that terminal closing, and returns
once `daemon.json` appears. Other commands never start the daemon; when it is not
running they fail with `unavailable` and say so.

### Exit codes

| Code | Meaning | Error codes |
| --- | --- | --- |
| 0 | ok (command `applied` or `queued`, query answered) | — |
| 1 | error | `not_found`, `invalid`, `busy`, `unavailable`, `internal`, command `failed` |
| 2 | CLI usage error (bad flags) | — |
| 3 | rejected — nothing happened | `unsupported`, `not_allowed`, `bad_state` |
| 4 | outcome unknown | command `unknown` |
| 5 | `wait` timed out | — |

```text
eliot daemon [--foreground]                 start the daemon (default: background, returns when ready)
eliot daemon stop [--drain | --force]       stop it (drain: let running turns finish, admit nothing new)
eliot status                                daemon, plugins, sources
eliot launch [--profile P] [--harness H] [--name N] [--parent A] [--label k=v]...
             [--project NAME] [--control self|tree|project]
             [--workspace PATH | --worktree BRANCH [--base REF]] [PROMPT]
eliot ls [--state S] [--tree A] [--label k=v] [--all]
eliot show A                                details, pending requests, goal, recent events
eliot watch [A|SELECTOR] [--kind GLOB]... [--since SEQ]    live event stream
eliot top                                   live table: agents, state, turn time, tokens, cpu/ram, git
eliot send|steer|inject A TEXT [--or-send]
eliot send A TEXT --wait [--timeout DUR]    send, wait for that turn to end, print its final text
eliot wait TARGET... [--for turn|idle|stopped|goal] [--timeout DUR]
                                            block until every target gets there (default --for turn,
                                            --timeout 9m: stay under harness shell-tool time limits; re-run to keep waiting)
eliot result A [--turns N]                  final text of the last turn(s)
eliot interrupt A (alias: abort) | stop A [--force] [--tree] | resume A [PROMPT]
eliot approve|deny A [--request R] [--remember]   default: the only pending request
eliot answer A TEXT [--request R] [--choice N]
eliot configure A [--model M] [--effort E] [--permissions P]
eliot goal set A TEXT [--mode auto|native|eliot] [--check CMD|marker:TEXT] [--every DUR]
                    [--max-turns N] [--max-cost USD] [--no-progress N]   (limits form `budget`)
eliot goal pause|resume|clear A [--mode native|eliot] | eliot goal show A
eliot cmd show ID                           command state/outcome
eliot cmd cancel ID                         withdraw a queued send that has not started
eliot cmd resolve ID --as applied|failed --reason TEXT   owner settles an `unknown` command by hand
eliot events [--since SEQ] [--kind GLOB] [--agent A]
eliot msg send TO TEXT [--mode queue|inject|send|steer] [--reply-to M]
eliot inbox [--wait SECS] [--all]           for agents: unread messages
eliot var get|set|list|watch ...
eliot who PATHSPEC... [--history N]
eliot claim GLOB [--note T] [--ttl DUR] | eliot release [GLOB]
eliot rule list|show|enable|disable|test ID [--event FILE] | eliot rules pause|resume
eliot timer list|run ID
eliot discover [--harness H]                list native sessions on this machine not launched by Eliot
eliot service list | stop NAME              proxies and native servers Eliot supervises (L2.7, L7.6)
eliot doctor                                orphans, unused services, stale worktrees, disk use (L7.7)
eliot daemon install | uninstall            per-user logon task (L7.8)
eliot attach SESSION [--observe-only]       start watching (and if supported, controlling) one of them
eliot plugin NAME                           (internal) run a built-in plugin on stdio
eliot plugin-test CMD... [--replay-dir DIR]  run the plugin conformance suite against any plugin command
eliot hook --harness H --event E            (internal) hook shim: stdin JSON → daemon → stdout decision
eliot detach A                              stop controlling A; its native session keeps running
eliot mcp                                   MCP server on stdio for agents (L6.5); tools depend on the caller's control
```

Selectors: see [core](core.md#commands).

`eliot` inside an agent: the daemon injects `ELIOT_HOME`, `ELIOT_AGENT_ID`,
`ELIOT_AGENT_TOKEN` and puts `eliot` on `PATH`, so every harness that can run shell
commands can use the CLI with no extra setup.
