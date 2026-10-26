# Client API and CLI

## Transport

- JSON-RPC 2.0, one JSON object per line (NDJSON), UTF-8.
- Windows: named pipe `\\.\pipe\eliot-<hash(data_dir)>`, ACL limited to the current
  user. Unix: socket `<data_dir>/eliot.sock`, mode `0600`.
- Single instance: the daemon holds an OS exclusive lock on `<data_dir>/daemon.lock`
  for its lifetime (the OS releases it on a crash, so no takeover logic is needed).
  It then writes `<data_dir>/daemon.json` (`pid`, `endpoint`, `version`,
  `started_at`). Clients read it; no discovery service. A client treats it as live
  only if that PID exists with the recorded process start time (PIDs are reused).
- Caller identity: requests without a token are the **owner**. Agents get
  `ELIOT_AGENT_TOKEN` at launch and the CLI sends it automatically; the daemon maps it
  to the agent id. This is attribution and routing, not a security boundary
  ([decisions](../decisions.md) D-07). Every process Eliot starts for an agent gets
  that agent's token; a native child without its own environment (`own_identity =
  false`) inherits its parent's and acts as the parent; a rule's script gets a run
  token that acts as the rule ([automation](automation.md#scripts)). A process that
  has `ELIOT_AGENT_ID` or `ELIOT_RULE` but no matching `ELIOT_AGENT_TOKEN` (a harness
  filtered its environment) is refused `not_allowed` by the CLI and the daemon — an
  agent or a project script never becomes the owner by losing its token. `daemon.status` reports the caller, so a
  plugin can check what a tool inside the harness sees.
- Max frame 4 MiB. Requests are multiplexed by `id`; a slow call never blocks others.

## Methods

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | — | version, uptime, plugins, sources health, counts by state, `caller` (owner or agent id) |
| `daemon.shutdown` | `drain?`, `force?` | with `drain` (**the** definition of drain): no new launches, sends, rule or timer firings or goal rounds; running turns finish; approvals, answers, interrupts, stops, cancels and reads keep working; then stop. Without `drain` it refuses (`busy`) while any stdio session has a running turn, unless `force` — those turns would be lost. |
| `agent.list` | `filter{state?, harness?, project?, label?, tree?, parent?}` (`tree` = the agent and all descendants), `after?`, `limit≤1000` | page of agents + `next` |
| `agent.get` | `agent` | agent + pending requests + goal + last events |
| `agent.wait` | `targets` (ids/names/selector), `for: turn|idle|stopped|goal`, `after_command?`, `timeout_ms` | per agent: state reached or not, state, last `final_text`. `turn` = the running or next turn ends — with `after_command`, the turn started by that command's dispatch, so a late end of an earlier turn never satisfies it (`send --wait` always passes it); `idle` = state is `idle` (immediate if already); `stopped` = `stopped`/`failed`/`lost`; `goal` = goal `done`/`blocked`. |
| `agent.result` | `agent`, `turns?` (default 1) | `final_text`, status and usage of the last N completed turns |
| `command.submit` | `id?`, `target?` (id/name/selector; omitted for `launch`), `verb`, `args`, `wait_ms?` (default 10000) | `{command_id, agent?, state, outcome?}` — waits up to `wait_ms` for the plugin reply, then returns the current state; `agent` is the new id for `launch` |
| `command.get` | `id` | command |
| `command.children` | `id`, `after?`, `limit≤1000` | page of the commands a selector command expanded into |
| `events.read` | `after_seq`, `filter`, `limit` | persisted events page; `history_gap` (with `earliest_seq`) if `after_seq` is older than retention |
| `events.subscribe` | `after_seq?`, `filter` | stream of `events.item` notifications: replays persisted events after `after_seq`, then live events. Each subscriber has a bounded buffer (items and bytes); on overflow it gets `events.lagged {dropped, from_seq, through_seq}` before newer events and can re-read persisted ones with `events.read` |
| `hook.event` | `agent`, `harness`, `hook`, `payload` | decision JSON for the hook shim ([plugin protocol](plugin-protocol.md#hooks)) |
| `agent.discover` | `harness?`, `cursor?`, `limit≤1000` | page of native sessions found by plugins' `discover` + `next` |
| `agent.attach` | `harness`, `native_scope`, `native_ref`, `control?`, `profile?` (required with `control`) | the agent of that subject: a new `origin=external` agent, or the existing row ([one subject, one agent](core.md#identifiers)). With `control`, the plugin must attach for control with that profile; on success the row stops being observe-only. `not_allowed` if the plugin cannot control it. |
| `mail.send` | `to`, `text`, `mode?`, `reply_to?` | message ids ([coordination](coordination.md#mailbox)) |
| `mail.inbox` | `wait_s?`, `all?`, `limit≤100` | caller's oldest unread messages (does not mark them); `all` pages through read ones with `after?` |
| `mail.ack` | `through` (message id) | marks the caller's messages up to it read |
| `mail.read` | `id` | one message; marks it read |
| `var.get` / `var.set` / `var.list` / `var.watch` | `scope/key`, `value`, `expect_version?`, `note?` / prefix (+ `after?`, `limit≤1000` for `list`) | value + version; `watch` streams `var.changed` ([coordination](coordination.md#shared-variables)) |
| `who` | `paths: [pathspec]`, `history?` | rows per agent ([coordination](coordination.md#who-works-on-this)) |
| `claim.add` / `claim.release` | `glob`, `note?`, `ttl?` | claim / conflicts ([coordination](coordination.md#claims)) |
| `rule.list` / `rule.get` / `rule.enable` / `rule.disable` / `rule.test` | `id`, `event?` (`list`: `after?`, `limit≤1000`) | rules; `test` returns the actions it would run ([automation](automation.md#rules)) |
| `rules.pause` / `rules.resume` | — | global automation switch |
| `timer.list` / `timer.run` | `id` | time-triggered rules with next fire time |

Goals are commands: `goal.set|pause|resume|clear` go through `command.submit`;
`agent.get` returns the current goal.

`filter` = `{kinds?: [glob], agents?: [id], tree?: id, sources?: [glob], ephemeral?: bool}`.

Every write method (`command.submit`, `mail.send`, `mail.ack`, `var.set`,
`claim.add`, ...) takes an `id` (a ULID; the CLI always sends one): repeating a call
with the same `id` and payload returns the first result; the same `id` with a
different payload fails `invalid`. The [command id rules](core.md#command-ids) apply
to all of them: an unknown id older than 24 h fails `invalid` (`id_expired`). Commands
keep their id in `commands`; other writes keep it in a small `writes` table
(`id`, `method`, `params`, `result`, `created_at`; added with L5, pruned after
`retention.commands_days`).

Errors use JSON-RPC error objects with a stable `data.code`: `not_found`,
`unsupported`, `not_allowed`, `bad_state`, `invalid`, `busy`, `unavailable`,
`history_gap`, `internal`; `data.reason` refines it (e.g. `id_expired`,
`secret_in_payload`).

Any CLI call without `ELIOT_AGENT_TOKEN` (and without `ELIOT_AGENT_ID`/`ELIOT_RULE`)
acts as the owner. Actions of a project rule — its `do` list, its scripts' stdout
actions and their `eliot` calls — may target only agents of that project
([automation](automation.md#scripts)).

## CLI

All commands accept `--json` (machine output on stdout, logs on stderr) and
`--data-dir` (default `%LOCALAPPDATA%\Eliot` / `~/.local/share/eliot`, or `ELIOT_HOME`).
Every command that changes something accepts `--id ID`; without it the CLI generates
one and prints it, and a retry after an unknown outcome must pass the same `--id`.

`eliot daemon` (without `--foreground`) removes a stale `daemon.json`, starts itself
again with `--foreground`, detached from the calling terminal so it survives that
terminal closing, and returns once a `daemon.json` with the child's PID appears. Other commands never start the daemon; when it is not
running they fail with `unavailable` and say so.

### Exit codes

| Code | Meaning | Error codes |
| --- | --- | --- |
| 0 | ok: command `applied` or `queued`, `cmd cancel` succeeded, query answered; with `--wait` / `wait`: the turn ended `completed` or the target reached the state | — |
| 1 | error, or a definite bad outcome | `not_found`, `invalid`, `busy`, `unavailable`, `history_gap`, `internal`, command `failed`; with `--wait` / `wait`: the turn ended `failed` or `interrupted`, a goal ended `blocked`, or the agent became `stopped`/`failed`/`lost` first |
| 2 | CLI usage error (bad flags) | — |
| 3 | rejected — nothing happened | `unsupported`, `not_allowed`, `bad_state`, command `rejected` or `cancelled` |
| 4 | outcome unknown | command `unknown` |
| 5 | `wait` timed out | — |
| 6 | pending: the command is still `accepted` or `dispatched` when the CLI stops waiting; read it back with `eliot cmd show ID` | — |

`eliot cmd show ID` exits with the code of the command's state from this table.

```text
eliot daemon [--foreground]                 start the daemon (default: background, returns when ready)
eliot daemon stop [--drain | --force]       stop it (--drain: see `daemon.shutdown`)
eliot status                                daemon, plugins, sources, the caller, where each profile's config came from
eliot launch [--profile P] [--harness H] [--name N] [--parent A] [--label k=v]...
             [--project NAME] [--control self|tree|project]
             [--workspace PATH | --worktree BRANCH [--base REF]] [PROMPT] [--wait]
                                            (--wait: wait for the linked first send's turn, print its final text)
eliot ls [--state S] [--tree A] [--label k=v] [--all]
eliot show A                                details, pending requests, goal, recent events
eliot watch [A|SELECTOR] [--kind GLOB]... [--since SEQ]    live event stream
eliot top                                   live table: agents, state, turn time, tokens, cpu/ram, git
eliot send|inject A TEXT
eliot steer A TEXT [--or-send]
eliot send A TEXT --wait [--timeout DUR]    send, wait for that turn to end, print its final text
eliot wait TARGET... [--for turn|idle|stopped|goal] [--timeout DUR]
                                            block until every target gets there (default --for turn,
                                            --timeout 9m: stay under harness shell-tool time limits; re-run to keep waiting)
eliot result A [--turns N]                  final text of the last turn(s)
eliot interrupt A (alias: abort) | stop A [--force] [--tree] | resume A [PROMPT]
                                            (`--tree` = the selector `@tree:A`)
eliot approve|deny A [--request R] [--remember]   default: the only pending request
eliot answer A [TEXT] [--request R] [--choice N]
eliot configure A [--model M] [--effort E] [--permissions P]
eliot compact|fork|background A             native-only verbs
eliot goal set A TEXT [--mode auto|native|eliot] [--check CMD|marker:TEXT] [--every DUR]
                    [--max-turns N] [--max-cost USD] [--no-progress N]   (limits form `budget`)
eliot goal pause|resume|clear A [--mode native|eliot] | eliot goal show A
eliot cmd show ID                           command state/outcome; exits with the code of that state
eliot cmd children ID [--after C]           the commands a selector command expanded into (paged)
eliot cmd cancel ID                         withdraw a queued send that has not started
eliot cmd resolve ID --as applied|failed --reason TEXT   owner settles an `unknown` command by hand
eliot events [--since SEQ] [--kind GLOB] [--agent A]
eliot msg send TO TEXT [--mode queue|inject|send|steer] [--reply-to M]
eliot inbox [--wait SECS] [--all]           for agents: unread messages; prints them, then `mail.ack`
                                            (a lost reply or crash shows them again — never drops them)
eliot var get KEY | set KEY VALUE [--expect-version N] [--note T] | list PREFIX | watch PATTERN
eliot who PATHSPEC... [--history N]
eliot claim GLOB [--note T] [--ttl DUR] | eliot release [GLOB]
eliot rule list|show|enable|disable|test ID [--event FILE] | eliot rules pause|resume
eliot timer list|run ID
eliot discover [--harness H] [--cursor C]   list native sessions on this machine not launched by Eliot (paged)
eliot service list | stop NAME              proxies and native servers Eliot supervises (L2.7, L7.6)
eliot hold list | clear ACCOUNT             provider holds per account (L2.7)
eliot worktree remove A                     remove a stopped agent's worktree (L3.2 rules)
eliot doctor                                orphans, unused services, stale worktrees, disk use (L7.7)
eliot daemon install | uninstall            per-user logon task (L7.8)
eliot attach --harness H --scope S --ref R [--control --profile P]   watch a session from `discover`; with --control
                                            also control it under profile P (same agent row either way)
eliot plugin NAME                           (internal) run a built-in plugin on stdio
eliot plugin-test CMD... [--replay-dir DIR]  run the plugin conformance suite against any plugin command
eliot hook --harness H --event E            (internal) hook shim: stdin JSON → daemon → stdout decision
eliot detach A                              stop controlling A; its native session keeps running
eliot mcp                                   MCP server on stdio for agents (L5.5); tools depend on the caller's control
```

Selectors: see [core](core.md#commands).

`eliot` inside an agent: the daemon injects `ELIOT_HOME`, `ELIOT_AGENT_ID`,
`ELIOT_AGENT_TOKEN` and puts `eliot` on `PATH`, so every harness that can run shell
commands can use the CLI with no extra setup.
