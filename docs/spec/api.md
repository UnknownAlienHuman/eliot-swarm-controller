# Client API and CLI

## Transport

- JSON-RPC 2.0 over NDJSON, UTF-8.
- Windows: named pipe `\\.\pipe\eliot-<hash(data_dir)>`, ACL current user only.
  Unix: `<data_dir>/eliot.sock`, mode `0600`.
- One daemon per data directory: exclusive OS lock on `daemon.lock`; `daemon.json`
  records pid, process start identity, endpoint, version and start time. Clients trust
  it only while `(pid,start_time)` still matches.
- Requests **without** a token are the owner. Agents receive current-generation
  `ELIOT_AGENT_ID`, `ELIOT_AGENT_GENERATION` and `ELIOT_AGENT_TOKEN`; the CLI sends
  them automatically. A request that supplies an invalid/stale token is rejected—it is
  never downgraded to owner. This is correctness/attribution, not an OS security
  boundary ([D-07](../decisions.md)).
- Maximum API frame 4 MiB; semantic text limits are smaller
  ([core](core.md#verbs)). Requests are multiplexed by JSON-RPC id.

## Methods

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | — | version, uptime, admission mode, plugins/custody, sources/gaps, counts by state and open command/action state |
| `daemon.shutdown` | `drain?`, `force?` | See [drain](#drain-and-shutdown). |
| `agent.list` | `filter{state?, harness?, project?, label?, tree?, parent?}`, `after?`, `limit≤1000` | page + `next`; every row includes generation |
| `agent.get` | `agent` | agent, generation/native refs, pending requests, open commands/actions, goals, event owners/gaps, recent events |
| `agent.wait` | `targets`, `for: turn|idle|stopped|goal`, `generation?`, `after_command?`, `timeout_ms` | per target result. Default generation is captured at call start; a later generation returns `stale_generation`, not success for replacement work. `turn` with `after_command` waits for the exact turn correlated to that command. |
| `agent.result` | `agent`, `generation?`, `turns?` | bounded final text/status/usage of completed turns in the selected generation (default current) |
| `command.submit` | `id?`, `target?`, `verb`, `args`, `wait_ms?` | `{command_id, agent?, generation?, state, outcome?}`. It waits only for state progress; timeout does not cancel. |
| `command.get` | `id` | command including bound generation/turn/request and outcome |
| `events.read` | `after_seq`, `filter`, `limit` | persisted events page |
| `events.subscribe` | `after_seq?`, `filter` | replay then live `events.item`; bounded buffer and `events.lagged` with persisted sequence range |
| `hook.event` | `agent`, `generation`, `harness`, `hook`, `payload` | exact hook decision |
| `agent.discover` | `harness?` | native sessions found by plugins |
| `agent.attach` | `harness`, `native_scope`, `native_ref`, `observe_only` | new external agent at generation 1, or `not_allowed` on live control conflict |
| `mail.send` | `to`, `text`, `mode?`, `reply_to?` | message ids |
| `mail.inbox` | `wait_s?`, `all?` | caller messages; marks returned items read |
| `mail.read` | `id` | one message; marks read |
| `var.get/set/list/watch` | scope/key/value/version/prefix | value + version or stream |
| `who` | `paths`, `history?` | per-agent rows |
| `claim.add/release` | `glob`, `note?`, `ttl?` | claim/conflicts |
| `rule.list/get/enable/disable/test` | `id`, `event?` | rules; test renders without admitting |
| `rules.pause/resume` | — | global automation switch |
| `timer.list/run` | `id` | schedules/runs |
| `action.get/resolve/cancel` | `run`, `index`, outcome/reason | automation action state/manual resolution |

Goals are commands. Event filter is
`{kinds?:[glob], agents?:[id], tree?:id, sources?:[glob], ephemeral?:bool}`.

Every write method accepts optional `id`. Same id + same canonical payload returns the
first admission/result; same id + different payload is `invalid`. This idempotence
ends at Eliot's transaction boundary—it does not justify replaying native I/O.

Stable error `data.code` values: `not_found`, `unsupported`, `not_allowed`,
`bad_state`, `stale_generation`, `invalid`, `busy`, `unavailable`, `internal`.

## Drain and shutdown

`daemon.shutdown {drain:true}` first commits admission mode `draining`.

While draining:

- reject new work-starting commands: `launch`, `send`, `steer`, `inject`, `resume`,
  `fork`, `background`, `goal.set`, `goal.resume`;
- admit `interrupt`, `stop`, `approve`, `deny`, `answer`, `cancel`, `detach`,
  `goal.pause`, `goal.clear`, read/query operations and exact manual resolution;
- admit no new rule/timer/goal-loop runs and dispatch no newly created automation
  action;
- continue commands/actions already admitted before the mode change, including queued
  sends, because accepted work is not silently discarded;
- keep streams, approvals, answers, results and readback alive.

The daemon exits only after pre-drain admitted work is terminal, no process action is
running, no unresolved `unknown` blocks custody, and daemon-owned live turns/sessions
have ended or been explicitly stopped. It reports blockers while waiting.

Without `drain`, shutdown refuses `busy` while daemon-owned sessions/process actions
are live. `force` terminates exact daemon-owned groups, marks affected in-flight
commands/actions `unknown` and sessions `lost`, then exits. It never kills attached
owner-run services. A service-mode session may be detached/preserved according to its
explicit service policy.

## Subscription boundary

For `events.subscribe`, the daemon:

1. registers the bounded live buffer;
2. captures current persisted high-water `seq`;
3. replays `(after_seq, high_water]`;
4. releases buffered live persisted events above high-water in sequence order, then
   follows live.

Ephemeral events received during replay are buffered best-effort and delivered after
the persisted boundary with no total-order claim. Overflow always reports lag; it is
never silent.

## CLI

All commands accept `--json` and `--data-dir`. Every write accepts `--id`; without it
the CLI generates/prints one. Retry of a lost client reply reuses the same id.

`eliot daemon` background-starts itself and returns only after a live `daemon.json`
with matching process identity appears. Other commands never auto-start it.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | query ok; command/action applied or queued; cancellation succeeded |
| 1 | error (`not_found`, `invalid`, `busy`, `unavailable`, `internal`, definite failure) |
| 2 | CLI usage error |
| 3 | rejected before effect (`unsupported`, `not_allowed`, `bad_state`, `stale_generation`) |
| 4 | outcome unknown |
| 5 | wait timed out |
| 6 | still dispatched/pending when CLI wait ended; read back by id |

```text
eliot daemon [--foreground]
eliot daemon stop [--drain | --force]
eliot status
eliot launch [--profile P] [--harness H] [--name N] [--parent A]
             [--label k=v]... [--project NAME] [--control self|tree|project]
             [--workspace PATH | --worktree BRANCH [--base REF]] [PROMPT]
eliot ls [--state S] [--tree A] [--label k=v] [--all]
eliot show A
eliot watch [A|SELECTOR] [--kind GLOB]... [--since SEQ]
eliot top
eliot send|inject A TEXT
eliot steer A TEXT [--or-send]
eliot send A TEXT --wait [--timeout DUR]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--timeout DUR]
eliot result A [--generation N] [--turns N]
eliot interrupt A | abort A
eliot stop A [--force] [--tree]
eliot resume A [PROMPT]
eliot approve|deny A [--request R] [--remember]
eliot answer A [TEXT] [--request R] [--choice N]
eliot configure A [--model M] [--effort E] [--permissions P]
eliot compact|fork|background A
eliot goal set A TEXT [--mode auto|native|eliot] [--check CMD|marker:TEXT]
                    [--every DUR] [--max-turns N] [--max-cost USD] [--no-progress N]
eliot goal pause|resume|clear A [--mode native|eliot]
eliot goal show A
eliot cmd show ID
eliot cmd cancel ID
eliot cmd resolve ID --as applied|failed --reason TEXT
eliot action show RUN/INDEX
eliot action cancel RUN/INDEX
eliot action resolve RUN/INDEX --as applied|failed --reason TEXT
eliot events [--since SEQ] [--kind GLOB] [--agent A]
eliot msg send TO TEXT [--mode queue|inject|send|steer] [--reply-to M]
eliot inbox [--wait SECS] [--all]
eliot var get KEY | set KEY VALUE [--expect-version N] [--note T] | list PREFIX | watch PATTERN
eliot who PATHSPEC... [--history N]
eliot claim GLOB [--note T] [--ttl DUR] | eliot release [GLOB]
eliot rule list|show|enable|disable|test ID [--event FILE]
eliot rules pause|resume
eliot timer list|run ID
eliot discover [--harness H]
eliot attach --harness H SCOPE/REF [--observe-only]
eliot detach A
eliot service list | stop NAME
eliot worktree remove A
eliot doctor
eliot daemon install | uninstall
eliot plugin NAME
eliot plugin-test CMD... [--replay-dir DIR]
eliot hook --harness H --event E
eliot mcp
```

Selectors are defined in [core](core.md#commands).

Inside an agent, the daemon injects `ELIOT_HOME`, `ELIOT_AGENT_ID`,
`ELIOT_AGENT_GENERATION`, `ELIOT_AGENT_TOKEN` and the `eliot` executable on PATH.
Every harness with shell/tool execution can therefore use the same CLI. The CLI never
falls back to owner when a token variable is present but rejected as stale.
