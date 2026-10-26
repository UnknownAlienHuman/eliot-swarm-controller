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
  them automatically. An invalid/stale supplied token is rejected, never downgraded to
  owner. This is correctness/attribution, not an OS security boundary.
- Maximum API frame is 4 MiB; semantic command text has the smaller core limit.
  Requests are multiplexed by JSON-RPC id.

## Methods

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | — | version, uptime, admission mode, plugins/custody, source health/gaps, counts by agent and open command/action state |
| `daemon.shutdown` | `drain?`, `force?` | See [drain](#drain-and-shutdown). |
| `agent.list` | `filter{state?, harness?, project?, label?, tree?, parent?}`, `after?`, `limit≤1000` | page + `next`; every row includes generation |
| `agent.get` | `agent` | generation/native refs, pending requests, open commands/actions, goals, source owners/gaps and recent events |
| `agent.wait` | `targets`, `for: turn|idle|stopped|goal`, `generation?`, `after_command?`, `timeout_ms` | Per target result. Default generation is captured at call start; replacement generation returns `stale_generation`. `after_command` waits for the exact turn correlated to that command. |
| `agent.result` | `agent`, `generation?`, `turns?` | bounded completed-turn results for selected generation (default current) |
| `command.submit` | `id?`, `target?`, `verb`, `args`, `expected_generation?`, `expected_turn_id?`, `request_id?`, `wait_ms?` | `{command_id, agent?, generation?, state, outcome?}` |
| `command.get` | `id` | command including bound generation/turn/request |
| `events.read` | `after_seq`, `filter`, `limit` | persisted events page |
| `events.subscribe` | `after_seq?`, `filter` | replay then live stream with explicit lag notification |
| `hook.event` | `agent`, `generation`, `harness`, `hook`, `payload` | exact hook decision |
| `agent.discover` | `harness?` | native sessions found by plugins |
| `agent.attach` | `harness`, `native_scope`, `native_ref`, `observe_only` | new external agent generation 1, or control conflict |
| `mail.send` | `to`, `text`, `mode?`, `reply_to?` | message ids |
| `mail.inbox` / `mail.read` | wait/filter or message id | returned messages and read state |
| `var.get/set/list/watch` | scope/key/value/version/prefix | value/version or stream |
| `who` | `paths`, `history?` | per-agent rows |
| `claim.add/release` | `glob`, `note?`, `ttl?` | claim/conflicts |
| `rule.list/get/enable/disable/test` | `id`, `event?` | rules/dry-run rendering |
| `rules.pause/resume` | — | global automation switch |
| `timer.list/run` | `id` | schedules/manual run |
| `action.get/resolve/cancel` | run/index/outcome/reason | automation action state/manual resolution |

Goals are commands. Event filter is
`{kinds?:[glob], agents?:[id], tree?:id, sources?:[glob], ephemeral?:bool}`.

### Exact command admission

For an agent command, the router performs one Store transaction:

1. resolve target/selector to concrete agent rows;
2. compare any caller-supplied expected binding;
3. capture the accepted generation and, for current-turn/request verbs, exact active
   turn/request;
4. persist immutable command rows/events.

`expected_generation` is optional for an interactive owner/agent call; when omitted,
the transaction captures the current generation. Durable automation **always**
supplies the generation captured when its run was admitted. `expected_turn_id` is
required internally for delayed steer/interrupt, and `request_id` for delayed
approval/answer. A mismatch fails `stale_generation` or `bad_state` before native I/O.

A selector command submitted interactively expands at that admission transaction.
Automation selectors are already expanded into concrete target actions at rule firing,
so a delayed run cannot pick up a newly created/replacement agent.

Every write method accepts optional `id`. Same id + same canonical payload returns the
first admission/result; same id + different payload is `invalid`. This idempotence ends
at Eliot's transaction boundary and never authorizes repeated native I/O.

Stable error `data.code`: `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `internal`.

## Drain and shutdown

`daemon.shutdown {drain:true}` first commits admission mode `draining`.

While draining:

- reject new work-starting commands: launch, send, steer, inject, resume, fork,
  background, goal set/resume;
- admit interrupt, stop, approve, deny, answer, cancel, detach, goal pause/clear,
  queries and exact manual resolution;
- admit no new rule/timer/goal-loop runs and dispatch no newly created automation
  action;
- continue commands/actions admitted before the mode change, including queued sends;
- keep event streams, requests, answers, results and readback alive.

Exit waits for pre-drain work to become terminal, no process action to remain running,
no unresolved unknown to block custody, and daemon-owned sessions to end or be stopped.
Status reports blockers.

Without drain, shutdown refuses while daemon-owned sessions/process actions are live.
`force` terminates exact owned groups, marks ambiguous in-flight effects unknown and
sessions lost, then exits. It never kills attached owner-run services.

## Subscription boundary

`events.subscribe` registers the bounded live buffer, captures persisted high-water
`seq`, replays `(after_seq, high_water]`, then releases buffered persisted events above
it in order. Ephemeral events seen during replay are best-effort and delivered after
the persisted boundary without a total-order claim. Overflow reports `events.lagged`;
it is never silent.

## CLI

All commands accept `--json` and `--data-dir`. Every write accepts `--id`; the CLI
generates/prints one otherwise. Retry of a lost client reply reuses the same id.
Interactive CLI commands normally omit expected bindings—the daemon captures them
atomically. Scripts may pass `--generation`, `--turn` and `--request` when they need a
stale-safe delayed action.

`eliot daemon` background-starts itself and returns only after a live `daemon.json`
with matching process identity appears. Other commands never auto-start it.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | query ok; command/action applied or queued; cancellation succeeded |
| 1 | definite error/failure |
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
eliot send|inject A TEXT [--generation N]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID]
eliot send A TEXT --wait [--timeout DUR]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--timeout DUR]
eliot result A [--generation N] [--turns N]
eliot interrupt A [--generation N] [--turn ID] | eliot abort A
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--generation N]
eliot approve|deny A [--request R] [--remember] [--generation N]
eliot answer A [TEXT] [--request R] [--choice N] [--generation N]
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

Inside an agent, the daemon injects `ELIOT_HOME`, `ELIOT_AGENT_ID`,
`ELIOT_AGENT_GENERATION`, `ELIOT_AGENT_TOKEN` and `eliot` on PATH. The CLI never
falls back to owner when token variables are present but invalid/stale.
