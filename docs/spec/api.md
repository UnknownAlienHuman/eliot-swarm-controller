# Client API and CLI

## Transport and caller identity

- JSON-RPC 2.0 over UTF-8 NDJSON.
- Windows named pipe is current-user ACL only; Unix socket is mode `0600` inside a
  private data directory.
- One daemon owns one data directory and OS lock. `daemon.json` records PID plus process
  birth identity; clients reject stale PID reuse.
- No supplied agent token means owner. A supplied invalid, incomplete or stale token is
  rejected and never downgraded to owner. If any `ELIOT_AGENT_*` variable is present,
  the CLI requires the complete current triple.
- API frame maximum is 4 MiB; command/mail/var limits are smaller.

## Main methods

| Method | Result/semantics |
| --- | --- |
| `daemon.status` | version, uptime, admission mode, Store/WAL health, plugin/custody backends, source gaps, account/provider holds, state/open-effect counts |
| `daemon.shutdown` | exact drain/force semantics below |
| `agent.list/get` | paginated rows; get includes current/target generation, native subject, goals, requests, open commands/actions and source ownership |
| `agent.wait` | captures target generation at call start; replacement generation is stale, not success |
| `agent.result` | bounded completed turns for explicit/current generation |
| `command.submit/get` | durable command admission/readback |
| `events.read/subscribe` | persisted replay plus bounded live stream |
| `hook.event` | generation-bound synchronous hook decision |
| `agent.discover` | native subjects not yet attached/controlled |
| `agent.attach` | return the canonical existing agent row for that native subject, or create one; optionally acquire control after exact attach |
| mailbox/vars/who/claims | coordination methods |
| rule/timer/action methods | automation state, dry run, cancellation and manual resolution |

### `command.submit`

```text
params {
  id?, target?, verb, args,
  expected_generation?, expected_turn_id?, request_id?,
  wait_ms? [10000]
}
result {
  command_id, agent?, expected_generation?, generation?, state, outcome?,
  linked_command?      // initial send created by launch/resume prompt sugar
}
```

Admission is one Store transaction:

1. resolve target/selector;
2. validate authority/capability/state and any expected binding;
3. capture immutable expected/target generation, turn and request;
4. create command rows/events, including linked initial send where requested.

For an ordinary command, expected and target generation are the current generation.
Launch targets generation 1 with no expected generation. Resume validates terminal
`g`, targets `g+1`, and moves the agent row only when resume dispatch commits.

`eliot launch ... PROMPT` and `eliot resume ... PROMPT` return a session command plus a
separate linked send command. Waiting on launch/resume waits for session readiness, not
turn completion. `--wait` with a prompt waits for the linked send's exact turn and
prints that result. A lost session-start reply never silently settles the input.

Interactive callers may omit expected fields; the transaction captures them. Durable
rules/scripts must supply the frozen values from their admitted action. A selector
submitted interactively expands at command admission. Automation selectors are already
expanded at rule firing.

All writes accept an optional external id. Reusing the same id and canonical payload
returns the original row; different payload is invalid. Internal child ids are fixed
length derived ids, not string concatenations.

Stable error codes: `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `internal`,
`secret_in_payload`.

### Attach/discover

`agent.discover` returns native subjects only; it does not create rows. `agent.attach`
looks up the unique (`native_scope`,`native_ref`):

- existing row → return it; an observe-only caller simply subscribes;
- existing observe-only row + control requested → exact plugin attach, then update the
  same row if allowed;
- absent subject → create one external row and attach;
- another live control path or unverified takeover → `not_allowed`.

This prevents duplicate agent rows and duplicate rule firings for one native session.

## Drain and shutdown

`daemon.shutdown {drain:true}` first commits admission mode `draining`.

During drain:

- reject launch/send/steer/inject/resume/fork/background/goal set or resume;
- allow exact interrupt/stop/approve/deny/answer/cancel/detach/goal pause or clear,
  queries and manual unknown resolution;
- admit no new rule/timer/goal rounds and dispatch no newly admitted automation action;
- continue all work admitted before the drain boundary, including queued sends and
  action chains;
- keep native streams, requests, results and readback alive.

Exit waits for pre-drain work to settle, process actions to end, and daemon-owned
sessions to end/stop. Unknown effects are reported blockers; drain never fabricates a
resolution. Without drain, shutdown refuses while owned work is live. `force` kills
only exact supported custody groups/sessions, marks ambiguous outcomes unknown, and
never kills attached owner services.

## Event subscription boundary

The daemon registers a bounded live buffer, captures Store high-water `seq`, replays
persisted pages through it, then releases buffered persisted events above the
watermark in order. It never holds a SQLite read transaction while waiting on a client.
Ephemeral events buffered during replay are best effort and have no total-order claim.
Overflow emits `events.lagged`; it is never silent.

## Mail read semantics

`mail.inbox` returns unread messages and a stable delivery batch id. It does not mark
them read until the same client acknowledges that batch (`mail.ack`, used automatically
by the CLI after writing complete stdout successfully). A lost reply therefore leaves
the batch unread and recoverable. `mail.read ID` is an explicit one-message read/ack.
`--all` includes acknowledged history.

## CLI

Every write supports `--id`; the CLI generates and prints one otherwise. Delayed
scripts may pass `--generation`, `--turn` and `--request`. User-facing commands:

```text
eliot daemon [--foreground]
eliot daemon stop [--drain | --force]
eliot status
eliot launch [profile/workspace/tree options] [PROMPT] [--wait]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send|inject A TEXT [--generation N]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--timeout DUR]
eliot result A [--generation N] [--turns N]
eliot interrupt|abort A [--generation N] [--turn ID]
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--generation N] [--wait]
eliot approve|deny A [--request R] [--remember] [--generation N]
eliot answer A [TEXT] [--request R] [--choice N] [--generation N]
eliot configure A [--model M] [--effort E] [--permissions P]
eliot compact|fork|background A
eliot goal set|pause|resume|clear|show ...
eliot cmd show|cancel|resolve ...
eliot action show|cancel|resolve ...
eliot events ...
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var get|set|list|watch ...
eliot who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover | attach ... | detach A
eliot service list|stop | worktree remove | doctor
eliot daemon install|uninstall
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | query ok; effect applied/queued; cancellation succeeded |
| 1 | definite error/failure |
| 2 | CLI usage error |
| 3 | rejected before native/external effect |
| 4 | outcome unknown |
| 5 | wait timed out |
| 6 | still dispatched/pending when the CLI stopped waiting |

The daemon injects `ELIOT_HOME`, agent id, generation, raw token and `eliot` on PATH.
The Store contains only token hash. Known token/credential values are masked from native
output and rejected from durable user payloads.
