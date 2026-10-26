# Client API and CLI

## Transport/identity

JSON-RPC 2.0 over UTF-8 NDJSON. Current-user-only Windows pipe or Unix `0600` socket
inside private data root. One daemon/data-dir OS lock; `daemon.json` includes
`host_boot_id`, PID and process birth identity.

No token means owner. Any supplied/inherited agent identity must contain matching id,
generation and valid raw token; invalid/incomplete/stale never falls back to owner.
Automation scripts receive a scoped automation token. API frame ≤4 MiB; semantic limits
are smaller.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, boot-scoped custody/source/account holds; exact drain/force |
| agent list/get | paginated current projections, generation/native subject/open effects |
| agent wait/result | exact captured generation/turn/goal terminal result |
| command submit/get | durable linked session/input commands and readback |
| turn get/resolve | exact native/readback result or constrained owner resolution of unknown inactive turn |
| events read/subscribe | bounded persisted replay + live stream with explicit gaps |
| hook event | generation/request-bound synchronous decision |
| discover/attach/detach | canonical native subject; one Agent row |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/goal | scoped automation state/dry run/cancel/manual resolution |

## Command admission

```text
params {
  id?, target?, verb, args,
  expected_generation?, expected_turn_id?, request_id?, wait_ms?
}
result {
  command_id, agent?, expected_generation?, generation?, state, outcome?,
  linked_command?
}
```

One transaction resolves target/selector, validates principal/capability/state and any
expected binding, captures immutable target generation/turn/request, and creates all
rows/events.

Ordinary expected=target current generation. Launch targets 1. Resume expects terminal
g and targets g+1; row/token change only when resume dispatch commits.

Launch/resume with text returns session command + linked send. Without `--wait`, launch
wait means session readiness only. `--wait` with text waits for linked send's exact
turn. Start and initial input never share one outcome.

Interactive calls may omit expected fields; admission captures them. Durable automation
supplies frozen values. Interactive selectors expand at admission; automation already
stores concrete targets.

Same external write id + canonical payload returns original result; different payload
invalid. Internal ids are fixed derived ids.

Errors: `not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`,
`invalid`, `busy`, `unavailable`, `history_gap`, `secret_in_payload`, `internal`.

## Async input, turn outcome and wait

For `send`, vendor transport/inbox acceptance is not an applied Eliot command. The
command remains dispatched until exact native turn/execution association. Then Store
commits the command→turn mapping and command becomes applied. Deadline after possible
inbox admission makes command unknown and blocks later send/configure/goal in that
generation until readback.

A send applied still does not settle turn terminal status. `turn.get` returns
`running|completed|unknown`, ordered input command ids, resolution source, status,
bounded final text and usage. Several input commands may map to one native turn.

If reconnect cannot prove terminal outcome, turn is unknown and live generation remains
blocked from new turn/goal work. Owner may call:

```text
turn.resolve {
  id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason,
  final_text?
}
```

Resolution rejects while native readback says turn still active. It records
`turn.resolved`, never fabricates plugin completion. Historical unknown in terminal old
generation does not block resume.

`agent.wait` captures target generation at call start:

- turn + `after_command`: find that command through `turn_inputs`; if its input is
  dispatched/unknown but not associated, wait/read back that exact input;
- turn without it: current active/unknown turn, or first turn starting after call
  high-water;
- idle: immediate only with no active, unknown or input-association gate;
- stopped: stopped/failed/lost for captured generation;
- goal: done/blocked for captured goal revision.

Several `after_command` waits may resolve from the same terminal turn. Unknown returns
exit 4; replacement generation is stale. Timeout has no cancellation side effect.

## Attach/discover

Discover returns subjects only. Attach looks up unique subject: return existing row,
upgrade observe-only to control after exact native attach, or create one external row.
Another live control path/unverified takeover rejects. Detach changes same row. Native
subject reuse requires new scope.

## Drain/shutdown

Drain commits admission mode first. It rejects new work-starting commands/runs but
allows exact interrupt/stop/request replies/cancel/detach/goal pause-clear, queries,
turn/command/action readback and manual resolution. All pre-boundary work continues;
streams/readback stay alive.

Exit waits for pre-drain effects and owned sessions. Unknown outcomes are blockers until
resolved or explicitly abandoned by owner policy. External/attached sessions may be
detached without kill after admitted work settles. Without drain, shutdown refuses
owned live work. Force kills only exact supported custody, marks ambiguous effects
unknown and never kills attached services.

## Event replay/gaps

Subscribe registers bounded live buffer, captures Store high-water, replays bounded
pages `(after_seq,high_water]`, then releases buffered persisted events in order. No
read transaction spans client wait. Ephemeral replay-window events are best effort.
Overflow emits `events.lagged`.

Store exposes `earliest_seq`. Request before it returns `history_gap`. Explicit
`allow_gap=true` first emits `events.gap` and continues from earliest. Rule cursors use
stricter retention and never opt into silent gap.

## Mail acknowledgement

One open immutable inbox batch/recipient. `mail.inbox` returns same batch until
`mail.ack(batch_id,id)` commits; new messages wait for next batch. CLI writes complete
stdout before ack. Lost inbox leaves unread; lost ack uses same id/readback.
`mail.read ID` is allowed only when no open batch contains it; otherwise acknowledge
the batch. `--all` includes history.

## CLI

```text
eliot daemon [--foreground] | daemon stop [--drain|--force]
eliot status
eliot launch [profile/workspace/tree options] [PROMPT] [--wait]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send|inject A TEXT [--generation N]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show A TURN [--generation N]
eliot turn resolve A TURN --generation N --as completed|interrupted|failed --reason T
eliot interrupt|abort A [--generation N] [--turn ID]
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--generation N] [--wait]
eliot approve|deny|answer ...
eliot configure ... | compact|fork|background ... | goal ...
eliot cmd show|cancel|resolve ...
eliot action show|cancel|resolve ...
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover | attach ... | detach A
eliot service list|stop | worktree remove | doctor
eliot daemon install|uninstall
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Exit codes: 0 success/queued; 1 definite error; 2 usage; 3 rejected before effect; 4
unknown; 5 wait timeout; 6 still pending/dispatched.

Daemon injects home, agent id/generation/raw token and CLI path. Store has token hash
only. Known credentials are masked from native output and rejected in durable semantic
payloads.
