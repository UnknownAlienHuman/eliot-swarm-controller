# Client API and CLI

## Transport/identity

JSON-RPC 2.0 over UTF-8 NDJSON. Current-user-only Windows pipe or Unix `0600` socket
inside private data root. One daemon/data-dir OS lock; `daemon.json` includes PID +
process birth identity.

No token means owner. Any supplied/inherited agent identity must contain matching id,
generation and valid raw token; invalid/incomplete/stale never falls back to owner.
Automation scripts receive a scoped automation token. API frame ≤4 MiB; semantic limits
are smaller.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, custody/source/account holds; exact drain/force |
| agent list/get | paginated current projections, generation/native subject/open effects |
| agent wait/result | exact captured generation/turn/goal terminal result |
| command submit/get | durable linked session/input commands and readback |
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

Ordinary expected=target current generation. Launch has no expected generation and
targets 1. Resume expects terminal g and targets g+1; row/token change only when resume
dispatch commits.

Launch/resume with text returns session command + linked send. Waiting without `--wait`
means session readiness only. `--wait` with text waits for the linked command's exact
turn and prints it. Start and initial input never share one outcome.

Interactive calls may omit expected fields; the admission transaction captures them.
Durable automation supplies frozen values. Interactive selectors expand at admission;
automation already stores concrete targets.

Same external write id + canonical payload returns original result; different payload
is invalid. Internal ids are fixed derived ids.

Errors: `not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`,
`invalid`, `busy`, `unavailable`, `history_gap`, `secret_in_payload`, `internal`.

## Wait semantics

`agent.wait` captures each target's generation at call start.

- `turn` with `after_command`: wait for the exact turn correlated to that command;
- `turn` without it: if busy/waiting, wait for current active turn; if idle, wait for
  the first turn starting after the call's event high-water;
- `idle`: immediate if captured generation is idle;
- `stopped`: stopped/failed/lost for captured generation;
- `goal`: done/blocked for captured goal revision.

Replacement generation returns `stale_generation`, never success for different work.
Timeout has no cancellation side effect.

## Attach/discover

Discover returns subjects only. Attach looks up unique subject: return existing row,
upgrade observe-only to control after exact native attach, or create one external row if
absent. Another live control path/unverified takeover rejects. Detach changes the same
row. Native subject reuse requires a new scope.

## Drain/shutdown

Drain commits admission mode first. It rejects new work-starting commands/runs but
allows exact interrupt/stop/request replies/cancel/detach/goal pause-clear, queries and
manual resolution. All pre-boundary work continues, including queued sends/action
chains; streams/readback stay alive.

Exit waits for pre-drain effects and daemon-owned sessions. Unknown outcomes are
blockers. Once admitted commands settle, external/attached service sessions may be
detached without killing them. Without drain, shutdown refuses owned live work. Force
kills only exact supported custody, marks ambiguous effects unknown and never kills
attached owner services.

## Event replay/gaps

Subscribe registers bounded live buffer, captures Store high-water, replays bounded
pages `(after_seq,high_water]`, then releases buffered persisted events in order. No
read transaction spans client wait. Ephemeral replay-window events are best effort.
Overflow emits `events.lagged`.

Store exposes `earliest_seq`. If caller requests an already-pruned sequence, default is
`history_gap {requested,earliest}`. With explicit `allow_gap=true`, stream first emits
`events.gap` and continues from earliest. No silent jump. Rule cursors use stricter
retention coordination and never opt into a silent gap.

## Mail acknowledgement

One open immutable inbox batch/recipient. `mail.inbox` repeatedly returns it until
`mail.ack(batch_id,id)` commits; newly arrived messages wait for next batch. CLI writes
complete stdout before ack. Lost inbox leaves unread; lost ack uses same id/readback.
`mail.read ID` explicitly acknowledges one message. `--all` includes acknowledged
history.

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

Daemon injects home, agent id/generation/raw token and CLI path. Store has only token
hash. Known credentials are masked from native output and rejected in durable semantic
payloads.
