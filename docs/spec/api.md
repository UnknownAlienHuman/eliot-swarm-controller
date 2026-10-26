# Client API and CLI

## Transport and caller identity

JSON-RPC 2.0 over UTF-8 NDJSON. Current-user-only Windows pipe or Unix `0600` socket
inside a private data root. One daemon owns one data-dir lock; `daemon.json` includes
`host_boot_id`, PID and process birth identity.

Caller resolution uses both credentials and OS peer custody:

1. Windows named-pipe client PID / Linux Unix-socket peer PID is mapped to a current
   Eliot custody owner when available;
2. a process in agent/service/action custody can never become owner merely by omitting
   or clearing environment variables;
3. agent and scoped-automation callers provide the matching raw token; invalid,
   incomplete or stale identity is rejected;
4. no token means owner only when the peer belongs to no controlled custody and is the
   local OS user.

Shared-service tools still need the per-session token because a server PID is not a
session identity. This is a correctness fence, not protection against an arbitrary
malicious same-user process outside Eliot custody.

API frame ≤4 MiB; semantic limits are smaller.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, boot-scoped custody/source/account holds; exact drain/force |
| agent list/get | paginated current projections, generation/native subject/open effects |
| agent wait/result | exact captured generation/turn/goal terminal result |
| command submit/get | durable linked session/input commands and readback |
| turn get/resolve | exact native/readback result or constrained owner assertion for unknown inactive turn |
| events read/subscribe | bounded persisted replay + live stream with explicit gaps |
| hook event | generation/request-bound synchronous proposal/decision |
| discover/attach/detach | canonical native subject; one Agent row |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/goal | scoped automation state/dry run/cancel/readback/manual assertion |

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
g and targets g+1; row/token changes only when resume dispatch commits.

Launch/resume with text returns session command + linked send. Without `--wait`, launch
wait means session readiness only. `--wait` with text waits for linked send's exact
turn. Start and initial input never share one outcome.

Interactive calls may omit expected fields; admission captures them. Durable automation
supplies frozen values. Interactive selectors expand at admission; automation already
stores concrete targets.

Same external write id + canonical payload returns original result; different payload
is invalid. Internal ids are fixed derived ids.

Errors: `not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`,
`invalid`, `busy`, `unavailable`, `history_gap`, `secret_in_payload`, `internal`.

## Async input, turn outcome and wait

For `send`, vendor transport/inbox acceptance is not an applied Eliot command.
`input.admitted` stores the exact native input id while the command remains dispatched.
The command becomes applied only when that input is associated with an exact native
turn/execution. Deadline after possible admission makes it unknown and blocks later
send/configure/goal in that generation until readback.

A send applied still does not settle the turn. `turn.get` returns
`running|completed|unknown`, ordered input command ids, resolution source, status,
bounded final text and usage. Several input commands may map to one native turn.

If reconnect cannot prove terminal outcome, the turn is unknown and the live generation
remains blocked from new turn/goal work. Owner may make an explicit assertion only when
native readback proves the turn is no longer active, or the generation is terminal:

```text
turn.resolve {
  id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason,
  final_text?
}
```

It records `resolution_source=owner`; it never fabricates a plugin completion. Historical
unknown in a terminal old generation does not block resume.

`agent.wait` captures target generation at call start:

- turn + `after_command`: follow native input then `turn_inputs`; if dispatched/unknown
  but not associated, wait/read back that exact input;
- turn without it: current active/unknown turn, or first turn starting after call
  high-water;
- idle: immediate only with no active/unknown turn or input-association gate;
- stopped: stopped/failed/lost for captured generation;
- goal: done/blocked for captured goal revision.

Several waits may resolve from one terminal native turn. Unknown returns exit 4;
replacement generation is stale. Timeout has no cancellation side effect.

## Unknown effect resolution

Automated resolution is verb/action-specific exact readback. Generic owner assertions
are break-glass and cannot synthesize missing native identity or silently free unsafe
lanes:

- start `applied` still requires exact attach/ready native subject;
- send `applied` requires exact turn association; `failed` requires exact pre-turn
  drop/not-found. An owner assertion without that evidence records
  `unknown_abandoned` and keeps the generation's input lane blocked; stop/resume is the
  safe recovery path;
- interrupt/stop applied/failed requires exact bound turn/generation readback;
- Git/webhook/script assertions are labelled `owner_assertion` and release their narrow
  hold only with an explicit `--unsafe-assert` flag.

No rule, timer, script or agent may use unsafe assertion. Every assertion stores reason,
principal and timestamp.

## Attach/discover

Discover is paginated and returns native subjects only. Attach looks up the unique
subject: return existing row, upgrade observe-only to control after exact native attach,
or create one external row. Another live control path or unverified takeover rejects.
Detach changes the same row and revokes its routing token. Native subject reuse requires
a new scope.

## Drain and shutdown

Drain commits admission mode first. It rejects new work-starting commands/runs but
allows exact interrupt/stop/request replies/cancel/detach/goal pause-clear, queries,
readback and constrained manual assertions. Pre-boundary work continues; streams and
readback remain alive.

Exit waits for pre-drain effects and owned sessions. Unknown outcomes remain blockers
unless safely resolved or explicitly abandoned. External/attached sessions may detach
without kill after admitted work settles.

`--force` kills only exact custody/session backends. Degraded custody remains a visible
blocker; the daemon never reports clean shutdown after a best-effort process-group kill.
A separate explicit `--abandon-degraded` may exit while recording possible orphaned
processes for the next `doctor`, but it is not success and never kills attached services.

## Event replay and gaps

Subscribe registers a bounded live buffer, captures Store high-water, replays bounded
pages `(after_seq,high_water]`, then releases buffered persisted events in order. No
read transaction spans client wait. Ephemeral replay-window events are best effort.
Overflow emits `events.lagged`.

Store exposes `earliest_seq`. Request before it returns `history_gap`. Explicit
`allow_gap=true` first emits `events.gap` and continues from earliest. Rule cursors use
stricter retention and never opt into a silent gap.

## Mail acknowledgement

One open immutable inbox batch per recipient. `mail.inbox` returns the same batch until
`mail.ack(batch_id,id)` commits; new messages wait for the next batch. CLI writes
complete stdout before ack. Lost inbox leaves unread; lost ack uses the same id/readback.
`mail.read ID` is allowed only when no open batch contains it. `--all` is paginated.

## CLI

```text
eliot daemon [--foreground] | daemon stop [--drain|--force|--abandon-degraded]
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
eliot cmd show|cancel|resolve|abandon ...
eliot action show|cancel|resolve ... [--unsafe-assert]
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover [--cursor C] [--limit N] | attach ... | detach A
eliot service list|stop | worktree remove | doctor
eliot daemon install|uninstall
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Exit codes: 0 success/queued; 1 definite error; 2 usage; 3 rejected before effect; 4
unknown; 5 wait timeout; 6 still pending/dispatched.

Daemon injects home, agent id/generation/raw token and CLI path. Store has token hash
only. Known credentials are masked from native output and rejected in durable semantic
payloads.
