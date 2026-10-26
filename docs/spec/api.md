# Client API and CLI

## Transport and caller identity

JSON-RPC 2.0 over UTF-8 NDJSON. The Windows named pipe is current-user-only; the Unix
socket is mode `0600` inside a private data root. One daemon owns one data-dir lock;
`daemon.json` includes `host_boot_id`, PID and process birth identity.

Caller resolution uses both credentials and **live OS custody classification**:

1. obtain the peer PID from the named pipe or Unix socket and pin its current
   `(host_boot_id,pid,start_time)`;
2. if any agent/automation token fields are supplied or inherited, require the complete
   matching identity and reject invalid, incomplete, revoked or stale values;
3. map the peer through the persisted process projection when an exact row exists;
4. when no row exists, check the pinned peer live against every current Eliot custody
   handle/cgroup/process group before considering owner fallback;
5. recheck peer birth/liveness after classification;
6. no token means owner only when the peer is the local OS user and the live check
   proves it is not a member of any current Eliot custody.

A projection miss is never proof of independence: a newly spawned descendant may
connect before process registration. Any unavailable peer credential, membership
access failure, birth race or incomplete custody inventory fails closed with
`caller_identity_unknown`. A controlled agent/action/service process cannot become
owner by clearing environment variables. Shared-service tools still need the exact
per-session token because a service PID is not an agent identity.

This is an attribution/correctness fence inside the declared one-user trust boundary,
not protection against an arbitrary malicious same-user process outside Eliot custody.

API frame maximum is 4 MiB; semantic limits are smaller.

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

Ordinary expected generation equals target current generation. Launch targets 1. Resume
expects terminal g and targets g+1; the row and token change only when resume dispatch
commits.

Launch/resume with text returns a session command plus a linked send. Without `--wait`,
a launch wait means session readiness only. `--wait` with text waits for the linked
send's exact turn. Start and initial input never share one outcome.

Interactive calls may omit expected fields; admission captures them. Durable automation
supplies frozen values. Interactive selectors expand at admission; automation already
stores concrete targets.

The same external write id plus canonical payload returns the original result; a
different payload is invalid. Internal ids are fixed derived ids.

Stable errors include `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `caller_identity_unknown`,
`history_gap`, `secret_in_payload`, `internal`.

## Async input, turn outcome and wait

For `send`, vendor transport/inbox acceptance is not an applied Eliot command.
`input.admitted` stores the exact native input id while the command remains dispatched.
The command becomes applied only when that input is associated with an exact native
turn/execution. A deadline after possible admission makes it unknown and blocks later
send/configure/goal work in that generation until readback.

A send applied still does not settle the turn. `turn.get` returns
`running|completed|unknown`, ordered input command ids, resolution source, status,
bounded final text and usage. Several input commands may map to one native turn.

If reconnect cannot prove terminal outcome, the turn is unknown and the live generation
remains blocked from new turn/goal work. The owner may make an explicit assertion only
when native readback proves the turn no longer active, or the generation is terminal:

```text
turn.resolve {
  id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason,
  final_text?
}
```

It records `resolution_source=owner`; it never fabricates plugin completion. Historical
unknown work in a terminal old generation does not block resume.

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

## Unknown-effect resolution

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
  hold only with explicit `--unsafe-assert`.

No rule, timer, script or agent may use unsafe assertion. Every assertion stores reason,
principal and timestamp.

## Attach and discover

Discover is paginated and returns native subjects only. Attach looks up the unique
subject: return the existing row, upgrade observe-only to control after exact native
attach, or create one external row. Another live control path or unverified takeover
rejects. Detach changes that same row, sets observe-only and revokes its token without
changing generation. Native subject reuse requires a new scope.

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
processes for the next `doctor`, but it is not success and never kills attached
services.

## Event replay and gaps

Subscribe registers a bounded live buffer, captures Store high-water, replays bounded
pages `(after_seq,high_water]`, then releases buffered persisted events in order. No
read transaction spans client wait. Ephemeral replay-window events are best effort.
Overflow emits `events.lagged`.

Store exposes `earliest_seq`. A request before it returns `history_gap`. Explicit
`allow_gap=true` first emits `events.gap` and continues from earliest. Rule cursors use
stricter retention and never opt into a silent gap.

## Mail acknowledgement

One open immutable inbox batch exists per recipient. `mail.inbox` returns the same batch
until `mail.ack(batch_id,id)` commits; new messages wait for the next batch. The CLI
writes complete stdout before ack. A lost inbox reply leaves messages unread; a lost ack
uses the same id/readback. `mail.read ID` is allowed only when no open batch contains
it. `--all` is paginated.

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

The daemon injects home, agent id/generation/raw token and CLI path. The Store has only
the current token hash. Known credentials are masked from native output and rejected
in durable semantic payloads.
