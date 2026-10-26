# Client API and CLI

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows pipe is current-user-only; Unix socket is mode
`0600` inside the private data root. One daemon owns one data-directory lock;
`daemon.json` includes `host_boot_id`, PID and process birth identity.

Every connected client presents exactly one explicit principal credential:

- owner: `eliot --owner ...` reads the private `owner.token` and sends it;
- Agent: complete `(agent_id,generation,agent_token)` triple;
- managed automation/check/setup process: complete scoped automation token;
- plugin connection: daemon-created instance credential on the private plugin pipe.

Missing, incomplete, revoked or mismatched credentials reject. There is **no** no-token
owner fallback and no process/PID classification grants API authority. This is a
correctness and attribution boundary inside one trusted OS user—not protection against a
malicious same-user process that deliberately reads another private file.

API frame maximum is 4 MiB; semantic fields are smaller. Handshake/status returns
`write_id_floor_ms`, `idempotency_window_ms`, `write_id_future_skew_ms`, page limits and
Store/WAL/routing-key health.

## External write IDs

Every external daemon mutation carries a canonical ULID `write_id` generated before
send. CLI uses a locked, atomically replaced monotonic state and advances to at least the
daemon floor + 1. `--id` accepts a canonical ULID. Internal automation uses derived ids.

Receipt canonicalization includes semantic mutation fields only: method, principal,
target/selector, verb/action, normalized args, expected bindings and probe choice. It
excludes JSON-RPC id, connection metadata, wait/display/pager/client-timeout options.
Thus one intended mutation may retry with another `wait_ms` or `--json` using the same
id.

Receipt lookup follows [core](core.md#external-write-idempotency): same id/payload
returns retained result; mismatch `id_conflict`; absent old/below-floor id `id_expired`;
absent excessive-future id `id_future`. Lost reply reuses the same id. Internal rule/
timer/Goal/action, linked-input and selector-child writes use `d_…` ids and no fake
receipts.

Daemon bootstrap and OS integration (`eliot daemon`, install/uninstall) are
out-of-process owner desired-state operations with exact OS readback, not Store
mutations. Connected `daemon.shutdown` is receipted; a lost success is read after the
next explicit start.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, write-id floor, routing key and custody/source/account holds |
| agent list/get | paginated projections, generation/binding/native subject/open effects |
| agent wait/result | exact captured generation/turn/Goal result |
| command submit/get/children | parent/child/session/input commands, paginated captured fanout and readback |
| turn get/resolve | native/readback result or constrained owner assertion |
| events read/subscribe | paged replay + bounded live stream with gaps |
| hook event | internal deadline-bound committed proposal or safe default |
| discover/attach/detach | paged native subjects; one canonical Agent row with explicit observe/control binding |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/Goal | scoped automation state/dry run/cancel/readback/assertion |

Every external mutating daemon method accepts `write_id`. Internal plugin/source/hook
requests use their own exact instance/open/sequence/hook identities. Reads allocate no
receipts.

## Command admission

```text
params {
  write_id, target?, selector?, verb, args,
  expected_generation?, expected_turn_id?, request_id?,
  probe? [false], wait_ms?
}
result {
  command_id, agent?, expected_generation?, generation?, state, outcome?,
  linked_command?, fanout?: { count, digest }
}
```

External concrete/fanout parent command has `command_id=write_id` and references its
receipt. Internal automation has a derived id and run/action cause.

One transaction resolves target/selector, validates principal/capability/state and
expected binding, captures generation/turn/request, inserts receipt where applicable,
and creates command/link/event rows.

Ordinary expected generation equals current target. Launch targets 1. Resume expects
terminal g and targets g+1; Agent generation/routing change only when dispatch commits.

Launch/resume with text returns the session command plus a derived linked send. Without
`--wait`, launch waits for session readiness only. `--wait` with text follows linked send
to its exact terminal turn. Session start, input association and terminal turn never
share one outcome.

### Selector fanout

Interactive selector creates one external Store-only parent and one fixed derived child
per concrete target captured in the admission transaction. Expansion:

- is sorted deterministically by AgentId;
- fails before any row above `limits.max_selector_targets`;
- stores child links and parent `{count,digest}`;
- marks parent applied when expansion commits, saying nothing about child effects.

Children settle independently. Repeating parent WriteId returns the same count/digest.
`command.children {command,after?,limit<=1000}` pages the captured set. `--wait` waits
that set and returns a paged/per-child summary. After link retention expires, children
return `history_gap` while receipt summary still prevents replay.

Interactive concrete calls may omit expected fields; admission captures them. Durable
automation supplies frozen values. Internal child/linked commands need no receipt.

Stable errors include `missing_principal`, `invalid_principal`, `not_found`,
`unsupported`, `not_allowed`, `bad_state`, `stale_generation`, `invalid`, `busy`,
`unavailable`, `capacity_unavailable`, `id_conflict`, `id_expired`, `id_future`,
`history_gap`, `secret_in_payload`, `frame_too_large`, `internal`.

### Explicit provider probe

`probe=true` is valid only for exact send or launch/resume with a linked prompt. A
matching hold must be past its retry boundary and one command atomically claims it. Only
owner/current directly routed Agent may request it; rules/timers/wakes/parent notices/
Goal may not. For launch/resume the linked input—not session creation—is the probe. Hold
clears only from that exact successful provider turn; unknown keeps hold/probe.

## Async input, turn outcome and wait

Vendor inbox acceptance is not `send applied`. `input.admitted` stores optional native
input id while command remains dispatched. Command applies only at exact command→turn or
execution association. Deadline after possible admission makes it unknown and blocks
later send/configure/Goal in that generation pending readback.

Applied send does not settle the turn. `turn.get` returns `running|completed|unknown`,
ordered input command ids, optional native input ids, resolution source, status, bounded
final text and usage. Several input commands may map to one native turn.

If reconnect cannot prove terminal outcome, the turn is unknown and a live generation
remains blocked. Owner assertion is allowed only when native readback proves not active
or generation is terminal:

```text
turn.resolve {
  write_id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason, final_text?
}
```

It records `resolution_source=owner`, never plugin completion. Historical unknown in an
old terminal generation does not block resume.

`agent.wait` captures target generation:

- turn + `after_command`: follow input then turn mapping;
- turn without it: active/unknown turn or first turn after call high-water;
- idle: only with no active/unknown turn or input gate;
- stopped: stopped/failed/lost for captured generation;
- Goal: done/blocked for captured Goal revision.

Several waits may resolve from one turn. Unknown exits 4; replacement generation is
stale; timeout never cancels.

## Unknown-effect resolution

Automatic settlement is verb/action-specific readback. Generic owner assertions cannot
synthesize identity or free unsafe lanes:

- start applied requires exact attach/ready subject;
- send applied requires turn association; failed requires exact pre-turn drop/not-found;
  otherwise `unknown_abandoned` retains the input gate;
- interrupt/stop require exact turn/generation readback;
- Git/webhook/script assertions are `owner_assertion` and release a narrow hold only
  with `--unsafe-assert`.

Rules/timers/scripts/Agents cannot unsafe-assert. Every assertion stores reason,
principal and timestamp.

## Synchronous hook event

```text
hook.event {
  hook_id, harness, hook,
  agent, generation, turn_id?, request_id,
  native_deadline_at, payload
}
→ { decision: allow|deny|ask|..., proposal_id? }
```

This is an internal authenticated plugin/hook call, not an external WriteId mutation.
`hook_id` is the stable native invocation. Daemon derives proposal id, validates exact
pending request and evaluates bounded synchronous rules. Non-default response requires
proposal commit before the internal margin while request remains pending. Proposal has
no dispatcher and acts only through the immediate response. Same-id retry before expiry
may return it; timeout/Store failure/expiry/uncertain response returns `ask` for decision
hooks (`allow` only for observation). Late proposal never approves later.

## Discover, attach and detach

`agent.discover {harness?,cursor?,limit<=1000}` is paginated and each result frame must
remain below the API frame cap. It returns native subjects and bounded metadata only; it
does not create Agent rows.

`agent.attach` is owner-only in the first local slice:

```text
attach {
  write_id, harness, native_scope, native_ref,
  mode: observe | control,
  profile?, project?, name?
}
```

- existing canonical row → return it after exact readback;
- absent + observe → create generation 1 external row with synthetic
  `observe:<harness>` binding, `observe_only=true`, `routing_enabled=false`;
- observe row + control → require an owner-selected frozen profile, project/profile
  authority, exact attach/readback, compatible workspace/route/permissions and an
  available profile/account/proxy capacity reservation; then update the same row;
- existing other live control path or unverified takeover → `not_allowed`.

Control attach may remain routing-disabled when the native API can control the session
but cannot inject a distinct Eliot environment. When direct routing is qualified, the
same attach transaction increments token version, enables routing, installs a new hash
and returns the derived token. Retrying the same WriteId returns the same binding/version.
If capacity is unavailable, control attach fails without changing the observe row.

Detach requires independently owned/observable service lifetime. It sets observe-only,
disables routing, revokes token and releases claims without changing generation or
killing session. A reservation for the still-live provider session remains until exact
terminal observation; detach does not pretend account capacity was freed. Daemon-owned
per-agent process cannot detach. Detach does not settle unknown input.

## Drain and shutdown

Drain commits admission mode, rejects new work but permits exact interrupt/stop/request
replies/cancel/detach/Goal pause-clear, queries/readback/assertions. Pre-boundary work
continues.

Exit waits for effects/sessions. Unknown blocks unless safely resolved/abandoned.
Attached session may detach after admitted work settles. `--force` kills only exact
backends. Degraded remains a blocker; `--abandon-degraded` records possible orphans and
is not clean success.

## Event replay and gaps

Subscribe registers a bounded live buffer, captures Store high-water, replays pages
`(after_seq,high_water]`, then releases buffered persisted events in order. No read
transaction spans client wait. Ephemeral events are best effort; overflow emits lag.

Request before `earliest_seq` returns `history_gap`. `allow_gap=true` emits a gap then
continues. Rule cursors never silently gap.

## Mail acknowledgement

One open immutable inbox batch exists per recipient. `mail.inbox` returns it until a
receipted `mail.ack`; new messages wait for the next batch. CLI writes complete stdout
before ack. Lost inbox leaves unread; lost ack reuses WriteId. `mail.read` is receipted
and prohibited for a message inside an open batch. History is paginated.

## CLI

Every connected mutation accepts `--id ULID`; CLI generates/persists/prints it before
send. Owner calls explicitly use `--owner`; Agent-injected CLI supplies its Agent
credential automatically. Bootstrap/install uses OS desired-state readback.

```text
eliot --owner daemon stop [--drain|--force|--abandon-degraded] [--id ULID]
eliot --owner status
eliot --owner launch [profile/workspace/tree options] [PROMPT] [--probe] [--wait] [--id ULID]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send|inject A TEXT [--generation N] [--probe] [--id ULID]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID] [--id ULID]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show A TURN [--generation N]
eliot --owner turn resolve A TURN --generation N --as completed|interrupted|failed --reason T --id ULID
eliot interrupt|abort A [--generation N] [--turn ID] [--id ULID]
eliot stop A [--force] [--tree] [--generation N] [--id ULID]
eliot resume A [PROMPT] [--generation N] [--probe] [--wait] [--id ULID]
eliot approve|deny|answer ... [--id ULID]
eliot configure ... | compact|fork|background ... | goal ... [--id ULID]
eliot cmd show|children|cancel|resolve|abandon ...
eliot action show|cancel|resolve ...
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot --owner discover [--cursor C] [--limit N]
eliot --owner attach --harness H SCOPE/REF --observe [--id ULID]
eliot --owner attach --harness H SCOPE/REF --control --profile P [--id ULID]
eliot detach A [--id ULID]
eliot --owner service list|stop | worktree remove | doctor
eliot daemon [--foreground] | daemon install | daemon uninstall
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Exit codes: 0 success/queued; 1 definite error; 2 usage; 3 rejected before effect; 4
unknown; 5 wait timeout; 6 still pending/dispatched.

Daemon injects home, Agent id/generation/raw token and CLI path only when
`routing_enabled=true`. Routing-disabled native children receive no independent Agent
credential. Store contains token hash/version only. Known credentials are masked from
native output and rejected in durable semantic payloads.
