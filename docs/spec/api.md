# Client API and CLI

## Transport and caller identity

JSON-RPC 2.0 over UTF-8 NDJSON. The Windows named pipe is current-user-only; the Unix
socket is mode `0600` inside a private data root. One daemon owns one data-dir lock;
`daemon.json` includes `host_boot_id`, PID and process birth identity.

Caller resolution uses credentials plus **live OS custody classification**:

1. obtain peer PID from pipe/socket and pin `(host_boot_id,pid,start_time)`;
2. if any agent/automation identity fields are supplied or inherited, require the
   complete matching identity and reject invalid, incomplete, revoked or stale values;
3. use the exact persisted process row as a fast path when present;
4. on projection miss, check the pinned peer live against every current Eliot Job,
   cgroup or registered degraded process group before owner fallback;
5. recheck peer birth/liveness after classification;
6. no token means owner only when the peer is the local OS user and definitively outside
   every current Eliot custody.

Any unavailable peer credential, membership access failure, birth race or incomplete
custody inventory fails `caller_identity_unknown`. A projection miss never proves
independence. A shared-service tool still needs its exact per-session token because a
service PID is not an agent identity. This is an attribution fence inside the declared
one-user trust boundary, not hostile same-user isolation.

API frame maximum is 4 MiB; semantic limits are smaller. Handshake/status returns
`write_id_floor_ms`, `idempotency_window_ms` and `write_id_future_skew_ms`.

## External write IDs

Every mutation initiated through a running daemon by owner/agent/API carries
`write_id`, a canonical 26-character ULID generated **before** sending. The CLI uses a
locked, atomically replaced current-user-only monotonic generator state and advances to
at least `write_id_floor_ms + 1` after daemon handshake. `--id` accepts canonical ULID.
Internal automation uses fixed derived IDs and never consumes this namespace.

Receipt canonicalization includes only semantic mutation fields: method, principal,
target/selector, verb/action, normalized args and explicit expected bindings/probe
choice. It excludes JSON-RPC id, connection metadata, `wait_ms`, output format, pager,
client timeout and other response-wait/display options. Therefore the same intended
mutation can be retried with a different wait timeout or `--json` using the same
WriteId.

Receipt lookup follows [core](core.md#external-write-idempotency-and-retention):

- same id + same principal/method/canonical payload → current/original result;
- mismatch → `id_conflict`;
- absent id at/below floor or older than live window → `id_expired`;
- absent id too far ahead → `id_future`.

A lost reply is retried with the same id, never a replacement. Receipt survives shorter
domain-history retention; after receipt expiry the monotonic floor prevents reapply.

Internal rule/timer/goal/action, linked-input and selector-child writes use `d_…` IDs
whose lifetime follows their durable owner. They do not get fake `write_receipts` rows.

Daemon process bootstrap and OS integration (`eliot daemon`, `daemon install`,
`daemon uninstall`) are out-of-process owner desired-state operations, not daemon Store
mutations: they use exact OS service/process readback and never repeat an uncertain
installer command blindly. Once connected, `daemon.shutdown` is a normal receipted
mutation; a lost successful shutdown result can be read after the next explicit start.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, write-id floor, custody/source/account holds; exact drain/force |
| agent list/get | paginated current projections, generation/native subject/open effects |
| agent wait/result | exact captured generation/turn/goal result |
| command submit/get | durable parent/child/session/input commands and readback |
| turn get/resolve | exact native/readback result or constrained owner assertion |
| events read/subscribe | bounded persisted replay + live stream with explicit gaps |
| hook event | exact request + deadline → committed non-dispatchable proposal or safe default |
| discover/attach/detach | canonical native subject; one Agent row |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/goal | scoped automation state/dry run/cancel/readback/assertion |

Every external mutating daemon method accepts `write_id`, except internal plugin/source
requests with their own exact open/sequence/registration contract. Reads allocate no
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
  linked_command?, child_commands?: [id]
}
```

For an external concrete command, `command_id=write_id` and the command references that
receipt. Internal automation commands use a derived id and owning run/action cause.

One transaction resolves target/selector, validates principal/capability/state and
expected binding, captures immutable generation/turn/request, inserts the external
receipt where applicable, and creates all command/link/event rows.

Ordinary expected generation equals current target generation. Launch targets 1. Resume
expects terminal g and targets g+1; row/token change only when dispatch commits.

Launch/resume with text returns session command plus a derived linked send. Without
`--wait`, launch/resume wait means session readiness. `--wait` with text follows the
linked send to its exact turn. Session start and initial input never share one outcome.

An interactive selector creates one external Store-only parent and a fixed derived child
command per concrete target captured in that transaction. The parent becomes applied
when expansion commits and returns `child_commands`; this says nothing about the child
native effects. Children queue/dispatch/settle independently. Repeating the parent
WriteId returns the same child set. `--wait` waits exactly that set and returns per-child
outcomes; agents created later are not included.

Interactive concrete calls may omit expected fields; admission captures them. Durable
automation supplies frozen values. Internal linked/selector children never need an
external receipt.

Stable errors include `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `caller_identity_unknown`,
`id_conflict`, `id_expired`, `id_future`, `history_gap`, `secret_in_payload`,
`internal`.

### Explicit provider probe

`probe=true` is valid only for exact `send`, or `launch|resume` with linked initial
prompt. It requires a matching persisted hold past retry boundary and atomically claims
that hold for one command. Only owner/current controlled agent may request it; rules,
timers, wakes, parent notices and Goal loop may not. For launch/resume the linked send,
not session creation, is the probe. Hold clears only from that exact successful
provider-backed turn; unknown keeps hold/probe visible.

## Async input, turn outcome and wait

Vendor transport/inbox acceptance is not `send applied`. `input.admitted` stores exact
native input id while the command stays dispatched. Command applies only when that input
is associated with exact native turn/execution. Deadline after possible admission makes
it unknown and blocks later send/configure/goal in that generation pending readback.

Applied send still does not settle the turn. `turn.get` returns
`running|completed|unknown`, ordered input command ids, resolution source, status,
bounded final text and usage. Several input commands may map to one native turn.

If reconnect cannot prove terminal outcome, turn is unknown and live generation remains
blocked from new turn/goal work. Owner assertion is allowed only when native readback
proves not active, or generation terminal:

```text
turn.resolve {
  write_id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason, final_text?
}
```

It records `resolution_source=owner`, never plugin completion. Historical unknown in a
terminal old generation does not block resume.

`agent.wait` captures target generation at call start:

- turn + `after_command`: follow native input then `turn_inputs`; unresolved association
  waits/readbacks that exact input;
- turn without it: current active/unknown turn, or first turn after call high-water;
- idle: immediate only with no active/unknown turn or input gate;
- stopped: stopped/failed/lost for captured generation;
- goal: done/blocked for captured revision.

Several waits may resolve from one native turn. Unknown is exit 4; replacement generation
is stale. Timeout never cancels.

## Unknown-effect resolution

Automatic settlement is verb/action-specific exact readback. Generic owner assertions
are break-glass and cannot synthesize missing identity or free unsafe lanes:

- start applied requires exact attach/ready native subject;
- send applied requires exact turn association; failed requires exact pre-turn drop or
  not-found. Otherwise `unknown_abandoned` retains the input gate; stop/resume is safe
  recovery;
- interrupt/stop require exact bound turn/generation readback;
- Git/webhook/script assertions are labelled `owner_assertion` and release their narrow
  hold only with explicit `--unsafe-assert`.

Rules, timers, scripts and agents cannot unsafe-assert. Every assertion records reason,
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

`hook_id` is stable native invocation identity, not WriteId. Daemon derives proposal id,
validates exact pending request and evaluates bounded synchronous rules. Non-default
response requires proposal commit before internal deadline margin while request remains
pending. Proposal has no dispatcher path and can act only through this immediate
response. Same-id retry before expiry may return it; timeout, Store failure, expiry or
uncertain response returns `ask` for decisions (`allow` only for observation). Late
proposal never approves later.

## Attach, discover and detach

Discover is paginated and returns subjects only. Attach looks up unique subject: return
existing row, upgrade observe-only to control after exact native attach, or create one
external row. Another control path/unverified takeover rejects. Native subject reuse
requires new scope.

Control reattach to the same still-live observe-only incarnation preserves generation,
atomically increments `token_version`, installs a fresh token hash and returns the
re-derived raw token only after attach succeeds. Retrying the same external WriteId
returns the same version/token. Daemon/plugin recovery of an already-controlled row does
not rotate it.

Detach is supported only when native lifetime is independently owned and exact
observation/reattach remains. It sets observe-only, revokes token and releases claims
without changing generation or killing native session. Daemon-owned per-agent process
cannot detach. Detach never settles unknown in-flight input.

## Drain and shutdown

Drain commits admission mode first. It rejects new work-starting commands/runs but
allows exact interrupt/stop/request replies/cancel/detach/goal pause-clear, queries,
readback and constrained assertions. Pre-boundary work continues; streams/readback stay
alive.

Exit waits for admitted effects/owned sessions. Unknown remains blocker unless safely
resolved or explicitly abandoned. Attached sessions may detach after admitted work
settles.

`--force` kills only exact custody/session backends. Degraded custody remains visible
blocker. Explicit `--abandon-degraded` may exit while recording possible orphans for
`doctor`; it is not clean success and never kills attached services.

## Event replay and gaps

Subscribe registers bounded live buffer, captures Store high-water, replays bounded
pages `(after_seq,high_water]`, then releases buffered persisted events in order. No
read transaction spans client wait. Ephemeral replay-window events are best effort;
overflow emits `events.lagged`.

Store exposes `earliest_seq`. Request before it returns `history_gap`. Explicit
`allow_gap=true` emits `events.gap` then continues from earliest. Rule cursors never opt
into silent gap.

## Mail acknowledgement

One open immutable inbox batch exists per recipient. `mail.inbox` returns it until
`mail.ack(batch_id,write_id)` commits; new messages wait for next batch. CLI writes full
stdout before ack. Lost inbox leaves unread; lost ack reuses WriteId. `mail.read ID` is
a receipted mutation and is allowed only when no open batch contains that message.
`--all` is paginated.

## CLI

Every daemon mutation accepts `--id ULID`; CLI generates, persists and prints one before
sending when omitted. Retry after uncertain client reply reuses it. Bootstrap/install
commands use desired-state OS readback rather than Store ids.

```text
eliot daemon [--foreground] | daemon stop [--drain|--force|--abandon-degraded] [--id ULID]
eliot status
eliot launch [profile/workspace/tree options] [PROMPT] [--probe] [--wait] [--id ULID]
eliot ls | show A | watch [A|SELECTOR] | top
eliot send|inject A TEXT [--generation N] [--probe] [--id ULID]
eliot steer A TEXT [--or-send] [--generation N] [--turn ID] [--id ULID]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show A TURN [--generation N]
eliot turn resolve A TURN --generation N --as completed|interrupted|failed --reason T --id ULID
eliot interrupt|abort A [--generation N] [--turn ID] [--id ULID]
eliot stop A [--force] [--tree] [--generation N] [--id ULID]
eliot resume A [PROMPT] [--generation N] [--probe] [--wait] [--id ULID]
eliot approve|deny|answer ... [--id ULID]
eliot configure ... | compact|fork|background ... | goal ... [--id ULID]
eliot cmd show|cancel|resolve|abandon ... [--id ULID where mutating]
eliot action show|cancel|resolve ... [--unsafe-assert] [--id ULID where mutating]
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover [--cursor C] [--limit N] | attach ... | detach A
eliot service list|stop | worktree remove | doctor
eliot daemon install | daemon uninstall
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Exit codes: 0 success/queued; 1 definite error; 2 usage; 3 rejected before effect; 4
unknown; 5 wait timeout; 6 still pending/dispatched.

Daemon injects home, agent id/generation/raw token and CLI path. Store has only current
token hash/version. Known credentials are masked from native output and rejected in
durable semantic payloads.
