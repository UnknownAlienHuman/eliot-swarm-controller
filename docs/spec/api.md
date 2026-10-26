# Client API and CLI

## Transport and caller identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows pipe is current-user-only; Unix socket is mode
`0600` in private data root. One daemon owns one data-dir lock; `daemon.json` includes
`host_boot_id`, PID and process birth identity.

Caller resolution uses credentials plus **live OS custody classification**:

1. obtain peer PID and pin `(host_boot_id,pid,start_time)`;
2. if any agent/automation identity fields are supplied/inherited, require complete
   matching identity; reject invalid/incomplete/revoked/stale;
3. use exact process projection row as fast path;
4. on miss, check pinned peer live against every current Job/cgroup/registered process
   group before owner fallback;
5. recheck birth/liveness;
6. no token means owner only for local OS user definitively outside every Eliot custody.

Unavailable peer credential, membership failure, birth race or incomplete inventory is
`caller_identity_unknown`. Projection miss never proves independence. Shared-service
tool still needs exact per-session token because service PID is not agent identity.
This is attribution inside one-user trust boundary, not hostile same-user isolation.

API frame maximum 4 MiB; semantic limits smaller. Handshake/status returns
`write_id_floor_ms`, `idempotency_window_ms`, `write_id_future_skew_ms`.

## External write IDs

Every mutation through running daemon carries canonical ULID `write_id` generated before
send. CLI uses locked, atomically replaced monotonic state and advances to at least
daemon floor + 1. `--id` accepts canonical ULID. Internal automation uses derived IDs.

Receipt canonicalization includes semantic mutation fields only: method, principal,
target/selector, verb/action, normalized args, expected bindings and probe choice. It
excludes JSON-RPC id, connection metadata, wait/display/pager/client-timeout options.
Thus same intended mutation may retry with another `wait_ms` or `--json` using same id.

Receipt lookup follows [core](core.md#external-write-idempotency-and-retention): same
id/payload returns current result; mismatch `id_conflict`; absent old/below-floor id
`id_expired`; absent excessive-future id `id_future`. Lost reply reuses same id. Receipt
outlives shorter domain history; floor prevents reapply after receipt expiry.

Internal rule/timer/goal/action, linked-input and selector-child writes use `d_…` IDs and
no fake receipts.

Daemon bootstrap/OS integration (`eliot daemon`, install/uninstall) are out-of-process
owner desired-state operations with exact OS readback, not Store mutations. Connected
`daemon.shutdown` is receipted; lost success can be read after next explicit start.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, write-id floor, custody/source/account holds |
| agent list/get | paginated projections, generation/native subject/open effects |
| agent wait/result | exact captured generation/turn/goal result |
| command submit/get/children | parent/child/session/input commands, paginated captured fanout and readback |
| turn get/resolve | native/readback result or constrained owner assertion |
| events read/subscribe | paged replay + bounded live stream with gaps |
| hook event | deadline-bound committed proposal or safe default |
| discover/attach/detach | canonical native subject, one Agent row |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/goal | scoped automation state/dry run/cancel/readback/assertion |

Every external mutating daemon method accepts `write_id`, except internal plugin/source
requests with exact open/sequence/registration contract. Reads allocate no receipts.

## Command admission

```text
params {
  write_id, target?, selector?, verb, args,
  expected_generation?, expected_turn_id?, request_id?,
  probe? [false], wait_ms?
}
result {
  command_id, agent?, expected_generation?, generation?, state, outcome?,
  linked_command?,
  fanout?: { count, digest }
}
```

External concrete/fanout parent command has `command_id=write_id` and references that
receipt. Internal automation uses derived id and run/action cause.

One transaction resolves target/selector, validates principal/capability/state and
expected binding, captures generation/turn/request, inserts receipt where applicable,
and creates command/link/event rows.

Ordinary expected generation equals current target. Launch targets 1. Resume expects
terminal g and targets g+1; row/token change only when dispatch commits.

Launch/resume with text returns session command plus derived linked send. Without
`--wait`, wait means session readiness. `--wait` with text follows linked send to exact
turn. Session start/input never share one outcome.

### Selector fanout

Interactive selector creates one external Store-only parent and fixed derived child per
concrete target captured in the admission transaction. Expansion:

- is sorted deterministically by AgentId;
- fails before any row when target count exceeds `limits.max_selector_targets`;
- stores child links and parent `{count,digest}`;
- marks parent applied when expansion commits, which says nothing about child effects.

Children queue/dispatch/settle independently. Repeating parent WriteId returns same
count/digest. `command.children {command,after?,limit<=1000}` pages exact captured child
ids/outcomes from `command_links`; `--wait` waits that set and returns paged/per-child
summary. Later agents are not included. After terminal link retention expires,
`command.children` returns `history_gap` while receipt still proves no replay and retains
count/digest/aggregate summary.

Interactive concrete calls may omit expected fields; admission captures them. Durable
automation supplies frozen values. Internal child/linked commands need no receipt.

Stable errors include `not_found`, `unsupported`, `not_allowed`, `bad_state`,
`stale_generation`, `invalid`, `busy`, `unavailable`, `caller_identity_unknown`,
`id_conflict`, `id_expired`, `id_future`, `history_gap`, `secret_in_payload`,
`internal`.

### Explicit provider probe

`probe=true` valid only for exact send or launch/resume with linked prompt. Matching
persisted hold must be past retry boundary and one command atomically claims it. Only
owner/current controlled agent may request; rules/timers/wakes/parent notices/Goal may
not. For launch/resume linked send—not session creation—is probe. Hold clears only from
that exact successful provider turn; unknown keeps hold/probe.

## Async input, turn outcome and wait

Vendor inbox acceptance is not `send applied`. `input.admitted` stores native input id
while command remains dispatched. Command applies only at exact turn/execution
association. Deadline after possible admission makes unknown and blocks later send/
configure/goal in generation pending readback.

Applied send does not settle turn. `turn.get` returns `running|completed|unknown`,
ordered input command ids, resolution source, status, bounded final text and usage.
Several input commands may map one turn.

If reconnect cannot prove terminal outcome, turn is unknown and live generation remains
blocked. Owner assertion allowed only when native readback proves not active or
generation terminal:

```text
turn.resolve {
  write_id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason, final_text?
}
```

It records `resolution_source=owner`, never plugin completion. Historical unknown in old
terminal generation does not block resume.

`agent.wait` captures target generation:

- turn + `after_command`: follow native input then turn mapping;
- turn without it: active/unknown turn or first after call high-water;
- idle: only no active/unknown turn/input gate;
- stopped: stopped/failed/lost for captured generation;
- goal: done/blocked for captured revision.

Several waits may resolve from one turn. Unknown exit 4; replacement generation stale;
timeout never cancels.

## Unknown-effect resolution

Automatic settlement is verb/action-specific readback. Generic owner assertions cannot
synthesize identity or free unsafe lanes:

- start applied requires exact attach/ready subject;
- send applied requires turn association; failed requires exact pre-turn drop/not-found;
  otherwise `unknown_abandoned` retains input gate;
- interrupt/stop require exact turn/generation readback;
- Git/webhook/script assertions are `owner_assertion` and release narrow hold only with
  `--unsafe-assert`.

Rules/timers/scripts/agents cannot unsafe-assert. Every assertion stores reason,
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

`hook_id` is stable native invocation, not WriteId. Daemon derives proposal id,
validates pending request and evaluates bounded synchronous rules. Non-default response
requires proposal commit before internal margin while request remains pending. Proposal
has no dispatcher and acts only through immediate response. Same-id retry before expiry
may return it; timeout/Store failure/expiry/uncertain response returns `ask` for decision
hooks (`allow` only observation). Late proposal never approves later.

## Attach, discover and detach

Discover is paginated and returns subjects. Attach returns existing row, upgrades
observe-only after exact attach, or creates one external row. Another control path/
unverified takeover rejects. Subject reuse requires new scope.

Control reattach to same live observe-only incarnation preserves generation, increments
`token_version`, installs fresh hash and returns re-derived raw token after attach.
Retrying same WriteId returns same version/token. Recovery of already-controlled row
does not rotate.

Detach requires independently owned/observable service lifetime. It sets observe-only,
revokes token and releases claims without changing generation/killing session.
Daemon-owned per-agent process cannot detach. Detach does not settle unknown input.

## Drain and shutdown

Drain commits admission mode, rejects new work but permits exact interrupt/stop/request
replies/cancel/detach/goal pause-clear, queries/readback/assertions. Pre-boundary work
continues.

Exit waits for effects/sessions. Unknown blocks unless safely resolved/abandoned.
Attached session may detach after admitted work settles. `--force` kills only exact
backends. Degraded remains blocker; `--abandon-degraded` records possible orphans and is
not clean success.

## Event replay and gaps

Subscribe registers bounded live buffer, captures Store high-water, replays pages
`(after_seq,high_water]`, then releases buffered persisted events in order. No read
transaction spans client wait. Ephemeral events are best effort; overflow emits lag.

Request before `earliest_seq` returns `history_gap`. `allow_gap=true` emits gap then
continues. Rule cursors never silently gap.

## Mail acknowledgement

One open immutable inbox batch per recipient. `mail.inbox` returns it until receipted
`mail.ack`; new messages wait for next. CLI writes full stdout before ack. Lost inbox
leaves unread; lost ack reuses id. `mail.read` is receipted and prohibited for a message
inside open batch. History is paginated.

## CLI

Every daemon mutation accepts `--id ULID`; CLI generates/persists/prints before send.
Bootstrap/install uses OS desired-state readback.

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
eliot cmd show|children|cancel|resolve|abandon ... [--id ULID where mutating]
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

Daemon injects home, agent id/generation/raw token and CLI path. Store has token hash/
version only. Known credentials are masked from native output and rejected in durable
semantic payloads.
