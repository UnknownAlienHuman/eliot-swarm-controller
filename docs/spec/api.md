# Client API and CLI

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows pipe is current-user-only; Unix socket is mode
`0600` inside the private data root. One daemon owns one data-directory lock;
`daemon.json` includes `host_boot_id`, PID and process birth identity.

Every connected client presents exactly one explicit principal credential:

- owner: `eliot --owner ...` reads private `owner.token`;
- Agent: complete `(agent_id,generation,agent_token)`;
- managed automation/check/setup process: complete scoped automation token;
- plugin: daemon-created instance credential on its private pipe.

Missing, incomplete, revoked or mismatched credentials reject. There is no no-token owner
fallback and process/PID identity never grants API authority. This is attribution inside
one trusted OS user, not hostile same-user security.

API frame maximum is 4 MiB; semantic limits are smaller. Handshake/status returns write
id floor/window/skew, page limits and Store/WAL/routing-key health.

## External write IDs

Every external daemon mutation carries a canonical ULID `write_id` generated before
send. CLI locks and atomically updates its monotonic ULID state, advancing to at least
daemon floor + 1. `--id` accepts one canonical ULID. Internal automation uses derived
ids.

Receipt canonicalization includes semantic mutation fields only: method, principal,
target/selector, verb/action, normalized args, expected binding revision and probe
choice. It excludes JSON-RPC id, connection metadata, output/pager/client timeout and
`wait_ms`.

Receipt lookup follows [core](core.md#external-write-idempotency): same id/payload
returns retained result; mismatch `id_conflict`; absent below floor/window `id_expired`;
absent too far in future `id_future`. Lost reply reuses the same id. Linked input,
selector child, rule/timer/Goal/action writes use derived ids and no fake receipts.

Daemon bootstrap/install is OS desired-state work with exact readback, not a connected
Store mutation. Connected `daemon.shutdown` is receipted.

## Methods

| Surface | Semantics |
| --- | --- |
| daemon status/shutdown | admission, Store/WAL, write-id floor, routing key, custody/source/account holds |
| agent list/get | paginated projection, generation, current binding revision/history cursor, native subject, open effects |
| agent wait/result | exact captured generation/turn/Goal result |
| command submit/get/children | parent/child/session/input commands, exact binding revision and paged fanout |
| turn get/resolve | native/readback result or constrained owner assertion |
| events read/subscribe | paged replay + bounded live stream with explicit gaps |
| hook event | internal deadline-bound committed proposal or safe default |
| discover/attach/detach | paged native subjects; one canonical Agent row and versioned binding transitions |
| mail send/inbox/ack/read | durable coordination with explicit acknowledgement |
| vars/who/claims | scoped coordination |
| rule/timer/action/Goal | scoped automation state, dry run, cancel, readback/assertion |

Every external mutation accepts `write_id`. Internal plugin/source/hook requests use
instance/open/sequence/hook identities. Reads allocate no receipt.

## Command admission

```text
params {
  write_id, target?, selector?, verb, args,
  expected_generation?, expected_binding_revision?,
  expected_turn_id?, request_id?, probe? [false], wait_ms?
}
result {
  command_id, agent?, expected_generation?, generation?, binding_revision?,
  state, outcome?, linked_command?, fanout?: {count,digest}
}
```

External concrete/fanout parent has `command_id=write_id`; internal automation has a
derived id. One transaction resolves target/selector, validates principal/capability/
state and expected binding, captures target generation/binding revision/turn/request,
inserts receipt and creates command/link/event rows.

Ordinary expected generation equals current target. Launch targets generation 1 binding
revision 1. Resume expects terminal g and targets `g+1` revision 1; Agent generation and
routing change only when dispatch commits.

Launch/resume with text returns session command plus a derived linked send. Without
`--wait`, launch waits for session readiness only. `--wait` with text follows linked
send to its exact terminal turn. Session start, input association and terminal result
remain separate.

### Selector fanout

Interactive selector creates one external Store-only parent and one derived child per
concrete target captured in the admission transaction. Expansion is sorted by AgentId,
checks `max_selector_targets` before rows, stores links/count/digest and marks parent
applied when expansion commits. It says nothing about child native effects.

`command.children {command,after?,limit<=1000}` pages the captured set. Repeating the
same WriteId returns the same count/digest. `--wait` waits only that set and returns a
paged summary. After link retention, child detail returns `history_gap`; receipt summary
still prevents replay.

Interactive concrete calls may omit expected fields; admission captures them. Durable
automation supplies frozen values. Internal child/linked commands need no receipt.

Stable errors include `missing_principal`, `invalid_principal`, `not_found`,
`unsupported`, `not_allowed`, `bad_state`, `stale_generation`,
`stale_binding_revision`, `invalid`, `busy`, `unavailable`, `capacity_unavailable`,
`id_conflict`, `id_expired`, `id_future`, `history_gap`, `secret_in_payload`,
`frame_too_large`, `internal`.

### Explicit provider probe

`probe=true` is valid only for exact send or launch/resume with linked prompt. A matching
hold must be past retry boundary and one command atomically claims it. Only owner/current
directly routed Agent may request it; rules/timers/wakes/parent notices/Goal may not.
For launch/resume the linked input—not session creation—is the probe. Exact successful
provider turn clears hold; possible provider I/O keeps hold/probe unknown.

## Async input, turn outcome and wait

Vendor inbox acceptance is not send applied. `input.admitted` stores optional native
input id while command remains dispatched. Command applies only at exact command→turn/
execution association. Deadline after possible admission makes it unknown and blocks
later send/configure/Goal in that generation.

Applied send does not settle turn. `turn.get` returns `running|completed|unknown`,
ordered input command ids, optional native input ids, resolution source, status, bounded
final text and usage. Several inputs may map to one native turn.

If reconnect cannot prove terminal outcome, turn is unknown and live generation remains
blocked. Owner assertion is allowed only after native-not-active proof or terminal
generation:

```text
turn.resolve {
  write_id, agent, generation, turn_id,
  status: completed | interrupted | failed,
  reason, final_text?
}
```

It records `resolution_source=owner`, never plugin completion. Historical unknown in an
old terminal generation does not block resume.

`agent.wait` captures target generation and current binding revision:

- turn + `after_command`: follow input then turn mapping;
- turn without it: active/unknown turn or first turn after call high-water;
- idle: only with no active/unknown turn or input gate;
- stopped: stopped/failed/lost for captured generation;
- Goal: done/blocked for captured Goal revision.

Several waits may resolve from one turn. Unknown exits 4; replacement generation/binding
is stale. Timeout never cancels.

## Unknown-effect resolution

Automatic settlement is verb/action-specific readback. Generic owner assertions cannot
synthesize identity or free unsafe lanes:

- start applied requires exact attach/ready subject and referenced binding revision;
- observe/control attach applied requires exact native attach/readback and only then
  moves the current binding pointer;
- detach applied requires exact release/readback and only then moves to the candidate
  detach-observe revision;
- send applied requires turn association; failed requires exact pre-turn drop/not-found;
- interrupt/stop require exact turn/generation readback;
- Git/webhook/script assertions need `--unsafe-assert` and release only narrow holds.

Rules/timers/scripts/Agents cannot unsafe-assert. Assertions store reason/principal/time.

## Synchronous hook event

```text
hook.event {
  hook_id, harness, hook,
  agent, generation, turn_id?, request_id,
  native_deadline_at, payload
}
→ {decision: allow|deny|ask|..., proposal_id?}
```

This is an internal authenticated plugin/hook request, not external WriteId mutation.
Daemon derives proposal id, validates exact pending request and evaluates bounded rules.
A non-default response requires proposal commit before margin while request remains
pending. Proposal has no dispatcher and acts only through the immediate response.
Same-id retry before expiry may return it; timeout/Store failure/expiry/uncertain
response returns `ask` for decisions (`allow` only for observation). Late proposal never
approves later.

## Discover, attach and detach

`agent.discover {harness?,cursor?,limit<=1000}` is paginated and each frame remains below
the API cap. It returns native subjects and bounded metadata only.

`agent.attach` is owner-only in the first local slice:

```text
attach {
  write_id, harness, native_scope, native_ref,
  mode: observe | control,
  profile?, project?, name?
}
```

### First observe attach

For an absent subject, daemon creates generation-1 external Agent and immutable binding
revision 1, `mode=observe_attach`, profile `observe:<harness>`. After exact plugin
observe attach/readback commits, current pointer becomes revision 1,
`observe_only=true`, `routing_enabled=false`. Lost client reply returns the same result
through the WriteId receipt.

### Observe → control

Daemon validates owner-selected allowed profile, project, capacity and native
compatibility, then creates immutable candidate revision `current+1` with
`supersedes_revision=current` and a durable attach command referencing it. Current
pointer/Agent profile/routing/capacity do **not** change yet.

- exact attach/environment/readback success: one transaction applies command, moves
  pointer to candidate, updates Agent profile/control/observe/routing/token version and
  commits capacity reservation/effective facts;
- definite failure: pointer remains observe; candidate is historical and command fails;
- unknown: pointer remains observe and no new control work is admitted until exact
  readback/owner resolution. Retrying same WriteId returns the same candidate/command.

Control may remain `routing_enabled=false` when native API can control but cannot inject
a distinct Eliot environment. If capacity is unavailable, no candidate/effect is
admitted.

### Detach and control reattach

Detach is valid only for independently owned/observable service lifetime. It creates a
candidate `detach_observe` revision. Exact success atomically moves the pointer, sets
observe-only, disables routing/revokes token and releases claims. Capacity for a still-
live provider session remains until exact terminal observation. Failed/unknown detach
leaves the control pointer/state unchanged. Daemon-owned per-Agent process cannot detach.

A later control reattach creates another immutable control revision; exact success moves
pointer and rotates token version. Recovery of an already-controlled incarnation reuses
the current control revision/version rather than creating a new revision.

Another live control path or unverified takeover returns `not_allowed`. Attach/detach do
not settle unknown input/turn.

## Drain and shutdown

Drain commits admission mode, rejects new work but permits exact interrupt/stop/request
replies/cancel/detach/Goal pause-clear, queries/readback/assertions. Pre-boundary work
continues.

Exit waits for effects/sessions. Unknown blocks unless safely resolved/abandoned.
Attached session may detach after admitted work settles. Force kills only exact backends.
Degraded remains blocker; `--abandon-degraded` records possible orphans and is not clean
success.

## Event replay and gaps

Subscribe registers bounded live buffer, captures Store high-water, replays pages
`(after_seq,high_water]`, then releases buffered persisted events. No read transaction
spans client wait. Ephemeral events are best effort; overflow emits lag.

Request before `earliest_seq` returns `history_gap`. `allow_gap=true` emits gap then
continues. Rule cursors never silently gap.

## Mail acknowledgement

One open immutable inbox batch per recipient. `mail.inbox` returns it until receipted
`mail.ack`; new messages wait for next batch. CLI writes stdout before ack. Lost inbox
leaves unread; lost ack reuses WriteId. `mail.read` is receipted and prohibited for a
message in an open batch. History is paginated.

## CLI

Every connected mutation accepts `--id ULID`; CLI generates/persists/prints it before
send. Owner calls explicitly use `--owner`; routed Agent CLI supplies its credential.
Bootstrap/install uses OS desired-state readback.

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
