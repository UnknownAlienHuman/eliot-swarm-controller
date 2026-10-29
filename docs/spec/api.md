# Client API and CLI

Implement only the current [roadmap](../roadmap.md) surface. The first client supports
core Agent commands/events; coordination, automation and service methods are added with
their modules. Do not advertise unimplemented commands.

## Transport and principal identity

JSON-RPC 2.0 over UTF-8 NDJSON. Windows uses a current-user-only named pipe; Unix a
mode-0600 socket under the private data root. One daemon owns the data-directory lock.
`daemon.json` records boot id, PID/birth identity, endpoint and diagnostic version.

Every call has an explicit principal. Owner CLI outside a managed Agent/rule environment
reads the private owner token automatically. Agents send id/generation/token; managed
scripts use scoped run tokens; plugins have instance credentials. Missing/incomplete/
revoked credentials reject, never become owner. No manual `--owner` step is required.
This prevents accidental attribution errors; it is not isolation from hostile processes
of the same OS user. Reads are scoped as well as writes.

Frames are at most 4 MiB, semantic fields smaller. Multiplex requests so one wait does
not block unrelated calls. Bound all lists and histories; never hold a read transaction
open while waiting for a client or native process.

Background `daemon` startup reports `starting`; use `daemon.status` for readiness.
Windows starts that independent process without inheriting caller handles, so captured
CLI stdout/stderr reach EOF while the daemon remains alive. Its own private IPC and
owned plugin/workspace custody establish subsequent authority.

## Mutation ids

External writes carry a ULID generated/printed before send. Bind it to principal,
method and canonical semantic payload. Exclude JSON-RPC id, output format, paging and
wait timeout. Same binding returns the retained result; different binding is
`id_conflict`; absent expired id is `id_expired`. Retry a lost client reply with the
same id, never a new native effect.

The CLI validates supplied ULIDs and sends them in uppercase. A direct RPC ID is used
literally by Router and Store as the mutation key; retry that same spelling.

Before printing either mutation reply or using it for exit/wait decisions, the CLI
validates the exact `command_id`, a nonempty response Agent, any explicit target, and a
nonnegative response generation without requiring universal equality to
`expected_generation`. Launch/resume with nonblank input requires the exact
`<id>/send` link. Shutdown validates the retained mode and reply shape; a mismatch is
`unknown` (exit 4), and even `applied` does not prove the daemon exited.

Commands store the binding directly; other writes use:

```sql
CREATE TABLE writes (
  id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
  payload_digest BLOB NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL
);
```

The writer checks both command/write namespaces. Later linked inputs, selectors and
rule actions have bounded derived ids. No general receipt/floor framework.

## Methods

| Method | Purpose |
| --- | --- |
| `daemon.status/shutdown` | owner-only current state/admission and exact drain/force |
| `agent.list/get/result` | scoped, paginated Agent state and exact results |
| `agent.processes/stats` | scoped process records and ephemeral OS samples |
| `agent.git/edits` | current Git snapshot and paginated native-attributed edits |
| `command.submit/get/children/resolve` | immutable admission, linked commands, child readback and constrained resolution |
| `turn.get/resolve` | running/completed/unknown turn; constrained owner assertion |
| `rule.get` | owner-only read of one startup-loaded global rule state |
| `events.read/subscribe` | persisted replay plus bounded live delivery |
| `agent.discover` | owner-only native metadata discovery, without creating Agents |
| `agent.history` | owner-only bounded history of an exact observed subject |
| `hook.event` | later exact request/deadline decision |
| `mail.send/inbox/read/ack`, `var.*`, `who`, `claim.*` | later coordination |
| `rule.*` except `rule.get`, `timer.*`, `goal.*` | later automation; effects remain commands |

Canonical public wire types are re-exported by
[`eliot_core::api`](../../crates/eliot-core/src/api.rs). These DTOs and their validators
define field names, JSON shapes and grammar; this spec owns behavior and authority. Principal
authentication precedes method parameter validation.

| Surface | Canonical DTO definitions |
| --- | --- |
| Agent/daemon views and cursors | [agent_views.rs](../../crates/eliot-core/src/api/agent_views.rs) |
| Nested observation/Git views | [monitor_views.rs](../../crates/eliot-core/src/api/monitor_views.rs) |
| Command, turn and result queries | [reads.rs](../../crates/eliot-core/src/api/reads.rs) |
| Command submission and shutdown | [mutations.rs](../../crates/eliot-core/src/api/mutations.rs) and [Core `SubmitCommand`](../../crates/eliot-core/src/lib.rs) |
| Discovery/history queries and pages | [native_requests.rs](../../crates/eliot-core/src/api/native_requests.rs), [native_pages.rs](../../crates/eliot-core/src/api/native_pages.rs) |
| Event queries, filters and pages | [api.rs](../../crates/eliot-core/src/api.rs) |

Errors include
`not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`, `invalid`,
`busy`, `unavailable`, `capacity_unavailable`, `provider_held`, `history_gap`, `source_integrity`,
`secret_in_payload`, `frame_too_large`, `mailbox_full`, `internal`.

`capacity_unavailable` is a pre-effect rejection from a configured Eliot `max_live`
scope, not a vendor quota or entitlement result. A full-cap request is not admitted:
there is no command row to read back, and the same ID may be submitted again once the
scope frees. Once a command is admitted, its same-ID readback returns the retained result
and never retries the native effect. Provider-hold methods are defined below;
service-management APIs remain `unsupported` in this increment.

[`Error.code`](../../crates/eliot-core/src/lib.rs) is an open string, not a closed wire
enum; plugin-specific codes may be preserved. Unfamiliar post-I/O error codes classify
as `Unknown` under [Core policy](../../crates/eliot-core/src/command.rs), never proof
of failure or permission to replay the effect.

## Agent and daemon read views

The seven view calls use the Core view DTOs linked above. `daemon.status` is owner-only
and accepts an empty object. Its bounded blocker/command lists retain truncation flags;
unknown commands remain unknown. Agent list observations appear only when requested.
Absent source cursors and sequence/acknowledgement values remain null.

`rule.get` uses the typed Core `RuleGetRequest`/`RuleStateView`, is owner-only, and
reads the current loaded rule state and durable cursor. It does not reload configuration
or advance the cursor. The first connected event-rule subset is described in
[automation](automation.md#first-connected-event-slice).

`agent.get` includes the current generation's source rows and the managed workspace
target generation's rows when it differs. If persisted state requires a live owner and
the corresponding source is absent, it returns `unknown` rather than asserting the
persisted state is current.

Request DTOs reject unknown fields. Required Agent IDs are nonempty after a trim check
and are kept as supplied. Optional `generation`, `after` and process `scope` members may
be omitted or null to mean absent; wrong types are `invalid`. `generation`, when present,
must be a nonnegative integer. `scope` accepts `agent` or `workspace`; absence/null means
`agent`. `limit` defaults to 100 only when omitted and must be 1–1,000; explicit null,
wrong types and out-of-range values are `invalid`. `observations` defaults to false only
when omitted and must be a boolean when present; explicit null is `invalid`. Absent
optional members are omitted when requests are serialized.

Owner `agent.list` calls page by Agent ID. An Agent principal gets only its own row and
the Router ignores `after` and page selection for that self-only result, with `next:null`;
generic response validation must not impose owner paging assumptions on it. `next` for
an owner is the last returned ID on a nonempty page; pass it back unchanged. It is not
proof another row exists. Typed response binding failures remain `unknown`; validate the
reply before printing it or advancing a cursor.

For `agent.processes`, omitted generation selects the current Agent generation for
`scope:"agent"`, or the managed workspace target generation for `scope:"workspace"`.
The Router resolves and authorizes that exact target before checking the cursor; owners
may read retained historical Agent generations. `ProcessPageCursor` binds the exact
Agent, generation, scope and process birth identity and is limited to 2 KiB. Pass it
back unchanged. `next` is the last returned identity even on the last nonempty page;
continue until `processes` is empty. A different Agent,
generation or scope rejects before querying process rows. The workspace response includes
`scope:"workspace"`; the Agent-scope response omits `scope`.

`agent.stats` and `agent.git` read the current generation; an explicit generation is an
expectation and a mismatch is `stale_generation`. `agent.edits` defaults to the current
generation, while an authorized owner may request a retained historical generation.
Its `after` is a path cursor: nonempty, at most 64 KiB, and without NUL. Pass it through
unchanged; the CLI does not resolve or normalize it. Edit pages default to 100 rows,
with limits 1–1,000, and `next` is the last returned path even on a nonempty final page.

The canonical monitoring DTOs distinguish process records, ephemeral CPU/RSS samples,
liveness, workspace custody and Git coverage. Their unavailable variants retain the
reason and may retain cached data. Missing or stale metrics stay null/unknown, never
zero; process activity is not model progress. Git and Windows canonical paths remain
opaque to the CLI, without resolution or normalization. Filesystem `retry_after_ms`
distinguishes an absent watch slot from an inactive one.

Workspace sources carry family ownership; Git monitoring sources do not. Source
digests are nullable JSON byte arrays, not strings; cursor and acknowledgement values
remain nullable. Stale, partial, gap and unknown values never become clean, complete
or zero. See [monitoring](monitoring.md#process-observation-and-terminal-ordering) for freshness and
[Git coverage](monitoring.md#git-and-filesystem-monitor) semantics.

`agent.edits` attribution requires verified, generation-matched native file-change hints;
a Git dirty set alone never proves Agent authorship. Git polling alone updates neither
process statistics nor model progress.

## Command admission

`command.submit` uses `CommandSubmitRequest` (the Core `SubmitCommand` alias) and
`CommandSubmitResponse` from the canonical mutation DTOs. `id` and `verb` are required;
unknown fields and wrong types are `invalid`. Optional `target`, `expected_generation`,
`expected_turn_id` and `request_id`
may be omitted or `null` to mean absent. `args` defaults to `{}` only when omitted and
must be a JSON object; explicit `null` or another type is `invalid`. Absent expectations
remain absent. All six response members remain present, including nullable binding,
outcome and linked-command values; `fanout` is not part of the current response.

Resolve the target and validate principal/capability/state before persisting the command
binding and events. Each verb checks `expected_generation` at its defined admission or
dispatch fence; it is not a universal equality check against the current generation at
the RPC boundary. The applicable expected-generation/turn mismatch rejects before that
verb's native effect. A generation-changing verb can establish or update its command
generation at dispatch, so the response generation is not universally equal to
`expected_generation`. Interactive calls may omit expectations; delayed automation
supplies its captured expectations.

The current request does not accept `selector`; unknown request fields are `invalid`.
The optional `probe` intent defaults to false; false is omitted on the wire, and true is
valid only for `send`. It is control metadata, never native input text or arguments.
`provider_held` is a pre-effect rejection (CLI exit 3); queued sends also recheck the
hold at dispatch before native I/O.

The current `command.submit` request has no `wait_ms`, and the daemon does not expose an
`agent.wait` method. CLI `--wait` follows command and turn readbacks with a bounded client
deadline; timeout does not cancel the admitted command. Server-side wait semantics below
are retained for a future API layer, not part of the current request shape.

Generation-changing dispatch follows [core](core.md#generation-fence): advance and
revoke before native I/O, no rollback, no replay after unknown. Terminal settlement
revokes the token even if degraded Unix custody reports possible escaped descendants.

Launch/resume with explicit text creates a start plus linked `<id>/send`. Without
`--wait`, return session readiness; with it, follow the linked input to its exact terminal
turn. An input-created native session still reports these distinct outcomes.

Worktree creation is opt-in on the same launch command:

```text
command.submit {id, verb:"launch",
  args:{profile, workspace, worktree?:BRANCH, base?:REF, text?}}
```

Without `worktree`, `workspace` remains the existing execution directory. With it,
`workspace` names the source repository; the daemon chooses the managed destination.
`base` requires `worktree`. Omitted `base` means local `HEAD`, resolved and frozen to a
commit OID by daemon admission without fetching. The CLI forwards branch/ref strings
unchanged and does not resolve refs, create directories or run Git. Same-ID retries reuse
the retained preparation and frozen base instead of resolving a moved ref or replaying
uncertain work. Native launch waits for preparation ready in the exact target generation;
the existing readiness/linked-input wait semantics above are unchanged.

Removal uses the same command admission and readback path:

```text
command.submit {id, target:AGENT, verb:"worktree.remove",
  args:{}, expected_generation?:N}
```

The Router/Store enforce owner authority, the captured generation and terminal-only,
clean removal under the admitted scratch policy. The daemon selects the retained
managed worktree, preserves its branch and never removes a caller-supplied path. The
CLI offers no force/path argument and uses its normal ULID binding and exit codes.
It reads the exact command to settlement with a bounded 30-second wait; timeout does
not cancel or retry removal. Same-ID retries return the retained outcome even after
the directory has gone.

When selector support is added, a parent snapshots/sorts concrete ids and generations in
a bounded transaction; children settle independently. Parent applied means expansion only.
`command.children` pages that fixed set. Retain/prune parent sets transactionally so a
caller never sees a partly pruned expansion; after removal return `history_gap`. Ordered
rule actions reuse the same representation with a distinct link kind, not selector
execution semantics.

## Command, turn, and result reads

These are read-only lookups using the canonical read DTOs and Core `Command`, `Turn`
and `Agent` models. IDs never pass through a mutation call and cannot submit or replay
work. Richer command/usage/resolution metadata remains deferred. `agent.result` requires
an array of turn objects and an object for Agent metadata, not positional records.

For every failed terminal turn, with or without a structured provider restriction, status
and exact command/turn binding remain authoritative when final text exceeds the event
budget. The event omits oversized final text rather than exposing a truncated string as
complete, and includes
`final_text_unavailable:"event_budget"` when representable. `Turn.final_text = None`
means the public final text is unavailable; full native text remains private history.

Required IDs must be strings whose trimmed value is nonempty. Validation does not trim,
normalize or impose a new length limit on them. `turn.get` requires a nonnegative integer
generation. For `agent.result`, omitted `generation` selects the current generation;
omitted `turn_id` requests turns for that generation; omitted `limit` defaults to 100.
Any supplied generation must be a nonnegative integer, and any supplied `turn_id` must
be a nonempty string. The limit must be 1–1,000 even when `turn_id` selects one exact
turn. Optional request members that are absent are omitted during serialization; do not
send `null`. Explicit `null`, wrong types, invalid values and unknown request fields are
`invalid`. Unknown RPC methods and command verbs retain their existing `unsupported`
behavior.

The existing principal checks, generation rules and unknown-state masking apply. An
explicit generation retains the existing principal-scoped historical read behavior.
`agent_metadata` is the current Agent object and may have a different generation from
the result when an explicit historical generation was requested.

Before printing a read response or using it to advance a wait, the client validates its
binding: `command.get` must return the requested command ID; `turn.get` must match the
requested Agent, generation and turn ID; and `agent.result` must match the requested
Agent, its requested generation (or the current metadata generation when omitted), and
the requested exact turn when present (exactly one matching turn). Every result turn
must match the response Agent and generation, and the response must respect the requested
limit. A mismatch yields only the fixed `unknown` error
`read response does not match the requested binding`, with no response data or
diagnostics exposed.

`eliot cmd show` and `eliot result` exit 0 after a valid read even if a returned command
or turn is `unknown`; that state is not a successful task outcome. The `eliot wait`
flow treats an `unknown` command or turn as exit 4. A binding mismatch is also unknown
and must not print the mismatched response.

## Input, turn and unknown gates

Transport/inbox admission is not send applied. Bind input to an exact native turn, then
track its result independently. Unassociated dispatched/unknown input blocks later
send/configure/Goal continuation; active/unknown turn blocks next turn; unknown transition
blocks further takeover. Exact stop/interrupt/request replies/readback remain available.

A dispatched or unknown `configure` also holds the settings lane: do not dispatch a
later configure, new input or native/Eliot Goal start/resume until its exact outcome and
effective settings are read back or explicitly resolved. Timeout does not release this
hold, even when the observed Agent state is idle. This is a predicate over existing
command rows, not another queue/table. Stop/interrupt/request replies/readback still work.

Owner turn resolution requires exact readback proving it inactive or the generation
terminal. Future resolution metadata may record `resolution_source=owner`; the current
`Turn` read object does not expose that field. Resolution remains an owner assertion,
not a synthetic native completion. Future turn-result metadata retains the planned
`command_ids` and `usage` fields.
Owner command resolution is `command.resolve {id, command_id, as: applied|failed,
reason}`; it records an owner assertion, never a native receipt, and releases only the
relevant hold. `turn.resolve` likewise takes a mutation id, Agent/generation/turn,
terminal status and reason; it cannot bypass the inactive/terminal precondition.
Known failure can include partial effects; it never implies rollback or automatic retry.

## Provider holds

`provider.holds` is owner-only and returns a bounded page ordered by local scope.
Optional `after` and `limit` select the page; the response provides `holds` and
`next`. The canonical Core `ProviderHoldsRequest`/`ProviderHoldsResponse` DTOs own
the wire shape.
`provider.clear_hold` is an owner-only idempotent write using
`ProviderClearHoldRequest`/`ProviderClearHoldResponse`. The corrective evidence is a
closed enum: `credentials_updated`, `configuration_updated`, or `owner_verified`;
free text is invalid. Its expected-probe binding is compare-and-swap, including explicit
null to mean no probe claim; a mismatch or unresolved probe cannot be cleared. See the canonical
[Core API DTOs](../../crates/eliot-core/src/api/provider.rs) rather than duplicating
their field list.

A provider scope comes from the frozen route, selected account, or profile, in that
order; Eliot never guesses a vendor account. A held scope rejects new launch/send before
native I/O and is rechecked when queued work dispatches. Launch with linked input and
resume with new text are gated as sends. Plain resume, reads, request replies, interrupt
and stop remain available. At initial admission, `provider_held` rejects before binding
and creates no command row; that ID may be submitted again after the hold is resolved.
An already queued command that becomes held is rejected at dispatch under its existing
binding, before native I/O.

Only an explicit `send --probe` can claim an eligible rate-limit/quota hold. It uses the
ordinary SEND authorization and target/scope/generation checks; it grants no owner
authority and adds no role flag. The Store atomically allows one claim per frozen
provider scope through the normal send path. Future bounded automation must use that
same path, not a parallel probe executor. The flag defaults false and does not enter
native input. Time makes a probe eligible but never clears a hold. A successful
exact-bound terminal probe clears rate-limit/quota holds; failed or interrupted probes
retain them, while unknown probes retain their claim across restart. Auth and ModelRemoved
holds require owner correction and are not probe-eligible. Provider restriction facts
are defined in [monitoring](monitoring.md#provider-holds).

## Waiting

The following server-side wait semantics are a future API contract; they do not add
`agent.wait` to the current method set. Current CLI `--wait` behavior is described under
Command admission.

Capture generation at call start. `turn --after-command ID` follows that input's exact
association/result; without an id, capture the current turn or the first after the call
high-water. `idle` requires no open input/turn/transition or unresolved settings gate.
`stopped` means terminal; `goal` captures its revision. Replacement returns
`stale_generation`; timeout never cancels admitted work. Several native inputs may
legitimately share one turn result.

## Discover, attach, detach and resume

Discovery is owner-only, read-only/paged and creates no Agent rows. It resolves the
required profile from current daemon configuration and derives its target environment;
a supplied harness must match that profile. It performs no native control operation.
Attachment uses one canonical native subject row. First attach targets generation 1; a
later ownership takeover gets the next generation. Validate before dispatch, commit
identity fence before native I/O, and install exact readback on success. Failure never
restores revoked control; unknown blocks another takeover.

`agent.discover` uses `AgentDiscoverRequest`/`DiscoverPage`; `agent.history` uses
`AgentHistoryRequest`/`ObservationPage` from the canonical native metadata DTOs.
`profile` and `id` are required nonempty strings. Present strings are checked for
nonemptiness after trimming but are preserved as supplied. For `agent.discover`, optional
`harness`, `native_scope` and `cursor` may be omitted or `null` to mean absent. For
`agent.history`, the same applies to optional `generation`, `turn_id` and `cursor`.
Wrong types are `invalid`. `limit` defaults to 100 only when omitted and must be an
integer from 1 through 1,000; explicit `null`, wrong types and out-of-range values are
`invalid`. Unknown request fields are `invalid`.

Discovery scope is limited to 512 bytes and rejects control characters. Its cursor is
limited to 16 KiB and rejects control characters. Pass that opaque cursor unchanged; it
is bound to the selected profile, target environment, verified native store, scope,
method, fixed native-list filters, ordering and page size. Changed store selection
rejects before listing subjects. Unverified selection is an explicit error.
`coverage.complete` means this paged enumeration
has reached its native end, not that it is a consistent snapshot or a live subscription.
Codex coverage is `state_db_only`; it never scans/repairs rollout storage to fill the
list. The current Codex query selects nonarchived threads and fixes the page size in
its cursor binding. Subject timestamps are milliseconds since the Unix epoch; native
seconds are converted before projection. Session previews, prompts and transcripts are
excluded. Native `notLoaded` is reported as unknown: a separate reader cannot prove
another process's session stopped.
Discovery installs no Agent identity and invokes no native start/resume/input operation.
Owned readers finish cleanup after a client disconnect; drain waits for them and force
cancels their own custody. New discovery rejects during shutdown.

The first observation mutations use the existing command pipeline:

```text
command.submit {id, verb:"attach", expected_generation?,
  args:{profile,harness,native_scope,native_ref,mode:"observe",project?}}
command.submit {id, target:AGENT, verb:"detach", expected_generation?, args:{}}
```

Require an explicit profile. Resolve the same mutation id before rereading mutable
configuration; retained retries keep their original subject/profile. Attachment reuses
the canonical scope/reference row. A controlled row or an active observer returns
`busy`; this increment does not convert control or grant a second identity. Attach
commits its generation/source fence before reader I/O and installs no Agent token.
All native control verbs reject observe-only targets, including owner-issued stop.
`mode:"control"` is unsupported. The vendor plugin method is `agent.attach`; it is not
a second public mutation method.

`agent.history` is a separate owner-only read. `source` is required and must be
`native` or `rollout`; `view` is required and must be `subject`, `turns` or `items`.
Omitting either required field, supplying `null`, or using a wrong type is `invalid`;
other source/view values are `invalid`, while unsupported source capabilities retain
their existing explicit error behavior. Optional `generation`, `turn_id` and `cursor`
may be omitted or `null` to mean absent; other wrong types are `invalid`. A supplied
generation must be a nonnegative integer (`invalid` otherwise) and equal the current
observer generation (`stale_generation` otherwise). `items` requires a nonempty `turn_id`;
`subject` and `turns` forbid it. The read targets an existing observe-only Agent with no
control identity and is bound to its current generation. The CLI defaults `--source` to
`native` and sends that value explicitly. Absent optional request members are omitted
when serialized, never sent as `null`.

Successful reader attachment can leave the Agent `unknown`. History pages are private
views: they neither replay lifecycle into the event bus nor acknowledge input, command
or request delivery, or write observation pages to Store. Bind every page/cursor to
Agent/generation, exact native subject, source/view, turn filter, ordering and selected
store. Items require an exact turn id. A supplied history `turn_id` is limited to 16 KiB
and rejects control characters. The `ObservationQuery` cursor bound is nonempty and at
most 16 KiB; it has no additional control-character rule. Pages contain at most 1,000
entries and are at most 900 KiB encoded. `entries` and `coverage` are opaque JSON values;
the wrapper does not reinterpret native/vendor records or coverage fields.

The Router rejects a non-null `next_cursor` equal to the request cursor, regardless of
source coverage. An absent cursor does not let a wrapper infer complete history: preserve
the opaque coverage object and do not synthesize completeness.
Unsupported source/paging stays explicit. Observation never resumes a thread to obtain
history. See the neutral
`ObservationPage` bounds in [plugin protocol](plugin-protocol.md#host-requests).
Observe detach fences new reads, closes only its reader and records source closure
after cleanup. It retains unknown native state and never reports native session end.

Controlled detach applies only to independently living sessions/services and remains
unsupported until that native path exists: revoke control and become observe-only on
exact success without killing the shared/adopted service. It does not settle an unknown
input. Controlled resume reopens the same native subject as a new generation and never
resends previous input.

## Event replay

Event requests, responses and `EventsFilter` are defined in the canonical Core API.

Before printing events or advancing a cursor, validate each response page against the
effective request cursor: `0 <= after <= next <= high_water`. Event `seq` values must be
strictly increasing, greater than `after`, and no greater than `next`. A malformed page
returns a constant `unknown` error. Do not clamp the response, default a bad cursor,
replay from another cursor, or skip forward.

`after` defaults to 0 only when omitted. If supplied, it must be a nonnegative `i64`;
`null`, negative, fractional, string, boolean and overflowing values are `invalid`.
`limit` defaults to 100 and must be an integer from 1 through 1,000. For both methods,
an omitted `filter` means an empty filter. Explicit `null`, an unknown request or filter
field, or any wrong field type is `invalid`. Each `agents`, `kinds` or `sources` array
may contain at most 1,000 raw entries; every entry must be a nonempty string. Agent filter
strings are compared verbatim rather than canonicalized, so a noncanonical value may
simply match no events. An Agent principal remains scoped to itself; an explicit nonempty
agent filter that excludes it returns `not_allowed`.

Only `events.subscribe` accepts `wait_ms`. It defaults to 0, must be a nonnegative `u64`
when supplied, and values above 30,000 are clamped to 30,000. Bad types, negative values
and overflow are `invalid`; `events.read` rejects `wait_ms` as an unknown field. This wait
fills an event page and does not wait for command or turn settlement.

Subscribe before reading the persisted high-water mark, replay through that mark, then
release buffered persisted events above it in order. `next` is the exclusive replay
cursor; `high_water` reports the persisted high-water observed by the read. A buffer lag
sets `lagged` and triggers persisted replay from the current cursor. `truncated` reports
page count/encoded-size limits. Ephemeral delivery is best effort; pruned history returns
`history_gap`, never silent skipping. Committed event sequence values are never reused.

For these known methods, malformed params, wrong types and unknown fields return
`invalid`. Unknown methods and unsupported command verbs remain `unsupported`; this keeps
request grammar errors distinct from feature availability.

## Mail acknowledgement

Mail belongs to the stable recipient Agent, while its presentation command remains
bound to an exact generation. A valid current Agent may explicitly read older unread
mail for that same Agent; responses show the original recipient generation and any stale
presentation. Resume does not resend the old prompt or retarget its presentation command.

`mail.inbox` returns at most 100 deliveries within the encoded response-frame limit
and marks nothing. `mail.read` is also read-only. After writing complete output, CLI sends:

```text
mail.ack { id: <new write id>, deliveries: [<exact delivery id>, ...] }
```

Authorize the current caller for that stable mailbox and acknowledge only those ids.
A lost response may redisplay mail, not lose it or acknowledge newly arrived messages.
See [coordination](coordination.md#mailbox) for bounds and presentation policy.

## Drain and shutdown

`daemon.shutdown` uses the canonical `DaemonShutdownRequest`/`DaemonShutdownResponse`.

This owner-only request requires exactly an `id` and the `ShutdownMode` value `drain` or
`force`; unknown fields, missing fields, wrong types and nulls are `invalid`. Its first
reply is retained under the literal mutation ID. An exact retry returns that same reply,
including the original drain blocker snapshot; use owner-only `daemon.status` for current
state. Reusing the ID with another mode returns `id_conflict`.
Force admission is monotonic; a later, in-flight, or replayed drain cannot downgrade it.

Drain returns `state:"applied"` with an empty typed blocker list when no blockers were
observed, otherwise `state:"dispatched"` with the captured `StatusBlocker` list. Force
normally returns `state:"dispatched"` and no blocker list. If a Store write fault
prevents retaining the force reply, the fallback still dispatches force and returns
`store_write_faulted:true` with the dispatched state; that fallback is not a retained
write result. When present, `store_write_faulted` is true.

Drain rejects new turn-producing work and new automation, while continuing pre-admitted
work and allowing exact stop/interrupt/request replies/cancel/detach/readback. Unknown
outcomes remain visible blockers. Force terminates only owned custody, never adopted
services. Degraded Unix reports possible orphans, not exact clean success.

## CLI

All commands accept `--json` and `--data-dir`; every mutation accepts `--id`.
Generation/turn/request flags are optional for immediate calls, explicit for delayed ones.

```text
eliot daemon [--foreground]
eliot daemon stop [--drain|--force]
eliot status
eliot launch --profile P --workspace PATH [--worktree BRANCH [--base REF]] [PROMPT] [--wait]
eliot worktree remove AGENT [--generation N]
eliot ls | show A | watch [A|SELECTOR] | top
eliot processes A [--scope agent|workspace] [--generation N] [--after CURSOR] [--limit N]
eliot stats A [--generation N]
eliot git-status A [--generation N]
eliot edits A [--generation N] [--after PATH] [--limit N]
eliot send A TEXT [--probe] [--wait] [--generation N]
eliot steer|inject A TEXT [--generation N] [--turn ID]
eliot interrupt|abort A [--generation N] [--turn ID]
eliot stop A [--force] [--tree] [--generation N]
eliot resume A [PROMPT] [--wait]
eliot wait TARGET... [--for turn|idle|stopped|goal] [--after-command ID]
eliot result A [--generation N] [--turns N]
eliot turn show|resolve ...
eliot approve|deny|answer ... [--request R]
eliot configure ... | compact|fork|background ... | goal ...
eliot cmd show|children|cancel|resolve ...
eliot events [--since SEQ] [--allow-gap]
eliot msg send ... | inbox [--wait SECS] [--all]
eliot var ... | who ... | claim ... | release ...
eliot rule ... | rules pause|resume | timer list|run
eliot discover --profile P [--harness H] [--scope S] [--cursor C] [--limit N]
eliot attach --profile P --harness H --scope S --ref R [--project LABEL]
eliot history A --view subject|turns|items [--source native|rollout] [--turn-id ID] [--cursor C] [--limit N]
eliot detach A [--generation N]
eliot provider holds [--after SCOPE] [--limit N]
eliot provider clear-hold SCOPE --corrective-evidence credentials_updated|configuration_updated|owner_verified [--expected-probe-command ID]
eliot rule show ID
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Only current-layer commands appear in help and Agent guidance. Exit codes: 0 success/
queued; 1 definite failure; 2 usage; 3 pre-effect rejection; 4 unknown; 5 wait timeout;
6 still dispatched/pending. A queued/applied input is not successful task completion.

`processes`, `stats`, `git-status` and `edits` make one read call and print the bounded
JSON response. They do not start a daemon or submit a mutation. The `--after` strings
for `processes` and `edits` are forwarded unchanged to the Router for validation.
