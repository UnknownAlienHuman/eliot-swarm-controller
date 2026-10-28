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
| `daemon.status/shutdown` | current state, admission mode and exact drain/force |
| `agent.list/get/result` | scoped, paginated Agent state and exact results |
| `agent.processes/stats` | scoped process records and ephemeral OS samples |
| `agent.git/edits` | current Git snapshot and paginated native-attributed edits |
| `command.submit/get/children/resolve` | immutable admission, linked commands, child readback and constrained resolution |
| `turn.get/resolve` | running/completed/unknown turn; constrained owner assertion |
| `events.read/subscribe` | persisted replay plus bounded live delivery |
| `agent.discover` | owner-only native metadata discovery, without creating Agents |
| `agent.history` | owner-only bounded history of an exact observed subject |
| `hook.event` | later exact request/deadline decision |
| `mail.send/inbox/read/ack`, `var.*`, `who`, `claim.*` | later coordination |
| `rule.*`, `timer.*`, `goal.*` | later automation; effects remain commands |

Filter shape is `{kinds?,agents?,tree?,sources?,ephemeral?}`. Errors include
`not_found`, `unsupported`, `not_allowed`, `bad_state`, `stale_generation`, `invalid`,
`busy`, `unavailable`, `capacity_unavailable`, `history_gap`, `source_integrity`,
`secret_in_payload`, `frame_too_large`, `mailbox_full`, `internal`.

Process reads use the existing daemon and private data directory:

```text
agent.processes {id, generation?, scope?:agent|workspace, after?, limit?}
  -> {agent, generation, processes:[ProcessRecord], next:string|null}
agent.stats {id, generation?}
  -> {agent, generation, ...monitorView}
```

Generation is a nonnegative integer; omission selects the current generation for
default scope `agent`, or the managed workspace's target generation for scope
`workspace`. The latter may still be preparing while the Agent is generation 0.
The Router validates the exact preparation target before allowing a future-generation
workspace read. Owners may read historical generations with `agent.processes`;
`agent.stats` accepts only the current generation, with an explicit generation acting
as an expectation. Process ownership is `agent:<id>#<generation>` or
`workspace:<id>#<target_generation>`. Each page selects one scope; the cursor stays
within that query's owner and generation. `agent.processes` is
paginated, with default limit 100 and allowed range 1–1,000. Its `after` cursor is an opaque string
containing `{agent,generation,scope,identity}`, where `identity` is the OS birth-bound
`ProcessIdentity` (`host_boot_id`, `pid`, `start_time`). Pass it back unchanged;
a different Agent, generation or scope rejects before querying process rows.
`next` contains the last returned identity even on the last nonempty page; continue
until `processes` is empty rather than using non-null `next` as proof of another record.

Process identity/state and CPU-time/RSS samples are distinct. Statistics are ephemeral,
and absent values remain unknown, never zero. Process activity does not establish
model progress; incomplete observation does not prove custody empty. `agent.get` retains
the Agent object and may enrich it with `observation`; `show` prints that object unchanged.
Windows canonical path strings remain opaque to these CLI reads.

Git and edit reads use the same existing daemon:

```text
agent.git {id, generation?}
agent.edits {id, generation?, after?, limit?}
```

Generation is nonnegative and defaults to the current generation. The Router validates
scope and requires the current generation for Git snapshots; owners may query historical
edit attribution. Edit pages default to 100 rows, with limits 1–1,000. `after` is an
opaque path string passed back unchanged; the CLI does not resolve it against the local
filesystem or normalize Windows canonical paths.

Git snapshots report current HEAD/branch/dirty paths and recovered commit history, with
`complete`, `history_complete` and `history_gap` describing observation coverage. Missing
or incomplete history is not an unchanged or clean workspace. Edit attribution requires
verified, generation-matched native file-change hints; a Git dirty set alone never proves
Agent authorship. Git polling alone updates neither process statistics nor model progress.

## Command admission

```text
command.submit {
  id, target?, verb, args,
  expected_generation?, expected_turn_id?, request_id?
}
-> {command_id, agent?, generation?, state, outcome?, linked_command?, fanout?:{count}}
```

In one transaction resolve the target, validate principal/capability/state, compare
supplied expectations, capture exact generation/turn/request, then persist immutable
rows/events. Interactive calls may omit expectations; delayed automation supplies its
captured expectations. Mismatch rejects before native I/O.

The current request does not accept `selector` or `probe`; the Router rejects either
field. These remain deferred `command.submit` extensions (`selector?`, `probe? [false]`),
not fields to send to the current API. The future selector contract is below; probe
semantics remain with its future owning feature and are not inferred here.

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

These are read-only lookups of existing records. Their IDs are never sent through a
mutation call and cannot submit or replay work. The current typed request/response
shapes are:

```text
command.get {id:string} -> Command
turn.get {agent:string, generation:i64, turn_id:string} -> Turn
agent.result {id:string, generation?:i64, turn_id?:string, limit?:usize}
  -> AgentResultResponse {agent:string, generation:i64, turns:[Turn], agent_metadata:Agent}
```

`Command` and `Agent` use their Core model fields. The current `Turn` object contains
`agent`, `generation`, `turn_id`, `state`, `status`, `final_text`, `started_at` and
`completed_at`; it has no `command_ids`, `usage` or `resolution_source` fields. Those
remain future turn-result or resolution metadata. `agent.result` decodes `turns` as an
array of objects and `agent_metadata` as an object.

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

Discovery is read-only/paged and creates no Agent rows. Attachment uses one canonical
native subject row. First attach targets generation 1; a later ownership takeover gets
the next generation. Validate before dispatch, commit identity fence before native I/O,
and install exact readback on success. Failure never restores revoked control; unknown
blocks another takeover.

The first discovery caller requires an explicit existing profile:

```text
agent.discover {profile, harness?, native_scope?, cursor?, limit?}
  -> {harness, native_scope, subjects:[{native_ref,workspace,state,
       created_at,updated_at,source,ephemeral}], next_cursor,
       coverage:{kind,complete}}
```

Limits default to 100 and range from 1 to 1,000, with a 900 KiB encoded page bound.
Pass the opaque cursor unchanged; it is bound to the selected profile, target environment,
verified native store, method, ordering and source filters. Changed store selection
rejects before listing subjects. Unverified selection is an explicit error.
`coverage.complete` means this paged enumeration
has reached its native end, not that it is a consistent snapshot or a live subscription.
Codex coverage is `state_db_only`; it never scans/repairs rollout storage to fill the
list. The current Codex query selects nonarchived threads and fixes the page size in
its cursor binding. Subject timestamps are milliseconds since the Unix epoch; native
seconds are converted before projection. Session previews, prompts and transcripts are
excluded. Native `notLoaded` is
reported as unknown: a separate reader cannot prove another process's session stopped.
Discovery installs no Agent identity and invokes no native start/resume/input operation.
Owned readers finish cleanup after a client disconnect; drain waits for them and force
cancels their own custody. New discovery rejects during shutdown.

The first observation mutations use the existing command pipeline:

```text
command.submit {id, verb:"attach", expected_generation?,
  args:{profile,harness,native_scope,native_ref,mode:"observe",project?}}
command.submit {id, target:AGENT, verb:"detach", expected_generation?, args:{}}
agent.history {id:AGENT, generation?, source?:"native"|"rollout",
  view:"subject"|"turns"|"items", turn_id?, cursor?, limit?}
```

Require an explicit profile. Resolve the same mutation id before rereading mutable
configuration; retained retries keep their original subject/profile. Attachment reuses
the canonical scope/reference row. A controlled row or an active observer returns
`busy`; this increment does not convert control or grant a second identity. Attach
commits its generation/source fence before reader I/O and installs no Agent token.
All native control verbs reject observe-only targets, including owner-issued stop.
`mode:"control"` is unsupported. The vendor plugin method is `agent.attach`; it is not
a second public mutation method.

Successful reader attachment can leave the Agent `unknown`. History pages are private
views: they neither replay lifecycle into the event bus nor acknowledge input, command
or request delivery. Bind every page/cursor to Agent/generation, exact native subject,
source/view, turn filter, ordering and selected store. Items require an exact turn id.
Limits are 1–1,000 entries, a 16 KiB cursor and a 900 KiB encoded page. Unsupported
source/paging stays explicit; observation never resumes a thread to obtain history.
Observe detach fences new reads, closes only its reader and records source closure
after cleanup. It retains unknown native state and never reports native session end.

Controlled detach applies only to independently living sessions/services and remains
unsupported until that native path exists: revoke control and become observe-only on
exact success without killing the shared/adopted service. It does not settle an unknown
input. Controlled resume reopens the same native subject as a new generation and never
resends previous input.

## Event replay

```text
events.read {
  after?: i64, limit?: usize, filter?: EventsFilter
} -> EventsReadResponse {
  events: Vec<Event>, next: i64, high_water: i64, truncated: bool
}

events.subscribe {
  after?: i64, limit?: usize, wait_ms?: u64, filter?: EventsFilter
} -> EventsSubscribeResponse {
  events: Vec<Event>, next: i64, high_water: i64, truncated: bool, lagged: bool
}

EventsFilter {
  agents?: Vec<String>, kinds?: Vec<String>, sources?: Vec<String>
}
```

The shared Rust DTO surface for these two methods is
`core::api::{EventsFilter,EventsReadRequest,EventsSubscribeRequest,EventsReadResponse,EventsSubscribeResponse}`.

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
eliot send A TEXT [--wait] [--generation N]
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
eliot service list|stop | hold list|clear | doctor
eliot plugin NAME | plugin-test ... | hook ... | mcp
```

Only current-layer commands appear in help and Agent guidance. Exit codes: 0 success/
queued; 1 definite failure; 2 usage; 3 pre-effect rejection; 4 unknown; 5 wait timeout;
6 still dispatched/pending. A queued/applied input is not successful task completion.

`processes`, `stats`, `git-status` and `edits` make one read call and print the bounded
JSON response. They do not start a daemon or submit a mutation. The `--after` strings
for `processes` and `edits` are forwarded unchanged to the Router for validation.
