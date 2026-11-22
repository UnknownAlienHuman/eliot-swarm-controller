# Plugin protocol

A plugin translates one native harness, normalizes its facts and supplies native
readback. Shared Rust owns retry timing, event-control deadlines, common lifecycle
decisions and diagnostic redaction. The daemon owns durable command settlement and
SQLite writes. The host and plugin share wire types and use vendor types only inside
the adapter.

## First harness subset

L1/L2 implement `initialize`, `agent.start`, `agent.command`, `agent.stop`,
`agent.snapshot`, `shutdown` and durable `events.put` for one per-Agent source. Start
with the verbs of the five native adapters (Codex, Claude, OpenCode, Muse,
Antigravity). The manifest advertises only implemented capabilities.

The first L3 process caller adds private `process.put` for the Codex app-server child.
It registers a process with the host monitor, without adding a native stream family.

Add discovery, control takeover, transcript handoff, shared services, children and hooks
with their roadmap caller. System-level context injection uses the existing command and
authenticated plugin-channel paths described below. A missing native application point
returns `unsupported`; do not create empty handlers, tables or generic frameworks to
anticipate it.

## Process modes

`per_agent` is one plugin/native process tree per controlled Agent generation. `shared`
is for a service with demonstrated exact session attribution/environment/readback and
isolated stop. Hosting multiple chats alone is insufficient. Adopted services are never
killed for an Agent. Shared support is not required for the first Codex path.

## Framing and bounds

JSON-RPC 2.0 over UTF-8 NDJSON; stdout is protocol only. Both directions may send
requests with direction-local ids. Agent messages carry `agent_id` and `generation`.
Wire version is `eliot-plugin/1`. Add fields compatibly; do not change existing meanings.

Drain each owned child stderr pipe continuously. Retain only a 16 KiB tail per child,
redacted by the shared `SecretPolicy` before retention. The structured snapshot is
`{text,truncated,eof,read_error}`; reaching the cap never stops drainage. Preserve plugin
and native-harness tails as separate source-tagged diagnostics. A possible secret prefix
split across reads remains withheld, including at EOF, cancellation or read failure.
These diagnostics do not participate in protocol framing or authoritative result state.

`Error` may carry at most two distinct `StderrDiagnostic` sources (`plugin` and
`harness`), each with a text tail no larger than 16 KiB. Keep the semantic error code
separate from diagnostics. The encoded diagnostics array is capped at 16 KiB in total,
including JSON escaping. Error serialization reserves space for its primary code and
message within the 64 KiB encoded Error bound. Do not serialize free-form error messages,
request arguments or environment values into the log record.

The host keeps four interactive metadata-reader slots and an independent 32-slot
launch-preparation lane. A launch's `profile.store` waits fairly for its own slot
before opening a reader, rather than refusing a transient full pool.
Only confirmed reader custody cleanup releases its slot; unknown cleanup retains it.
Shutdown closes slot admission and wakes waiting launches. Catalogue (`model.list`) and
quota reads wait up to 8 s for an interactive slot (a desktop asks every harness at once
when it connects); after that wait, and for other explicit discovery reads at once, they
refuse `busy` with a message naming `metadata_readers` and its limit.

Catalogue requests use [`ModelListParams`](../../crates/eliot-plugin/src/protocol.rs).
OpenCode reads ids and model metadata (names and variants) in separate reader lifetimes, sharing one admission
slot and read deadline. Each phase closes its owned family before the next starts
or a result is returned. The host joins metadata by exact `provider/model` ID.
Optional metadata failures permit ids-only output only after
confirmed cleanup; cancellation or unconfirmed closure never becomes success.

Maximum encoded frame is 1 MiB including the NDJSON newline. Inline input is at most
512 KiB; persisted event data/final text 64 KiB; pages at most 1,000 entries and below
900 KiB. Both semantic and encoded limits apply, including JSON escaping. Reject
oversize input before native I/O.
Continuously drain cancel-safe readers; never cancel a partially read line. Credentials
are excluded from logs, recordings and plugin state.

The [shared event actor](../../crates/eliot-plugin/src/event_actor.rs) uses one
deadline for submitting a control and waiting for its reply. It uses the
caller's deadline when supplied and 25 seconds otherwise, inside the host's
command reply window. An elapsed or lost reply reports `unknown`.
Admission-sensitive callers can distinguish an
unavailable queue or a task that stopped before it took the control. A timeout never
authorizes replay. A queued control may still finish behind an in-progress native fact.
Internal hook offers and completion notifications wait for queue space while
their event consumer remains open. This keeps settlement deliverable during
backpressure and allows forwarding to continue when the consumer catches up.

The [shared retry policy](../../crates/eliot-plugin/src/retry.rs) schedules another
attempt only from a proven pre-effect failure or an idempotent operation. Safe startup
retries once with a 25–100 ms delay. A session hook bind makes at most three attempts,
100 ms apart. Both stay within the caller's deadline. Repeated observations of the
same outstanding hook offer pace from 250 ms up to 2 s. The first offer forwards
immediately. A failed hook read waits 250 ms before the next read.

Read-only native CLI helpers bound stdout and process exit under the caller's deadline.
Claude auth status can consume its JSON result on either exit status. The same shared
reader reaps the owned helper on a deadline or read failure.

## Manifest

An initial plugin may declare:

The [`PluginManifest`](../../crates/eliot-plugin/src/protocol.rs) DTO owns the manifest
field catalog.

`proxy` remains a string capability. "none" remains valid for direct native
adapters (for example direct Codex). A controlled Codex profile with an adopted Responses route
advertises "responses" to declare per-Agent route-configuration support. This does not
claim provider authentication or inference success and does not add a flag or enum.

Extend verbs/families/features only when their paths exist. Version is diagnostic;
methods/fields/capabilities establish compatibility. The host selects one owner per
normalized native family. Hooks never mirror lifecycle already supplied by the stream.

## Host requests

| Method | Essential fields | Meaning |
| --- | --- | --- |
| `initialize` | protocol, frozen plugin config, private instance directory, optional `store_root` (required by OpenCode) | return actual manifest |
| `agent.start` | agent/generation, profile, workspace, target environment, note, source plan/state, optional resume subject/linked initial input | establish session; input/result settle separately; a proven-no-effect failure returns `"state": "failed"` with a short non-secret `reason` instead of an error, and the launch settles failed, never unknown |
| `agent.command` | agent/generation, command id, verb/args, exact turn/request where required | applied/rejected/failed/unknown, native mechanism and application point |
| `agent.stop` | agent/generation, command id | gracefully close the bound controlled native session and drain its events |
| `agent.snapshot` | controlled command/turn binding, or exact subject plus `observation` page query | bounded readback; private history never acknowledges controlled effects |
| `agent.models` | exact Agent and generation ([`AgentModelsParams`](../../crates/eliot-plugin/src/protocol.rs)) | read the owned session's model catalogue; OpenCode advertises `models` in its manifest and echoes the exact binding. A control request can preempt this bounded read |
| `agent.attach` | exact subject, generation, observe/control, profile/environment, source plan/cursors | attach only to that subject; otherwise not found/unsupported. A bound `failed` result is limited to a proven pre-native refusal or `native_start_failed` (no thread/session effect and confirmed native-helper cleanup); uncertain outcomes remain unknown. |
| `agent.detach` | exact observed subject/generation and command id | close only its reader; never end the native session |
| `discover` | frozen profile, target environment without Agent identity, optional exact scope/cursor, `archived` (default `false`), bounded limit | read-only native-subject metadata page |
| `native.archive` | shared [`NativeArchiveParams`](../../crates/eliot-plugin/src/protocol.rs): frozen profile, exact native reference, and for OpenCode the Agent id, retained native scope and workspace | archive or delete exactly one retained native session of a terminal Swarm-created Agent through the documented native call (Codex `thread/archive`, OpenCode `DELETE /api/session/{id}` on an owned reader server bound to the retained store). OpenCode returns `native_archive_refused` for a confirmed refusal and reports uncertainty if exact readback is unavailable after an ambiguous response. The daemon owns all cleanup policy and keeps raw store files out of cleanup. Other harnesses return `unsupported` |
| `profile.store` (L3) | shared [`ProfileStoreParams`](../../crates/eliot-plugin/src/protocol.rs) | Return [`ProfileStoreResult`](../../crates/eliot-plugin/src/protocol.rs): the effective `NativeStoreBinding` and optional plugin-owned startup projection, using the caller's actual prepared workspace and one bounded, owned metadata reader. Store freezes both with the exact launch dispatch; success requires confirmed reader cleanup. It creates no model or thread, performs no native control and publishes no Agent events. This is outside the L1/L2 first-harness subset. |
| `shutdown` | none | close owned paths, drain, exit; never kill an adopted service |

For an ordinary stop with an active turn, the router attempts `agent.command`
with `interrupt` under the stop's authority. It then sends `agent.stop` following
the [generation and turn rules](core.md#states-and-reducer). The daemon alone
terminates owned custody when needed, using the generation's Windows Job or the
weaker Unix process custody. Explicit force bypasses plugin stop.

Stop is applied only after independent, bounded process-custody exit and native
source-drain proofs are committed for that generation. A missing native stop
reply or `session.ended` terminal does not block applied once both proofs hold;
the result records `native_session_end_observed=false`. If either proof is
missing, the stop remains unknown with the missing proof named. The daemon never
reissues an uncertain native call.

The [shared session runtime](../../crates/eliot-plugin/src/session.rs) sequences
plugin-side cleanup and drain. Adapters supply their native close and readback
steps. Antigravity waits for notification of its exact turn's settlement after
dispatching cancellation. Only an observed cancellation terminal confirms it.

Steer on harnesses with a direct native steer RPC uses an ordered, bounded write followed
by a correlated response wait: the host writes its `agent.command` frame in dispatch order
under the writer deadline. The host then waits up to the plugin RPC response deadline
while continuing to process `interrupt`, `stop`, `agent.snapshot`, and lifecycle messages.
A timeout releases the correlation slot and settles the command as `unknown` with the
elapsed deadline as its reason. An unresolved transport or generation teardown also settles
as `unknown`. The host never replays the steer.

A plugin with no native steer declares `steer_delivery: "turn_safe_point"` in its
`session.ready` effective settings (Claude does); absent means a direct native steer, and
any other value rejects the event as `invalid`. A daemon restart ends every live
generation, so each live session carries the declaration of the plugin build that started
it. Such a session's `steer` does not use `agent.command`: the dispatch transaction leaves it
pending, and Store claims it alongside `turn_safe_point` injects through the authenticated
`hook.event` callback and settles it from the committed hook receipt.

An optional shared Core `RouteBinding` travels inside the frozen profile snapshot; it
does not add a service RPC or a new harness. The controlled Codex process must apply the
explicit named-provider override and read back its effective configuration before
starting a session. It must not mutate the owner's global Codex configuration or claim
provider authentication, inference, or auxiliary traffic. An adopted endpoint remains
outside Agent custody and is never stopped by Agent or daemon shutdown.
Eliot does not issue model/list or entitlement probes. The native app-server may issue a
vendor-owned read-only catalog GET at startup; the route must pin its supported catalog
URL to the selected provider or suppress it before process birth. The metadata reader
must not receive the client-key value, while retaining it in its diagnostic redaction
policy.

Treat the returned binding as metadata: reject metadata-secret values in it. Redact
reader errors and stderr with the broader diagnostic-secret set.

`resume_from={native_scope,native_ref}` is an `agent.start` option, not replay of the
old input. `initial={command_id,text}` is supplied only for a harness that requires
input to create a session. Explicit new text remains a separate linked command.

`eliot_core::AgentStartParams` is the shared start-parameter definition used by Store
preflight, host dispatch and plugin decoding; Plugin reexports it. Missing optional
fields decode as `None`, and serialization retains their nullable wire fields. Store's
preflight cannot include runtime secrets/environment or later source readback. The host
must check the complete encoded request, including its actual id and NDJSON newline,
after filling those fields and before enqueueing the request. Admission preflight is
not proof that the completed runtime frame fits.

The router enqueues first `agent.command` deliveries at the Store dispatch boundary.
It appends to the bounded runtime mailbox under the Store writer lock after commit, so
deliveries to a plugin follow dispatch sequence order. A missing owned runtime or a
full mailbox rejects the command with proof that no native write was dispatched.
The actor checks the complete encoded request, including its actual id, arguments and
NDJSON newline, against the 1 MiB frame bound before writing the plugin RPC.

Each command ID is delivered through `agent.command` at most once per plugin generation.
Store answers exact settled retries. An unresolved dispatch can settle from exact
read-only native evidence through `agent.snapshot` or from later authoritative source
facts. Without such evidence its outcome remains `unknown` and the router never
redelivers the write. A plugin retains only native handles bounded by live state, such
as the current native turn binding. Store owns command identity, history and settlement.
An initial send linked to launch or resume follows ordinary send dispatch.

A controlled snapshot using the session's start or stop command ID may omit the turn
ID to request session state. Other command snapshots need an exact turn, supplied by
the caller or proven by the adapter's current native binding. A read that cannot prove
the requested binding returns an unknown snapshot. When an exact-turn snapshot
obtains fresh native evidence, it reports the Agent, generation, command, native
scope/reference, session state, active native turn, and the exact turn's
status and terminal flag. `turn_evidence` names the native read that established the
facts. An idle session alone does not establish the turn's outcome. The status uses
the universal vocabulary (`completed`, `interrupted`, `failed`) and keeps the native
word in `native_status`. A stream adapter with no native turn-status query (Claude,
Antigravity) reads its own owned reader: a bound turn without its terminal record, on
a healthy source and a live process, is running; the exact terminal record is
terminal. The adapter retains that record's bounded facts (universal status and raw
native status) before writing anything for it, so a failed later write does not erase
the evidence. A terminal the adapter exposes before its own terminal write resolves
carries `last_turn.write_pending: true` until that write commits or fails. When the owned process exit is observed after its output fully drained
with no terminal for the exact turn, the snapshot reports `state: "lost"`,
`process_exited: true`, no active turn and a non-terminal `last_turn`. A declared
source gap or lost stream is not evidence.

For SEND, the same `agent.command` effect carries the FIFO `next_input` items claimed
from existing command rows, plus the effective configuration overlay when one
applies: optional `model_override`/`effort_override` resolved by the daemon
from the send command's retained revision, with `config_revision` echoing that
revision. The adapter prefers the overrides over its session values when
building the native turn request and echoes them in its applied response;
without an overlay all three stay absent and the frame keeps its exact
historical shape. The adapter prepends those items inside that native send;
it does not issue a second send. It reports their application only with the original
send's exact turn association using `turn.started.data.context_outcomes`, with one
`ContextOutcome` per claimed item in FIFO order. Store checks the exact claim count and
consumer binding, then applies them in the same transaction as that turn association.
`turn_safe_point` items are delivered through the hook callback below for their captured
turn. An `active_turn` inject is dispatched directly as `agent.command` with verb
`inject`, the captured exact turn, and the original `InjectArgs` payload; the adapter
applies it through its actual native turn/steer effect on that turn. An applied ack
carries `context_outcome` with mechanism `codex.turn/steer` or `muse.turn/steer`,
application point `active_turn`, and the original inject command id as consumer.
A rejected or unknown native result settles the command unchanged; an uncertain
effect is never retried or carried into a later turn. The shared `ContextItem` and `ContextOutcome` types are defined in
[Core context types](../../crates/eliot-core/src/context.rs); their DTOs own the field
catalog.

`features.discover` defaults to false. A discovery instance initializes independently
of an Agent and has no authority to call `events.put`, `process.put` or other durable
Agent requests. Its result is `{harness,native_scope,subjects,next_cursor,coverage}`
using the bounded metadata shape in [API](api.md#discover-attach-detach-and-resume).
It owns only its reader process tree. `archived` is part of the cursor filter binding;
an adapter that does not support archived selection rejects `archived:true` as
`unsupported` rather than substituting the default selection. Profile/scope/filter cursor mismatches reject;
an oversized entry or malformed/nonadvancing page is an explicit error, not a skipped
subject or a synthetic complete page. Plugin shutdown closes the reader without
controlling any discovered subject.
Unix reader custody retains the documented process-group degradation; it does not
prove that escaped descendants exited.

The dispatch transaction already advanced the Agent generation, revoked old identity
and installed the source plan before native I/O. Controlled dispatch also installs its
target token; observation installs none. A plugin rejects stale
generation/turn/request before sending bytes. Unknown never rolls generation back or
rebinds a failed call to another process.

`features.observe` defaults to false. A supported observed manifest uses
`sources.observed` and the `native_read` mechanism. Attach requests carry
`agent_id,generation,command_id,mode:"observe",native_scope,native_ref,profile,
target_environment,source_plan,source_state`; the environment has no Agent identity.
Its response binds the same identifiers with `state:"attached"`,
`observation_state:"unknown"`, bounded metadata and coverage. This acknowledges reader
attachment, not native idle/readiness or prompt execution.

Observation snapshots carry the exact Agent/generation/scope/reference and
`observation:{source,view,turn_id?,cursor?,limit}`. Responses preserve those bindings,
entries, next cursor and explicit coverage. They are private historical views, not
`events.put` batches or command reconciliation. Detach returns `state:"detached"`
after closing its own reader. Observe reader loss never emits native session end.

Discovery and observation pages use the shared harness-neutral `DiscoverPage` and
`ObservationPage` DTOs, not vendor SDK records. Core exports `DiscoverSubject`,
`DiscoverCoverage`, `DiscoverPage`, `ObservationQuery` and `ObservationPage` from its
API module, along with `MAX_DISCOVER_PAGE_ENTRIES`, `MAX_DISCOVER_PAGE_BYTES`,
`MAX_OBSERVATION_PAGE_ENTRIES`, `MAX_OBSERVATION_PAGE_BYTES` and
`MAX_OBSERVATION_CURSOR_BYTES`.

`ObservationPage.entries` remains a list of opaque JSON values and `coverage` an opaque
JSON object; shared layers enforce page bindings, bounds and existing secret redaction
without assigning vendor-specific entry schemas. Limits remain 1–1,000 entries, 16 KiB
per cursor and 900 KiB per encoded page. The public host rejects a non-null `next_cursor`
equal to the request cursor. It does not interpret coverage or infer complete history
from an absent cursor; preserve source-specific coverage shape without rewriting its
semantics.

## Start ordering

Arm the reader/subscription before native create/resume/input. For a session-first
harness: create/resume without input → establish subject/effective settings → commit
`session.ready` → return start success → host sends the linked input normally.

The private start payload can carry the frozen [owner rules](configuration.md#shared-owner-rules).
Codex passes them as native thread developer instructions, Claude appends them from a
private instance file, and OpenCode loads them from the instance's isolated
`AGENTS.md`. The daemon supplies the first-input delivery for Muse and Antigravity.

For resumed `session.ready`, Store requires `data.native_scope` and `data.native_ref` to
match the retained subject pair exactly before reducing the event. `data.command_id` may be
absent or null; when supplied, it must match the unique Resume command for that target
generation. The plugin need not add command correlation when its readiness observation does
not carry it.

If a just-created OpenCode session fails verification and its removal cannot
be proved, the plugin returns `unknown` and keeps the server running with the
exact session identity in memory for readback. The generation stays unready.
If the plugin exits, the controller lacks a durable native subject for resume
or reconciliation. An unknown create is never replayed.

For an input-created session, perform start+linked input once but report session,
input-to-turn association and terminal result independently. Ready alone never proves
input delivery. Buffer early facts within a bound; overflow records a gap rather than
inventing completion or issuing another start.

## System context hooks

`hook.event` is a plugin-initiated request on the existing authenticated plugin instance
channel. The connection credential binds the Agent generation; the plugin cannot choose
or override that identity in the callback. The shared wire DTOs are named `HookEvent`
and `HookResponse`; their field catalog belongs in the [protocol DTO module](../../crates/eliot-plugin/src/protocol.rs),
while this spec owns their semantics. A callback reports
`before_tool` (Claude Code `PreToolUse`), `after_tool` (Claude Code `PostToolUse`), or `before_model` (OpenCode `context` hook and
Antigravity `PreInvocation`), its exact `turn_id`, and an opaque `consumer_id`; the response
carries `context[]` as shared Core `ContextItem` values.

For `turn_safe_point`, the host rechecks the authenticated generation, captured turn and
expiry, then commits FIFO claims in existing command rows before returning the response.
A committed claim is not yet an applied result: the existing command outcome records the
actual mechanism/application point/consumer, and an uncertain response is `unknown` and
never requeued. `next_input` items travel with the same ordinary `agent.command` SEND and
are not delivered through a later hook. If the callback times out or is cancelled before
claims are committed, it returns no context; any late context is discarded and never
applied on a subsequent turn. Permission decisions stay on the existing exact approval
callbacks.

After the native write is confirmed, the plugin publishes a `hook.before_tool`,
`hook.after_tool` or `hook.before_model` fact on the existing event path with the exact turn and consumer plus
one shared `ContextOutcome` for that callback. Store settles its fixed FIFO claims in the
same source-batch transaction. A failed or uncertain native write emits no applied fact;
the claim stays frozen and is never requeued. A confirmed receipt may reconcile a claimed
`unknown` in the same generation against its captured turn record even when its native
terminal won the race; selection always requires the exact active turn. This adds no second
source or lifecycle stream. The daemon selects the delivery point and handles an unclaimed
note that misses its turn. Hooks deliver the committed context selection and supply native facts.

A `hook.before_tool` fact may also carry optional bounded progress diagnostics taken
only from the native hook input: `tool` (bounded native tool name), `native_tool_id`
(bounded matched invocation id), `operation` (closed vocabulary derived from the tool
name only: `read`, `write`, `exec`), and `path` (one workspace-relative file path).
File paths use the same lexical workspace-relative normalization as native file hints;
an out-of-workspace path is dropped, never persisted. Full command lines, file contents
and every other argument are dropped at extraction. Each kept field passes through the
existing `SecretPolicy` redaction before its char cap. Absent native data stays absent,
so the kind, envelope and `on.event` compatibility of the fact are unchanged: a missing
field simply does not match an ordinary rule condition. A hook whose payload carries no
tool metadata (such as Antigravity `PreInvocation`, which is a `before_model` point)
emits no diagnostics.

## Durable plugin requests

| Method | Fields / result |
| --- | --- |
| `events.put` | stable batch id, agent/generation/source, complete bounded normalized native-record batch, optional exclusive cursor; returns committed seq range/cursor |
| `source.state` | private readback of source state, derived `observer_ready`, all current family owners, latest gap, batch acknowledgement, and latest handoff digest/ack |
| `source.health` | [`SourceHealthUpdate`](../../crates/eliot-core/src/lib.rs) request; returns the same four-field update after commit |
| `source.gap` | stable gap id, owned families, reason and optional from/to cursor; commits one gap boundary |
| `source.handoff` | `SourceHandoff` request; atomically returns `SourceHandoffAck` after commit |
| `process.put` | credential, agent_id, generation, pid, role; returns the OS-verified `ProcessRecord`, not plugin-authoritative process lifecycle |
| `child` | `ChildParams { credential, registration }` with `ChildRegistration` (registration id, parent/generation, exact child subject); canonical child row/source plan |

For an observe-only caller, an open `observe` row created during attach dispatch does
not grant publication. Private `source.state` derives `observer_ready` only from an
applied attach for the current generation and exact native scope/ref, the unknown
observe-only Agent without a control token, an open observe source and no detach fence.
`observer_active` or source-open alone is insufficient. Store recomputes this proof and
the captured subject inside each handoff, observed-batch and health transaction; callers
wait for settled readiness before handoff or health updates.

An observed `events.put` is restricted to the granted live families and current
transcript owner. Its records cannot contain `command_id`, `cause` or `request_id`, and
do not enter controlled command/input settlement. Private `agent.history` pages never
become `events.put` records.

### Commit acknowledgement

Normalize one native record into one atomic batch. Each event carries stable native id,
kind/family, timestamp, exact correlations and bounded data. The daemon computes
canonical event/batch digests, excluding transport ids/timing; cursor is compared too.
A source permits at most one unacknowledged batch. Keep later input in a bounded spool.

Host validates generation/owner/correlations and accepts either a wholly new batch or
an exact duplicate of its last committed batch. Commit events, current projections,
derived facts, cursor and last-batch acknowledgement together before replying/publishing.
The next commit replaces only that last-batch acknowledgement in `source_state`.

Same id with another semantic payload/cursor, or mixed new/duplicate events, is a
source-integrity rejection. The host replies with the committed acknowledgement or
a definitive no-write error, including authorization failures, Store lock deadlines
and rolled-back transactions. `unknown` is reserved for a commit whose fate the
Store cannot establish. After an unusable acknowledgement or broken channel, the
adapter stops publishing and the daemon retires the reader and records the missing
coverage. Adapters do not reconcile or retry event writes through `source.state`.

The host records gaps and health from rejected batches for authorized source owners.
A rejected `session.ready` before input can fail the start definitively. A rejected
write after native input dispatch leaves the native outcome unknown unless separately
proven. The exact dispatched command/turn binding still fences fresh input until
settlement. A replayable source resumes strictly after its committed cursor. A source
without an exclusive cursor starts at the live edge, records a gap and reads current
native state without republishing history.

Plugins publish only assigned native families. Agent/command/rule/mail/Git/process
lifecycle, `agent.lost` and owner resolutions are daemon facts, not arbitrary plugin
notifications.

### Gap and handoff

A source row's `open|closed` lifecycle, health and latest historical gap are independent.
A gap neither closes the source nor changes its owner or health automatically.
An adapter may report a family-scoped observation-coverage gap (for example,
bounded tool telemetry) without declaring the separately associated native
turn outcome or later input unavailable. A valid native message whose
`message.completed` data exceeds the persisted-event bound is one such `message`
gap ([`StreamSource::commit_message`](../../crates/eliot-plugin/src/session.rs)):
the turn keeps running and its terminal can retain a bounded final reply. A blank
native reply (empty after trimming) is not a `message.completed` fact at all: it
commits nothing and opens no gap. A gap naming only `message` never starts turn recovery. This requires a canonical gap ACK;
an unresolved gap/batch ACK still fences admission. Session/turn identity loss,
unproven delivery and transport loss cannot use this coverage-only path, and
it never clears existing uncertainty or replays a missing fact.
`source.health` changes only health and `updated_at` on an already-open current-generation
native source; an unchanged value is a no-op with no event. It does not clear a gap,
move a cursor, change ownership or reopen a closed row. A controlled caller binds
`agent_id`/`generation` to its source credential; the observe-only caller keeps its
settled exact-subject proof. The controlled path rejects reserved `daemon`, `os`,
`git` and `workspace_process` sources, stale generations, closed rows, and
observer/child escalation. The committed health event and
`source.state` readback carry lifecycle `state:"open"`; the four-field ACK does not.
The event is published before the response. A reader with an uncertain health reply
stops publishing on that source. Report `ok` only after verified live-edge recovery
and keep the historical gap visible. A successful private-history read does not
establish live source health.

The typed handoff contract is:

The [`SourceHandoff`](../../crates/eliot-core/src/source_handoff.rs),
[`SourceHandoffGap`](../../crates/eliot-core/src/source_handoff.rs) and
[`SourceHandoffAck`](../../crates/eliot-core/src/source_handoff.rs) DTOs own the
typed handoff field catalog.

`seq` is positive and is the committed `source.handoff` event sequence; it is independent
of the predecessor's `last_store_seq`. For a gap-backed handoff, `0 < gap_seq < ack.seq`.

`source` is the expected predecessor. `families` is a unique nonempty set; order is not
significant, and Store sorts it before hashing the canonical operation and forming the
ACK. Store rechecks the selected harness subset: Codex, Muse and OpenCode support
`session`, `turn`, `message`, `tool` and `request`; each other native adapter advertises
only the families its implemented handlers support. Muse `tool` facts are the
normalized `toolCall`/turn-bound `userShell` item revisions; a completed edit-family `toolCall`
attaches bounded `changed_paths` from the inline `args` target (`path`), else from one bounded
`item/readOutput` fetch of the stored `patchRef` headers, with `changed_paths_truncated` on any
coverage gap. `tool.started` and `tool.completed` may carry `native_at_ms`, the harness's
own time for that edge of the call (Codex `startedAtMs`/`completedAtMs`, Claude frame
`timestamp`). `native_background: true` on `tool.started` marks a call the harness runs as
its own background task; `tool.background_ended` (`turn_id`, `item_id`) reports that the
task ended and may commit after that turn. A command tool's `tool.started` may carry `command`, the command text the
harness reports (Codex: its parsed `commandActions[].command`, one per line, else the raw
`command`; Claude: the `tool_use` `input.command`; OpenCode: the
`session.tool.called` tool input string `command`, the shell tool), redacted with the secret policy and
bounded to [`MAX_TOOL_COMMAND_BYTES`](../../crates/eliot-core/src/limits.rs); command
output is never persisted, except the bounded, redacted tail of a failed Codex
`commandExecution` (`error_tail`): the whole native output is redacted, then the
LAST [`MAX_TOOL_ERROR_TAIL_BYTES`](../../crates/eliot-core/src/limits.rs) bytes are
kept at a character boundary, starting at a line boundary when one is retained, so
a long failure keeps its ending. A completed Codex `commandExecution` also carries
its native `exit_code` when present; a successful event grows only by that code,
and other arguments are never persisted. An item that does not match the installed
native shape keeps its prior `tool.completed` fact (status alone) rather than
failing the event. Handoff transfer for
`child`, `goal` and `usage` stays `unsupported`. Native-child observation is
implemented separately through the shared
[`ChildParams`](../../crates/eliot-plugin/src/protocol.rs) /
[`ChildRegistration`](../../crates/eliot-core/src/native_child.rs) contract and its
[observation grant](../../crates/eliot-store/src/native_children.rs): Codex supports
three observation families (`session`, `turn`, `message`), while Muse workflow-attempt
observation provides turn-only coverage ([`MUSE_NATIVE_CHILD_FAMILIES`](../../crates/eliot-core/src/native_child.rs))
for observe-only tokenless agents under normalized Eliot turn keys (no native MSP turn
identity, child principal, or control), with private history
([`agent.history`](../../crates/eliot-daemon/src/router.rs)) returning `unsupported`
at the daemon dispatch boundary. Daemon-owned
families and reserved `daemon`, `os`, `git` and `workspace_process` sources return
`not_allowed`. Exactly one
of `cursor` or `gap` is non-null. A cursor must equal the predecessor's committed
exclusive cursor; a gap carries its stable id/reason and optional from/to boundary.
There is no `handoff_id`.

The [shared native-child runtime](../../crates/eliot-plugin/src/native_child.rs)
validates registration acknowledgements and tracks observed turn transitions. The
adapter supplies native identity and terminal evidence. An unresolved registration
keeps its original id. Source loss leaves incomplete activity unknown and preserves
committed terminal facts.

`gap_id` identifies one uncovered interval. Same id and canonical fields return its
stored result; the same id with different fields is an integrity error. After that
operation is acknowledged, a new interval uses a new stable id and replaces
only the latest gap metadata; the previous gap event remains in retained history.

At most one unacknowledged gap/handoff operation is allowed per source. A lost
reply fences later writes from that reader.

Handoff is a single Store compare-and-swap. It verifies the current generation, an
open predecessor and predecessor ownership of every named family. A controlled caller
must have a controlled Agent; an observe-only caller must have the exact settled
`observer_ready` subject proof.
An absent successor row is created open in the same transaction; an existing successor
must already be open. Closed rows never reopen. Transfer only the named family-owner
rows. The predecessor lifecycle is unchanged, including when it owns zero families.

Commit any new gap boundary, successor-open fact, owner transfer, `source.handoff` event
and predecessor `last_handoff_digest`/`last_handoff` acknowledgement in that transaction.
Publish the committed events and return `SourceHandoffAck` only after commit. The
connected native adapter waits for the committed handoff ACK before publishing
successor facts. Private history cannot be handed off or published by this caller. The separate Codex observe-only live grant is limited
to `turn`, `message` and `tool`, with `observe` to `transcript` and mechanism
`rollout_tail`. It reads the exact subject's rollout file from its captured live edge,
commits an explicit gap-backed handoff before publishing complete normalized records,
and never publishes private history, control identity or input/command acknowledgement.

The private authenticated `source.state` readback includes paired nullable
`last_handoff_digest` (32-byte array) and `last_handoff` (ACK) fields, null before the
first handoff, and all current generation owner/mechanism rows.
`SourceHandoff::prepare(&mut self, policy:&SecretPolicy)->CoreResult<()>` is the shared,
idempotent normalization step. It rejects known secrets in immutable fields before
mutation and redacts only `gap.reason`; the canonical digest includes the full prepared
request, including the redacted reason. The host uses the same prepare path.
A reader with an uncertain handoff reply stops publishing on that source. It may
publish successor facts only after a validated acknowledgement. The public Agent
`source_states` projection omits the digest and ACK. No source epoch or open-id layer.

### Children and processes

Register only PIDs verified inside the calling Agent custody; shared server PIDs have
service custody. OS enumeration and registration upsert one process identity.

The first `process.put` caller sends `{credential,agent_id,generation,pid,role:"harness"}`
on its private plugin connection, using the actual child PID. It omits claimed birth;
the host derives and verifies `(host_boot_id,pid,start_time)` and custody, then returns
one `ProcessRecord` directly with owner `agent:<id>#<generation>`. The caller verifies
that the returned PID, owner and role match, the birth is present, and state is running
with no exit code. It registers after arming the reader and before native initialization,
thread create/resume or input. A registration error or invalid acknowledgement blocks
those operations and returns `process_registration` after confirmed child cleanup;
unconfirmed cleanup returns `unknown`. Neither case invents an identity or retries the
start. Process lifecycle and stats remain daemon-monitor facts; native lifecycle stays
on the already-owned stream.

Child registration is idempotent for parent generation/registration id/native subject.
Commit its canonical row and source plan before reply; buffer child facts until then.
Conflicting identity rejects; overflow becomes a child gap. Independent child tokens
exist only when the plugin can install an isolated environment before its tools run.
Otherwise native child calls inherit the parent principal and that limitation is shown.
A fanout guard never hides an already-created native child.

## Translation and normalization

Unsupported semantics reject; never quietly map steer to send, resume to fresh launch,
answer to a default choice or force-stop to shared-service termination. Steer/interrupt
carry exact turn and replies exact request. Report delayed application points.

Native inbox/HTTP acceptance is intermediate; send applies at exact input-to-turn
association. A correlated terminal response can establish that association even
when the vendor emitted no intermediate start notification; it is not a synthetic
completion inferred from silence. Persist the durable `command.applied` association
event in the same transaction as the source batch that proves it. An exact duplicate
batch/readback does not publish another `command.applied`. The event means the input was
associated with its exact turn; it is not the turn's terminal result. Final text, when
available, precedes the terminal turn; process exit or silence is not a turn result.
Session-scoped usage (`session.usage` with a `SessionUsage` snapshot) replaces
the retained snapshot for its Agent generation rather than being summed or
rolled up: a strictly newer `observed_at` replaces, an older or
equal-timestamp or duplicate snapshot is ignored. Adapters fold their native
session signals (Codex `thread/tokenUsage/updated` cumulative total plus
`modelContextWindow`, Muse `session/tokenUsage` cumulative plus cost and
`session/contextUsage` occupancy, Antigravity `usage_update` occupancy plus
cost, Claude `result.total_cost_usd`) into the latest merged snapshot and
commit each changed snapshot when accepted, bound to the exact
Agent generation with the adapter plus native signal name as `source`.
Cumulative usage replaces
its prior scope value rather than being summed. OpenCode attributes its final assistant
message's native token usage to the bound turn's terminal event at message scope; a
repeated message id replaces, and the value is never presented as a turn or session total.
When its latest assistant message has complete input, cache-read and cache-write
counters, OpenCode also commits their checked per-call context sum as
`context_used_tokens` without a window.
`session.context.compacted` is a session fact. Codex emits it for a confirmed v2
`item/completed` whose typed item is `contextCompaction`. The fact carries
`native_turn_id` and includes `turn_id` when the compaction belongs to the exact active
Swarm turn. Its native identity binds thread, turn and item. The manual compact command
still settles through the existing native lifecycle. The installed app-server favors
the item lifecycle over the deprecated `thread/compacted` notification.
An exactly associated OpenCode SSE execution terminal proves its status even when
HTTP transcript enrichment is unavailable, including after the native server exits.
Commit that status without final text or usage; unavailable enrichment is not a
contradictory transcript, and neither permits inferring a missing terminal event.

The terminal event preserves the exact command/turn binding, status and structured
provider restriction. The adapter translates the turn's final assistant reply into
`final_text`. A stream with tool boundaries uses the last assistant message after
the last tool invocation. Earlier messages retain their own `message.completed`
events within the message budget.
Adapters mark known synthetic or out-of-turn diagnostic messages with
`model_output: false` when retaining them as message events. These events provide
no proof that the model consumed the wake input.
After redaction, the shared retention path bounds the encoded terminal event. An
oversized `final_text` retains its ending after a plain `[... truncated ...]` line.
The same rule applies to every terminal outcome and recovered turn. A Core
`Turn.final_text` of `None` means public final text is unavailable.

Requested route/settings are not proof of effective values; unavailable readback is
reported as such. The host owns queued-send FIFO and unknown gates.

Injection must reach the requested native application point. `next_input` is prepended
inside the same native SEND and applies only with that SEND's exact turn association;
`turn_safe_point` is bound to its captured turn and delivered through the authenticated
hook callback; `active_turn` is dispatched directly as `agent.command` inject and
applies through the native turn/steer on the captured exact turn, never reported as a
safe point. Report the actual native mechanism and application point in the existing
typed context outcome. Accepted/queued context is not applied, and uncertain claimed
context is never retried or moved to a later point.

`turn.completed.data.native_error` optionally summarizes an available native error as
`{message,truncated}`. Redact known secrets with `SecretPolicy` before bounding its
encoded JSON to 4 KiB and the remaining event-data budget; omit it if it cannot fit.
Never invent unavailable text or alter terminal settlement, bindings, status, final
text or input acknowledgement for diagnostics. Raw vendor detail maps stay private;
message text never determines error classification.

`turn.notice` is a non-progress event in the existing `turn` family. Its Core
[`NativeNotice`](../../crates/eliot-core/src/api/agent_views.rs) data binds `turn_id`
and `NativeNoticeKind` (`retrying`, `reconnecting` or `error`) to the event's Agent and
generation. Optional message text uses the same `SecretPolicy` redaction and 4 KiB
bound as `native_error`; the projection limits it further for fleet pages. Store accepts
the notice only for the exact active turn. It does not settle the turn, update model
progress, create a provider hold, or authorize a retry.

The Codex adapter translates its native `error` notification only when `threadId` and
`turnId` match the owned thread and active turn. `willRetry:true` maps to `retrying`,
except the typed `httpConnectionFailed`, `responseStreamConnectionFailed` and
`responseStreamDisconnected` `codexErrorInfo` variants, which map to `reconnecting`;
other notices map to `error`. An absent turn binding is not attached to an Agent.
Transient notices never carry `provider_restriction`: only the existing exact-bound
terminal rule below may create or clear a hold.

An optional `turn.completed.data.provider_restriction` is the shared Core
[`ProviderRestriction` DTO](../../crates/eliot-core/src/routing.rs) and is actionable only for that exact controlled Agent,
generation, command and turn binding. A separate provider event, generic failure,
native-error message, or HTTP status alone does not create or clear a hold.

`failed` means a known unsuccessful completed operation, which may have partial side
effects; it does not mean rollback. `rejected` proves no native I/O. Missing authoritative
outcome is `unknown`. None authorizes automatic re-execution of an external effect.
After dispatch, only explicit `native_not_dispatched` proves no native I/O; any other
generic error defaults to `unknown`. A validated authoritative plugin result state is
handled separately from generic error classification.

During controlled startup, a plugin may return the internal
`native_endpoint_exited` signal only after its owned native process waiter observed an
exit, its native reader closed, and that reader task finished. The daemon still settles
the dispatched start/resume command as `unknown`; it drains the plugin event source and
confirms owned plugin-tree cleanup before marking the Agent `lost`. The signal does not
resolve the native effect or permit replay. Without all three native-exit facts, the
ordinary unknown-start behavior remains in force.

For every controlled session, including an unknown start/resume, the plugin
subscribes to its existing owned native-process waiter. A confirmed exit retires
that exact per-generation plugin after bounded native-reader and event-queue
drain, including commit acknowledgements and pending RPC settlement. A stream
gap, EOF or failed wait alone does not prove process death. Buffered terminal
turn evidence is committed before retirement; retirement never manufactures
`session.ended` or changes an already-applied command. The daemon independently
confirms its owned plugin exit and drains the plugin reader before its existing
generation-fenced loss settlement. Without a committed terminal turn, the running
turn becomes `unknown`; completed turns remain completed. For an active turn
with a committed native source gap (other than a `message`-only coverage gap), stale
health, or source closure, the daemon's attention
monitor makes at most four read-only snapshot attempts with backoff, for every
adapter. Fresh evidence that the exact turn is still running, or a terminal marked
`write_pending`, keeps it open for the next sweep. A `busy` read (the Agent's snapshot slot or queue is occupied)
proves nothing and spends no attempt: it also defers to the next sweep while the turn
has committed progress within `daemon.attention_suspicion_secs`. After that window a
busy read counts as inconclusive, so a source that stays busy still settles. The host
marks an `events.put` frame carrying this exact turn's `turn.completed` from decode
until intake finishes it, and no recovered outcome settles the turn while such a write
is marked. The adapter's write commits the turn, or its failure clears the mark for the
next sweep. These deferrals only spare a needless `unknown`; correctness does not
depend on them. The Store rechecks the generation, native subject, turn, and source episode
before committing `turn.completed` from a proven terminal and applying the terminal
event budget, `turn.unknown` with
`native_exit_without_session_terminal` at once from a lost readback, or
`turn.unknown` after four inconclusive reads. It retains the source gap and settles only that
turn. A settled turn never reopens or runs again. Of its later facts only the adapter's own
exact-turn `turn.completed` is accepted, and only against an `unknown` settlement: it is
evidence and resolves that turn once ([turn resolution](api.md#input-turn-and-unknown-gates)).
Every other late fact is refused. Retiring or duplicate
exit observations cannot settle a newer generation and never authorize replay.
An event owner closing before the tail barrier or a drain deadline expiring is
an explicit unknown retirement error, never successful tail settlement. Retirement
still ends the unusable plugin; the daemon records missing native evidence as gaps
and preserves unknown outcomes after its separate custody and reader settlement.
Every plugin command runs through the shared `native_lifecycle::run_plugin_entry`
runtime owner. Each per-Agent runtime uses two async workers rather than a worker
per host core; Tokio's separate on-demand blocking pool remains available for
stdio and file reads. This changes no reader ownership, native custody or drain
ordering. After adapter drains and replies finish, it flushes stdout with a
one-second deadline and bounds runtime shutdown to one second. An open daemon
stdin cannot keep an uncancellable Tokio blocking read alive indefinitely at
runtime teardown. This bound is not proof of native or plugin custody exit.
Failed-start native-exit evidence remains bound to the exact Agent generation
and start command until retirement, independently of start-response delivery.

Public requests and answers are universal: `request.opened` details and kinds use the
shared [`eliot_core::requests`](../../crates/eliot-core/src/requests.rs) shapes
(`QuestionRequest`/`ApprovalRequest`), and `answer` args use `AnswerArgs`. Only the
plugin translates between these shapes and its vendor format in both directions.
The desktop consumes the nested question DTOs: text entry, one-choice lists,
multiple-choice checkboxes, number text entry without rounding, and a Yes/No choice. Unedited
questions stay omitted from partial answers; Leave unanswered removes an
edited question without conflating it with an explicit empty string. Controls never invent native
selection bounds. Resource and confidential fields show instructions without
accepting a value. Replies keep the universal string-list shape and the exact
request/run binding; plugin-owned native validation remains authoritative.

Native-private request metadata stays in the existing plugin pending objects; plugins
normalize public event details and kinds only. There are no vendor-public
compatibility formats.

Vendor fields without a verified value-submission operation — notably a native
external form field that is applied by opening its URL outside the plugin — are
normalized as a neutral `QuestionKind::Resource` question with the field and form
title/description as descriptive context. The native URL itself is not persisted
in public details. The committed pending form remains owned by the plugin, so the
owner can still deny-cancel it, and ordinary partial answers omitting the resource
field remain available; a value submission for the resource is `unsupported` until
a verified operation exists. This preserves cancel and partial-answer behavior; it
does not claim a full external workflow.

## Lifecycle, hooks and qualification

Contain owned children before tools run; use absolute deadlines. Drain buffered native
output before declaring loss. Plugin replacement never rebinds an uncertain old call.
Persisted facts backpressure; ephemeral telemetry may drop with visible lag.


Native harness inputs use the shared 8 MiB
[`MAX_NATIVE_INBOUND_FRAME_BYTES`](../../crates/eliot-core/src/limits.rs) bound.
For NDJSON it includes the newline. Antigravity can discard an oversized ACP
`session/update` when a bounded header identifies the notification and the
newline ends an envelope with no top-level `id`. The reader counts the bytes
while discarding the record and queues one gap in wire order. The gap records
the method, size and limit and leaves the native prompt reply authoritative.
If an update was omitted during a turn, its final text is reported unavailable
with `native_update_gap`. Oversized replies, requests and unclassified records
are fatal. Discard ends in a fatal oversized-frame fault if the newline never
arrives before the discard deadline. `swarm why` shows the retained reason for
the Agent.

Native reader faults split by proven line boundary. Fatal reader end (EOF,
codec/UTF-8/IO fault, a required oversized record, exit-drain
deadline) atomically closes response-router registration and the write
boundary: new RPC/notifications are rejected `native_not_dispatched` before
any write, while requests already queued or written remain `unknown` — never
silently dropped, retried, or restarted automatically. A bounded UTF-8
NDJSON line whose JSON or envelope is invalid is record-local: the typed
`NativeStreamFault` label is recorded as a gap, current pending calls fail
`unknown`, and the same reader keeps routing later well-formed records.
The recorded fault is sticky: EOF, exit-drain, and stop handling never
overwrite it with a healthy-looking closure, so a busy consumer cannot miss
it. Frame/codec/UTF-8/IO errors never continue without a proven line boundary.
Antigravity's fenced load/prompt replies and record-local fault markers share
one ordered queue. Consume the retained pre-fault prefix before applying the
fault; frames after the marker cannot settle a turn. A full fault-marker queue
latches overflow and suppresses subsequent emitted frames. Fatal/overflow watch
faults apply after their retained prefix drains, preserving any committed terminal.
Owner drop/cancel fences writer and response router before aborting either
task (no await in `Drop`, no lock held over IO): already-written calls
resolve `unknown`, fresh writes are refused `native_not_dispatched`.

Add hooks only for a missing native fact/decision. Config is session-scoped; decisions
bind exact generation/request/deadline. Timeout uses the safe native default; late
approval is discarded, not queued. Hooks do not duplicate the authoritative stream.

Claude Code delivers `turn_safe_point` context and exact-turn `steer` commands through
`PreToolUse.additionalContext` and `PostToolUse.additionalContext`. Store frames each
claimed item using the shared Swarm note format (preserving Owner and manager sender
identity) and supplies it to the soonest `before_tool` or `after_tool` `hook.event`
callback for that exact turn. The adapter writes the supplied text on the selected
callback and reports `claude.PreToolUse.additionalContext` or
`claude.PostToolUse.additionalContext` on the committed `hook.before_tool` /
`hook.after_tool` receipt. Existing approval callbacks retain permission decisions.

Both Claude tool hooks have a three-second native timeout and matching adapter deadlines.
Empty responses contribute to `empty_after_tool_hooks` on the turn terminal.
Only a context delivery emits a `hook.after_tool` receipt. A successful file write
with an empty selection emits a `tool.file.edited` fact. The `edit_path` on both
events carries a verified worktree-relative path or an outside-or-unverifiable
SHA-256 and redacted basename. A native `compact_boundary` record yields
`session.context.compacted` and includes `turn_id` only for a committed active turn.
For [#881 item 2](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/881),
moving pending inserts to the next input at turn end substitutes for the Stop hook.

The OpenCode `context` hook (`before_model`) is the second consumer. The Rust host binds
the native session over RPC `eliot-context` version 1 (`bind`/`next`/`resolve` on the
existing authenticated HTTP transport) before readiness and advertises `turn_safe_point`
only after a verified v1 binding. An owned longpoll claims through the same authenticated
`hook.event` channel before the vendor `resolve` mutates the native system draft. A
confirmed `applied` or `empty` emits a receipt. OpenCode rebuilds that draft between
tool calls. The plugin keeps the applied parts with the same exact native input so
later drafts include them. The next input gets its own context.

The Antigravity `PreInvocation` hook (`before_model`) is the third consumer. The adapter
stages a private generation-scoped hook group in a dedicated hook-only directory's
`.agents/hooks.json` (the only `--add-dir` native root; the private instance directory
is never exposed) invoking an authenticated helper command. When the vendor runner executes the hook, the helper connects to the session-owned
loopback listener and proves its kernel birth identity. The plugin claims FIFO `turn_safe_point`
context through the existing authenticated `hook.event` channel, combines it with any pending
exact-turn steer retained inside the plugin, and packages both into one `HookOutput` with native
`injectSteps` entries using nested `systemMessage.systemMessage`. Only confirmed helper stdout
write acknowledgment commits
`hook.before_model` with `ContextOutcome` (mechanism `antigravity.PreInvocation.injectSteps.systemMessage`).
A retained exact-turn steer waits for this callback while other control requests remain serviceable;
this is native `PreInvocation`, not an `active_turn` inject or a new SEND. See [Antigravity harness](../harnesses/antigravity.md)
for native details.


Native adapters echo turns and emit request callbacks over this same transport
through their real implementations. Add crash/child/recovery steps with their real caller. Record/replay and common
conformance are completed in L7, not a separate framework blocking L2. CI never calls a
model and never runs synthetic/fake executions.
## Agent skill delivery

The repository owns one Agent Skills source, `skills/eliot-swarm/`
(`SKILL.md` plus at most `reference.md`), embedded in the binaries with
`include_str!` ([`eliot_plugin::skill`](../../crates/eliot-plugin/src/skill.rs)).
The daemon materializes it under the data directory at startup
(`<data-dir>/skills/`), with a private ACL and atomic replace; the layout
doubles as a Claude Code plugin directory
([`eliot_daemon::skill`](../../crates/eliot-daemon/src/skill.rs)).
Each harness plugin exposes the materialized copy to its Agent sessions
through that harness's own per-session mechanism ? per-session flags or env
only, never the user checkout, never global directories. The per-harness
mechanism is documented in each [harness page](../harnesses/README.md):
Codex `skills/extraRoots/set`, Claude Code `--plugin-dir`, OpenCode
per-Agent global skills, Antigravity staged `.agents/skills`, and the Muse
first-turn fallback below.

Muse `serve` loads the session workspace's skills and rules. Its plugin
prepends a short pointer to the materialized skill on the first accepted
turn of the native session. The Agent reads the file when it needs Swarm
coordination. A missing or unreadable canonical copy fails session start.

Skill content points at the live surface (`api.methods` / `api.help`,
`swarm <cmd> --help`) and never copies field catalogs. `swarm identity`
(`api.identity`) answers "which one am I", and `swarm who` marks the
caller's own row (`is_self`), so an Agent never has to guess its identity.
