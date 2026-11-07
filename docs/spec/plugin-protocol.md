# Plugin protocol

A plugin translates one native harness, normalizes its facts and supplies native
readback. It never writes SQLite or repeats an uncertain native effect. Host and plugin
share wire types, not vendor SDK types or the daemon's implementation.

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

Maximum encoded frame is 1 MiB including the NDJSON newline. Inline input is at most
512 KiB; persisted event data/final text 64 KiB; pages at most 1,000 entries and below
900 KiB. Both semantic and encoded limits apply, including JSON escaping. Reject
oversize input before native I/O.
Continuously drain cancel-safe readers; never cancel a partially read line. Credentials
are excluded from logs, recordings and plugin state.

## Manifest

An initial plugin may declare:

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "installed version",
  "available": true,
  "mode": "per_agent",
  "verbs": ["launch", "send", "stop"],
  "child_verbs": [],
  "features": {"start_requires_input": false, "native_children": false},
  "sources": {"controlled": {"session": "stream", "turn": "stream", "message": "stream"}},
  "proxy": "none"
}
```

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
| `agent.start` | agent/generation, profile, workspace, target environment, note, source plan/state, optional resume subject/linked initial input | establish session; input/result settle separately |
| `agent.command` | agent/generation, command id, verb/args, exact turn/request where required, optional dispatch_seq | applied/rejected/failed/unknown, native mechanism and application point |
| `agent.stop` | agent/generation, command id, force | success means the bound controlled session/generation ended, not merely that observation detached |
| `agent.snapshot` | controlled command/turn binding, or exact subject plus `observation` page query | bounded readback; private history never acknowledges controlled effects |
| `agent.attach` | exact subject, generation, observe/control, profile/environment, source plan/cursors | attach only to that subject; otherwise not found/unsupported |
| `agent.detach` | exact observed subject/generation and command id | close only its reader; never end the native session |
| `discover` | frozen profile, target environment without Agent identity, optional exact scope/cursor, `archived` (default `false`), bounded limit | read-only native-subject metadata page |
| `profile.store` (L3) | shared [`ProfileStoreParams`](../../crates/eliot-plugin/src/protocol.rs) | Return the effective [`NativeStoreBinding`](../../crates/eliot-core/src/native_store.rs) using the caller's actual prepared workspace and a bounded, owned metadata reader; success requires confirmed reader cleanup. It creates no model or thread, performs no native control and publishes no Agent events. This is outside the L1/L2 first-harness subset. |
| `shutdown` | none | close owned paths, drain, exit; never kill an adopted service |

Steer uses an ordered, bounded write followed by a correlated response wait: the host
writes its `agent.command` frame in dispatch order under the writer deadline, then keeps
the exact response pending without a native/model-turn response deadline while continuing
to process `interrupt`, `stop`, `agent.snapshot`, and lifecycle messages. The response,
or transport/generation teardown, settles the wait; unresolved teardown remains `unknown`
and is never replayed. Other RPC deadlines are unchanged.

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

`eliot_plugin::AgentCommandParams` carries optional `dispatch_seq` (positive integer).
When present, it binds the exact committed `command.dispatched` Event sequence in
Store for that command execution. Missing or null decodes as `None` for compatibility;
existing adapters ignore it. The host checks the complete encoded `agent.command` request,
including its actual id, arguments, optional sequence and NDJSON newline, against the 1 MiB
frame bound before enqueueing. Retained command retry retains the original committed sequence
rather than allocating a fresh sequence; initial send linked to launch/resume follows
ordinary send dispatch.

For SEND, the same `agent.command` effect carries the FIFO `next_input` items claimed
from existing command rows. The adapter prepends those items inside that native send;
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

For resumed `session.ready`, Store requires `data.native_scope` and `data.native_ref` to
match the retained subject pair exactly before reducing the event. `data.command_id` may be
absent or null; when supplied, it must match the unique Resume command for that target
generation. The plugin need not add command correlation when its readiness observation does
not carry it.

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
`before_tool` (Claude Code `PreToolUse`) or `before_model` (OpenCode `context` hook and
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

After the native write is confirmed, the plugin publishes a `hook.before_tool` /
`hook.before_model` fact on the existing event path with the exact turn and consumer plus
one shared `ContextOutcome` for that callback. Store settles its fixed FIFO claims in the
same source-batch transaction. A failed or uncertain native write emits no applied fact;
the claim stays frozen and is never requeued. A confirmed receipt may reconcile a claimed
`unknown` in the same generation against its captured turn record even when its native
terminal won the race; selection always requires the exact active turn. This adds no second
source/lifecycle stream, client API method, hook-decision RPC, or policy engine.

## Durable plugin requests

| Method | Fields / result |
| --- | --- |
| `events.put` | stable batch id, agent/generation/source, complete bounded normalized native-record batch, optional exclusive cursor; returns committed seq range/cursor |
| `source.state` | private readback of source state, derived `observer_ready`, all current family owners, latest gap, batch acknowledgement, and latest handoff digest/ack |
| `source.health` | `SourceHealthUpdate {agent_id,generation,source,health}`; returns the same four-field update after commit |
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

Same id with another semantic payload/cursor, or mixed new/duplicate events, is
`source_integrity`. After a lost reply, read `source.state`; retry only that exact last
batch. This retry is Store admission, never a native command retry. No adapter journal
or unbounded seen-event store is required by this contract.

A replayable source resumes strictly after its opaque committed cursor. A source with
no exclusive cursor reconnects at the live edge, records a gap and reads current native
state; it does not republish history. With neither stable record id nor cursor, a lost
acknowledgement is a gap/reconciliation case, not a speculative batch retry.

Plugins publish only assigned native families. Agent/command/rule/mail/Git/process
lifecycle, `session.lost` and owner resolutions are daemon facts, not arbitrary plugin
notifications.

### Gap and handoff

A source row's `open|closed` lifecycle, health and latest historical gap are independent.
A gap neither closes the source nor changes its owner or health automatically.
`source.health` changes only health and `updated_at` on an already-open current-generation
native source; an unchanged value is a no-op with no event. It does not clear a gap,
move a cursor, change ownership or reopen a closed row. A controlled caller binds
`agent_id`/`generation` to its source credential; the observe-only caller keeps its
settled exact-subject proof. The controlled path rejects reserved `daemon`, `os`,
`git` and `workspace_process` sources, stale generations, closed rows, and
observer/child escalation. The committed health event and
`source.state` readback carry lifecycle `state:"open"`; the four-field ACK does not.
The event is published before the response. After an uncertain reply, settle through
`source.state`, matching agent/generation/source and verifying open state plus requested
health before proceeding; otherwise the outcome remains unknown. Do not infer health
from a successful private-history read. Report `ok` only after verified live-edge
recovery; keep the historical gap visible.

The typed handoff contract is:

```text
SourceHandoff {
  agent_id:String, generation:i64, source:String, families:Vec<String>,
  successor:String, mechanism:String, cursor:Option<String>, gap:Option<SourceHandoffGap>
}
SourceHandoffGap { gap_id:String, reason:String, from:Option<String>, to:Option<String> }
SourceHandoffAck {
  agent_id:String, generation:i64, source:String, families:Vec<String>,
  successor:String, mechanism:String, cursor:Option<String>, gap_id:Option<String>, seq:i64
}
```

`seq` is positive and is the committed `source.handoff` event sequence; it is independent
of the predecessor's `last_store_seq`. For a gap-backed handoff, `0 < gap_seq < ack.seq`.

`source` is the expected predecessor. `families` is a unique nonempty set; order is not
significant, and Store sorts it before hashing the canonical operation and forming the
ACK. Store rechecks the selected harness subset: Codex supports `session`,
`turn`, `message`, `tool` and `request`; each other native adapter advertises
only the families its implemented handlers support. Handoff transfer for
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

`gap_id` identifies one uncovered interval. Same id and canonical fields return its
stored result; the same id with different fields is an integrity error. After that
operation is acknowledged/read back, a new interval uses a new stable id and replaces
only the latest gap metadata; the previous gap event remains in retained history.

At most one unacknowledged gap/handoff operation is allowed per source. Settle a lost
reply with `source.state` before issuing the next operation.

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
connected caller is a native adapter source; it verifies the handoff through
`source.state` before switching to successor facts. Private history cannot be handed
off or published by this caller. The separate Codex observe-only live grant is limited
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
request, including the redacted reason. Each caller prepares before sending, freezes that
value, and reuses it for `source.state` proof. Host uses the same prepare path.
Callers must not assume Host-only secret
knowledge: if normalized request/digest agreement cannot be proven, the result is
`unknown`. After a lost reply, accept only a matching digest/ACK, current successor
owners and the same cursor or gap boundary. If any owner or boundary advanced, or the
state is mixed/stale, the result is unknown; do not blindly retry. Retain only the latest
handoff proof on the predecessor row. The public Agent `source_states` projection remains
unchanged and never exposes that digest or ACK. No source epoch or open-id layer.

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
associated with its exact turn; it is not the turn's terminal result. Final text precedes
the terminal turn; process exit or silence is not a turn result. Cumulative usage replaces
its prior scope value rather than being summed. OpenCode attributes its final assistant
message's native token usage to the bound turn's terminal event at message scope; a
repeated message id replaces, and the value is never presented as a turn or session total.

On a failed terminal event, preserve its exact command/turn binding, status and structured
provider restriction before optional final text. The following final-text budget rule
applies to every failed terminal event, whether or not it has a structured provider
restriction. If final text exceeds the event budget, omit it rather than publish a
truncated string as complete; add
`final_text_unavailable:"event_budget"` when that marker fits. The full raw text remains
only in private native history. A Core `Turn.final_text` of `None` means public final
text is unavailable, not empty or successful.
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

Public requests and answers are universal: `request.opened` details and kinds use the
shared [`eliot_core::requests`](../../crates/eliot-core/src/requests.rs) shapes
(`QuestionRequest`/`ApprovalRequest`), and `answer` args use `AnswerArgs`. Only the
plugin translates between these shapes and its vendor format in both directions.
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


Native reader faults split by proven line boundary. Fatal reader end (EOF,
codec/UTF-8/IO fault, oversize without a proven line boundary, exit-drain
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
Owner drop/cancel fences writer and response router before aborting either
task (no await in `Drop`, no lock held over IO): already-written calls
resolve `unknown`, fresh writes are refused `native_not_dispatched`.

Add hooks only for a missing native fact/decision. Config is session-scoped; decisions
bind exact generation/request/deadline. Timeout uses the safe native default; late
approval is discarded, not queued. Hooks do not duplicate the authoritative stream.

The first system-context hook consumer is Claude Code 2.1.278:
`PreToolUse.additionalContext` is the native `turn_safe_point` application point. The
adapter reports the exact turn and opaque consumer through the existing authenticated
plugin channel. Timeout/cancellation supplies no context, does not override a native
result, and discards late context instead of queuing it for a later turn. This context
path does not make permission decisions; existing approval callbacks retain that
responsibility. Hooks do not duplicate lifecycle already supplied by the stream.

The OpenCode `context` hook (`before_model`) is the second consumer. The Rust host binds
the native session over RPC `eliot-context` version 1 (`bind`/`next`/`resolve` on the
existing authenticated HTTP transport) before readiness and advertises `turn_safe_point`
only after a verified v1 binding. An owned longpoll claims through the same authenticated
`hook.event` channel before the vendor `resolve` mutates the native system draft; only a
confirmed `applied`/`empty` may emit a receipt.

The Antigravity `PreInvocation` hook (`before_model`) is the third consumer. The adapter
installs a private generation-scoped hook group in `.agents/hooks.json` invoking an authenticated
helper command. When the vendor runner executes the hook, the helper connects to the session-owned
loopback listener and proves its kernel birth identity. The plugin claims FIFO `turn_safe_point`
context through the existing authenticated `hook.event` channel, combines it with any pending
exact-turn steer retained inside the plugin, and packages both into one `HookOutput` with native
`injectSteps` (`ephemeralMessage`). Only confirmed helper stdout write acknowledgment commits
`hook.before_model` with `ContextOutcome` (mechanism `antigravity.PreInvocation.injectSteps.ephemeralMessage`).
A retained exact-turn steer waits for this callback while other control requests remain serviceable;
this is native `PreInvocation`, not an `active_turn` inject or a new SEND. See [Antigravity harness](../harnesses/antigravity.md)
for native details.


Native adapters echo turns and emit request callbacks over this same transport
through their real implementations. Add crash/child/recovery steps with their real caller. Record/replay and common
conformance are completed in L7, not a separate framework blocking L2. CI never calls a
model and never runs synthetic/fake executions.
