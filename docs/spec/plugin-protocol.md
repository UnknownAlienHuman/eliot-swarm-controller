# Plugin protocol

A plugin translates one native harness, normalizes its facts and supplies native
readback. It never writes SQLite or repeats an uncertain native effect. Host and plugin
share wire types, not vendor SDK types or the daemon's implementation.

## First harness subset

L1/L2 implement `initialize`, `agent.start`, `agent.command`, `agent.stop`,
`agent.snapshot`, `shutdown` and durable `events.put` for one per-Agent source. Start
with the actual fake/Codex verbs. The manifest advertises only implemented capabilities.

The first L3 process caller adds private `process.put` for the Codex app-server child.
It registers a process with the host monitor, without adding a native stream family.

Add discovery, control takeover, transcript handoff, shared services, children and hooks
with their first roadmap caller. A missing feature returns `unsupported`; do not create
empty handlers, tables or generic frameworks to anticipate it.

## Process modes

`per_agent` is one plugin/native process tree per controlled Agent generation. `shared`
is for a service with demonstrated exact session attribution/environment/readback and
isolated stop. Hosting multiple chats alone is insufficient. Adopted services are never
killed for an Agent. Shared support is not required for the first Codex path.

## Framing and bounds

JSON-RPC 2.0 over UTF-8 NDJSON, stdout protocol only, bounded stderr. Both directions may
send requests with direction-local ids. Agent messages carry `agent_id` and `generation`.
Wire version is `eliot-plugin/1`. Add fields compatibly; do not change existing meanings.

Maximum encoded frame is 1 MiB. Inline input is at most 512 KiB; persisted event data/final
text 64 KiB; pages at most 1,000 entries and below 900 KiB. Both semantic and encoded
limits apply, including JSON escaping. Reject oversize input before native I/O.
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

Extend verbs/families/features only when their paths exist. Version is diagnostic;
methods/fields/capabilities establish compatibility. The host selects one owner per
normalized native family. Hooks never mirror lifecycle already supplied by the stream.

## Host requests

| Method | Essential fields | Meaning |
| --- | --- | --- |
| `initialize` | protocol, frozen plugin config, private instance directory | return actual manifest |
| `agent.start` | agent/generation, profile, workspace, target environment, note, source plan/state, optional resume subject/linked initial input | establish session; input/result settle separately |
| `agent.command` | agent/generation, command id, verb/args, exact turn/request where required | applied/rejected/failed/unknown, native mechanism and application point |
| `agent.stop` | agent/generation, command id, force | success means the bound controlled session/generation ended, not merely that observation detached |
| `agent.snapshot` | controlled command/turn binding, or exact subject plus `observation` page query | bounded readback; private history never acknowledges controlled effects |
| `agent.attach` | exact subject, generation, observe/control, profile/environment, source plan/cursors | attach only to that subject; otherwise not found/unsupported |
| `agent.detach` | exact observed subject/generation and command id | close only its reader; never end the native session |
| `discover` | frozen profile, target environment without Agent identity, optional exact scope/cursor, bounded limit | read-only native-subject metadata page |
| `shutdown` | none | close owned paths, drain, exit; never kill an adopted service |

`resume_from={native_scope,native_ref}` is an `agent.start` option, not replay of the
old input. `initial={command_id,text}` is supplied only for a harness that requires
input to create a session. Explicit new text remains a separate linked command.

`features.discover` defaults to false. A discovery instance initializes independently
of an Agent and has no authority to call `events.put`, `process.put` or other durable
Agent requests. Its result is `{harness,native_scope,subjects,next_cursor,coverage}`
using the bounded metadata shape in [API](api.md#discover-attach-detach-and-resume).
It owns only its reader process tree. Profile/scope/filter cursor mismatches reject;
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

## Start ordering

Arm the reader/subscription before native create/resume/input. For a session-first
harness: create/resume without input → establish subject/effective settings → commit
`session.ready` → return start success → host sends the linked input normally.

For an input-created session, perform start+linked input once but report session,
input-to-turn association and terminal result independently. Ready alone never proves
input delivery. Buffer early facts within a bound; overflow records a gap rather than
inventing completion or issuing another start.

## Durable plugin requests

| Method | Fields / result |
| --- | --- |
| `events.put` | stable batch id, agent/generation/source, complete bounded normalized native-record batch, optional exclusive cursor; returns committed seq range/cursor |
| `source.state` | read-only family owners, cursor, lifecycle, health, latest gap and last-batch acknowledgement |
| `source.gap` | stable gap id, owned families, reason and optional from/to cursor; commits one gap boundary |
| `source.handoff` | named families, expected predecessor, successor/mechanism and final cursor or gap; atomic ownership transfer |
| `process.put` | credential, agent_id, generation, pid, role; returns the OS-verified `ProcessRecord`, not plugin-authoritative process lifecycle |
| `child` | registration id, parent/generation, exact child subject and optional identity request; canonical child row/source plan |

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

`gap_id` identifies one uncovered interval. Same id and canonical fields return its
stored result; the same id with different fields is an integrity error. After that
operation is acknowledged/read back, a new interval uses a new stable id and replaces
only the latest gap metadata; the previous gap event remains in retained history.

At most one unacknowledged gap/handoff operation is allowed per source. Settle a lost
reply with `source.state` before issuing the next operation.

Handoff checks that all named families have the expected predecessor, records the
final exclusive cursor or gap, and transfers only those families to an open successor
in one transaction. The predecessor may remain open for other families or as a private
standby. Its lifecycle changes only on its own close/drain facts. Successor publication
starts after commit. Readback of owner plus supplied boundary settles a lost reply;
unproven/mixed state remains unknown. No source epoch, open-id or source sequence layer.

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
completion inferred from silence. Final text precedes terminal turn; process exit or silence is not a turn
result. Cumulative usage replaces its prior scope value rather than being summed.
Requested route/settings are not proof of effective values; unavailable readback is
reported as such. The host owns queued-send FIFO and unknown gates.

`failed` means a known unsuccessful completed operation, which may have partial side
effects; it does not mean rollback. `rejected` proves no native I/O. Missing authoritative
outcome is `unknown`. None authorizes automatic re-execution of an external effect.

## Lifecycle, hooks and qualification

Contain owned children before tools run; use absolute deadlines. Drain buffered native
output before declaring loss. Plugin replacement never rebinds an uncertain old call.
Persisted facts backpressure; ephemeral telemetry may drop with visible lag.

Add hooks only for a missing native fact/decision. Config is session-scoped; decisions
bind exact generation/request/deadline. Timeout uses the safe native default; late
approval is discarded, not queued. Hooks do not duplicate the authoritative stream.

The initial fake echoes turns over this same transport. Add request/crash/child/recovery
steps with their real caller. Record/replay and common conformance are completed in L7,
not a separate framework blocking L2. CI never calls a model.
