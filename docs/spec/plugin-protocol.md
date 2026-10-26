# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain effect.

## Modes

- `per_agent`: one plugin instance/custody unit per Eliot generation.
- `shared`: one plugin talks to a service with exact per-session identity,
  environment/tool attribution, attach/readback and isolated stop.

A service missing any shared-control prerequisite is per-agent or observe-only. Shared
mode is not selected merely because the vendor hosts multiple chats.

## Framing

JSON-RPC 2.0 over UTF-8 NDJSON, ≤1 MiB/line. stdout is protocol; stderr bounded logs.
Both directions issue requests. Every scoped message carries agent + generation.
Readers are continuously drained/cancel-safe. Protocol `eliot-plugin/1` allows additive
fields only.

## Host → plugin

| Method | Key fields/result |
| --- | --- |
| `initialize` | config/private dir/instance → manifest |
| `agent.start` | target generation, resolved profile/workspace/env/note, resume subject?, linked initial? → session result + separate initial status |
| `agent.attach` | exact subject/generation/control mode + host-known active turn? → exact native state, turn reconciliation, requests, pids/effective or not-found |
| `agent.command` | command id, exact generation/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation + command/turn ids → current state/readback |
| `agent.stop` | exact generation/command/force → success only after generation ended |
| `discover` | canonical native subjects |
| `shutdown` | finish owned cleanup and exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }
start result = {
  native_scope, native_ref, pids, effective,
  initial_status?: applied | rejected | failed | unknown,
  initial_turn_id?: string
}
attach/snapshot turn readback =
  { turn_id, state: running | completed | not_active_unknown,
    input_commands?: [id], status?, final_text?, usage? }
```

Most harnesses start without initial input. A plugin requiring input to obtain a native
subject declares `start_requires_input=true`; it receives linked command in `initial`.
Session and initial command remain separate. Initial `applied` requires exact native
turn/execution association, not merely a write/HTTP inbox acknowledgement.

### Start contract

1. Establish exact native subject ↔ generation.
2. Emit one commit-acknowledged ready when control is possible.
3. If initial is required, admit it once; hold its command reply until exact turn/
   execution association, or return unknown on deadline after possible admission.
4. Return matching session result and separate initial status/turn id.

Ready never proves initial input. Before ready commit, plugin backpressures native
reader or keeps bounded spool. If spool cannot retain all persisted events, report gap,
mark initial effect unknown as needed, and report session start from actual evidence;
never falsely fail whole session or silently drop terminal data.

Launch/resume both use `agent.start`. Resume supplies same canonical subject and a new
Eliot generation. Old input is never inferred/replayed. Exact recovery attach to same
live incarnation preserves generation.

### Send and async inboxes

For `send`, transport/inbox acceptance (for example HTTP 204) is an intermediate plugin
fact, not an Eliot applied reply. Plugin keeps `agent.command` pending until:

- an exact native turn/execution id is associated with that command → reply applied and
  emit/ack `turn.started` or `turn.inputs`; or
- exact readback proves no effect → failed; or
- absolute deadline expires after possible admission → unknown.

The daemon therefore does not dispatch another send/configuration/goal continuation
while input association is unresolved. Plugin must not return applied early merely to
free the lane. If native service legitimately associates several Eliot inputs with one
execution, emit one turn plus exact ordered `input_commands`; Store maps all waits to
that terminal turn.

### Attach and turn reconciliation

Host supplies known active turn. Plugin must not answer idle merely because an active
list is empty:

- exact same turn running → running;
- exact terminal history/result → completed;
- session exists, turn no longer active, no terminal proof → `not_active_unknown`;
- subject absent → not-found.

Unknown turn keeps live generation blocked from new turns until exact readback or owner
resolution. Plugin never invents completion or silently starts next input.

## Plugin → host requests/notifications

Persisted facts are commit-acknowledged requests:

| Method | Result |
| --- | --- |
| `event.put {agent,generation,kind,native_id,ts?,data}` | `{seq,duplicate}` after Store commit/dedupe |
| `process.put {agent,generation,pid,start_time,role}` | committed identity/duplicate ack; host adds boot id |
| `child {parent agent/generation, child scope/ref,...}` | canonical child row/generation/token |
| `log` notification | bounded diagnostic; no durability claim |
| `event.ephemeral` notification | best-effort delta/sample only |

Plugin advances source cursor/removes spool entry only after commit reply. Lost reply
resends same id; Store returns duplicate. If vendor cannot replay/backpressure and spool
overflows, emit source gap and do not claim complete coverage.

`child` upserts canonical native subject. Parent generation is provenance.

## Manifest

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "...",
  "available": true,
  "session_mode": "per_agent",
  "verbs": ["launch","send","steer","inject","interrupt","stop","resume"],
  "child_verbs": ["send","interrupt"],
  "features": {
    "start_requires_input": false,
    "native_goal": true,
    "native_children": true,
    "hooks": false,
    "transcripts": true,
    "discover": true,
    "cost": false,
    "session_environment": true,
    "isolated_force_stop": true
  },
  "proxy": "config"
}
```

Shared control requires session environment where native tools need Eliot identity and
exact isolated stop/interrupt. `instances=N` is shared-only. Versions are facts, not
authority gates.

## Translation rules

1. Validate generation before native I/O.
2. Steer/interrupt bind exact active turn; request replies exact request.
3. Map one semantic verb to one documented mechanism/application point.
4. Reject weaker substitutions/hidden route changes.
5. Unknown is never retried.
6. Preserve command/native input/turn/request/event correlation.
7. Daemon owns queued-send order/generation expiry.
8. Reject oversized/known-secret durable payloads before native I/O.

## Source ownership and normalization

One source owns each family per generation. Live stream normally owns controlled
lifecycle; transcript owns observe-only/explicit recovery; hooks own assigned missing
facts/synchronous decisions only.

One turn start and one terminal completion per native turn. `turn.started` identifies
all known input commands; later exact coalesced inputs use `turn.inputs`. Intermediate
status, inbox acceptance, process exit or silence is not completion. Final output commits
before terminal event or plugin delays terminal publication until output drains.

Reconnect resends only unacknowledged or native-cursor-new events. Unbridgeable interval
is gap, never replayed completion.

## Hooks and transcripts

Hooks are session-scoped with current identity and one absolute deadline. Timeout
returns native-safe allow for observation or ask for decisions. Synchronous decision
returns only after durable admission commits; no Store commit means ask. No durable
hook queue.

Transcript readers persist cursors, wait only for incomplete final line, and report a
complete unparsable line/gap. They never infer end from silence/truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent + owner-death guardian; membership/group
  kill/readback prove termination.
- Linux/other Unix fallback: process group + guardian for non-escaping members,
  degraded; exact force/daemon-death containment unsupported.

Guardian lives outside owned group and disarms only after clean empty-group shutdown.
Attached services never killed; managed services have separate custody.

When process exit races final output, drain authoritative source to EOF or bounded
terminal-drain deadline. Deadline creates gap; lost requires exact empty custody or
session-not-found readback, never silence.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound, stable-event-id and persists minimum exact
snapshot/resume state. Recordings are bounded sanitized native NDJSON. CI never calls a
model.

L7 conformance covers start vs initial, async inbox association/timeout, multiple input
commands per turn, commit-ack replay/dedupe, source-gap overflow, reconnect unknown
turn, exact bindings, canonical children, no unknown retry, custody, attach/readback,
bounded buffers and unsupported-before-I/O.
