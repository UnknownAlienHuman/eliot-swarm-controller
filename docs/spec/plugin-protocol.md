# Plugin protocol

A plugin translates one native harness and normalizes facts. It never writes SQLite or
retries an uncertain effect.

## Modes

- `per_agent`: one plugin instance/custody unit per Eliot generation.
- `shared`: one plugin talks to a native service with exact per-session identity,
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
| `agent.attach` | exact subject/generation/control mode → exact state/turn/requests/pids/effective or not-found |
| `agent.command` | command id, exact generation/turn/request, verb/args → applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | exact generation + command ids → current state/readback |
| `agent.stop` | exact generation/command/force → success only after that generation ended |
| `discover` | canonical native subjects |
| `shutdown` | finish owned cleanup and exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }
start result = {
  native_scope, native_ref, pids, effective,
  initial_status?: applied | rejected | failed | unknown
}
```

Most harnesses start without initial input. A plugin requiring input to obtain a native
subject declares `start_requires_input=true`; it receives the linked command in
`initial`. Session and initial command remain separate outcomes. `applied` means native
input admitted, not turn completed.

### Start contract

1. Establish exact native subject ↔ generation.
2. Emit one commit-acknowledged `session.ready` when control is possible.
3. If initial is required, admit it once and report its separate status/events.
4. Return matching start result.

Ready never proves initial input. Before ready commit, the plugin backpressures the
native reader or keeps a bounded spool. If the spool cannot retain all persisted events,
it reports a source gap, marks the initial effect unknown as needed, and still reports
session start according to actual native evidence; it never falsely turns the whole
session into a definite failure or drops a terminal event silently.

Launch and resume both use `agent.start`. Resume supplies the same canonical subject and
a new Eliot generation. Old input is never inferred/replayed. Exact recovery attach to
the same live incarnation preserves generation.

## Plugin → host requests/notifications

Persisted facts are requests, not fire-and-forget notifications:

| Method | Result |
| --- | --- |
| `event.put {agent,generation,kind,native_id,ts?,data}` | `{seq, duplicate}` only after Store commit/dedupe |
| `process.put {agent,generation,pid,start_time,role}` | committed identity/duplicate acknowledgement |
| `child {parent agent/generation, child scope/ref,...}` | canonical child row/generation/token |
| `log` notification | no durability claim; bounded diagnostic only |
| `event.ephemeral` notification | best-effort delta/sample only |

Every persisted plugin lifecycle event has stable per-event identity. Plugin advances
its source cursor/removes unacknowledged spool entry only after `event.put` reply. Lost
reply causes resend with the same id; Store uniqueness returns duplicate safely. If the
vendor cannot replay/backpressure and the bounded unacknowledged spool overflows, emit a
source gap and do not claim complete lifecycle coverage.

`child` upserts one canonical native subject. Parent generation is provenance; child
scope/ref defines identity.

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

Shared control requires session environment when native tools need Eliot identity and an
exact isolated stop/interrupt. `instances=N` is shared-only. Versions are facts, not
authority gates.

## Translation rules

1. Validate generation before native I/O.
2. Steer/interrupt bind exact active turn; request replies bind exact request.
3. Map one semantic verb to one documented native mechanism and application point.
4. Reject weaker substitutions/hidden route changes.
5. Unknown is never retried.
6. Preserve command/native turn/request/event correlation.
7. Daemon owns queued-send order/generation expiry.
8. Reject oversized/known-secret durable payloads before native I/O.

## Source ownership/normalization

One source owns each family per generation. Live stream normally owns controlled
lifecycle; transcript owns observe-only or explicit cursor recovery; hooks own assigned
missing facts/synchronous decisions only.

One turn start and one terminal completion per native turn. Intermediate status/process
exit/silence is not completion. Final output is committed before terminal completion or
the plugin delays terminal publication until authoritative output is drained. Unknown
vendor facts can be diagnostic but cannot mutate core state.

Reconnect resends only unacknowledged or native-cursor-new events. An unbridgeable
interval is a gap, never replayed completion.

## Hooks/transcripts

Hooks are session-scoped with current agent/generation/token and one absolute deadline.
Timeout returns native-safe allow for observation or ask for decisions. A synchronous
decision is returned only after its durable action/command admission commits; no Store
commit means ask. No durable hook queue.

Transcript readers persist cursors, wait only for an incomplete final line, and report
a complete unparsable line/gap. They never infer end from silence/truncation.

## Custody

- Windows exact: assign-before-resume Job, no breakaway, kill-on-close for owned
  sessions.
- Linux exact: delegated cgroup v2/equivalent plus owner-death guardian; membership and
  group kill/readback prove termination.
- Linux/other Unix fallback: process group + guardian for non-escaping members,
  reported degraded; exact force/daemon-death containment unsupported.

Guardian lives outside the owned group, survives daemon loss within the user session,
and disarms only after clean empty-group shutdown. Attached services are never killed;
managed services have separate custody.

When process exit races final output, drain authoritative source to EOF or bounded
terminal-drain deadline. Deadline creates a gap; lost requires exact empty custody or
session-not-found readback, never silence alone.

## Fake, recordings and L7

Fake is shared/isolated, generation-bound, stable-event-id and persists minimum exact
snapshot/resume state. Recordings are bounded sanitized native NDJSON with version/date.
CI never calls a model.

L7 conformance covers start vs initial outcome, commit-ack event replay/dedupe,
source-gap overflow, exact bindings, canonical children, no unknown retry,
exact/degraded custody, attach/readback, bounded buffers and unsupported-before-I/O.
