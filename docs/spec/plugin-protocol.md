# Plugin protocol

A plugin translates Eliot semantic commands to one native harness and normalizes
facts. It never writes the Store or retries an uncertain effect.

## Modes

Manifest `session_mode`:

- `per_agent`: one plugin instance owns one Eliot agent generation and its native
  processes;
- `shared`: one plugin talks to a service that provides exact per-session identity,
  environment/tool attribution and stop/attach. One agent stop never kills the shared
  process/service.

A service lacking per-session environment/control may be observed, but cannot be a
shared **controlling** plugin. The plugin must use per-agent service mode or reject the
control attach.

## Framing

JSON-RPC 2.0 over UTF-8 NDJSON, maximum 1 MiB per line. stdout is protocol only;
stderr is bounded log text. Both directions may issue requests. Every agent-scoped
message carries `agent_id` and `generation`. Readers are cancel-safe and continuously
drained. Protocol version is `eliot-plugin/1` with additive fields only.

## Host → plugin

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | protocol, config, private data dir, instance id | manifest/capabilities |
| `agent.start` | agent id, target generation, resolved profile, workspace, env, system note, `resume_from?`, `initial?` | session result plus separate optional initial-command status |
| `agent.attach` | agent/generation, exact native subject, observe-only/control | exact state, active turn, pending requests, pids/effective or not-found |
| `agent.command` | agent/generation, command id, verb/args, expected turn/request | applied/rejected/failed/unknown + mechanism/application point |
| `agent.snapshot` | agent/generation, optional command ids | exact current state and command readback |
| `agent.stop` | agent/generation, command id, force | success only after that generation ended |
| `discover` | — | native subjects with stable scope/ref and metadata |
| `shutdown` | — | reply, finish owned cleanup, exit |

```text
resume_from = { native_scope, native_ref }
initial = { command_id, text }       // linked send, not part of launch identity
start result = {
  native_scope, native_ref, pids, effective,
  initial_status?: applied | failed | unknown
}
```

Most plugins start the native session without `initial`; the daemon later dispatches
the linked send normally. A plugin that requires first input to obtain a session id
sets `features.start_requires_input=true` and receives the linked send in `initial`.
The session and initial input still have separate Eliot commands/outcomes.

### Start contract

1. Establish the exact native subject ↔ target generation mapping.
2. Emit exactly one matching `session.ready` once the session accepts control.
3. If `initial` exists, admit it exactly once or return `failed/unknown` for that
   command; emit its normal turn/input events with the linked command id.
4. Return the session result and separate initial status.

`session.ready` proves session start only. It never proves initial prompt admission.
The host does not dispatch later commands until ready is committed and the reply agrees.
Native events arriving earlier are kept in a bounded pre-ready buffer. Overflow fails
the source/start explicitly; it never grows without limit or drops a terminal event.

Launch and resume both use `agent.start`; resume supplies `resume_from` and a new Eliot
generation. Old input is never inferred or resent. Exact re-attach to the same live
incarnation preserves generation.

## Plugin → host

| Method | Params |
| --- | --- |
| `event` | agent, generation, kind, timestamp?, unique native event id?, data |
| `process` | agent, generation, pid, native start identity, role |
| `log` | level/message |
| `child` request | parent agent/generation, child `native_scope`, child `native_ref`, optional name/labels |

`child` upserts the canonical native subject. Parent generation is provenance; host
returns the existing/new child row and token where independently injectable.

Stale generation, non-owner source, duplicate `(kind,native_id)`, wrong turn and wrong
request are rejected before the normal event bus. Plugins must provide a true event id
where available, not reuse a turn/item id for multiple updates of the same kind.

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

Shared control requires `session_environment` when native tools need Eliot token/Git
environment, and an exact session stop/interrupt path. `instances=N` applies only to
shared plugins. Versions are display/fixture facts, not gates.

## Translation rules

1. Validate generation before native I/O.
2. Steer/interrupt bind exact active turn; approval/answer bind exact request.
3. Translate one semantic verb to one documented mechanism and report
   `now|next_tool|next_turn|next_session` where relevant.
4. Reject unsupported semantics; no steer→send, resume→fresh unrelated session,
   answer→default choice, or force→shared-service kill.
5. Unknown is never retried.
6. Preserve native event/request/turn ids and command correlation.
7. Daemon owns send FIFO and generation expiry.
8. Reject oversized or known-secret durable payloads before native I/O.

## Event normalization and source ownership

One authoritative source owns each normalized family for one generation. Controlled
live stream normally owns lifecycle; transcript owns observe-only/recovery after an
explicit cursor handoff; hooks own only assigned missing facts or synchronous decisions.

Emit one start and one terminal event per native turn. Intermediate status is not
completion. Process exit alone is not completion. Final output is ordered before the
terminal event or the plugin delays completion until its output stream is drained.
Unknown vendor events may be `x.<harness>.*` but never mutate core state.

Reconnect suppresses already committed native event ids. If no lossless cursor boundary
exists, report a source gap; never replay an old completion.

## Hooks and transcripts

Hooks are session-scoped and receive current agent id/generation/token. The shim has a
single absolute deadline; timeout returns a configured native-safe default (`allow`
for observation, `ask` for decisions). There is no durable hook queue.

Transcript readers store bounded cursors, wait only for an incomplete final line, and
report complete unparsable lines as gaps. They never infer end from silence/truncation.

## Custody

Custody capability is explicit:

- Windows exact backend: Job object, assign-before-resume, no breakaway, kill-on-close
  for daemon-owned sessions.
- Linux exact backend: delegated cgroup v2 (or equivalent qualified containment) plus
  an owner-death guardian. `cgroup.kill`/membership proves group termination.
- Linux/other Unix fallback: process group + guardian for non-escaping members. It is
  reported `degraded`; exact `stop --force` and kill-on-daemon-death guarantees are
  unsupported until an exact backend is available.

The internal guardian is outside the plugin's native group, monitors daemon death, and
kills/verifies the exact owned group on abnormal owner loss. Clean shutdown disarms it
only after the group is empty. This closes the gap that a Unix process group alone does
not die when its parent daemon crashes, while still refusing to pretend an escaped
`setsid` child is contained.

Shared services have separate ownership: attached services are never killed; managed
services are stopped only through service lifecycle, not an agent stop.

A process exit and final stdout/terminal event may race. The host drains the authoritative
source to EOF or a bounded terminal-drain deadline before deciding lost. Deadline expiry
creates a source gap plus `session.lost` only after exact custody/session readback; no
fixed silence delay is terminal evidence.

## Fake plugin and recordings

Fake is shared, generation-bound and persists the minimum state needed for exact
snapshot/resume tests. It emits stable per-step event ids and rejects stale
turn/request/generation before effects. Scripts cover say/sleep/tool/request/child/
usage/crash/hang/exit.

Recordings are sanitized bounded NDJSON native traffic with harness version/date and
scenario. Replay checks stable method/path/payload fields; hang/exit controls simulate
failure. CI never calls a model.

## L7 conformance

The complete kit verifies:

1. manifest/mode/capability honesty;
2. one ready-before-start-success and separate initial-input outcome;
3. exact generation/turn/request fences;
4. no duplicate lifecycle on replay/handoff;
5. no unknown retry;
6. canonical child/native subject upsert;
7. exact/degraded custody reporting and no cross-agent kill;
8. attach/readback or explicit not-found/gap;
9. bounded pre-ready/output/telemetry behavior;
10. unsupported verbs rejected before native I/O.
