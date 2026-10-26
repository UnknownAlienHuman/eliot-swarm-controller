# Plugin protocol

A plugin translates Eliot semantic commands to one harness and normalizes native
facts. It is a process speaking this protocol on stdin/stdout.

- Built-in plugins are Rust crates invoked as `eliot plugin <name>`.
- External plugins may use any language and are started from plugin config.
- Manifest `session_mode` is:
  - `per_agent`: one plugin process owns one Eliot agent generation and every native
    child process it launches; default for stdio/CLI/SDK harnesses;
  - `shared`: one plugin process talks to a native service that already isolates
    sessions, or simulates isolated sessions. One agent stop never kills the shared
    plugin/service. Fake and an attached OpenCode server are examples.

Many small per-agent controller processes are acceptable. Exact custody is more
important than saving a few MiB.

## Framing

- JSON-RPC 2.0, one UTF-8 JSON object per line, no BOM, maximum 1 MiB/line.
- stdout is protocol only; stderr is bounded log text.
- Both directions may issue requests/notifications; ids are unique per direction.
- Every agent-scoped message carries `agent_id` and `generation`.
- A long-lived reader has its own task. Do not cancel a partial line read in a select.
- Protocol version is `eliot-plugin/1`; fields may be added compatibly, meaning may not
  be changed.

## Host → plugin requests

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | `protocol`, plugin config, data dir, instance id | `Manifest` |
| `agent.start` | `agent_id`, `generation`, resolved profile, workspace?, prompt?, `resume_from?`, env, system note, parent native ref? | native scope/ref, pids, effective after generation is ready |
| `agent.attach` | agent/generation, native scope/ref, observe-only | exact still-live state/turn/pending/pids/effective or `not_found` |
| `agent.command` | agent/generation, command id, verb, args, expected turn?, request id? | `applied|rejected|failed|unknown`, application point/detail |
| `agent.snapshot` | agent/generation, optional command ids | state, active turn, pending, goal, usage, exact command readback |
| `agent.stop` | agent/generation, command id, force | success only after that generation ended |
| `discover` | — | native sessions with scope/ref/workspace/time/title/live |
| `shutdown` | — | reply then exit |

```text
resume_from = { native_scope, native_ref, prompt? }
Pid = { pid, start_time, role: harness|server|tool|proxy }
```

`resume` is a semantic core command handled by the daemon as a new-generation
`agent.start` with `resume_from`. It is **not** sent through `agent.command`. The
manifest still declares `resume` to say the plugin supports this start mode. The old
prompt is never inferred or resent; only an explicit new `prompt` field may start a
new turn after native resume.

### Start contract

Launch and resume share one contract:

1. establish the exact native session ↔ Eliot generation mapping;
2. emit exactly one `session.ready` with matching native scope/ref/effective settings;
3. return the same mapping.

The host does not dispatch another command until the ready event is committed and the
reply agrees. Earlier native events are buffered for that connection and released
after ready commits.

If ready committed but the reply was lost, it proves that exact start applied. If
native I/O may have occurred but neither ready nor negative exact readback exists, the
launch/resume command is unknown and is never repeated. A definite pre-I/O failure is
failed/rejected as appropriate.

Re-attaching the same still-live incarnation after host/plugin recovery preserves
Eliot generation. `resume_from` after terminal/lost state creates a new generation and
rotates token even when native ref is reused.

## Plugin → host notifications and requests

| Method | Params |
| --- | --- |
| `event` | agent, generation, kind, optional timestamp/native id, data |
| `process` | agent, generation, pid, start time, role |
| `log` | level/message |
| `child` (request) | parent agent/generation, native ref, optional name/labels |

Host rejects stale-generation notification before the normal bus. Plugin suppresses
native reconnect/replay duplicates and includes stable native ids where available.

`child` is idempotent for `(parent agent,parent generation,native ref)`. Host returns
child id/generation/token. If native children cannot receive separate environment,
their Eliot CLI calls are attributed to parent and `show` states that limitation.

## Manifest

```json
{
  "name": "codex",
  "version": "0.1.0",
  "protocol": "eliot-plugin/1",
  "harness_version": "codex-cli 0.164.0",
  "available": true,
  "session_mode": "per_agent",
  "verbs": ["launch", "send", "steer", "inject", "interrupt", "stop", "resume",
            "approve", "deny", "answer", "configure",
            "goal.set", "goal.pause", "goal.resume", "goal.clear"],
  "child_verbs": ["send", "interrupt"],
  "features": {
    "native_goal": true,
    "native_children": true,
    "hooks": false,
    "transcripts": true,
    "discover": true,
    "cost": false
  },
  "proxy": "config"
}
```

`available` means the native route can currently start. `proxy` is `config|env|none`.
Versions are display/fixture facts, not gates. Shared plugins may declare native
`max_sessions`; `instances=N` applies only to shared plugins. Each live per-agent
session gets its own instance/state directory.

## Binding and translation

1. Validate generation before native I/O; stale returns rejected.
2. Steer/interrupt require exact expected active turn. Approval/deny/answer require
   exact pending request. Never redirect to whatever is current.
3. Translate one verb to one documented native mechanism and report it plus
   application point (`now|next_tool|next_turn|next_session`).
4. Unsupported semantics are rejected. Never silently map steer→send,
   resume→fresh launch, answer→default option or force-stop→shared service kill.
5. Unknown means native effect may have happened. Plugin never retries it, including
   interrupt/stop/start.
6. Preserve native request/item/turn ids. No newest/list-position identity.
7. Daemon owns queued-send FIFO. Plugin receives a send only when its bound generation
   is idle.
8. A payload over its semantic/frame limit fails before native I/O.

## Event normalization

- Start produces one `session.ready`; normal end `session.ended`; disappearance of
  exact owned process/session `session.lost`; fatal live protocol/session error
  `session.error`.
- Emit one `turn.started` and one terminal `turn.completed` per native turn id.
  Terminal status is completed/interrupted/failed; intermediate native states are not
  completion.
- Final assistant text is bounded persisted `message.agent`; deltas are ephemeral and
  carry turn id. Full detail remains native.
- Tool/plan/file/request facts carry exact native turn/item/request ids.
- Usage states scope (turn/session); cumulative values replace, never sum.
- Provider errors distinguish rate limit, quota, model removed and auth when native
  evidence supports it.
- Unknown native events may be `x.<harness>.*` but never change core state.
- Events may precede command reply. Correlate by generation/native ids, not arrival.

## Source ownership

For each agent generation and normalized family, one source is authoritative:

- controlled live stream normally owns session/turn/message/tool/request;
- transcript owns those families for observe-only sessions or after an explicit cursor
  handoff;
- hooks own only assigned missing facts or synchronous decisions;
- process/Git/filesystem sources own their separate families.

A plugin never tails a transcript to publish duplicate lifecycle while its live stream
owns it. Handoff starts after the last committed native cursor/id. If lossless boundary
cannot be proved, report a source gap; do not replay completion or treat interval as
empty.

## Hooks

Where native command hooks are necessary, generate **session-scoped** config and invoke:

```text
eliot hook --harness <name> --event <HookName>
```

Shim receives agent id, generation and current token; reads bounded JSON; calls
`hook.event`; prints native decision. It normalizes only event families assigned to
that hook. Default deadline 2 s. Timeout returns configured safe native default
(observation: allow; decision: ask) and exits successfully. If daemon is unreachable,
try immediately, 100 ms and 400 ms, then drop and report source loss after reconnect.
There is no durable hook queue. SDK plugins use in-process callbacks.

## Transcripts and discovery

Plugins may tail native transcripts for discovery, observe-only agents or explicit
stream recovery. Cursors live under plugin data dir; host never parses vendor format.

Wait only for an incomplete final line. A complete unparsable line creates a source
gap, not silent loss/reconnect loop. On Windows open/read/close each poll for rotation.
Never emit old history as new live turns or infer terminal state from silence,
truncation or list absence.

## Lifecycle and custody

- Plugin start is single-flight per instance. Stop during start cancels it; late result
  from cancelled instance is discarded and owned group terminated.
- Per-agent plugin starts in an agent-owned Windows Job / Unix process group before it
  can launch native children. Exit causes host to terminate/verify remainder; only
  that agent is affected.
- Shared plugin may not launch unisolated per-agent process trees. Per-session force is
  supported only through an exact native operation; otherwise unsupported.
- Closing per-agent plugin stdin requests shutdown. After 10 s, terminate/verify exact
  group. Shared plugin closes only native sessions it owns; attached owner service is
  never stopped.
- Windows uses explicit inheritable handle list; Unix sets process group before exec.
- Requests use one absolute deadline (default 30 s; start 120 s). Timeout after possible
  native I/O is unknown; trickled bytes do not extend deadline.
- Shared plugin restart re-attaches exact generations and snapshots open commands.
  Per-agent restart normally loses stdio-owned native process; service-backed exact
  attach may recover. `not_found` yields lost.
- Daemon restart turns dispatched into unknown; no replay instruction is sent.
- Process exit and final stdout can race. Finish only after terminal native input or
  stdout EOF, never exit alone.
- Continuously drain stdout. Replies/requests outrank ephemeral telemetry; persisted
  events apply backpressure, ephemeral may drop with lag health.
- Shared plugin reader never waits on one session's native I/O.

## Fake plugin

`plugins/fake` is shared and simulates isolated generation-bound sessions. It persists
minimal state for resume/readback and emits stable step ids. Wrong generation, turn or
request is rejected before effects.

A launch/send script is `{"steps":[...]}`; other text is echoed. Steps:

| Step | Behavior |
| --- | --- |
| `say {text}` | assistant message |
| `sleep_ms {n}` | no progress; exact steer/interrupt may arrive |
| `tool {name,ok?,ms?}` | tool lifecycle |
| `approval {...}` / `question {...}` | exact pending request and wait |
| `spawn {name?,steps}` | child registration/turn |
| `usage {...}` | turn usage |
| `crash` / `hang` / `exit` | process crash, no reply, normal session end |

Interrupt affects only supplied turn. Old steps are not replayed after restart.

## Recordings

Real native traffic can be recorded for later L7 replay; CI never calls a model.

```text
{"t":ms,"dir":"out|in","chan":"stdio|http|sse|...","msg":...}
```

`ELIOT_RECORD=<file>` appends bounded sanitized lines. `ELIOT_REPLAY=<file>` checks
stable outgoing method/path/payload fields (ignoring generated ids/timestamps) and
delivers recorded input; special hang/exit records simulate failure. Fixtures include
harness version/date/scenario and no secrets.

## Conformance kit

L7 completes `eliot plugin-test <cmd...>` and verifies:

1. manifest/protocol/session mode;
2. one matching ready-before-start-success;
3. one turn per send with current generation;
4. stale generation/turn/request rejection before native I/O;
5. declared verbs/requests/goals/children only;
6. reconnect/replay does not duplicate lifecycle;
7. no unknown retry after timeout/disconnect/restart;
8. per-agent stop affects only exact group; shared stop never kills service/other session;
9. attach restores exact incarnation or reports not found;
10. unbridgeable source handoff reports a gap.

Real plugins run against recordings and compact live runs; fake runs deterministically.
