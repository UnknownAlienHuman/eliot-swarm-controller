# Plugin protocol

A plugin translates Eliot's semantic commands to one harness and normalizes native
facts. It is a process speaking this protocol on stdin/stdout.

- **Built-in plugins** are Rust crates in `plugins/<name>/` linked into `eliot` and
  executed as `eliot plugin <name>`.
- **External plugins** may use any language and are started from
  `[plugins.<name>] cmd = [...]`.
- A plugin declares `session_mode = per_agent | shared`:
  - `per_agent`: one plugin process owns one agent incarnation and any native child
    processes. This is the default for stdio/CLI harnesses and gives the daemon one
    exact Job/process group for `stop --force`.
  - `shared`: one plugin process multiplexes sessions already isolated by a native
    service, or simulated sessions. It must not require killing the shared plugin or
    service to stop one agent. Fake, an attached OpenCode server and ACP gateways are
    typical shared cases.

The daemon may run many per-agent plugin processes. A few MiB of controller overhead
is preferable to an untrue process-ownership model.

## Framing

- JSON-RPC 2.0, one UTF-8 JSON object per line, no BOM, maximum 1 MiB per line.
- Rust uses `tokio_util::codec::LinesCodec` / `FramedRead`; never cancel a partial
  `read_line` in `select!`. A long-lived native reader has its own task.
- stderr is bounded plugin log text; stdout is protocol only.
- Both directions may send requests and notifications. Request ids are unique per
  direction.
- Every agent-scoped request, reply context, notification and plugin request carries
  `agent_id` and `generation`. The plugin keeps an exact mapping to native session,
  active turn and pending requests for that generation.
- Protocol version is `eliot-plugin/1`. Within it, fields may be added but existing
  meaning cannot change.

## Host → plugin requests

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | `protocol`, `plugin_config`, `data_dir`, `instance_id` | `Manifest` |
| `agent.start` | `agent_id`, `generation`, resolved `profile`, `workspace?`, `prompt?`, `env`, `system_note`, `parent_native_ref?` | `{native_scope, native_ref, pids, effective}` after the generation is ready |
| `agent.attach` | `agent_id`, `generation`, `native_scope`, `native_ref`, `observe_only` | `{state, active_turn?, pending, pids, effective?}` for the exact still-live incarnation; `not_found` if absent |
| `agent.command` | `agent_id`, `generation`, `command_id`, `verb`, `args`, `expected_turn_id?`, `request_id?` | `{status: applied|rejected|failed|unknown, applies?: now|next_tool|next_turn|next_session, detail?}` |
| `agent.snapshot` | `agent_id`, `generation`, `commands?: [command_id]` | `{state, active_turn?, pending, goal?, usage?, commands?: {id: applied|not_applied|unknown}}` |
| `agent.stop` | `agent_id`, `generation`, `command_id`, `force` | `{}` only after that generation ended |
| `discover` | `{}` | `[{native_scope, native_ref, workspace?, started_at?, title?, live}]` |
| `shutdown` | `{}` | `{}`; the plugin exits after replying |

`Pid = {pid, start_time, role}`; `role = harness | server | tool | proxy`.

### Start contract

A successful `agent.start` has one unambiguous sequence:

1. establish the exact native session and generation mapping;
2. emit exactly one `session.ready` event with the same `native_scope`, `native_ref`,
   effective settings and generation;
3. return the matching result.

The daemon does not dispatch another command for the generation until both facts are
consistent. Events that race ahead of `session.ready` are buffered for that connection
and released after `session.ready` commits. If `session.ready` committed but the start
reply was lost, it is exact readback that the launch applied. If native I/O may have
occurred but neither the ready fact nor a negative readback exists, the launch is
`unknown`, not `failed` and never retried.

`resume` is routed through `agent.command` only for native APIs that reopen within the
same plugin process; the host has already created a new Eliot generation. A
per-agent plugin may instead implement resume during `agent.start` using the retained
`native_scope/native_ref` supplied in `profile`/start context. The harness page states
which mapping it uses. Old input is never resent.

`agent.attach` is for daemon/plugin recovery and discovered sessions. Re-attaching the
same still-live incarnation preserves generation. Starting a replacement session is
`resume` and increments generation.

## Plugin → host notifications

| Method | Params |
| --- | --- |
| `event` | `{agent_id, generation, kind, ts?, native_id?, data}` |
| `process` | `{agent_id, generation, pid, start_time, role}` |
| `log` | `{level, message}` |

The host rejects stale-generation notifications before they reach the normal event
bus. Repeated notifications must carry a stable `native_id` where the native protocol
has one; the plugin itself suppresses repeats from reconnect/replay.

## Plugin → host requests

| Method | Params | Result |
| --- | --- | --- |
| `child` | `{parent_agent_id, parent_generation, native_ref, name?, labels?}` | `{agent_id, generation, token}` |

`child` is idempotent for `(parent agent, parent generation, native_ref)`. The host
creates an `origin=native` child at generation 1. Where the harness can set child
environment, the plugin gives it the returned token. If native subagents share the
parent process/environment and cannot receive a distinct token, their own `eliot`
CLI calls are attributed to the parent; `eliot show` says so.

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

`available` means the native harness was found and can be started. `proxy` is
`config | env | none`. Harness versions are display facts, never gates.

A `shared` plugin may declare a finite `max_sessions` if its native service has a hard
limit. `per_agent` plugins do not declare multiplexing or rely on `[plugins.X]
instances`; each live agent gets its own instance.

## Binding and translation rules

1. Validate `generation` before any native I/O. For a stale generation return
   `rejected {reason: stale_generation}`.
2. `steer` and `interrupt` require `expected_turn_id` equal to the current native turn.
   Approval/deny/answer require the exact pending `request_id`. A mismatch is
   `rejected`, never redirected to whatever is current.
3. Translate one Eliot verb to exactly one documented native mechanism and return it
   in `detail.mechanism` (for example `turn/steer`, `hook:additionalContext`,
   `queued:next_send`).
4. If the native side cannot express the semantics, return `rejected` with a reason.
   Never silently substitute `send` for `steer`, restart for resume, or default choice
   for an answer.
5. `unknown` means native I/O may have happened but no exact outcome is available.
   The plugin never retries it, including `interrupt` and `stop`.
6. Report `applies` for `configure` and delayed mechanisms.
7. Native request ids pass through unchanged. Do not use list position or "the newest"
   request when an id exists.
8. A queued `send` arrives at the plugin only when its bound generation is idle. The
   daemon, not the plugin, owns its FIFO queue.

## Event normalization

- The successful start result is preceded by one `session.ready`; normal end is
  `session.ended`; an unexpected owned-process exit is `session.lost`; a fatal native
  protocol/session error while the process remains is `session.error`.
- Emit `turn.started` / `turn.completed` exactly once per native turn id. A terminal
  turn status is `completed`, `interrupted` or `failed`. Intermediate statuses are not
  completions.
- `message.agent` carries bounded final assistant text. Deltas are ephemeral
  `message.delta` and carry `turn_id`.
- Tool summaries are short; full output stays native. Every tool/request event carries
  the exact turn/request/item id available from the harness.
- Usage counters state their scope (`turn` or `session`). Never sum cumulative values.
  Unknown quota is unknown, not zero.
- Provider errors map to `provider.limited.kind`: `rate_limit`, `quota`, `model_gone`,
  `auth`. A 429 is `rate_limit` unless the native error proves quota exhaustion.
- Unknown native events may be forwarded as `x.<harness>.<name>`.
- Events may precede the command reply that caused them. Correlate through generation,
  command/native ids and turn ids, not arrival order.
- Child usage belongs to the child unless the native source explicitly reports an
  aggregate; then mark it as aggregate.

## Source ownership

For one agent generation, exactly one source owns each normalized family:

- a controlled live native stream normally owns session/turn/message/tool/request;
- a transcript is primary for observe-only sessions or takes over after a recorded
  stream cursor/watermark;
- hooks own only facts unavailable from the primary stream, or synchronous decisions;
- process/Git/filesystem sources own their separate families.

A plugin must not tail a transcript and publish duplicate lifecycle events while its
live stream already owns them. On hand-off it records the last native id/cursor it has
published and starts after it. If the formats cannot establish a lossless boundary,
it reports an explicit source gap and continues without inventing or replaying a
completion. See [monitoring](monitoring.md#source-ownership).

## Hooks

For a harness with native command hooks, the plugin writes a **session-scoped** hook
configuration; never edit the user's global settings. The command is:

```text
eliot hook --harness <name> --event <HookName>
```

The shim receives `ELIOT_AGENT_ID`, `ELIOT_AGENT_GENERATION` and the current token,
reads bounded hook JSON from stdin, calls `hook.event`, and prints the harness-native
decision. The daemon normalizes only event families assigned to hooks, runs matching
approval rules and may return pending `inject` text when the hook supports it.

Default timeout is 2 s. On timeout the shim returns the configured safe native default
(`allow` for observation-only hooks, `ask` for decision hooks) and exits 0. If the
daemon is unreachable it tries at 0, 100 and 400 ms, then drops the hook event and
records source-health loss after reconnection; there is no durable hook queue. SDK
plugins use in-process callbacks rather than the shim.

## Transcripts and discovery

A plugin with `features.transcripts` may tail native transcript files for discovery,
observe-only sessions and explicit stream recovery. It stores cursors in
`<data_dir>/plugins/<name>/`; the host does not parse vendor transcripts.

For a file still being written, only a final unterminated line waits. A complete line
that cannot be parsed produces a source gap, never silent loss or a reconnect loop.
On Windows, open/read/close each poll so the writer can rotate the file.

Transcript readers must obey source ownership. They never emit old history as new live
turns and never infer a terminal event from silence, truncation or list absence.

## Lifecycle and process custody

- Plugin start is single-flight for an instance. Stop during start cancels the start;
  a late result with the cancelled instance id is discarded and its owned process tree
  is terminated.
- In `per_agent` mode the daemon starts the plugin in an agent-owned Windows Job / Unix
  process group. Every native child it launches belongs to that custody boundary.
  Plugin exit causes the daemon to terminate and verify the remainder of that group;
  only that agent is affected.
- In `shared` mode the plugin must not launch unisolated per-agent child trees. A
  per-session `force` is accepted only when the native service exposes an exact force
  operation; otherwise it is `unsupported`. Stopping one agent never kills the shared
  plugin or service.
- Closing a per-agent plugin's stdin requests shutdown. If it or its children remain
  after 10 s, the daemon terminates the exact group and verifies `(pid,start_time)`.
  A shared plugin closes only sessions it owns according to its native protocol; an
  attached owner-run service is never stopped.
- Windows child stdio uses an explicit inheritable-handle list. Unrelated children may
  not inherit the parent's pipe ends. Unix children enter the assigned process group
  before exec.
- Requests time out (default 30 s; start 120 s) against one absolute deadline. Timeout
  produces `unknown` when native I/O may have occurred. No timeout extends because a
  peer trickles bytes.
- After a shared plugin restart, the host re-attaches its live generations and asks
  snapshots for open commands. A per-agent plugin restart normally loses its owned
  stdio native process; if a service-backed native session survived, exact attach may
  recover it. `not_found` becomes `session.lost`.
- On daemon restart, every `dispatched` command is `unknown`; no plugin is instructed
  to replay it. Readback may settle it.
- A child process exit and its final stdout line can arrive in either order. Finish a
  turn/request only after terminal native input or stdout EOF, never from exit alone.
- Bound every native payload (default 1 MiB). Oversize input becomes an explicit gap.
- Read plugin stdout continuously. Replies and plugin requests never wait behind
  telemetry; under pressure ephemeral events are dropped and subscribers see lag,
  while persisted events apply backpressure.
- A plugin stdin reader never waits for native I/O. Slow native work for one session
  cannot block control of another shared session.

## Fake plugin

`plugins/fake` uses `session_mode = shared` and simulates many independent sessions in
one process. It is deterministic and used by all L1 and load tests. Its declared verbs
may be narrowed by config.

Any launch/send/steer/inject text is a script if it parses as
`{"steps": [...]}`; otherwise it is echoed. A send/launch script is one turn.

| Step | Fields | Emits |
| --- | --- | --- |
| `say` | `text` | `message.agent` |
| `sleep_ms` | `n` | no progress for n ms; steer may arrive |
| `tool` | `{name, ok?=true, ms?=0}` | `tool.started`, then `tool.completed` |
| `approval` | `{kind, command?, path?}` | request, wait, resolution |
| `question` | `{text, options?}` | question, wait, answer |
| `spawn` | `{name?, steps}` | `child`, then child's first turn |
| `usage` | `{tokens_in, tokens_out}` | turn usage |
| `crash` | — | plugin exits |
| `hang` | — | stops replying |
| `exit` | — | `session.ended` |

Every emitted event contains the script session's generation and stable step id.
`interrupt` only affects the supplied expected turn. A stale generation, turn or
request is rejected. Fake session state is persisted under its plugin data directory
so exact resume/readback tests survive plugin restart; old steps are not replayed.

## Recordings

Real harness traffic is recorded once and replayed in tests; CI never calls a model.

- `ELIOT_RECORD=<file>` appends bounded lines
  `{"t":ms,"dir":"out|in","chan":"stdio|http|sse|...","msg":...}`.
- `ELIOT_REPLAY=<file>` checks outgoing method/path and stable payload fields, ignoring
  generated ids/timestamps, then delivers recorded input. `{"dir":"hang"}` and
  `{"dir":"exit","code":N}` simulate failure.
- Fixtures live in `plugins/<name>/fixtures/` with a first-line metadata object
  containing harness version, date and scenario. Secrets are removed before commit.

`eliot-plugin` supplies record/replay transports for Rust; external plugins use the
same format.

## Conformance kit

`eliot plugin-test <cmd...> [--replay-dir DIR]` verifies every declared capability:

1. valid initialize/manifest and the declared session mode;
2. start emits one matching `session.ready` before success;
3. send starts exactly one turn and carries the current generation;
4. wrong generation, turn and request ids are rejected before native I/O;
5. steer, interrupt, approvals, answers, goals and child mapping behave as declared;
6. duplicate/replayed native events do not create duplicate normalized lifecycle
   events;
7. no unknown command is retried after hang, disconnect or restart;
8. per-agent stdin close/force ends and verifies only its process group; shared-mode
   stop never kills the service or another session;
9. attach either restores the exact incarnation or reports `not_found`;
10. source hand-off reports a gap when it cannot prove a complete cursor boundary.

Real plugins run the kit against recordings; fake runs live. A plugin passes before
merge.
