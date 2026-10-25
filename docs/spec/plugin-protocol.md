# Plugin protocol

A plugin is a process that speaks this protocol on stdin/stdout. A plugin process
serves agents of one harness kind — many of them if its native side keeps sessions
independent (`features.multi_session`), otherwise one.

- **Built-in plugins** are Rust library crates in `plugins/<name>/` that depend on
  `eliot-plugin`; the `eliot` binary links them and runs one as `eliot plugin <name>`.
- **External plugins** (any language) live in `plugins/<name>/` with their own
  toolchain and are started from config: `[plugins.<name>] cmd = ["node",
  "plugins/claude/main.mjs"]`.

## Framing

- JSON-RPC 2.0, one JSON object per line on stdout/stdin, UTF-8, no BOM, at most
  1 MiB per line.
- Rust: read lines with `tokio_util` `FramedRead` + `LinesCodec` (cancel-safe); never
  `read_line` inside a `select!` that can cancel it (partial lines are lost). A
  long-lived native reader gets its own task — `spawn_blocking` work cannot be aborted.
- stderr is free-form log text; the host captures it (bounded) into the daemon log.
- Both sides may send requests and notifications. Ids are unique per direction.
- Every agent-scoped message carries `agent_id`. The plugin keeps its own map
  `agent_id → native session`.
- Protocol version: `"eliot-plugin/1"`. Additive fields only within a version.

## Host → plugin requests

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | `protocol`, `plugin_config` (its `[plugins.<name>]` table), `data_dir` | `Manifest` (below) |
| `agent.start` | `agent_id`, `profile` (resolved), `workspace?`, `prompt?`, `env` (Eliot vars to inject), `system_note` ([text](coordination.md#communication-guidance-for-agent-prompts)), `parent_native_ref?` | `{native_scope, native_ref, pids: [Pid], effective: {model?, permissions?, system_note: native|prepended, endpoint?}}` |
| `agent.attach` | `agent_id`, `native_scope`, `native_ref`, `observe_only` | `{state, pids}` — after a plugin/daemon restart or for discovered sessions; error `not_found` if the session is gone. If a turn was running and its end is not confirmed, the state is `busy` (no `turn.completed` is invented) and the host repeats `agent.snapshot` until the native side settles it. |
| `agent.command` | `agent_id`, `command_id`, `verb`, `args` | `{status: applied|rejected|failed|unknown, applies?: now|next_tool|next_turn|next_session, detail?}` |
| `agent.snapshot` | `agent_id`, `commands?: [command_id]` | `{state, pending: [Request], goal?, usage?, commands?: {id: applied|not_applied|unknown}}` |
| `agent.stop` | `agent_id`, `force` | `{}` once the native session ended (or `force` killed it) |
| `discover` | `{}` | `[{native_scope, native_ref, workspace?, started_at?, title?, live}]` — native sessions on this machine |
| `shutdown` | `{}` | `{}`; plugin exits after replying |

`Pid = {pid, start_time, role}`; `role` = `harness` | `server` | `tool` | `proxy`.

Verb routing: `launch` arrives as `agent.start`, `stop` as `agent.stop`; every other
verb arrives as `agent.command` — including `resume`, which carries `native_scope` and
`native_ref` and re-opens the session from native (or, for the fake, persisted)
state. `agent.attach` is only for re-attaching sessions that may still be running
after a plugin or daemon restart, and for discovered sessions.

## Plugin → host notifications

| Method | Params |
| --- | --- |
| `event` | `{agent_id?, kind, ts?, data}` — normalized kinds from [core](core.md#event-kinds) |
| `process` | `{agent_id, pid, start_time, role}` — register another native process |
| `log` | `{level, message}` |

## Plugin → host requests

| Method | Params | Result |
| --- | --- | --- |
| `child` | `{parent_agent_id, native_ref, name?, labels?}` | `{agent_id, token}` — the host creates an `origin=native` agent (idempotent per `native_ref`). Where the harness lets the plugin set a child's environment, it gives the child this token. Where it cannot (subagents running inside the parent's process share its environment), the child's `eliot` calls are indistinguishable from the parent's and are attributed to the parent; `eliot show` says so for that child. |

## Manifest

```json
{
  "name": "codex", "version": "0.1.0", "protocol": "eliot-plugin/1",
  "harness_version": "codex-cli 0.164.0 (as reported by the native side, or null)",
  "available": true,
  "verbs": ["launch","send","steer","inject","interrupt","stop","resume",
            "approve","deny","answer","configure","goal.set","goal.pause","goal.resume","goal.clear"],
  "child_verbs": ["send","interrupt"],
  "features": { "native_goal": true, "native_children": true, "hooks": false,
                "transcripts": true, "discover": true, "cost": false,
                "multi_session": true },
  "proxy": "config"
}
```

`available`: the plugin found a usable native harness (binary present and runnable);
`launch` preflight requires it. `proxy`: `config` | `env` | `none` — how the plugin
points its harness at a proxy (details on its harness page). Method names on this page
belong to the plugin connection; they are separate from client API methods of the
same name. The host routes only the declared verbs. Harness
versions are for display; they are never a gate.

## Verb translation rules

1. Translate the semantic verb to exactly one native mechanism and say which in
   `detail.mechanism` (e.g. `turn/steer`, `hook:additionalContext`, `queued:next_send`).
2. If the native side cannot express the verb faithfully, return `rejected` with
   `detail.reason`. Never substitute a weaker action silently (e.g. `send` for `steer`).
3. `unknown` when the request may have reached the native side but no reply arrived.
   The plugin must not retry it.
4. Report `applies` for `configure` and for mechanisms that land later
   (`next_tool` = after the current tool call).
5. Approval/question ids are native ids passed through unchanged as `request_id`.
6. Children: when the harness spawns a subagent, call `child` once, then send that
   child's events with the returned `agent_id`.

## Event normalization rules

- Session lifecycle: `session.ready` once the native session accepts input;
  `session.ended` when it ends normally; `session.error` when the native side reports
  a fatal error while still running; `session.lost` when the native process exits
  unexpectedly (an exit is never reported as `session.error`).
  The host derives `agent.*` state events; plugins never send `agent.*`.
- Emit `turn.started` / `turn.completed` exactly once per native turn; `status`
  distinguishes completed, interrupted and failed. Non-terminal native statuses are not
  turn ends.
- `message.agent` carries the final text of an assistant message (bounded); streaming
  pieces go to `message.delta` (ephemeral).
- `tool.*` summaries are short (`"bash: cargo test -p foo"`); full output stays native.
- Usage: send cumulative values with their `scope` (`turn` or `session`); never sum
  cumulative counters. A harness without a machine-readable quota reports quota as
  unknown, never zero.
- Provider errors map to `provider.limited.kind`: `rate_limit` (retry soon), `quota`
  (window exhausted), `model_gone`, `auth`. A 429 is `rate_limit` unless the native
  error says quota.
- Unknown native events may be forwarded as `x.<harness>.<name>` for debugging.
- Events may arrive before the reply to the command that caused them (a fast
  `turn.started` before `send`'s reply). Correlate by native turn/request ids, never
  by arrival order.
- Usage of a child is reported on the child; a parent's usage never includes its
  children unless the native side says it does (then say so in `data`).

## Hooks

For harnesses with native hooks (Codex, Claude Code CLI, Antigravity), the plugin
writes a **session-scoped** hook config (never the user's global settings) whose
command is:

```text
eliot hook --harness <name> --event <HookName>
```

The shim reads the hook JSON from stdin, calls `hook.event` on the daemon with
`ELIOT_AGENT_ID`, and prints the decision in the harness's native hook output format.
The daemon turns the hook into events (`tool.started`, `approval.requested`, ...),
runs matching [approval rules](automation.md#approvals) and delivers pending `inject`
text as additional context when the hook allows it. If the daemon does not answer
within the hook timeout (default 2 s), the shim prints the configured default
(`allow` for observation-only hooks, `ask` for decision hooks) and exits 0. If the
daemon is unreachable the shim tries at most three times (0, 100, 400 ms); after that
the hook event is dropped and counted in `source.health` — there is no durable queue. SDK-based
plugins (Claude Agent SDK) use in-process hook callbacks instead of the shim.

## Transcripts and discovery

Plugins with `features.transcripts` can tail native transcript files (e.g. Codex
rollout JSONL, Claude project JSONL) and emit the same normalized events. This is how
Eliot watches sessions it did not launch (`origin=external`, `observe_only=true` unless
the plugin can attach for control). The plugin stores its read offsets in its own state
dir (`<data_dir>/plugins/<name>/`); the host does not parse transcripts.

Tailing a file that is still being written: only a final line without a newline is a
torn write and waits for more bytes; a complete line that fails to parse becomes a gap
event, never a silent drop. On Windows open, read and close the file on each poll —
a held handle blocks the harness from rotating or replacing it.

## Lifecycle and errors

- The host starts a plugin on first use, sends `initialize`, and restarts it with
  backoff (1 s → 30 s) if it exits. Starting a plugin or a service is single-flight
  per name (two launches share one start); a stop during a start cancels it and a
  late start result is discarded. On restart it re-attaches every live agent
  (`not_found` → `session.lost`) and asks `agent.snapshot` for open commands.
- **A plugin exits when its stdin closes** and ends the native processes it launched.
  Closing stdin is a request, not a kill: if a plugin or native process is still alive
  after the stop timeout (10 s), the host kills its process tree and verifies the
  recorded `(pid, start_time)` identities. Job objects / process groups (L3.1) make this
  hold even if the daemon itself dies.
- Windows: only the child's ends of its stdio pipes are inheritable, and they are
  passed in an explicit handle list (`PROC_THREAD_ATTRIBUTE_HANDLE_LIST`, which
  requires inheritable handles) via `CreateProcessW` in `eliot-daemon::platform`;
  the parent's ends are never inheritable. Otherwise an unrelated child that
  inherited a write end keeps the pipe open and stdin EOF never arrives. `std::process`
  has no handle-list API.
- A plugin that fails `initialize` three times is marked `failed`; its agents become
  `lost`. Other plugins are unaffected.
- Requests time out (default 30 s, `agent.start` 120 s). A deadline bounds the whole
  reply, not each read (a peer trickling bytes must not extend it). A timed-out command
  becomes `unknown` and is never resent. A parent deadline is always longer than the
  whole budget of what it waits for; no wait on a child process is unbounded — every
  wait ends in kill-and-verify.
- A child's exit and its last stdout line arrive in either order: finish a turn or
  request only after stdout reached EOF (or the terminal event arrived), never on the
  exit signal alone.
- Bound every native payload the plugin forwards (default 1 MiB per message); an
  oversized item becomes an explicit gap event, never a reconnect loop.
- The host reads every plugin's stdout continuously. Replies and plugin requests
  (`child`) are handled on a path that never waits for the event queue, so an
  approval is never stuck behind telemetry. Under pressure ephemeral events are
  dropped (subscribers see `events.lagged`); persisted events never are.
- A plugin must not block its own stdin reader on native I/O: a slow native call must
  not delay `interrupt` or `stop` for another agent.

## Fake plugin

`plugins/fake` simulates a harness deterministically; it is used by every L1 test and
by load tests. It declares `launch`, `send`, `steer`, `inject`, `interrupt`, `stop`,
`resume`, `approve`, `deny`, `answer`, `configure` (no native goal, children are
created by the `spawn` step), unless `[plugins.fake] verbs = [...]` narrows them.
It reports `system_note: native` and `configure` applies `now`.

Any input text (launch prompt, `send`, `steer`, `inject`) is a script if it parses as
`{"steps": [...]}`; otherwise it is echoed (`message.agent` with the same text).
`inject` shows up as `message.user {injected: true}` at the next step boundary of a
running turn, or at the start of the next turn when idle. A
`send`/`launch` script runs as one turn: `turn.started` … `turn.completed`.

| Step | Fields | Emits |
| --- | --- | --- |
| `say` | text | `message.agent {text}` |
| `sleep_ms` | n | nothing for n ms (turn stays busy; `steer` during it emits `message.user` and continues) |
| `tool` | `{name, ok?=true, ms?=0}` | `tool.started`, then after `ms` `tool.completed {ok}` |
| `approval` | `{kind, command?, path?}` | `approval.requested`; the script waits for `approve`/`deny`, then `approval.resolved` (on deny, the next step is skipped) |
| `question` | `{text, options?}` | `question.asked`; waits for `answer`, then `question.answered` |
| `spawn` | `{name?, steps}` | calls `child`, then runs `steps` as the child's first turn |
| `usage` | `{tokens_in, tokens_out}` | `usage.updated {scope: turn}` |
| `crash` | — | the plugin process exits with code 1 |
| `hang` | — | stops answering requests (to test timeouts) |
| `exit` | — | `session.ended` for this agent |

`interrupt` ends the running turn with `turn.completed status=interrupted`. `stop` emits
`session.ended`. Like a stdio harness, live sessions exist only inside the plugin
process: after the process dies, `agent.attach` answers `not_found`. Each session's
history and position are written to `<data_dir>/plugins/fake/`, so `resume` re-opens
it (like a harness resuming from its transcript); resuming never replays earlier
steps.

## Recordings

Real harness traffic is recorded once and replayed in tests, so CI never calls a model.

- `ELIOT_RECORD=<file>`: the plugin appends every native message it sends or
  receives as one JSON line `{"t": <ms since start>, "dir": "out"|"in", "chan":
  "<stdio|http|sse|...>", "msg": <native message>}`.
- `ELIOT_REPLAY=<file>`: instead of starting the native process, the plugin uses a
  replay transport that checks each outgoing message against the next `out` line
  (same method/path; ids and timestamps ignored) and delivers the following `in`
  lines. A mismatch fails the test with both messages printed. Two control lines
  simulate failures: `{"dir": "hang"}` (no further replies) and `{"dir": "exit",
  "code": N}` (the native process dies).
- Files live in `plugins/<name>/fixtures/<scenario>.jsonl`, named with the harness
  version and date in a first line `{"meta": {"harness_version", "recorded_at",
  "scenario"}}`. Secrets are removed before commit (the recorder drops auth headers
  and env).

`eliot-plugin` provides both transports for Rust plugins; external plugins implement
the same file format.

## Conformance kit

`eliot plugin-test <cmd...> [--replay-dir DIR]` starts the plugin with a temp data dir
and checks, for the verbs the manifest declares:

1. `initialize` returns a valid manifest;
2. `agent.start` → `session.ready` → a turn with `turn.started` / `turn.completed`;
3. `send` arrives only while the agent is idle (the host queues sends for busy
   agents) and starts exactly one turn;
4. `steer`, `interrupt`, `approve`/`deny`, `answer`, `goal.*`, `child` — each declared
   verb produces its documented events; each undeclared verb is never sent;
5. closing stdin ends the plugin and its native processes;
6. a killed and restarted plugin re-attaches or reports `not_found`;
7. a request without a reply is reported `unknown` and not retried.

For real harnesses the kit runs in replay mode against `plugins/<name>/fixtures/`;
for the fake plugin it runs live. Every plugin, in any language, passes it before merge.
