# Plugin protocol

A plugin is a process that speaks this protocol on stdin/stdout. It translates one
harness kind; it never writes the Store and never retries a native effect whose
outcome is unknown.

- **Built-in plugins** are Rust library crates in `plugins/<name>/` that depend on
  `eliot-plugin`; the `eliot` binary links them and runs one as `eliot plugin <name>`.
- **External plugins** (any language) live in `plugins/<name>/` with their own
  toolchain and are started from config: `[plugins.<name>] cmd = ["node",
  "plugins/claude/main.mjs"]`.

**One plugin process per agent** is the default. A harness process is the session,
and tools inherit that process's environment, so an agent's own token, Git identity
and proxy credentials need their own process; the agent's process tree (plugin,
harness, tools) is one custody unit ([monitoring](monitoring.md#process-monitor)), so
`stop --force` and a plugin crash touch only that agent. A plugin that declares
`features.multi_session` serves many agents from one process — the fake plugin, and an
attached server only when it gives each session its own environment and stop
(otherwise its sessions are observe-only).

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
- Size limits inside the frame: input text ≤ 512 KiB, persisted event `data` ≤ 64 KiB,
  list pages ≤ 1,000 items. Long system notes and scripts are passed as file paths,
  never inline.

## Host → plugin requests

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | `protocol`, `plugin_config` (its `[plugins.<name>]` table), `data_dir` | `Manifest` (below) |
| `agent.start` | `agent_id`, `profile` (the agent's `profile_snapshot`), `workspace?`, `env` (the complete [environment](configuration.md#environment) for the harness), `system_note` (path of a [text file](coordination.md#communication-guidance-for-agent-prompts)), `parent_native_ref?`, `resume?: {native_scope, native_ref, cursors}`, `initial?: {command_id, text}` | `{native_scope, native_ref, pids: [Pid], effective: {model?, permissions?, system_note: native|prepended, endpoint?}, initial?: rejected|failed}` |
| `agent.attach` | `agent_id`, `native_scope`, `native_ref`, `observe_only`, `profile?` (for control), `cursors`, `active_turn?` | `{state, pids, active_turn?, pending}` — after a plugin/daemon restart, for discovered sessions and for `attach --control`; error `not_found` if the session is gone. If `active_turn` was running and its end is not confirmed, the state is `busy` (no `turn.completed` is invented) and the host repeats `agent.snapshot` until the native side settles it. |
| `agent.command` | `agent_id`, `command_id`, `verb`, `args`, `turn_id?` (steer, interrupt), `request_id?` (approve, deny, answer) | `{status: applied|rejected|failed|unknown, applies?: now|next_tool|next_turn|next_session, detail?}` |
| `agent.snapshot` | `agent_id`, `commands?: [command_id]` | `{state, active_turn?, pending: [Request], goal?, usage?, commands?: {id: applied|not_applied|unknown}}` |
| `agent.stop` | `agent_id`, `force` | `{}` once the native session ended (or `force` killed it) |
| `discover` | `{cursor?, limit?}` (limit ≤ 1,000) | `{sessions: [{native_scope, native_ref, workspace?, started_at?, title?, live}], next?}` — native sessions on this machine, one page |
| `shutdown` | `{}` | `{}`; plugin exits after replying |

`Pid = {pid, start_time, role}`; `role` = `harness` | `server` | `tool` | `proxy`.

Verb routing: `launch` and `resume` arrive as `agent.start` (`resume` carries the
native subject and the saved [cursors](#cursors-and-replay)), `stop` as `agent.stop`;
every other verb arrives as `agent.command`. `agent.attach` is for re-attaching
sessions that may still be running after a plugin or daemon restart, and for
discovered sessions.

`initial` is set only when the manifest says `start_requires_input` and the launch or
resume has a linked send: the plugin delivers that text as the session's first input.
Either way the send is settled on its own, by `turn.started {commands: [...]}` or
`agent.snapshot` ([core](core.md#commands)). A plugin without `start_requires_input`
never receives input in `agent.start`.

## Plugin → host requests

| Method | Params | Result |
| --- | --- | --- |
| `event` | `{agent_id?, kind, ts?, native_id?, cursor?: {source, value}, data}` for a **persisted** kind ([core](core.md#event-kinds)) | `{seq}` — sent after the event and everything it changed are committed. A duplicate `native_id` returns the existing `seq`. |
| `child` | `{parent_agent_id, native_scope, native_ref, name?, labels?}` | `{agent_id, token?}` — the host creates (or returns, same subject) an `origin=native` agent. `token` is present only if the manifest says `child_environment`: the plugin puts it into the child's environment before the child's tools run. Otherwise the child has `own_identity = false` and its `eliot` calls count as the parent's. Tree limits never refuse the row ([core](core.md#agent)). |

## Plugin → host notifications

| Method | Params |
| --- | --- |
| `event` | `{agent_id?, kind, ts?, data}` for an **ephemeral** kind (`message.delta`, `process.stats`, `usage.updated`, `fs.changed`) |
| `process` | `{agent_id, pid, start_time, role}` — a native process the plugin knows about; the host records it only if it is inside that agent's custody (a shared server's processes belong to the service) |
| `log` | `{level, message}` |

The host handles a plugin's requests in the order they arrive. A plugin may pipeline
`event` requests but treats a fact as stored only when its reply arrived.

## Cursors and replay

A native source that can be read again — a transcript file, a server's event log, a
paged history — has a position. The plugin puts it in `cursor` of each persisted event
it derived from that source; the Store saves the latest value per (agent, `source`) in
the same transaction as the event. `agent.attach` and `agent.start` with `resume` hand
the saved cursors back, and the plugin continues strictly after them. So a crash
between commit and reply repeats nothing, and an uncommitted event is read again.

```sql
CREATE TABLE source_cursors (   -- migration of L3.4, the first replaying source
  agent INTEGER NOT NULL REFERENCES agents(id), source TEXT NOT NULL,
  value TEXT NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY (agent, source)
);
```

Where a source can deliver the same fact twice (an SSE stream after reconnect, a
paged readback that overlaps the live stream), the plugin sets `native_id` and the
Store drops the repeat. A source with neither a cursor nor stable ids does not replay:
after a reconnect the plugin emits `source.gap` and continues with new facts only.
Silence or a gap never becomes a finished turn.

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
                "multi_session": false, "start_requires_input": false,
                "child_environment": false },
  "proxy": "config"
}
```

`available`: the plugin found a usable native harness (binary present and runnable);
`launch` preflight requires it. `proxy`: `config` | `env` | `none` — how the plugin
points its harness at a proxy (details on its harness page). `start_requires_input`:
the harness creates a session only together with the first input.
`child_environment`: the plugin can give a native child its own environment before
the child's tools run. Method names on this page belong to the plugin connection; they
are separate from client API methods of the same name. The host routes only the
declared verbs. Harness versions are for display; they are never a gate.

## Verb translation rules

1. Translate the semantic verb to exactly one native mechanism and say which in
   `detail.mechanism` (e.g. `turn/steer`, `hook:additionalContext`, `queued:next_send`).
2. If the native side cannot express the verb faithfully, return `rejected` with
   `detail.reason`. Never substitute a weaker action silently (e.g. `send` for `steer`).
3. `unknown` when the request may have reached the native side but no reply arrived.
   The plugin must not retry it.
4. Report `applies` for `configure` and for mechanisms that land later
   (`next_tool` = after the current tool call).
5. `steer` and `interrupt` act only on `turn_id`. If the native current turn is a
   different one, return `rejected` (`stale_turn`); a steer that has not landed when
   its turn ends is `failed` (`turn_ended`), never delivered into the next turn.
6. Approval/question ids are native ids passed through unchanged as `request_id`.
7. Children: when the harness spawns a subagent, call `child` once, then send that
   child's events with the returned `agent_id`.
8. `send` is `applied` when the native side started (or joined) a turn with that
   input — not when an inbox or HTTP endpoint accepted it. Name the command ids in
   `turn.started.commands`; until then the command stays `dispatched`.

## Event normalization rules

- Start reading the native stream before writing the first input, so no early event
  is lost.
- Session lifecycle: `session.ready` once the native session accepts input;
  `session.ended` when it ends normally; `session.error` when the native side reports
  a fatal error while still running; `session.lost` when the native process exits
  unexpectedly (an exit is never reported as `session.error`).
  The host derives `agent.*` state events; plugins never send `agent.*`.
- Emit `turn.started` / `turn.completed` exactly once per native turn; `status`
  distinguishes completed, interrupted and failed. Non-terminal native statuses are not
  turn ends. Send the turn's final `message.agent` before its `turn.completed`.
- `message.agent` carries the final text of an assistant message (bounded); streaming
  pieces go to `message.delta` (ephemeral).
- `tool.*` summaries are short (`"bash: cargo test -p foo"`); full output stays native.
- `native_id` is unique per agent: when one native item yields several events
  (`tool.started` and `tool.completed`), use `<item id>:<kind>`.
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
(`allow` for observation-only hooks, `ask` for decision hooks) and exits 0; a decision
the daemon reaches after that is discarded and never applied to a later request. If
the daemon is unreachable the shim tries at most three times (0, 100, 400 ms); after
that the hook event is dropped and counted in `source.health` — there is no durable
queue. SDK-based plugins (Claude Agent SDK) use in-process hook callbacks instead of
the shim.

## Transcripts and discovery

Plugins with `features.transcripts` can tail native transcript files (e.g. Codex
rollout JSONL, Claude project JSONL) and emit the same normalized events. This is how
Eliot watches sessions it did not launch (`origin=external`, `observe_only=true` until
an `attach --control` succeeds). The read offset is the event's
[cursor](#cursors-and-replay); the host does not parse transcripts.

Tailing a file that is still being written: only a final line without a newline is a
torn write and waits for more bytes; a complete line that fails to parse becomes a
`source.gap` event, never a silent drop. On Windows open, read and close the file on
each poll — a held handle blocks the harness from rotating or replacing it.

`discover` and long history reads are paged; a plugin never builds one reply larger
than the frame limit.

## Lifecycle and errors

- The host starts a per-agent plugin for `agent.start` or `agent.attach` and a
  `multi_session` plugin on first use; it sends `initialize` first. Starting a plugin
  or a service is single-flight per name (two launches share one start); a stop during
  a start cancels it and a late start result is discarded.
- If a per-agent plugin exits, the host kills and verifies the rest of that agent's
  process tree and the agent becomes `lost` (an open `stop` then counts as applied);
  `resume` starts a new plugin. A `multi_session` plugin is restarted with backoff
  (1 s → 30 s); it re-attaches every live agent (`not_found` → `session.lost`) and the
  host asks `agent.snapshot` for open commands.
- **A plugin exits when its stdin closes** and ends the native processes it launched.
  Closing stdin is a request, not a kill: if a plugin or native process is still alive
  after the stop timeout (10 s), the host kills its process tree and verifies the
  recorded `(pid, start_time)` identities. On Windows the Job object makes this hold
  even if the daemon itself dies; on Unix it is best effort
  ([monitoring](monitoring.md#process-monitor)).
- Windows: only the child's ends of its stdio pipes are inheritable, and they are
  passed in an explicit handle list (`PROC_THREAD_ATTRIBUTE_HANDLE_LIST`, which
  requires inheritable handles) via `CreateProcessW` in `eliot-daemon::platform`;
  the parent's ends are never inheritable. Otherwise an unrelated child that
  inherited a write end keeps the pipe open and stdin EOF never arrives. `std::process`
  has no handle-list API.
- A plugin that fails `initialize` three times in a row is marked `failed`; its agents
  become `lost`. Other plugins are unaffected.
- Requests time out (default 30 s, `agent.start` 120 s). A deadline bounds the whole
  reply, not each read (a peer trickling bytes must not extend it). A timed-out command
  becomes `unknown` and is never resent. A parent deadline is always longer than the
  whole budget of what it waits for; no wait on a child process is unbounded — every
  wait ends in kill-and-verify.
- A child's exit and its last stdout line arrive in either order: finish a turn or
  request only after stdout reached EOF (or the terminal event arrived), never on the
  exit signal alone.
- Bound every native payload the plugin forwards (default 1 MiB per message); an
  oversized item becomes an explicit `source.gap`, never a reconnect loop.
- The host reads every plugin's stdout continuously. Replies and plugin requests
  (`child`, `event`) are handled on a path that never waits for subscribers, so an
  approval is never stuck behind telemetry. Under pressure ephemeral events are
  dropped (subscribers see `events.lagged`); persisted events never are — the plugin
  waits for their replies instead.
- A plugin must not block its own stdin reader on native I/O: a slow native call must
  not delay `interrupt` or `stop`.

## Fake plugin

`plugins/fake` simulates a harness deterministically; it is used by every L1 test and
by load tests. It is `multi_session`. It declares `launch`, `send`, `steer`, `inject`,
`interrupt`, `stop`, `resume`, `approve`, `deny`, `answer`, `configure` (no native
goal, children are created by the `spawn` step), unless `[plugins.fake] verbs = [...]`
narrows them. `start_requires_input = true` in its config makes it declare that feature
and accept the first input only through `agent.start.initial`. It reports
`system_note: native` and `configure` applies `now`.

Any input text (linked launch prompt, `send`, `steer`, `inject`) is a script if it
parses as `{"steps": [...]}`; otherwise it is echoed (`message.agent` with the same
text). `inject` shows up as `message.user {injected: true}` at the next step boundary
of a running turn, or at the start of the next turn when idle. A `send` script runs as
one turn: `turn.started` … `turn.completed`.

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
process: after the process dies, `agent.attach` answers `not_found`. `resume` opens a
new idle session under the same `native_ref`; resuming never replays earlier steps.

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
  "scenario"}}`. Secrets are removed before commit (the recorder drops auth headers,
  env and Eliot tokens).

`eliot-plugin` provides both transports for Rust plugins; external plugins implement
the same file format.

## Conformance kit

`eliot plugin-test <cmd...> [--replay-dir DIR]` starts the plugin with a temp data dir
and checks, for the verbs the manifest declares:

1. `initialize` returns a valid manifest;
2. `agent.start` → `session.ready` → a turn with `turn.started` / `turn.completed`; a
   launch prompt arrives as a separate linked send (or, with `start_requires_input`,
   as `initial`) and is settled by `turn.started.commands`;
3. `send` arrives only while the agent is idle (the host queues sends for busy
   agents) and starts exactly one turn;
4. `steer`, `interrupt`, `approve`/`deny`, `answer`, `goal.*`, `child` — each declared
   verb produces its documented events; a `steer`/`interrupt` with a stale `turn_id`
   is rejected; each undeclared verb is never sent;
5. closing stdin ends the plugin and its native processes;
6. a killed and restarted plugin re-attaches or reports `not_found`; given the cursors
   of the last answered `event`, it re-emits only the events that were never answered
   (replay scenario with an `exit` line; plugins with replayable sources);
7. a request without a reply is reported `unknown` and not retried.

For real harnesses the kit runs in replay mode against `plugins/<name>/fixtures/`;
for the fake plugin it runs live. Every plugin, in any language, passes it before merge.
