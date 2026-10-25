# Plugin protocol

A plugin is an executable that speaks this protocol on stdin/stdout. Built-in Rust
plugins run as `eliot plugin <name>`; external plugins are any command declared in
config (`[plugins.<name>] cmd = ["node", "plugins/claude/main.mjs"]`). One plugin
process serves many agents of one harness kind.

## Framing

- JSON-RPC 2.0, one JSON object per line on stdout/stdin, UTF-8, no BOM.
- stderr is free-form log text; the host captures it (bounded) into the daemon log.
- Both sides may send requests and notifications. Ids are unique per direction.
- Every agent-scoped message carries `agent_id`. The plugin keeps its own map
  `agent_id → native session`.
- Protocol version: `"eliot-plugin/1"`. Additive fields only within a version.

## Host → plugin requests

| Method | Params | Result |
| --- | --- | --- |
| `initialize` | `protocol`, `plugin_config` (from Eliot config), `data_dir` | `Manifest` (below) |
| `agent.start` | `agent_id`, `profile` (resolved), `workspace?`, `prompt?`, `env` (Eliot vars to inject), `system_note` ([text](coordination.md#communication-guidance-for-agent-prompts)), `parent_native_ref?` | `{native_ref, pids: [Pid], effective: {model?, permissions?, system_note: native|prepended, ...}}` |
| `agent.attach` | `agent_id`, `native_ref`, `observe_only` | `{state, pids}` — after daemon restart or for discovered sessions |
| `agent.command` | `agent_id`, `command_id`, `verb`, `args` | `{status: applied|rejected|failed|unknown, applies?: now|next_turn|next_session, detail?}` |
| `agent.snapshot` | `agent_id`, `commands?: [command_id]` | `{state, pending: [Request], goal?, usage?, commands?: {id: applied|not_applied|unknown}}` |
| `agent.stop` | `agent_id`, `force` | `{}` once the native session ended (or `force` killed it) |
| `discover` | `{}` | `[{native_ref, workspace?, started_at?, title?, live}]` — native sessions on this machine |
| `shutdown` | `{}` | `{}`; plugin exits after replying |

`Pid = {pid, start_time, role}`; `role` = `harness` | `server` | `tool` | `proxy`.

Verb routing: `launch` arrives as `agent.start`, `stop` as `agent.stop`, `resume` as
`agent.attach` followed by `agent.command {verb: resume}`; every other verb arrives as
`agent.command`.

## Plugin → host notifications

| Method | Params |
| --- | --- |
| `event` | `{agent_id?, kind, ts?, data}` — normalized kinds from [core](core.md#event-kinds) |
| `process` | `{agent_id, pid, start_time, role}` — register another native process |
| `log` | `{level, message}` |

## Plugin → host requests

| Method | Params | Result |
| --- | --- | --- |
| `child` | `{parent_agent_id, native_ref, name?, labels?}` | `{agent_id}` — the host creates an `origin=native` agent (idempotent per `native_ref`) |
| `decide` | `{agent_id, request: Request}` | `{decision: allow|deny|ask, reason?, context?}` — synchronous policy answer for approvals the plugin must answer inline (host runs approval rules with a timeout; default `ask`) |

## Manifest

```json
{
  "name": "codex", "version": "0.1.0", "protocol": "eliot-plugin/1",
  "harness_version": "codex-cli 0.x (reported by the native side, or null)",
  "verbs": ["launch","send","steer","inject","interrupt","stop","resume",
            "approve","deny","answer","configure","goal.set","goal.pause","goal.resume","goal.clear"],
  "child_verbs": ["send","interrupt"],
  "features": { "native_goal": true, "native_children": true, "hooks": false,
                "transcripts": true, "discover": true, "survives_daemon_restart": false },
  "permissions": { "read-only": "...", "workspace-write": "...", "full": "...", "ask": "..." },
  "proxy": { "env": ["OPENAI_BASE_URL"], "config": "model_providers.<name>.base_url" }
}
```

The host routes only the declared verbs. Harness versions are reported for display and
debugging; they are never a gate.

## Verb translation rules

1. Translate the semantic verb to exactly one native mechanism and say which in
   `detail.mechanism` (e.g. `turn/steer`, `hook:additionalContext`, `queued:next_send`).
2. If the native side cannot express the verb faithfully, return `rejected` with
   `detail.reason`. Never substitute a weaker action silently (e.g. `send` for `steer`).
3. `unknown` when the request may have reached the native side but no reply arrived.
   The plugin must not retry it.
4. Report `applies` for `configure`: many harnesses read settings only at session or
   turn start.
5. Approval/question ids are native ids passed through unchanged as `request_id`.
6. Children: when the harness spawns a subagent, call `child` once, then send that
   child's events with the returned `agent_id`.

## Event normalization rules

- Emit `turn.started` / `turn.completed` exactly once per native turn; `status`
  distinguishes completed, interrupted and failed.
- `message.agent` carries the final text of an assistant message (bounded); streaming
  pieces go to `message.delta` (ephemeral).
- `tool.*` summaries are short (`"bash: cargo test -p foo"`); full output stays native.
- Usage: send cumulative values with their scope (`turn` or `session`); never sum
  cumulative counters.
- Provider errors map to `provider.limited.kind`: `rate_limit` (retry soon), `quota`
  (window exhausted), `model_gone`, `auth`. A 429 is `rate_limit` unless the native
  error says quota.
- Unknown native events may be forwarded as `x.<harness>.<name>` for debugging.

## Hooks

For harnesses with native hooks (Claude Code, Antigravity, ...), the plugin writes a
**session-scoped** hook config (never the user's global settings) whose command is:

```text
eliot hook --harness <name> --event <HookName>
```

The shim reads the hook JSON from stdin, calls `hook.event` on the daemon with
`ELIOT_AGENT_ID`, and prints the decision in the harness's native hook output format.
The daemon turns the hook into events (`tool.started`, `approval.requested`, ...),
runs matching [approval rules](automation.md#approvals) and delivers pending `inject`
text as additional context when the hook allows it. If the daemon does not answer
within the hook timeout (default 2 s), the shim prints the configured default
(`allow` for observation-only hooks, `ask` for decision hooks) and exits 0.

## Transcripts and discovery

Plugins with `features.transcripts` can tail native transcript files (e.g. Codex
rollout JSONL, Claude project JSONL) and emit the same normalized events. This is how
Eliot watches sessions it did not launch (`origin=external`, `observe_only=true` unless
the plugin can attach for control). The plugin stores its read offsets in its own state
dir (`<data_dir>/plugins/<name>/`); the host does not parse transcripts.

## Lifecycle and errors

- The host starts a plugin on first use, sends `initialize`, and restarts it with
  backoff (1 s → 30 s) if it exits. On restart it re-attaches every live agent and asks
  `agent.snapshot` for open commands.
- A plugin that fails `initialize` three times is marked `failed`; its agents become
  `lost`. Other plugins are unaffected.
- Requests time out (default 30 s, `agent.start` 120 s). A timed-out command becomes
  `unknown`.

## Conformance kit

`eliot plugin-test <cmd...>` (L1) runs a scripted suite against any plugin with a
mock or recorded native side: initialize, start, send, steer/interrupt if declared,
unsupported verb rejection, child registration, crash/restart re-attach, unknown
outcome handling. Every plugin, in any language, must pass it before merge.
