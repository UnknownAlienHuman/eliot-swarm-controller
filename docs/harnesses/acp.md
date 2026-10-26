# ACP agents (generic plugin)

Layer L4.6. Plugin: `plugins/acp` (Rust, `agent-client-protocol` crate). One plugin
covers every agent that speaks the Agent Client Protocol; the profile names the
command.

Facts checked 2026-10-25 against [agentclientprotocol.com](https://agentclientprotocol.com)
([transports](https://agentclientprotocol.com/protocol/v1/transports.md),
[schema](https://agentclientprotocol.com/protocol/v1/schema.md),
[agents](https://agentclientprotocol.com/overview/agents)) and
[agentclientprotocol/agent-client-protocol releases](https://github.com/agentclientprotocol/agent-client-protocol/releases)
(schema v1.26.0; Rust crate v1.12.0; v2 is a draft announced 2026-07-20 — implement v1).

## Transport

JSON-RPC over stdio, one message per line, agent logs on stderr. Eliot is the ACP
**client**; the agent is a subprocess — one plugin process and one agent subprocess
per Eliot agent. `initialize` negotiates `protocolVersion: 1` and capabilities.
`child_environment` is `false` unless the agent exposes children as separate
processes the plugin can give their own environment.

## Verb mapping

| Eliot verb | ACP v1 | Notes |
| --- | --- | --- |
| `launch` | `initialize` → `session/new` (cwd) → `session.ready`; the prompt is the linked send | `native_ref` = session id. `start_requires_input: false`. Install the update/response routing before `session/new`, `session/load` and the first prompt (see pitfalls). |
| `send` | `session/prompt` | One prompt at a time; the request completes at turn end — `applied` when its first update arrives, `turn.completed` at the response. Arrives only when idle (Eliot queues). |
| `steer`, `inject` | not in v1 → `rejected` (`inject` may prepend to next prompt) | |
| `interrupt` | `session/cancel` | Queued-input cancel, turn cancel and session close are different things. |
| `stop` | `session/close` if supported, then end the process | |
| `resume` | `session/load` / `session/resume` when the agent advertises them | Never resend the old prompt; a prompt is a linked send. Updates replayed by `session/load` are history, not new turns: the plugin marks them with `native_id` or does not forward them. |
| `approve` / `deny` | answer `session/request_permission` with one of the offered option ids | Use the agent's option ids; never invent "allow". |
| `configure` | `session/set_config_option` with typed selectors (`model`, `thought_level` for effort, `mode`), confirmed by readback; `session/set_mode` for mode when advertised | |
| `goal.*` | Eliot Goal loop | |

Events come from `session/update` notifications (message chunks, tool calls and
updates, plans); map them to `message.delta`, `tool.*`, `plan.updated`. Exact update
variants: take from the schema crate, not from this page.

## Profiles

The agent binary is the profile's `cmd` ([core](../spec/core.md#profile)), e.g.
`[profiles.gemini] harness = "acp"`, `cmd = ["gemini", "--acp"]`.

| Agent | `cmd` (from the ACP registry, 2026-10-25) |
| --- | --- |
| Gemini CLI | `gemini --acp` (v0.65.0; `--experimental-acp` is the deprecated name; docs lag, see google-gemini/gemini-cli#29010) |
| Claude (adapter) | `npx @agentclientprotocol/claude-agent-acp` — prefer the native [Claude plugin](claude-code.md) |
| Codex (adapter) | `npx @agentclientprotocol/codex-acp` — prefer the native [Codex plugin](codex.md) |
| GitHub Copilot CLI | `copilot --acp` (public preview) |
| Others | see the [registry](https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json) |

Gemini CLI headless without ACP (`gemini -p ... --output-format stream-json`, events
`init`, `message`, `tool_use`, `tool_result`, `error`, `result`; exit codes 0/1/42/53;
`--approval-mode default|auto_edit|yolo|plan`) is a fallback if ACP misbehaves.

## v1 pitfalls

- `session/prompt` success means the turn finished, not that it succeeded at the task.
- Install the response route before sending `session/load`, or early updates are lost
  (ACP Rust SDK issues #323, #214).
- ACPX ([donors](../reference/donors.md)) may replace this plugin's backend; it must
  not run beside it.
