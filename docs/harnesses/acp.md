# ACP agents (generic plugin)

Layer L4.6. Plugin code: `plugins/acp` (Rust, `agent-client-protocol` crate).
The same implementation supports every ACP agent, but its manifest uses
`session_mode = "per_agent"`: the daemon starts one plugin instance and one configured
ACP subprocess per Eliot agent/generation. A process failure or `stop --force` can
therefore affect only that agent.

Facts checked 2026-10-25 against [agentclientprotocol.com](https://agentclientprotocol.com)
([transports](https://agentclientprotocol.com/protocol/v1/transports.md),
[schema](https://agentclientprotocol.com/protocol/v1/schema.md),
[agents](https://agentclientprotocol.com/overview/agents)) and
[agentclientprotocol/agent-client-protocol releases](https://github.com/agentclientprotocol/agent-client-protocol/releases)
(schema v1.26.0; Rust crate v1.12.0; v2 is a draft announced 2026-07-20 — implement v1).

## Transport and binding

ACP v1 is JSON-RPC over stdio, one message per line, with logs on stderr. Eliot is the
ACP client; the configured agent is a child subprocess in the per-agent custody group.
`initialize` negotiates `protocolVersion: 1` and capabilities.

The plugin binds one ACP session to one Eliot `(agent_id, generation)`. Every prompt,
cancel, permission response and update is checked against that generation. Native
session/request ids pass through unchanged. A restarted/resumed ACP process is a new
Eliot generation even when `session/load` reuses the native session id.

## Verb mapping

| Eliot verb | ACP v1 | Notes |
| --- | --- | --- |
| `launch` | `initialize` → `session/new` (cwd) → optional `session/prompt` | Emit `session.ready` with the exact session id/effective capabilities before returning success. A lost reply is read back if possible, never retried by creating another session. |
| `send` | `session/prompt` | One prompt at a time; the request returns at turn end. Eliot dispatches only when idle and for the current generation. |
| `steer` | unsupported in v1 | Reject before native I/O. |
| `inject` | prepend to the next prompt only | Report `mechanism=queued:next_send`; it never claims current-turn delivery. |
| `interrupt` | `session/cancel` for the bound active prompt/turn | Queued-input cancellation, active turn cancellation and session close are different operations. Never retry after unknown. |
| `stop` | exact `session/close` when advertised, then close this agent's process; force kills only its custody group | Reply only after the bound generation ended. |
| `resume` | new ACP process + `session/load` / `session/resume` when advertised | Eliot generation increments; old input is never resent. |
| `approve` / `deny` | answer `session/request_permission` with one offered option id | Bind the exact request; never invent an `allow` value or choose by position. |
| `answer` | only when the negotiated schema advertises a matching user-input request | Otherwise unsupported. |
| `configure` | `session/set_config_option` with typed selectors; `session/set_mode` where advertised | Confirm by readback and report `applies`. |
| `goal.*` | Eliot Goal loop | ACP v1 has no generic native goal. |

`session/update` notifications provide message chunks, tool calls/updates and plans.
Map exact schema variants to `message.delta`, `tool.*` and `plan.updated`. The active
`session/prompt` request and its terminal response define one turn; a process exit or
silence alone does not.

## Profiles

The agent binary is the profile's `cmd`, for example:

```toml
[profiles.gemini-acp]
harness = "acp"
cmd = ["gemini", "--acp"]
```

| Agent | `cmd` from ACP registry/docs (checked 2026-10-25) |
| --- | --- |
| Gemini CLI | `gemini --acp` (v0.65.0; `--experimental-acp` is deprecated) |
| Claude adapter | `npx @agentclientprotocol/claude-agent-acp` — prefer the native Claude plugin when its extra controls are needed |
| Codex adapter | `npx @agentclientprotocol/codex-acp` — prefer the native Codex plugin when its extra controls are needed |
| GitHub Copilot CLI | `copilot --acp` (public preview) |
| Others | ACP registry |

A non-ACP headless route such as `gemini -p --output-format stream-json` is **not** an
automatic fallback. If it is ever needed, implement and select it as a separate
harness/profile with its own recordings and capability declaration. A failed ACP
session must fail visibly; Eliot never silently switches protocol or semantics.

## Source ownership and recovery

The live ACP connection owns session/turn/message/tool/request lifecycle for its
controlled generation. Do not publish the same lifecycle from logs or transcript
files beside it. Install the response/update route before `session/load` or prompt so
early updates cannot be lost. If reconnect/load cannot prove a complete cursor
boundary, expose a source gap rather than replaying an old completion.

A successful `session/prompt` response means the native turn ended, not that the task
succeeded. Final text/status remain native facts; Eliot acceptance/workflow is outside
the core.

## v1 pitfalls retained for L7 qualification

- `session/prompt` completion is not product success.
- The response route must exist before `session/load` (ACP Rust SDK issues #323,
  #214).
- Stale generation or permission ids must be rejected before native I/O.
- Unknown cancel/close is never replayed against a replacement process.
- ACPX may replace this plugin's backend only by deleting the old backend in the same
  PR; two concurrent owners of one ACP session are forbidden.
