# Codex (app-server)

Layer L2 — first real harness ([D-13](../decisions.md)). Plugin: `plugins/codex`
(Rust, built in as `eliot plugin codex`). Its manifest uses
`session_mode = "per_agent"`: one small plugin process and one app-server process per
live Eliot agent.

Facts checked 2026-10-25 against `openai/codex` `main` and release
`rust-v0.164.0` (2026-10-23). The app-server is experimental upstream; generate its
schema from the installed binary before implementing or recording fixtures.

## Sources

- Protocol source: [app-server-protocol `common.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/common.rs),
  [v2 `thread.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs),
  [v2 `turn.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/turn.rs),
  [`rpc.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/rpc.rs)
- Docs: [app-server](https://learn.chatgpt.com/docs/app-server),
  [hooks](https://learn.chatgpt.com/docs/hooks),
  [config reference](https://learn.chatgpt.com/docs/config-file/config-reference),
  [advanced config](https://learn.chatgpt.com/docs/config-file/config-advanced)
- Installed schema: `codex app-server generate-json-schema`,
  `codex app-server generate-ts` (experimental commands).

## Transport and custody

- Launch `codex app-server --listen stdio://`. Upstream also supports `unix://` and
  `ws://IP:PORT` for service mode.
- Stdio framing is newline-delimited JSON. It is JSON-RPC-shaped (`id`, `method`,
  `params`, `result`/`error`) **without** the `"jsonrpc":"2.0"` field; do not use a
  strict JSON-RPC 2.0 codec that requires it.
- Handshake: request `initialize` with `clientInfo` and `capabilities`
  (`experimentalApi: true` is needed for child filters), then send the initialized
  notification named by the generated schema.
- One app-server can host many Codex threads, but tools in all threads inherit that
  process's environment. Eliot needs a distinct generation token, Git identity,
  workspace defaults and account/profile environment per agent. Therefore L2 uses one
  app-server per Eliot agent inside that agent's Job/process group. Do not multiplex
  Eliot agents through one app-server merely because the protocol permits threads.
- L7 service mode, when implemented, also uses one endpoint/service per agent unless an
  upstream protocol later proves per-thread environment isolation. A daemon restart
  may re-attach the same thread/generation; a new resume increments Eliot generation.

## Authentication

The app-server reuses the login under its `CODEX_HOME` (ChatGPT login or API key).
Logging in is an owner action done once per account; Eliot never runs a login flow.

The token-sharing route (SIWC, owner-supplied OpenAI guide, not live-checked) uses
short-lived access tokens. A refreshed token reaches this per-agent app-server through
restart plus exact `thread/resume`. Usage or 5xx errors never delete credentials.

v1 attached to a shared owner-run WebSocket server. v2 L2 owns a per-agent stdio
app-server; service attachment returns only in L7 with exact endpoint/thread custody.

## Verb mapping

Every call below is additionally bound by Eliot `agent_id + generation`. Turn controls
also carry the exact current Codex turn id.

| Eliot verb | Codex | Notes |
| --- | --- | --- |
| `launch` | `thread/start`, then optional `turn/start` for prompt | `thread/start` has no caller correlation/idempotency key. After the thread is established, emit Eliot `session.ready` with native thread id/effective model/provider/cwd **before** returning start success. Compare effective model/provider/cwd with the request; a mismatch fails the launch with readback attached rather than adopting substituted settings. A lost reply without `session.ready`/readback is `unknown`; never start a second thread to retry. |
| `send` | `turn/start` | Eliot dispatches only when idle and only for the queued command's current generation. Map `command_id` to `clientUserMessageId` where supported for correlation, never as proof of idempotency. |
| `steer` | `turn/steer` with `expectedTurnId` | Reject without an active exact turn. Track it from `turn/started`; if readback is required, page to the end of thread turns. A goal continuation or completed turn makes the old id stale; do not retry against the new turn. |
| `inject` | `turn/steer` while busy; prepend to next `turn/start` while idle | Return the actual mechanism. Investigate the SDK `ExternalMessage` type for untrusted peer text; do not claim app-server support until recorded. |
| `interrupt` | `turn/interrupt` for the bound active turn | No blind retry. Snapshot/readback may settle applied only when that exact turn ended interrupted; the same turn still active proves not applied. |
| `stop` | interrupt exact active turn if any; `thread/goal/clear`; close the thread if supported; then gracefully stop this agent's app-server process group | Clearing the native goal is mandatory because v1 observed stopped sessions waking. The plugin reply waits until the bound generation is ended. `force` terminates only this per-agent group. No retry after unknown. |
| `resume` | new per-agent app-server, then `thread/resume` by retained thread id | Eliot has already incremented generation and rotated token. Never re-send previous input. |
| `fork` | `thread/fork` | New native thread becomes a new Eliot agent/generation if retained independently. |
| `approve` / `deny` | reply to command/file/permissions approval request | `approve` → `accept`; `approve --remember` → `acceptForSession`; `deny` → `decline`. `cancel` that ends a turn is used only as part of exact interrupt semantics. |
| `answer` | reply to `item/tool/requestUserInput` | Bind exact request id. Unknown server requests are answered with a typed error/decline and exposed as `x.codex.request`, never empty `{}`. |
| `goal.*` | `thread/goal/set` and `thread/goal/clear` | On idle, goal set may start work immediately, so apply `configure` first. Pause/resume use native status. Native goal status maps to Eliot goal status; active native goal drives itself, so an `on_idle` Eliot goal waits. |
| `configure` | supported per-turn fields on `turn/start`; session settings apply next generation/session | Generate schema to determine exact accepted fields. Report `applies`. |

## Permissions

| Eliot `permissions` | Codex `sandbox_mode` | Codex `approval_policy` |
| --- | --- | --- |
| `read-only` | `read-only` | `never` |
| `workspace-write` | `workspace-write` | `never` |
| `ask` | `workspace-write` | `on-request` |
| `full` | `danger-full-access` | `never` |

Pass per thread where the installed schema allows it, otherwise as launch-time config
overrides. Confirm exact enum names from the generated schema.

## Events and source ownership

The app-server stream is authoritative for controlled live session/turn/message/tool
and request families. Do not tail the rollout concurrently to publish the same events.
Rollouts are for discovery, observe-only attachment or explicit recovery after a
recorded stream boundary.

| Codex notification | Eliot event |
| --- | --- |
| established `thread/started` / start readback | `session.ready` with thread id (once per Eliot generation) |
| `turn/started` / `turn/completed` | `turn.started` / `turn.completed` with exact turn id |
| `item/started` / `item/completed` | `tool.*`, `message.agent`, `file.changed`, `plan.updated` by item type |
| `item/agentMessage/delta`, command output delta | ephemeral `message.delta` |
| approval / user-input requests | `approval.requested` / `question.asked` with native request id |
| `serverRequest/resolved` | matching resolution event |
| `error` | failed turn, provider limit, or fatal session error according to native scope |
| `collabAgentToolCall`, `subAgentActivity` | child registration using exact parent/child thread ids |

A plugin reconnect suppresses native ids already committed. If upstream replay/cursor
semantics cannot prove the boundary, report a source gap rather than emitting old
`turn.completed` again.

Children can be discovered with `thread/list` filters `parentThreadId` /
`ancestorThreadId` and `sourceKinds subAgent*` (experimental). `thread/list` without
those filters can hide subagent threads. Pagination is not an atomic snapshot and
notifications cover only the current connection, so family coverage may be partial and
must be labelled as such.

## Observation without control

- Route by exact thread id and parent thread id, never by count/title/cwd/time window.
  Whether a root stream includes child-thread events remains unverified; test in L2.6
  and read child threads independently when needed.
- Observe-only attach uses `thread/read`/rollouts and never calls `thread/resume`, goal,
  fork or send; those can start billed work.
- Rollouts normally live under
  `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<timestamp>-<thread-id>.jsonl`, archived
  under `archived_sessions/`. The format is internal; parse defensively.
- `thread/read` is neither resume nor a live subscription. Connection health is not
  inferred from time since the last model event.
- `notify = [...]` reportedly runs on turn completion and may be ignored in app-server
  mode (openai/codex#13019, not revalidated). Eliot does not need it while it owns the
  app-server stream.

## Hooks

Codex documents `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PreCompact`,
`PostCompact`, `UserPromptSubmit`, `SubagentStart`, `SubagentStop`, `Stop`,
`Interrupt`, `SessionStart`, `SessionEnd`. L2 does not use them because the owned
app-server stream already carries the required facts. Add a hook only for a proven
missing fact, assign it that event family, and configure it session-scoped; never let
it duplicate stream lifecycle.

## Proxy (OpenCodex or another Responses-compatible endpoint)

```toml
model_provider = "eliot-proxy"
[model_providers.eliot-proxy]
name = "eliot-proxy"
base_url = "http://127.0.0.1:10100/v1"
env_key = "OPENCODEX_API_KEY"
wire_api = "responses"
```

- Project `.codex/config.toml` cannot set `model_provider`/`model_providers`. Start the
  per-agent app-server with config overrides so the owner's global config is not
  edited.
- OpenCodex expects its client key in `x-opencodex-api-key`. Initially configure both
  bearer `env_key` and `env_http_headers` from the named environment variable; remove
  any unnecessary path after L2.8 records the real request.
- Prefer a named provider over global `openai_base_url`, so the effective route is
  explicit and can be read back.
- A profile's proxy cap counts all live per-agent app-servers using it.

## v1 failures retained as tests

- Lost `thread/start` reply: outcome unknown; no replacement thread.
- `clientUserMessageId` is correlation, not idempotency (upstream proposal
  openai/codex#32254 remains open at the checked revision).
- History/readback failure remains unknown; it does not authorize replay.
- Long-thread turn lookup must page to the end; v1 read only the first 20.
- Effective settings are read back; a server-substituted model/provider/cwd is not
  silently adopted.
- Settings such as `agents.max_concurrent_threads_per_session` are applied at
  `thread/start`.
- A stopped generation clears native goal before process shutdown.
- A stale plugin message/turn id cannot complete or interrupt a resumed generation.

Useful protocol details may be read from
`git show v1-final:crates/swarm-adapter-codex/src/lib.rs`; do not port its journals,
proof packets or checkpoint machinery.

## Recording scenarios

`just record-codex` records the cheapest-model scenarios into
`plugins/codex/fixtures/`:

- launch + one turn; two turns; steer; interrupt;
- approve/deny command and file; user input;
- native goal set/pause/resume/clear;
- subagent; resume; bad model/effective-setting mismatch;
- wrong generation, stale turn and stale request (must reject before native I/O);
- stream reconnect with duplicate events and an unbridgeable gap;
- replay-only `no-reply` and process `exit`/reattach cases.

## Unverified until recordings

- Exact initialized-notification name and start/turn setting fields in the installed
  generated schema.
- Whether command-line `-c` overrides reach app-server exactly as expected. If not,
  use supported per-thread config; only if neither works may the Codex proxy route fall
  back to OpenCodex's documented shim, and the plugin must report that different
  mechanism.
- Whether `turn/steer` is accepted while an approval is pending.
- Whether app-server exits promptly when stdin closes on all supported platforms.
- Exact thread-close operation, if any; otherwise graceful stop ends the per-agent
  app-server after interrupt + goal clear.
