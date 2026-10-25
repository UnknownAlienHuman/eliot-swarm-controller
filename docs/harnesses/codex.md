# Codex (app-server)

Layer L2 — the first real harness ([D-13](../decisions.md)). Plugin: `plugins/codex`
(Rust, built in as `eliot plugin codex`).

Facts checked 2026-10-25 against `openai/codex` `main` (latest release
`rust-v0.164.0`, 2026-10-23) and learn.chatgpt.com docs. The app-server is marked
experimental upstream: re-check method names against the installed version with the
schema generator before coding.

## Sources

- Protocol source: [app-server-protocol `common.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/common.rs),
  [v2 `thread.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs),
  [v2 `turn.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/turn.rs),
  [`rpc.rs`](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/rpc.rs)
- Docs: [app-server](https://learn.chatgpt.com/docs/app-server),
  [hooks](https://learn.chatgpt.com/docs/hooks),
  [config reference](https://learn.chatgpt.com/docs/config-file/config-reference),
  [advanced config](https://learn.chatgpt.com/docs/config-file/config-advanced)
- Schema from the installed binary: `codex app-server generate-json-schema`,
  `codex app-server generate-ts` (experimental).

## Transport

- Launch `codex app-server` (default `--listen stdio://`; also `unix://`, `ws://IP:PORT`).
- Framing: newline-delimited JSON over stdio. It is JSON-RPC-shaped (`id`, `method`,
  `params`, `result`/`error`) **without** the `"jsonrpc": "2.0"` field; do not use a
  strict JSON-RPC 2.0 library that requires it.
- Handshake: request `initialize` with `clientInfo` and `capabilities`
  (`experimentalApi: true` is needed for child discovery filters), then the client
  initialized notification (exact name: check the generated schema).
- One app-server process can host many threads → one app-server per plugin instance
  (or per workspace if cwd/config must differ; decide in L2.1 after a test).

## Verb mapping

| Eliot verb | Codex | Notes |
| --- | --- | --- |
| `launch` | `thread/start` then `turn/start` with the prompt | `thread/start` has no caller correlation id: a lost reply is `unknown`; never start a second thread to "retry". |
| `send` | `turn/start` when idle; when busy, keep in the plugin queue and start at `turn/completed` | |
| `steer` | `turn/steer` with the active `expectedTurnId` | Rejected if no active turn. |
| `inject` | `turn/steer` when busy; prepend to next `turn/start` when idle | Report mechanism. Hooks (`UserPromptSubmit`/`PostToolUse`) are an alternative if session-scoped hook config proves workable. |
| `interrupt` | `turn/interrupt` | |
| `stop` | interrupt active turn, `thread/goal/clear` if a goal is set, then close the thread / stop the app-server instance if it serves no other agent | v1 field failure: a server-side goal woke a stopped session. |
| `resume` | `thread/resume` | Never re-send the previous input. |
| `fork` | `thread/fork` | |
| `approve` / `deny` | reply to `item/commandExecution/requestApproval` (`accept`, `acceptForSession`, `decline`, `cancel`), `item/fileChange/requestApproval` (`accept`, `acceptForSession`, `decline`, `cancel`), `item/permissions/requestApproval` | `remember` → `acceptForSession`. |
| `answer` | reply to `item/tool/requestUserInput` | |
| `goal.*` | `thread/goal/set`, `thread/goal/get`, `thread/goal/clear` (`status` active/paused for pause/resume) | Needs the Goals feature and an allowed provider. |
| `configure` | per-turn settings on `turn/start`; session-level settings `applies: next_session` | Check which fields `turn/start` accepts in the schema. |

## Events

| Codex notification | Eliot event |
| --- | --- |
| `thread/started` | `agent.ready` (`native_ref` = thread id) |
| `turn/started` / `turn/completed` | `turn.started` / `turn.completed` (+ usage) |
| `item/started` / `item/completed` | `tool.*`, `message.agent`, `file.changed`, `plan.updated` by item type |
| `item/agentMessage/delta`, `item/commandExecution/outputDelta` | `message.delta` (E) |
| approval / user-input requests | `approval.requested` / `question.asked` |
| `serverRequest/resolved` | `approval.resolved` / `question.answered` |
| `error` | `turn.completed status=failed` or `provider.limited` by error content |
| items `collabAgentToolCall` (`receiverThreadIds`), `subAgentActivity` (`agentThreadId`) | `child` registration |

Children: discover with `thread/list` filters `parentThreadId` / `ancestorThreadId`
(experimental) and `sourceKinds` `subAgent*`. `thread/list` without filters hides
subagent threads (v1 observation).

## Observation without control

- Rollouts: `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<timestamp>-<thread-id>.jsonl`
  (`CODEX_HOME` defaults to `~/.codex`; archived under `archived_sessions/`). Tail
  them for `discover` / `attach --observe-only`. The format is internal; parse
  defensively.
- `notify = [...]` in user config runs a command on `agent-turn-complete` only; it
  is reportedly ignored in app-server mode (openai/codex#13019, unverified). Not needed
  when Eliot owns the app-server.

## Hooks

Codex has hooks: `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PreCompact`,
`PostCompact`, `UserPromptSubmit`, `SubagentStart`, `SubagentStop`, `Stop`,
`Interrupt`, `SessionStart`, `SessionEnd`; configured in `config.toml` `[hooks]` or
`hooks.json` under `~/.codex/` or `<repo>/.codex/` (project hooks need a trusted
project), feature flag `[features] hooks`. Command hooks get JSON on stdin with
`session_id`, `transcript_path`, `cwd`, `hook_event_name`, `model`. L2 does not need
hooks (the app-server stream has the same facts); use them only if a fact is missing.

## Proxy (OpenCodex or any Responses-compatible endpoint)

```toml
model_provider = "eliot-proxy"
[model_providers.eliot-proxy]
name = "eliot-proxy"
base_url = "http://127.0.0.1:10100/v1"
env_key = "OPENCODEX_API_KEY"     # env var holding the key
wire_api = "responses"            # the only accepted value; "chat" is rejected
```

- Project-local `.codex/config.toml` cannot set `model_provider`/`model_providers`;
  the plugin passes them as config overrides at launch (`codex -c key=value`, or the
  app-server's per-thread config if available — verify in L2.6) so the user's global
  config is never edited.
- OpenCodex expects its client key in header `x-opencodex-api-key`; check in L2.6
  whether Codex's `http_headers` / `env_http_headers` provider keys are needed for
  that, or whether OpenCodex also accepts the bearer token.
- `openai_base_url` (top level) overrides the built-in provider's URL; prefer a named
  provider so the proxy is explicit.

## v1 pitfalls

- A lost `thread/start` reply leaves the outcome unknown; do not create another thread.
- `clientUserMessageId` correlates input with history; it is not an idempotency key
  (upstream proposal openai/codex#32254 is open).
- `-32601` from history reads had no known cause; failed readback stays `unknown`.
- Settings like `agents.max_concurrent_threads_per_session` apply at `thread/start`.
- v1 code with protocol details: `git show v1-final:crates/swarm-adapter-codex/src/lib.rs`.

## Verify during L2

- [ ] Exact initialized-notification name and `turn/start` settings fields (schema).
- [ ] Per-thread config overrides vs. `-c` flags for proxy wiring.
- [ ] Whether `turn/steer` is accepted while an approval is pending.
- [ ] App-server behavior when the plugin's stdin closes (does the process exit?).
