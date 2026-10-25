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
- One app-server process hosts many threads. Topology: one app-server per distinct
  set of launch-time config overrides (normally one per proxy route); agents whose
  profiles resolve to the same overrides share it. `cwd` is per thread.

## Auth

The app-server reuses the existing Codex login of its `CODEX_HOME` (ChatGPT login or
API key). Logging in is the owner's job, done once per account with the Codex CLI;
Eliot never runs a login flow.

The token-sharing route (SIWC, owner-supplied OpenAI guide, not live-checked) uses
access tokens of about 1 h; a refreshed token reaches the app-server only through a
restart plus `thread/resume`. A usage or 5xx error never deletes credentials.

v1 attached to an owner-run, shared app-server (WebSocket). v2 L2 launches its own
over stdio; attaching to a long-lived server is the service mode of L7.6.

## Verb mapping

| Eliot verb | Codex | Notes |
| --- | --- | --- |
| `launch` | `thread/start` then `turn/start` with the prompt | `thread/start` has no caller correlation id: a lost reply is `unknown`; never start a second thread to "retry". Compare the effective model, provider and cwd in the `thread/start` response with the request before using the thread; a mismatch fails the launch with the readback attached (v1 adopted a server-substituted config and wedged). |
| `send` | `turn/start` (Eliot holds sends for a busy agent and dispatches them when it is idle) | |
| `steer` | `turn/steer` with the active `expectedTurnId` | Rejected if no active turn. Track the active turn id from `turn/started`; if it must be looked up, page through the thread's turns to the end (v1 read only the first 20 and missed it in long threads). A goal continuation starts a new turn and makes the old id stale: that steer fails `bad_state` and is not retried. |
| `inject` | `turn/steer` when busy; prepend to next `turn/start` when idle | Report mechanism. Hooks (`UserPromptSubmit`/`PostToolUse`) are an alternative if session-scoped hook config proves workable. The Codex SDK has an `ExternalMessage` input type for untrusted text from another agent or tool (grants no authority); check whether the app-server accepts it for messages from peers. |
| `interrupt` | `turn/interrupt` | |
| `stop` | interrupt active turn, `thread/goal/clear` if a goal is set, then close the thread / stop the app-server instance if it serves no other agent | v1 field failure: a server-side goal woke a stopped session. |
| `resume` | `thread/resume` | Never re-send the previous input. |
| `fork` | `thread/fork` | |
| `approve` / `deny` | reply to `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`: `approve` → `accept`, `approve --remember` → `acceptForSession`, `deny` → `decline` | `cancel` (ends the turn) is used only by `interrupt` while a request is pending. |
| `answer` | reply to `item/tool/requestUserInput` | Any other server request (elicitation, dynamic tool calls, unknown methods) is answered with an error or decline — never with an empty `{}` — and reported as `x.codex.request`. |
| `goal.*` | `goal.set` → `thread/goal/set` (on an idle thread this starts a turn at once — send `configure` first); `goal.pause`/`goal.resume` → `thread/goal/set` with `status` `paused`/`active`; `goal.clear` → `thread/goal/clear` | Needs the Goals feature and an allowed provider. An active native goal continues by itself when the thread goes idle (Eliot goal `on_idle` rounds therefore pause). Status → `goal.updated.status`: active → `active`, paused → `paused`, complete → `done`, blocked / usage-limited / budget-limited → `blocked` (`reason` = native status), cleared → `cleared`. |
| `configure` | per-turn settings on `turn/start`; session-level settings `applies: next_session` | Check which fields `turn/start` accepts in the schema. |

## Permissions

| Eliot `permissions` | Codex `sandbox_mode` | Codex `approval_policy` |
| --- | --- | --- |
| `read-only` | `read-only` | `never` |
| `workspace-write` | `workspace-write` | `never` |
| `ask` | `workspace-write` | `on-request` (requests surface as `approval.requested`) |
| `full` | `danger-full-access` | `never` |

Passed per thread on `thread/start` when the schema allows it, else as launch-time
overrides. Value names are from the long-standing config keys; confirm them in the
generated schema of the installed version.

## Events

| Codex notification | Eliot event |
| --- | --- |
| `thread/started` | `session.ready` (`native_ref` = thread id) |
| `turn/started` / `turn/completed` | `turn.started` / `turn.completed` (+ usage) |
| `item/started` / `item/completed` | `tool.*`, `message.agent`, `file.changed`, `plan.updated` by item type |
| `item/agentMessage/delta`, `item/commandExecution/outputDelta` | `message.delta` (E) |
| approval / user-input requests | `approval.requested` / `question.asked` |
| `serverRequest/resolved` | `approval.resolved` / `question.answered` |
| `error` | `turn.completed status=failed` or `provider.limited` by error content |
| items `collabAgentToolCall` (`receiverThreadIds`), `subAgentActivity` (`agentThreadId`) | `child` registration |

Children: discover with `thread/list` filters `parentThreadId` / `ancestorThreadId`
(experimental) and `sourceKinds` `subAgent*`. `thread/list` without filters hides
subagent threads (v1 observation). `thread/list` pages are not an atomic snapshot and
lifecycle notifications cover only the current connection: family coverage can be
partial — show it as such.

## Observation without control

- Route events by thread id; attribute children by their exact parent thread id,
  never by counting. Whether events of subagent threads arrive on the root's stream is
  unverified (v1 research says a root subscription does not include them) — check in
  L2.6 and read child threads explicitly if needed. `thread/read` is neither resume nor
  subscription. Connection health is a separate check, not "time since the last
  event".
- Observe-only attach reads (`thread/read`, rollouts) and never calls
  `thread/resume`, `goal.set`, `fork` or `send` — each of those can start billed work.
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

- Project-local `.codex/config.toml` cannot set `model_provider`/`model_providers`.
  Default: the plugin starts the app-server with config overrides
  (`codex -c model_provider=... -c model_providers.eliot-proxy.base_url=... app-server`),
  so the owner's global config is never edited.
- OpenCodex expects its client key in header `x-opencodex-api-key`. Default: set both
  `env_key = "OPENCODEX_API_KEY"` (bearer) and
  `env_http_headers = { "x-opencodex-api-key" = "OPENCODEX_API_KEY" }`; drop whichever
  OpenCodex does not need once the L2.8 live run shows it.
- `openai_base_url` (top level) overrides the built-in provider's URL; prefer a named
  provider so the proxy is explicit.

## v1 pitfalls

- A lost `thread/start` reply leaves the outcome unknown; do not create another thread.
- `clientUserMessageId` correlates input with history; it is not an idempotency key
  (upstream proposal openai/codex#32254 is open).
- `-32601` from history reads had no known cause; failed readback stays `unknown`.
- Settings like `agents.max_concurrent_threads_per_session` apply at `thread/start`.
- v1 code with protocol details: `git show v1-final:crates/swarm-adapter-codex/src/lib.rs`.

## Scenarios

`just record-codex` records these into `plugins/codex/fixtures/` (cheapest model, tiny
prompts in a scratch repo): `turn` (launch + one turn), `two-turns`, `steer`,
`interrupt`, `approve-command`, `deny-command`, `approve-file`, `user-input`, `goal`
(set, pause, resume, clear), `subagent`, `resume`, `bad-model` (error).

## Unverified

Facts the docs did not settle; the recordings answer them. Correct this page when they
do.

- Name of the client's initialized notification and the settings fields `turn/start`
  and `thread/start` accept (generated schema).
- Whether `-c` overrides reach `codex app-server`, or per-thread config is needed.
- Whether `turn/steer` is accepted while an approval is pending.
- Whether the app-server exits when its stdin closes.
