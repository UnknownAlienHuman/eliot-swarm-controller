# Claude Code

Layer L4.2. Plugin: `plugins/claude`, initially TypeScript on the Agent SDK.
Manifest mode is `per_agent`: one plugin/SDK query process group per Eliot agent
generation. This preserves session-specific environment, hook callbacks, Git identity
and exact force-stop custody. A later Rust port drives stream-json behind the same
protocol and conformance cases.

Facts checked 2026-10-25: docs page marker v2.1.309; npm
`@anthropic-ai/claude-code` 2.1.310 and `@anthropic-ai/claude-agent-sdk` 0.3.310;
PyPI `claude-agent-sdk` 0.2.174. The SDK is under Anthropic commercial terms, not an
open-source license; recheck terms/version when adding it.

## Sources

[headless](https://code.claude.com/docs/en/headless),
[CLI reference](https://code.claude.com/docs/en/cli-reference),
[hooks](https://code.claude.com/docs/en/hooks),
[Agent SDK TypeScript](https://code.claude.com/docs/en/agent-sdk/typescript),
[subagents](https://code.claude.com/docs/en/sub-agents),
[sessions](https://code.claude.com/docs/en/sessions),
[goal](https://code.claude.com/docs/en/goal),
[env vars](https://code.claude.com/docs/en/env-vars),
[LLM gateway](https://code.claude.com/docs/en/llm-gateway).

## Transport and binding

- SDK: `query({prompt: AsyncIterable<SDKUserMessage>, options})` keeps one native
  session open with streaming input. It runs the bundled Claude Code binary unless
  `pathToClaudeCodeExecutable` is set.
- Rust-port CLI: `claude -p --input-format stream-json --output-format stream-json
  --verbose`; optional `--include-partial-messages`, `--resume`, `--session-id`,
  explicit permission/settings flags.
- A custom Windows executable path must be an absolute `.exe`; do not pass `.cmd` or
  `.bat` shims where the SDK forbids them.
- Do not use `--bare` for a managed agent: it disables hooks/skills/MCP/instructions.
  Pass explicit settings so upstream default changes cannot silently remove controls.

The plugin binds the SDK query/session to Eliot `(agent_id,generation)`. All callbacks,
hooks, inputs and requests carry that generation. Resume increments Eliot generation
and rotates the token even when Claude reuses the native session id. Late callbacks
from the old SDK object are dropped from the normal bus.

## Verb mapping

| Eliot verb | Claude Code | Notes |
| --- | --- | --- |
| `launch` | `query()` with streaming input, cwd/model/permission mode | Read native session id/effective settings from init, emit `session.ready`, then return success. A pre-init error is definite only when no session/input was admitted; ambiguity is unknown and never relaunched blindly. |
| `send` | push `SDKUserMessage` to the input stream | Eliot dispatches only when idle/current generation. |
| `steer` | `additionalContext` returned by the next `PostToolUse` callback | Not exact in-turn immediate steer. Bind the current Eliot turn, report `applies=next_tool`; if the turn ends before another tool, do not apply it to a later turn without explicit readback/requeue semantics. Verify installed behavior before declaring support. |
| `inject` | `additionalContext` through assigned `UserPromptSubmit`/`PostToolUse` hook | Bounded by native hook limits; report actual safe point. |
| `interrupt` | `Query.interrupt()` for the bound active query/turn | No blind retry. Exact snapshot/session result must settle unknown. |
| `stop` | end input stream and close query; force terminates only this per-agent group | Reply after bound generation ended. Clear any native goal mechanism first if it can wake the session. |
| `resume` | new plugin generation with `options.resume=<session id>` | System prompt may not refresh; never resend previous user input. |
| `approve` / `deny` | exact `canUseTool` callback decision | `canUseTool` occurs only for prompts. Verify universal observation through assigned hooks; never answer a different current request. |
| `answer` | exact `AskUserQuestion` request through callback shape | Verify installed schema before support. |
| `configure` | live setter where documented; otherwise next turn/session option | Report `applies`; requested and effective values remain separate. |
| `goal.*` | native goal only after installed qualification; otherwise Eliot Goal loop | Native Stop-hook goal checks may spend model calls and are independent of Eliot goal. |

## Event/source ownership

The SDK message stream owns controlled session/turn/message/tool lifecycle. In-process
hooks own only synchronous decisions and facts not present in that stream; transcript
JSONL is for discovery/observe-only or explicit cursor recovery, not a concurrent
second producer.

| Native fact | Eliot |
| --- | --- |
| init | `session.ready` with session id/model/tools/permission mode |
| assistant/user messages | `message.agent` / `message.user`; tool lifecycle |
| partial `stream_event` | ephemeral `message.delta` with turn id |
| `parent_tool_use_id`, Subagent hooks | exact child registration/events |
| permission callbacks/assigned hooks | request/resolution events |
| Stop/StopFailure and result subtype | exact `turn.completed` or fatal `session.error` |
| Pre/PostCompact | harness-specific diagnostics unless core ownership is assigned |

Result subtype `success` maps completed; documented execution/budget/max-turn/output
errors map failed with subtype retained. An error before init may be launch failure;
a lost callback/transport after possible admission is unknown.

Subagents are spawned by the `Agent` tool (`Task` remains an alias in current docs).
Full text may require `forwardSubagentText=true`. A child is complete only from its
exact tool result/task notification, never silence. Cumulative resumed-session cost is
reported as `scope=session`, not summed as a turn delta.

## Hooks

The SDK exposes in-process callbacks across session/tool/permission/subagent/compact
and stop events. Configure only the families needed for inject/decision/missing facts.
Never publish duplicate tool/turn completion beside the SDK stream. After the native
Stop-hook block cap, upstream may override repeated blocks; preserve that as a source
fact rather than pretending the block remained effective.

The CLI hook shim is for the later Rust/CLI port and uses session-scoped configuration.
Global owner settings are never edited.

## Transcript and proxy

Transcript default: `~/.claude/projects/<encoded-cwd>/<session-id>.jsonl`
(`%USERPROFILE%\.claude` on Windows; relocatable with `CLAUDE_CONFIG_DIR`). It is an
internal format and has native retention; parse defensively and expose gaps.

Proxy route uses `ANTHROPIC_BASE_URL` plus the documented auth/header variables.
`ANTHROPIC_API_KEY` may force API-key behavior in print mode, so the per-agent profile
must explicitly construct/clear environment and never leak the owner's key into a
proxy route. Read the effective endpoint/billing route from available init facts; a
requested base URL is not proof of use.

## v1 failures retained for L7

- Several content blocks may share one message id; dedupe by native item/block, not
  message id alone.
- Respawn without exact resume/history can repeat the original prompt.
- `--bg` is not print mode; `--bare` is not a quiet flag.
- Late hook/callback from an old generation cannot answer or complete a replacement.
- A queued `additionalContext` tied to a turn must not leak into the next turn after
  that turn already ended.
