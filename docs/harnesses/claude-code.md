# Claude Code

Layer L4.2. Plugin: `plugins/claude`. Start in TypeScript on the Agent SDK (fastest
documented path: streaming input, `interrupt()`, `canUseTool`, in-process hooks);
port to Rust in L8 by driving the CLI's stream-json mode directly.

Facts checked 2026-10-25 (docs page marker v2.1.309; npm `@anthropic-ai/claude-code`
2.1.310, `@anthropic-ai/claude-agent-sdk` 0.3.310; PyPI `claude-agent-sdk` 0.2.174).
The SDK is under Anthropic's commercial terms, not an open-source license.

## Sources

[headless](https://code.claude.com/docs/en/headless),
[CLI reference](https://code.claude.com/docs/en/cli-reference),
[hooks](https://code.claude.com/docs/en/hooks),
[Agent SDK TypeScript](https://code.claude.com/docs/en/agent-sdk/typescript),
[Agent SDK Python](https://code.claude.com/docs/en/agent-sdk/python),
[subagents](https://code.claude.com/docs/en/sub-agents),
[sessions](https://code.claude.com/docs/en/sessions),
[goal](https://code.claude.com/docs/en/goal),
[env vars](https://code.claude.com/docs/en/env-vars),
[LLM gateway](https://code.claude.com/docs/en/llm-gateway).

## Transport

- SDK: `query({ prompt: AsyncIterable<SDKUserMessage>, options })` keeps one session
  open with streaming input; the SDK runs the Claude Code binary.
- CLI (for the Rust port): `claude -p --input-format stream-json --output-format
  stream-json` (examples also pass `--verbose`); `--include-partial-messages` for
  deltas; `--resume <id>`, `--session-id <uuid>`; `--permission-mode`.
- Do not use `--bare` for managed agents: it skips hooks, skills, MCP and CLAUDE.md
  discovery, and the docs say it will become the default for `-p` — pass explicit
  settings so hooks keep working.

## Verb mapping

| Eliot verb | Claude Code | Notes |
| --- | --- | --- |
| `launch` | `query()` with streaming input, `cwd`, `model`, `permissionMode` | `native_ref` = session id from the init message. |
| `send` | push an `SDKUserMessage` into the input stream | Verify whether a message pushed mid-turn is queued for the next turn (expected) or merged. |
| `steer` | text returned as `additionalContext` from the next `PostToolUse` hook | Lands after the current tool call; verify `additionalContext` on `PostToolUse` in L4.2. Reject if the turn has no further tool calls? → report `applies: next_tool_or_turn`. |
| `inject` | `additionalContext` on `UserPromptSubmit` / `PostToolUse` hooks (≤ 10,000 chars) | |
| `interrupt` | `Query.interrupt()` | Streaming mode only. |
| `stop` | end the input stream; `force` kills the process tree | |
| `resume` | `options.resume = <session id>` | Resume does not refresh the system prompt; never resend the old input. |
| `approve` / `deny` | `canUseTool` callback returns allow/deny — the plugin awaits an Eliot `approve`/`deny` (or host `decide`) | `canUseTool` runs only when a prompt would appear; a `PreToolUse` hook sees every call. |
| `answer` | the `AskUserQuestion` tool via `canUseTool` (answers in the returned input) | Verify the exact shape in L4.2. |
| `configure` | `setPermissionMode()` (now); model per query option (`applies: next_session` unless the SDK exposes a live setter) | |
| `goal.*` | Claude Code has a native goal feature ([goal docs](https://code.claude.com/docs/en/goal)); evaluate in L4.2, default to the Eliot Goal loop | Its Stop-hook check spends model calls. |

## Events

| Native | Eliot |
| --- | --- |
| system init message | `agent.ready` (session id, model, tools, permission mode) |
| assistant / user messages, `result` message | `message.agent`, `tool.started/completed`, `turn.completed` (+ cumulative usage) |
| `stream_event` partials | `message.delta` (E) |
| messages with `parent_tool_use_id`; `SubagentStart`/`SubagentStop` hooks (`agent_id`, `agent_type`, `agent_transcript_path`) | `child` registration and child events |
| `PreToolUse`, `PermissionRequest`, `PermissionDenied` | `approval.requested` / `approval.resolved` |
| `Stop`, `StopFailure`, `PreCompact`/`PostCompact` | `turn.completed`, `x.claude.*` |

Subagents are spawned with the `Agent` tool (renamed from `Task` in v2.1.63; `Task`
remains an alias).

## Hooks

33 events, including `SessionStart`, `UserPromptSubmit`, `PreToolUse`,
`PermissionRequest`, `PostToolUse`, `PostToolUseFailure`, `SubagentStart`,
`SubagentStop`, `Stop`, `PreCompact`, `SessionEnd`. Exit code 2 blocks for decision
events; JSON output uses `hookSpecificOutput` with `hookEventName`
(`permissionDecision: allow|deny|ask|defer`, `additionalContext`). After 8
consecutive Stop-hook blocks the block is overridden
(`CLAUDE_CODE_STOP_HOOK_BLOCK_CAP`). With the SDK, hooks are in-process callbacks
(`options.hooks`); the [hook shim](../spec/plugin-protocol.md#hooks) is for the CLI
port.

## Transcripts

`~/.claude/projects/<cwd with non-alphanumerics as ->/<session-id>.jsonl`
(`%USERPROFILE%\.claude` on Windows; `CLAUDE_CONFIG_DIR` relocates it). Internal
format — parse defensively; retained `cleanupPeriodDays` (default 30).

## Proxy

`ANTHROPIC_BASE_URL` + `ANTHROPIC_AUTH_TOKEN` (sent as `Authorization: Bearer`);
`ANTHROPIC_CUSTOM_HEADERS` for extra headers (e.g. `x-opencodex-api-key: ...`). A
gateway credential replaces the claude.ai login; `ANTHROPIC_BASE_URL` alone does not.
`ANTHROPIC_API_KEY`, when set, is always used with `-p` — the plugin must not leak the
owner's key into proxied agents.

## v1 pitfalls

- Several content blocks share one `message.id`; deduplicating by it loses tool blocks.
- Respawning without history can repeat the original prompt.
- `--bg` is not `-p`; `--bare` is not a "quiet" flag.
