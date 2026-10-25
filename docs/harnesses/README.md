# Harnesses

Plugin order: Codex (L2) → Claude Code, OpenCode, Antigravity, ACP agents (L4) →
others only on request. Each page lists official sources with the date they were
checked and a "verify during L-x" list. When a plugin lands, update its column below
from what the plugin actually does (not from docs).

## Capability matrix (from official docs, 2026-10-25)

✓ documented native mechanism · ~ emulated by the plugin (mechanism reported) ·
✗ not available (verb rejected) · ? to verify in that plugin's layer

| Verb / feature | [Codex](codex.md) | [Claude Code](claude-code.md) | [OpenCode](opencode.md) | [Antigravity](antigravity.md) | [ACP](acp.md) |
| --- | --- | --- | --- | --- | --- |
| Transport | app-server, NDJSON stdio | Agent SDK / `-p` stream-json | HTTP + SSE | `agy` stream-json stdio | JSON-RPC stdio |
| `launch` / `send` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `steer` | ✓ `turn/steer` | ~ PostToolUse `additionalContext` ? | ? | ✗ | ✗ |
| `inject` | ~ steer / prepend | ✓ hook `additionalContext` | ? / prepend | ~ prepend | ~ prepend |
| `interrupt` | ✓ `turn/interrupt` | ✓ `interrupt()` | ✓ `abort` | ? | ✓ `session/cancel` |
| `resume` | ✓ `thread/resume` | ✓ `resume` | ✓ re-attach | ✓ `--conversation` | ? `session/load` |
| `approve` / `deny` | ✓ request replies | ✓ `canUseTool` | ✓ permission reply | ✗ policy only | ✓ `request_permission` |
| `answer` | ✓ `requestUserInput` | ? AskUserQuestion | ? | ✗ | ✗ |
| native goal | ✓ `thread/goal/*` | ? `/goal` | ✗ → Eliot loop | ✗ → Eliot loop | ✗ → Eliot loop |
| children | ✓ subagent threads | ✓ `parent_tool_use_id`, Subagent hooks | ✓ `/children` | ✓ `subagent_info` | ? |
| hooks | ✓ 12 events | ✓ 33 events | ? plugins | ✓ (page exists) | ✗ |
| transcripts | ✓ rollout JSONL | ✓ project JSONL | ✓ SQLite (read-only) | ✗ | ✗ |
| proxy wiring | ✓ `model_providers` | ✓ `ANTHROPIC_BASE_URL` | ✓ provider `baseURL` | ✗ | per agent |

## Writing a plugin

1. Read the harness page and the [plugin protocol](../spec/plugin-protocol.md).
2. Record scenarios with `just record-<name>`
   ([recordings](../spec/plugin-protocol.md#recordings)): the page's list, or by
   default `turn`, `send-busy`, `steer`, `interrupt`, `approve`, `deny`, `question`,
   `goal`, `subagent`, `resume`, `error` — skipping what the harness cannot do.
3. Implement against the recordings; pass `eliot plugin-test` in replay mode.
4. Add `just live-<name>`, run it, paste the output in the PR.
5. Where the harness behaved differently from this page or the matrix, correct them
   with the source (recording name, version, date).
