# Harnesses

Implementation order: Codex (L2) → Claude Code, Muse, OpenCode, Antigravity and ACP
(L4) → [others](others.md) only when needed. Proxies are separate services:
[OpenCodex](opencodex.md), [local model servers](local-models.md).

Every page records official sources, the check date and facts still requiring an
installed/live recording. When a plugin lands, update this matrix from what the plugin
actually does—not from an advertised feature alone.

## Capability matrix (official docs/source checked 2026-10-25)

✓ native mechanism · ~ explicit Eliot mechanism with reported semantics · ✗ not
available/rejected · ? verify before declaring support

| Verb / feature | [Codex](codex.md) | [Claude Code](claude-code.md) | [Muse](muse.md) | [OpenCode](opencode.md) | [Antigravity](antigravity.md) | [ACP](acp.md) |
| --- | --- | --- | --- | --- | --- | --- |
| Native transport | app-server NDJSON stdio | Agent SDK / stream-json | MSP JSON-RPC stdio | HTTP + SSE service | `agy` stream-json stdio | JSON-RPC stdio |
| Eliot plugin mode | per-agent | per-agent | per-agent | shared service client | per-agent | per-agent |
| `launch` / `send` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `steer` | ✓ exact turn | ~ next `PostToolUse` context; verify | ✓ exact turn | ? installed API | ✗ | ✗ |
| `inject` | ~ steer/prepend, mechanism reported | ✓ hook context | ~ steer/prepend | ? / prepend | ~ prepend next send | ~ prepend next prompt |
| `interrupt` | ✓ exact turn | ✓ current query | ✓ exact turn | ✓ abort, scope verify | ? → reject until verified | ✓ active prompt cancel |
| `resume` | ✓ thread resume | ✓ session resume | ✓ session resume | ✓ re-attach | ✓ conversation | ? advertised load/resume |
| `approve` / `deny` | ✓ native request reply | ✓ `canUseTool` | ✓ choice ids | ✓ permission reply | ✗ policy only | ✓ offered option id |
| `answer` | ✓ user-input request | ? exact AskUserQuestion shape | ✓ requirement id | ? | ✗ | capability-dependent |
| native goal | ✓ | ? | ✓ | ✗ → Eliot goal | ✗ → Eliot goal | ✗ → Eliot goal |
| native children | ✓ subagent threads | ✓ provenance/hooks | ✓ subagent methods | ✓ children endpoint/query | ✓ `subagent_info` | capability-dependent |
| hooks | ✓ documented, unused unless needed | ✓ SDK callbacks | ? | ? plugins | ✓ documented | protocol-dependent |
| transcripts/readback | ✓ thread/rollout | ✓ project JSONL | ✓ page/output refs | ✓ messages/SQLite read-only | limited conversation resume | agent-dependent |
| proxy wiring | ✓ named model provider | ✓ base URL env | ✗ Muse-owned inference | ✓ provider base URL | ✗ | profile/agent-specific |

The mode column is a custody contract, not an optimization hint:

- **per-agent**: one plugin/native process group per Eliot agent generation; force stop
  cannot affect another agent;
- **shared**: native service already isolates sessions; stopping one agent uses its
  exact native session API and never kills the service/plugin.

A harness may later add service mode only after exact per-session re-attach is proven.
It does not silently switch modes at runtime.

## Implementing a plugin

1. Read its harness page plus the [plugin protocol](../spec/plugin-protocol.md).
2. Select the correct custody mode and exact native identity/turn/request fields.
3. Record only the scenarios needed to implement the current vertical slice; remove
   secrets and include harness version/date.
4. Implement the real native route. Missing semantics are rejected, not emulated by a
   weaker verb or hidden fallback.
5. During L0–L6 run formatting and changed-package Clippy. Retain fixtures and named
   acceptance cases for the L7 conformance/live campaign.
6. In L7 run `eliot plugin-test`, the complete replay set and one compact
   `just live-<name>`; then correct this matrix from observed behavior.

All agent-scoped calls/events carry Eliot generation. Steer/interrupt carry exact
turn; approval/answer carry exact request. No unknown native effect is retried.
