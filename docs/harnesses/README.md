# Harnesses

Implementation order: Codex (L2) → Claude Code, Muse, OpenCode, Antigravity and ACP
(L4) → [others](others.md) only on concrete need. Provider services are separate:
[OpenCodex](opencodex.md), [local models](local-models.md).

Each page records official/installed sources, check date, native identity, process mode,
source ownership/cursor behavior and unverified facts. Update it from actual installed
behavior when the plugin is implemented; advertised capability alone is not support.

## Common plugin contract

Every harness page must answer:

1. What exact `(native_scope,native_ref)` identifies one session?
2. What creates a new Eliot `generation`, and what reconnect preserves it?
3. Is the plugin `per_agent` or a proven isolated `shared` service client?
4. Which source owns each lifecycle family while controlled and while observe-only?
5. Which source has an opaque exclusive replay cursor? If none, reconnect records a gap
   and starts at the live edge.
6. What stable vendor/cursor-derived `native_id` makes a retried `events.put` safe? If
   no stable id/cursor exists, the plugin must not retry a lost acknowledgement.
7. What exact `source.handoff` transfers a family, and what cursor/gap boundary proves
   it? A lost handoff reply is settled through `source.state`, not another blind handoff.
8. What native id binds input→turn, steer/interrupt→turn and approval/answer→request?
9. What exact readback settles a lost native reply? Unsupported/unknown is preferable to
   heuristic substitution.
10. Can a native child receive an isolated Eliot environment before its tools run?
11. What is the effective provider/account/permissions route, and how is it read back?

No plugin adds binding revisions, source epochs, adapter journals or its own workflow
engine. It uses the common generation/source/cursor/command contracts.

## Capability matrix

✓ qualified/documented native mechanism · ~ explicit weaker Eliot mechanism with its
actual application point · ✗ rejected · ? unresolved until installed recording/live
qualification

| Feature | [Codex](codex.md) | [Claude](claude-code.md) | [Muse](muse.md) | [OpenCode](opencode.md) | [Antigravity](antigravity.md) | [ACP](acp.md) |
| --- | --- | --- | --- | --- | --- | --- |
| Transport | app-server NDJSON stdio | Agent SDK / stream-json | MSP JSON-RPC stdio | HTTP + SSE service | `agy` stream-json | JSON-RPC stdio |
| Eliot process mode | per-Agent | per-Agent | per-Agent | shared only after isolation proof; otherwise per-Agent server/observe | per-Agent | per-Agent |
| Native subject | Codex store + thread id | Claude config/project + session id | Muse host/store + session id | server identity + session id | auth/config + conversation id | agent command/store + session id |
| Controlled lifecycle owner | app-server stream | SDK/message stream | MSP stream | session-filtered SSE | stdout stream | ACP connection |
| Observe/recovery cursor | rollout byte offset / native readback | JSONL offset / session readback | native view/page cursor | native history cursor if installed API proves it; else gap+snapshot | none qualified → gap+snapshot | agent-dependent; none by default |
| session before first input | ✓ thread start | ? | ✓ session start | ✓ session create | ? | ✓ session/new |
| isolated child environment | ✗ app-server shared | ✗ in-process | ? | ? installed per-session env | ✗ | agent capability-dependent |
| `steer` | ✓ exact turn | ~ next hook context; not exact immediate steer | ✓ exact turn | ? exact native API only | ✗ | ✗ v1 |
| `inject` | exact steer or next-send prepend | ✓ assigned hook context | steer/prepend | verified safe point or prepend | prepend next send | prepend next prompt |
| `interrupt` | ✓ exact turn | ✓ active query | ✓ exact turn | ✓ exact session execution, verify scope | ? → reject until proven | ✓ active prompt cancel |
| `resume` | ✓ thread resume, new Eliot generation | ✓ session resume | ✓ session resume | attach same native subject, new control generation | ✓ conversation | capability-dependent load/resume |
| approvals/questions | exact request replies | exact SDK callbacks | exact choice/requirement ids | exact permission/input ids if installed API exposes them | no request channel | negotiated request ids |
| native Goal | ✓ | ? | ✓ | ✗ Eliot Goal | ✗ Eliot Goal | ✗ Eliot Goal |
| children | exact subagent threads | provenance/hooks | native child provenance | exact parent/session API | `subagent_info` | capability-dependent |
| proxy route | named provider | base URL/auth env | none | provider base URL | none | agent-specific |

## Implementing a plugin

1. Read its page, [plugin protocol](../spec/plugin-protocol.md) and the exact donor
   boundary named in [donors](../reference/donors.md).
2. Implement the real native path first. Missing semantics reject; no hidden fallback.
3. Record only traffic needed to discover/implement the current verb; sanitize secrets
   and record installed version/date.
4. During L0–L6 run format + smallest changed-package Clippy. Keep named fixture/live
   cases for final L7 conformance; do not build a broad campaign around every partial
   task.
5. Update the page and this matrix from actual behavior, including exact generation,
   source owner, cursor/native-id and handoff/readback facts.

L7 runs one common black-box conformance suite over recordings and compact live runs for
all required harnesses.
