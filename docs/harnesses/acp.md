# ACP agents

Optional L6 adapter using the official Rust Agent Client Protocol types. One plugin and
one configured ACP subprocess per Eliot Agent generation; the generic adapter is not a
prerequisite for the native Codex plugin.

Sources: [ACP transport](https://agentclientprotocol.com/protocol/v1/transports.md),
[schema](https://agentclientprotocol.com/protocol/v1/schema.md),
[official repository](https://github.com/agentclientprotocol/agent-client-protocol).
Inherited notes target v1 as checked 2026-10-25. Select the installed version/capabilities
at implementation; do not assume all agents implement every schema method.

## Identity and source

Subject: `(canonical configured agent/store scope, exact ACP session id)`. New/load/resume
use Eliot generation rules. Install update/request/response routing before new/load or
prompt. Live ACP connection owns supplied lifecycle; no duplicate transcript producer.
Children/environment are agent-specific and default to no independent identity.

No generic durable replay cursor is assumed. Load replay is historical/readback, not new
turns. Use an exclusive cursor only where the selected agent guarantees it; otherwise
record a gap and read current state. Exact active prompt/request correlation matters,
not the UI's latest visible message.

## Native mapping

| Eliot verb | ACP mechanism / limitation |
| --- | --- |
| launch | initialize then session/new without input; linked prompt separate |
| send | session/prompt; exact request/update/result association, terminal response settles separately |
| steer | unsupported in generic v1 |
| inject | next-prompt prepend, never current-turn steering |
| interrupt | session/cancel for the bound active prompt, never queued-input cancel or session close |
| stop | advertised exact session close and/or end owned subprocess; no shared service kill |
| resume | advertised session/load/resume in new generation, no previous input replay |
| approve/deny | exact session/request_permission and one offered option id |
| answer | only advertised exact user-input request/reply shape |
| configure | typed mode/config selector where supported, with actual application point |
| goal | Eliot Goal, no generic native Goal |

A correlated terminal prompt reply may establish both association and completion if no
intermediate update arrived. RPC success is native turn completion, not task acceptance.
Lost prompt/cancel/close outcomes are never blindly replayed.

## Profile and boundaries

```toml
[profiles.gemini-acp]
harness = "acp"
cmd = ["gemini", "--acp"]
```

Resolve the selected command explicitly and verify its installed ACP flag. Native Eliot
adapters are preferred for stronger controls. A non-ACP headless route is not a hidden
fallback; it needs an explicitly selected separate adapter. Replacing an ACP backend
removes the old owner rather than running two controllers on one session.

L7 covers source-before-load, stale generation/request rejection, load replay/gaps,
unknown effects, offered option ids and accurate unsupported capabilities.
