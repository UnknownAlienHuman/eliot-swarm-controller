# Harnesses

Order: Codex (L2) → Claude, Muse, OpenCode, Antigravity, ACP (L4) → others only on
need. Proxies/model servers are separate service routes.

✓ qualified/documented native mechanism · ~ explicit weaker/delayed mechanism with
reported semantics · ✗ rejected · ? verify installed version before support

| Feature | Codex | Claude | Muse | OpenCode | Antigravity | ACP |
| --- | --- | --- | --- | --- | --- | --- |
| transport | app-server stdio | Agent SDK/stream-json | MSP stdio | HTTP+SSE service | stream-json stdio | JSON-RPC stdio |
| default mode | per-agent | per-agent | per-agent | shared only after isolation proof; otherwise per-agent/observe-only | per-agent | per-agent |
| session start without first input | ✓ thread/start | ? SDK recording | ✓ session/start | ✓ session create | ? installed recording | ✓ session/new |
| send | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| steer | ✓ exact turn | ~ next-tool context, verify | ✓ exact turn | ? exact installed route | ✗ | ✗ |
| inject | ~ exact steer/prepend | hook safe point | ~ steer/prepend | ?/prepend | prepend next send | prepend next prompt |
| interrupt | ✓ exact turn | ✓ exact query | ✓ exact turn | abort scope verify | ✗ until proven | ✓ active prompt cancel |
| resume | ✓ thread | ✓ session | ✓ session | exact persisted attach | conversation | advertised load/resume only |
| approvals/answer | native exact ids | callback exact ids | native exact ids | installed exact ids | policy only | offered ids/capability |
| native goal | ✓ | ? | ✓ | ✗ | ✗ | ✗ |
| children | thread ids | provenance/hooks | subagent ids | paged native ids | subagent info | capability-dependent |
| source/readback | stream + rollout handoff | stream + JSONL handoff | stream + page/cursor | SSE + paged reads | stream + limited resume | stream + capability reads |
| proxy | named provider | explicit base/auth env | none | provider base URL | none | agent-specific |

## Mode is a contract

**Per-agent** means one plugin/native custody unit per Eliot generation. Exact force is
advertised only on a qualified backend.

**Shared controlling** requires all of:

- canonical per-session native identity and exact attach/readback;
- per-session environment/tool attribution for Eliot token, generation and Git identity;
- per-session interrupt/stop that cannot affect another session;
- source events that can be attributed/recovered without guesswork.

If any is missing, use a per-agent server/process or attach observe-only. Do not choose
shared mode because the vendor can technically host multiple chats.

## Start contract

Launch/resume session and optional initial prompt are separate Eliot commands/outcomes.
A plugin normally creates the session, emits ready, then receives the linked send. If
the harness cannot expose a session before first input, it declares
`start_requires_input`; `agent.start` receives the linked command and reports session
start plus initial-input status separately. Ready alone never proves the prompt ran.

## Implementing a plugin

1. Read plugin protocol and this harness page; inspect installed schema/source first.
2. Choose exact/degraded custody and per-agent/shared mode honestly.
3. Define canonical native scope/ref and unique native event ids.
4. Record only scenarios needed by the current vertical path; include version/date and
   remove secrets.
5. Implement native semantics. Reject weaker substitutions/hidden fallback.
6. L0–L6: fmt + changed-package Clippy. L7: complete conformance, replay, live,
   platform and isolation cases.

All calls/events carry generation; controls carry exact turn/request. Unknown effects
are never retried. Source ownership and duplicate rejection are mandatory.
