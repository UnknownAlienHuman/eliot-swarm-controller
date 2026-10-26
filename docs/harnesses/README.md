# Harnesses

Order: Codex (L2) → Claude, Muse, OpenCode, Antigravity, ACP (L4) → others only on
need. Proxies/model servers are separate service routes.

✓ qualified/documented native mechanism · ~ explicit weaker/delayed mechanism with
reported semantics · ✗ rejected · ? verify installed version before support

| Feature | Codex | Claude | Muse | OpenCode | Antigravity | ACP |
| --- | --- | --- | --- | --- | --- | --- |
| transport | app-server stdio | Agent SDK/stream-json | MSP stdio | HTTP+SSE service | stream-json stdio | JSON-RPC stdio |
| default mode | per-agent | per-agent | per-agent | shared only after isolation proof; otherwise per-agent/observe-only | per-agent | per-agent |
| session start before first input | ✓ thread/start | ? installed SDK recording | ✓ session/start | ✓ session create | ? installed recording | ✓ session/new |
| send | ✓ | ✓ | ✓ | ✓ async association | ✓ | ✓ |
| steer | ✓ exact turn | ~ next-tool context, verify | ✓ exact turn | ? exact installed route | ✗ | ✗ |
| inject | ~ exact steer/prepend | hook safe point | ~ steer/prepend | ?/prepend | prepend next send | prepend next prompt |
| interrupt | ✓ exact turn | ✓ exact query | ✓ exact turn | exact abort scope verify | ✗ until proven | ✓ active prompt cancel |
| resume | ✓ thread | ✓ session | ✓ session | exact persisted attach | conversation | advertised load/resume only |
| approvals/answer | native exact ids | callback exact ids | native exact ids | installed exact ids | policy only | offered ids/capability |
| native goal | ✓ | ? | ✓ | ✗ | ✗ | ✗ |
| native children | thread ids | provenance/hooks | subagent ids | paged native ids | subagent info | capability-dependent |
| distinct child environment/token | ✗ unless upstream proves per-thread env; attribute CLI to parent | ? verify SDK/subagent env | ? verify MSP host child env | ? must prove per-session child tool env | ✗ same process unless proven | capability-dependent |
| source/readback | app-server + rollout handoff | stream + JSONL handoff | MSP + page/cursor | SSE + paged reads | stream + limited resume | stream + capability reads |
| proxy | named provider | explicit base/auth env | none | provider base URL | none | agent-specific |

## Mode is a contract

**Per-agent** means one plugin/native custody unit per Eliot generation. Exact force is
advertised only on qualified OS backend.

**Shared controlling** requires all of:

- canonical per-session identity and exact attach/readback;
- per-session environment/tool attribution for Eliot token, generation and Git identity;
- per-session interrupt/stop that cannot affect another session;
- source events attributable/recoverable without guesswork.

If any is missing, use per-agent server/process or observe-only attach. Do not select
shared mode merely because vendor hosts multiple chats. Shared server processes remain
`service:<name>` custody; they are never registered as Agent-owned PIDs.

## Start and input contract

Launch/resume session and optional initial prompt are separate Eliot commands/outcomes.
A plugin normally creates session, opens authoritative source families, emits
commit-acknowledged ready, then receives linked send. If harness cannot expose session
before first input, it declares `start_requires_input`; start receives linked command
but still reports session and input status separately. Ready never proves prompt ran.
Transport/HTTP inbox acceptance never means send applied; exact native turn/execution
association does.

## Source and child contract

- Plugin durably chooses `open_id`, daemon returns source key/family ownership and
  watermark. Persisted facts use contiguous sequence and one unacknowledged envelope.
- Vendor duplicate may advance source sequence but cannot publish lifecycle twice.
  Unbridgeable interval is a visible gap.
- `process.put` is only verified per-agent descendant registration. OS monitor owns
  process start/exit/stats; shared-service PIDs stay service-owned.
- Native child registration carries stable `registration_id`, exact child scope/ref and
  parent provenance. Parent/root/project/harness conflict rejects.
- A distinct child token is returned only when plugin declares and L7 proves
  `child_environment`. Otherwise child CLI calls are attributed to parent; Eliot does
  not pretend separate control identity.

## Implementing a plugin

1. Read plugin protocol and harness page; inspect installed schema/source first.
2. Choose per-agent/shared and exact/degraded custody honestly.
3. Define native subject, stable source-open identity, family ownership, native cursor/
   event ids and child-environment capability.
4. Record only current vertical scenarios; include version/date and remove secrets.
5. Implement native semantics. Reject weaker substitutions/hidden fallback.
6. L0–L6: fmt + changed-package Clippy. L7: conformance, replay, live, platform and
   isolation cases.

All scoped calls/facts carry generation; controls exact turn/request. Unknown native
or external effects are never retried.
