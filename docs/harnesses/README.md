# Harnesses

Order: Codex (L2) → Claude, Muse, OpenCode, Antigravity, ACP (L4) → others only on
need. Proxies/model servers are separate service routes.

✓ qualified/documented native mechanism · ~ explicit weaker/delayed mechanism with
reported semantics · ✗ rejected · ? verify installed version before support

| Feature | Codex | Claude | Muse | OpenCode | Antigravity | ACP |
| --- | --- | --- | --- | --- | --- | --- |
| transport | app-server stdio | Agent SDK/stream-json | MSP stdio | HTTP+SSE service | stream-json stdio | JSON-RPC stdio |
| default mode | per-Agent | per-Agent | per-Agent | shared only after isolation proof; otherwise per-Agent/observe-only | per-Agent | per-Agent |
| session start before first input | ✓ thread/start | ? installed SDK recording | ✓ session/start | ✓ session create | ? installed recording | ✓ session/new |
| send | ✓ | ✓ | ✓ | ✓ async association | ✓ | ✓ |
| steer | ✓ exact turn | ~ next-tool context, verify | ✓ exact turn | ? exact installed route | ✗ | ✗ |
| inject | ~ exact steer/prepend | hook safe point | ~ steer/prepend | ?/prepend | prepend next send | prepend next prompt |
| interrupt | ✓ exact turn | ✓ exact query | ✓ exact turn | exact abort scope verify | ✗ until proven | ✓ active prompt cancel |
| resume | ✓ thread | ✓ session | ✓ session | exact persisted attach | conversation | advertised load/resume only |
| approvals/answer | native exact ids | callback exact ids | native exact ids | installed exact ids | policy only | offered ids/capability |
| native Goal | ✓ | ? | ✓ | ✗ | ✗ | ✗ |
| native children | thread ids | provenance/hooks | subagent ids | paged native ids | subagent info | capability-dependent |
| distinct child environment/routing | ✗ until per-thread env proof | ? verify | ? verify MSP host | ? prove child/session tool env | ✗ unless proven | capability-dependent |
| source/readback | app-server + rollout handoff | stream + JSONL handoff | MSP + page/cursor | SSE + paged reads | stream + limited resume | stream + capability reads |
| proxy | named provider | explicit base/auth env | none | provider base URL | none | Agent-specific |

## Mode is a contract

**Per-Agent** means one plugin/native custody unit per Eliot generation. Exact force is
advertised only on a qualified OS backend.

**Shared controlling** requires all of:

- canonical per-session identity and exact attach/readback;
- per-session environment/tool attribution where direct Eliot routing/Git identity is
  claimed;
- per-session interrupt/stop that cannot affect another session;
- source events attributable/recoverable without guesswork.

If any is missing, use per-Agent server/process or observe-only attach. A shared server
PID remains `service:<name>` custody and is never Agent-owned.

## Start and input contract

Before the first native session/input effect, the plugin opens provisional authoritative
source epochs for every family that can emit during start. Session and optional initial
prompt are separate Eliot commands/outcomes.

A session-first plugin opens sources, creates the native session, commits ready, then
receives linked send. A plugin that cannot expose session before first input declares
`start_requires_input`; it opens sources first, performs one linked start+input and still
reports session and input outcomes separately. Ready never proves prompt ran.
Transport/HTTP inbox acceptance never means send applied; exact native turn/execution
association does. A native input id is optional if exact command→turn mapping exists.

## Source, paging and child contract

- Plugin durably chooses `open_id`; daemon returns source key/family ownership and
  watermark. Persisted facts use contiguous sequence and one unacknowledged envelope.
- Source declares `strict_cursor|event_id|none` replay mode. Native-event receipts
  survive event pruning in `event_id` mode; unbridgeable replay is a visible gap.
- Planned source replacement is one predecessor-aware atomic handoff. Separate close/
  open is explicitly gapped.
- `discover`, snapshots and history are paged: at most 1,000 items and below the plugin
  frame byte limit.
- `process.put` is only verified per-Agent descendant registration. OS monitor owns
  process start/exit/stats; shared-service PIDs remain service-owned.
- Native child registration carries stable registration id, exact child scope/ref and
  parent provenance. Existing vendor child is recorded even above controller guard;
  guard-exceeded child gets reduced authority/routing/wake rather than disappearing.
- A distinct child routing token is returned only when plugin declares and L7 proves
  child environment injection before tools run. Otherwise child may still be natively
  controllable through the parent plugin, but `routing_enabled=false` and CLI/Git
  attribution remains parent/session.

## Implementing a plugin

1. Read plugin protocol and harness page; inspect installed schema/source first.
2. Choose per-Agent/shared and exact/degraded custody honestly.
3. Define native subject, pre-native source opens, family ownership, replay mode,
   cursor/event ids, paged reads and child-environment capability.
4. Record only current vertical scenarios; include version/date and remove secrets.
5. Implement native semantics. Reject weaker substitutions/hidden fallback.
6. L0–L6: fmt + changed-package Clippy. L7: conformance, replay, live, platform and
   isolation cases.

All scoped calls/facts carry generation; controls exact turn/request. Unknown native or
external effects are never retried.
