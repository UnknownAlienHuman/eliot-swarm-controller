# Claude Code

Layer L4.2. Initial plugin may be TypeScript on the official Agent SDK; default
`per_agent`. One plugin/SDK/CLI custody unit owns one Eliot generation. A later Rust
stream-json port keeps the same protocol/conformance.

Facts checked 2026-10-25 against official headless/CLI/hooks/Agent SDK/session/subagent/
Goal/env/gateway docs and package versions in the prior revision. SDK uses Anthropic
commercial terms; recheck version/terms at implementation.

## Transport and start

SDK `query({prompt: AsyncIterable<SDKUserMessage>, options})` is the multi-turn route.
CLI port uses print stream-json with explicit settings; never `--bare` because it
disables managed instructions/hooks/MCP/skills.

Before constructing/calling the SDK query or writing a first message, plugin persists and
opens source epochs for stream lifecycle, requests/hooks and optional descendant
registration. Installed recording must establish whether a stable session exists before
the first user message:

- yes: `start_requires_input=false`; open sources, start session, commit ready, then
  linked send yields first message;
- no: `start_requires_input=true`; open sources first, start receives linked send but
  reports session and exact input/turn association separately.

Pre-init failure is definite only when no session/input may have applied. Otherwise
unknown; never respawn/repeat prompt. Resume opens new-generation sources first, then
creates the native resume query; optional text is linked send. Effective system
instructions are reported because resume may retain the old prompt.

Persisted SDK facts use stable native event ids where available. Transcript JSONL is
observe-only or explicit recovery after predecessor-aware source handoff. If stream/
JSONL cannot prove a boundary, use a visible gap. Discovery/transcript reads are paged
below frame limits. Large system instructions arrive by ArtifactRef.

## Verb mapping

| Eliot | Claude |
| --- | --- |
| send | yield one SDK user message when idle/current generation; applied only at exact native turn/query association |
| steer | only installed turn-bound next-tool context; report `next_tool`; turn end stales it |
| inject | assigned hook `additionalContext` at exact safe point |
| interrupt | `Query.interrupt()` exact active query; no unknown retry |
| stop | end input/close query, clear native driver if any, exact custody cleanup |
| approve/deny | exact `canUseTool`/permission request |
| answer | exact AskUserQuestion shape after installed qualification |
| configure | live or next-turn/session setting with effective readback |
| Goal | native only after installed qualification; otherwise Eliot Goal |

A hook/context path is not steer unless tied to the exact current turn and unable to
spill into the next. Unsupported semantics reject before native I/O.

## Events, hooks and readback

SDK stream owns controlled session/turn/message/tool lifecycle. In-process hooks own only
synchronous decisions/facts absent from stream. Source family ownership and replacement
are transactional; hook proposal is non-dispatchable and deadline-bound.

Init yields native subject/effective settings. Assistant/user/tool blocks map by exact
content/tool/update identities; message id alone is insufficient because blocks may
share it. Partial stream events are ephemeral. Exact result subtype settles turn;
process exit/silence does not. Reconnect with session present but no terminal proof is an
unknown turn. Persisted events/final text use bounded projections.

Per-Agent SDK/CLI descendants may register only after custody verification. OS monitor
owns process exit/stats.

## Native children and routing

Subagents register by exact parent tool-use/native Agent ids. Child completion requires
an exact tool result/notification; forwarded text may need an SDK option. Child lists and
transcript reads are paginated.

Until an installed recording proves a distinct child process/session environment that
can receive its own Eliot token and Git identity before tools run, declare
`child_environment=false`. Child Agent row and native controls/provenance may still
exist, but `routing_enabled=false`; CLI/Git activity is attributed to parent. Do not mint
a token merely because hooks expose a subagent id. Guard-exceeding vendor children remain
recorded with reduced authority/wake.

Resumed cumulative cost is session scope and converted to exact integer micro-USD when
reported. Late callbacks/hooks from old generation are diagnostic. Stop-hook block caps
or overrides remain explicit.

## Environment, proxy and custody

Per-Agent minimal environment passes named Claude config/auth/proxy plus parent Eliot
identity only when direct routing is enabled. `ANTHROPIC_API_KEY` can force API-key mode
in print route; never inherit it accidentally into subscription/proxy profile. Requested
base URL is not effective proof.

Windows custom executable is a real absolute `.exe` where required. Exact force only on
an exact backend; degraded Unix rejects it. Shared service mode would require
per-session environment/readback/stop and service-owned PID; it is not assumed.

## L7 cases

- source open committed before SDK query/session/first input;
- init before/after first message and every lost start/input/source acknowledgement;
- no prompt replay on resume/restart;
- additionalContext turn-spill prevention;
- exact permission/question/hook deadline identities;
- multi-block event ids, event-row pruning, source handoff and reconnect unknown turn;
- discovery/transcript page and frame bounds;
- child canonical subject/guard/completion and parent attribution when child env absent;
- cumulative usage, minimal env/proxy billing isolation;
- descendant registration vs OS enumeration and exact/degraded custody.
