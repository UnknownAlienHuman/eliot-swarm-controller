# Claude Code

Layer L4.2. Initial plugin may be TypeScript on official Agent SDK; default
`per_agent`. One plugin/SDK/CLI custody unit owns one Eliot generation. A later Rust
stream-json port keeps same protocol/conformance.

Facts checked 2026-10-25 against official headless/CLI/hooks/Agent SDK/session/subagent/
goal/env/gateway docs and package versions in prior revision. SDK uses Anthropic
commercial terms; recheck version/terms at implementation.

## Transport and start

SDK `query({prompt: AsyncIterable<SDKUserMessage>, options})` is multi-turn route. CLI
port uses print stream-json with explicit settings; never `--bare` because it disables
managed instructions/hooks/MCP/skills.

Record whether installed SDK exposes init/session id before first user message:

- yes: `start_requires_input=false`; start session, open source epochs, commit ready,
  then linked send yields first message;
- no: `start_requires_input=true`; start receives linked send but reports session and
  exact turn/input association separately.

Pre-init failure is definite only when no session/input may have applied. Otherwise
unknown; never respawn/repeat prompt. Resume creates new generation with native session
id; optional text is linked send. Effective system instructions are reported because
resume may retain old prompt.

Persisted SDK facts use stable source open id/key and contiguous commit-ack sequence.
Transcript JSONL is observe-only or explicit recovery after handoff. Lost acknowledgement
resends same envelope; old callbacks cannot publish on replacement generation.

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
| goal | native only after installed qualification; otherwise Eliot goal |

A hook/context path is not steer unless tied to exact current turn and cannot spill into
next. Unsupported semantics reject before native I/O.

## Events, hooks and readback

SDK stream owns controlled session/turn/message/tool lifecycle. In-process hooks own
only synchronous decisions/facts absent from stream. Each source family is opened
transactionally; hook proposal is non-dispatchable and deadline-bound.

Init yields native subject/effective settings. Assistant/user/tool blocks map by exact
content/tool/update ids; message id alone is insufficient because blocks may share it.
Partial stream events are ephemeral. Exact result subtype settles turn; process exit or
silence does not. Reconnect with session present but no terminal proof is unknown turn.

Per-agent SDK/CLI descendants may be registered only after custody verification. OS
monitor owns process exit/stats.

## Native children and environment

Subagents register by exact parent tool-use/native agent ids. Child completion requires
exact tool result/notification; forwarded text may need SDK option.

Until an installed recording proves a distinct child process/session environment that
can receive its own Eliot token and Git identity, declare `child_environment=false`.
Child Agent row and native control/provenance still exist, but its CLI/Git activity is
attributed to parent. Do not mint a child token merely because hooks expose a subagent id.
A future true claim requires lost child-registration reply/token retry coverage.

Resumed cumulative cost is session scope and converted to exact integer micro-USD when
reported. Late callbacks/hooks from old generation are diagnostic. Stop-hook block caps
or overrides remain explicit.

## Environment, proxy and custody

Per-agent minimal environment passes named Claude config/auth/proxy plus parent Eliot
identity. `ANTHROPIC_API_KEY` can force API-key mode in print route; never inherit it
accidentally into subscription/proxy profile. Requested base URL is not effective proof.

Windows custom executable is real absolute `.exe` where required. Exact force only on
exact backend; degraded Unix rejects it. Shared service mode would require per-session
environment/readback/stop and service-owned PID; not assumed.

## L7 cases

- init before/after first message and every lost start/input/source ack;
- no prompt replay on resume/restart;
- additionalContext turn-spill prevention;
- exact permission/question/hook deadline ids;
- multi-block event IDs, source handoff and reconnect unknown turn;
- child canonical subject, completion and parent attribution when child env absent;
- cumulative usage, minimal env/proxy billing isolation;
- descendant registration vs OS enumeration and exact/degraded custody.
