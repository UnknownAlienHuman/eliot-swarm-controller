# Claude Code

Layer L4.2. Initial plugin may be TypeScript on the official Agent SDK; manifest mode
`per_agent`. One plugin/SDK/CLI custody unit owns one Eliot generation. A later Rust
stream-json port keeps the same protocol and qualification cases.

Facts checked 2026-10-25 against official headless/CLI/hooks/Agent SDK/session/
subagent/goal/env/gateway docs and package versions recorded in the previous revision.
The SDK is under Anthropic commercial terms; recheck version/terms at implementation.

## Transport and start

SDK `query({prompt: AsyncIterable<SDKUserMessage>, options})` is the multi-turn route.
CLI port uses print mode with stream-json input/output and explicit settings; never use
`--bare`, which disables managed instructions/hooks/MCP/skills.

Whether the installed SDK emits an init/session id before the first yielded user
message must be recorded:

- if yes: `start_requires_input=false`; start query/session, commit ready, then linked
  send yields the first SDK message;
- if no: `start_requires_input=true`; `agent.start.initial` is that linked send and the
  plugin reports session start and initial-input outcome separately.

A pre-init failure is definite only when no input/session may have been admitted.
Otherwise session/initial command remains unknown. Never respawn and repeat the prompt.
Resume creates a new Eliot generation with `options.resume=<session id>`; optional text
is a separate linked send. Claude resume may retain the old system prompt, so effective
instructions are read/reported rather than assumed refreshed.

## Verb mapping

| Eliot | Claude |
| --- | --- |
| send | yield one SDK user message only when idle/current generation |
| steer | only if installed SDK/hook proves turn-bound next-tool context; report `next_tool`; if turn ends first, fail/stale rather than leak to next turn |
| inject | assigned hook `additionalContext` at an exact reported safe point |
| interrupt | `Query.interrupt()` bound to exact active turn/query; no retry after unknown |
| stop | end input/close query, clear native driver if any, then exact custody cleanup |
| approve/deny | exact `canUseTool`/permission callback request |
| answer | exact AskUserQuestion callback shape after installed qualification |
| configure | live setter or next-turn/session option with effective readback |
| goal | native only after installed qualification; otherwise Eliot goal |

A hook/context mechanism is not called steer unless it is tied to the current turn and
cannot spill into the next one. Unsupported semantics reject before native I/O.

## Events/source ownership

SDK stream owns controlled lifecycle. In-process hooks own only synchronous decisions
or facts missing from the stream. Transcript JSONL is observe-only/recovery after an
explicit cursor handoff, not a duplicate source.

Init gives session id/effective model/tools/permission/effort. Assistant/user/tool
blocks map by exact content/tool ids; multiple blocks may share a message id, so message
id alone is not event dedupe. Partial stream events are ephemeral deltas. Exact result
subtype settles the active turn; process exit/silence does not.

Subagents register from exact parent tool-use/native agent ids. Forwarded text may
require an SDK option. Child completion needs its exact tool result/notification.
Resumed cumulative cost is session scope and converted to exact integer micro-USD when
reported.

Late SDK callbacks/hooks from an old generation are diagnostic only. Stop-hook block
caps/overrides are reported honestly.

## Environment, proxy and custody

Per-agent minimal environment passes only named Claude config/auth/proxy variables plus
Eliot generation token/Git identity. `ANTHROPIC_API_KEY` can force API-key behavior in
print mode; never inherit it accidentally into a subscription/proxy profile. Requested
base URL is not effective-route proof.

Windows custom executable path is a real absolute `.exe` where required. Exact force
is advertised only on an exact custody backend; degraded Unix fallback rejects it.

## L7 cases

- init before/after first message and every lost-reply point;
- no original prompt replay on resume/restart;
- additionalContext turn spill prevention;
- exact permission/question ids;
- multi-block event dedupe and source handoff;
- child provenance/completion and cumulative usage;
- minimal environment/proxy billing isolation;
- old-generation callback rejection and exact/degraded custody.
