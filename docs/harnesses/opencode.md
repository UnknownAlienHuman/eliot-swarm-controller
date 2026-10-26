# OpenCode

Layer L4.4. Plugin: `plugins/opencode`. It may use `session_mode=shared` **only** when
the installed server schema provides exact session identity, per-session tool
environment, event/readback correlation and isolated session stop. Otherwise controlled
OpenCode uses one server per Eliot agent; a shared owner server may be observe-only.

Facts are selected from installed `GET /doc`. Public source review on 2026-10-26 showed
`PUT /api/session/{sessionID}/environment` and shell spawning from that environment;
this is promising but does not replace installed live qualification.

## API generation selection

At initialize: read version/OpenAPI, select one complete supported generation, record it
in manifest/effective settings/fixtures, and reject missing routes rather than mixing
generations. A 404/405 proves absence only after complete response from intended server.
Temporary model-catalogue refresh may be retried only when response proves no session/
input was admitted.

## Shared-control prerequisites

Live qualification must prove:

- exact session/directory create/read;
- per-session environment set/read before first prompt;
- two concurrent sessions' tools receive different Eliot token/generation/Git identity;
- abort/close one cannot affect another;
- events/messages/children carry exact session/input/execution ids and recover after SSE
  gap;
- owner-run attached server is never stopped by agent lifecycle.

If session environment is absent/not used by tools, use per-agent managed server. Never
share and silently attribute all tools/commits to one token.

## HTTP and async input outcome

2xx is endpoint-specific. `prompt_async` 204 proves only native inbox admission. The
plugin retains exact Eliot command ↔ native input/message id, keeps `agent.command`
pending, and does not free Eliot's send lane until:

- exact execution/turn starts and includes that input → send applied + turn mapping;
- exact readback proves rejection/drop before execution → failed;
- absolute deadline expires after possible admission → unknown.

Unknown inbox input blocks another send/configure/goal for that generation until exact
readback. It is never resubmitted. If one native execution legitimately drains several
already-admitted inputs, emit one turn with ordered `input_commands`; all waits resolve
from that one terminal result.

Complete 4xx is rejected only when endpoint semantics prove no effect. Transport/
timeout/ambiguous server failure after write is unknown.

## Verb mapping

| Eliot | OpenCode |
| --- | --- |
| launch | create session, set per-session env, read effective directory/model/agent, ready; initial input is separate linked command |
| send | async prompt; applied only at exact execution association, not 204 |
| steer | only recorded exact-current-execution mechanism; queue is not steer |
| inject | recorded safe point, otherwise explicit prepend-next-send |
| interrupt | exact session/execution abort; verify child scope |
| stop | close/abort exact session; never kill shared server for one agent |
| resume | exact persisted session attach; never resend old input |
| fork/background | installed native route with exact readback |
| approve/deny/answer | exact native request id/offered values |
| configure | installed session/per-prompt setters plus readback/application point |
| goal | Eliot Goal until native goal qualified |

## Observation and recovery

SSE owns live facts. Final text/status comes from fully paged native reads. Stable event/
message/part/input/execution ids suppress replay duplicates. After gap, rebuild current
state and report unbridgeable interval. Active-list absence/update timestamp is not
liveness. If execution disappeared without terminal result, return
`not_active_unknown`, not idle completion.

Child queries are paged; parent disappearance does not finish children. Read-only SQLite
discovery is a fallback for facts absent from API and never a concurrent duplicate
source.

## Provider/proxy configuration

Use selected installed provider schema and generated per-session config; never edit
owner global config. Report requested/effective provider/model/endpoint/billing
separately. Split provider/model only where schema defines it.

## L7 retained failures

- async 204 followed by delayed start, timeout, dropped input and multi-input execution;
- volatile SSE recovery and missing terminal → unknown turn;
- per-tool Git snapshot contention;
- owner desktop restarting attached service;
- old schemas missing parent/input correlation;
- stale generation abort/result;
- two-session environment/token/Git isolation;
- session stop/force never affecting another shared session.
