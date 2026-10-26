# OpenCode

Layer L4.4. Plugin: `plugins/opencode`. It may use `session_mode=shared` **only** when
the installed server schema provides exact session identity, per-session tool
environment, event/readback correlation and isolated session stop. Otherwise controlled
OpenCode uses one server per Eliot agent; a shared owner server may be observe-only.

Facts are selected from installed `GET /doc`. Public source review on 2026-10-26 showed
`PUT /api/session/{sessionID}/environment` and shell spawning from that environment.
The same source stores session environments in a process-local Map, so environment
survival across server restart must **not** be assumed. Installed live qualification is
still mandatory.

## API generation selection

At initialize: read version/OpenAPI, select one complete supported generation, record it
in manifest/effective settings/fixtures, and reject missing routes rather than mixing
generations. A 404/405 proves absence only after a complete response from the intended
server. Temporary model-catalogue refresh may be retried only when the response proves
no session/input was admitted.

## Shared-control prerequisites

Live qualification must prove:

- exact session/directory create/read;
- per-session environment set/read before first prompt;
- two concurrent sessions' tools receive different Eliot token/generation/Git identity;
- abort/close one cannot affect another;
- events/messages/children carry exact session/input/execution ids and recover after an
  SSE gap;
- owner-run attached server is never stopped by agent lifecycle.

If session environment is absent or not used by tools, use a per-agent managed server.
Never share and silently attribute all tools/commits to one token.

### Service incarnation and environment rebind

The plugin records an exact `service_incarnation` from endpoint plus server process
birth/boot identity where managed, or from an installed server-instance identity/readback
where attached. Every controlled session binding records the incarnation on which its
Eliot token, generation, Git identity, synthetic HOME and account environment were
installed.

After a service disconnect/restart/incarnation change:

1. freeze new input/configure/goal dispatch for all affected sessions;
2. reattach/read exact native session and in-flight execution state;
3. treat any missing terminal execution as `not_active_unknown`, never idle success;
4. generate/retain the current Eliot token according to the existing Agent generation;
5. reapply the complete frozen per-session environment before any new native tool/input;
6. verify environment readback when the installed API exposes it; otherwise run the
   explicit two-session tool-environment qualification probe before declaring shared
   control available on that installed version;
7. create a new authoritative source ownership epoch after the old SSE cursor is closed
   or marked gapped.

A service restart never silently clears the binding gate. If exact environment rebind
cannot be proved, the session becomes observe-only or the route is rejected; Eliot does
not keep sending work with missing/stale token or Git identity. An old raw token left in
the previous process is harmless after token-hash revocation/rotation.

## HTTP and async input outcome

2xx is endpoint-specific. `prompt_async` 204 proves only native inbox admission. The
plugin retains exact Eliot command ↔ native input/message id, keeps `agent.command`
pending, and does not free Eliot's send lane until:

- exact execution/turn starts and includes that input → send applied plus turn mapping;
- exact readback proves rejection/drop before execution → failed;
- absolute deadline expires after possible admission → unknown.

Unknown inbox input blocks another send/configure/goal for that generation until exact
readback. It is never resubmitted. If one native execution legitimately drains several
already-admitted inputs, emit one turn with ordered `input_commands`; all waits resolve
from that one terminal result.

A complete 4xx is rejected only when endpoint semantics prove no effect. Transport,
timeout or ambiguous server failure after a write is unknown.

## Verb mapping

| Eliot | OpenCode |
| --- | --- |
| launch | create session, install/read per-session environment and effective directory/model/agent, then ready; initial input is a separate linked command |
| send | async prompt; applied only at exact execution association, not 204 |
| steer | only recorded exact-current-execution mechanism; queue is not steer |
| inject | recorded safe point, otherwise explicit prepend-next-send |
| interrupt | exact session/execution abort; verify child scope |
| stop | close/abort exact session; never kill shared server for one agent |
| resume | exact persisted session attach plus current-incarnation environment rebind; never resend old input |
| fork/background | installed native route with exact readback |
| approve/deny/answer | exact native request id/offered values |
| configure | installed session/per-prompt setters plus readback/application point |
| goal | Eliot Goal until native goal qualified |

## Observation and recovery

SSE owns live facts. Final text/status comes from fully paged native reads. Stable event,
message, part, input and execution ids suppress vendor replay; Eliot source sequencing
provides Store-side lost-ack idempotency. After a gap, rebuild current state and report
an unbridgeable interval. Active-list absence or update timestamp is not liveness. If an
execution disappeared without terminal result, return `not_active_unknown`, not idle
completion.

Child queries are paged; parent disappearance does not finish children. Read-only SQLite
discovery is a fallback for facts absent from API and never a concurrent duplicate
source.

## Provider/proxy configuration

Use the selected installed provider schema and generated per-session configuration;
never edit owner global config. Report requested/effective provider, model, endpoint and
billing separately. Split provider/model only where that schema defines it.

## L7 retained failures

- async 204 followed by delayed start, timeout, dropped input and multi-input execution;
- service restart after session persistence but process-local environment loss: no new
  work before exact environment/token/Git rebind;
- volatile SSE recovery and missing terminal → unknown turn;
- per-tool Git snapshot contention;
- owner desktop restarting its attached service;
- old schemas missing parent/input correlation;
- stale generation abort/result;
- two-session environment/token/Git isolation before and after service restart;
- session stop/force never affecting another shared session.
