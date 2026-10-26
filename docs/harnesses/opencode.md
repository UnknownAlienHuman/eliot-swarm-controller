# OpenCode

Layer L4.4. Plugin: `plugins/opencode`. It may use `session_mode=shared` **only** when
the installed server schema provides exact session identity, per-session tool
environment, event/readback correlation and isolated session stop. Otherwise controllable
OpenCode runs one server per Eliot agent (`per_agent`); a shared owner server may still
be observe-only.

Facts must be selected from the installed `GET /doc` schema. Public/current source
review on 2026-10-26 also showed a stable `PUT /api/session/{sessionID}/environment`
route and shell spawning from that session environment; this is promising but not a
substitute for checking the owner's installed version.

## API generation selection

OpenCode has had several endpoint generations. At initialize:

1. read server version and OpenAPI document;
2. select one supported generation as a complete contract;
3. record it in manifest/effective settings/fixtures;
4. reject missing required routes rather than mixing generations.

A 404/405 proves absence only after the complete response from the intended server is
read. A newly started server's temporary incomplete model catalogue may be retried only
when the response proves no session/input was admitted.

## Shared-control prerequisites

Before returning `session_mode=shared` for control, live qualification must prove:

- create/read one exact session and directory;
- set/read its environment before first prompt;
- shell/tool subprocess receives that session's Eliot token, generation and Git
  identity, while another session receives different values;
- session abort/close cannot affect another session;
- events/messages/children carry exact session/native ids and can be rebuilt after SSE
  gap;
- attached owner-run server is never stopped by agent lifecycle.

If `session.environment` is absent or not applied to tools, use a per-agent managed
server with profile environment. Do not share a server and silently attribute all tool
CLI calls/Git commits to one token.

## HTTP outcome rule

2xx uses endpoint-specific meaning; 204 async prompt is inbox admission, not turn
completion. A complete 4xx may be rejected when endpoint semantics prove no effect.
Timeout/transport/ambiguous server error after write is unknown and resolved only from
exact native input/message/execution state—never retried against the current turn.

## Verb mapping

| Eliot | OpenCode |
| --- | --- |
| launch | create exact session, set per-session environment, read effective directory/model/agent, then `session.ready`; optional initial input remains a separate linked command |
| send | async prompt with retained native input/message identity |
| steer | only a recorded exact-current-execution mechanism; queue is not steer |
| inject | recorded instruction/safe-point mechanism, otherwise explicit prepend-next-send |
| interrupt | exact session/execution abort; verify child scope |
| stop | close/abort exact session; never kill shared server for one agent |
| resume | exact persisted session attach/resume; never resend old input |
| fork/background | installed native route with exact readback |
| approve/deny/answer | exact native request id and offered values |
| configure | installed session/per-prompt setters plus readback/application point |
| goal | Eliot Goal until a native goal is qualified |

## Observation

SSE is authoritative while connected. Final text/status comes from paged native reads;
follow every cursor. Stable native event/message/part/execution ids suppress replay
duplicates. After a gap, rebuild current state and report any unbridgeable interval.
Session update timestamp/active-list absence is not liveness. Child queries are paged
and parent disappearance does not finish children.

Read-only SQLite discovery is a fallback for facts absent from API and never writes the
live WAL. It does not run concurrently as a duplicate lifecycle producer.

## Provider/proxy configuration

Use installed provider schema and a generated per-agent/session configuration; never
edit owner global config. Report requested and effective provider/model/endpoint/
billing separately. Model ids containing `/` are split only where the selected schema
defines provider + model.

## L7 retained failures

- per-tool Git snapshots/index contention;
- volatile SSE recovery;
- owner desktop restarting its own attached service;
- missing parent/input correlation in older schemas;
- stale generation abort/result;
- per-session environment isolation across two concurrent managed agents;
- session stop/force never affecting another shared session.
