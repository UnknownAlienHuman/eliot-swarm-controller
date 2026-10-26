# OpenCode

Layer L4.4, `plugins/opencode`. `session_mode=shared` is allowed only when installed
server provides exact session identity, per-session tool environment, event/readback
correlation and isolated session stop. Otherwise controlled OpenCode runs one managed
server per Eliot agent; shared owner server may be observe-only.

Facts come from installed `GET /doc`. Public source review on 2026-10-26 showed
`PUT /api/session/{sessionID}/environment` and shell spawning from that environment,
but environments are process-local Map state, so restart persistence is not assumed.
Installed live qualification is mandatory.

## API generation and service ownership

At initialize: read version/OpenAPI, select one complete supported generation, record in
manifest/effective settings/fixtures and reject missing routes rather than mix versions.
404/405 proves absence only after complete response from intended server. Temporary
catalogue refresh may retry only with proof no session/input was admitted.

Shared/managed server PID and descendants belong to `service:<name>` custody. They are
never submitted through `process.put` as Agent-owned. Agent session stop uses exact
native session API and cannot kill server. Per-session tool PID data is diagnostic unless
OS proves a separately contained per-agent descendant; it does not change service
ownership.

The plugin opens stable source epochs per session/family. SSE lifecycle and paged
readback use commit-ack source sequence; lost acknowledgement resends identical envelope.
A server reconnect opens a new source epoch only after old cursor closes/gaps.

## Shared-control prerequisites

Live qualification proves:

- exact session/directory create/read;
- per-session environment set/read before first prompt;
- two concurrent sessions' tools receive different Eliot token/generation/Git identity;
- abort/close one cannot affect another;
- events/messages/children carry exact session/input/execution ids and recover after SSE
  gap without duplicate lifecycle;
- owner-run attached server is never stopped by agent lifecycle;
- shared server process remains service-owned.

If any fails, use per-agent managed server or observe-only. Never share while attributing
all tools/commits to one token.

### Service incarnation and environment rebind

Record exact service incarnation from endpoint + managed process boot/birth identity, or
installed attached-server identity/readback. Every session binding records incarnation
where Eliot token, generation, Git identity, synthetic HOME and account env were set.

After disconnect/restart/incarnation change:

1. freeze new input/configure/Goal for affected sessions;
2. reattach/read exact session and in-flight execution;
3. missing terminal execution → `not_active_unknown`, never idle success;
4. retain current Agent generation/token version;
5. reapply complete frozen per-session environment before any new tool/input;
6. verify readback or rerun explicit two-session environment probe before shared control;
7. close/gap old SSE source and open a new stable source epoch.

If rebind cannot be proved, session becomes observe-only or route rejects. A restart
never silently removes binding gate.

## HTTP and async input

`prompt_async` 204 proves only inbox admission. Plugin retains Eliot command ↔ native
input/message id and keeps command pending until:

- exact execution/turn starts with that input → send applied + turn mapping;
- exact readback proves pre-execution drop/rejection → failed;
- absolute deadline after possible admission → unknown.

Unknown input blocks send/configure/Goal in generation and is never resubmitted. One
native execution may drain several admitted inputs; emit one turn with ordered
`input_commands`. Complete 4xx rejects only when endpoint semantics prove no effect;
transport/timeout/ambiguous write is unknown.

## Verb mapping

| Eliot | OpenCode |
| --- | --- |
| launch | create session, install/read environment/effective directory/model/agent, commit ready; initial input linked separately |
| send | async prompt, applied only at exact execution association |
| steer | only recorded exact-current-execution mechanism; queue is not steer |
| inject | recorded safe point, otherwise explicit prepend-next-send |
| interrupt | exact session/execution abort; verify child scope |
| stop | exact session close/abort; never kill shared service |
| resume | persisted attach + current-incarnation env rebind; never resend old input |
| fork/background | installed route with exact readback |
| approve/deny/answer | exact request id/offered values |
| configure | installed session/per-prompt setter + application point/readback |
| goal | Eliot Goal until native goal qualified |

## Observation, children and recovery

SSE owns live facts. Final text/status requires fully paged native reads. Stable event,
message, part, input and execution IDs suppress vendor replay; Eliot source sequence
handles Store lost ack. Gap rebuilds current state and reports missing interval. Active
list absence/timestamp is not liveness; disappeared execution without terminal is
unknown turn.

Child queries are paged and parent disappearance never finishes child. Child registration
uses exact scope/ref and stable registration id. Declare `child_environment=true` only
if child-specific environment/tool isolation is proved separately from parent session.
Otherwise child row may be observable but its CLI/Git identity is parent/session and no
child token is minted.

Read-only SQLite discovery is fallback for API-missing facts and never concurrent
lifecycle producer.

## Provider configuration

Use selected installed provider schema and frozen per-session configuration; never edit
owner global config. Report requested/effective provider/model/endpoint/billing. Split
provider/model only where schema defines it.

## L7 retained failures

- lost source-open/SSE event/process registration replies;
- async 204 delayed start, timeout, dropped input and multi-input execution;
- restart with persisted session but lost process-local environment: no work before
  exact token/Git/environment rebind;
- vendor duplicate/gap/missing terminal → unknown turn;
- two-session environment/token/Git and child attribution isolation;
- service PID never Agent-owned; session stop/force never affects another session or
  server;
- per-tool Git snapshot contention, owner desktop restart and old-schema correlation;
- stale generation abort/result.
