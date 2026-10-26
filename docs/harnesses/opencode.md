# OpenCode

Layer L4.4, `plugins/opencode`. `session_mode=shared` is allowed only when the installed
server provides exact session identity, per-session tool environment, event/readback
correlation and isolated session stop. Otherwise controlled OpenCode runs one managed
server per Eliot Agent; a shared owner server may still be observe-only.

Facts come from the installed `GET /doc`. Public source review on 2026-10-26 showed
`PUT /api/session/{sessionID}/environment` and shell spawning from that environment,
but environments are process-local Map state, so restart persistence is not assumed.
Installed live qualification is mandatory.

## API generation and service ownership

At initialize: read version/OpenAPI, select one complete supported generation, record it
in manifest/effective settings/fixtures and reject missing routes rather than mix API
generations. A 404/405 proves absence only after the complete response from the intended
server. Temporary catalogue refresh may retry only with proof no session/input was
admitted.

Shared/managed server PID and descendants belong to `service:<name>` custody. They are
never submitted through `process.put` as Agent-owned. Agent stop uses exact native
session API and cannot kill the server. Per-session tool PID data is diagnostic unless
OS proves separately contained per-Agent custody.

For launch or control attach, plugin persists and opens per-session source epochs before
session create/attach or any prompt/environment-triggered native work. SSE lifecycle and
paged readback use commit-ack source sequence. A server reconnect replaces the old epoch
atomically at an exact cursor or visible gap. Discovery/session/message/child reads are
paged below plugin frame limits.

## Shared-control prerequisites

Live qualification proves:

- exact session/directory create/read and control attach;
- owner-selected frozen profile/route/account compatibility and capacity reservation;
- per-session environment set/read before first prompt;
- two concurrent sessions' tools receive different Eliot token/generation/Git identity;
- abort/close one cannot affect another;
- events/messages/children carry exact session/input/execution ids and recover after SSE
  gap without duplicate lifecycle;
- owner-run attached server is never stopped by Agent lifecycle;
- shared server process remains service-owned.

If any fails, use a per-Agent managed server or observe-only binding. Never share while
attributing all tools/commits to one token.

### Service incarnation and environment rebind

Record exact service incarnation from endpoint + managed process boot/birth identity, or
installed attached-server identity/readback. Every controlled session binding records
the incarnation where Eliot token, generation, Git identity, synthetic HOME and account
environment were installed.

After disconnect/restart/incarnation change:

1. freeze new input/configure/Goal for affected sessions;
2. reattach/read exact session and in-flight execution;
3. missing terminal execution → `not_active_unknown`, never idle success;
4. retain current Agent generation/token version;
5. reapply complete frozen per-session environment before any new tool/input;
6. verify readback or rerun the explicit two-session environment probe before shared
   control;
7. replace old SSE source atomically at exact cursor/gap boundary.

If rebind cannot be proved, the row remains/becomes observe-only or route rejects. A
restart never silently removes this binding gate. Observe-only attach uses the synthetic
observe binding and consumes no control credentials; control upgrade requires frozen
profile and capacity as defined by the API.

## HTTP and async input

`prompt_async` 204 proves only inbox admission. Plugin retains Eliot command ↔ native
input/message identity and keeps command pending until:

- exact execution/turn starts with that command/input → send applied + turn mapping;
- exact readback proves pre-execution drop/rejection → failed;
- absolute deadline after possible admission → unknown.

Unknown input blocks send/configure/Goal in generation and is never resubmitted. One
native execution may consume several admitted inputs; emit one turn with ordered input
mappings. If an installed generation exposes command→execution but no stable input id,
the native id is optional. A complete 4xx rejects only when endpoint semantics prove no
effect; transport/timeout/ambiguous write is unknown.

## Verb mapping

| Eliot | OpenCode |
| --- | --- |
| launch | open sources, create session, install/read environment/effective directory/model/Agent, commit ready; initial input linked separately |
| send | async prompt, applied only at exact execution association |
| steer | only a recorded exact-current-execution mechanism; queue is not steer |
| inject | recorded safe point, otherwise explicit prepend-next-send |
| interrupt | exact session/execution abort; verify child scope |
| stop | exact session close/abort; never kill shared service |
| resume | new-generation source open + persisted attach + current-incarnation env rebind; never resend old input |
| fork/background | installed route with exact readback |
| approve/deny/answer | exact request id/offered values |
| configure | installed session/per-prompt setter + application point/readback |
| Goal | Eliot Goal until native Goal is qualified |

## Observation, children and recovery

SSE owns live facts. Final text/status requires fully paged native reads. Stable event,
message, part, input and execution identities use `event_id`; when the installed SSE has
only a resumable total-order cursor use `strict_cursor`; when neither is true, reconnect
is an explicit gap. Eliot source sequence handles lost Store acknowledgement separately.
Session update timestamp/active-list absence is not liveness; a disappeared execution
without terminal proof is unknown turn.

Child queries are paged and parent disappearance never finishes a child. Child
registration uses exact scope/ref and stable registration id. Declare
`child_environment=true` only if child-specific environment/tool isolation is proved
separately from the parent session. Otherwise child row may be observable/natively
controllable but `routing_enabled=false`; CLI/Git identity remains parent/session and no
child token is minted. Guard-exceeding vendor children remain recorded with reduced
authority/wake.

Read-only SQLite discovery is fallback for API-missing facts and never a concurrent
lifecycle producer.

## Provider configuration

Use the selected installed provider schema and frozen per-session configuration; never
edit owner global config. Report requested/effective provider/model/endpoint/billing.
Split provider/model only where the selected schema defines it.

## L7 retained failures

- source open committed before session create/control attach/prompt;
- lost source/handoff/SSE/process acknowledgements;
- async 204 delayed start, timeout, dropped input and multi-input execution;
- restart with persisted session but lost process-local environment: no work before
  exact token/Git/environment rebind;
- observe→control attach binding/capacity and detach still-live capacity retention;
- event-id/strict-cursor/no-replay recovery, event-row pruning and missing terminal;
- discovery/message/child pagination and frame bounds;
- two-session environment/token/Git and child routing attribution;
- service PID never Agent-owned; session stop/force never affects another session/server;
- per-tool Git snapshot contention, owner desktop restart and old-schema correlation;
- stale generation abort/result.
