# OpenCode

Layer L4.4. Plugin is an HTTP/SSE client implemented against one installed OpenCode API
generation selected from `GET /doc`; do not mix public/v2/v1 endpoints opportunistically.

Facts checked 2026-10-25/26 against `anomalyco/opencode` release v1.19.4, official
server/SDK/provider/permission docs and OpenCode source. Waku's current OpenCode driver is
the donor for ownership/reconnect semantics: one shared event hub, exact session
demultiplexing, subscribe before create, adopted service never killed and reconnect by
readback rather than fabricated completion.

## Native identity and process mode

```text
native_scope = opencode:<canonical server identity/URL + persistent store identity>
native_ref   = exact OpenCode session id
```

One server can host many sessions, but shared control is allowed only after the installed
API proves all of:

- every event/request/readback is attributable to exact session id;
- each session's tools receive isolated Eliot token/Git/account environment before input;
- one session can be aborted/stopped without killing or mutating another;
- server restart lets environment be restored before the next input.

Otherwise Eliot launches one server per Agent generation or attaches owner server
sessions observe-only. The server PID belongs to service custody, not Agent custody.
Owner-run/adopted server is never killed or “repaired” by Eliot.

First control creates generation 1; control takeover/resume after server/session
reattach creates the next generation unless the same current controller ownership is
proven. SSE/readback/plugin messages all carry Eliot generation.

## Transport

- start/attach `opencode serve` on loopback;
- SSE from the exact installed `/event` or `/global/event` route;
- install one server-level event hub and demultiplex by exact session id; events missing
  session/directory identity are dropped with source health/gap, never guessed;
- subscribe before creating/sending to a session so no early event is lost;
- carry exact workspace/directory parameter on every session request;
- read full HTTP error bodies. A complete client error may prove rejection; transport
  failure/timeout/ambiguous server error after write is unknown.

A temporary model-catalogue startup retry is allowed only when the complete native error
proves no session/input was admitted.

## Source ownership and reconnect

For controlled sessions, the exact-session SSE stream owns lifecycle families it
provides. Native message/session reads are readback and bounded repair, not a second
concurrent publisher. Observe-only sessions may use native history only when the
installed API exposes an opaque exclusive cursor.

Most OpenCode event streams do not provide such a durable history cursor. Therefore on
SSE break/reconnect:

1. mark stream stale; do not mark Agent/session lost merely because transport broke;
2. reconnect the shared hub and increment its transport generation;
3. discard stale resync work from an older transport generation;
4. read current session, active execution, permissions/forms/inbox and paged current
   messages;
5. gap-fill only the known active assistant step/output using exact message/part ids;
6. record `source.gap` for the uncovered stream interval;
7. continue at the new live edge.

Do **not** replay the full old message history after event retention merely because ids
are available. `native_id` removes bounded overlap only. A running execution absent from
an active list is not terminal without the exact native delimiter/result; silence and
session update time prove nothing.

If an installed version provides a true exclusive durable session-log cursor, record it
and resume strictly after it; update this page/fixture for that version.

## Verb mapping

Every request is bound to Eliot generation. Controls use exact current execution/request
where the installed API exposes it; otherwise the verb is rejected.

| Eliot verb | Native operation | Settlement |
| --- | --- | --- |
| `launch` | create exact session without prompt | commit `session.ready`; linked input is separate |
| `send` | installed async prompt/inbox endpoint | HTTP 204/inbox enqueue is intermediate; applied only when exact native execution starts and names/contains this input |
| `steer` | exact active-execution steer only | queueing another prompt is not steer; reject until recorded |
| `inject` | verified instruction/safe-point mechanism or prepend to next send | report actual mechanism/application point |
| `interrupt` | exact session/execution abort | unknown is read back, never retried; verify child scope |
| `stop` | exact session close/abort where supported, then detach tracking | never kill shared/adopted server for one Agent |
| `resume` | attach exact persisted session id as a new Eliot generation | old input never resent |
| `fork` | exact session fork | new native subject/new Agent |
| `background` | installed exact background operation | 204 is not proof; settle by exact resulting notice/state |
| `approve|deny` | exact permission id reply | use offered values; bind request/generation |
| `answer` | exact installed user-input id reply | otherwise unsupported |
| `goal.*` | Eliot Goal | no qualified generic native Goal |
| `configure` | installed per-session/per-prompt model/agent controls | read back and report application point |

Several input commands may join one native execution. `turn.started` lists all associated
command ids. An unknown input keeps Eliot's input lane blocked until inbox/execution
readback proves association/drop/unknown.

## Current-state reconciliation

Waku's ordering is retained:

1. permission/forms snapshots first—a pending native request proves the resumed turn is
   live;
2. inbox snapshot settles only whether a pending input remains undelivered;
3. active-session/execution snapshot;
4. gap-fill current assistant output;
5. exact native idle/terminal delimiter settles the turn; otherwise keep running/unknown.

A server restart can suspend a run temporarily. A grace/readback path may wait for native
resume, but absence from the active list alone never manufactures completion.

## Children, messages and discovery

- route every event by exact session/parent id, never title/cwd/time/count;
- child lists/history are paged; parent absent from active set may still have live
  children;
- final text requires following message pagination to the terminal record;
- native store/database may be opened read-only for discovery only when API lacks that
  fact; never write it and never make it a parallel lifecycle source.

## Provider/proxy configuration

Generate per-Agent/server config or `OPENCODE_CONFIG_CONTENT`; never edit owner global
config. Report requested and effective provider/model/endpoint/billing route separately.
Model ids may contain `/`; follow installed schema instead of flattening.

## Retained L7 cases

- two controlled sessions prove isolated token/Git/account environment and stop;
- owner/adopted server is never killed;
- HTTP accepted input is not applied until execution association;
- stream disconnect while run continues yields stale+gap+readback, not terminal;
- reconnect generation discards stale resync;
- current-output gap fill does not republish old messages/rules;
- exact request/input/turn ids survive pagination and restart;
- no installed exact steer/answer is reported as supported.
