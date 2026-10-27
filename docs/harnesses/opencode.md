# OpenCode

L6 adapter. Select one installed HTTP/SSE API generation from its OpenAPI document
(`GET /doc` where supported); do not mix generations after a 404. Source:
[anomalyco/opencode](https://github.com/anomalyco/opencode). Existing notes were checked
2026-10-25/26; installed behavior remains to be recorded during implementation.

Waku's [service](https://github.com/egoist/waku/blob/main/crates/waku-core/src/opencode_service.rs)
and `driver/opencode.rs` are pattern-only references: subscribe before create, exact
session routing, adopted service ownership and reconnect by readback. No GPL code copying.

## Native identity and process mode

Subject is `(canonical server/store identity, exact native session id)`. Use one server
per Agent unless the installed API demonstrates exact session event/request attribution,
isolated tool environment, isolated stop and restoration of that environment after
restart. Otherwise an owner-run shared session is observe-only.

Server custody is separate from Agent custody. Adopted services are never signalled or
repaired. New/uncertain controller takeover increments Eliot generation; exact recovery
of the same live controller may preserve it.

## Transport

Subscribe before session create/input. Route SSE by exact session/directory, never
cwd/title/time heuristics. Missing identity is a visible gap. Include the installed API's
workspace parameter on each request. Read complete error bodies; only authoritative
rejection proves no effect. Timeout/ambiguous server response after a write is unknown.
Catalogue preflight may retry only before any session/input may have been admitted.

## Native mapping

| Eliot operation | Mechanism / settlement |
| --- | --- |
| launch | create session without input; exact readiness, linked send separate |
| send | async prompt/inbox; 204 is intermediate, applied needs exact input-to-execution association |
| steer | exact-current-execution method only; queueing another prompt is not steer |
| inject | verified safe point or explicit next-send prepend |
| interrupt | captured session/execution abort, with installed child scope verified |
| stop | only a native close/disable operation that proves the session ended; abort-plus-detach is **not** stop |
| detach | explicitly stop controlling/observing as defined, leaving native service/session alive |
| resume | installed exact native session reattachment/resume; no old input replay |
| approve/deny/answer | exact native permission/input id and offered response schema |
| configure | supported session/prompt setters, effective readback and application point |
| fork/background | installed native method and exact resulting identity/state |
| goal | explicitly selected Eliot Goal; no assumed native Goal |

If a shared server has no isolated end operation, return `unsupported` for stop/force;
do not claim success after interrupt or kill the shared server. A per-Agent managed
server may be ended only through its owned custody.

Several inputs may join one execution. Persist all exact command associations; one
uncertain unassociated input holds the next input lane until association/drop/readback.

## Source ownership and recovery

Controlled exact-session SSE owns supplied lifecycle. Message/session reads repair
current state, not a second live event stream. On disconnect: mark stale, reconnect,
discard stale resync work, read permissions/forms/inbox/current execution, recover current
output by exact message/part ids, record the uncovered gap, then follow live events.

A local reconnect counter may order concurrent snapshots; it is not another persisted
Agent epoch. No guaranteed exclusive native history cursor means no historical replay.
If a selected API supplies one, use its exact continuation contract. List absence,
update time or silence never proves terminal execution.

## Children, configuration and final checks

Page all native history/child lists and retain exact parent ids. Parent idle does not
finish children. Native database discovery, if needed, is read-only and not a parallel
publisher. Generate per-Agent/server provider config; never edit owner global settings.
Follow installed model-id syntax, including slashes, and expose requested/effective route.

L7 verifies two-session isolation when shared mode is selected, adopted-service safety,
204-vs-execution, lost stream/input, snapshot races, pagination, exact controls and no
historical lifecycle duplication. Unsupported API semantics remain explicit.
