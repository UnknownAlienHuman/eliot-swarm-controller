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

The host-owned store root is passed at `initialize` as `store_root`; the plugin
derives the per-Agent store as `opencode-{agent_id}/data` under that root. It
equals the old `instance_dir` parent (`{data_root}/plugins`), so the on-disk
layout and per-Agent identity are unchanged across generations.

Server custody is separate from Agent custody. Adopted services are never signalled or
repaired. New/uncertain controller takeover increments Eliot generation; exact recovery
of the same live controller may preserve it.

## Transport

Subscribe before session create/input. Route SSE by exact session/directory, never
cwd/title/time heuristics. Missing identity is a visible gap. Session routes bind their
location from the session; requests outside a session pass the installed API's workspace
parameter. Read complete error bodies; only authoritative
rejection proves no effect. Timeout/ambiguous server response after a write is unknown.
Catalogue preflight may retry only before any session/input may have been admitted.

## Native mapping

| Eliot operation | Mechanism / settlement |
| --- | --- |
| launch | installed `POST /api/session` with the workspace directory, no input; per-Agent `serve` on loopback with a generated `OPENCODE_PASSWORD` and per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`; before `serve`, the frozen nonsecret `auth_integration` selector's canonical credential moves opaquely into the isolated store (the selection must hold exactly one credential, never all integrations; explicit resume reuses a correctly present one; an unknown import settles only by proven child exit plus native `auth list` readback, never by retry); exact session/model/directory echo required before Eliot `session.ready` |
| send | installed `POST /api/session/{id}/prompt`; the 200 inbox id is intermediate, applied needs the exact input-to-execution association on SSE (`session.inbox.enqueued`/`delivered` plus `session.execution.started`; turn id is the exact `started` event id) |
| steer | exact-current-execution method only; queueing another prompt is not steer |
| inject | verified safe point or explicit next-send prepend |
| interrupt | installed `POST /api/session/{id}/interrupt` (`{"interrupted": bool}`); the exact-turn gate refuses a finished turn as stale without a write, `interrupted:true` applies and the terminal pair settles the turn |
| stop | installed interrupt-quiesce (`POST .../interrupt`) plus `GET /api/session/{id}` readback proving the session is retained for resume (proven retained applies; proven absent or ambiguous readback is unknown), then the owned per-Agent server ends through the generation's process custody; stop ends the directly spawned `serve` child while the generation Job owns any descendants it spawned; ordinary stop never DELETEs the session; abort-plus-detach is **not** stop |
| detach | explicitly stop controlling/observing as defined, leaving native service/session alive |
| resume | installed exact native session reattachment: `GET /api/session/{id}` proves the retained session, then the interrupt-quiesce (`POST .../interrupt`, which aborts a running retained execution — refusing would wedge crash-recovery resume) plus the empty `inbox`/`permission`/`form` lists prove it idle (anything still pending refuses the resume); never replays the prompt |
| approve/deny/answer | installed `POST .../permission/{id}/reply` (`once`/`always`/`reject`; approve must name `once` or `always`, there is no default choice), `POST .../form/{id}/reply` (`answer` map), `DELETE .../form/{id}` for deny; the public request id is the native id, choices stage against the offered schema (a miss rejects before any write), a lost reply is unknown and never rewritten |
| configure | resume-time only: installed `POST .../model` / `POST .../agent` setters realign the retained session with `session.get` effective readback (a refused switch fails the resume as unknown); no mid-turn configure verb |
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

A restart of the externally owned service while input is queued or running is decided
when this harness is implemented: either proven continuation of that input on the same
store, or proven final interruption. Until one is shown, the input stays unknown and is
never replayed.

## Children, configuration and final checks

Page all native history/child lists and retain exact parent ids. Parent idle does not
finish children. Native database discovery, if needed, is read-only and not a parallel
publisher. Generate per-Agent/server provider config; never edit owner global settings.
The profile model is the exact catalog string `provider/model-id`. Split only its first
slash: `openrouter/stealth/crouton-alpha` selects provider `openrouter` and model id
`stealth/crouton-alpha`, without shortening or substituting it. This follows
[v2.0.32 `Model.Ref.parse`](https://github.com/anomalyco/opencode/blob/v2.0.32/packages/schema/src/model.ts#L27).
Empty slash segments, backslash separators, whitespace and control characters reject.
The native `#variant` suffix is not mapped by this slice and rejects; requests retain
`variant=None` and the existing exact requested/effective model comparison.

L7 verifies two-session isolation when shared mode is selected, adopted-service safety,
204-vs-execution, lost stream/input, snapshot races, pagination, exact controls and no
historical lifecycle duplication. Unsupported API semantics remain explicit.
