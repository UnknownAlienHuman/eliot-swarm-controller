# OpenCode

Layer L4.4. Plugin: `plugins/opencode` (Rust or TypeScript using the selected official
SDK/schema). Manifest mode is `shared`: one plugin may control many sessions because
`opencode serve` already isolates them by native session id. Stopping one Eliot agent
uses only that session's API and never kills the shared plugin/server. If the plugin
starts a server, that server is a managed service with separate custody and refcount.

Facts checked 2026-10-25 against `anomalyco/opencode` (the former `sst/opencode`
location redirects), release v1.19.4 dated 2026-10-22, and official server/SDK/provider/
permission/troubleshooting pages updated 2026-10-24.

## Select one installed API generation

Current documentation and owner installations may expose different generations. v1
Eliot used a `/api/...` surface from an OpenCode 2.0.7 build; current public docs also
describe non-`/api` endpoints and an SDK v2. L4.4 must:

1. start/attach to the owner's installed server;
2. fetch its OpenAPI document (`GET /doc`) and version;
3. choose one supported generation in the profile/plugin;
4. implement only that generation and record it.

Do not mix endpoints opportunistically. A 404/405 proves a route absent only after the
body is read and the request definitely reached the intended server/version.

## Transport and identity

- `opencode serve --port ... --hostname 127.0.0.1`; one server, many native sessions.
- SSE `GET /event` / `GET /global/event`; first connection event is server-connected
  in current docs.
- Every session request carries the exact workspace/directory parameter required by
  the installed schema. A server default cwd is not an acceptable substitute.
- Global SSE may mix directories. Route only events with exact session/directory
  ownership; an event lacking required identity is a source gap, not guessed by time.
- The plugin binds each native session to one Eliot `(agent_id,generation)`. Resume or
  controlled re-attach to the same still-live incarnation preserves generation; an
  explicit replacement/resume after terminal state increments it.
- The owner-run service registry may identify URL/pid/version and a configured password
  variable. Read credentials only from the named environment variable, never logs or
  the registry file itself.

A newly started server can briefly advertise an incomplete model catalogue. A bounded
retry is allowed only for the **pre-admission catalogue refresh error** that proves no
session/prompt was accepted. Once a native write may have occurred, use readback and
never repeat it.

## HTTP outcome rule

- 2xx: use endpoint-specific semantics (for example 204 may mean accepted to inbox,
  not executed).
- 400/401/403/404/405/409/413/422: rejected only when the complete response proves no
  effect for that endpoint.
- transport failure, timeout or any ambiguous server error after write: `unknown`.

Unknown is resolved by exact session/message/input/turn readback. It is never retried
against whatever turn is current.

## Verb mapping

| Eliot verb | OpenCode | Notes |
| --- | --- | --- |
| `launch` | create session, then optional async prompt | Emit `session.ready` with exact session id/effective model/agent/directory before success. A lost create/prompt reply requires readback; no replacement session. |
| `send` | installed async-prompt endpoint | 204/accepted means inbox admission only. Track native input/message identity through delivery/execution/terminal facts where available. |
| `steer` | only if installed schema exposes exact current-turn steer | Reject until recorded. Queueing another prompt is not steer. |
| `inject` | verified instruction/safe-point mechanism, otherwise prepend to next send | Report mechanism and application point. |
| `interrupt` | installed session abort endpoint | Bind the exact current execution/turn where the API exposes it. Verify child scope. Unknown is not retried. |
| `stop` | abort/close exact session and stop tracking | Never stop the shared server for one agent. Force is supported only if native API isolates that session. |
| `resume` | attach to persisted session id / installed resume operation | Old input is never resent. Idle/list absence does not settle a previous input. |
| `fork` | installed session fork endpoint | Result is a new native session/new Eliot agent. |
| `background` | verified background endpoint | A 204 is not proof the intended tool moved; settle by exact synthetic/native notice. Idle no-op is distinct. |
| `approve` / `deny` | exact permission id reply | Use native response values; bind generation/request. |
| `answer` | only when installed schema exposes exact user-input request/reply | Otherwise unsupported. |
| `goal.*` | Eliot Goal loop | No qualified native goal. |
| `configure` | per-prompt or installed session model/agent setters | Confirm by session readback and report `applies`. Split provider/model at the first `/` only where schema defines that pair. |

## Observation and source ownership

- Live SSE owns controlled session lifecycle while connected.
- Final text/status comes from paged native message/session reads. Follow every cursor
  to completion; v1 lost results after the first 50 messages.
- After an SSE gap, rebuild current state from native reads. Do not re-publish old
  lifecycle events; use stable native message/part/execution ids. If the API cannot
  bridge the cursor exactly, report a gap.
- Session update time may not move while tools run; it is not liveness.
- Child lists/queries are paginated. A parent missing from `active` may still have live
  children. Parent/child relation, not title/cwd/time, defines the family.
- Read-only discovery may inspect the native SQLite/WAL only through safe read-only
  access and only when the installed server API lacks the fact. Never write its DB.
  Live SSE/message APIs remain the normal controlled source.

## Proxy/provider configuration

Configure a custom provider through the installed OpenCode schema using a generated
per-agent/session config or `OPENCODE_CONFIG_CONTENT`; never edit owner global config.
Report requested provider, model, effective server values and billing route
separately. Model availability comes from the attached server, not a public catalogue.

## v1 failures retained for L7

- Per-tool Git snapshots caused index contention; recommend/record `snapshot=false`
  for managed agents only when the installed version supports it.
- Event streams were volatile; recovery uses native messages, not replay assumptions.
- The desktop app may restart its owner-run service; Eliot must distinguish attached
  service from one it owns.
- Old schemas lacked enough parent/input correlation. Do not declare control/result
  support unless the selected installed API supplies the exact ids.
- A late abort/result from an old generation cannot mutate a replacement session.
