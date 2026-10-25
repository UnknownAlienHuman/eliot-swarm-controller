# OpenCode

Layer L4.4. Plugin: `plugins/opencode` (HTTP + SSE client; Rust or TypeScript with
`@opencode-ai/sdk`).

Facts checked 2026-10-25: repo [anomalyco/opencode](https://github.com/anomalyco/opencode)
(`sst/opencode` redirects there), latest release v1.19.4 (2026-10-22); docs
[server](https://opencode.ai/docs/server), [SDK](https://opencode.ai/docs/sdk),
[providers](https://opencode.ai/docs/providers),
[permissions](https://opencode.ai/docs/permissions),
[troubleshooting](https://opencode.ai/docs/troubleshooting) (pages updated
2026-10-24).

**Two API generations.** Current docs describe the endpoints below and also a v2 API
(`@opencode-ai/sdk/v2`). v1 Eliot used the v2 `/api/...` surface of a 2.0.7 build.
Pick one generation in L4.4 against the owner's installed version (`GET /doc`) and
implement only that one. The v2 paths v1 used: `GET /api/info` (version),
`POST /api/session/{id}/agent|model|background`, `GET /api/session/{id}/diff`,
`GET /api/fs/read/*`, `GET /api/experimental/session/{id}/log?follow=false`
(durable session log).

## Transport

- `opencode serve [--port 4096] [--hostname 127.0.0.1]`; one server, many sessions.
- Attach to an owner-run server, or let the plugin start one per workspace. Never
  drive sessions through repeated CLI invocations (v1: CLI recovery under load caused
  restart storms and lost writers).
- Events: SSE `GET /event` (first event `server.connected`), `GET /global/event`.
- One server can serve several directories: every session request carries the
  agent's directory (the exact parameter per the installed schema), otherwise the
  server falls back to its own cwd; `/global/event` mixes directories — drop events of
  other directories, and an event without directory metadata is a gap.
- OpenAPI: `GET /doc`.
- A freshly started server may report a configured model as unavailable until its
  catalogue refresh finishes (2.0.7 published a bundled catalogue first): retry the
  launch on that error only, for a bounded time (v1 used 15 s).
- HTTP writes: 400/401/403/404/405/409/413/422 mean rejected (not applied); any other
  non-2xx or a transport error means `unknown`, resolved only by reading back. A 404 or
  405 proves a missing route only if its body was read completely.

## Verb mapping (current docs endpoints)

| Eliot verb | OpenCode | Notes |
| --- | --- | --- |
| `launch` | `POST /session` then prompt | |
| `send` | `POST /session/:id/prompt_async` (204) | The 204 means accepted into the inbox, not executed. In the v2 log: `session.inbox.enqueued` → `session.inbox.delivered` → `session.execution.started`; one `execution.started` can cover several inputs (a busy period, not one turn). |
| `steer` | verify in L4.4 (v1 research found queue/steer in the v2 API; v1 rejected exact-turn steer) | Reject until verified. |
| `inject` | verify (v2 API had instruction entries applied at the next step); else prepend to next send | |
| `interrupt` | `POST /session/:id/abort` | Check whether abort also stops child sessions. |
| `stop` | abort + stop tracking; stop the server only if the plugin started it and nothing else uses it | |
| `resume` | sessions persist in the server DB; re-attach by id | After a server restart an input is settled only by its exact continuation or by the interrupted terminal the server writes for the previous execution; idle state, list absence or a new PID settle nothing. |
| `fork` | `POST /session/:id/fork` `{messageID?}` | |
| `background` | v2: `POST /api/session/{id}/background` (204, no body) | Moves running backgroundable tools to the background; idle is a no-op; the only proof is a new synthetic notice naming the moved work. Not interrupt, not steer. |
| `approve` / `deny` | `POST /session/:id/permissions/:permissionID` `{response: once|always|reject}` | `remember` → `always`. |
| `goal.*` | no native goal → Eliot Goal loop | |
| `configure` | v1-style API: model/agent per prompt; v2 API: session setters `POST /api/session/{id}/model` and `/agent` (apply to following turns, confirm by reading the session back) | A model is `providerID` + a model id that may itself contain `/` (e.g. `kilo` + `stepfun/step-5-preview-free`): split at the first `/` only, never flatten. Availability comes from this server's model list, not the public catalogue. |

## Observation

- Live events from SSE; final text from message reads. Message reads are paged: follow
  the cursor to the last page before treating a message as the final result (v1 failed
  on sessions over 50 messages).
- Activity comes from message/part updates; the session's update time does not move
  while tools run.
- Children: paginated `GET /session/:id/children` (on versions without it, a paginated
  query by `parentID`). `GET /session/active` lists running sessions, not the family:
  a parent missing from it may still have live children.
- The SSE stream is volatile: after a gap, rebuild from message reads.
- The installed client registers its server in `$XDG_STATE_HOME/opencode/service.json`
  (default `~/.local/state/opencode/service.json`): `url`, `pid`, optional `version`,
  `id` and local HTTP `password` — use it to find the owner's server URL; the
  password, if any, comes from the env var named in config (`password_env`), never
  from that file or a log.
- Data dir `~/.local/share/opencode` (`%USERPROFILE%\.local\share\opencode` on
  Windows) with `opencode.db` (SQLite: projects, sessions, messages). Read-only access
  for `discover`; never write; it is a live WAL database.

## Proxy

Custom provider in OpenCode config: `provider.<id>` with
`npm = "@ai-sdk/openai-compatible"`, `options.baseURL`, a `models` map. Pass it per
launch via `OPENCODE_CONFIG_CONTENT` or a generated config file
(`OPENCODE_CONFIG`) so the owner's config is not edited.

## v1 pitfalls

- Per-tool Git snapshots competed for the index; turning them off cut median edit
  time 42 s → 4.9 s (owner report, 2.0.7). Recommend `snapshot: false` for managed
  agents if the installed version still snapshots per tool.
- In 2.0.7 the event bus did not persist events by default; after a disconnect, rebuild
  from message reads, not from the event stream.
- The OpenCode desktop app restarted its own `serve --service` when the swarm stopped
  it: distinguish the owner's server from one the plugin started.
- An old pinned contract lacked the assistant `parentID`, so results could not be tied
  to inputs; check that the chosen API returns it.
