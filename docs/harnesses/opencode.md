# OpenCode

Layer L4.3. Plugin: `plugins/opencode` (HTTP + SSE client; Rust or TypeScript with
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
Pick one generation in L4.3 against the owner's installed version (`GET /doc`) and
implement only that one.

## Transport

- `opencode serve [--port 4096] [--hostname 127.0.0.1]`; one server, many sessions.
- Attach to an owner-run server, or let the plugin start one per workspace. Never
  drive sessions through repeated CLI invocations (v1: CLI recovery under load caused
  restart storms and lost writers).
- Events: SSE `GET /event` (first event `server.connected`), `GET /global/event`.
- OpenAPI: `GET /doc`.

## Verb mapping (current docs endpoints)

| Eliot verb | OpenCode | Notes |
| --- | --- | --- |
| `launch` | `POST /session` then prompt | |
| `send` | `POST /session/:id/prompt_async` (204) | The 204 means accepted into the inbox, not executed. |
| `steer` | verify in L4.3 (v1 research found queue/steer in the v2 API; v1 rejected exact-turn steer) | Reject until verified. |
| `inject` | verify (v2 API had instruction entries applied at the next step); else prepend to next send | |
| `interrupt` | `POST /session/:id/abort` | Check whether abort also stops child sessions. |
| `stop` | abort + stop tracking; stop the server only if the plugin started it and nothing else uses it | |
| `resume` | sessions persist in the server DB; re-attach by id | |
| `fork` | `POST /session/:id/fork` `{messageID?}` | |
| `approve` / `deny` | `POST /session/:id/permissions/:permissionID` `{response: once|always|reject}` | `remember` → `always`. |
| `goal.*` | no native goal → Eliot Goal loop | |
| `configure` | model/agent per prompt | |

Children: `GET /session/:id/children`.

## Observation

- SSE stream for live events; message reads for final text.
- Data dir `~/.local/share/opencode` (`%USERPROFILE%\.local\share\opencode` on Windows)
  with `opencode.db` (SQLite: projects, sessions, messages). Read-only access for
  `discover`; never write; it is a live WAL database.

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
