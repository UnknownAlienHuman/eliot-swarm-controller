# OpenCodex (provider proxy)

Not a harness: a provider proxy that harnesses talk to. Eliot treats it as a
**service** ([configuration](../spec/configuration.md#proxies)). Layer L2.6.

Facts checked 2026-10-25 against [lidge-jun/opencodex](https://github.com/lidge-jun/opencodex)
(MIT; latest release v2.92.0, 2026-10-24). Third-party project, fast-moving: re-check
the README before L2.6.

## What it does

"Universal provider proxy for OpenAI Codex, Claude Code, Claude Desktop & Grok Build":
it accepts Codex's Responses API (and Anthropic-style requests for Claude Code) and
routes them to configured providers.

## Run

- Install `npm install -g @bitkyc08/opencodex`, then `ocx init`, `ocx start`.
- Default port 10100; proxy and dashboard on `http://localhost:10100`; inference base
  `http://127.0.0.1:<port>/v1`.
- Home dir `OPENCODEX_HOME` (default `~/.opencodex`); admin token in
  `<home>/admin-api-token`.
- Clients authenticate with header `x-opencodex-api-key`.
  `OPENCODEX_API_AUTH_TOKEN` is required before binding beyond loopback.

Eliot config (attach to an owner-run instance is the default; `launch` only if the
owner wants Eliot to supervise it):

```toml
[proxies.opencodex]
attach = "http://127.0.0.1:10100"
health = "/api/system/health"        # verify path in L2.6; v1 used it against v2.75
api_key_env = "OPENCODEX_API_KEY"
# launch = ["ocx", "start"]          # check whether it stays in the foreground
```

## Pointing harnesses at it

| Harness | Wiring | Source |
| --- | --- | --- |
| Codex | named `model_providers` entry with `base_url = <origin>/v1`, `wire_api = "responses"` ([Codex page](codex.md#proxy-opencodex-or-any-responses-compatible-endpoint)); OpenCodex's own alternative is `ocx codex-shim install` | README |
| Claude Code | `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN` (+ `ANTHROPIC_*_MODEL` slots) — exactly what `ocx claude` sets | `src/cli/claude.ts` (source, not README) |

Eliot sets these per agent process only; it never runs `ocx ... install` or changes
global client config. If OpenCodex's own client-integration feature is used, the owner
does it.

## Management API (optional monitoring)

Base `/api/` with the admin token. Documented: `GET /api/system/memory`; v1 also used
`GET /api/system/health`, providers, models and usage reads (against v2.75). Use only
read endpoints for `source.health` and `provider.limited`; Eliot does not change
OpenCodex settings.

## Billing and routes

- Billing follows the provider's `authMode` in OpenCodex: `forward` = the ChatGPT plan
  behind the caller's Codex login, `oauth` = that provider account's subscription,
  `key` = the key's own account. OpenCodex reports quota windows but not the plan tier.
- A working main chat does not prove the whole route: titles, compaction and subagent
  calls may use other models or endpoints. Check them in the L2.8 live run; until then
  `eliot show` marks the route as verified for the main chat only.
- Management reads use the admin token (header `X-OpenCodex-API-Key` or bearer), which
  is separate from the client key `x-opencodex-api-key`. Changing OpenCodex settings
  stays with the owner (v1 had a config-write module; v2 drops it).

## v1 pitfalls

- Claude via OpenCodex's `anthropic` provider in OAuth mode spends the subscription
  stored in OpenCodex, not the native route — make the route visible in `eliot show`.
- `activeTurnCount = 0` in OpenCodex does not mean native children stopped.
- A stale (409) integration plan must not be blindly retried.
- v1 never stopped/restarted the service; keep that: attached services are not ours.
