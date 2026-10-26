# OpenCodex (provider proxy)

OpenCodex is a provider proxy, not an agent harness. Eliot treats it as an attached or
managed **shared service**
([configuration](../spec/configuration.md#proxies-and-managed-services)); agent
sessions remain owned by their harness plugins. Layer L2.7.

Facts checked 2026-10-25 against `lidge-jun/opencodex` (MIT; release v2.92.0 dated
2026-10-24). It is fast-moving; re-read its official README/API source before L2.7 and
record the installed version.

## What it does

It accepts Codex Responses-style and Anthropic-style client requests and routes them
to configured providers/accounts. Eliot does not use it as a scheduler, session store
or authority; it is only the explicit inference route selected by a profile.

## Run and ownership

- Owner install/init remains an owner action.
- Default documented service port is 10100; inference base is the service origin plus
  `/v1`.
- Home directory is selected by `OPENCODEX_HOME`; tokens stay there or in named
  environment variables and never enter Eliot config/events.
- Client requests use the documented OpenCodex client-key header. Binding beyond
  loopback requires the proxy's own authentication configuration.

```toml
[proxies.opencodex]
attach = "http://127.0.0.1:10100"
health = "/api/system/health"        # verify against installed version
api_key_env = "OPENCODEX_API_KEY"
# launch = ["ocx", "start"]         # only after foreground/lifecycle is recorded
```

`attach` is the default: Eliot health-checks but never stops/reconfigures the owner's
service. `launch` creates one separately supervised service; its process group is not
an agent group and stopping one agent never kills it.

## Harness wiring

| Harness | Wiring |
| --- | --- |
| Codex | named `model_providers` entry with `<origin>/v1` and Responses wire API ([Codex page](codex.md#proxy-opencodex-or-another-responses-compatible-endpoint)) |
| Claude Code | documented `ANTHROPIC_BASE_URL`/auth/header variables, equivalent to the proxy's own Claude wrapper |

Eliot sets route configuration only in the per-agent harness environment/launch
arguments. It never runs global integration installers or edits owner client config.
Requested base URL is not proof of use: the harness reports effective provider/
endpoint where possible, and the L7 live run verifies main chat plus auxiliary calls.

## Management reads

The optional management API can feed `source.health`, provider/model availability and
usage. Use read endpoints only and the separate admin credential. The exact health,
provider, model and usage paths/headers are versioned facts to confirm against the
installed OpenCodex source; v1's v2.75 paths are donors, not a current contract.

A management timeout or stale response does not alter a live agent state and does not
trigger provider fallback. It marks only the proxy source stale/unknown.

## Billing and route visibility

OpenCodex provider auth mode determines which subscription/key/account is spent.
Eliot profiles name the intended billing/account and display effective facts the
proxy/harness exposes. It never changes route because one account is limited.

A successful main completion does not prove title, compaction, subagent or auxiliary
requests used the same route. Until recorded, `show` labels only the verified scope.

## v1 failures retained for L7

- Claude routed through an OpenCodex OAuth provider may spend the account stored in
  OpenCodex rather than Claude's native login; keep route/billing visible.
- `activeTurnCount = 0` does not prove native children stopped.
- A 409/stale integration plan is not blindly retried.
- Attached service ownership is external: Eliot never restarts or kills it.
- Client key and admin key are different credentials and must not be confused or
  persisted.
