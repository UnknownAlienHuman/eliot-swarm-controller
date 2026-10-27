# OpenCodex provider proxy

OpenCodex is a provider service, not an Agent harness. Harness plugins remain owners of
Agent/native session identity and lifecycle; OpenCodex supplies one explicit inference
route. Layer L2.7.

Facts checked 2026-10-25 against `lidge-jun/opencodex` release v2.92.0. It is
fast-moving; recheck installed README/source/API before implementation and record the
installed version.

## Ownership

Configuration selects exactly one mode:

- `attach`: owner-run/adopted service. Eliot health-checks/read-only monitors it, never
  starts, stops, restarts or rewrites it;
- `launch`: Eliot-managed service in separate service custody. It may be started/stopped
  by service lifecycle, never because one Agent stops.

This follows Waku's adopted-service rule. An adopted service PID/registration is not
proof Eliot owns it.

```toml
[proxies.opencodex]
attach = "http://127.0.0.1:10100"
health = "/api/system/health"       # verify on installed version
api_key_env = "OPENCODEX_API_KEY"
max_live = 8
# launch = ["ocx", "start"]        # only after foreground/lifecycle is recorded
```

Owner performs install/init/login. `OPENCODEX_HOME` and provider credentials stay owner
configuration; Eliot stores only environment-variable names. Client and management
credentials are distinct and never persisted in events/messages/fixtures.

## Harness wiring

| Harness | Explicit wiring |
| --- | --- |
| Codex | named Responses-compatible provider with `<origin>/v1` and client key headers |
| Claude Code | documented Anthropic base URL/auth/header variables |

Eliot injects route configuration only into the frozen per-Agent profile environment.
It never runs global integration installers or edits owner client configuration.
Requested URL is not proof of use: plugin reports effective provider/endpoint where
available and L7 verifies main/auxiliary requests.

## Monitoring, capacity and holds

Optional management API is read-only and version-checked. It may feed service health,
provider/model availability, usage and proven rate/quota facts. A management timeout
marks only the proxy source stale; it does not change Agent state or trigger fallback.

Profiles using the same OpenCodex service/account share capacity and persisted provider
holds. A 429 is rate limit unless exact response proves quota. One explicit probe may
claim a hold; unknown probe stays reserved across restart. Eliot never silently selects
another provider/account/model.

## Billing visibility

Billing follows the configured OpenCodex provider/account/auth mode. Eliot displays the
profile's intended account/billing and every effective fact exposed by harness/proxy.
A successful main chat does not prove title/compaction/subagent traffic used the same
route; unverified scope remains labelled unknown.

## Retained L7 cases

- adopted service is never signalled/restarted by Agent stop or daemon recovery;
- managed service uses separate custody and exact endpoint/PID birth identity;
- client/admin keys never cross roles or persist;
- stale/failed management read does not reroute or complete Agent work;
- effective Codex/Claude route and auxiliary calls are verified;
- provider hold/capacity is shared correctly across profiles and restart;
- stale integration/config write is never blindly retried.
