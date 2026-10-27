# OpenCodex provider proxy

Optional route in L3, after direct Codex already works. OpenCodex is an inference service,
not another Agent harness. Source: [lidge-jun/opencodex](https://github.com/lidge-jun/opencodex).
Inherited source check: 2026-10-25, v2.92.0. Recheck the selected installed API; this is
not a required version or current runtime qualification.

## Ownership

`attach` adopts an owner-run endpoint: bounded health/read only, never start/stop/repair.
`launch` explicitly supervises a service in separate custody. Start with attach; managed
service orchestration must not delay the first working route. Agent stop never signals
a shared or adopted service.

```toml
[proxies.opencodex]
attach = "http://127.0.0.1:10100"
health = "/api/system/health" # verify installed route
api_key_env = "OPENCODEX_API_KEY"
max_live = 8
```

Owner performs install/init/login. Client/admin credentials remain separate secret
references. Frozen profile captures endpoint, mode, health path, route/account and, for
managed service, exact launch/config inputs. Reload affects future admissions only;
service reuse requires matching binding and endpoint/custody readback.

## Harness wiring

Codex uses an explicit named Responses-compatible provider at the configured endpoint.
Claude later uses documented Anthropic base URL/auth/header variables. Inject configuration
per Agent; do not run global integration installers or alter owner client configuration.
Keep native sessions, generation and lifecycle in the harness plugin.

## Monitoring, capacity and holds

Optional management reads check supported endpoints/fields/capabilities. Version is
diagnostic, never an equality gate. A timeout marks only that source stale; it neither
completes Agent work nor authorizes fallback. Shared account/service capacity and provider
holds survive restart. Unknown probe remains reserved; no silent model/account switch.

## Billing visibility and final checks

Display intended profile/billing and native/proxy effective facts separately. Requested
URL or one successful main request does not prove auxiliary/subagent traffic used the
same route. Mark unobservable scope unknown. L7 exercises actual direct/proxy requests,
secret separation, config reload, service ownership, effective routing and hold recovery.
