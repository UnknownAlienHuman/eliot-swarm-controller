# OpenCodex provider proxy

OpenCodex is an inference service, not an Agent harness. This increment adopts an
already-running owner service as an optional Codex route; it does not manage service
installation or lifecycle. Source: [lidge-jun/opencodex](https://github.com/lidge-jun/opencodex).

Inspection dated 2026-10-29 of installed OpenCodex 2.92.0 source
`src/server/index/serve-options.ts` shows an anonymous literal `GET /readyz`: readiness
returns HTTP 200 with `{"status":"ready"}`; pending or failed states return HTTP 503.
`/healthz` reports liveness only. Configure the readiness path explicitly. There is no
default path or OpenCodex version gate. This source contract does not establish provider
authentication or inference success.

## Ownership and configuration

Eliot makes only a bounded unauthenticated readiness read. It never starts, stops or
repairs an adopted endpoint. Agent stop owns the controlled Agent process tree and never
signals the shared service. Managed service launch and service-management APIs remain
`unsupported`; the owner retains service and credential administration.

```toml
[proxies.opencodex]
attach = "http://127.0.0.1:10300"
health = "/readyz"
base_url_for = { codex = "http://127.0.0.1:10300/v1" }
max_live = 8
# Optional; store only the environment-variable name, never its value.
# api_key_env = "OPENCODEX_API_KEY"
```

If api_key_env is set, the selected profile must explicitly allow that variable name in
pass_env. Its value remains runtime-only and is not used for readiness.

The profile explicitly selects this proxy and may name an account. The frozen snapshot
captures the selected names, optional shared Core `RouteBinding`, and configured
`CapacityScope` values. The canonical Core DTOs own their field catalog. `None` means
unlimited; `max_live = 0` disables that configured scope. These local workload limits
do not describe OpenCodex account entitlement or vendor quota. Named Fake profiles may
exercise profile/account caps, but Fake has no proxy route.

## Codex wiring and limits

The route uses the explicitly selected native executable's process-scoped configuration
override and verifies the effective named provider before session start. It does not
silently select another executable, provider or account. The owner's global Codex
configuration is not rewritten. Only the optional client-key environment-variable name
is frozen; its value is runtime-only.

The readiness and effective-config checks establish those limited facts only. Runtime
authentication, inference, billing, and auxiliary/subagent traffic remain unverified in
this increment. No model/provider probe or automatic route/account fallback is part of
the path. Provider holds and their management API are the next connected increment and
remain `unsupported` here.
