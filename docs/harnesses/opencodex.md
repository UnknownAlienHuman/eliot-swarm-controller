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

The route must use the explicitly selected native executable's process-scoped
configuration override and verify the effective named provider before session start. It
must not silently select another executable, provider or account or rewrite the owner's
global Codex configuration. Only the optional client-key environment-variable name is
frozen; its value is runtime-only.

Eliot itself does not call `model/list`, query provider entitlement, or send an inference
probe. Native Codex app-server startup may make its own read-only model-catalog GET,
normally against the selected provider base URL. A supported absolute
`model_catalog_url` can direct that request to another host and may cause native client
authentication to accompany it. The route must pin that URL to the selected provider
or suppress the catalog refresh before process birth. This vendor-owned catalog read is
not proof of entitlement or inference and does not create a provider hold; holds require
the structured restriction on an exact-bound completed turn. The route must omit the
client-key value from the metadata reader while retaining it in the reader's redaction
policy; the controlled process still requires the explicitly configured key. Runtime
authentication, inference, billing, and auxiliary/subagent traffic remain unverified.
Provider holds use local Eliot scopes as described in
[monitoring](../spec/monitoring.md#provider-holds). No automatic route/account fallback
is permitted.
