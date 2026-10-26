# Local model servers

A local model server is an inference endpoint, not an agent harness. Eliot treats it as
an attached or managed [proxy/service](../spec/configuration.md#plugins-sources-services-and-webhooks).
These notes are inherited from v1 and require installed-server verification.

## Wiring

- Codex named provider requires the Responses wire API. Chat-Completions-only servers
  need an explicitly selected compatible proxy; Eliot never inserts one silently.
- OpenCode/Kilo use their installed provider/base-URL mechanism
  ([OpenCode route](opencode.md#providerproxy-configuration)).
- Profile names model, service, account and capacity scope. No cloud/API fallback when
  local inference is slow/unavailable/incompatible.

## Ownership/capacity

`attach` is owner-run and never stopped/reconfigured. `launch` is a separately managed
service and stops only through service lifecycle. Agents never kill the shared model
server. Models sharing one GPU/capacity pool share a service/account cap; missing
metrics remain unknown.

## Qualification

A health/catalogue response proves only reachability. One real harness tool-call round
trip must prove effective model/endpoint and terminal streaming semantics. Context
overflow, early EOF, truncated 200 or bare incomplete `[DONE]` is failure/unknown—not
success. A timed-out request that may have reached inference is never blindly retried.

## WSL2

Read-only health checks must not start a stopped distro. Never blanket shutdown/
terminate WSL, change firewall/portproxy or expose `0.0.0.0` automatically. Prefer
loopback and test from Windows. A systemd unit inside WSL does not prove distro/service
lifetime; report the boundary honestly.
