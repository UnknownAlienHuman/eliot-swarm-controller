# Local model servers

A local model server (llama.cpp `llama-server`, vLLM, LM Studio, Unsloth Studio, ...)
is an inference service, not an Agent harness. Eliot treats it as an adopted or managed
[proxy/service](../spec/configuration.md#proxies-and-services). Notes here come from v1
and were not rechecked on 2026-10-26; verify the installed server/model/route before
implementation.

## Ownership

- adopted/owner-run endpoint: health-check/read-only only; Eliot never starts, stops,
  restarts or reconfigures it;
- managed endpoint: separate service custody; stopped only through service lifecycle,
  never because one Agent stops;
- sessions/turns remain owned by the harness plugin, not the model server;
- models sharing one GPU/capacity pool share one proxy scope and hold.

## Wiring

- Codex named providers require the Responses wire API. A server exposing only Chat
  Completions needs an explicitly selected Responses-capable proxy; Eliot never inserts
  one silently.
- OpenCode/Kilo use their documented OpenAI-compatible provider/base URL mechanism.
- A profile names the intended server/model/account and Eliot never substitutes a cloud
  route when it is slow/unavailable/incompatible.

## Qualification rules

- `/v1/models` or health 200 proves only reachability/catalogue. A route becomes usable
  after one real tool-call round trip through the selected harness and effective
  model/endpoint readback.
- Context overflow, early EOF, HTTP 200 with cut stream or bare `[DONE]` without the
  expected terminal semantics is failed/unknown according to native evidence, never
  truncated success.
- Timeout after a request may have reached inference is not retried automatically; use
  harness readback or keep the turn unknown.
- Missing GPU/context/quota counters remain unknown, not zero.

## WSL2 on the owner's Windows machine

- A read-only health check must not use a command that starts a stopped distro.
- Never run blanket `wsl --shutdown` / `--terminate`; unrelated work may use it.
- Prefer loopback-only exposure and verify reachability from Windows. Eliot does not add
  `portproxy`, firewall rules or `0.0.0.0` bindings automatically.
- A systemd unit inside a distro does not by itself guarantee WSL lifetime; report the
  boundary instead of pretending the service is durable.
