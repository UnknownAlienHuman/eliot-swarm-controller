# Local model servers

A local model server (llama.cpp `llama-server`, vLLM, LM Studio, Unsloth Studio, ...)
is an inference endpoint, not an agent harness. Eliot treats it as an attached or
managed [proxy/service](../spec/configuration.md#proxies-and-managed-services). These
notes are carried from v1 and were not rechecked on 2026-10-25; verify the installed
server, model and route before implementation.

## Wiring

- Codex's named provider route requires the Responses wire API. A server exposing only
  Chat Completions needs an explicitly selected Responses-capable proxy; Eliot does not
  insert one silently.
- OpenCode/Kilo can use their documented OpenAI-compatible provider/base URL mechanism
  ([OpenCode page](opencode.md#proxyprovider-configuration)).
- A profile names the intended model/server/account. Eliot never substitutes a cloud
  or API route when the local server is slow, unavailable or incompatible.

## Ownership and capacity

- `attach`: owner-run service; health-check only, never stop or reconfigure it.
- `launch`: separately supervised managed service; stop only through service lifecycle,
  never because one agent stops.
- Models sharing one GPU/capacity pool share one proxy `max_live`/hold scope. Unknown
  GPU/memory/quota counters remain unknown.

## Qualification rules

- `/v1/models` or a health 200 proves only reachability/catalogue. The route becomes
  usable for an agent profile after one real tool-call round trip through the selected
  harness succeeds and effective model/endpoint are observed.
- Context overflow, early EOF, HTTP 200 with a cut stream or bare `[DONE]` without the
  expected terminal semantics are failures/unknown according to native evidence,
  never silently truncated success.
- A timeout after a request may have reached inference is not retried automatically.
  Read back through the harness if possible; otherwise keep the turn unknown.

## WSL2 on the owner's Windows host

- A read-only health check must not invoke a command that starts a stopped distro.
- Never run blanket `wsl --shutdown` or `--terminate`; unrelated work may use it.
- Prefer loopback-only exposure and test reachability from Windows. Eliot does not add
  `portproxy`, firewall rules or `0.0.0.0` bindings automatically.
- A systemd unit inside a distro does not itself guarantee WSL lifetime; report that
  boundary rather than pretending the service is durable.
