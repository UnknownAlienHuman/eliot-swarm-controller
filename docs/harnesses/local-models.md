# Local model servers

Not a harness: an inference endpoint (llama.cpp `llama-server`, vLLM, LM Studio,
Unsloth Studio, ...) that a harness talks to. Eliot treats it like any other
[proxy](../spec/configuration.md#proxies): an attached service it health-checks and
never stops. Notes carried over from v1 (`git show v1-final:docs/local-models/`), not
re-checked; verify against the installed server before relying on them.

## Wiring

- Codex accepts only `wire_api = "responses"`. A server that speaks only Chat
  Completions needs a Responses-capable proxy in front (e.g. OpenCodex); otherwise use
  a harness with an OpenAI-compatible provider (OpenCode, Kilo).
- OpenCode/Kilo: a custom provider with `npm = "@ai-sdk/openai-compatible"` and
  `options.baseURL` ([OpenCode page](opencode.md#proxy)).

## Rules

- A route is usable only after one real tool-call round trip succeeds; `/v1/models`
  proves neither tool support nor a loaded model.
- Context overflow, early EOF, an HTTP 200 with a cut stream or a bare `[DONE]` are
  failures with the native finish reason, never silently truncated success.
- Models sharing one GPU are one capacity unit: put them behind one proxy entry with
  `max_live`. Missing GPU counters are `unknown`, not zero.

## WSL (vLLM inside WSL2 on the owner's Windows machine)

- Read-only checks must not use `wsl.exe --exec`: it starts a stopped distro.
- Never `wsl --shutdown` / `--terminate`: other work runs there.
- Bind the server to `127.0.0.1` inside WSL and test from Windows; never `0.0.0.0`,
  `portproxy` or firewall changes.
- A systemd service inside the distro does not by itself keep WSL running.
