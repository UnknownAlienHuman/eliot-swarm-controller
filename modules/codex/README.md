# Codex bridge (C07, slice 1: shared-server attach, read-only)

External bridge module for the **existing shared Codex app-server**. The
whole pinned upstream Python SDK (`openai/codex`, `sdk/python`) is kept as
one usable unit under `vendor_bridge/`; this directory's own code
(`bridge.py`) adapts its transport to the shared server's WebSocket and
exposes only an attach/read surface. The Rust host, the shared server's
lifecycle and the shared `.codex` home are untouched by this slice, and no
controller route is registered yet (see "Scope" below).

## Setup

```sh
cd modules/codex
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # exact pins, module-local
python3 verify_vendor.py                    # donor tree == SHA256SUMS
```

Copy `module.example.json` outside the repository, keep it untracked, and
adjust `endpoint`. The referenced server is started by the operator, for
example `codex app-server --listen ws://127.0.0.1:4500`; this bridge never
starts, stops or owns it. When the server requires WebSocket
authentication, `tokenEnv` names the environment variable holding its
bearer capability token (`--capability-token-file` / `--ws-auth` on the
server side); the token is read from the environment at connect time and
never appears in the config file, in output, or on a command line.

## Use

```sh
.venv/bin/python bridge.py --config /path/to/codex-module.json describe
.venv/bin/python bridge.py --config /path/to/codex-module.json open --thread <native-thread-id>
.venv/bin/python bridge.py --config /path/to/codex-module.json snapshot [--limit N] [--cursor C] [--source-kinds cli vscode ...]
```

Every outcome is one JSON object on stdout and always carries an
`executor` block: the **server's** reported name/version (the actual
executor that served the calls) kept separate from the **pinned SDK**
identity (`openai-codex` `0.0.0-dev` at the vendored upstream commit and
the donor's matching-binary pin). Errors are reported as an `error`
object on stderr with exit 1; nothing is fabricated on failure.

## Fixture checks

No live server is needed to verify the slice:

```sh
.venv/bin/python -m unittest test_bridge -v
.venv/bin/python bridge.py --fixture fixtures/recorded_session.json snapshot
```

`fixtures/recorded_session.json` is a **synthetic script**, not a live
capture: request ids are generated per call, so the fixture peer answers
each request from a method→result script and echoes its id; every result
is still validated by the pinned SDK's generated models. The tests assert
the slice's acceptance properties: `initialize` precedes every read, a
read issues no resume/start/turn/goal method, the method allowlist blocks
mutating calls before the socket, server-initiated approvals are
declined, one JSON-RPC object travels per WebSocket text frame (including
a round-trip over a real local WebSocket), and the executor version is
present in every outcome.

## Scope and boundaries

Delivered in this slice: WebSocket attach + `describe` / `open`
(`thread/read`, passive — read is not resume) / `snapshot`
(`thread/list`). **Not** in this slice, by plan: send/turn methods,
native goal controls (they follow the comparable OpenCode goal slice in a
later C07 slice), thread fork/archive/name, account/config reads, any
Rust host or route registration, WebSocket-over-Unix-socket endpoints,
and reconnect/single-flight handling for a failed attach. `sourceKinds`
on `snapshot` is passed through verbatim only when the caller supplies
it: which values the installed server's schema accepts — and the
experimental lineage filters (`parentThreadId` / `ancestorThreadId`,
absent from this pin's `ThreadListParams`) — are live-qualification
points against the actually installed server, whose matrix entry is
still `installed_runtime_verified: false`. An owned JSONL app-server is a
separate, explicitly chosen deployment mode and is never a fallback when
a shared-server attach fails.

Transport notes: the pinned SDK's own transport spawns `codex app-server`
over stdio JSONL and cannot attach; `bridge.py` subclasses its
`CodexClient` and replaces only the transport touch-points, reusing the
SDK's router and generated protocol types unchanged (donor stays
byte-identical, verified by `SHA256SUMS`). No RFC 6455 framing is
hand-written; frames are the `websockets` library's, one JSON-RPC
message per text frame, matching the app-server WebSocket transport.
