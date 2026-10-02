#!/usr/bin/env python3
"""ELIOT Codex bridge — slice 1: attach to an existing shared Codex
app-server over WebSocket and expose a read-only surface.

Operations (no others exist in this slice):

- ``describe``  — ``initialize`` handshake; reports the *server's* version
  (the actual executor) separately from the pinned SDK version.
- ``open``      — attach to one existing thread with ``thread/read``
  (passive: it neither resumes nor starts the thread).
- ``snapshot``  — ``thread/list`` page; ``sourceKinds`` is passed through
  only when the caller supplies it (filters are an installed-schema
  qualification point, not a documented guarantee).

The vendored pinned Python SDK under ``vendor_bridge/`` is byte-identical
to upstream (see ``vendor_bridge/SHA256SUMS``). Its ``CodexClient`` speaks
JSONL over the stdio of a process it spawns itself; that transport cannot
attach to a shared server. The adaptation therefore lives here, in
ELIOT-owned code: ``SharedCodexClient`` subclasses the pinned client and
replaces only the transport touch-points (``start``, ``close``,
``_start_reader_thread``, ``_write_message``, ``_read_message``) so the
SDK's request routing, notification router and generated protocol types
are reused unchanged. One JSON-RPC message travels per WebSocket text
frame, matching the app-server WebSocket transport. No RFC 6455 code is
written here; framing is the ``websockets`` library's.

A hard allowlist in ``_write_message`` refuses every client method outside
the read-only set before anything reaches the socket, and the approval
handler declines every server-initiated approval: a read-only attach must
never admit work. The bridge never spawns, stops or restarts the shared
server, and never touches the shared ``.codex`` home.

Run ``python3 bridge.py --help``. Configuration is a JSON file copied from
``module.example.json`` and kept outside the repository; a bearer token,
when the server requires one, is read from the environment variable the
config names — never from the config file and never logged.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
_SDK_SRC = _HERE / "vendor_bridge" / "src"
if str(_SDK_SRC) not in sys.path:
    sys.path.insert(0, str(_SDK_SRC))

from openai_codex.client import CodexClient, CodexConfig  # noqa: E402
from openai_codex.errors import CodexError, TransportClosedError  # noqa: E402

SDK_UPSTREAM_COMMIT = "18194bfd3534ca567d886eac454028dafaa68b6c"
SDK_PACKAGE_VERSION = "0.0.0-dev"
MATCHING_BINARY_PIN = "openai-codex-cli-bin==0.153.4"
MODULE_ARTIFACT_ID = "codex-sdk-18194bf-bridge.1"

# The complete client-originated surface of this slice. Server-request
# responses (an ``id`` with no ``method``) are replies, not new calls, and
# are let through; the approval handler below declines their substance.
READ_ONLY_METHODS = frozenset(
    {"initialize", "initialized", "thread/read", "thread/list"}
)


class ReadOnlyViolation(CodexError):
    """A non-read-only client method was attempted on a read-only attach."""


def _decline_all_approvals(method: str, params: Any) -> dict[str, Any]:
    """Server-initiated requests never admit work from a read-only bridge."""
    if method in (
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
    ):
        return {"decision": "decline"}
    return {}


class WebSocketTransport:
    """One JSON-RPC message per text frame, via the ``websockets`` library."""

    def __init__(self, url: str, token: str | None) -> None:
        from websockets.sync.client import connect

        headers = {"Authorization": f"Bearer {token}"} if token else None
        self._conn = connect(url, additional_headers=headers, open_timeout=15)

    def send(self, text: str) -> None:
        self._conn.send(text)

    def recv(self) -> str:
        from websockets.exceptions import ConnectionClosed

        try:
            frame = self._conn.recv()
        except ConnectionClosed as exc:
            raise TransportClosedError("shared app-server closed the WebSocket") from exc
        if isinstance(frame, bytes):
            raise CodexError("shared app-server sent a binary frame; text expected")
        return frame

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


class ScriptedPeerTransport:
    """Fixture transport: a scripted protocol peer, not a recording replay.

    The SDK generates a fresh UUID request id per call, so recorded frames
    cannot be replayed verbatim. This peer instead answers each request
    from a method -> result script, echoing the request's own id, and can
    emit scripted server notifications before a response. The full SDK
    path — request build, router waiter, generated-model validation of
    every result — is exercised exactly as against a live server.
    """

    def __init__(self, script: dict[str, Any]) -> None:
        self._responses: dict[str, Any] = script.get("responses", {})
        self._notifications: dict[str, list[Any]] = script.get("notifications", {})
        self._inbox: queue.Queue[str] = queue.Queue()
        self._closed = False
        self.sent: list[dict[str, Any]] = []

    def send(self, text: str) -> None:
        message = json.loads(text)
        self.sent.append(message)
        method = message.get("method")
        if method is None:
            return  # response to a server-initiated request; nothing to script
        for note in self._notifications.get(method, []):
            self._inbox.put(json.dumps(note))
        if method == "initialized":
            return  # notification: no response
        if method not in self._responses:
            raise CodexError(f"fixture script has no response for {method!r}")
        outcome = self._responses[method]
        if "error" in outcome:
            reply = {"id": message["id"], "error": outcome["error"]}
        else:
            reply = {"id": message["id"], "result": outcome.get("result")}
        self._inbox.put(json.dumps(reply))

    def recv(self) -> str:
        while True:
            if self._closed:
                raise TransportClosedError("fixture transport closed")
            try:
                return self._inbox.get(timeout=0.05)
            except queue.Empty:
                continue

    def close(self) -> None:
        self._closed = True


class SharedCodexClient(CodexClient):
    """Pinned ``CodexClient`` whose stdio process transport is replaced by
    an already-connected message transport (WebSocket or fixture peer).

    Only the transport touch-points are overridden; routing, coercion and
    generated types are the pinned SDK's own. No process is spawned and
    ``codex_bin`` resolution never runs: the executor is the external
    shared server, whose lifecycle this bridge does not own.
    """

    def __init__(self, transport: Any, config: CodexConfig | None = None) -> None:
        super().__init__(config=config, approval_handler=_decline_all_approvals)
        self._shared_transport = transport

    def start(self) -> None:
        self._start_reader_thread()

    def close(self) -> None:
        self._runtime_version = None
        self._shared_transport.close()
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=1)

    def _start_reader_thread(self) -> None:
        if self._reader_thread and self._reader_thread.is_alive():
            return
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

    def _write_message(self, payload: dict[str, Any]) -> None:
        method = payload.get("method")
        if method is not None and method not in READ_ONLY_METHODS:
            raise ReadOnlyViolation(
                f"{method} is outside the read-only attach surface of this slice"
            )
        self._shared_transport.send(json.dumps(payload))

    def _read_message(self) -> dict[str, Any]:
        frame = self._shared_transport.recv()
        try:
            message = json.loads(frame)
        except json.JSONDecodeError as exc:
            raise CodexError(f"Invalid JSON-RPC frame: {frame!r}") from exc
        if not isinstance(message, dict):
            raise CodexError(f"Invalid JSON-RPC payload: {message!r}")
        return message


def _executor_block(client: SharedCodexClient, init: Any) -> dict[str, Any]:
    """Version facts, kept separate: server executor vs pinned SDK."""
    server = init.serverInfo if init is not None else None
    return {
        "server_name": server.name if server else None,
        "server_version": server.version if server else None,
        "runtime_version": client._runtime_version,
        "user_agent": init.userAgent if init is not None else None,
        "platform_family": init.platformFamily if init is not None else None,
        "platform_os": init.platformOs if init is not None else None,
        "sdk_package": "openai-codex",
        "sdk_version": SDK_PACKAGE_VERSION,
        "sdk_upstream_commit": SDK_UPSTREAM_COMMIT,
        "matching_binary_pin": MATCHING_BINARY_PIN,
        "module_artifact_id": MODULE_ARTIFACT_ID,
    }


def _dump(model: Any) -> Any:
    # The pinned generated Thread model carries a `historyMode` default
    # ("legacy") its own serializer warns about at this upstream commit;
    # the warning is the donor's, the values are passed through verbatim.
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Pydantic serializer warnings.*")
        return model.model_dump(by_alias=True, mode="json", exclude_none=True)


def run_operation(op: str, client: SharedCodexClient, args: argparse.Namespace) -> dict[str, Any]:
    init = client.initialize()
    out: dict[str, Any] = {"operation": op, "executor": _executor_block(client, init)}
    if op == "describe":
        out["attached"] = True
        out["read_only_methods"] = sorted(READ_ONLY_METHODS)
    elif op == "open":
        # thread/read is passive: it neither resumes nor subscribes the
        # thread for new work. thread/resume is outside the allowlist.
        result = client.thread_read(args.thread, include_turns=False)
        out["thread"] = _dump(result.thread)
    elif op == "snapshot":
        params: dict[str, Any] = {"limit": args.limit}
        if args.source_kinds:
            # Passed through verbatim for the caller's installed server;
            # which sourceKinds values its schema accepts is a live
            # qualification point, not asserted here.
            params["sourceKinds"] = args.source_kinds
        if args.cursor:
            params["cursor"] = args.cursor
        result = client.thread_list(params)
        out["threads"] = [_dump(t) for t in result.data]
        out["next_cursor"] = result.next_cursor
        out["backwards_cursor"] = result.backwards_cursor
    else:  # pragma: no cover - argparse restricts choices
        raise CodexError(f"unknown operation {op!r}")
    return out


def _load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="module config JSON (see module.example.json)")
    parser.add_argument(
        "--fixture",
        help="fixture script JSON: run against a scripted peer instead of a server",
    )
    sub = parser.add_subparsers(dest="op", required=True)
    sub.add_parser("describe")
    p_open = sub.add_parser("open")
    p_open.add_argument("--thread", required=True, help="existing native thread id")
    p_snap = sub.add_parser("snapshot")
    p_snap.add_argument("--limit", type=int, default=50)
    p_snap.add_argument("--cursor")
    p_snap.add_argument(
        "--source-kinds",
        nargs="*",
        default=None,
        help="passed through verbatim to thread/list (installed-schema dependent)",
    )
    args = parser.parse_args(argv)

    if bool(args.config) == bool(args.fixture):
        parser.error("pass exactly one of --config or --fixture")

    transport: Any
    if args.fixture:
        transport = ScriptedPeerTransport(_load_json(args.fixture))
    else:
        config = _load_json(args.config)
        endpoint = config.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.startswith(("ws://", "wss://")):
            parser.error("config.endpoint must be a ws:// or wss:// URL of an existing shared app-server")
        token = None
        token_env = config.get("tokenEnv")
        if token_env:
            token = os.environ.get(str(token_env))
            if token is None:
                parser.error(f"environment variable {token_env} named by config.tokenEnv is not set")
        transport = WebSocketTransport(endpoint, token)

    client = SharedCodexClient(transport)
    try:
        client.start()
        outcome = run_operation(args.op, client, args)
    except Exception as exc:  # reported, not hidden: no fabricated outcome
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 1
    finally:
        client.close()
    print(json.dumps(outcome, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
