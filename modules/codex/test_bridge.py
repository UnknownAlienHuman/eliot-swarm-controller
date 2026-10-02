#!/usr/bin/env python3
"""Fixture tests for the ELIOT Codex bridge (slice 1, read-only attach).

Run in the module-local environment:

    python3 -m unittest test_bridge -v

Every test drives the real pinned SDK code path (request build, router
waiter, generated-model validation) against ``ScriptedPeerTransport``;
only the socket is replaced. The properties under test are the slice's
plan criteria: initialize precedes all reads, reading never resumes or
starts anything, one JSON-RPC object per frame, executor version present
in every outcome, and the JSONL shape of an owned process is never
accepted as an attach.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import bridge  # noqa: E402
from bridge import (  # noqa: E402
    ReadOnlyViolation,
    ScriptedPeerTransport,
    SharedCodexClient,
)

FIXTURE = HERE / "fixtures" / "recorded_session.json"
FORBIDDEN = {
    "thread/resume",
    "thread/start",
    "thread/fork",
    "turn/start",
    "turn/steer",
    "turn/interrupt",
    "thread/goal/set",
    "thread/goal/clear",
}


def _script() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _client():
    transport = ScriptedPeerTransport(_script())
    client = SharedCodexClient(transport)
    client.start()
    return client, transport


class _Args:
    thread = "thr_fixture_root"
    limit = 50
    cursor = None
    source_kinds = None


class BridgeFixtureTests(unittest.TestCase):
    def test_describe_reports_executor_and_sdk_versions_separately(self):
        client, _ = _client()
        try:
            out = bridge.run_operation("describe", client, _Args())
        finally:
            client.close()
        ex = out["executor"]
        self.assertEqual(ex["server_version"], "0.153.4")
        self.assertEqual(ex["runtime_version"], "0.153.4")
        self.assertEqual(ex["sdk_version"], "0.0.0-dev")
        self.assertEqual(
            ex["sdk_upstream_commit"], "18194bfd3534ca567d886eac454028dafaa68b6c"
        )
        self.assertEqual(out["read_only_methods"], sorted(bridge.READ_ONLY_METHODS))

    def test_open_reads_without_resume_and_initialize_is_first(self):
        client, transport = _client()
        try:
            out = bridge.run_operation("open", client, _Args())
        finally:
            client.close()
        self.assertEqual(out["thread"]["id"], "thr_fixture_root")
        methods = [m.get("method") for m in transport.sent if "method" in m]
        self.assertEqual(methods[0], "initialize")
        self.assertIn("initialized", methods)
        self.assertIn("thread/read", methods)
        self.assertFalse(FORBIDDEN.intersection(methods))
        read = next(m for m in transport.sent if m.get("method") == "thread/read")
        self.assertEqual(read["params"], {"threadId": "thr_fixture_root", "includeTurns": False})

    def test_snapshot_lists_threads_and_keeps_child_link(self):
        client, _ = _client()
        try:
            out = bridge.run_operation("snapshot", client, _Args())
        finally:
            client.close()
        ids = [t["id"] for t in out["threads"]]
        self.assertEqual(ids, ["thr_fixture_root", "thr_fixture_child"])
        child = out["threads"][1]
        self.assertEqual(child["parentThreadId"], "thr_fixture_root")
        self.assertIsNone(out["next_cursor"])

    def test_write_allowlist_blocks_mutating_methods_before_socket(self):
        client, transport = _client()
        try:
            with self.assertRaises(ReadOnlyViolation):
                client._write_message({"id": "x", "method": "thread/resume", "params": {}})
            with self.assertRaises(ReadOnlyViolation):
                client._write_message({"id": "y", "method": "turn/start", "params": {}})
        finally:
            client.close()
        sent_methods = [m.get("method") for m in transport.sent]
        self.assertNotIn("thread/resume", sent_methods)
        self.assertNotIn("turn/start", sent_methods)

    def test_server_approvals_are_declined(self):
        self.assertEqual(
            bridge._decline_all_approvals(
                "item/commandExecution/requestApproval", None
            ),
            {"decision": "decline"},
        )
        self.assertEqual(
            bridge._decline_all_approvals("item/fileChange/requestApproval", None),
            {"decision": "decline"},
        )

    def test_jsonl_line_is_not_a_fixture_or_attach(self):
        # The owned-process JSONL shape (bare lines, no per-frame request
        # ids echoed by a peer) must not satisfy the fixture peer: a
        # scripted response keyed to no request never resolves a waiter.
        script = _script()
        transport = ScriptedPeerTransport(script)
        with self.assertRaises(bridge.CodexError):
            transport.send('{"jsonrpc":"2.0","method":"thread/resume"}\n')
        # thread/resume is not in the script at all: the fixture surface
        # has no mutating method to replay.

    def test_websocket_transport_roundtrip_over_real_frames(self):
        # A real WebSocket (websockets library both ends) carrying the
        # scripted peer's answers: proves one JSON-RPC object per text
        # frame works against the actual transport, not only in-process.
        from websockets.sync.server import serve

        script = _script()
        received: list[str] = []

        def handler(ws):
            for raw in ws:
                received.append(raw)
                message = json.loads(raw)
                method = message.get("method")
                if method in (None, "initialized"):
                    continue
                result = script["responses"][method]["result"]
                ws.send(json.dumps({"id": message["id"], "result": result}))

        server = serve(handler, "127.0.0.1", 0)
        port = server.socket.getsockname()[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            transport = bridge.WebSocketTransport(f"ws://127.0.0.1:{port}", None)
            client = SharedCodexClient(transport)
            client.start()
            try:
                out = bridge.run_operation("describe", client, _Args())
            finally:
                client.close()
        finally:
            server.shutdown()
        self.assertEqual(out["executor"]["server_version"], "0.153.4")
        frames = [json.loads(r) for r in received]
        self.assertTrue(all(isinstance(f, dict) for f in frames))
        self.assertEqual(frames[0]["method"], "initialize")

    def test_cli_end_to_end_against_fixture(self):
        proc = subprocess.run(
            [sys.executable, str(HERE / "bridge.py"), "--fixture", str(FIXTURE), "describe"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["operation"], "describe")
        self.assertEqual(out["executor"]["server_version"], "0.153.4")


if __name__ == "__main__":
    unittest.main()
