#!/usr/bin/env python3
"""Fixture tests for the ELIOT Codex observer and controller bridge.

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

import base64
import json
import hashlib
import subprocess
import sys
import tempfile
import threading
import unittest
import warnings
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import bridge  # noqa: E402
from bridge import (  # noqa: E402
    ReadOnlyViolation,
    ScriptedPeerTransport,
    SharedCodexClient,
)
from controller import Checkpoint, ControllerEngine, MODULE_ARTIFACT_ID  # noqa: E402

FIXTURE = HERE / "fixtures" / "recorded_session.json"
CONTROLLER_FIXTURE = HERE / "fixtures" / "controller_session.json"
FIXTURE_WORKSPACE = str(Path(tempfile.gettempdir()) / "eliot-codex-fixture-workspace")
OTHER_FIXTURE_WORKSPACE = str(Path(tempfile.gettempdir()) / "eliot-codex-fixture-other-workspace")
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


def _controller_script() -> dict:
    """Use a host-native absolute temp path in the cross-platform fixture."""
    script = json.loads(CONTROLLER_FIXTURE.read_text(encoding="utf-8"))

    def replace_workspace(value):
        if isinstance(value, dict):
            return {key: replace_workspace(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace_workspace(item) for item in value]
        if value == r"C:\Fixture\workspace":
            return FIXTURE_WORKSPACE
        return value

    return replace_workspace(script)


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
    def test_result_outcome_uses_durable_module_result_path(self):
        class RecordingHost:
            def __init__(self):
                self.calls = []

            def call(self, method, params):
                self.calls.append((method, params))
                return {"recorded": True}

        host = RecordingHost()
        page = {
            "source": {"kind": "codex_turn_history"},
            "offset_bytes": 0,
            "byte_length": 3,
            "total_bytes": 3,
            "eof": True,
            "media_type": "application/json",
            "content_base64": "e30=",
            "page_sha256": hashlib.sha256(b"{}").hexdigest(),
        }
        bridge._report_outcome(
            host,
            {"operation_id": "operation-result-fixture", "result_page": page},
        )
        self.assertEqual(
            host.calls,
            [
                (
                    "module.result",
                    {"operation_id": "operation-result-fixture", "page": page},
                )
            ],
        )

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

    def test_codex_0159_user_agent_identity_and_thread_read_turn_projection(self):
        script = _script()
        initialize = script["responses"]["initialize"]["result"]
        initialize.pop("serverInfo", None)
        initialize["userAgent"] = (
            "Codex Desktop/0.159.0 (Windows 10.0.26200; x86_64) dumb "
            "(codex_python_sdk; 0.0.0-dev)"
        )
        thread = script["responses"]["thread/read"]["result"]["thread"]
        thread["cliVersion"] = "0.159.0"
        thread["turns"] = [
            {
                "id": "turn-0159",
                "status": "completed",
                "items": [
                    {
                        "id": "native-user-0159",
                        "clientId": "operation-0159",
                        "content": [{"type": "text", "text": "synthetic request"}],
                        "type": "userMessage",
                    },
                    {
                        "id": "native-agent-0159",
                        "text": "synthetic response",
                        "type": "agentMessage",
                    },
                ],
            }
        ]
        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(transport)
        client.start()
        try:
            init = client.initialize()
            executor = bridge._executor_block(client, init)
            self.assertIsNone(init.serverInfo)
            self.assertEqual(client._runtime_version, "0.159.0")
            self.assertEqual(executor["server_name"], "Codex Desktop")
            self.assertEqual(executor["server_name_source"], "initialize_user_agent")
            self.assertEqual(executor["server_version"], "0.159.0")
            self.assertEqual(
                executor["server_version_source"],
                "sdk_runtime_version_from_initialize_user_agent",
            )

            read = client.thread_read("thr_fixture_root", include_turns=True)
            self.assertEqual(len(read.thread.turns), 1)
            self.assertEqual(read.thread.turns[0].id, "turn-0159")
            self.assertEqual(read.thread.turns[0].status.value, "completed")
            self.assertEqual(
                [item.root.type for item in read.thread.turns[0].items],
                ["userMessage", "agentMessage"],
            )
            request = next(
                message
                for message in transport.sent
                if message.get("method") == "thread/read"
            )
            self.assertEqual(
                request["params"],
                {"threadId": "thr_fixture_root", "includeTurns": True},
            )

            endpoint_identity = "endpoint-fixture-hash"
            actual_scope = (
                f"codex-appserver:{endpoint_identity}:Codex Desktop:0.159.0"
            )
            self.assertEqual(
                bridge._native_scope_key(endpoint_identity, executor, None),
                (actual_scope, False),
            )
            legacy_scope = (
                f"codex-appserver:{endpoint_identity}:unknown-server:unknown-version"
            )
            self.assertEqual(
                bridge._native_scope_key(endpoint_identity, executor, legacy_scope),
                (legacy_scope, True),
            )
        finally:
            client.close()

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

    def test_native_tool_notification_window_preserves_ids_without_payload(self):
        script = _controller_script()
        script["notifications"] = {
            "initialize": [
                {
                    "method": "thread/started",
                    "params": {
                        "thread": {
                            "id": "thread-fixture-child",
                            "parentThreadId": "thread-fixture-root",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        "completedAtMs": 10,
                        "threadId": "thread-fixture-root",
                        "turnId": "turn-fixture-tool",
                        "item": {
                            "id": "native-tool-call-17",
                            "type": "mcpToolCall",
                            "server": "fixture-server",
                            "tool": "lookup",
                            "arguments": {"private": "not copied into live state"},
                            "status": "completed",
                            "result": {
                                "content": [{"type": "text", "text": "fixture result"}]
                            },
                        },
                    },
                }
            ]
        }
        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(
            transport, allowed_methods=bridge.CONTROLLER_METHODS
        )
        client.start()
        try:
            client.initialize()
            snapshot = client.native_event_snapshot({"thread-fixture-root"})
        finally:
            client.close()
        self.assertFalse(snapshot["complete"])
        self.assertEqual(snapshot["coverage"], "current_connection_window_only")
        self.assertEqual(len(snapshot["events"]), 2)
        child_event = snapshot["events"][0]
        self.assertEqual(child_event["event"], "thread/started")
        self.assertEqual(child_event["native_thread_id"], "thread-fixture-child")
        self.assertEqual(child_event["parent_thread_id"], "thread-fixture-root")
        event = snapshot["events"][1]
        self.assertEqual(event["event"], "item/completed")
        self.assertEqual(event["native_thread_id"], "thread-fixture-root")
        self.assertEqual(event["native_turn_id"], "turn-fixture-tool")
        self.assertEqual(event["native_item_id"], "native-tool-call-17")
        self.assertEqual(event["tool_server"], "fixture-server")
        self.assertEqual(event["tool_name"], "lookup")
        self.assertEqual(event["tool_status"], "completed")
        self.assertNotIn("arguments", event)
        self.assertNotIn("result", event)

    def test_native_child_history_result_keeps_tool_ids_and_partial_family_claim(self):
        script = _controller_script()
        root_thread = script["responses"]["thread/start"]["result"]["thread"]
        child = json.loads(json.dumps(root_thread))
        child.update(
            {
                "id": "thread-fixture-child",
                "parentThreadId": "thread-fixture-root",
                "sessionId": "session-fixture-root",
                "model": "fixture-child-model",
                "modelProvider": "fixture-child-provider",
                "preview": "synthetic child thread",
                "source": {
                    "subAgent": {
                        "thread_spawn": {
                            "depth": 1,
                            "parent_thread_id": "thread-fixture-root",
                        }
                    }
                },
            }
        )
        unrelated = json.loads(json.dumps(child))
        unrelated.update(
            {
                "id": "thread-unrelated-child",
                "parentThreadId": "another-root",
                "source": {
                    "subAgent": {
                        "thread_spawn": {
                            "depth": 1,
                            "parent_thread_id": "another-root",
                        }
                    }
                },
            }
        )
        script["responses"]["thread/list"] = [
            {"result": {"data": [child], "nextCursor": "children-page-2"}},
            {"result": {"data": [unrelated], "nextCursor": None}},
        ]
        root_input = {
            "turnId": "turn-fixture-root-input",
            "item": {
                "id": "native-user-fixture-22",
                "clientId": "operation-fixture-dispatch",
                "content": [{"type": "text", "text": "Task specification: {}\n\nreturn marker"}],
                "type": "userMessage",
            },
        }
        parent_activity = {
            "turnId": "turn-fixture-root-input",
            "item": {
                "id": "native-subagent-activity-3",
                "agentPath": "fixture:subagent",
                "agentThreadId": "thread-fixture-child",
                "kind": "started",
                "type": "subAgentActivity",
            },
        }
        child_history = [
            {
                "turnId": "turn-fixture-child-result",
                "item": {
                    "id": "native-assistant-fixture-30",
                    "text": "child result from native history",
                    "phase": "final_answer",
                    "type": "agentMessage",
                },
            },
            {
                "turnId": "turn-fixture-child-result",
                "item": {
                    "id": "native-tool-call-31",
                    "server": "fixture-server",
                    "tool": "lookup",
                    "arguments": {"key": "fixture"},
                    "status": "completed",
                    "result": {
                        "content": [{"type": "text", "text": "exact tool output"}],
                        "structuredContent": {"answer": "exact"},
                    },
                    "type": "mcpToolCall",
                },
            },
            {
                "turnId": "turn-fixture-child-result",
                "item": {
                    "id": "native-reasoning-fixture-32",
                    "content": ["private reasoning fixture"],
                    "summary": [],
                    "type": "reasoning",
                },
            },
        ]
        script["responses"]["thread/items/list"] = [
            {"result": {"data": [root_input], "nextCursor": None}},
            {"result": {"data": [parent_activity], "nextCursor": None}},
            {"result": {"data": [parent_activity], "nextCursor": None}},
            {"result": {"data": child_history, "nextCursor": None}},
            {"result": {"data": child_history, "nextCursor": None}},
        ]
        script["responses"]["thread/turns/list"] = [
            {
                "result": {
                    "data": [
                        {"id": "turn-fixture-root-input", "status": "completed", "items": []}
                    ],
                    "nextCursor": None,
                }
            },
            {
                "result": {
                    "data": [
                        {"id": "turn-fixture-child-result", "status": "completed", "items": []}
                    ],
                    "nextCursor": None,
                }
            },
        ]

        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(
            transport, allowed_methods=bridge.CONTROLLER_METHODS
        )
        client.start()
        try:
            init = client.initialize()
            executor = bridge._executor_block(client, init)
            scope_key = "codex-appserver:fixture:codex-app-server-fixture:0.153.4"
            owner = {"version": 1, "process": {"purpose": "module"}, "token": "boot-result"}
            with tempfile.TemporaryDirectory() as temp_dir:
                checkpoint = Checkpoint(Path(temp_dir), owner)
                prompt = "Task specification: {}\n\nreturn marker"
                checkpoint.data.update(
                    native_root_id="thread-fixture-root",
                    native_scope_key=scope_key,
                    requested_model_provider="fixture-provider",
                    requested_model="fixture-model",
                    effective_model_provider="fixture-provider",
                    effective_model="fixture-model",
                    effective_model_status="thread_configuration_exact",
                    workspace_root=FIXTURE_WORKSPACE,
                    operations={
                        "operation-fixture-dispatch": {
                            "method": "task.dispatch",
                            "kind": "send",
                            "native_root_id": "thread-fixture-root",
                            "native_scope_key": scope_key,
                            "client_user_message_id": "operation-fixture-dispatch",
                            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                            "prompt_bytes": len(prompt.encode()),
                            "delivery": "next_turn",
                            "returned_turn_id": "turn-fixture-root-input",
                            "turn_id": "turn-fixture-root-input",
                            "native_input_id": "native-user-fixture-22",
                        }
                    },
                )
                checkpoint.save()
                engine = ControllerEngine(
                    checkpoint, lambda: (client, executor, scope_key)
                )
                result = engine.handle(
                    {
                        "operation_id": "operation-fixture-result",
                        "method": "agent.result",
                        "native_root_id": "thread-fixture-root",
                        "input": {
                            "selector": {
                                "kind": "codex_turn_history",
                                "input_operation_id": "operation-fixture-dispatch",
                                "native_thread_id": "thread-fixture-child",
                                "native_turn_id": "turn-fixture-child-result",
                            }
                        },
                    }
                )[0]

                self.assertEqual(result["outcome"], "applied", result)
                self.assertEqual(
                    result["details"]["completion_condition"], "native_result_observed"
                )
                page = result["result_page"]
                self.assertTrue(page["eof"])
                self.assertEqual(page["source"]["native_thread_id"], "thread-fixture-child")
                self.assertEqual(page["source"]["parent_thread_id"], "thread-fixture-root")
                self.assertEqual(page["source"]["root_thread_model_provider"], "fixture-provider")
                self.assertEqual(page["source"]["thread_model_provider"], "fixture-child-provider")
                self.assertNotEqual(
                    page["source"]["root_thread_model_provider"],
                    page["source"]["thread_model_provider"],
                )
                self.assertEqual(page["source"]["history_projection"], "allowlisted_native_item_fields_v1")
                self.assertEqual(result["details"]["served_model_status"], "unknown")
                self.assertEqual(result["details"]["billing_status"], "unknown")
                self.assertFalse(page["source"]["family_complete"])
                document = json.loads(base64.b64decode(page["content_base64"]))
                tool_item = next(
                    entry["item"]
                    for entry in document["items"]
                    if entry["item"]["type"] == "mcpToolCall"
                )
                self.assertEqual(tool_item["id"], "native-tool-call-31")
                self.assertEqual(tool_item["tool"], "lookup")
                self.assertEqual(tool_item["arguments"], {"key": "fixture"})
                self.assertEqual(tool_item["result"]["structuredContent"], {"answer": "exact"})
                self.assertNotIn("private reasoning fixture", page["content_base64"])
                observed = engine.observation()
                self.assertEqual(
                    [child["native_thread_id"] for child in observed["observed_children"]],
                    ["thread-fixture-child"],
                )
                self.assertEqual(observed["family_enumeration"]["status"], "complete")
                self.assertEqual(observed["family_enumeration"]["family_completeness"], "partial")
                self.assertFalse(observed["family_enumeration"]["atomic"])
        finally:
            client.close()

    def test_selected_child_identity_conflict_rejects_result(self):
        script = _controller_script()
        root_thread = script["responses"]["thread/start"]["result"]["thread"]
        child = json.loads(json.dumps(root_thread))
        child.update(
            {
                "id": "thread-fixture-conflicted-child",
                "parentThreadId": "thread-fixture-root",
                "sessionId": "session-fixture-root",
                "preview": "synthetic child thread",
                "source": {
                    "subAgent": {
                        "thread_spawn": {
                            "depth": 1,
                            "parent_thread_id": "thread-fixture-root",
                        }
                    }
                },
            }
        )
        conflicting_child = json.loads(json.dumps(child))
        conflicting_child.update(
            {
                "parentThreadId": "thread-other-root",
                "source": {
                    "subAgent": {
                        "thread_spawn": {
                            "depth": 1,
                            "parent_thread_id": "thread-other-root",
                        }
                    }
                },
            }
        )
        script["responses"]["thread/list"] = [
            {"result": {"data": [child], "nextCursor": "conflict-page-2"}},
            {"result": {"data": [conflicting_child], "nextCursor": None}},
        ]
        prompt = "Task specification: {}\n\nreturn marker"
        script["responses"]["thread/items/list"] = [
            {
                "result": {
                    "data": [
                        {
                            "turnId": "turn-fixture-root-input",
                            "item": {
                                "id": "native-user-fixture-conflict",
                                "clientId": "operation-fixture-conflict-dispatch",
                                "content": [{"type": "text", "text": prompt}],
                                "type": "userMessage",
                            },
                        }
                    ],
                    "nextCursor": None,
                }
            }
        ]

        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(transport, allowed_methods=bridge.CONTROLLER_METHODS)
        client.start()
        try:
            init = client.initialize()
            executor = bridge._executor_block(client, init)
            scope_key = "codex-appserver:fixture:codex-app-server-fixture:0.153.4"
            owner = {"version": 1, "process": {"purpose": "module"}, "token": "boot-conflict"}
            with tempfile.TemporaryDirectory() as temp_dir:
                checkpoint = Checkpoint(Path(temp_dir), owner)
                checkpoint.data.update(
                    native_root_id="thread-fixture-root",
                    native_scope_key=scope_key,
                    effective_model_provider="fixture-provider",
                    effective_model="fixture-model",
                    operations={
                        "operation-fixture-conflict-dispatch": {
                            "method": "task.dispatch",
                            "kind": "send",
                            "native_root_id": "thread-fixture-root",
                            "native_scope_key": scope_key,
                            "client_user_message_id": "operation-fixture-conflict-dispatch",
                            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                            "prompt_bytes": len(prompt.encode()),
                            "delivery": "next_turn",
                            "returned_turn_id": "turn-fixture-root-input",
                            "turn_id": "turn-fixture-root-input",
                            "native_input_id": "native-user-fixture-conflict",
                        }
                    },
                )
                checkpoint.save()
                engine = ControllerEngine(checkpoint, lambda: (client, executor, scope_key))
                result = engine.handle(
                    {
                        "operation_id": "operation-fixture-conflict-result",
                        "method": "agent.result",
                        "native_root_id": "thread-fixture-root",
                        "input": {
                            "selector": {
                                "kind": "codex_turn_history",
                                "input_operation_id": "operation-fixture-conflict-dispatch",
                                "native_thread_id": "thread-fixture-conflicted-child",
                                "native_turn_id": "turn-fixture-child-result",
                            }
                        },
                    }
                )[0]

                self.assertEqual(result["outcome"], "rejected", result)
                self.assertEqual(
                    result["details"]["diagnostic_code"],
                    "RESULT_CHILD_IDENTITY_CONFLICT",
                )
                self.assertFalse(result["details"]["native_replay"])
                self.assertNotIn("result_page", result)
                family = engine.observation()["family_enumeration"]
                self.assertEqual(family["conflicting_thread_ids"], ["thread-fixture-conflicted-child"])
                self.assertEqual(family["family_completeness"], "partial")
                self.assertEqual(family["status"], "partial")
                self.assertFalse(family["atomic"])
        finally:
            client.close()

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

    def test_server_requests_get_schema_shaped_non_granting_replies(self):
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
        self.assertEqual(
            bridge._decline_all_approvals("item/permissions/requestApproval", None),
            {"permissions": {}, "scope": "turn"},
        )
        self.assertEqual(
            bridge._decline_all_approvals("item/tool/call", None),
            {"contentItems": [], "success": False},
        )
        self.assertEqual(
            bridge._decline_all_approvals(
                "item/tool/requestUserInput",
                {"questions": [{"id": "q1"}, {"id": "q2"}]},
            ),
            {"answers": {"q1": {"answers": []}, "q2": {"answers": []}}},
        )
        self.assertEqual(
            bridge._decline_all_approvals("mcpServer/elicitation/request", None),
            {"action": "decline"},
        )
        self.assertEqual(
            bridge._decline_all_approvals("applyPatchApproval", None),
            {"decision": "denied"},
        )
        self.assertEqual(
            bridge._decline_all_approvals("execCommandApproval", None),
            {"decision": "denied"},
        )
        with self.assertRaises(bridge.UnsupportedServerRequest):
            bridge._decline_all_approvals("unknown/request", None)

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
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", DeprecationWarning)
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
        self.assertIsNone(transport._connection_context)
        self.assertFalse(
            [warning for warning in caught if issubclass(warning.category, DeprecationWarning)]
        )

    def test_cli_end_to_end_against_fixture(self):
        proc = subprocess.run(
            [sys.executable, "-B", str(HERE / "bridge.py"), "--fixture", str(FIXTURE), "describe"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["operation"], "describe")
        self.assertEqual(out["executor"]["server_version"], "0.153.4")

    def test_controller_exact_model_and_dual_native_identity_readback(self):
        script = _controller_script()
        script["notifications"] = {
            "turn/start": [
                {
                    "method": "turn/started",
                    "params": {
                        "threadId": "thread-fixture-root",
                        "turn": {
                            "id": "turn-fixture-1",
                            "status": "inProgress",
                            "items": [],
                        },
                    },
                }
            ]
        }
        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(transport, allowed_methods=bridge.CONTROLLER_METHODS)
        client.start()
        try:
            init = client.initialize()
            executor = bridge._executor_block(client, init)
            scope_key = "codex-appserver:fixture:codex-app-server-fixture:0.153.4"
            owner = {"version": 1, "process": {"purpose": "module"}, "token": "boot-fixture"}
            with tempfile.TemporaryDirectory() as temp_dir:
                checkpoint = Checkpoint(Path(temp_dir), owner)
                engine = ControllerEngine(checkpoint, lambda: (client, executor, scope_key))
                route = {
                    "runtime": "codex",
                    "module_artifact_id": MODULE_ARTIFACT_ID,
                    "native_options": {
                        "workspaceRoot": FIXTURE_WORKSPACE,
                        "modelProvider": "fixture-provider",
                        "model": "fixture-model",
                    },
                }
                opened = engine.handle(
                    {"operation_id": "operation-fixture-open", "method": "agent.open", "route": route}
                )[0]
                self.assertEqual(opened["outcome"], "applied")
                self.assertEqual(opened["native_root_id"], "thread-fixture-root")
                self.assertEqual(opened["details"]["requested_model_provider"], "fixture-provider")
                self.assertEqual(opened["details"]["effective_model"], "fixture-model")
                self.assertEqual(
                    opened["details"]["effective_model_status"], "thread_configuration_exact"
                )
                self.assertEqual(
                    opened["details"]["requested_workspace_root"],
                    FIXTURE_WORKSPACE,
                )
                self.assertEqual(
                    opened["details"]["observed_workspace_root"],
                    FIXTURE_WORKSPACE,
                )
                self.assertEqual(opened["details"]["workspace_status"], "workspace_exact")
                observed_thread = engine.observation()["native"]["thread"]
                self.assertEqual(
                    observed_thread["requested_workspace_root"], FIXTURE_WORKSPACE
                )
                self.assertEqual(
                    observed_thread["observed_workspace_root"], FIXTURE_WORKSPACE
                )
                self.assertEqual(observed_thread["cwd"], FIXTURE_WORKSPACE)
                self.assertNotIn("workspace_root", observed_thread)
                duplicate_open = engine.handle(
                    {"operation_id": "operation-fixture-open-again", "method": "agent.open", "route": route}
                )[0]
                self.assertEqual(duplicate_open["outcome"], "rejected")
                self.assertEqual(
                    duplicate_open["details"]["diagnostic_code"],
                    "CONTROLLER_THREAD_ALREADY_RESERVED",
                )
                self.assertEqual(
                    sum(m.get("method") == "thread/start" for m in transport.sent), 1
                )

                dispatch = {
                    "operation_id": "operation-fixture-dispatch",
                    "method": "task.dispatch",
                    "native_root_id": "thread-fixture-root",
                    "route": route,
                    "input": {
                        "task_snapshot": {},
                        "task_snapshot_canonical": "{}",
                        "text": "return marker",
                    },
                }
                result = engine.handle(dispatch)[0]
                self.assertEqual(result["outcome"], "applied", result)
                self.assertEqual(result["turn_id"], "turn-fixture-1")
                self.assertEqual(result["native_input_id"], "native-item-fixture-9")
                self.assertNotEqual(result["native_input_id"], result["details"]["client_user_message_id"])
                self.assertEqual(
                    result["details"]["client_user_message_id"], "operation-fixture-dispatch"
                )
                prompt = "Task specification: {}\n\nreturn marker"
                self.assertEqual(result["details"]["prompt_sha256"], hashlib.sha256(prompt.encode()).hexdigest())
                self.assertEqual(result["details"]["prompt_bytes"], len(prompt.encode()))
                self.assertEqual(result["details"]["completion_condition"], "native_input_admitted")
                self.assertIsNone(result["details"]["served_model"])
                self.assertEqual(result["details"]["billing_status"], "unknown")

                thread_start = next(m for m in transport.sent if m.get("method") == "thread/start")
                self.assertEqual(thread_start["params"]["modelProvider"], "fixture-provider")
                self.assertEqual(thread_start["params"]["model"], "fixture-model")
                self.assertEqual(thread_start["params"]["cwd"], FIXTURE_WORKSPACE)
                turn_start = next(m for m in transport.sent if m.get("method") == "turn/start")
                self.assertEqual(turn_start["params"]["model"], "fixture-model")
                self.assertEqual(
                    turn_start["params"]["clientUserMessageId"], "operation-fixture-dispatch"
                )
                self.assertEqual(
                    turn_start["params"]["input"], [{"type": "text", "text": prompt}]
                )
                self.assertNotIn("turn-fixture-1", client._router._turn_notifications)
                self.assertNotIn("turn-fixture-1", client._router._turn_states)

                # Simulate a process loss after native acceptance but before
                # the original outcome receipt. The next bridge reads exact
                # native history and does not send a second turn/start.
                record = checkpoint.data["operations"]["operation-fixture-dispatch"]
                record["outcome"] = None
                record["state"] = "native_effect_may_have_started"
                checkpoint.save()
                recovered_engine = ControllerEngine(
                    checkpoint, lambda: (client, executor, scope_key)
                )
                recovered = recovered_engine.handle(dispatch)[0]
                self.assertEqual(recovered["outcome"], "applied")
                self.assertEqual(recovered["turn_id"], "turn-fixture-1")
                self.assertEqual(recovered["native_input_id"], "native-item-fixture-9")
                self.assertEqual(
                    sum(m.get("method") == "turn/start" for m in transport.sent), 1
                )
                observed = recovered_engine.observation()
                turn = observed["turns"][0]
                self.assertEqual(turn["sessionId"], "thread-fixture-root")
                self.assertEqual(turn["turnId"], "turn-fixture-1")
                self.assertEqual(turn["nativeInputId"], "native-item-fixture-9")
                self.assertEqual(turn["clientUserMessageId"], "operation-fixture-dispatch")
                self.assertNotIn("terminal", turn)
                self.assertEqual(ControllerEngine._turn_observation("s", "t", "interrupted")["terminal"], "cancelled")

                changed_route = json.loads(json.dumps(route))
                changed_route["native_options"]["model"] = "different-model"
                changed = recovered_engine.handle(
                    {
                        **dispatch,
                        "operation_id": "operation-fixture-wrong-route",
                        "route": changed_route,
                    }
                )[0]
                self.assertEqual(changed["outcome"], "rejected")
                self.assertEqual(changed["details"]["diagnostic_code"], "ROUTE_CONFIGURATION_MISMATCH")
                self.assertEqual(
                    sum(m.get("method") == "turn/start" for m in transport.sent), 1
                )
        finally:
            client.close()

    def test_open_fails_closed_on_unconfirmed_thread_workspace_or_identity(self):
        route = {
            "runtime": "codex",
            "module_artifact_id": MODULE_ARTIFACT_ID,
            "native_options": {
                "workspaceRoot": FIXTURE_WORKSPACE,
                "modelProvider": "fixture-provider",
                "model": "fixture-model",
            },
        }

        cases = (
            ("workspace-mismatch", OTHER_FIXTURE_WORKSPACE, True, "workspace_mismatch"),
            ("workspace-missing", None, True, "workspace_unknown"),
            ("identity-missing", FIXTURE_WORKSPACE, False, "workspace_exact"),
        )
        for case_name, cwd, include_id, workspace_status in cases:
            with self.subTest(case=case_name):
                script = _controller_script()
                returned_thread = script["responses"]["thread/start"]["result"]["thread"]
                if cwd is None:
                    returned_thread.pop("cwd", None)
                else:
                    returned_thread["cwd"] = cwd
                if not include_id:
                    returned_thread.pop("id", None)

                transport = ScriptedPeerTransport(script)
                client = SharedCodexClient(
                    transport, allowed_methods=bridge.CONTROLLER_METHODS
                )
                client.start()
                try:
                    init = client.initialize()
                    executor = bridge._executor_block(client, init)
                    scope_key = "codex-appserver:fixture:codex-app-server-fixture:0.153.4"
                    owner = {
                        "version": 1,
                        "process": {"purpose": "module"},
                        "token": f"boot-{case_name}",
                    }
                    with tempfile.TemporaryDirectory() as temp_dir:
                        checkpoint = Checkpoint(Path(temp_dir), owner)
                        engine = ControllerEngine(
                            checkpoint, lambda: (client, executor, scope_key)
                        )
                        opened = engine.handle(
                            {
                                "operation_id": f"operation-{case_name}",
                                "method": "agent.open",
                                "route": route,
                            }
                        )[0]
                        self.assertEqual(opened["outcome"], "unknown")
                        self.assertEqual(
                            opened["details"]["requested_workspace_root"],
                            FIXTURE_WORKSPACE,
                        )
                        self.assertEqual(
                            opened["details"]["workspace_status"], workspace_status
                        )
                        if include_id:
                            self.assertEqual(opened["native_root_id"], "thread-fixture-root")
                            self.assertEqual(
                                opened["details"]["observed_workspace_root"], cwd
                            )
                            observed_thread = engine.observation()["native"]["thread"]
                            self.assertEqual(
                                observed_thread["requested_workspace_root"],
                                FIXTURE_WORKSPACE,
                            )
                            self.assertEqual(
                                observed_thread["observed_workspace_root"], cwd
                            )
                            self.assertEqual(
                                observed_thread["workspace_status"], workspace_status
                            )
                            self.assertNotIn("workspace_root", observed_thread)
                        else:
                            self.assertNotIn("native_root_id", opened)
                            self.assertEqual(
                                opened["details"]["observed_workspace_root"],
                                FIXTURE_WORKSPACE,
                            )
                            # A repeated open operation has an ambiguous
                            # native effect; it must retain Unknown without
                            # issuing a second thread/start.
                            repeated = engine.handle(
                                {
                                    "operation_id": f"operation-{case_name}",
                                    "method": "agent.open",
                                    "route": route,
                                }
                            )[0]
                            self.assertEqual(repeated["outcome"], "unknown")
                            self.assertEqual(
                                repeated["details"]["diagnostic_code"],
                                "THREAD_START_IDENTITY_MISSING",
                            )
                        self.assertEqual(
                            sum(m.get("method") == "thread/start" for m in transport.sent),
                            1,
                        )
                finally:
                    client.close()

    def test_steer_requires_persisted_ack_and_history_turn_match(self):
        route = {
            "runtime": "codex",
            "module_artifact_id": MODULE_ARTIFACT_ID,
            "native_options": {
                "workspaceRoot": FIXTURE_WORKSPACE,
                "modelProvider": "fixture-provider",
                "model": "fixture-model",
            },
        }

        def make_engine(operation_id, ack_turn, history_turn, *, lose_ack=False):
            script = _controller_script()
            thread = script["responses"]["thread/read"]["result"]["thread"]
            thread["id"] = "thread-steer-root"
            thread["status"] = {"type": "active", "activeFlags": []}
            script["responses"]["thread/read"]["result"]["thread"] = thread
            script["responses"]["thread/turns/list"] = {
                "result": {
                    "data": [{"id": "turn-expected", "status": "inProgress", "items": []}],
                    "nextCursor": None,
                }
            }
            if lose_ack:
                script["responses"]["turn/steer"] = {
                    "error": {"code": -32000, "message": "synthetic lost acknowledgment"}
                }
            else:
                script["responses"]["turn/steer"] = {"result": {"turnId": ack_turn}}
            script["responses"]["thread/items/list"] = {
                "result": {
                    "data": [
                        {
                            "turnId": history_turn,
                            "item": {
                                "id": f"native-item-{operation_id}",
                                "clientId": operation_id,
                                "content": [{"type": "text", "text": "continue this turn"}],
                                "type": "userMessage",
                            },
                        }
                    ],
                    "nextCursor": None,
                }
            }
            transport = ScriptedPeerTransport(script)
            client = SharedCodexClient(
                transport, allowed_methods=bridge.CONTROLLER_METHODS
            )
            client.start()
            init = client.initialize()
            owner = {"version": 1, "process": {"purpose": "module"}, "token": "boot-steer"}
            temp_dir = tempfile.TemporaryDirectory()
            checkpoint = Checkpoint(Path(temp_dir.name), owner)
            checkpoint.data.update(
                native_root_id="thread-steer-root",
                native_scope_key="codex-appserver:fixture:server:0.153.4",
                requested_model_provider="fixture-provider",
                requested_model="fixture-model",
                effective_model_provider="fixture-provider",
                effective_model="fixture-model",
                effective_model_status="thread_configuration_exact",
                workspace_root=FIXTURE_WORKSPACE,
            )
            checkpoint.save()
            executor = bridge._executor_block(client, init)
            engine = ControllerEngine(
                checkpoint,
                lambda: (
                    client,
                    executor,
                    "codex-appserver:fixture:server:0.153.4",
                ),
            )
            command = {
                "operation_id": operation_id,
                "method": "agent.send",
                "native_root_id": "thread-steer-root",
                "route": route,
                "input": {
                    "text": "continue this turn",
                    "delivery": "steer",
                    "expected_turn_id": "turn-expected",
                },
            }
            return temp_dir, checkpoint, client, transport, engine, command

        cases = (
            ("steer-ack-mismatch", "turn-other", "turn-expected", "steer_ack_turn_mismatch"),
            ("steer-history-mismatch", "turn-expected", "turn-other", "steer_history_turn_mismatch"),
        )
        for operation_id, ack_turn, history_turn, expected_diagnostic in cases:
            with self.subTest(operation_id=operation_id):
                temp_dir, checkpoint, client, transport, engine, command = make_engine(
                    operation_id, ack_turn, history_turn
                )
                try:
                    outcome = engine.handle(command)[0]
                    self.assertEqual(checkpoint.data["operations"][operation_id]["expected_turn_id"], "turn-expected")
                    self.assertEqual(outcome["outcome"], "unknown")
                    self.assertEqual(outcome["details"]["diagnostic_code"], expected_diagnostic)
                    self.assertEqual(
                        sum(message.get("method") == "turn/steer" for message in transport.sent), 1
                    )
                finally:
                    client.close()
                    temp_dir.cleanup()

        temp_dir, checkpoint, client, transport, engine, command = make_engine(
            "steer-lost-ack", None, "turn-other", lose_ack=True
        )
        try:
            first = engine.handle(command)[0]
            self.assertEqual(first["outcome"], "unknown")
            self.assertEqual(
                checkpoint.data["operations"]["steer-lost-ack"]["expected_turn_id"],
                "turn-expected",
            )
            reconciled = engine.handle(
                {
                    "operation_id": "reconcile-steer-lost-ack",
                    "method": "agent.reconcile",
                    "input": {"operation_id": "steer-lost-ack"},
                }
            )
            self.assertEqual(reconciled[0]["outcome"], "unknown")
            self.assertEqual(
                reconciled[1]["details"]["disposition"],
                "steer_history_turn_mismatch",
            )
            self.assertEqual(
                sum(message.get("method") == "turn/steer" for message in transport.sent), 1
            )
        finally:
            client.close()
            temp_dir.cleanup()

        temp_dir, checkpoint, client, transport, engine, command = make_engine(
            "old-checkpoint-steer", None, "turn-expected"
        )
        try:
            checkpoint.data["operations"]["old-checkpoint-steer"] = {
                "method": "agent.send",
                "kind": "send",
                "state": "native_effect_may_have_started",
                "native_root_id": "thread-steer-root",
                "native_scope_key": "codex-appserver:fixture:server:0.153.4",
                "client_user_message_id": "old-checkpoint-steer",
                "prompt_sha256": hashlib.sha256(b"continue this turn").hexdigest(),
                "prompt_bytes": len(b"continue this turn"),
                "delivery": "steer",
                "outcome": None,
            }
            checkpoint.save()
            outcome = engine.handle(command)[0]
            self.assertEqual(outcome["outcome"], "unknown")
            self.assertEqual(
                outcome["details"]["diagnostic_code"], "expected_steer_turn_missing"
            )
            sent_methods = [message.get("method") for message in transport.sent]
            self.assertNotIn("turn/steer", sent_methods)
            self.assertNotIn("thread/items/list", sent_methods)
        finally:
            client.close()
            temp_dir.cleanup()

    def test_legacy_unknown_scope_allows_reconcile_but_blocks_new_dispatch(self):
        script = _controller_script()
        transport = ScriptedPeerTransport(script)
        client = SharedCodexClient(
            transport, allowed_methods=bridge.CONTROLLER_METHODS
        )
        client.start()
        init = client.initialize()
        executor = bridge._executor_block(client, init)
        root_id = "thread-fixture-root"
        operation_id = "operation-fixture-dispatch"
        prompt = "Task specification: {}\n\nreturn marker"
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        legacy_scope = "codex-appserver:fixture:unknown-server:unknown-version"
        owner = {"version": 1, "process": {"purpose": "module"}, "token": "boot-legacy"}
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint = Checkpoint(Path(temp_dir), owner)
            checkpoint.data.update(
                native_root_id=root_id,
                native_scope_key=legacy_scope,
                legacy_scope_read_only=True,
                requested_model_provider="fixture-provider",
                requested_model="fixture-model",
                effective_model_provider="fixture-provider",
                effective_model="fixture-model",
                effective_model_status="thread_configuration_exact",
                workspace_root=FIXTURE_WORKSPACE,
                requested_workspace_root=FIXTURE_WORKSPACE,
                observed_workspace_root=FIXTURE_WORKSPACE,
                workspace_status="workspace_exact",
            )
            checkpoint.data["operations"][operation_id] = {
                "method": "task.dispatch",
                "kind": "send",
                "state": "reported_pending",
                "native_root_id": root_id,
                "native_scope_key": legacy_scope,
                "client_user_message_id": operation_id,
                "requested_model_provider": "fixture-provider",
                "requested_model": "fixture-model",
                "effective_model_provider": "fixture-provider",
                "effective_model": "fixture-model",
                "prompt_sha256": prompt_hash,
                "prompt_bytes": len(prompt.encode("utf-8")),
                "delivery": "next_turn",
                "returned_turn_id": "turn-fixture-1",
                "turn_status": "inProgress",
                "outcome": {
                    "operation_id": operation_id,
                    "outcome": "unknown",
                    "native_root_id": root_id,
                    "native_scope_key": legacy_scope,
                    "details": {"diagnostic_code": "prior_readback_unavailable"},
                },
            }
            checkpoint.save()
            engine = ControllerEngine(
                checkpoint,
                lambda: (client, executor, legacy_scope),
            )
            try:
                reconciled = engine.handle(
                    {
                        "operation_id": "operation-fixture-reconcile",
                        "method": "agent.reconcile",
                        "input": {"operation_id": operation_id},
                    }
                )
                self.assertEqual(reconciled[0]["outcome"], "applied")
                self.assertEqual(reconciled[0]["turn_id"], "turn-fixture-1")
                self.assertEqual(
                    reconciled[0]["native_input_id"], "native-item-fixture-9"
                )
                self.assertEqual(reconciled[0]["native_scope_key"], legacy_scope)
                self.assertEqual(reconciled[1]["details"]["resolved"], True)

                route = {
                    "runtime": "codex",
                    "module_artifact_id": MODULE_ARTIFACT_ID,
                    "native_options": {
                        "workspaceRoot": FIXTURE_WORKSPACE,
                        "modelProvider": "fixture-provider",
                        "model": "fixture-model",
                    },
                }
                rejected = engine.handle(
                    {
                        "operation_id": "operation-fixture-new-dispatch",
                        "method": "task.dispatch",
                        "native_root_id": root_id,
                        "route": route,
                        "input": {
                            "task_snapshot": {},
                            "task_snapshot_canonical": "{}",
                            "text": "return marker",
                        },
                    }
                )[0]
                self.assertEqual(rejected["outcome"], "rejected")
                self.assertEqual(
                    rejected["details"]["diagnostic_code"],
                    "LEGACY_NATIVE_SCOPE_READ_ONLY",
                )
                self.assertEqual(checkpoint.data["native_scope_key"], legacy_scope)
                sent_methods = [
                    message.get("method")
                    for message in transport.sent
                    if message.get("method")
                ]
                self.assertEqual(sent_methods.count("thread/items/list"), 1)
                self.assertNotIn("thread/start", sent_methods)
                self.assertNotIn("turn/start", sent_methods)
                self.assertNotIn("turn/steer", sent_methods)
            finally:
                client.close()


if __name__ == "__main__":
    unittest.main()
