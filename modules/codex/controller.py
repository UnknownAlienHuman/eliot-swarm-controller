"""ELIOT host-module controller for the pinned Codex app-server SDK.

This is deliberately a bridge over an operator-owned app-server, not a
second Codex process owner. It records operation identity before a native
write and resolves that identity only from native history; it never retries
an ambiguous model write.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import socket
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field

from openai_codex.errors import CodexError
from openai_codex.generated.v2_all import (
    ThreadItemsListResponse,
    ThreadStartParams,
    ThreadTurnsListResponse,
    TurnStartParams,
    TurnStartResponse,
    TurnSteerResponse,
)

MODULE_ARTIFACT_ID = "codex-sdk-18194bf-bridge.2"
MAX_FRAME_BYTES = 1_048_576
MAX_HISTORY_PAGES = 100
PAGE_SIZE = 100


class _ThreadStartIdentity(BaseModel):
    """The identity projection needed even when start omits required fields."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    id: str | None = None
    cwd: str | None = None
    model: str | None = None
    model_provider: str | None = Field(default=None, alias="modelProvider")


class _ThreadStartIdentityResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    thread: _ThreadStartIdentity | None = None

CAPABILITIES = {
    "describe": "implemented",
    "open": "implemented",
    "send_next_turn": "implemented",
    "steer": "implemented",
    "refresh": "implemented",
    "reconcile": "implemented",
    "recover": "implemented",
    "configure_model": "unavailable",
    "configure_effort": "unavailable",
    "goal": "unavailable",
    "reply": "unavailable",
    "background": "unavailable",
    "family": "unavailable",
    "result_pages": "unavailable",
    "artifact_publication": "unavailable",
}


class ControllerError(CodexError):
    def __init__(self, code: str, *, admission_possible: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.admission_possible = admission_possible


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ControllerError(f"MISSING_{name.upper()}")
    return value


def _digest_text(text: str) -> tuple[str, int]:
    encoded = text.encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _absolute_workspace(value: Any) -> str:
    root = _required_string(value, "workspaceRoot")
    if not Path(root).is_absolute():
        raise ControllerError("WORKSPACE_ROOT_MUST_BE_ABSOLUTE")
    return root


def _workspace_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value if value.strip() else None
    # The pinned SDK models AbsolutePathBuf as a Pydantic RootModel[str].
    # Read that root value directly; do not resolve it against the filesystem.
    root = getattr(value, "root", None)
    if isinstance(root, str):
        return root if root.strip() else None
    return None


def _workspace_facts(requested: Any, observed: Any) -> dict[str, Any]:
    requested_path = _workspace_text(requested)
    observed_path = _workspace_text(observed)
    if requested_path is None or observed_path is None:
        status = "workspace_unknown"
    else:
        # This is deliberately lexical: resolving symlinks or filesystem paths
        # would turn an app-server configuration report into a host-side probe.
        status = (
            "workspace_exact"
            if os.path.normcase(os.path.abspath(requested_path))
            == os.path.normcase(os.path.abspath(observed_path))
            else "workspace_mismatch"
        )
    return {
        "requested_workspace_root": requested_path,
        "observed_workspace_root": observed_path,
        "workspace_status": status,
    }


def _prompt_for(command: dict[str, Any]) -> str:
    operation = command.get("operation_id")
    payload = command.get("input") or {}
    if command.get("method") == "task.dispatch":
        canonical = payload.get("task_snapshot_canonical")
        text = payload.get("text")
        if not isinstance(canonical, str) or not canonical:
            raise ControllerError("TASK_SNAPSHOT_CANONICAL_REQUIRED")
        if not isinstance(text, str) or not text.strip():
            raise ControllerError("PROMPT_REQUIRED")
        # The host supplies its exact canonical UTF-8 rendering so the digest
        # can be checked byte-for-byte by Rust without Python reserialization.
        prompt = f"Task specification: {canonical}\n\n{text}"
    else:
        prompt = payload.get("text")
    if not isinstance(operation, str) or not operation.strip():
        raise ControllerError("MISSING_OPERATION_ID")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ControllerError("PROMPT_REQUIRED")
    return prompt


def _safe_endpoint_identity(endpoint: str) -> str:
    """Hash an endpoint after removing credentials/query without returning it."""
    parsed = urlsplit(endpoint)
    host = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        port = None
    authority = host.lower() + (f":{port}" if port is not None else "")
    safe = urlunsplit((parsed.scheme.lower(), authority, parsed.path, "", ""))
    return hashlib.sha256(safe.encode("utf-8")).hexdigest()


def _thread_details(thread: Any, requested_workspace: str | None) -> dict[str, Any]:
    observed_workspace = _workspace_text(getattr(thread, "cwd", None))
    return {
        "id": getattr(thread, "id", None),
        "model_provider": getattr(thread, "model_provider", None),
        "model": getattr(thread, "model", None),
        "cwd": observed_workspace,
        **_workspace_facts(requested_workspace, observed_workspace),
    }


def _active_turn_id(thread: Any) -> str | None:
    status = getattr(thread, "status", None)
    root = getattr(status, "root", status)
    if getattr(root, "type", None) in ("active", "running"):
        return getattr(root, "active_turn_id", None) or getattr(root, "turn_id", None)
    if isinstance(root, dict) and root.get("type") in ("active", "running"):
        return root.get("activeTurnId") or root.get("turnId")
    return None


def _is_idle(thread: Any) -> bool:
    status = getattr(thread, "status", None)
    root = getattr(status, "root", status)
    if getattr(root, "type", None) == "idle":
        return True
    if isinstance(root, dict):
        return root.get("type") == "idle"
    return False


def _user_item_text(item: Any) -> str | None:
    root = getattr(item, "root", item)
    if getattr(root, "type", None) != "userMessage":
        return None
    content = getattr(root, "content", None)
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for entry in content:
        value = getattr(entry, "root", entry)
        if getattr(value, "type", None) != "text":
            return None
        text = getattr(value, "text", None)
        if not isinstance(text, str):
            return None
        parts.append(text)
    return "".join(parts)


class Checkpoint:
    """Atomic private checkpoint; prompt contents and credentials are excluded."""

    def __init__(self, directory: Path, owner: dict[str, Any]) -> None:
        if not directory.is_absolute():
            raise ControllerError("MODULE_STATE_PATH_MUST_BE_ABSOLUTE")
        if (
            owner.get("version") != 1
            or (owner.get("process") or {}).get("purpose") != "module"
            or not isinstance(owner.get("token"), str)
        ):
            raise ControllerError("INVALID_MODULE_OWNER_RECORD")
        self.directory = directory
        self.path = directory / "checkpoint.json"
        self.owner = owner
        self.data: dict[str, Any] = {
            "version": 1,
            "module_artifact_id": MODULE_ARTIFACT_ID,
            "boot_id": owner["token"],
            "binding_id": None,
            "generation": None,
            "native_root_id": None,
            "native_scope_key": None,
            "requested_model_provider": None,
            "requested_model": None,
            "workspace_root": None,
            "requested_workspace_root": None,
            "observed_workspace_root": None,
            "workspace_status": "workspace_unknown",
            "effective_model_provider": None,
            "effective_model": None,
            "operations": {},
        }
        self._load()

    def _load(self) -> None:
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            raise ControllerError("CHECKPOINT_UNREADABLE") from exc
        if not isinstance(loaded, dict) or loaded.get("version") != 1:
            raise ControllerError("CHECKPOINT_VERSION_UNKNOWN")
        if loaded.get("module_artifact_id") != MODULE_ARTIFACT_ID:
            raise ControllerError("CHECKPOINT_ARTIFACT_MISMATCH")
        if not isinstance(loaded.get("operations"), dict):
            raise ControllerError("CHECKPOINT_CORRUPT")
        self.data.update(loaded)

    def save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(self.data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        fd, temp_name = tempfile.mkstemp(prefix=".codex-checkpoint-", suffix=".tmp", dir=self.directory)
        temp_path = Path(temp_name)
        try:
            if os.name != "nt":
                os.chmod(temp_path, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.path)
            if os.name != "nt":
                directory_fd = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    def bind(self, binding_id: str, generation: int) -> None:
        old_id = self.data.get("binding_id")
        old_generation = self.data.get("generation")
        if old_id is not None and (old_id != binding_id or old_generation != generation):
            raise ControllerError("CHECKPOINT_BINDING_CHANGED")
        self.data["binding_id"] = binding_id
        self.data["generation"] = generation


class _WindowsNamedPipe:
    """Small synchronous byte-mode client for the host's Windows named pipe."""

    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

    def __init__(self, endpoint: str) -> None:
        if os.name != "nt":
            raise ControllerError("WINDOWS_NAMED_PIPE_ON_NON_WINDOWS")
        self.endpoint = endpoint
        self._kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self._kernel.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        self._kernel.CreateFileW.restype = ctypes.c_void_p
        self._kernel.WriteFile.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_ulong),
            ctypes.c_void_p,
        ]
        self._kernel.WriteFile.restype = ctypes.c_int
        self._kernel.ReadFile.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_ulong),
            ctypes.c_void_p,
        ]
        self._kernel.ReadFile.restype = ctypes.c_int
        self._kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        self._kernel.CloseHandle.restype = ctypes.c_int
        self._handle = None
        deadline = time.monotonic() + 5
        while True:
            handle = self._kernel.CreateFileW(
                endpoint,
                self.GENERIC_READ | self.GENERIC_WRITE,
                0,
                None,
                self.OPEN_EXISTING,
                self.FILE_ATTRIBUTE_NORMAL,
                None,
            )
            if handle != self.INVALID_HANDLE_VALUE:
                self._handle = handle
                break
            error = ctypes.get_last_error()
            if error != 231 or time.monotonic() >= deadline:
                raise OSError(error, "unable to connect to host named pipe")
            time.sleep(0.025)
        self._buffer = bytearray()

    def sendall(self, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            chunk = data[offset:]
            written = ctypes.c_ulong()
            buf = ctypes.create_string_buffer(chunk)
            ok = self._kernel.WriteFile(self._handle, buf, len(chunk), ctypes.byref(written), None)
            if not ok or written.value <= 0:
                raise OSError(ctypes.get_last_error(), "host named-pipe write failed")
            offset += written.value

    def readline(self) -> bytes:
        while True:
            end = self._buffer.find(b"\n")
            if end >= 0:
                line = bytes(self._buffer[:end])
                del self._buffer[: end + 1]
                return line
            if len(self._buffer) > MAX_FRAME_BYTES:
                raise ControllerError("HOST_FRAME_TOO_LARGE")
            buf = ctypes.create_string_buffer(65536)
            read = ctypes.c_ulong()
            ok = self._kernel.ReadFile(self._handle, buf, len(buf), ctypes.byref(read), None)
            if not ok or read.value == 0:
                raise OSError(ctypes.get_last_error(), "host named-pipe read failed")
            self._buffer.extend(buf.raw[: read.value])

    def close(self) -> None:
        if self._handle is not None:
            self._kernel.CloseHandle(self._handle)
            self._handle = None


class HostLink:
    """Newline JSON-RPC client matching the local Rust host IPC framing."""

    def __init__(self, endpoint: str, credential: dict[str, Any]) -> None:
        self.endpoint = endpoint
        self.credential = credential
        self.transport: Any = None

    def connect(self) -> dict[str, Any]:
        if self.endpoint.startswith("\\\\.\\pipe\\"):
            self.transport = _WindowsNamedPipe(self.endpoint)
        else:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.endpoint)
            self.transport = sock
            self._reader = sock.makefile("rb")
        result = self.call("client.hello", self.credential)
        return result

    def _sendall(self, data: bytes) -> None:
        if isinstance(self.transport, socket.socket):
            self.transport.sendall(data)
        else:
            self.transport.sendall(data)

    def _readline(self) -> bytes:
        if isinstance(self.transport, socket.socket):
            line = self._reader.readline(MAX_FRAME_BYTES + 1)
            if not line:
                raise ConnectionError("host IPC disconnected")
            return line.rstrip(b"\r\n")
        return self.transport.readline()

    def call(self, method: str, params: Any) -> Any:
        if self.transport is None:
            raise ConnectionError("host IPC is not connected")
        request_id = str(uuid.uuid4())
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        if len(payload) > MAX_FRAME_BYTES:
            raise ControllerError("HOST_FRAME_TOO_LARGE")
        self._sendall(payload)
        raw = self._readline()
        if len(raw) > MAX_FRAME_BYTES:
            raise ControllerError("HOST_FRAME_TOO_LARGE")
        response = json.loads(raw.decode("utf-8"))
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise ControllerError("HOST_RESPONSE_ID_MISMATCH")
        if "error" in response:
            error = response["error"]
            raise ControllerError(str((error.get("data") or {}).get("code") or "HOST_RPC_ERROR"))
        if "result" not in response:
            raise ControllerError("HOST_RESPONSE_MISSING_RESULT")
        return response["result"]

    def close(self) -> None:
        if self.transport is None:
            return
        try:
            if isinstance(self.transport, socket.socket):
                self._reader.close()
                self.transport.close()
            else:
                self.transport.close()
        finally:
            self.transport = None


class ControllerEngine:
    """Native controller operations. `client_factory` attaches but never spawns."""

    def __init__(
        self,
        checkpoint: Checkpoint,
        client_factory: Callable[[], tuple[Any, dict[str, Any], str]],
    ) -> None:
        self.checkpoint = checkpoint
        self.client_factory = client_factory
        self.outcomes: dict[str, dict[str, Any]] = {}
        self.native_attached = False
        self.last_native: dict[str, Any] = {"turns": []}

    @property
    def data(self) -> dict[str, Any]:
        return self.checkpoint.data

    def hello_facts(self) -> dict[str, Any]:
        return {
            "native_ready": self.native_attached,
            "native_root_id": self.data.get("native_root_id"),
            "native_scope_key": self.data.get("native_scope_key"),
        }

    def _base_details(self, executor: dict[str, Any]) -> dict[str, Any]:
        return {
            "module_artifact_id": MODULE_ARTIFACT_ID,
            "executor": {
                "server_name": executor.get("server_name"),
                "server_version": executor.get("server_version"),
                "sdk_version": executor.get("sdk_version"),
                "sdk_upstream_commit": executor.get("sdk_upstream_commit"),
            },
            "served_model": None,
            "served_model_status": "unknown",
            "billing_status": "unknown",
            "fallback_used": False,
        }

    def _finish(
        self,
        operation_id: str,
        outcome: str,
        details: dict[str, Any],
        *,
        root_id: str | None = None,
        scope_key: str | None = None,
        turn_id: str | None = None,
        native_input_id: str | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "operation_id": operation_id,
            "outcome": outcome,
            "details": details,
        }
        root_id = root_id or self.data.get("native_root_id")
        scope_key = scope_key or self.data.get("native_scope_key")
        if root_id:
            result["native_root_id"] = root_id
        if scope_key:
            result["native_scope_key"] = scope_key
        if turn_id:
            result["turn_id"] = turn_id
        if native_input_id:
            result["native_input_id"] = native_input_id
        self.outcomes[operation_id] = result
        record = self.data["operations"].get(operation_id)
        if isinstance(record, dict):
            record["outcome"] = result
            record["state"] = "reported_pending"
            self.checkpoint.save()
        return result

    def _client(self) -> tuple[Any, dict[str, Any], str]:
        client, executor, scope_key = self.client_factory()
        return client, executor, scope_key

    def _legacy_scope_requires_read_only(self) -> bool:
        scope_key = self.data.get("native_scope_key")
        return (
            isinstance(scope_key, str)
            and scope_key.endswith(":unknown-server:unknown-version")
        )

    def _observe_workspace(self, observed: Any) -> dict[str, Any]:
        requested = self.data.get("requested_workspace_root")
        if requested is None:
            # Compatibility for checkpoints written before workspace
            # provenance was split into requested and observed values.
            requested = self.data.get("workspace_root")
        facts = _workspace_facts(requested, observed)
        self.data.update(facts)
        self.checkpoint.save()
        return facts

    def _route(self, command: dict[str, Any]) -> tuple[str, str, str]:
        route = command.get("route") or {}
        if (
            route.get("runtime") != "codex"
            or route.get("module_artifact_id") != MODULE_ARTIFACT_ID
        ):
            raise ControllerError("CODEX_ROUTE_ARTIFACT_MISMATCH")
        options = route.get("native_options") or {}
        provider = _required_string(options.get("modelProvider"), "modelProvider")
        model = _required_string(options.get("model"), "model")
        workspace = _absolute_workspace(options.get("workspaceRoot"))
        return provider, model, workspace

    def _assert_root(self, command: dict[str, Any]) -> str:
        expected = self.data.get("native_root_id")
        supplied = command.get("native_root_id")
        if not expected or supplied != expected:
            raise ControllerError("NATIVE_IDENTITY_MISMATCH")
        return expected

    def _read_history(self, client: Any, root_id: str, client_id: str) -> tuple[list[dict[str, Any]], bool]:
        cursor: str | None = None
        matches: list[dict[str, Any]] = []
        for page_index in range(MAX_HISTORY_PAGES):
            params: dict[str, Any] = {
                "threadId": root_id,
                "limit": PAGE_SIZE,
                "sortDirection": "desc",
            }
            if cursor:
                params["cursor"] = cursor
            response = client.request(
                "thread/items/list", params, response_model=ThreadItemsListResponse
            )
            for entry in response.data:
                item = entry.item.root
                if getattr(item, "type", None) != "userMessage":
                    continue
                if getattr(item, "client_id", None) != client_id:
                    continue
                matches.append(
                    {
                        "native_input_id": getattr(item, "id", None),
                        "turn_id": getattr(entry, "turn_id", None),
                        "client_user_message_id": getattr(item, "client_id", None),
                        "text": _user_item_text(entry.item),
                    }
                )
            cursor = response.next_cursor
            if not cursor:
                return matches, False
        return matches, True

    def _resolve_send(self, record: dict[str, Any], client: Any) -> tuple[dict[str, Any] | None, str]:
        is_steer = record.get("delivery") == "steer"
        expected_turn = record.get("expected_turn_id")
        if is_steer and (not isinstance(expected_turn, str) or not expected_turn):
            # Older checkpoints did not persist the precondition. Never infer
            # it from whichever turn happens to be active during recovery.
            return None, "expected_steer_turn_missing"
        root_id = record.get("native_root_id") or self.data.get("native_root_id")
        client_id = record.get("client_user_message_id")
        if not root_id or not client_id:
            return None, "native_identity_unavailable"
        try:
            matches, truncated = self._read_history(client, root_id, client_id)
        except Exception:
            return None, "native_history_read_failed"
        if truncated:
            return None, "native_history_page_limit"
        if len(matches) == 0:
            return None, "native_item_not_observed"
        if len(matches) != 1:
            return None, "native_item_correlation_not_unique"
        match = matches[0]
        digest, byte_count = _digest_text(match["text"] or "")
        if (
            digest != record.get("prompt_sha256")
            or byte_count != record.get("prompt_bytes")
            or not match.get("native_input_id")
            or not match.get("turn_id")
        ):
            return None, "native_item_content_or_identity_mismatch"
        returned_turn = record.get("returned_turn_id")
        if is_steer and returned_turn is not None and returned_turn != expected_turn:
            return None, "steer_ack_turn_mismatch"
        if is_steer and match["turn_id"] != expected_turn:
            return None, "steer_history_turn_mismatch"
        if returned_turn and returned_turn != match["turn_id"]:
            return None, "native_turn_identity_mismatch"
        return match, "native_item_readback_verified"

    def _dispatch(self, command: dict[str, Any]) -> dict[str, Any]:
        operation_id = _required_string(command.get("operation_id"), "operation_id")
        method = command.get("method")
        previous = self.data["operations"].get(operation_id)
        if previous:
            if previous.get("outcome"):
                self.outcomes[operation_id] = previous["outcome"]
                return previous["outcome"]
            # A persisted pre-write marker means the native request may have
            # crossed the socket. Only exact readback is allowed; never resubmit.
            try:
                client, executor, _ = self._client()
                match, disposition = self._resolve_send(previous, client)
            except Exception:
                match, disposition = None, "native_client_unavailable"
                executor = {}
            if match:
                details = self._base_details(executor)
                details.update(self._model_facts())
                details.update(self._input_facts(previous))
                details.update(
                    completion_condition="native_input_admitted",
                    client_user_message_id=previous["client_user_message_id"],
                    native_input_readback="verified",
                )
                previous["turn_id"] = match["turn_id"]
                previous["native_input_id"] = match["native_input_id"]
                self.checkpoint.save()
                self.last_native["turns"] = [
                    self._turn_observation(
                        previous["native_root_id"],
                        match["turn_id"],
                        previous.get("turn_status") or "unknown",
                        match["native_input_id"],
                        match["client_user_message_id"],
                    )
                ]
                return self._finish(
                    operation_id,
                    "applied",
                    details,
                    root_id=previous["native_root_id"],
                    scope_key=previous["native_scope_key"],
                    turn_id=match["turn_id"],
                    native_input_id=match["native_input_id"],
                )
            return self._finish(
                operation_id,
                "unknown",
                {
                    "diagnostic_code": disposition,
                    "native_replay": False,
                    **self._input_facts(previous),
                },
                root_id=previous.get("native_root_id"),
                scope_key=previous.get("native_scope_key"),
            )

        root_id = self._assert_root(command)
        if self._legacy_scope_requires_read_only():
            # The bridge reattaches only after initialize proves the actual
            # executor, while preserving this historical scope key. Existing
            # sessions with unknown identity remain read-only.
            self._client()
            return self._finish(
                operation_id,
                "rejected",
                {
                    "diagnostic_code": "LEGACY_NATIVE_SCOPE_READ_ONLY",
                    "native_replay": False,
                },
                root_id=root_id,
                scope_key=self.data.get("native_scope_key"),
            )
        try:
            provider, model, workspace = self._route(command)
        except ControllerError as exc:
            return self._finish(
                operation_id,
                "rejected",
                {"diagnostic_code": exc.code, **self._model_facts()},
            )
        if (
            provider != self.data.get("requested_model_provider")
            or model != self.data.get("requested_model")
            or workspace != self.data.get("workspace_root")
        ):
            return self._finish(
                operation_id,
                "rejected",
                {
                    "diagnostic_code": "ROUTE_CONFIGURATION_MISMATCH",
                    **self._model_facts(),
                },
                root_id=root_id,
                scope_key=self.data.get("native_scope_key"),
            )
        if method == "agent.send" and (command.get("input") or {}).get("delivery") == "steer":
            delivery = "steer"
        else:
            delivery = "next_turn"
        expected_turn: str | None = None
        prompt = _prompt_for(command)
        digest, byte_count = _digest_text(prompt)
        client_user_message_id = operation_id
        if delivery == "next_turn":
            client, executor, scope_key = self._client()
            thread = client.thread_read(root_id, include_turns=False).thread
            workspace_facts = self._observe_workspace(getattr(thread, "cwd", None))
            if workspace_facts["workspace_status"] != "workspace_exact":
                return self._finish(
                    operation_id,
                    "rejected",
                    {
                        "diagnostic_code": "THREAD_WORKSPACE_NOT_CONFIRMED",
                        **workspace_facts,
                        **self._model_facts(),
                    },
                    root_id=root_id,
                    scope_key=scope_key,
                )
            if not _is_idle(thread):
                return self._finish(
                    operation_id,
                    "rejected",
                    {"diagnostic_code": "THREAD_NOT_IDLE", **self._model_facts()},
                    root_id=root_id,
                    scope_key=scope_key,
                )
        else:
            client, executor, scope_key = self._client()
            input_data = command.get("input") or {}
            expected_turn = _required_string(input_data.get("expected_turn_id"), "expected_turn_id")
            thread = client.thread_read(root_id, include_turns=False).thread
            workspace_facts = self._observe_workspace(getattr(thread, "cwd", None))
            if workspace_facts["workspace_status"] != "workspace_exact":
                return self._finish(
                    operation_id,
                    "rejected",
                    {
                        "diagnostic_code": "THREAD_WORKSPACE_NOT_CONFIRMED",
                        **workspace_facts,
                        **self._model_facts(),
                    },
                    root_id=root_id,
                    scope_key=scope_key,
                )
            active_turns = self._active_turns(client, root_id)
            if (
                _is_idle(thread)
                or len(active_turns) != 1
                or active_turns[0].id != expected_turn
            ):
                return self._finish(
                    operation_id,
                    "rejected",
                    {"diagnostic_code": "EXPECTED_TURN_NOT_ACTIVE", **self._model_facts()},
                    root_id=root_id,
                    scope_key=scope_key,
                )

        record = {
            "method": method,
            "state": "native_effect_may_have_started",
            "kind": "send",
            "native_root_id": root_id,
            "native_scope_key": scope_key,
            "client_user_message_id": client_user_message_id,
            "requested_model_provider": self.data.get("requested_model_provider"),
            "requested_model": self.data.get("requested_model"),
            "effective_model_provider": self.data.get("effective_model_provider"),
            "effective_model": self.data.get("effective_model"),
            "prompt_sha256": digest,
            "prompt_bytes": byte_count,
            "delivery": delivery,
            "expected_turn_id": expected_turn,
            "returned_turn_id": None,
            "turn_status": None,
            "outcome": None,
        }
        self.data["operations"][operation_id] = record
        self.checkpoint.save()
        try:
            user_input = [{"type": "text", "text": prompt}]
            if delivery == "next_turn":
                # turn_start installs a low-level event subscription for a
                # streaming consumer. This controller only checks admission
                # and history, so issue the pinned typed request directly.
                params = TurnStartParams.model_validate(
                    {
                        "threadId": root_id,
                        "input": user_input,
                        "model": self.data["requested_model"],
                        "clientUserMessageId": client_user_message_id,
                    }
                ).model_dump(by_alias=True, exclude_none=True, exclude_unset=True)
                started = client.request(
                    "turn/start",
                    params,
                    response_model=TurnStartResponse,
                )
                returned_turn_id = started.turn.id
                turn_status = str(getattr(started.turn.status, "value", started.turn.status))
            else:
                started = client.request(
                    "turn/steer",
                    {
                        "threadId": root_id,
                        "expectedTurnId": expected_turn,
                        "clientUserMessageId": client_user_message_id,
                        "input": user_input,
                    },
                    response_model=TurnSteerResponse,
                )
                returned_turn_id = started.turn_id
                turn_status = "inProgress"
            record["returned_turn_id"] = returned_turn_id
            record["turn_status"] = turn_status
            self.checkpoint.save()
            match, disposition = self._resolve_send(record, client)
            if match is None:
                return self._finish(
                    operation_id,
                    "unknown",
                    {
                        "diagnostic_code": disposition,
                        "native_replay": False,
                        **self._model_facts(),
                        **self._input_facts(record),
                    },
                    root_id=root_id,
                    scope_key=scope_key,
                )
            details = self._base_details(executor)
            details.update(self._model_facts())
            details.update(self._input_facts(record))
            details.update(
                completion_condition="native_input_admitted",
                client_user_message_id=client_user_message_id,
                native_input_readback="verified",
            )
            self.last_native["turns"] = [
                self._turn_observation(
                    root_id,
                    match["turn_id"],
                    turn_status,
                    match["native_input_id"],
                    client_user_message_id,
                )
            ]
            record["turn_id"] = match["turn_id"]
            record["native_input_id"] = match["native_input_id"]
            self.checkpoint.save()
            return self._finish(
                operation_id,
                "applied",
                details,
                root_id=root_id,
                scope_key=scope_key,
                turn_id=match["turn_id"],
                native_input_id=match["native_input_id"],
            )
        except Exception as exc:
            return self._finish(
                operation_id,
                "unknown",
                {
                    "diagnostic_code": getattr(exc, "code", type(exc).__name__),
                    "native_replay": False,
                    **self._model_facts(),
                    **self._input_facts(record),
                },
                root_id=root_id,
                scope_key=scope_key,
            )

    def _model_facts(self) -> dict[str, Any]:
        requested_workspace = self.data.get("requested_workspace_root") or self.data.get(
            "workspace_root"
        )
        return {
            "requested_model_provider": self.data.get("requested_model_provider"),
            "requested_model": self.data.get("requested_model"),
            "effective_model_provider": self.data.get("effective_model_provider"),
            "effective_model": self.data.get("effective_model"),
            "effective_model_status": self.data.get("effective_model_status", "unknown"),
            **_workspace_facts(
                requested_workspace,
                self.data.get("observed_workspace_root"),
            ),
            "served_model": None,
            "served_model_status": "unknown",
            "billing_status": "unknown",
        }

    @staticmethod
    def _input_facts(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "prompt_sha256": record.get("prompt_sha256"),
            "prompt_bytes": record.get("prompt_bytes"),
        }

    @staticmethod
    def _terminal(status: str) -> str | None:
        return {
            "completed": "completed",
            "failed": "failed",
            "interrupted": "cancelled",
        }.get(status)

    @classmethod
    def _turn_observation(
        cls,
        root_id: str,
        turn_id: str,
        status: str,
        native_input_id: str | None = None,
        client_user_message_id: str | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "sessionId": root_id,
            "thread_id": root_id,
            "turnId": turn_id,
            "turn_id": turn_id,
            "status": status,
            "event": "codex.thread/turns/list",
            "viewCursor": None,
        }
        terminal = cls._terminal(status)
        if terminal:
            result["terminal"] = terminal
        if native_input_id:
            result["nativeInputId"] = native_input_id
            result["native_input_id"] = native_input_id
        if client_user_message_id:
            result["clientUserMessageId"] = client_user_message_id
            result["client_user_message_id"] = client_user_message_id
        return result

    @staticmethod
    def _active_turns(client: Any, root_id: str) -> list[Any]:
        response = client.request(
            "thread/turns/list",
            {"threadId": root_id, "limit": 20, "sortDirection": "desc"},
            response_model=ThreadTurnsListResponse,
        )
        return [
            turn
            for turn in response.data
            if str(getattr(turn.status, "value", turn.status)) == "inProgress"
        ]

    def _open(self, command: dict[str, Any]) -> dict[str, Any]:
        operation_id = _required_string(command.get("operation_id"), "operation_id")
        if operation_id in self.data["operations"]:
            record = self.data["operations"][operation_id]
            if record.get("outcome"):
                self.outcomes[operation_id] = record["outcome"]
                return record["outcome"]
            return self._finish(
                operation_id,
                "unknown",
                {"diagnostic_code": "OPEN_REQUEST_ALREADY_MAY_HAVE_STARTED", "native_replay": False},
                root_id=record.get("native_root_id"),
                scope_key=record.get("native_scope_key"),
            )
        if any(
            record.get("kind") == "open"
            for record in self.data["operations"].values()
            if isinstance(record, dict)
        ):
            return self._finish(
                operation_id,
                "rejected",
                {
                    "diagnostic_code": "CONTROLLER_THREAD_ALREADY_RESERVED",
                    "native_replay": False,
                },
                root_id=self.data.get("native_root_id"),
                scope_key=self.data.get("native_scope_key"),
            )
        provider, model, workspace = self._route(command)
        client, executor, scope_key = self._client()
        if self._legacy_scope_requires_read_only():
            return self._finish(
                operation_id,
                "rejected",
                {
                    "diagnostic_code": "LEGACY_NATIVE_SCOPE_READ_ONLY",
                    "native_replay": False,
                },
                scope_key=self.data.get("native_scope_key"),
            )
        record = {
            "method": "agent.open",
            "kind": "open",
            "state": "native_effect_may_have_started",
            "requested_model_provider": provider,
            "requested_model": model,
            "native_scope_key": scope_key,
            "outcome": None,
        }
        self.data["requested_model_provider"] = provider
        self.data["requested_model"] = model
        self.data["workspace_root"] = workspace
        self.data["requested_workspace_root"] = workspace
        self.data["observed_workspace_root"] = None
        self.data["workspace_status"] = "workspace_unknown"
        self.data["effective_model_provider"] = None
        self.data["effective_model"] = None
        self.data["effective_model_status"] = "unknown"
        self.data["operations"][operation_id] = record
        self.checkpoint.save()
        try:
            params = ThreadStartParams.model_validate(
                {"cwd": workspace, "modelProvider": provider, "model": model}
            ).model_dump(by_alias=True, exclude_none=True, exclude_unset=True)
            started = client.request(
                "thread/start",
                params,
                response_model=_ThreadStartIdentityResponse,
            )
        except Exception as exc:
            return self._finish(
                operation_id,
                "unknown",
                {
                    **self._base_details(executor),
                    "diagnostic_code": getattr(exc, "code", type(exc).__name__),
                    "requested_model_provider": provider,
                    "requested_model": model,
                    "effective_model_provider": None,
                    "effective_model": None,
                    "effective_model_status": "unknown",
                    **self._model_facts(),
                    "native_replay": False,
                },
                scope_key=scope_key,
            )
        thread = getattr(started, "thread", None)
        root_id = getattr(thread, "id", None)
        workspace_facts = self._observe_workspace(getattr(thread, "cwd", None))
        if not isinstance(root_id, str) or not root_id:
            return self._finish(
                operation_id,
                "unknown",
                {
                    **self._base_details(executor),
                    "diagnostic_code": "THREAD_START_IDENTITY_MISSING",
                    "requested_model_provider": provider,
                    "requested_model": model,
                    "effective_model_provider": None,
                    "effective_model": None,
                    "effective_model_status": "unknown",
                    **workspace_facts,
                    "native_replay": False,
                },
                scope_key=scope_key,
            )
        effective_provider = getattr(thread, "model_provider", None)
        effective_model = getattr(thread, "model", None)
        self.data["native_root_id"] = root_id
        self.data["native_scope_key"] = scope_key
        self.data["effective_model_provider"] = effective_provider
        self.data["effective_model"] = effective_model
        self.data["effective_model_status"] = (
            "thread_configuration_exact"
            if effective_provider == provider and effective_model == model
            else "thread_configuration_mismatch"
        )
        record["native_root_id"] = root_id
        record["effective_model_provider"] = effective_provider
        record["effective_model"] = effective_model
        self.checkpoint.save()
        self.native_attached = True
        details = self._base_details(executor)
        details.update(self._model_facts())
        details["native_replay"] = False
        self.last_native = {
            "thread": {
                "thread_id": root_id,
                "model_provider": effective_provider,
                "configured_model": effective_model,
                **workspace_facts,
                "cwd": getattr(thread, "cwd", None),
                "status": "observed",
            },
            "turns": [],
        }
        if workspace_facts["workspace_status"] != "workspace_exact":
            details["diagnostic_code"] = "REQUESTED_THREAD_WORKSPACE_NOT_CONFIRMED"
            details["native_thread_created_observed"] = True
            return self._finish(
                operation_id,
                "unknown",
                details,
                root_id=root_id,
                scope_key=scope_key,
            )
        if effective_provider != provider or effective_model != model:
            details["diagnostic_code"] = "REQUESTED_THREAD_MODEL_NOT_CONFIRMED"
            details["native_thread_created_observed"] = True
            return self._finish(
                operation_id,
                "unknown",
                details,
                root_id=root_id,
                scope_key=scope_key,
            )
        details["completion_condition"] = "native_thread_created"
        return self._finish(
            operation_id,
            "applied",
            details,
            root_id=root_id,
            scope_key=scope_key,
        )

    def _refresh(self, command: dict[str, Any]) -> dict[str, Any]:
        operation_id = _required_string(command.get("operation_id"), "operation_id")
        root_id = self._assert_root(command)
        client, executor, scope_key = self._client()
        thread = client.thread_read(root_id, include_turns=False).thread
        workspace_facts = self._observe_workspace(getattr(thread, "cwd", None))
        pages: list[Any] = []
        cursor = None
        for _ in range(3):
            params = {"threadId": root_id, "limit": 20, "sortDirection": "desc"}
            if cursor:
                params["cursor"] = cursor
            page = client.request(
                "thread/turns/list", params, response_model=ThreadTurnsListResponse
            )
            pages.extend(page.data)
            cursor = page.next_cursor
            if not cursor:
                break
        turns = []
        for turn in pages:
            status = str(getattr(turn.status, "value", turn.status))
            correlation = next(
                (
                    record
                    for record in self.data["operations"].values()
                    if record.get("native_root_id") == root_id
                    and record.get("turn_id") == turn.id
                    and record.get("native_input_id")
                ),
                None,
            )
            turns.append(
                self._turn_observation(
                    root_id,
                    turn.id,
                    status,
                    correlation.get("native_input_id") if correlation else None,
                    correlation.get("client_user_message_id") if correlation else None,
                )
            )
        active_id = next(
            (turn["turnId"] for turn in turns if turn.get("status") == "inProgress"),
            None,
        )
        native = {
            "thread": _thread_details(
                thread,
                self.data.get("requested_workspace_root") or self.data.get("workspace_root"),
            ),
            "turns": turns,
            "truncated": bool(cursor),
            "model": self._model_facts(),
        }
        self.last_native = native
        self.last_native["turns"] = turns
        self.last_native["active_turn_id"] = active_id
        details = self._base_details(executor)
        details.update(
            target_operation_id=None,
            native=native,
            **self._model_facts(),
        )
        if workspace_facts["workspace_status"] != "workspace_exact":
            details["diagnostic_code"] = "THREAD_WORKSPACE_NOT_CONFIRMED"
            return self._finish(
                operation_id,
                "unknown",
                details,
                root_id=root_id,
                scope_key=scope_key,
            )
        details["completion_condition"] = "native_read_completed"
        return self._finish(
            operation_id,
            "applied",
            details,
            root_id=root_id,
            scope_key=scope_key,
        )

    def _recover(self, command: dict[str, Any]) -> dict[str, Any]:
        operation_id = _required_string(command.get("operation_id"), "operation_id")
        expected_boot = _required_string((command.get("input") or {}).get("expected_boot_id"), "expected_boot_id")
        if expected_boot != self.checkpoint.owner["token"]:
            raise ControllerError("RECOVERY_BOOT_ID_MISMATCH")
        root_id = _required_string(self.data.get("native_root_id"), "native_root_id")
        client, executor, scope_key = self._client()
        if self._legacy_scope_requires_read_only():
            return self._finish(
                operation_id,
                "rejected",
                {
                    **self._base_details(executor),
                    "diagnostic_code": "LEGACY_NATIVE_SCOPE_READ_ONLY",
                    "native_replay": False,
                },
                root_id=root_id,
                scope_key=self.data.get("native_scope_key"),
            )
        if scope_key != self.data.get("native_scope_key"):
            raise ControllerError("NATIVE_SCOPE_CHANGED")
        record = {
            "method": "agent.recover",
            "kind": "recover",
            "state": "native_effect_may_have_started",
            "native_root_id": root_id,
            "native_scope_key": scope_key,
            "outcome": None,
        }
        self.data["operations"][operation_id] = record
        self.checkpoint.save()
        try:
            client.thread_resume(root_id)
            thread = client.thread_read(root_id, include_turns=False).thread
        except Exception as exc:
            return self._finish(
                operation_id,
                "unknown",
                {
                    **self._base_details(executor),
                    "diagnostic_code": getattr(exc, "code", type(exc).__name__),
                    "native_replay": False,
                },
                root_id=root_id,
                scope_key=scope_key,
            )
        workspace_facts = self._observe_workspace(getattr(thread, "cwd", None))
        if workspace_facts["workspace_status"] != "workspace_exact":
            return self._finish(
                operation_id,
                "unknown",
                {
                    **self._base_details(executor),
                    **self._model_facts(),
                    "diagnostic_code": "RECOVERY_THREAD_WORKSPACE_NOT_CONFIRMED",
                    "native_replay": False,
                },
                root_id=root_id,
                scope_key=scope_key,
            )
        if (
            thread.id != root_id
            or thread.model_provider != self.data.get("requested_model_provider")
            or thread.model != self.data.get("requested_model")
        ):
            return self._finish(
                operation_id,
                "unknown",
                {
                    **self._base_details(executor),
                    "diagnostic_code": "RECOVERY_THREAD_CONFIGURATION_MISMATCH",
                    "native_replay": False,
                },
                root_id=root_id,
                scope_key=scope_key,
            )
        self.native_attached = True
        return self._finish(
            operation_id,
            "applied",
            {
                **self._base_details(executor),
                **self._model_facts(),
                "completion_condition": "native_session_resumed",
                "resume_boot_id": expected_boot,
                "native_replay": False,
            },
            root_id=root_id,
            scope_key=scope_key,
        )

    def _reconcile(self, command: dict[str, Any]) -> list[dict[str, Any]]:
        operation_id = _required_string(command.get("operation_id"), "operation_id")
        target_id = _required_string((command.get("input") or {}).get("operation_id"), "operation_id")
        record = self.data["operations"].get(target_id)
        if not isinstance(record, dict):
            raise ControllerError("RECONCILIATION_CONTEXT_UNAVAILABLE")
        target_result = record.get("outcome")
        disposition = "recorded_outcome"
        if record.get("kind") == "send" and record.get("native_root_id"):
            try:
                client, executor, _ = self._client()
                match, disposition = self._resolve_send(record, client)
            except Exception:
                match, disposition = None, "native_client_unavailable"
                executor = {}
            if match:
                details = self._base_details(executor)
                details.update(self._model_facts())
                details.update(self._input_facts(record))
                details.update(
                    completion_condition="native_input_admitted",
                    client_user_message_id=record["client_user_message_id"],
                    native_input_readback="verified",
                    native_replay=False,
                )
                target_result = {
                    "operation_id": target_id,
                    "outcome": "applied",
                    "native_root_id": record["native_root_id"],
                    "native_scope_key": record["native_scope_key"],
                    "turn_id": match["turn_id"],
                    "native_input_id": match["native_input_id"],
                    "details": details,
                }
                record["outcome"] = target_result
                record["state"] = "reported_pending"
                disposition = "native_item_readback_verified"
                self.checkpoint.save()
            else:
                disposition = disposition
        resolved = bool(target_result and target_result.get("outcome") in ("applied", "rejected"))
        reconcile = {
            "operation_id": operation_id,
            "outcome": "applied",
            "native_root_id": self.data.get("native_root_id"),
            "native_scope_key": self.data.get("native_scope_key"),
            "details": {
                "completion_condition": "native_readback_completed",
                "target_operation_id": target_id,
                "resolved": resolved,
                "disposition": disposition,
                "native_replay": False,
            },
        }
        self.outcomes[target_id] = target_result if target_result else {
            "operation_id": target_id,
            "outcome": "unknown",
            "native_root_id": record.get("native_root_id"),
            "native_scope_key": record.get("native_scope_key"),
            "details": {
                "diagnostic_code": disposition,
                "native_replay": False,
                **self._input_facts(record),
            },
        }
        self.outcomes[operation_id] = reconcile
        reconcile_record = self.data["operations"].setdefault(
            operation_id,
            {"method": "agent.reconcile", "kind": "reconcile", "state": "reported_pending"},
        )
        reconcile_record["outcome"] = reconcile
        reconcile_record["state"] = "reported_pending"
        self.checkpoint.save()
        # Ordered dict insertion guarantees the target's exact native
        # evidence is committed before the reconcile receipt is submitted.
        return [self.outcomes[target_id], reconcile]

    def handle(self, command: dict[str, Any]) -> list[dict[str, Any]]:
        method = command.get("method")
        try:
            if method == "agent.open":
                return [self._open(command)]
            if method in ("task.dispatch", "agent.send"):
                return [self._dispatch(command)]
            if method == "agent.refresh":
                return [self._refresh(command)]
            if method == "agent.reconcile":
                return self._reconcile(command)
            if method == "agent.recover":
                return [self._recover(command)]
            capability = {
                "agent.configure": "configure_model",
                "agent.goal": "goal",
                "agent.reply": "reply",
                "agent.background": "background",
                "agent.result": "result_pages",
            }.get(method)
            error = ControllerError("CAPABILITY_UNAVAILABLE")
            details = {"diagnostic_code": "CAPABILITY_UNAVAILABLE", "capability": capability or method}
            return [
                self._finish(
                    _required_string(command.get("operation_id"), "operation_id"),
                    "rejected",
                    details,
                )
            ]
        except Exception as exc:
            operation_id = command.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise
            admission_possible = bool(getattr(exc, "admission_possible", False))
            return [
                self._finish(
                    operation_id,
                    "unknown" if admission_possible else "rejected",
                    {
                        "diagnostic_code": getattr(exc, "code", type(exc).__name__),
                        "native_replay": False,
                    },
                )
            ]

    def observation(self) -> dict[str, Any]:
        native_root_id = self.data.get("native_root_id")
        native_turns = self.last_native.get("turns", [])
        active_turn_id = self.last_native.get("active_turn_id") or next(
            (
                turn.get("turnId")
                for turn in native_turns
                if turn.get("status") == "inProgress"
            ),
            None,
        )
        return {
            "module_artifact_id": MODULE_ARTIFACT_ID,
            "boot_id": self.checkpoint.owner["token"],
            "capabilities": CAPABILITIES,
            "describe": {
                **self._model_facts(),
                "executor": self.data.get("executor", {}),
                "module_artifact_id": MODULE_ARTIFACT_ID,
            },
            "native": {
                "root_id": native_root_id,
                "scope_key": self.data.get("native_scope_key"),
                "ready": self.native_attached,
                **self.last_native,
            },
            "native_root_id": native_root_id,
            "session": {
                "sessionId": native_root_id,
                "activeTurnId": active_turn_id,
            },
            "turns": native_turns,
            "observed_children": [],
        }

    def mark_reported(self, operation_id: str) -> None:
        self.outcomes.pop(operation_id, None)
        record = self.data["operations"].get(operation_id)
        if isinstance(record, dict):
            record["state"] = "reported"
            self.checkpoint.save()


def load_module_owner() -> tuple[str, Path, dict[str, Any]]:
    state_dir = os.environ.get("ELIOT_SWARM_MODULE_STATE")
    owner_file = os.environ.get("ELIOT_SWARM_MODULE_OWNER")
    if not state_dir or not owner_file:
        raise ControllerError("MODULE_RUN_OWNER_REQUIRED")
    directory = Path(state_dir)
    expected_owner = directory / "owner.json"
    if not directory.is_absolute() or Path(owner_file) != expected_owner:
        raise ControllerError("INVALID_MODULE_OWNER_PATH")
    try:
        owner = json.loads(expected_owner.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControllerError("MODULE_OWNER_RECORD_UNREADABLE") from exc
    if (
        owner.get("version") != 1
        or (owner.get("process") or {}).get("purpose") != "module"
        or not isinstance(owner.get("token"), str)
    ):
        raise ControllerError("INVALID_MODULE_OWNER_RECORD")
    return owner["token"], directory, owner
