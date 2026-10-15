#!/usr/bin/env python3
"""Muse connector for the Eliot Swarm Controller.

A dependency-free (Python stdlib only) client for the controller's
authenticated local IPC, plus a GitHub-side repository check. It mirrors the
semantics of the Rust `swarm-client` crate exactly where they matter:

- Endpoint discovery: Unix -> `<canonical data-dir>/control.sock`
  (socket path must fit in 100 bytes). Windows endpoint discovery and
  named-pipe transport are explicitly unsupported here; use the `swarm` CLI.
- Wire format: one JSON-RPC 2.0 object per line, frames bounded by
  `max_frame_bytes` (default 1 MiB).
- The first exchange on a connection is `client.hello` with the credential
  object `{"client_id": ..., "token": ...}` as params. `client.hello` is
  reserved and cannot be used as an application method.
- No automatic retry, ever. A failed write, a missing reply, a malformed
  reply, or a reply whose `id`/`jsonrpc` does not match the request is
  `OUTCOME_UNKNOWN`: the request may have been applied, so the caller must
  inspect the original operation before retrying. A transport failure
  poisons the connection; this CLI opens exactly one connection per
  invocation, so a poisoned client can never be reused by accident.
- Application errors are returned in the JSON-RPC error envelope; the
  stable machine-readable code lives in `error.data.code`.

Commands:
  endpoint   --data-dir DIR
  call       --data-dir DIR --credential FILE METHOD [--params JSON | --file F]
  status     --data-dir DIR --credential FILE      (host.status)
  doctor     --data-dir DIR --credential FILE      (doctor.inspect)
  repo-check [--repo OWNER/REPO]                   (GitHub REST, read-only)
  selftest                                          (offline fake-host tests)

Exit codes: 0 ok, 2 usage, 3 host unavailable, 4 handshake failed,
5 application (RPC) error, 6 outcome unknown, 7 frame too large.

The credential file is read locally and never printed, logged, or sent
anywhere except the local IPC handshake it belongs to.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import tempfile
import threading
import time
import uuid

MAX_FRAME_BYTES = 1_048_576
WRITE_TIMEOUT_SECONDS = 15
READ_TIMEOUT_SECONDS = 60
DEFAULT_REPO = "UnknownAlienHuman/eliot-swarm-controller"

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_HOST_UNAVAILABLE = 3
EXIT_HANDSHAKE_FAILED = 4
EXIT_RPC_ERROR = 5
EXIT_OUTCOME_UNKNOWN = 6
EXIT_FRAME_TOO_LARGE = 7


class ConnectorError(RuntimeError):
    """Error with a stable code and the process exit code to report."""

    def __init__(self, code: str, message: str, exit_code: int):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.exit_code = exit_code


def outcome_unknown(message: str) -> ConnectorError:
    return ConnectorError("OUTCOME_UNKNOWN", message, EXIT_OUTCOME_UNKNOWN)


# --------------------------------------------------------------------------
# Unix endpoint discovery (mirrors swarm-client/src/endpoint.rs)
# --------------------------------------------------------------------------

def ipc_endpoint(data_dir: str) -> str:
    if os.name == "nt":
        raise ConnectorError(
            "HOST_UNAVAILABLE",
            "Windows endpoint discovery and named-pipe transport are not "
            "supported by this connector; use the `swarm` CLI on Windows",
            EXIT_HOST_UNAVAILABLE,
        )
    canonical = os.path.realpath(os.path.abspath(data_dir))
    if not os.path.isdir(canonical):
        raise ConnectorError(
            "HOST_UNAVAILABLE",
            f"data directory does not exist: {data_dir}",
            EXIT_HOST_UNAVAILABLE,
        )
    path = os.path.join(canonical, "control.sock")
    if len(os.fsencode(path)) > 100:
        raise ConnectorError(
            "INVALID_PARAMS",
            "data-dir too long for a Unix socket",
            EXIT_USAGE,
        )
    return path


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------

class _LineSocket:
    """Minimal buffered line reader over a socket with a frame bound."""

    def __init__(self, sock: socket.socket, limit: int):
        self._sock = sock
        self._limit = limit
        self._buf = bytearray()

    def send_line(self, data: bytes) -> None:
        if len(data) - 1 > self._limit:
            raise ConnectorError(
                "FRAME_TOO_LARGE",
                "use smaller pages or artifact ranges",
                EXIT_FRAME_TOO_LARGE,
            )
        try:
            self._sock.sendall(data)
        except OSError:
            raise outcome_unknown(
                "request write did not complete; delivery is uncertain; "
                "preserve the logical request ID and inspect the original "
                "operation before retrying"
            )

    def read_line(self, timeout_seconds: float) -> str:
        deadline = time.monotonic() + timeout_seconds
        while True:
            idx = self._buf.find(b"\n")
            if idx >= 0:
                raw = bytes(self._buf[:idx])
                del self._buf[: idx + 1]
                if len(raw) > self._limit:
                    raise outcome_unknown("reply exceeded the frame bound")
                try:
                    return raw.decode("utf-8", "strict")
                except UnicodeDecodeError as exc:
                    raise outcome_unknown("reply was not valid UTF-8") from exc
            if len(self._buf) > self._limit:
                raise outcome_unknown("reply exceeded the frame bound")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise outcome_unknown(
                    "reply deadline expired; inspect the operation before retrying"
                )
            try:
                # Socket timeouts apply to each recv. Reduce the timeout after
                # every partial chunk so the complete reply has one deadline.
                self._sock.settimeout(remaining)
                chunk = self._sock.recv(65536)
            except socket.timeout:
                raise outcome_unknown(
                    "reply missing; inspect the operation before retrying"
                )
            except OSError:
                raise outcome_unknown("reply could not be read")
            if not chunk:
                raise outcome_unknown("server closed before reply")
            self._buf.extend(chunk)


def _open_stream(endpoint: str) -> socket.socket:
    if os.name == "nt":
        raise ConnectorError(
            "HOST_UNAVAILABLE",
            "Windows endpoint discovery and named-pipe transport are not "
            "supported by this connector; use the `swarm` CLI on Windows",
            EXIT_HOST_UNAVAILABLE,
        )
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(WRITE_TIMEOUT_SECONDS)
    try:
        sock.connect(endpoint)
    except OSError:
        sock.close()
        raise ConnectorError(
            "HOST_UNAVAILABLE",
            "controller IPC is unavailable; no application request was sent; "
            "start or reconnect the host",
            EXIT_HOST_UNAVAILABLE,
        )
    return sock


def _encode_json_frame(value: object, limit: int) -> bytes:
    try:
        line = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        data = line.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ConnectorError(
            "INVALID_PARAMS",
            "request contains values that cannot be represented as JSON",
            EXIT_USAGE,
        ) from exc
    if len(data) > limit:
        raise ConnectorError(
            "FRAME_TOO_LARGE",
            "use smaller pages or artifact ranges",
            EXIT_FRAME_TOO_LARGE,
        )
    return data + b"\n"


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"unsupported JSON constant: {value}")


# --------------------------------------------------------------------------
# Client (mirrors swarm-client/src/client.rs)
# --------------------------------------------------------------------------

class SwarmClient:
    def __init__(self, data_dir: str, credential: dict, max_frame_bytes: int = MAX_FRAME_BYTES):
        self._limit = max_frame_bytes
        endpoint = ipc_endpoint(data_dir)
        self._io = _LineSocket(_open_stream(endpoint), self._limit)
        self._usable = True
        try:
            self._exchange("client.hello", credential)
        except ConnectorError as exc:
            if exc.code in ("OUTCOME_UNKNOWN", "PROTOCOL_ERROR", "INVALID_PARAMS"):
                raise ConnectorError(
                    "HOST_HANDSHAKE_FAILED",
                    "controller authentication handshake did not complete; "
                    "no application request was sent; reconnect before dispatch",
                    EXIT_HANDSHAKE_FAILED,
                ) from exc
            raise

    def request(self, method: str, params: dict) -> object:
        if method == "client.hello":
            raise ConnectorError(
                "INVALID_PARAMS",
                "client.hello is reserved for the transport",
                EXIT_USAGE,
            )
        if not isinstance(params, dict):
            raise ConnectorError(
                "INVALID_PARAMS", "params must be a JSON object", EXIT_USAGE
            )
        return self._exchange(method, params)

    def _exchange(self, method: str, params: object) -> object:
        if not self._usable:
            raise ConnectorError(
                "DISCONNECTED",
                "client link failed; reconnect before another request",
                EXIT_HOST_UNAVAILABLE,
            )
        request_id = str(uuid.uuid4())
        data = _encode_json_frame(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            self._limit,
        )
        # From here until a well-formed, matched reply arrives, any failure
        # leaves the outcome unknown. Never retry inside this method.
        try:
            self._io._sock.settimeout(WRITE_TIMEOUT_SECONDS)
        except OSError as exc:
            raise ConnectorError(
                "HOST_UNAVAILABLE",
                "local IPC write could not be configured; no request was sent",
                EXIT_HOST_UNAVAILABLE,
            ) from exc
        self._usable = False
        self._io.send_line(data)
        frame = self._io.read_line(READ_TIMEOUT_SECONDS)
        try:
            response = json.loads(frame, parse_constant=_reject_json_constant)
        except ValueError:
            raise outcome_unknown("reply was malformed")
        if (
            not isinstance(response, dict)
            or response.get("id") != request_id
            or response.get("jsonrpc") != "2.0"
        ):
            raise outcome_unknown("reply could not be matched to the request")
        if "error" in response:
            error = response["error"]
            data = error.get("data") if isinstance(error, dict) else None
            stable_code = data.get("code") if isinstance(data, dict) else None
            error_code = error.get("code") if isinstance(error, dict) else None
            if (
                "result" in response
                or not isinstance(error, dict)
                or type(error_code) is not int
                or not -(1 << 63) <= error_code < (1 << 63)
                or not isinstance(error.get("message"), str)
                or not isinstance(stable_code, str)
                or not stable_code
            ):
                raise outcome_unknown("error reply was malformed")
            self._usable = True
            raise ConnectorError(stable_code, error["message"], EXIT_RPC_ERROR)
        if "result" not in response:
            raise outcome_unknown("reply carried neither result nor error")
        self._usable = True
        return response["result"]


def load_credential(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            credential = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ConnectorError(
            "INVALID_PARAMS", f"cannot read credential file: {exc}", EXIT_USAGE
        )
    if (
        not isinstance(credential, dict)
        or set(credential) != {"client_id", "token"}
        or not all(isinstance(v, str) and v for v in credential.values())
    ):
        raise ConnectorError(
            "INVALID_PARAMS",
            "credential file must be a JSON object with exactly "
            "client_id and token (non-empty strings)",
            EXIT_USAGE,
        )
    return credential


def do_call(data_dir: str, credential_path: str, method: str, params: dict) -> object:
    client = SwarmClient(data_dir, load_credential(credential_path))
    return client.request(method, params)


# --------------------------------------------------------------------------
# GitHub repository check (read-only; token only from the environment)
# --------------------------------------------------------------------------

def repo_check(repo: str) -> dict:
    import urllib.error
    import urllib.request

    def get(path: str):
        req = urllib.request.Request(
            "https://api.github.com" + path,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "muse-swarm-connector",
            },
        )
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise ConnectorError(
                "HOST_UNAVAILABLE",
                f"GitHub API HTTP {exc.code} for {path}",
                EXIT_HOST_UNAVAILABLE,
            )

    def get_all(path: str) -> list:
        items = []
        page = 1
        while True:
            separator = "&" if "?" in path else "?"
            batch = get(f"{path}{separator}per_page=100&page={page}")
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1

    info = get(f"/repos/{repo}")
    branch = get(f"/repos/{repo}/branches/{info['default_branch']}")
    runs = get(f"/repos/{repo}/actions/runs?per_page=1")
    pulls = get_all(f"/repos/{repo}/pulls?state=open")
    issues = get_all(f"/repos/{repo}/issues?state=open")
    latest = runs["workflow_runs"][0] if runs.get("workflow_runs") else None
    return {
        "repo": info["full_name"],
        "default_branch": info["default_branch"],
        "main_sha": branch["commit"]["sha"],
        "pushed_at": info["pushed_at"],
        "latest_actions_run": None
        if latest is None
        else {
            "id": latest["id"],
            "name": latest["name"],
            "head_sha": latest["head_sha"],
            "status": latest["status"],
            "conclusion": latest["conclusion"],
        },
        "open_pull_requests": len(pulls),
        "open_issues": len([i for i in issues if "pull_request" not in i]),
    }


# --------------------------------------------------------------------------
# Selftest: a portable socketpair fake host speaking the real wire protocol
# --------------------------------------------------------------------------

def _fake_host(
    conn: socket.socket, mode: str, seen: list, wire: list[bytes] | None = None
) -> None:
    with conn:
        buf = b""
        for index in range(3 if mode == "rpc-error-then-ok" else 2):
            while b"\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
            line, buf = buf.split(b"\n", 1)
            if wire is not None:
                wire.append(line)
            request = json.loads(line)
            seen.append(request)
            if request["method"] == "client.hello":
                reply = {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}
            elif mode == "garbage":
                conn.sendall(b"this is not json\n")
                return
            elif mode == "invalid-utf8":
                conn.sendall(b"\xff\n")
                return
            elif mode == "nan":
                conn.sendall(
                    b'{"jsonrpc":"2.0","id":'
                    + json.dumps(request["id"]).encode("ascii")
                    + b',"result":NaN}\n'
                )
                return
            elif mode == "timeout":
                return
            elif mode == "slow-fragments":
                for _ in range(8):
                    time.sleep(0.04)
                    conn.sendall(b" ")
                reply = {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"method": request["method"], "params": request["params"]},
                }
            elif mode == "wrong-id":
                reply = {"jsonrpc": "2.0", "id": "someone-else", "result": {}}
            elif mode in ("bool-error-code", "overflow-error-code", "null-error-result"):
                if mode == "null-error-result":
                    reply = {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": None,
                        "result": {},
                    }
                else:
                    code = True if mode == "bool-error-code" else 1 << 63
                    reply = {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {
                            "code": code,
                            "message": "malformed peer error",
                            "data": {"code": "FORBIDDEN"},
                        },
                    }
            elif mode == "rpc-error" or (
                mode == "rpc-error-then-ok" and index == 1
            ):
                reply = {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": {
                        "code": -32000,
                        "message": "forbidden",
                        "data": {"code": "FORBIDDEN"},
                    },
                }
            else:
                reply = {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"method": request["method"], "params": request["params"]},
                }
            conn.sendall(
                (json.dumps(reply, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
                    "utf-8"
                )
            )


class _ObservedSocket:
    """Socketpair wrapper for deterministic timeout and framing checks."""

    def __init__(
        self,
        sock: socket.socket,
        *,
        fail_send_number: int | None = None,
        timeout_after_send_number: int | None = None,
    ):
        self._sock = sock
        self.fail_send_number = fail_send_number
        self.timeout_after_send_number = timeout_after_send_number
        self.send_count = 0
        self.timeout_values = []
        self.events = []
        self.current_timeout = None

    def settimeout(self, value):
        self.current_timeout = value
        self.timeout_values.append(value)
        self.events.append(("timeout", value))
        self._sock.settimeout(value)

    def sendall(self, data: bytes):
        self.send_count += 1
        self.events.append(("send", self.current_timeout))
        if self.send_count == self.fail_send_number:
            raise socket.timeout("offline simulated write timeout")
        self._sock.sendall(data)

    def recv(self, size: int) -> bytes:
        self.events.append(("recv", self.current_timeout))
        if (
            self.timeout_after_send_number is not None
            and self.send_count >= self.timeout_after_send_number
        ):
            raise socket.timeout("offline simulated read timeout")
        return self._sock.recv(size)

    def close(self):
        self._sock.close()


def _selftest_client(sock, credential: dict, limit: int = MAX_FRAME_BYTES):
    """Use the real client state machine over a socketpair, not OS IPC."""
    client = SwarmClient.__new__(SwarmClient)
    client._limit = limit
    client._io = _LineSocket(sock, limit)
    client._usable = True
    client._exchange("client.hello", credential)
    return client


def selftest() -> int:
    from unittest.mock import patch

    checks = []

    def record(name: str, ok: bool, detail: str = ""):
        checks.append((name, ok, detail))

    with tempfile.TemporaryDirectory() as tmp:
        if os.name == "nt":
            try:
                ipc_endpoint(tmp)
                record("Windows endpoint discovery is explicitly unsupported", False)
            except ConnectorError as exc:
                record(
                    "Windows endpoint discovery is explicitly unsupported",
                    exc.code == "HOST_UNAVAILABLE",
                )
        else:
            record(
                "Unix endpoint uses canonical control.sock path",
                ipc_endpoint(tmp)
                == os.path.join(os.path.realpath(tmp), "control.sock"),
            )

        cred_path = os.path.join(tmp, "cred.json")
        credential = {"client_id": "operator", "token": "secret"}
        with open(cred_path, "w", encoding="utf-8") as fh:
            json.dump(credential, fh)
        record(
            "credential shape enforced",
            load_credential(cred_path)["client_id"] == "operator",
        )
        try:
            with open(cred_path, "w", encoding="utf-8") as fh:
                json.dump({"client_id": "operator", "token": "x", "extra": 1}, fh)
            load_credential(cred_path)
            record("credential extra field rejected", False)
        except ConnectorError:
            record("credential extra field rejected", True)
        with open(cred_path, "w", encoding="utf-8") as fh:
            json.dump(credential, fh)

        for mode, name in [
            ("ok", "hello + call roundtrip"),
            ("rpc-error", "stable RPC error code surfaces"),
            ("garbage", "garbage reply is OUTCOME_UNKNOWN"),
            ("invalid-utf8", "invalid UTF-8 reply is OUTCOME_UNKNOWN"),
            ("wrong-id", "mismatched id is OUTCOME_UNKNOWN"),
            ("bool-error-code", "boolean error code is malformed"),
            ("overflow-error-code", "out-of-range error code is malformed"),
            ("null-error-result", "present null error is malformed"),
            ("nan", "non-standard NaN reply is malformed"),
        ]:
            client_sock, host_sock = socket.socketpair()
            seen: list = []
            wire: list[bytes] = []
            thread = threading.Thread(
                target=_fake_host,
                args=(host_sock, mode, seen, wire),
                daemon=True,
            )
            thread.start()
            observed = _ObservedSocket(client_sock) if mode == "ok" else client_sock
            client = _selftest_client(observed, credential)
            try:
                result = client.request("host.status", {"probe": True})
                record(name, mode == "ok" and result["method"] == "host.status")
            except ConnectorError as exc:
                if mode == "rpc-error":
                    record(name, exc.code == "FORBIDDEN" and exc.exit_code == EXIT_RPC_ERROR)
                elif mode not in ("ok", "rpc-error"):
                    record(name, exc.code == "OUTCOME_UNKNOWN")
                else:
                    record(name, False, str(exc))
            record(
                "hello carried the local credential",
                seen[0]["method"] == "client.hello"
                and seen[0]["params"] == credential,
            )
            if mode == "ok":
                record(
                    "write/read timeouts reset around each frame",
                    len(observed.timeout_values) == 4
                    and observed.timeout_values[0] == WRITE_TIMEOUT_SECONDS
                    and 0 < observed.timeout_values[1] <= READ_TIMEOUT_SECONDS
                    and observed.timeout_values[2] == WRITE_TIMEOUT_SECONDS
                    and 0 < observed.timeout_values[3] <= READ_TIMEOUT_SECONDS
                    and [value for event, value in observed.events if event == "send"]
                    == [WRITE_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS]
                    and sum(event == "recv" for event, _ in observed.events) >= 2
                    and all(
                        0 < value <= READ_TIMEOUT_SECONDS
                        for event, value in observed.events
                        if event == "recv"
                    ),
                )
            if mode not in ("ok", "rpc-error"):
                try:
                    client.request("host.status", {"retry": True})
                    poisoned = False
                except ConnectorError as exc:
                    poisoned = exc.code == "DISCONNECTED"
                record(f"{mode} leaves no reusable connection", poisoned)
            client_sock.close()
            thread.join(timeout=5)
            record(f"{mode} fake host completed", not thread.is_alive())

        client_sock, host_sock = socket.socketpair()
        seen = []
        wire = []
        thread = threading.Thread(
            target=_fake_host,
            args=(host_sock, "rpc-error-then-ok", seen, wire),
            daemon=True,
        )
        thread.start()
        client = _selftest_client(client_sock, credential)
        try:
            client.request("task.create", {})
            rpc_error_surfaces = False
        except ConnectorError as exc:
            rpc_error_surfaces = exc.code == "FORBIDDEN" and client._usable
        following = client.request("host.status", {})
        client_sock.close()
        thread.join(timeout=5)
        record(
            "valid RPC error leaves the matched connection usable",
            rpc_error_surfaces
            and following["method"] == "host.status"
            and not thread.is_alive(),
        )

        # A socketpair exercises the client framing/state machine portably;
        # it does not qualify Unix listener setup or Windows Named Pipes.
        client_sock, host_sock = socket.socketpair()
        seen = []
        wire: list[bytes] = []
        thread = threading.Thread(
            target=_fake_host, args=(host_sock, "ok", seen, wire), daemon=True
        )
        thread.start()
        client = _selftest_client(client_sock, credential)
        cjk = "界" * 200_000
        result = client.request("host.status", {"text": cjk})
        client_sock.close()
        thread.join(timeout=10)
        record(
            "200,000 CJK characters fit as compact UTF-8",
            result["params"]["text"] == cjk
            and len(wire[1]) <= MAX_FRAME_BYTES
            and b"\\u754c" not in wire[1]
            and not thread.is_alive(),
        )

        client_sock, host_sock = socket.socketpair()
        seen = []
        wire = []
        thread = threading.Thread(
            target=_fake_host, args=(host_sock, "ok", seen, wire), daemon=True
        )
        thread.start()
        client = _selftest_client(client_sock, credential, limit=256)
        try:
            client.request("host.status", {"value": float("nan")})
            nan_rejected = False
        except ConnectorError as exc:
            nan_rejected = exc.code == "INVALID_PARAMS" and client._usable
        try:
            client.request("host.status", {"value": "x" * 512})
            oversized_rejected = False
        except ConnectorError as exc:
            oversized_rejected = exc.code == "FRAME_TOO_LARGE" and client._usable
        result = client.request("host.status", {})
        client_sock.close()
        thread.join(timeout=5)
        record(
            "invalid JSON and oversized requests are rejected before write",
            nan_rejected
            and oversized_rejected
            and result["method"] == "host.status"
            and [request["method"] for request in seen]
            == ["client.hello", "host.status"]
            and not thread.is_alive(),
        )

        client_sock, host_sock = socket.socketpair()
        seen = []
        wire = []
        thread = threading.Thread(
            target=_fake_host, args=(host_sock, "timeout", seen, wire), daemon=True
        )
        thread.start()
        observed = _ObservedSocket(client_sock, timeout_after_send_number=2)
        client = _selftest_client(observed, credential)
        try:
            client.request("host.status", {})
            read_timeout_unknown = False
        except ConnectorError as exc:
            read_timeout_unknown = exc.code == "OUTCOME_UNKNOWN"
        try:
            client.request("host.status", {})
            read_retry_refused = False
        except ConnectorError as exc:
            read_retry_refused = exc.code == "DISCONNECTED"
        observed.close()
        thread.join(timeout=5)
        record(
            "read timeout poisons and refuses an implicit retry",
            read_timeout_unknown
            and read_retry_refused
            and len(observed.timeout_values) == 4
            and observed.timeout_values[0] == WRITE_TIMEOUT_SECONDS
            and 0 < observed.timeout_values[1] <= READ_TIMEOUT_SECONDS
            and observed.timeout_values[2] == WRITE_TIMEOUT_SECONDS
            and 0 < observed.timeout_values[3] <= READ_TIMEOUT_SECONDS
            and not thread.is_alive(),
        )

        client_sock, host_sock = socket.socketpair()
        seen = []
        wire = []
        thread = threading.Thread(
            target=_fake_host, args=(host_sock, "slow-fragments", seen, wire), daemon=True
        )
        thread.start()
        observed = _ObservedSocket(client_sock)
        read_timeout_seconds = 0.12
        with patch(f"{__name__}.READ_TIMEOUT_SECONDS", read_timeout_seconds):
            client = _selftest_client(observed, credential)
            started = time.monotonic()
            try:
                client.request("host.status", {})
                slow_frame_timed_out = False
            except ConnectorError as exc:
                slow_frame_timed_out = exc.code == "OUTCOME_UNKNOWN"
            elapsed = time.monotonic() - started
            try:
                client.request("host.status", {})
                slow_retry_refused = False
            except ConnectorError as exc:
                slow_retry_refused = exc.code == "DISCONNECTED"
        status_read_timeouts = observed.timeout_values[3:]
        shortened_timeout_used = any(
            current < previous
            for previous, current in zip(status_read_timeouts, status_read_timeouts[1:])
        )
        thread.join(timeout=5)
        observed.close()
        record(
            "slow partial reply fragments share one absolute deadline",
            slow_frame_timed_out
            and elapsed < 0.25
            and slow_retry_refused
            and shortened_timeout_used
            and [request["method"] for request in seen]
            == ["client.hello", "host.status"]
            and not thread.is_alive(),
        )

        client_sock, host_sock = socket.socketpair()
        seen = []
        wire = []
        thread = threading.Thread(
            target=_fake_host, args=(host_sock, "ok", seen, wire), daemon=True
        )
        thread.start()
        observed = _ObservedSocket(client_sock, fail_send_number=2)
        client = _selftest_client(observed, credential)
        try:
            client.request("host.status", {})
            write_timeout_unknown = False
        except ConnectorError as exc:
            write_timeout_unknown = exc.code == "OUTCOME_UNKNOWN"
        try:
            client.request("host.status", {})
            write_retry_refused = False
        except ConnectorError as exc:
            write_retry_refused = exc.code == "DISCONNECTED"
        observed.close()
        thread.join(timeout=5)
        record(
            "write timeout poisons and refuses an implicit retry",
            write_timeout_unknown
            and write_retry_refused
            and len(observed.timeout_values) == 3
            and observed.timeout_values[0] == WRITE_TIMEOUT_SECONDS
            and 0 < observed.timeout_values[1] <= READ_TIMEOUT_SECONDS
            and observed.timeout_values[2] == WRITE_TIMEOUT_SECONDS
            and len(seen) == 1
            and not thread.is_alive(),
        )

        # The repository count check stays offline while exercising all pages.
        import urllib.parse

        class _Response:
            def __init__(self, payload):
                self.payload = json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return self.payload

        api_pages = []

        def fake_urlopen(request, timeout):
            parsed = urllib.parse.urlsplit(request.full_url)
            query = urllib.parse.parse_qs(parsed.query)
            page = int(query.get("page", ["1"])[0])
            api_pages.append((parsed.path, page))
            if parsed.path == "/repos/example/project":
                payload = {
                    "full_name": "example/project",
                    "default_branch": "main",
                    "pushed_at": "2026-10-14T00:00:00Z",
                }
            elif parsed.path.endswith("/branches/main"):
                payload = {"commit": {"sha": "fixture-sha"}}
            elif parsed.path.endswith("/actions/runs"):
                payload = {"workflow_runs": []}
            elif parsed.path.endswith("/pulls"):
                payload = (
                    [{"number": i} for i in range(100)]
                    if page == 1
                    else [{"number": 100}, {"number": 101}]
                    if page == 2
                    else []
                )
            elif parsed.path.endswith("/issues"):
                payload = (
                    [
                        {"pull_request": {}} if i == 0 else {"number": i}
                        for i in range(100)
                    ]
                    if page == 1
                    else [{"number": 100}, {"number": 101}, {"pull_request": {}}]
                    if page == 2
                    else []
                )
            else:
                raise AssertionError(f"unexpected GitHub URL: {request.full_url}")
            return _Response(payload)

        with patch("urllib.request.urlopen", fake_urlopen):
            repo_state = repo_check("example/project")
        record(
            "repo-check counts every pull and issue page",
            repo_state["open_pull_requests"] == 102
            and repo_state["open_issues"] == 101
            and ("/repos/example/project/pulls", 2) in api_pages
            and ("/repos/example/project/issues", 2) in api_pages,
        )

    failed = [c for c in checks if not c[1]]
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'} {name}" + (f" ({detail})" if detail else ""))
    print(f"selftest: {len(checks) - len(failed)}/{len(checks)} passed")
    return EXIT_OK if not failed else 1


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="swarm_connector.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("endpoint")
    p.add_argument("--data-dir", required=True)

    for name in ("call", "status", "doctor"):
        p = sub.add_parser(name)
        p.add_argument("--data-dir", required=True)
        p.add_argument("--credential", required=True)
        if name == "call":
            p.add_argument("method")
            p.add_argument("--params", default=None)
            p.add_argument("--file", default=None)

    p = sub.add_parser("repo-check")
    p.add_argument("--repo", default=DEFAULT_REPO)

    sub.add_parser("selftest")

    args = parser.parse_args(argv)
    try:
        if args.command == "endpoint":
            print(ipc_endpoint(args.data_dir))
        elif args.command == "repo-check":
            print(json.dumps(repo_check(args.repo), indent=2))
        elif args.command == "selftest":
            return selftest()
        else:
            if args.command == "status":
                method, params = "host.status", {}
            elif args.command == "doctor":
                method, params = "doctor.inspect", {}
            else:
                method = args.method
                if args.file:
                    with open(args.file, encoding="utf-8") as fh:
                        params = json.load(fh)
                else:
                    params = json.loads(args.params or "{}")
            result = do_call(args.data_dir, args.credential, method, params)
            print(json.dumps(result, indent=2))
    except ConnectorError as exc:
        print(json.dumps({"code": exc.code, "message": exc.message}), file=sys.stderr)
        return exc.exit_code
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
