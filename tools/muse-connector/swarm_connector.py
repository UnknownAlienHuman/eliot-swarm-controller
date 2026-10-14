#!/usr/bin/env python3
"""Muse connector for the Eliot Swarm Controller.

A dependency-free (Python stdlib only) client for the controller's
authenticated local IPC, plus a GitHub-side repository check. It mirrors the
semantics of the Rust `swarm-client` crate exactly where they matter:

- Endpoint discovery: Unix -> `<canonical data-dir>/control.sock`
  (socket path must fit in 100 bytes); Windows ->
  `\\\\.\\pipe\\eliot-swarm-<first 16 bytes of SHA-256 of the lowercased
  canonical path, hex>`.
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
import hashlib
import json
import os
import socket
import sys
import tempfile
import threading
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
# Endpoint discovery (mirrors swarm-client/src/endpoint.rs)
# --------------------------------------------------------------------------

def windows_pipe_suffix(canonical_path: str) -> str:
    digest = hashlib.sha256(canonical_path.lower().encode("utf-8")).digest()
    return digest[:16].hex()


def ipc_endpoint(data_dir: str) -> str:
    canonical = os.path.realpath(os.path.abspath(data_dir))
    if not os.path.isdir(canonical):
        raise ConnectorError(
            "HOST_UNAVAILABLE",
            f"data directory does not exist: {data_dir}",
            EXIT_HOST_UNAVAILABLE,
        )
    if os.name == "nt":
        return r"\\.\pipe\eliot-swarm-" + windows_pipe_suffix(canonical)
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

    def send_line(self, line: str) -> None:
        data = (line + "\n").encode("utf-8")
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

    def read_line(self) -> str:
        while True:
            idx = self._buf.find(b"\n")
            if idx >= 0:
                raw = bytes(self._buf[:idx])
                del self._buf[: idx + 1]
                if len(raw) > self._limit:
                    raise outcome_unknown("reply exceeded the frame bound")
                return raw.decode("utf-8", "strict")
            if len(self._buf) > self._limit:
                raise outcome_unknown("reply exceeded the frame bound")
            try:
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
            "named-pipe transport is not implemented in this connector; "
            "run on the host's Unix socket or use the `swarm` CLI on Windows",
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
        line = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            separators=(",", ":"),
        )
        # From here until a well-formed, matched reply arrives, any failure
        # leaves the outcome unknown. Never retry inside this method.
        self._usable = False
        self._io.send_line(line)
        self._io._sock.settimeout(READ_TIMEOUT_SECONDS)
        frame = self._io.read_line()
        try:
            response = json.loads(frame)
        except ValueError:
            raise outcome_unknown("reply was malformed")
        if (
            not isinstance(response, dict)
            or response.get("id") != request_id
            or response.get("jsonrpc") != "2.0"
        ):
            raise outcome_unknown("reply could not be matched to the request")
        error = response.get("error")
        if error is not None:
            data = error.get("data") if isinstance(error, dict) else None
            stable_code = data.get("code") if isinstance(data, dict) else None
            if (
                "result" in response
                or not isinstance(error, dict)
                or not isinstance(error.get("code"), int)
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

    info = get(f"/repos/{repo}")
    branch = get(f"/repos/{repo}/branches/{info['default_branch']}")
    runs = get(f"/repos/{repo}/actions/runs?per_page=1")
    pulls = get(f"/repos/{repo}/pulls?state=open&per_page=100")
    issues = get(f"/repos/{repo}/issues?state=open&per_page=100")
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
# Selftest: an in-process fake host speaking the real wire protocol
# --------------------------------------------------------------------------

def _fake_host(server: socket.socket, mode: str, seen: list) -> None:
    conn, _ = server.accept()
    with conn, server:
        buf = b""
        for _ in range(2):
            while b"\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
            line, buf = buf.split(b"\n", 1)
            request = json.loads(line)
            seen.append(request)
            if request["method"] == "client.hello":
                reply = {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}
            elif mode == "garbage":
                conn.sendall(b"this is not json\n")
                return
            elif mode == "wrong-id":
                reply = {"jsonrpc": "2.0", "id": "someone-else", "result": {}}
            elif mode == "rpc-error":
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
            conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))


def selftest() -> int:
    checks = []

    def record(name: str, ok: bool, detail: str = ""):
        checks.append((name, ok, detail))

    record(
        "windows pipe suffix is sha256 prefix",
        windows_pipe_suffix(r"C:\Swarm\Data")
        == hashlib.sha256(r"c:\swarm\data".encode()).digest()[:16].hex(),
    )

    with tempfile.TemporaryDirectory() as tmp:
        cred_path = os.path.join(tmp, "cred.json")
        with open(cred_path, "w", encoding="utf-8") as fh:
            json.dump({"client_id": "operator", "token": "secret"}, fh)
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
            json.dump({"client_id": "operator", "token": "secret"}, fh)

        for mode, name in [
            ("ok", "hello + call roundtrip"),
            ("rpc-error", "stable RPC error code surfaces"),
            ("garbage", "garbage reply is OUTCOME_UNKNOWN"),
            ("wrong-id", "mismatched id is OUTCOME_UNKNOWN"),
        ]:
            data_dir = os.path.join(tmp, f"host-{mode}")
            os.mkdir(data_dir)
            seen: list = []
            # Bind and listen in this thread so the endpoint is ready
            # before the client makes its single connection attempt.
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(os.path.join(data_dir, "control.sock"))
            server.listen(1)
            thread = threading.Thread(
                target=_fake_host,
                args=(server, mode, seen),
                daemon=True,
            )
            thread.start()
            try:
                result = do_call(
                    data_dir, cred_path, "host.status", {"probe": True}
                )
                record(name, mode == "ok" and result["method"] == "host.status")
                record(
                    "hello carried the credential",
                    seen[0]["method"] == "client.hello"
                    and seen[0]["params"]["client_id"] == "operator",
                )
            except ConnectorError as exc:
                if mode == "rpc-error":
                    record(name, exc.code == "FORBIDDEN" and exc.exit_code == EXIT_RPC_ERROR)
                elif mode in ("garbage", "wrong-id"):
                    record(name, exc.code == "OUTCOME_UNKNOWN")
                else:
                    record(name, False, str(exc))
            thread.join(timeout=5)

        try:
            do_call(tmp + "-missing", cred_path, "host.status", {})
            record("missing data dir is HOST_UNAVAILABLE", False)
        except ConnectorError as exc:
            record(
                "missing data dir is HOST_UNAVAILABLE",
                exc.exit_code == EXIT_HOST_UNAVAILABLE,
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
