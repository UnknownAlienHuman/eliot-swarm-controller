#!/usr/bin/env python3
"""Host-only load measurement for the Eliot Swarm Controller.

Drives one real `swarm host` process over its local IPC (newline-delimited
JSON-RPC 2.0 on the Unix socket / Windows named pipe endpoint) with
controller clients only: no native runtimes, no bridges, no model calls.

What is measured (see docs/host-load-qualification.md for the methodology
and the recorded run):

  * admission latency   - sequential host.status and task.create round trips;
  * event throughput    - sustained durable message.send admissions per
                          second (one admitted mutation = one observation in
                          the host event stream), and the report.delta drain
                          rate that reads that stream back;
  * status latency      - host.status round trips sampled while the event
                          load is running;
  * connected clients   - the configured client population stays connected
                          for the whole run and drains its own mailboxes
                          with message.read while senders produce events;
  * host RSS            - Linux VmRSS or Windows process working set
                          (reported as null on unsupported platforms).

Every admitted message.send is verified afterwards: the final report.delta
drain must contain exactly as many `message.send` observations as the
senders counted successful admissions. A mismatch is reported, not hidden.

Python 3 standard library only. The data directory is temporary and is
removed afterwards unless --keep-data-dir is given.
"""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


# --------------------------------------------------------------------------
# Small JSON-RPC client for the controller IPC.
#
# The server answers requests on one connection out of order (it spawns a
# task per request, bounded by ipc.max_inflight_per_connection), so replies
# are matched to callers by JSON-RPC id.
# --------------------------------------------------------------------------


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code


class Client:
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._next_id = 0
        self._pending = {}
        self._reader_task = None

    @classmethod
    async def connect(cls, sock_path, credential):
        if os.name == "nt":
            loop = asyncio.get_running_loop()
            reader = asyncio.StreamReader(limit=1048576)
            protocol = asyncio.StreamReaderProtocol(reader)
            transport, _ = await loop.create_pipe_connection(lambda: protocol, sock_path)
            writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        else:
            reader, writer = await asyncio.open_unix_connection(sock_path, limit=1048576)
        client = cls(reader, writer)
        client._reader_task = asyncio.ensure_future(client._read_loop())
        await client.call("client.hello", credential)
        return client

    async def _read_loop(self):
        try:
            while True:
                line = await self._reader.readline()
                if not line:
                    break
                msg = json.loads(line)
                fut = self._pending.pop(msg.get("id"), None)
                if fut is None or fut.done():
                    continue
                if "error" in msg:
                    err = msg["error"]
                    data = err.get("data") or {}
                    fut.set_exception(
                        RpcError(data.get("code", "RPC_ERROR"), err.get("message", ""))
                    )
                else:
                    fut.set_result(msg.get("result"))
        except Exception:
            pass
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(RpcError("DISCONNECTED", "connection closed"))
        self._pending.clear()

    async def call(self, method, params, timeout=30.0):
        self._next_id += 1
        # The server requires a nonempty *string* JSON-RPC id
        # (model::Request::validate); a numeric id closes the connection.
        request_id = str(self._next_id)
        fut = asyncio.get_running_loop().create_future()
        self._pending[request_id] = fut
        line = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        self._writer.write((line + "\n").encode())
        await self._writer.drain()
        return await asyncio.wait_for(fut, timeout)

    async def close(self):
        if self._reader_task is not None:
            self._reader_task.cancel()
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except Exception:
            pass


def percentile(values, pct):
    """Nearest-rank percentile; None for an empty sample."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100.0 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def latency_block(values_ms):
    return {
        "count": len(values_ms),
        "min_ms": round(min(values_ms), 3) if values_ms else None,
        "p50_ms": round(percentile(values_ms, 50), 3) if values_ms else None,
        "p95_ms": round(percentile(values_ms, 95), 3) if values_ms else None,
        "p99_ms": round(percentile(values_ms, 99), 3) if values_ms else None,
        "max_ms": round(max(values_ms), 3) if values_ms else None,
        "mean_ms": round(sum(values_ms) / len(values_ms), 3) if values_ms else None,
    }


def windows_process_metrics(pid):
    """Read the exact owned process using native Windows APIs; never signal it."""
    import ctypes
    from ctypes import wintypes

    class MemoryCounters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage",
            )
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
        ctypes.POINTER(wintypes.FILETIME)
    ] * 4
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(MemoryCounters), wintypes.DWORD,
    ]
    handle = kernel.OpenProcess(0x0410, False, pid)
    if not handle:
        return None, None
    try:
        counters = MemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        memory = counters.WorkingSetSize if psapi.GetProcessMemoryInfo(
            handle, ctypes.byref(counters), counters.cb
        ) else None
        times = [wintypes.FILETIME() for _ in range(4)]
        cpu = None
        if kernel.GetProcessTimes(handle, *(ctypes.byref(value) for value in times)):
            cpu = sum((value.dwHighDateTime << 32) | value.dwLowDateTime
                      for value in times[2:]) / 10_000_000
        return memory, cpu
    finally:
        kernel.CloseHandle(handle)


def read_rss_bytes(pid):
    if os.name == "nt":
        return windows_process_metrics(pid)[0]
    try:
        with open(f"/proc/{pid}/status", "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def read_cpu_seconds(pid):
    """User+system CPU seconds of a process from /proc/<pid>/stat (Linux)."""
    if os.name == "nt":
        return windows_process_metrics(pid)[1]
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="ascii") as fh:
            fields = fh.read().rsplit(")", 1)[1].split()
        # After the comm field, index 0 is state; utime is field 14 and
        # stime field 15 overall, i.e. indexes 11 and 12 here.
        ticks = int(fields[11]) + int(fields[12])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None


TASK_SPEC = {
    "objective": "Host-load fixture task; no native execution is requested.",
    "phase": "implementation",
    "requirements": [
        {"id": "L1", "statement": "Exist only as controller admission load."}
    ],
    "dependencies": [],
    "scope": {
        "initial_paths": ["src/"],
        "forbidden_paths": [],
        "prerequisite_policy": "owner_module_prerequisites",
    },
    "source_refs": ["docs/agent_swarm.implementation-v6.md"],
}


async def timed_call(client, method, params):
    started = time.perf_counter()
    result = await client.call(method, params)
    return result, (time.perf_counter() - started) * 1000.0


async def drain(client, after, want_kind=None):
    """Read report.delta to the end of the stream; return (count, kind_count, cursor)."""
    total = 0
    kind_count = 0
    cursor = after
    while True:
        result = await client.call("report.delta", {"after": cursor, "limit": 200})
        items = result["items"]
        if not items:
            break
        total += len(items)
        if want_kind is not None:
            kind_count += sum(1 for item in items if item["kind"] == want_kind)
        cursor = result["next_cursor"]
    return total, kind_count, cursor


async def run(args):
    data_dir = args.data_dir or tempfile.mkdtemp(prefix="swarm-host-load-")
    sock_path = None if os.name == "nt" else os.path.join(data_dir, "control.sock")
    # The host refuses a data directory containing unrelated files, so its
    # stderr log lives next to the data directory, never inside it.
    host_log_path = data_dir.rstrip(os.sep) + ".host-stderr.log"
    host_log = open(host_log_path, "wb")
    host = subprocess.Popen(
        [args.swarm, "--data-dir", data_dir, "host"],
        stdout=subprocess.DEVNULL,
        stderr=host_log,
    )
    result = {"scope": "host-only fixture load; no native runtimes, bridges or model calls"}
    driver_cpu_started = time.process_time()
    try:
        # Wait for the host endpoint and the bootstrap operator credential.
        deadline = time.time() + 60
        operator_cred = None
        while time.time() < deadline:
            if host.poll() is not None:
                raise RuntimeError(f"host exited early with {host.returncode}")
            cred_path = os.path.join(data_dir, "operator.json")
            if os.name == "nt":
                # Use the host's actual endpoint, including its canonical-path
                # namespace, rather than reimplementing its hashing rules.
                with open(host_log_path, "r", encoding="utf-8") as log:
                    for line in log:
                        if line.startswith("swarm host ready: "):
                            sock_path = line[len("swarm host ready: "):].strip()
            endpoint_ready = bool(sock_path) if os.name == "nt" else os.path.exists(sock_path)
            if endpoint_ready and os.path.exists(cred_path):
                with open(cred_path, "r", encoding="utf-8") as fh:
                    operator_cred = json.load(fh)
                break
            await asyncio.sleep(0.1)
        if operator_cred is None:
            raise RuntimeError("host did not become ready in 60 s")

        rss_samples = {"idle_bytes": None, "peak_bytes": None, "final_bytes": None}

        async def sample_rss():
            while True:
                value = read_rss_bytes(host.pid)
                if value is not None:
                    if rss_samples["peak_bytes"] is None or value > rss_samples["peak_bytes"]:
                        rss_samples["peak_bytes"] = value
                await asyncio.sleep(0.25)

        rss_task = asyncio.ensure_future(sample_rss())

        operator = await Client.connect(sock_path, operator_cred)
        status0 = await operator.call("host.status", {})
        result["controller_version"] = status0.get("version")
        result["sqlite_version"] = status0.get("sqlite")

        # --- Register the client population (role: manager; these are
        # controller clients, not native agents). -------------------------
        clients = []
        started = time.perf_counter()
        for i in range(args.clients):
            client_id = f"load-{i:03d}"
            token = f"host-load-{i:03d}-{os.urandom(8).hex()}"
            await operator.call(
                "client.register",
                {
                    "client_request_id": f"hl-register-{i}",
                    "client_id": client_id,
                    "role": "manager",
                    "token_hash": hashlib.sha256(token.encode()).hexdigest(),
                },
            )
            clients.append((client_id, {"client_id": client_id, "token": token}))
        result["registration"] = {
            "clients": args.clients,
            "seconds": round(time.perf_counter() - started, 3),
        }

        # --- Connect the whole population and keep it connected. ---------
        started = time.perf_counter()
        connections = await asyncio.gather(
            *(Client.connect(sock_path, cred) for _, cred in clients)
        )
        result["connect_all"] = {
            "clients": len(connections),
            "seconds": round(time.perf_counter() - started, 3),
        }
        rss_samples["idle_bytes"] = read_rss_bytes(host.pid)

        # --- Idle admission latency: host.status, then task.create. ------
        status_lat = []
        for _ in range(args.status_samples):
            _, ms = await timed_call(operator, "host.status", {})
            status_lat.append(ms)
        result["status_latency_idle"] = latency_block(status_lat)

        task_lat = []
        for i in range(args.tasks):
            _, ms = await timed_call(
                operator,
                "task.create",
                {
                    "client_request_id": f"hl-task-{i}",
                    "project_id": "host-load",
                    "origin_key": f"host-load-{i}",
                    "spec": TASK_SPEC,
                },
            )
            task_lat.append(ms)
        result["task_create_latency"] = latency_block(task_lat)

        # --- Baseline cursor: everything admitted so far is drained. -----
        _, _, cursor0 = await drain(operator, 0)

        # --- Sustained event load. ---------------------------------------
        stop_at = time.monotonic() + args.duration
        sent = {"ok": 0, "errors": 0}
        error_codes = {}
        consumed = {"calls": 0, "items": 0}
        inflight = 8  # ipc.max_inflight_per_connection default

        async def sender(idx, client):
            seq = 0
            pending = set()

            async def one(n):
                recipient = clients[(idx + n) % len(clients)][0]
                try:
                    await client.call(
                        "message.send",
                        {
                            "client_request_id": f"hl-msg-{idx}-{n}",
                            "recipient": recipient,
                            "text": "host-load fixture event",
                        },
                    )
                    sent["ok"] += 1
                except RpcError as exc:
                    sent["errors"] += 1
                    error_codes[exc.code] = error_codes.get(exc.code, 0) + 1
                except Exception:
                    sent["errors"] += 1
                    error_codes["TRANSPORT"] = error_codes.get("TRANSPORT", 0) + 1

            while time.monotonic() < stop_at:
                while len(pending) >= inflight:
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )
                pending.add(asyncio.ensure_future(one(seq)))
                seq += 1
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        async def consumer(client):
            cursor = 0
            while time.monotonic() < stop_at:
                try:
                    page = await client.call(
                        "message.read", {"after": cursor, "limit": 200}
                    )
                except Exception:
                    await asyncio.sleep(0.05)
                    continue
                consumed["calls"] += 1
                consumed["items"] += len(page["items"])
                cursor = page["next_cursor"]
                if not page["items"]:
                    # Consumers demonstrate concurrent mailbox service; they
                    # are not a second load generator, so poll gently.
                    await asyncio.sleep(0.1)

        prober = await Client.connect(sock_path, operator_cred)
        status_load_lat = []

        async def probe_status():
            while time.monotonic() < stop_at:
                try:
                    _, ms = await timed_call(prober, "host.status", {})
                    status_load_lat.append(ms)
                except Exception:
                    pass
                await asyncio.sleep(args.status_interval)

        load_started = time.perf_counter()
        workers = []
        for idx in range(args.senders):
            workers.append(asyncio.ensure_future(sender(idx, connections[idx])))
        for idx in range(args.senders, len(connections)):
            workers.append(asyncio.ensure_future(consumer(connections[idx])))
        workers.append(asyncio.ensure_future(probe_status()))
        await asyncio.gather(*workers, return_exceptions=True)
        load_seconds = time.perf_counter() - load_started

        result["event_load"] = {
            "seconds": round(load_seconds, 3),
            "senders": args.senders,
            "consumers": len(connections) - args.senders,
            "message_send_ok": sent["ok"],
            "message_send_errors": sent["errors"],
            "error_codes": error_codes,
            "events_per_second": round(sent["ok"] / load_seconds, 1)
            if load_seconds
            else None,
            "consumer_message_read_calls": consumed["calls"],
            "consumer_items_read": consumed["items"],
        }
        result["status_latency_under_load"] = latency_block(status_load_lat)

        # --- Verify: the durable event stream holds exactly the admitted
        # message.send events, and measure the drain rate. ----------------
        started = time.perf_counter()
        total, kind_count, _ = await drain(operator, cursor0, want_kind="message.send")
        drain_seconds = time.perf_counter() - started
        result["verification_drain"] = {
            "events_after_baseline": total,
            "message_send_events": kind_count,
            "matches_admissions": kind_count == sent["ok"],
            "seconds": round(drain_seconds, 3),
            "events_per_second": round(total / drain_seconds, 1)
            if drain_seconds
            else None,
        }

        # --- Post-load status latency and final state. -------------------
        status_lat = []
        for _ in range(args.status_samples):
            _, ms = await timed_call(operator, "host.status", {})
            status_lat.append(ms)
        result["status_latency_after_load"] = latency_block(status_lat)
        final_status = await operator.call("host.status", {})
        result["final_status"] = {
            "tasks": final_status.get("tasks"),
            "queued_operations": final_status.get("queued_operations"),
            "native_modules_connected": final_status.get("native_modules_connected"),
        }
        db_path = os.path.join(data_dir, "swarm.db")
        result["database_bytes"] = (
            os.path.getsize(db_path) if os.path.exists(db_path) else None
        )

        await prober.close()
        for conn in connections:
            await conn.close()
        await operator.close()
        rss_task.cancel()
        rss_samples["final_bytes"] = read_rss_bytes(host.pid)
        result["host_cpu_seconds"] = read_cpu_seconds(host.pid)
        result["driver_cpu_seconds"] = round(
            time.process_time() - driver_cpu_started, 2
        )
        result["host_rss"] = {
            "idle_bytes": rss_samples["idle_bytes"],
            "peak_bytes": rss_samples["peak_bytes"],
            "final_bytes": rss_samples["final_bytes"],
        }
        return result
    finally:
        if host.poll() is None:
            if os.name == "nt":
                # This fixture host owns no native agents or checks. Terminate
                # only the Popen process created above; never a shared host.
                host.terminate()
                result["shutdown_disposition"] = "owned_fixture_host_terminated"
            else:
                host.send_signal(signal.SIGINT)
                result["shutdown_disposition"] = "owned_fixture_host_sigint"
            try:
                host.wait(timeout=15)
            except subprocess.TimeoutExpired:
                host.kill()
                host.wait(timeout=5)
        host_log.close()
        result["host_exit_code"] = host.returncode
        if not args.keep_data_dir and not args.data_dir:
            shutil.rmtree(data_dir, ignore_errors=True)
            try:
                os.unlink(host_log_path)
            except OSError:
                pass
        elif args.keep_data_dir:
            result["data_dir"] = data_dir
            result["host_log"] = host_log_path


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--swarm", required=True, help="path to the swarm binary")
    parser.add_argument("--clients", type=int, default=200)
    parser.add_argument("--senders", type=int, default=32)
    parser.add_argument("--duration", type=float, default=20.0,
                        help="seconds of sustained event load")
    parser.add_argument("--tasks", type=int, default=200,
                        help="sequential task.create admission samples")
    parser.add_argument("--status-samples", type=int, default=50)
    parser.add_argument("--status-interval", type=float, default=0.05)
    parser.add_argument("--data-dir", default=None,
                        help="default: a fresh temporary directory")
    parser.add_argument("--keep-data-dir", action="store_true")
    parser.add_argument("--out", default=None, help="also write the JSON result here")
    args = parser.parse_args()
    args.swarm = os.path.abspath(args.swarm)
    if args.senders >= args.clients:
        parser.error("--senders must be smaller than --clients")

    loadavg_at_start = [round(v, 2) for v in os.getloadavg()] if hasattr(os, "getloadavg") else None
    result = asyncio.run(run(args))
    result["environment"] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
        "hostname": socket.gethostname(),
        "loadavg_at_start": loadavg_at_start,
    }
    result["parameters"] = {
        "clients": args.clients,
        "senders": args.senders,
        "duration_seconds": args.duration,
        "tasks": args.tasks,
        "ipc_defaults": "max_connections=256, max_inflight_per_connection=8, "
        "max_frame_bytes=1048576 (config.rs defaults; no --config passed)",
    }
    text = json.dumps(result, indent=2, ensure_ascii=False)
    print(text)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    if not result["verification_drain"]["matches_admissions"]:
        print("MISMATCH: drained message.send events != admitted sends", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
