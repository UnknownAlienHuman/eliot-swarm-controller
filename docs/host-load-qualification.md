# Host-only load contour

C11 measurement, recorded 2026-10-02. This document describes a reproducible
host-only measurement scenario and one recorded run of it. It is contour
qualification on fixtures: no native runtimes, bridges or model calls were
involved, and nothing here is platform qualification, a benchmark claim, or
evidence about real incidents. The plan's figures (200 observed agents /
1 000 small events per second / p95 host < 100 ms; implementation plan v6
§12, architecture §14) are *measurement targets*; this contour records what
one run measured, under the conditions stated below, and no more.

## Scenario

`tools/host-load.py` (Python 3 standard library only) starts one real
`swarm host` process on a fresh temporary data directory and drives it over
its local IPC — newline-delimited JSON-RPC 2.0 on the Unix socket
(`control.sock`; the request `id` must be a nonempty string,
`model::Request::validate`). No `--config` is passed, so the `config.rs`
defaults apply (`max_connections = 256`,
`max_inflight_per_connection = 8`, `max_frame_bytes = 1 048 576`).

Phases, in order:

1. **Registration.** The operator registers the client population
   (`client.register`, role `manager`). These are controller clients, not
   native agents; the contour's "population" is the host-side proxy for an
   observed population, and native-agent observation is not measured.
2. **Connect.** All clients connect (`client.hello`) and stay connected for
   the whole run.
3. **Idle admission latency.** Sequential `host.status` samples, then
   sequential `task.create` admissions with unique origin keys (operator).
4. **Sustained event load** (default 20 s). Sender connections pipeline
   `message.send` up to the per-connection in-flight limit; every admitted
   mutation is one durable event (an operation plus one observation in the
   host event stream, SQLite `synchronous = FULL`). The remaining clients
   act as consumers, draining their own mailboxes with `message.read`
   (independent per-consumer cursors). A separate operator connection
   samples `host.status` every 50 ms during the window.
5. **Verification drain.** After the window, `report.delta` (pages of 200)
   reads the event stream from the pre-load baseline cursor. The count of
   `message.send` observations must equal the senders' successful
   admissions exactly; a mismatch fails the run (exit 1) and is reported,
   not hidden.
6. **Post-load latency and state.** `host.status` samples again, final
   counters, database size, and host RSS (VmRSS sampled every 250 ms;
   Linux `/proc`, reported as null elsewhere). The host is stopped with
   SIGINT and its exit code recorded. The temporary data directory is
   removed unless `--keep-data-dir` is given.

Reproduce:

```sh
cargo build --locked --release --bin swarm
python3 tools/host-load.py --swarm target/release/swarm --out result.json
```

## Recorded run

Source: commit `e6f0ace` (release build, controller `0.1.0`, SQLite 3.53.2).
Parameters: 200 clients, 32 senders, 168 consumers, 20 s load window,
200 `task.create` samples, 50 status samples per latency block.

Environment — read this before the numbers: a shared 2-vCPU Linux VM
(`Linux-7.0.0-39-generic x86_64`, Python 3.12.3 driver) that was *not*
quiet: unrelated parallel Rust builds by other agents kept the load
average at 7.1–12.6 for the whole run. CPU counters collected by the
harness show the host used 3.26 CPU-seconds and the driver 1.13
CPU-seconds over the ~30 s run: neither side was CPU-bound. The admission
ceiling below is therefore an environment-bound contour observation (the
store serializes durable commits, and on this VM each queued request also
waits behind an oversubscribed scheduler), not a measured host capacity.

| Measurement | Result |
|---|---|
| Registration, 200 clients | 1.93 s total |
| Connect all 200 clients | 0.15 s total |
| `host.status` latency, idle | p50 3.4 ms, p95 14.9 ms, max 18.7 ms (n=50) |
| `task.create` latency, sequential | p50 5.0 ms, p95 22.2 ms, max 75.0 ms (n=200) |
| Event load window | 23.0 s wall (20 s window + sender drain) |
| `message.send` admissions | 2 713 ok, 0 errors |
| Admission rate | 117.9 events/s |
| Consumer `message.read` during window | 1 802 calls, 2 076 items |
| `host.status` latency under load | p50 1 947 ms, p95 3 621 ms (n=11) |
| Verification drain | 2 713 events after baseline, 2 713 `message.send` — exact match; 0.29 s = 9 391 events/s readback |
| `host.status` latency after load | p50 5.1 ms, p95 21.2 ms, max 28.6 ms (n=50) |
| Final state | 200 tasks, 0 queued operations, 0 native modules |
| Database size at end | 4 448 256 bytes |
| Host RSS | idle 13.6 MB, peak 19.3 MB, final 19.2 MB |
| Host exit | 0 (SIGINT) |

Reading of the results, without decoration:

- **200 connected clients work.** Registration, simultaneous connection,
  concurrent mailbox service and zero admission errors are demonstrated at
  the plan's population size — for controller clients. Observing 200
  *native agents* additionally requires real bindings/bridges and remains
  platform qualification on the owner's machine.
- **The 1 000 events/s target was not demonstrated.** The contour admitted
  117.9 durable events/s. The CPU evidence above shows this run was bound
  by the contended VM, so the number is a lower-bound observation under
  stated conditions — neither a platform qualification nor a refutation of
  the target. A qualification run needs a quiet machine.
- **Status latency under load is queueing, not status cost.** Idle and
  post-load `host.status` p95 stayed at or below ~21 ms on this VM, inside
  the plan's 100 ms target. During the load window the prober's requests
  queued behind up to 256 in-flight durable mutations in the serialized
  store and took seconds. That is a property of this load shape (a full
  in-flight backlog of fsync-committed writes) recorded as observed; it is
  not an idle-status regression.
- **RSS is far under the orientation figure.** Peak host RSS was 19.3 MB
  against the ~256 MiB orientation (architecture §14, host without
  histories). This contour's history is small (200 tasks, ~2 900 events);
  RSS with real retained histories is not measured here.
- **Event-stream integrity held.** The verification drain found exactly
  the admitted events — no loss, no duplication — and readback ran at
  9 391 events/s even on this VM.

A first run with a more aggressive consumer poll interval (20 ms instead
of 100 ms) produced consistent figures under the same conditions
(106.0 events/s admitted, drain 9 359 events/s, peak RSS 19.6 MB, exact
verification match), which is why the recorded run's gentler consumer
polling is not believed to flatter the result.

## What remains platform qualification (not this contour)

- Load with real bridges/native runtimes and model calls on the owner's
  machine; bridge, runtime, Cargo and model-quota counters are measured
  separately there (plan §12).
- Qualification on real operating incidents, as they occur (plan §13);
  incidents are not fabricated for this contour.
- Zed batch and ACP remain separate later slices (plan §11/§16).

## Raw result of the recorded run

```json
{
  "scope": "host-only fixture load; no native runtimes, bridges or model calls",
  "controller_version": "0.1.0",
  "sqlite_version": "3.53.2",
  "registration": {
    "clients": 200,
    "seconds": 1.925
  },
  "connect_all": {
    "clients": 200,
    "seconds": 0.147
  },
  "status_latency_idle": {
    "count": 50,
    "min_ms": 0.669,
    "p50_ms": 3.413,
    "p95_ms": 14.867,
    "p99_ms": 18.65,
    "max_ms": 18.65,
    "mean_ms": 5.048
  },
  "task_create_latency": {
    "count": 200,
    "min_ms": 0.605,
    "p50_ms": 4.954,
    "p95_ms": 22.243,
    "p99_ms": 41.632,
    "max_ms": 75.008,
    "mean_ms": 6.991
  },
  "event_load": {
    "seconds": 23.014,
    "senders": 32,
    "consumers": 168,
    "message_send_ok": 2713,
    "message_send_errors": 0,
    "error_codes": {},
    "events_per_second": 117.9,
    "consumer_message_read_calls": 1802,
    "consumer_items_read": 2076
  },
  "status_latency_under_load": {
    "count": 11,
    "min_ms": 177.562,
    "p50_ms": 1947.501,
    "p95_ms": 3620.542,
    "p99_ms": 3620.542,
    "max_ms": 3620.542,
    "mean_ms": 1934.813
  },
  "verification_drain": {
    "events_after_baseline": 2713,
    "message_send_events": 2713,
    "matches_admissions": true,
    "seconds": 0.289,
    "events_per_second": 9390.9
  },
  "status_latency_after_load": {
    "count": 50,
    "min_ms": 0.318,
    "p50_ms": 5.052,
    "p95_ms": 21.23,
    "p99_ms": 28.582,
    "max_ms": 28.582,
    "mean_ms": 6.983
  },
  "final_status": {
    "tasks": 200,
    "queued_operations": 0,
    "native_modules_connected": 0
  },
  "database_bytes": 4448256,
  "host_rss": {
    "idle_bytes": 13570048,
    "peak_bytes": 19259392,
    "final_bytes": 19226624
  },
  "host_cpu_seconds": 3.26,
  "driver_cpu_seconds": 1.13,
  "host_exit_code": 0,
  "environment": {
    "platform": "Linux-7.0.0-39-generic-x86_64-with-glibc2.39",
    "machine": "x86_64",
    "cpu_count": 2,
    "python": "3.12.3",
    "hostname": "htch-runtime",
    "loadavg_at_start": [
      7.11,
      11.51,
      12.05
    ]
  },
  "parameters": {
    "clients": 200,
    "senders": 32,
    "duration_seconds": 20.0,
    "tasks": 200,
    "ipc_defaults": "max_connections=256, max_inflight_per_connection=8, max_frame_bytes=1048576 (config.rs defaults; no --config passed)"
  }
}
```
