# Observability — Adjustable Logs and Live Agent Monitoring

Source status (2026-10-05): Section 2 describes the implemented recorder and
Manager policy path. Sections 1 and 3–6 retain architecture, target, or
acceptance guidance; they do not claim every listed projection or metric exists.
Use the module/process boundaries in [Modular Runtime](modularity.md).
Existing receipts, `report.delta`, attention/capacity, family observations and
Doctor remain useful foundations; this document does not claim they are absent.

## 1. Three data classes, one explanation of reality

| Class | Source/owner | Loss and authority |
|---|---|---|
| Durable business facts | Kernel/Store: Operations, normalized events, review/acceptance and source identities | Committed atomically; cannot be switched off by a log-level setting. Never acknowledge a lost commit. |
| Diagnostic logs and spans | Component producer → bounded recorder | Configurable, sampled/rotated where appropriate. Loss is counted and visible; logs do not authorize a transition or replace a receipt. |
| Live status and metrics | Shared read-only projections of the above plus real process/native observations | Include timestamp, source freshness and coverage. Silence, disconnection, idle and finished are different states. |

Do not duplicate a full conversation per observer or introduce a second Task
store in the logging module. Derived indexes/files are rebuildable or explicitly
partial; they do not become a competing truth for who owns work or whether it
passed. No model is queried to discover its heartbeat, health or resource usage.

## 2. Configurable depth without a restart

Log **level** and captured **content** are separate settings. Levels are
`off/error/warn/info/debug/trace`. Default is `info` with metadata only. Content
choices are metadata and bounded Atlas-redacted text. Native frames are
unsupported because this source has no bounded frame producer. Increasing to
trace must not silently enable raw prompts, tool arguments, environment dumps,
auth headers, account data or credentials. Hidden reasoning unavailable from a
native API is not a logging feature and must not be fabricated.

The current observer source supports metadata by default and bounded
Atlas-redacted text for selected module-supervisor lifecycle diagnostics. Live
config schema 3 adds `content: "metadata" | "redacted_text"` and optional
scoped content overrides; schema 1 and 2 remain metadata-only. Redacted native
frames are explicitly unsupported until a real bounded frame source exists.

Managers can use `logging.get` and `logging.set` to inspect and save their own
diagnostic scope policy without a GM session. The API accepts metadata or
`redacted_text` for the Manager's own client, current Task/Attempt, owned
Operation, binding generation, or retained module scope. A selected Manager
policy applies its absolute expiry on each producer emission and after-commit
reload; it can raise detail above the observer's default Info level for that
exact scope. Explicit operator `off`, excluded kinds, or a scoped operator
metadata override still fail closed. A scoped Manager `redacted_text` choice
overrides the observer's global metadata default, but Atlas redaction occurs
before redacted-text serialization and either queue, and the observer still applies
its bounded file selector, retention, and TTL. Redacted native frames remain
unsupported because there is no bounded native-frame producer.

`logging.get` reports source capability separately from live availability. Its
runtime projection shows whether the Producer is enabled, has an observer
callback, redactor and live policy, whether the persisted scope policy matches
the loaded Producer snapshot, and whether a non-emitting `module_stopped` scope
probe passes the current operator policy. A passing probe means policy admission,
not proof of a recorder file write; configured operator files remain fail-closed
until the existing recorder worker validates and publishes their first snapshot.

### Host recorder configuration

The host accepts only these fields under `[observability]`; level and content
live in the separate JSON policy file, not in TOML. The recorder is disabled by
default. Relative paths resolve against the controller config file, and an
omitted `directory` uses `<storage.data_dir>/diagnostics`.

```toml
[observability]
enabled = true
live_config_file = "diagnostics-live.json"
queue_bytes = 8388608
queue_records = 256
max_record_bytes = 65536
file_segment_bytes = 16777216
retention_bytes = 134217728
retention_days = 7
```

The exact host `ObservabilityConfig` fields are `enabled`, optional `directory`
and `live_config_file`, `queue_records`, `queue_bytes`, `max_record_bytes`,
`file_segment_bytes`, `retention_bytes`, and `retention_days`. The Host maps
these to the observer's `RecorderConfig` (`file_segment_bytes` becomes
`segment_bytes`) and creates a `LineObserver`; the callback forwards the exact
scoped Manager policy to `HostRecorder`. There is no separate `ObserverOptions`
configuration object. `Store` installs the Atlas text redactor and observer
text-capture policy before constructing the Producer. Raw text is never
serialized: Atlas redaction precedes observer-line text serialization and either
queue admission, while ordinary stderr remains metadata-only.

### Pinned operator live policy

The optional JSON file is read by the existing recorder writer after lazy
startup. Schema 3 is the first version with content controls; schemas 1 and 2
remain metadata-only. Replace `scope_id` with the exact canonical local data
root path. This sample uses all supported kinds and a bounded, expiring
operation override:

```json
{
  "schema_version": 3,
  "config_version": 1,
  "scope_id": "C:\\path\\to\\canonical-data-root",
  "level": "info",
  "content": "metadata",
  "included_kinds": [
    "client_disconnected",
    "store_operation_failed",
    "module_started",
    "module_stopped",
    "agent_delivery_failed",
    "recorder_failure"
  ],
  "overrides": [
    {
      "operation_id": "op_example",
      "level": "debug",
      "content": "redacted_text",
      "expires_at_unix_ms": 4102444800000
    }
  ],
  "retention_bytes": 134217728,
  "retention_days": 7
}
```

Only one selector (`module_id`, `client_id`, or `operation_id`) is accepted per
override; expiry is an absolute Unix timestamp in milliseconds. The live file
is capped at 16 KiB, 64 overrides, and 128 bytes per selector. Explicit operator
`off`, excluded kinds, or a matching metadata-only content override remain
fail-closed restrictions.

### Manager tool

An ordinary registered Manager discovers the `logging_get` and `logging_set`
tools in the typed Manager MCP profile; they dispatch the `logging.get` and
`logging.set` application methods. Read `logging_get` for the target scope
first; `logging_set` accepts the typed `level`, `content`, optional exact scope
selectors, and optional `ttl_seconds` from 1 through 86,400. Omitting selectors
targets the authenticated Manager's own client scope. For example, a temporary
client-scope opt-in is:

```json
{
  "client_request_id": "diagnostic-manager-debug-001",
  "level": "debug",
  "content": "redacted_text",
  "ttl_seconds": 600
}
```

The MCP tool arguments are flat; use the same fields when calling the Store
method directly. Task policies require the exact `task_id`, `task_revision`,
and `attempt_id` together; binding policies require `binding_id` and
`binding_generation` together. A successful mutation commits durably, then
reloads the existing Producer; its policy carries an absolute expiry checked on
each emission. `logging.get` distinguishes source capability and runtime
availability from proof that any record reached disk. Native frames remain
unsupported because no bounded native-frame producer exists.

The queue and retention defaults are bounded engineering settings, not measured
fleet sizing. Bounds are validated. `enabled` concerns the optional recorder,
not the durable event journal. Merely saving a Manager policy does not start an
agent or native session.
The supervisor publishes the effective logging configuration revision to live
producers; each reports applied/unsupported/error for that revision. Validate a
whole change before atomically swapping a process's filter. A manager can narrow
or temporarily increase detail for its existing scope without operator approval.
It cannot expose another restricted principal's content or enable privileged raw
capture through an ordinary log setting. Overrides expire back to baseline even
after a producer restart; changing a filter never restarts a native session.

`logging.get/set` are the ordinary Manager's durable scope-policy interface and
live Producer projection. `monitor.snapshot/follow` and CLI `swarm logs` and
`swarm monitor` remain separate projections of shared observations.
Use stable query fields for module, manager, agent, Task, Attempt, Operation,
binding/generation, time, event kind and severity. Errors name an actual supported
next action, not a mandatory additional approval chain.

## 3. Common record and causal relationships

Use versioned structured JSON records; plain console text is only a rendering.
Do not encode identity in the human message string. Common fields:

```text
schema_version, event_id, stream_id, sequence
occurred_at_utc, recorded_at_utc, monotonic_elapsed_ms, clock_domain
module_id, module_version, build_id, worker_boot_id
process_identity (pid + verified birth/owner reference), native_runtime
actor_id, effective_manager_id, agent_id, parent_agent_id
project_id, task_id, attempt_id, operation_id
binding_id, binding_generation, native_session_id, native_turn_id
trace_id, span_id, parent_span_id, causation_id, correlation_id
kind, level, phase, status, duration_ms
error_code, error_class, exit_code, signal, restart_count
payload_ref, redaction_state, coverage, source_freshness
```

Fields without evidence are absent/null, not synthetic native IDs. Sequence is
per named stream/boot; wall time does not establish a total distributed order.
Only the same monotonic clock domain supports direct duration subtraction.
Trace context is correlation, never authority. Keep original actor separate from
the manager for whom automation runs; retain historical parents across handover.

Capture public agent replies, native-visible tool start/end/results, addressed
control requests, delivery outcomes, child observations and final result links
when the selected source exposes them. Store large text once as an authorized
bounded artifact/reference, not in event-selector metadata or every notification.
Exact effective model/usage is recorded only when observed; requested model/effort
is a separate field. Missing usage/cost stays unknown, never zero by default.

For each failure answer: **which module/agent, what operation, when, which phase,
which process boot, observed outcome, restart/readback action and remaining gap**.
A supervisor's OS wait result can identify an exit or signal even if its child
could not log. A killed process may not emit a backtrace; distinguish a suspected
OOM from proven OS evidence. Panic/source-location diagnostics use protected
symbol/backtrace artifacts linked to the exact build; release symbols may be
stored separately. Do not broadcast raw panic strings as public event metadata.

## 4. Implementation and failure isolation

Use complete maintained Rust tracing libraries: `tracing`,
`tracing-subscriber` filter/reload and structured formatting. Wrap the filter, not
an entire sink unnecessarily. Apply `enabled!`/equivalent checks before expensive
formatting or serialization; metadata-off must not construct a huge payload and
throw it away afterward. Compile-time removal of debug/trace must not invalidate
the advertised ability to enable them dynamically.

The producer has a bounded queue with both byte and record limits. The recorder
owns file I/O/rotation/indexing. `tracing-appender` offers a bounded nonblocking
writer and loss counter, but its line-count capacity alone is not a byte budget:
bound each record and account total queued bytes. Retain its `WorkerGuard` for
the worker's lifetime; graceful shutdown performs a bounded flush. SIGKILL/power
loss can still lose trailing diagnostics. Never claim exactly-once logs.

Do not put logs on protocol stdout. Use a dedicated framed diagnostic channel or
stderr captured once by the owner. Bound and redact stderr too. Slow/disconnected
readers cannot block control messages, cancellation or terminal evidence. Prioritize
small control/terminal notifications over bulk text without claiming business
facts were committed before Store says so. Share one upstream native reader per
service scope and fan out locally; no repeated CLI launches per panel/viewer.

When the recorder is disabled or broken, workers retain a bounded emergency path
for critical diagnostics and report sink health/lost-record counters on reconnect.
There is no always-running recorder thread when its sink is disabled. A failure
while recording a failure must not recurse into the same broken sink. A kernel
journal error is different: it cannot be swallowed as `false` or downgraded into
an acknowledged operation. Surface it via supervisor health and fail the affected
admission truthfully.

Retention applies byte and age limits, rotates off the hot path and exposes disk
quota/exhaustion. Never delete source evidence referenced by unresolved work or
accepted-candidate policy under a generic log cleanup rule. A disconnected slow
subscriber retains only its bounded cursor/lag state, not an unbounded private
copy of every event. Sampling may reduce diagnostic traces, not durable business
receipts. Account and display dropped records and the affected interval.

## 5. Live monitoring and future charts

Provide a read-only initial snapshot tied to a journal cut, followed by events
after its cursor. Register/capture the cut so updates racing with the snapshot
are recoverable. On a lagged volatile channel, replay retained journal facts;
on an expired retention cursor, return an explicit gap and fresh snapshot.
Do not silently report continuous history or complete native family coverage.

Minimum live view: manager/agent tree, module boot/state, current Task/Operation,
last activity, native child coverage, queue depth, waiting reason, recent outcome,
error and next recovery action. Process alive is not native turn running; an
unchanging model output is not proof of a dead process. A viewer disconnect does
not stop or restart the observed agent. Refresh rate and inspected subjects are
user configurable and based on shared observations, not active model prompts.

Record aggregate rates/counters/histograms for admitted/completed/failed/unknown
operations, queue bytes/age, event lag/loss, dispatch and native durations,
module starts/restarts, and measured active-process CPU/RSS. Use bounded metric
labels such as module/runtime/status. Agent/Task/Operation IDs belong in logs or
exemplars, not unlimited time-series labels. Avoid counting final cumulative
usage again after per-turn deltas. Shared native-process RSS must not be added
once for every session hosted in it.

A bounded JSONL/CSV query/export is sufficient for later time plots and dependency
views. Optional OTLP/Prometheus/dashboard exporters can be separate modules when
needed; none is required to launch an agent or run the local monitor. Preserve
schema/version, units, unknowns and time ranges so a future chart is not guessing.

## 6. Privacy and acceptance

Apply redaction **before** disk, fanout and export, including secrets split across
native chunks. Escape untrusted terminal control sequences/markup when rendering.
Truncate or suppress a frame that cannot be safely bounded/redacted; do not allow
trace to disable secret scrubbing. Local file permissions match the controller's
private state model. Project/participant scope is checked before query pagination,
subscription and artifact retrieval, not only when formatting a response.

After the product path is implemented, validate filter reload/expiry, correlated
manager → child → tool → result events, one optional worker crash/recovery,
recorder loss and disk-full behavior, slow-subscriber gap recovery, redaction of
split synthetic secrets and disabled-module idle behavior. Verify ordinary agent
work still proceeds with observer/recorder absent. These are acceptance scenarios,
not a request to run a new broad test campaign before writing the implementation.

Primary library documentation read 2026-10-05: [tracing-subscriber reload, 0.3.23](https://docs.rs/tracing-subscriber/0.3.23/tracing_subscriber/reload/),
[tracing-appender nonblocking, 0.2.5](https://docs.rs/tracing-appender/0.2.5/tracing_appender/non_blocking/),
[Tokio broadcast](https://docs.rs/tokio/latest/tokio/sync/broadcast/) and
[Tokio watch](https://docs.rs/tokio/latest/tokio/sync/watch/).
These are reviewed source/API versions, not dependencies installed by this PR.
