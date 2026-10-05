# Observability — Adjustable Logs and Live Agent Monitoring

Owner direction: 2026-10-05. **Target contract; not implemented by this documentation PR.**
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
choices are metadata, redacted text and bounded redacted native frames. Increasing
to trace must not silently enable raw prompts, tool arguments, environment dumps,
auth headers, account data or credentials. Hidden reasoning unavailable from a
native API is not a logging feature and must not be fabricated.

Target configuration example (proposed schema; not accepted by current Config):

```toml
[observability]
enabled = true
level = "info"
content = "metadata"
queue_bytes = 8388608
max_record_bytes = 65536
file_segment_bytes = 16777216
retention_bytes = 134217728
retention_days = 7
metrics_interval_ms = 2000

[observability.overrides.opencode_debug]
module = "opencode"
level = "debug"
content = "redacted_text"
ttl_seconds = 600
```

Defaults above are initial engineering budgets, not measured fleet sizing. Bounds
are validated, configurable and reported. `enabled` concerns the optional recorder,
not the durable event journal. Resource sampling runs only for active selected
subjects/observers; merely saving this configuration does not load an agent.

The supervisor publishes the effective logging configuration revision to live
producers; each reports applied/unsupported/error for that revision. Validate a
whole change before atomically swapping a process's filter. A manager can narrow
or temporarily increase detail for its existing scope without operator approval.
It cannot expose another restricted principal's content or enable privileged raw
capture through an ordinary log setting. Overrides expire back to baseline even
after a producer restart; changing a filter never restarts a native session.

Proposed normal interfaces: `logging.get/set` and `monitor.snapshot/follow` through
the shared method registry; CLI `swarm logs` and `swarm monitor` are projections.
They are **new interfaces to implement**, not available commands in this revision.
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
